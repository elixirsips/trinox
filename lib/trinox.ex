defmodule Trinox do
  @moduledoc """
  A Trino driver for Elixir.

  A connection is started explicitly and then queried, the same way `Postgrex` works:

      {:ok, conn} =
        Trinox.start_link(
          scheme: :https,
          hostname: "trino.example.com",
          username: "alice",
          password: "s3cret",
          catalog: "tpch",
          schema: "sf1"
        )

      {:ok, result} = Trinox.query(conn, "SELECT 1 AS n")
      result.rows
      #=> [[1]]

  Under it, `Trinox.Connection` is a process holding one HTTP connection to the
  coordinator and the session that connection has accumulated, so a `USE` or
  `SET SESSION` in one query is still in force for the next query on the same connection.

  One connection runs one query at a time: a query holds its connection for as long as
  Trino takes to finish it, and a caller that needs two at once needs two connections.
  There is no pool here — start as many connections as you need concurrency, under your
  own supervisor or a pooler of your choosing. Give each one a `:name` and they can sit
  side by side in the same supervisor.

  A connection that cannot reach its coordinator keeps trying rather than failing to
  start, so Trino being slow to come up does not stop your application coming up; see
  `Trinox.Connection` for what that means for the queries in between.

  `transaction/3` runs several statements in one Trino transaction, for the connectors
  that support them.

  ## Options

  `start_link/1` takes `Trinox.Protocol`'s options — `:username` (required), `:password`,
  `:scheme`, `:hostname`, `:port`, `:catalog`, `:schema`, `:transport_opts`,
  `:connect_timeout`, `:receive_timeout`, `:poll_interval_ms` — plus `:name` and the
  other `GenServer.start_link/3` options, which name the connection process.

  `query/3` takes `:timeout` (default `:infinity`), which bounds the whole query, plus
  `:receive_timeout` and `:poll_interval_ms`, which override the connection's for that one
  query. A query that runs past its `:timeout` is cancelled on the coordinator and
  returns a `Trinox.Error`, leaving the connection free for the next caller — an
  abandoned query would otherwise go on running and go on holding it.

  ## Supervision

  `Trinox` is a supervisor child like any other, so a connection usually belongs in the
  application's tree rather than in a bare `start_link/1`:

      children = [
        {Trinox, name: MyApp.Trino, hostname: "trino.example.com", username: "alice"}
      ]

  A named connection is then queried by that name: `Trinox.query(MyApp.Trino, sql)`.
  """

  alias Trinox.Connection
  alias Trinox.Result

  @typedoc "A connection: the pid `start_link/1` returned, or the `:name` it was given."
  @type conn :: Connection.conn()

  @doc """
  Starts a connection to a Trino coordinator and links it to the calling process.

  See the module documentation for the options.
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts), do: Connection.start_link(opts)

  @doc """
  The child specification for starting a connection under a supervisor.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts), do: Connection.child_spec(opts)

  @doc """
  Runs `statement` on `conn` and waits for it to finish.

  Returns `{:error, %Trinox.Error{}}` for a query Trino refused — a missing table, a
  syntax error, a query that ran out of resources — since Trino reports those in the
  result rather than by failing the request. Other errors mean the conversation with the
  coordinator itself broke down, and arrive as whatever `Mint` or `Jason` said about it;
  a connection that did not survive one stops once the caller has its answer.

  Trino runs a statement asynchronously and this call polls it to completion, so a long
  query holds its connection for as long as it runs. `:timeout` bounds that, and a query
  that reaches it is cancelled on the coordinator rather than merely abandoned.

      {:ok, result} = Trinox.query(conn, "SELECT name FROM nation LIMIT 2")
      result.columns
      #=> ["name"]
  """
  @spec query(conn(), String.t(), keyword()) :: {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, opts \\ []), do: Connection.query(conn, statement, opts)

  @doc """
  Runs `statement` on `conn` and returns the `Trinox.Result`, raising on failure.

  Same as `query/3` otherwise.
  """
  @spec query!(conn(), String.t(), keyword()) :: Result.t()
  def query!(conn, statement, opts \\ []) do
    case query(conn, statement, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Runs `fun` inside a Trino transaction on `conn`.

  `START TRANSACTION` is sent first, `fun` is called with `conn`, and then `COMMIT` — or
  `ROLLBACK`, if `fun` raises, throws, exits or calls `rollback/2`. Returns `{:ok, value}`
  with whatever `fun` returned, or `{:error, reason}` if the transaction could not be
  started or committed. Anything `fun` raised is re-raised once the rollback has been sent.

      Trinox.transaction(conn, fn conn ->
        Trinox.query!(conn, "INSERT INTO t VALUES (1)")
        Trinox.query!(conn, "INSERT INTO t VALUES (2)")
      end)

  Only connectors that implement transactions support this; against one that does not,
  Trino refuses the `START TRANSACTION` and the refusal comes back as `{:error, _}`.

  ## The connection is yours for the duration

  A Trino transaction lives in the session, and the session belongs to one connection, so
  for as long as `fun` runs that connection is reserved for the calling process. A query
  sent from any other process is refused rather than silently joined to the transaction —
  which means `fun` must do its own work rather than handing `conn` to a `Task`. Transactions
  do not nest: a `transaction/3` inside a `transaction/3` returns an error.

  If the calling process dies inside `fun`, the transaction is rolled back and the
  connection stops, since what it was in the middle of is nobody's to finish. And if the
  connection itself dies, this call exits like any other call to a dead process.
  """
  @spec transaction(conn(), (conn() -> result), keyword()) ::
          {:ok, result} | {:error, Exception.t()}
        when result: var
  def transaction(conn, fun, opts \\ []) do
    case Connection.begin(conn, opts) do
      :ok -> run_transaction(conn, fun, opts)
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Rolls the surrounding `transaction/3` back, which returns `{:error, reason}`.

  This throws, so nothing after it in `fun` runs.

      Trinox.transaction(conn, fn conn ->
        Trinox.query!(conn, "DELETE FROM t")
        Trinox.rollback(conn, :changed_my_mind)
      end)
      #=> {:error, :changed_my_mind}
  """
  @spec rollback(conn(), term()) :: no_return()
  def rollback(_conn, reason), do: throw({__MODULE__, :rollback, reason})

  @doc """
  Asks the coordinator whether `conn` is still usable.

  Nothing calls this for you — there is no pool watching the connection while it sits
  idle, so this is how a caller that cares checks one it has been holding on to.
  """
  @spec ping(conn(), keyword()) :: :ok | {:error, Exception.t()}
  def ping(conn, opts \\ []), do: Connection.ping(conn, opts)

  defp run_transaction(conn, fun, opts) do
    fun.(conn)
  catch
    :throw, {__MODULE__, :rollback, reason} ->
      _ = Connection.rollback(conn, opts)
      {:error, reason}

    kind, reason ->
      stacktrace = __STACKTRACE__
      _ = Connection.rollback(conn, opts)
      :erlang.raise(kind, reason, stacktrace)
  else
    value -> commit(conn, value, opts)
  end

  defp commit(conn, value, opts) do
    case Connection.commit(conn, opts) do
      :ok -> {:ok, value}
      {:error, error} -> {:error, error}
    end
  end
end
