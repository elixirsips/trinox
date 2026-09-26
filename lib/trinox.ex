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
  Asks the coordinator whether `conn` is still usable.

  Nothing calls this for you — there is no pool watching the connection while it sits
  idle, so this is how a caller that cares checks one it has been holding on to.
  """
  @spec ping(conn(), keyword()) :: :ok | {:error, Exception.t()}
  def ping(conn, opts \\ []), do: Connection.ping(conn, opts)
end
