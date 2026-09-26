defmodule Trinox do
  @moduledoc """
  A Trino driver for Elixir, built on `DBConnection`.

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

  Under it, `Trinox.Protocol` holds one HTTP connection to the coordinator and the
  session that connection has accumulated, so a `USE` or `SET SESSION` in one query is
  still in force for the next query on the same connection. With a pool of more than one
  connection, that "next query" may land on a different connection and see a different
  session — the same caveat Postgres's `SET` carries.

  ## Options

  `start_link/1` takes `Trinox.Protocol`'s options — `:username` (required), `:password`,
  `:scheme`, `:hostname`, `:port`, `:catalog`, `:schema`, `:transport_opts`,
  `:connect_timeout`, `:receive_timeout`, `:poll_interval_ms` — and passes anything else
  on to `DBConnection`, which is where `:pool_size`, `:name` and `:idle_interval` are
  documented.

  `query/3` takes `DBConnection`'s per-call options (`:timeout`, `:queue_target`, ...)
  plus `:receive_timeout` and `:poll_interval_ms`, which override the connection's for
  that one query.

  ## Supervision

  `Trinox` is a supervisor child like any other, so a connection usually belongs in the
  application's tree rather than in a bare `start_link/1`:

      children = [
        {Trinox, name: MyApp.Trino, hostname: "trino.example.com", username: "alice"}
      ]

  A named connection is then queried by that name: `Trinox.query(MyApp.Trino, sql)`.
  """

  alias Trinox.Query
  alias Trinox.Result

  @typedoc "A connection: the pid `start_link/1` returned, or the `:name` it was given."
  @type conn :: DBConnection.conn()

  @doc """
  Starts a connection to a Trino coordinator and links it to the calling process.

  See the module documentation for the options.
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    DBConnection.start_link(Trinox.Protocol, opts)
  end

  @doc """
  The child specification for starting a connection under a supervisor.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    DBConnection.child_spec(Trinox.Protocol, opts)
  end

  @doc """
  Runs `statement` on `conn` and waits for it to finish.

  Returns `{:error, %Trinox.Error{}}` for a query Trino refused — a missing table, a
  syntax error, a query that ran out of resources — since Trino reports those in the
  result rather than by failing the request. Other errors mean the conversation with the
  coordinator itself broke down, and arrive as whatever `Mint`, `Jason` or `DBConnection`
  raised about it.

  Trino runs a statement asynchronously and this call polls it to completion, so a long
  query holds its connection for as long as it runs; `:timeout` bounds the wait.

      {:ok, result} = Trinox.query(conn, "SELECT name FROM nation LIMIT 2")
      result.columns
      #=> ["name"]
  """
  @spec query(conn(), String.t(), keyword()) :: {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, opts \\ []) do
    case DBConnection.prepare_execute(conn, %Query{statement: statement}, [], opts) do
      {:ok, _query, result} -> {:ok, result}
      {:error, error} -> {:error, error}
    end
  end

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
end
