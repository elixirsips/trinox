defmodule Trinox.Connection do
  @moduledoc """
  The process that owns one connection to a Trino coordinator.

  `Trinox.Protocol` is purely functional and its Mint connection may only be used by the
  process that opened it, so something has to own that state and serialise access to it.
  That is all this `GenServer` does: it opens the connection, runs one query at a time
  against it, and closes it on the way out.

  Queries run in this process, so a query holds the connection for as long as it takes
  Trino to finish it; a caller that needs two queries at once needs two connections.

  `Trinox.start_link/1` and `Trinox.query/3` are the public face of this module — reach
  for it directly only to start a connection without `Trinox`'s option handling.

  ## Starting up, and staying up

  `start_link/1` returns as soon as the process exists, and the connection is opened
  immediately afterwards. A coordinator that cannot be reached is therefore not a startup
  failure: the process stays alive and retries, doubling the wait from 200ms up to 30
  seconds, so an application whose Trino is slower to boot than it is does not die trying.
  Queries in the meantime return the error that connecting last failed with.

  Bad *options* are a different matter — a missing `:username` will still be missing on
  the next attempt — so those refuse to start at all.

  A connection that breaks mid-life stops instead of reconnecting, once the caller in
  flight has its answer. The session built up on it (catalog, schema, properties) died
  with it, and quietly carrying on with an empty one would be a lie; stopping tells a
  supervisor to put a fresh connection in its place, and tells anyone monitoring that the
  session they were relying on is gone.

  ## Timeouts

  `:timeout` is enforced here rather than by the caller's `GenServer.call/3`, so that a
  query which runs out of time is *cancelled* — `Trinox.Statement` `DELETE`s it on the
  coordinator — and the connection is free again the moment the caller gives up. A caller
  that simply stopped waiting would have left the query running and the connection busy.
  """

  use GenServer

  alias Trinox.Protocol
  alias Trinox.Result

  require Logger

  # The options GenServer takes for the process itself, rather than for the connection.
  @server_opts [:name, :debug, :spawn_opt, :hibernate_after]

  @backoff_min_ms 200
  @backoff_max_ms 30_000

  # Slack on the caller's side of a bounded call, so that the deadline the connection
  # enforces is reached first and the caller gets an error rather than an exit. It only
  # comes into play when a connection never picks the call up at all.
  @call_grace_ms 250

  defstruct [:opts, :protocol, :error, :backoff]

  @typedoc "A connection: the pid `start_link/1` returned, or the `:name` it was given."
  @type conn :: GenServer.server()

  @typep t :: %__MODULE__{
           opts: keyword(),
           protocol: Protocol.t() | nil,
           error: Exception.t() | nil,
           backoff: pos_integer() | nil
         }

  @doc """
  Starts a connection and links it to the calling process.

  Takes `Trinox.Protocol`'s options, plus `:name` and the other `GenServer.start_link/3`
  options, which are used for the process rather than passed on to the protocol.

  Returns `{:error, reason}` only for options that cannot work; a coordinator that is
  merely unreachable is retried, as the module documentation describes.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {server_opts, connect_opts} = Keyword.split(opts, @server_opts)

    GenServer.start_link(__MODULE__, connect_opts, server_opts)
  end

  @doc """
  The child specification for starting a connection under a supervisor.

  A named connection is identified by its `:name`, so that the several connections it
  takes to run several queries at once can sit side by side under one supervisor without
  colliding.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: Keyword.get(opts, :name, __MODULE__), start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Runs `statement` on `conn` and waits for it to finish.

  `:timeout` (default `:infinity`) bounds the whole query, polling included; a query that
  reaches it is cancelled on the coordinator and returns a `Trinox.Error`.
  `:receive_timeout` bounds each single HTTP request underneath it.
  """
  @spec query(conn(), String.t(), keyword()) :: {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    GenServer.call(conn, {:execute, statement, opts, deadline(timeout)}, call_timeout(timeout))
  end

  @doc """
  Asks the coordinator whether the connection is still good.

  Returns `{:error, reason}` and stops the connection when it is not — there is nothing
  left to salvage, and a supervised connection comes back with a fresh one.
  """
  @spec ping(conn(), keyword()) :: :ok | {:error, Exception.t()}
  def ping(conn, opts \\ []) do
    GenServer.call(conn, :ping, Keyword.get(opts, :timeout, :infinity))
  end

  @impl true
  def init(opts) do
    case Protocol.validate(opts) do
      :ok -> {:ok, %__MODULE__{opts: opts}, {:continue, :connect}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_call({:execute, _statement, _opts, _deadline}, _from, %{protocol: nil} = state) do
    {:reply, {:error, state.error}, state}
  end

  @impl true
  def handle_call({:execute, statement, opts, deadline}, _from, state) do
    state.protocol
    |> Protocol.execute(statement, Keyword.put(opts, :deadline, deadline))
    |> reply(state)
  end

  @impl true
  def handle_call(:ping, _from, %{protocol: nil} = state) do
    {:reply, {:error, state.error}, state}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    case Protocol.ping(state.protocol) do
      {:ok, protocol} -> {:reply, :ok, %{state | protocol: protocol}}
      {:disconnect, reason, protocol} -> stop(reason, %{state | protocol: protocol})
    end
  end

  @impl true
  def handle_info(:reconnect, state), do: {:noreply, connect(state)}

  @impl true
  def terminate(_reason, %{protocol: nil}), do: :ok
  def terminate(_reason, %{protocol: protocol}), do: Protocol.close(protocol)

  @spec connect(t()) :: t()
  defp connect(%__MODULE__{opts: opts} = state) do
    case Protocol.connect(opts) do
      {:ok, protocol} -> %{state | protocol: protocol, error: nil, backoff: nil}
      {:error, reason} -> schedule_reconnect(state, reason)
    end
  end

  defp schedule_reconnect(%__MODULE__{} = state, reason) do
    backoff = next_backoff(state.backoff)

    Logger.warning(
      "Trinox could not connect to Trino (#{Exception.message(reason)}); " <>
        "retrying in #{backoff}ms"
    )

    Process.send_after(self(), :reconnect, backoff)

    %{state | protocol: nil, error: reason, backoff: backoff}
  end

  defp next_backoff(nil), do: @backoff_min_ms
  defp next_backoff(backoff), do: min(backoff * 2, @backoff_max_ms)

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp call_timeout(:infinity), do: :infinity
  defp call_timeout(timeout), do: timeout + @call_grace_ms

  defp reply({:ok, result, protocol}, state) do
    {:reply, {:ok, result}, %{state | protocol: protocol}}
  end

  defp reply({:error, reason, protocol}, state) do
    {:reply, {:error, reason}, %{state | protocol: protocol}}
  end

  defp reply({:disconnect, reason, protocol}, state) do
    stop(reason, %{state | protocol: protocol})
  end

  # A connection the coordinator dropped is of no further use, so the process goes with
  # it once the caller has its answer. Stopping normally leaves the caller's own link
  # alone and still tells a supervisor to start a fresh connection.
  defp stop(reason, state), do: {:stop, :normal, {:error, reason}, state}
end
