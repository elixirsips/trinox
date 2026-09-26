defmodule Trinox.Protocol do
  @moduledoc """
  Trino's protocol state, and the operations that move it forward.

  Trino speaks a stateless HTTP/JSON protocol rather than holding state on a socket, but
  a connection still owns something worth keeping: the open HTTP connection to the
  coordinator, and the session properties the coordinator pushes back.

  This module is purely functional — every operation takes the state and hands back the
  state it produced, and nothing here runs in a process of its own. `Trinox.Connection`
  is what owns the state and serialises access to it; this module is what it calls.

      {:ok, state} = Trinox.Protocol.connect(scheme: :http, hostname: "localhost",
                                             port: 8080, username: "alice")
      {:ok, result, state} = Trinox.Protocol.execute(state, "SELECT 1", [])

  ## Options

    * `:username` — required; used as `X-Trino-User` and as the Basic-auth user.
    * `:password` — optional; when given, requests carry `Authorization: Basic ...`.
      Trino only accepts Basic auth over TLS, so this belongs with `scheme: :https`.
    * `:scheme` — `:http` or `:https` (default `:https`).
    * `:hostname` — default `"localhost"`.
    * `:port` — default `default_port/1` for the scheme.
    * `:catalog`, `:schema` — optional session defaults.
    * `:source` — what to report as `X-Trino-Source` (default `"trinox"`). This is what an
      operator sees beside a query in the Trino UI and the query log.
    * `:client_info` — sent as `X-Trino-Client-Info`, for anything else worth recording
      about the caller.
    * `:time_zone` — sent as `X-Trino-Time-Zone`, an IANA name like `"Europe/Berlin"`.
      Nothing is sent unless it is given, and the coordinator then applies its own default
      — so set it if a `timestamp with time zone` has to mean the same thing everywhere.
    * `:transport_opts`, `:connect_timeout`, `:receive_timeout` — passed to `Trinox.HTTP`.
    * `:poll_interval_ms` — passed to `Trinox.Statement`; `:receive_timeout` and this
      one may also be given per query, where they override the connection's.

  Any other option is ignored.

  Transactions and cursors are not supported. Trino does support transactions for
  connectors that implement them, and its `nextUri` paging is the obvious way to
  implement cursors later, so both are gaps to fill rather than rules of the protocol.
  """

  alias Trinox.Error
  alias Trinox.HTTP
  alias Trinox.Result
  alias Trinox.ResultDecoder
  alias Trinox.Session
  alias Trinox.Statement

  @info_path "/v1/info"

  # The options a caller may set per query as well as per connection.
  @query_opts [:receive_timeout, :poll_interval_ms, :max_attempts]

  # Who is asking, as opposed to what is being asked. None of this is session state the
  # coordinator can change, so it is settled once at connect time.
  @client_opts [:source, :client_info, :time_zone]

  @default_source "trinox"

  defstruct [
    :conn,
    :scheme,
    :hostname,
    :port,
    :user,
    :password,
    session: %Session{},
    client_opts: [],
    request_opts: []
  ]

  @type t :: %__MODULE__{
          conn: HTTP.conn(),
          scheme: Mint.Types.scheme(),
          hostname: String.t(),
          port: :inet.port_number(),
          user: String.t(),
          password: String.t() | nil,
          session: Session.t(),
          client_opts: keyword(),
          request_opts: keyword()
        }

  @doc "The port a Trino coordinator listens on by default, per scheme."
  @spec default_port(Mint.Types.scheme()) :: :inet.port_number()
  def default_port(:http), do: 8080
  def default_port(:https), do: 8443

  @doc """
  Checks the options without opening anything.

  This is the half of `connect/1` that can only ever fail for the same reason twice — a
  missing `:username`, or a `:scheme` of `"http"` where `:http` was meant, is a mistake in
  the caller's code rather than a coordinator having a bad day — which is what lets
  `Trinox.Connection` refuse to start over one while still retrying a connection that
  merely could not be reached.

  Checking here rather than letting `Mint` or `default_port/1` fall over is the difference
  between a caller being told what is wrong and a connection process crashing on its first
  breath.
  """
  @spec validate(keyword()) :: :ok | {:error, Exception.t()}
  def validate(opts) do
    with :ok <- validate_username(opts),
         :ok <- validate_scheme(opts),
         :ok <- validate_hostname(opts) do
      validate_port(opts)
    end
  end

  @doc """
  Opens a connection to a coordinator.

  See the module documentation for the options.
  """
  @spec connect(keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def connect(opts) do
    case validate(opts) do
      :ok -> do_connect(Keyword.fetch!(opts, :username), opts)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Closes the connection."
  @spec close(t()) :: :ok
  def close(%__MODULE__{conn: conn}) do
    {:ok, _conn} = HTTP.close(conn)
    :ok
  end

  @doc "Whether the connection is still usable."
  @spec open?(t()) :: boolean()
  def open?(%__MODULE__{conn: conn}), do: HTTP.open?(conn)

  @doc """
  Checks the connection with a `GET #{@info_path}`.

  Trino keeps no server-side session for us, so this exists purely to notice a
  connection the coordinator (or something in between) has dropped. `:disconnect` means
  the connection is spent and its owner should be replaced.
  """
  @spec ping(t()) :: {:ok, t()} | {:disconnect, Exception.t(), t()}
  def ping(%__MODULE__{} = state) do
    headers = request_headers(state)

    case HTTP.request(state.conn, "GET", @info_path, headers, nil, state.request_opts) do
      {:ok, conn, %{status: status}} when status in 200..299 -> {:ok, %{state | conn: conn}}
      {:ok, conn, response} -> {:disconnect, ping_error(response), %{state | conn: conn}}
      {:error, conn, reason} -> {:disconnect, reason, %{state | conn: conn}}
    end
  end

  @doc """
  Submits `statement`, polls it to completion and decodes the result.

  The session travels with the query in both directions: the outbound headers are built
  from the connection's session, and the `X-Trino-Set-*` headers of *every* page are
  folded back into it, so a `USE` or `SET SESSION` in one query is in force for the next
  query on this connection.

  A query Trino refused is a `Trinox.Error` — Trino answers `200` with an error page
  rather than failing the request, and the connection stays perfectly usable. A broken
  conversation is different: it returns the transport or decoding error, and says
  `:disconnect` if the connection did not survive it.

  `opts` may carry `:receive_timeout` and `:poll_interval_ms`, which override the
  connection's for this one query, and `:deadline` — an absolute
  `System.monotonic_time(:millisecond)` past which the query is cancelled rather than
  polled on. A cancelled query is an `:error`, not a `:disconnect`: the point of asking
  Trino to stop is that the connection survives to serve the next caller.
  """
  @spec execute(t(), String.t(), keyword()) ::
          {:ok, Result.t(), t()} | {:error | :disconnect, Exception.t(), t()}
  def execute(%__MODULE__{} = state, statement, opts) do
    headers = request_headers(state)

    case Statement.run(state.conn, statement, headers, query_opts(state, opts)) do
      {:ok, conn, pages, response_headers} ->
        decode(pages, apply_session(state, conn, response_headers))

      {:error, conn, reason} ->
        failed(reason, %{state | conn: conn})
    end
  end

  defp validate_username(opts) do
    case Keyword.fetch(opts, :username) do
      {:ok, username} when is_binary(username) -> :ok
      {:ok, other} -> invalid(:username, other, "a string")
      :error -> {:error, %ArgumentError{message: "Trinox requires a :username option"}}
    end
  end

  defp validate_scheme(opts) do
    case Keyword.fetch(opts, :scheme) do
      {:ok, scheme} when scheme in [:http, :https] -> :ok
      {:ok, other} -> invalid(:scheme, other, "the atom :http or :https")
      :error -> :ok
    end
  end

  defp validate_hostname(opts) do
    case Keyword.fetch(opts, :hostname) do
      {:ok, hostname} when is_binary(hostname) -> validate_bare_host(hostname)
      {:ok, other} -> invalid(:hostname, other, "a string")
      :error -> :ok
    end
  end

  # `"trino.example.com:8080"` is a URL authority, not a host, and resolves to nothing —
  # so it is worth saying so rather than retrying DNS until someone reads the logs. One
  # colon followed by digits is that mistake; an IPv6 address always has at least two.
  defp validate_bare_host(hostname) do
    case String.split(hostname, ":") do
      [host, port] -> bare_host_error(hostname, host, port)
      _host_or_ipv6 -> :ok
    end
  end

  defp bare_host_error(hostname, host, port) do
    if host != "" and port != "" and match?({_integer, ""}, Integer.parse(port)) do
      {:error,
       %ArgumentError{
         message:
           "Trinox :hostname must be a host on its own, got #{inspect(hostname)}; " <>
             "pass #{inspect(host)} as :hostname and #{port} as :port"
       }}
    else
      :ok
    end
  end

  defp validate_port(opts) do
    case Keyword.fetch(opts, :port) do
      {:ok, port} when is_integer(port) and port > 0 and port < 65_536 -> :ok
      {:ok, other} -> invalid(:port, other, "a port number")
      :error -> :ok
    end
  end

  defp invalid(key, value, expected) do
    {:error,
     %ArgumentError{
       message: "Trinox #{inspect(key)} must be #{expected}, got #{inspect(value)}"
     }}
  end

  defp do_connect(username, opts) do
    scheme = Keyword.get(opts, :scheme, :https)
    hostname = Keyword.get(opts, :hostname, "localhost")
    port = Keyword.get(opts, :port, default_port(scheme))
    connect_opts = Keyword.take(opts, [:transport_opts, :connect_timeout])

    case HTTP.connect(scheme, hostname, port, connect_opts) do
      {:ok, conn} -> {:ok, build_state(conn, username, {scheme, hostname, port}, opts)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_state(conn, username, {scheme, hostname, port}, opts) do
    %__MODULE__{
      conn: conn,
      scheme: scheme,
      hostname: hostname,
      port: port,
      user: username,
      password: Keyword.get(opts, :password),
      session: %Session{
        catalog: Keyword.get(opts, :catalog),
        schema: Keyword.get(opts, :schema)
      },
      client_opts: client_opts(opts),
      request_opts: Keyword.take(opts, @query_opts)
    }
  end

  defp client_opts(opts) do
    opts
    |> Keyword.take(@client_opts)
    |> Keyword.put_new(:source, @default_source)
  end

  defp request_headers(%__MODULE__{} = state) do
    opts = [user: state.user, password: state.password] ++ state.client_opts

    Session.build_headers(state.session, opts)
  end

  # A per-query option wins over the connection's: the connection sets the house style,
  # one slow query overrides it. A deadline is never a connection-wide setting — it is
  # one caller's patience — so it is taken from the query's options alone.
  defp query_opts(%__MODULE__{} = state, opts) do
    state.request_opts
    |> Keyword.merge(Keyword.take(opts, @query_opts))
    |> Keyword.put(:deadline, Keyword.get(opts, :deadline, :infinity))
  end

  defp apply_session(%__MODULE__{} = state, conn, headers) do
    %{state | conn: conn, session: Session.apply_response_headers(state.session, headers)}
  end

  defp decode(pages, state) do
    case ResultDecoder.decode(pages) do
      {:ok, result} -> {:ok, result, state}
      {:error, error} -> {:error, Error.from_page(error, query_id(pages)), state}
    end
  end

  # An error page carries the query id like any other page, and it is the one thing that
  # lets an operator find the failure in Trino's own logs.
  defp query_id(pages), do: Enum.find_value(pages, & &1["id"])

  # A statement that broke the conversation rather than failing on its merits. If the
  # connection did not survive it, only a disconnect is honest about it; a bad status or
  # an unreadable body leaves the connection perfectly fine to reuse.
  defp failed(reason, %__MODULE__{} = state) do
    if open?(state) do
      {:error, reason, state}
    else
      {:disconnect, reason, state}
    end
  end

  defp ping_error(%{status: status}) do
    %RuntimeError{message: "Trino answered #{@info_path} with unexpected status #{status}"}
  end
end
