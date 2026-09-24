defmodule Trinox.Protocol do
  @moduledoc """
  `DBConnection` implementation for Trino.

  Trino speaks a stateless HTTP/JSON protocol rather than holding state on a socket,
  but a connection still owns something worth pooling: the open HTTP connection to the
  coordinator, and (later) the session properties the coordinator pushes back. This
  module is where both live, exactly as `Postgrex.Protocol` owns its socket.

  Connections are opened explicitly, like `Postgrex`:

      DBConnection.start_link(Trinox.Protocol,
        scheme: :http,
        hostname: "trino.example.com",
        port: 8080,
        username: "alice"
      )

  ## Options

    * `:username` — required; used as `X-Trino-User` and as the Basic-auth user.
    * `:password` — optional; when given, requests carry `Authorization: Basic ...`.
      Trino only accepts Basic auth over TLS, so this belongs with `scheme: :https`.
    * `:scheme` — `:http` or `:https` (default `:https`).
    * `:hostname` — default `"localhost"`.
    * `:port` — default `default_port/1` for the scheme.
    * `:catalog`, `:schema` — optional session defaults.
    * `:transport_opts`, `:connect_timeout`, `:receive_timeout` — passed to `Trinox.HTTP`.

  Any other option is ignored here and handled by `DBConnection` itself (`:pool_size`,
  `:idle_interval`, `:name`, ...).

  Query execution is not wired up yet: every `handle_*` callback other than
  `handle_status/2` returns a "not implemented" error for now.
  """

  @behaviour DBConnection

  alias Trinox.HTTP

  @info_path "/v1/info"

  defstruct [
    :conn,
    :scheme,
    :hostname,
    :port,
    :user,
    :auth_header,
    :catalog,
    :schema,
    request_opts: []
  ]

  @type t :: %__MODULE__{
          conn: HTTP.conn(),
          scheme: Mint.Types.scheme(),
          hostname: String.t(),
          port: :inet.port_number(),
          user: String.t(),
          auth_header: String.t() | nil,
          catalog: String.t() | nil,
          schema: String.t() | nil,
          request_opts: keyword()
        }

  @doc "The port a Trino coordinator listens on by default, per scheme."
  @spec default_port(Mint.Types.scheme()) :: :inet.port_number()
  def default_port(:http), do: 8080
  def default_port(:https), do: 8443

  @impl true
  @spec connect(keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def connect(opts) do
    case Keyword.fetch(opts, :username) do
      {:ok, username} -> do_connect(username, opts)
      :error -> {:error, %ArgumentError{message: "Trinox requires a :username option"}}
    end
  end

  @impl true
  @spec disconnect(Exception.t(), t()) :: :ok
  def disconnect(_err, %__MODULE__{conn: conn}) do
    {:ok, _conn} = HTTP.close(conn)
    :ok
  end

  @doc """
  Verifies the connection is still open before handing it to a client.

  Disconnecting on a closed connection is what makes the pool replace it.
  """
  @impl true
  @spec checkout(t()) :: {:ok, t()} | {:disconnect, Exception.t(), t()}
  def checkout(%__MODULE__{conn: conn} = state) do
    if HTTP.open?(conn) do
      {:ok, state}
    else
      {:disconnect, %RuntimeError{message: "connection to Trino is closed"}, state}
    end
  end

  @doc """
  Checks an idle connection with a `GET #{@info_path}`.

  Trino keeps no server-side session for us, so this exists purely to notice a
  connection the coordinator (or something in between) has dropped.
  """
  @impl true
  @spec ping(t()) :: {:ok, t()} | {:disconnect, Exception.t(), t()}
  def ping(%__MODULE__{} = state) do
    headers = request_headers(state)

    case HTTP.request(state.conn, "GET", @info_path, headers, nil, state.request_opts) do
      {:ok, conn, %{status: status}} when status in 200..299 -> {:ok, %{state | conn: conn}}
      {:ok, conn, response} -> {:disconnect, ping_error(response), %{state | conn: conn}}
      {:error, conn, reason} -> {:disconnect, reason, %{state | conn: conn}}
    end
  end

  @impl true
  def handle_prepare(_query, _opts, state), do: {:error, not_implemented(), state}

  @impl true
  def handle_execute(_query, _params, _opts, state), do: {:error, not_implemented(), state}

  @impl true
  def handle_close(_query, _opts, state), do: {:error, not_implemented(), state}

  @doc """
  Transactions are not supported; these report the `:error` transaction status.

  `DBConnection` turns that into a `DBConnection.TransactionError` for the caller
  without tearing the connection down, which is the only "no" these three callbacks
  can express — unlike the others, they cannot return an exception of their own.
  """
  @impl true
  @spec handle_begin(keyword(), t()) :: {:error, t()}
  def handle_begin(_opts, state), do: {:error, state}

  @impl true
  @spec handle_commit(keyword(), t()) :: {:error, t()}
  def handle_commit(_opts, state), do: {:error, state}

  @impl true
  @spec handle_rollback(keyword(), t()) :: {:error, t()}
  def handle_rollback(_opts, state), do: {:error, state}

  @impl true
  def handle_declare(_query, _params, _opts, state), do: {:error, not_implemented(), state}

  @impl true
  def handle_fetch(_query, _cursor, _opts, state), do: {:error, not_implemented(), state}

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state), do: {:error, not_implemented(), state}

  @doc """
  Always `:idle` — Trino transactions are out of scope for this milestone.
  """
  @impl true
  @spec handle_status(keyword(), t()) :: {:idle, t()}
  def handle_status(_opts, state), do: {:idle, state}

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
      auth_header: auth_header(username, Keyword.get(opts, :password)),
      catalog: Keyword.get(opts, :catalog),
      schema: Keyword.get(opts, :schema),
      request_opts: Keyword.take(opts, [:receive_timeout])
    }
  end

  defp auth_header(_username, nil), do: nil
  defp auth_header(username, password), do: "Basic " <> Base.encode64("#{username}:#{password}")

  # Provisional: `Trinox.Session` takes over header building in a later issue.
  defp request_headers(%__MODULE__{auth_header: nil} = state), do: [{"x-trino-user", state.user}]

  defp request_headers(%__MODULE__{} = state) do
    [{"authorization", state.auth_header}, {"x-trino-user", state.user}]
  end

  defp ping_error(%{status: status}) do
    %RuntimeError{message: "Trino answered #{@info_path} with unexpected status #{status}"}
  end

  defp not_implemented do
    %RuntimeError{message: "not implemented"}
  end
end
