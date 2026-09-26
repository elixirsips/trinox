defmodule Trinox.Protocol do
  @moduledoc """
  `DBConnection` implementation for Trino.

  Trino speaks a stateless HTTP/JSON protocol rather than holding state on a socket,
  but a connection still owns something worth pooling: the open HTTP connection to the
  coordinator, and the session properties the coordinator pushes back. This module is
  where both live, exactly as `Postgrex.Protocol` owns its socket.

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
    * `:poll_interval_ms` — passed to `Trinox.Statement`; `:receive_timeout` and this
      one may also be given per query, where they override the connection's.

  Any other option is ignored here and handled by `DBConnection` itself (`:pool_size`,
  `:idle_interval`, `:name`, ...).

  Transactions and cursors are not supported; see `handle_begin/2` and `handle_declare/4`.
  """

  @behaviour DBConnection

  alias Trinox.Error
  alias Trinox.HTTP
  alias Trinox.Result
  alias Trinox.ResultDecoder
  alias Trinox.Session
  alias Trinox.Statement

  @info_path "/v1/info"

  # The options a caller may set per query as well as per connection.
  @query_opts [:receive_timeout, :poll_interval_ms]

  defstruct [
    :conn,
    :scheme,
    :hostname,
    :port,
    :user,
    :password,
    session: %Session{},
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
          request_opts: keyword()
        }

  @typedoc """
  Anything carrying the statement to run.

  `Trinox.Query` is what the public API builds; nothing here needs more than the SQL.
  """
  @type query :: %{:statement => String.t(), optional(any()) => any()}

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

  @doc """
  Hands the query back unchanged.

  Trino's REST API has no prepare step to speak of — a statement is submitted as text —
  so there is no round trip to make here.
  """
  @impl true
  @spec handle_prepare(query(), keyword(), t()) :: {:ok, query(), t()}
  def handle_prepare(query, _opts, state), do: {:ok, query, state}

  @doc """
  Submits the query's statement, polls it to completion and decodes the result.

  The session travels with the query in both directions: the outbound headers are built
  from the connection's session, and the `X-Trino-Set-*` headers of *every* page are
  folded back into it, so a `USE` or `SET SESSION` in one query is in force for the next
  query on this connection.

  A query Trino refused is a `Trinox.Error` — Trino answers `200` with an error page
  rather than failing the request, and the connection stays perfectly usable. A broken
  conversation is different: it returns the transport or decoding error, and disconnects
  if the connection did not survive, so the pool replaces it.

  `params` are ignored. Trino takes a statement as text and this milestone has no
  prepared statements, so a query carries whatever SQL it was built with.
  """
  @impl true
  @spec handle_execute(query(), term(), keyword(), t()) ::
          {:ok, query(), Result.t(), t()} | {:error | :disconnect, Exception.t(), t()}
  def handle_execute(%{statement: statement} = query, _params, opts, state) do
    headers = request_headers(state)

    case Statement.run(state.conn, statement, headers, query_opts(state, opts)) do
      {:ok, conn, pages, response_headers} ->
        decode(query, pages, apply_session(state, conn, response_headers))

      {:error, conn, reason} ->
        failed(reason, %{state | conn: conn})
    end
  end

  @doc """
  Nothing to close — `handle_prepare/3` left nothing behind on the coordinator.
  """
  @impl true
  @spec handle_close(query(), keyword(), t()) :: {:ok, nil, t()}
  def handle_close(_query, _opts, state), do: {:ok, nil, state}

  @doc """
  Transactions are not supported; these report the `:error` transaction status.

  `DBConnection` turns that into a `DBConnection.TransactionError` for the caller
  without tearing the connection down, which is the only "no" these three callbacks
  can express — unlike the others, they cannot return an exception of their own.

  Trino does support transactions for connectors that implement them, so this is a gap
  to fill rather than a rule of the protocol.
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

  @doc """
  Cursors are not supported; these three refuse with a `Trinox.Error`.

  `DBConnection.stream/4` and friends land here. Trino's `nextUri` paging is the obvious
  way to implement them later — `Trinox.Statement` already walks exactly those pages —
  but until then a stream would have to buffer the whole result, which is the opposite
  of what the caller asked for.
  """
  @impl true
  @spec handle_declare(query(), term(), keyword(), t()) :: {:error, Error.t(), t()}
  def handle_declare(_query, _params, _opts, state), do: {:error, no_cursors(), state}

  @impl true
  @spec handle_fetch(query(), term(), keyword(), t()) :: {:error, Error.t(), t()}
  def handle_fetch(_query, _cursor, _opts, state), do: {:error, no_cursors(), state}

  @impl true
  @spec handle_deallocate(query(), term(), keyword(), t()) :: {:error, Error.t(), t()}
  def handle_deallocate(_query, _cursor, _opts, state), do: {:error, no_cursors(), state}

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
      password: Keyword.get(opts, :password),
      session: %Session{
        catalog: Keyword.get(opts, :catalog),
        schema: Keyword.get(opts, :schema)
      },
      request_opts: Keyword.take(opts, @query_opts)
    }
  end

  defp request_headers(%__MODULE__{} = state) do
    Session.build_headers(state.session, user: state.user, password: state.password)
  end

  # A per-query option wins over the connection's: the connection sets the house style,
  # one slow query overrides it.
  defp query_opts(%__MODULE__{} = state, opts) do
    Keyword.merge(state.request_opts, Keyword.take(opts, @query_opts))
  end

  defp apply_session(%__MODULE__{} = state, conn, headers) do
    %{state | conn: conn, session: Session.apply_response_headers(state.session, headers)}
  end

  defp decode(query, pages, state) do
    case ResultDecoder.decode(pages) do
      {:ok, result} -> {:ok, query, result, state}
      {:error, error} -> {:error, Error.from_page(error, query_id(pages)), state}
    end
  end

  # An error page carries the query id like any other page, and it is the one thing that
  # lets an operator find the failure in Trino's own logs.
  defp query_id(pages), do: Enum.find_value(pages, & &1["id"])

  # A statement that broke the conversation rather than failing on its merits. If the
  # connection did not survive it, only a disconnect gets the pool to replace it; a bad
  # status or an unreadable body leaves the connection perfectly fine to reuse.
  defp failed(reason, %__MODULE__{} = state) do
    if HTTP.open?(state.conn) do
      {:error, reason, state}
    else
      {:disconnect, reason, state}
    end
  end

  defp no_cursors do
    %Error{message: "Trinox does not support cursors; use a query instead"}
  end

  defp ping_error(%{status: status}) do
    %RuntimeError{message: "Trino answered #{@info_path} with unexpected status #{status}"}
  end
end
