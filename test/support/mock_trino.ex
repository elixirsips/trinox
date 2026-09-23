defmodule Trinox.MockTrino do
  @moduledoc """
  An in-process mock Trino coordinator, served by `Bandit` on an ephemeral port.

  It speaks enough of Trino's REST protocol for the driver's tests to run without a
  live cluster: `POST /v1/statement` answers with a queued first page carrying a
  `nextUri`, and `GET`ting that `nextUri` walks the query to a terminal page.

  Every request it receives is recorded (method, path, headers, body) so tests can
  assert on the headers the driver builds — Basic auth, `X-Trino-User`,
  `X-Trino-Catalog`/`X-Trino-Schema` and `X-Trino-Session`.

  ## Scenarios

  The statement text selects the scenario; `sql/1` returns the statement for each.
  Any other statement behaves like `:single`.

    * `:single` — queued page, then one terminal page with columns and one row.
    * `:immediate` — the `POST` response is already terminal (no `nextUri`).
    * `:multi_page` — three polled pages; only the first carries `columns`.
    * `:error` — the polled page carries an `error` object instead of data.
    * `:session` — the `POST` response sets catalog, schema and a session property;
      the terminal page sets one more, so tests can check that *every* page is applied.
    * `:clear_session` — the terminal page clears a session property.

  ## Example

      mock = Trinox.MockTrino.start!()
      url = Trinox.MockTrino.base_url(mock) <> "/v1/statement"
      # ... POST Trinox.MockTrino.sql(:multi_page) to `url` ...
      assert Trinox.MockTrino.header(Trinox.MockTrino.last_request(mock), "x-trino-user") == "alice"
  """

  defstruct [:server, :recorder, :port, :scheme]

  @type t :: %__MODULE__{
          server: pid(),
          recorder: pid(),
          port: :inet.port_number(),
          scheme: :http | :https
        }

  @type request :: %{
          method: String.t(),
          path: String.t(),
          headers: [{String.t(), String.t()}],
          body: binary()
        }

  @query_id "20260923_120000_00000_mock"

  @scenarios %{
    "single" => "SELECT 1",
    "immediate" => "SELECT 'immediate'",
    "multi_page" => "SELECT * FROM multi_page",
    "error" => "SELECT * FROM boom",
    "session" => "SET SESSION mock_scenario = 'session'",
    "clear_session" => "RESET SESSION mock_scenario"
  }

  @doc """
  Starts the mock and links it to the calling process.

  Options: `:scheme` (`:http`, the default, or `:https`), plus `:certfile` and
  `:keyfile` which are passed through to `Bandit` for `:https`.
  """
  @spec start!(keyword()) :: t()
  def start!(opts \\ []) do
    {:ok, recorder} = Agent.start_link(fn -> [] end)

    bandit_opts =
      Keyword.merge(
        [
          plug: {__MODULE__.Router, recorder: recorder},
          scheme: :http,
          port: 0,
          startup_log: false
        ],
        Keyword.take(opts, [:scheme, :certfile, :keyfile])
      )

    {:ok, server} = Bandit.start_link(bandit_opts)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    %__MODULE__{server: server, recorder: recorder, port: port, scheme: bandit_opts[:scheme]}
  end

  @doc "Stops the mock and its request recorder."
  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = mock) do
    :ok = Supervisor.stop(mock.server)
    :ok = Agent.stop(mock.recorder)
  end

  @doc "The base URL the mock is listening on, e.g. `\"http://127.0.0.1:52341\"`."
  @spec base_url(t()) :: String.t()
  def base_url(%__MODULE__{scheme: scheme, port: port}), do: "#{scheme}://127.0.0.1:#{port}"

  @doc "Every request the mock has received, oldest first."
  @spec requests(t()) :: [request()]
  def requests(%__MODULE__{recorder: recorder}), do: Agent.get(recorder, &Enum.reverse/1)

  @doc "The most recent request the mock has received, or `nil`."
  @spec last_request(t()) :: request() | nil
  def last_request(%__MODULE__{} = mock) do
    mock |> requests() |> List.last()
  end

  @doc "Fetches one (lowercase) header from a recorded request, or `nil`."
  @spec header(request(), String.t()) :: String.t() | nil
  def header(%{headers: headers}, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end

  @doc "The statement text that selects the given scenario."
  @spec sql(atom()) :: String.t()
  def sql(scenario), do: Map.fetch!(@scenarios, Atom.to_string(scenario))

  @doc "The query id the mock reports for every query."
  @spec query_id() :: String.t()
  def query_id, do: @query_id

  @doc false
  @spec scenario_for(binary()) :: String.t()
  def scenario_for(statement) do
    Enum.find_value(@scenarios, "single", fn {name, sql} -> sql == statement && name end)
  end

  defmodule Router do
    @moduledoc """
    The `Plug` router backing `Trinox.MockTrino`.

    Expects a `:recorder` (an `Agent` pid) in its init options, which it appends
    each received request to.
    """

    # `:copy_opts_to_assign` puts this router's init options (the `:recorder`) in `conn.assigns`.
    use Plug.Router, copy_opts_to_assign: :mock_trino

    @set_session_headers [
      {"x-trino-set-catalog", "memory"},
      {"x-trino-set-schema", "default"},
      {"x-trino-set-session", "mock_scenario=session"}
    ]

    plug(:match)
    plug(:dispatch)

    post "/v1/statement" do
      {:ok, body, conn} = read_body(conn)

      conn
      |> record(body)
      |> respond(Trinox.MockTrino.scenario_for(body), 0)
    end

    get "/v1/statement/:scenario/:query_id/:token" do
      _ = query_id

      conn
      |> record("")
      |> respond(scenario, String.to_integer(token))
    end

    get "/v1/info" do
      conn
      |> record("")
      |> json(%{
        "coordinator" => true,
        "starting" => false,
        "nodeVersion" => %{"version" => "mock"}
      })
    end

    match _ do
      conn
      |> record("")
      |> send_resp(404, "not found")
    end

    defp record(conn, body) do
      request = %{
        method: conn.method,
        path: conn.request_path,
        headers: conn.req_headers,
        body: body
      }

      Agent.update(Keyword.fetch!(conn.assigns.mock_trino, :recorder), &[request | &1])
      conn
    end

    defp respond(conn, scenario, token) do
      %{body: body, headers: headers} = page = page(scenario, token)
      query_id = Trinox.MockTrino.query_id()

      body =
        %{"id" => query_id, "infoUri" => "#{base(conn)}/ui/query.html?#{query_id}"}
        |> Map.merge(body)
        |> maybe_put_next_uri(page, conn, scenario, token)

      conn
      |> put_headers(headers)
      |> json(body)
    end

    # Token 0 is the POST /v1/statement response.
    defp page("immediate", 0), do: done(%{"columns" => columns(), "data" => [[1, "one"]]})
    defp page("session", 0), do: with_headers(queued(), @set_session_headers)
    defp page(_scenario, 0), do: queued()

    defp page("multi_page", 1), do: more(%{"columns" => columns(), "data" => [[1, "one"]]})
    defp page("multi_page", 2), do: more(%{"data" => [[2, "two"]]})
    defp page("multi_page", 3), do: done(%{"data" => [[3, "three"]]})

    defp page("error", 1), do: failed(%{"error" => error()})

    defp page("session", 1) do
      %{"columns" => columns(), "data" => [[1, "one"]]}
      |> done()
      |> with_headers([{"x-trino-set-session", "mock_page_prop=2"}])
    end

    defp page("clear_session", 1) do
      %{"columns" => columns(), "data" => [[1, "one"]]}
      |> done()
      |> with_headers([{"x-trino-clear-session", "mock_scenario"}])
    end

    defp page(_scenario, 1), do: done(%{"columns" => columns(), "data" => [[1, "one"]]})

    defp queued, do: %{body: %{"stats" => stats("QUEUED")}, headers: [], next?: true}

    defp more(body),
      do: %{body: Map.put(body, "stats", stats("RUNNING")), headers: [], next?: true}

    defp done(body),
      do: %{body: Map.put(body, "stats", stats("FINISHED")), headers: [], next?: false}

    defp failed(body),
      do: %{body: Map.put(body, "stats", stats("FAILED")), headers: [], next?: false}

    defp with_headers(page, headers), do: %{page | headers: headers}

    defp maybe_put_next_uri(body, %{next?: false}, _conn, _scenario, _token), do: body

    defp maybe_put_next_uri(body, %{next?: true}, conn, scenario, token) do
      next = "#{base(conn)}/v1/statement/#{scenario}/#{Trinox.MockTrino.query_id()}/#{token + 1}"
      Map.put(body, "nextUri", next)
    end

    defp base(conn), do: "#{conn.scheme}://#{conn.host}:#{conn.port}"

    defp columns do
      [
        %{
          "name" => "id",
          "type" => "bigint",
          "typeSignature" => %{"rawType" => "bigint", "arguments" => []}
        },
        %{
          "name" => "name",
          "type" => "varchar(5)",
          "typeSignature" => %{"rawType" => "varchar", "arguments" => []}
        }
      ]
    end

    defp stats(state) do
      %{
        "state" => state,
        "queued" => state == "QUEUED",
        "scheduled" => state != "QUEUED",
        "nodes" => 1
      }
    end

    defp error do
      %{
        "message" => "line 1:15: Table 'mock.default.boom' does not exist",
        "errorCode" => 44,
        "errorName" => "TABLE_NOT_FOUND",
        "errorType" => "USER_ERROR"
      }
    end

    defp put_headers(conn, headers) do
      Enum.reduce(headers, conn, fn {name, value}, acc -> put_resp_header(acc, name, value) end)
    end

    defp json(conn, body) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode!(body))
    end
  end
end
