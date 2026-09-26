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
    * `:unavailable` — the `POST` is answered with `503`, as a busy coordinator does.
    * `:bad_request` — the `POST` is answered with `400`, which no retry can help with.
    * `:invalid_json` — the `POST` is answered with `200` and a broken JSON body.
    * `:slow` — the `POST` is answered only after `slow_delay/0`, then behaves like
      `:single`.
    * `:closing` — the `POST` is answered with `Connection: close`, so the connection is
      gone by the time the poll is sent and the poll fails outright.
    * `:endless` — every page carries a `nextUri`, so the query never reaches a terminal
      page. This is the query that only a deadline can end.
    * `:flaky` — the `POST` is answered `503` once and succeeds on the next attempt.
    * `:flaky_poll` — the same, but on a poll rather than on the `POST`.
    * `:update` — a statement that changed something: `updateType` and `updateCount`
      instead of `columns` and `data`.
    * `:session_extras` — the terminal page sets a SQL path and adds a prepared statement.
    * `:deallocate` — the terminal page deallocates that prepared statement.
    * `:begin`, `:commit`, `:rollback` — the statements `Trinox.transaction/3` sends, which
      start a transaction (`X-Trino-Started-Transaction-Id`) and end one
      (`X-Trino-Clear-Transaction-Id`).

  `DELETE`ing a `nextUri` is how a client cancels a running query, and the mock answers
  `204` to one like a coordinator does. The `DELETE` is recorded like any other request,
  so a test can assert that an abandoned query was actually cancelled.

  `:multi_page` hands back a `nextUri` carrying a query string, since a `nextUri` is
  opaque and has to be followed exactly as given.

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
  @transaction_id "8a6a0e1a-mock-4d1e-9c1f-000000000001"

  @scenarios %{
    "single" => "SELECT 1",
    "immediate" => "SELECT 'immediate'",
    "multi_page" => "SELECT * FROM multi_page",
    "error" => "SELECT * FROM boom",
    "session" => "SET SESSION mock_scenario = 'session'",
    "clear_session" => "RESET SESSION mock_scenario",
    "unavailable" => "SELECT * FROM unavailable",
    "bad_request" => "SELECT * FROM bad_request",
    "invalid_json" => "SELECT * FROM garbage",
    "slow" => "SELECT * FROM slow",
    "closing" => "SELECT * FROM closing",
    "endless" => "SELECT * FROM endless",
    "flaky" => "SELECT * FROM flaky",
    "flaky_poll" => "SELECT * FROM flaky_poll",
    "update" => "INSERT INTO mock VALUES (1)",
    "session_extras" => "SET PATH mock.default",
    "deallocate" => "DEALLOCATE PREPARE mock_statement",
    "begin" => "START TRANSACTION",
    "commit" => "COMMIT",
    "rollback" => "ROLLBACK"
  }

  @doc """
  Starts the mock and links it to the calling process.

  Options:

    * `:scheme` — `:http` (the default) or `:https`, with `:certfile`/`:keyfile`
      passed through to `Bandit`.
    * `:info_status` — the status `GET /v1/info` answers with (default `200`), for
      simulating a coordinator that is unreachable or still starting.
    * `:port` — the port to listen on (default `0`, an ephemeral one). Pin it to bring a
      coordinator back up where one was before.
    * `:fail` — scenarios whose `POST` is answered `503` rather than normally, for the
      statements a test cannot choose the text of (`:begin`, `:commit`, `:rollback`).
  """
  @spec start!(keyword()) :: t()
  def start!(opts \\ []) do
    {:ok, recorder} = Agent.start_link(fn -> [] end)

    plug_opts = Keyword.merge([recorder: recorder], Keyword.take(opts, [:info_status, :fail]))

    bandit_opts =
      Keyword.merge(
        [
          plug: {__MODULE__.Router, plug_opts},
          scheme: :http,
          port: 0,
          startup_log: false,
          # One acceptor, not ThousandIsland's default hundred. A mock serves one client's
          # requests one after another, so the other ninety-nine do nothing but spawn and
          # then, when the mock goes down with its test, race the server they report to and
          # log a crash each. A hundred of those per mock buries a CI log.
          thousand_island_options: [num_acceptors: 1]
        ],
        Keyword.take(opts, [:scheme, :certfile, :keyfile, :port])
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

  @doc """
  `start!/1` options for serving the mock over TLS with the checked-in test certificate.

  The certificate is signed by a throwaway CA, so clients need `ca_path/0` as their
  `:cacertfile`. It is valid for both `localhost` and `127.0.0.1`.
  """
  @spec tls_opts() :: keyword()
  def tls_opts, do: [scheme: :https, certfile: cert_path(), keyfile: key_path()]

  @doc "Path to the CA that signed `cert_path/0`, for use as a client `:cacertfile`."
  @spec ca_path() :: String.t()
  def ca_path, do: Path.join(__DIR__, "tls/ca.pem")

  @doc "Path to the certificate the mock serves over `:https`."
  @spec cert_path() :: String.t()
  def cert_path, do: Path.join(__DIR__, "tls/cert.pem")

  @doc "Path to the private key for `cert_path/0`."
  @spec key_path() :: String.t()
  def key_path, do: Path.join(__DIR__, "tls/key.pem")

  @doc "How long `GET /v1/slow` waits before responding, in milliseconds."
  @spec slow_delay() :: pos_integer()
  def slow_delay, do: 100

  @doc "The query id the mock reports for every query."
  @spec query_id() :: String.t()
  def query_id, do: @query_id

  @doc "The transaction id the mock starts every transaction with."
  @spec transaction_id() :: String.t()
  def transaction_id, do: @transaction_id

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

    # A prepared statement travels URL-encoded, exactly like a session property value.
    @session_extra_headers [
      {"x-trino-set-path", "mock.default"},
      {"x-trino-added-prepare", "mock_statement=SELECT+%3F"}
    ]

    plug(:match)
    plug(:dispatch)

    post "/v1/statement" do
      {:ok, body, conn} = read_body(conn)

      conn
      |> record(body)
      |> answer(Trinox.MockTrino.scenario_for(body))
    end

    get "/v1/statement/:scenario/:query_id/:token" do
      _ = query_id

      conn
      |> record("")
      |> respond(scenario, String.to_integer(token))
    end

    delete "/v1/statement/*_next_uri" do
      conn
      |> record("")
      |> send_resp(204, "")
    end

    get "/v1/info" do
      info = %{
        "coordinator" => true,
        "starting" => false,
        "nodeVersion" => %{"version" => "mock"}
      }

      conn
      |> record("")
      |> json(info, Keyword.get(conn.assigns.mock_trino, :info_status, 200))
    end

    # Responds only after `Trinox.MockTrino.slow_delay/0`, for receive-timeout tests.
    get "/v1/slow" do
      # Record first: a client that gives up on the delay takes the mock down with it.
      conn = record(conn, "")
      Process.sleep(Trinox.MockTrino.slow_delay())
      json(conn, %{"slow" => true})
    end

    match _ do
      conn
      |> record("")
      |> send_resp(404, "not found")
    end

    defp seen(conn, matcher) do
      recorder = Keyword.fetch!(conn.assigns.mock_trino, :recorder)

      Agent.get(recorder, fn requests -> Enum.count(requests, matcher) end)
    end

    defp record(conn, body) do
      request = %{
        method: conn.method,
        path: conn.request_path,
        headers: conn.req_headers,
        body: body
      }

      recorder = Keyword.fetch!(conn.assigns.mock_trino, :recorder)

      # A cancelled or slow request can still be in flight when the test that started the
      # mock ends, and the recorder goes down with it. There is nobody left to record for.
      if Process.alive?(recorder), do: Agent.update(recorder, &[request | &1])

      conn
    end

    # A scenario the test asked to fail is answered `503` whatever it would have said.
    defp answer(conn, scenario) do
      failing =
        conn.assigns.mock_trino
        |> Keyword.get(:fail, [])
        |> Enum.map(&to_string/1)

      if scenario in failing do
        send_resp(conn, 503, "Service Unavailable")
      else
        submit(conn, scenario)
      end
    end

    defp submit(conn, "unavailable"), do: send_resp(conn, 503, "Service Unavailable")
    defp submit(conn, "bad_request"), do: send_resp(conn, 400, "Bad Request")

    defp submit(conn, "flaky") do
      sql = Trinox.MockTrino.sql(:flaky)

      # The current request is already recorded, so a second sighting is the retry.
      if seen(conn, &(&1.body == sql)) > 1 do
        respond(conn, "single", 0)
      else
        send_resp(conn, 503, "Service Unavailable")
      end
    end

    defp submit(conn, "invalid_json") do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, "{\"id\": truncated")
    end

    defp submit(conn, "slow") do
      Process.sleep(Trinox.MockTrino.slow_delay())
      respond(conn, "single", 0)
    end

    defp submit(conn, scenario), do: respond(conn, scenario, 0)

    # Only the polls are flaky; token 0 is the POST, which must queue normally for there to
    # be a poll at all.
    defp respond(conn, "flaky_poll", token) when token > 0 do
      path = conn.request_path

      if seen(conn, &(&1.method == "GET" and &1.path == path)) > 1 do
        respond(conn, "single", token)
      else
        send_resp(conn, 503, "Service Unavailable")
      end
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

    # A nextUri the client can never follow: Bandit closes the connection after this
    # response, so the poll fails on a dead socket instead of on a stopwatch.
    defp page("closing", 0), do: with_headers(queued(), [{"connection", "close"}])
    # No token ever terminates: the only way out of this query is a deadline.
    defp page("endless", _token), do: more(%{"columns" => columns(), "data" => []})

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

    defp page("update", 1) do
      done(%{"updateType" => "INSERT", "updateCount" => 42})
    end

    defp page("session_extras", 1) do
      %{"columns" => columns(), "data" => [[1, "one"]]}
      |> done()
      |> with_headers(@session_extra_headers)
    end

    defp page("deallocate", 1) do
      %{"columns" => columns(), "data" => [[1, "one"]]}
      |> done()
      |> with_headers([{"x-trino-deallocated-prepare", "mock_statement"}])
    end

    defp page("begin", 1) do
      %{}
      |> done()
      |> with_headers([{"x-trino-started-transaction-id", Trinox.MockTrino.transaction_id()}])
    end

    defp page(scenario, 1) when scenario in ~w(commit rollback) do
      %{}
      |> done()
      |> with_headers([{"x-trino-clear-transaction-id", "true"}])
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
      # A nextUri is opaque: multi_page hangs a query string off it to prove clients
      # follow it verbatim instead of rebuilding the path.
      next = if scenario == "multi_page", do: next <> "?slug=mock", else: next
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

    defp json(conn, body, status \\ 200) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(status, Jason.encode!(body))
    end
  end
end
