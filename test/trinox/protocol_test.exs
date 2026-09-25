defmodule Trinox.ProtocolTest do
  use ExUnit.Case, async: true

  alias Trinox.Error
  alias Trinox.HTTP
  alias Trinox.MockTrino
  alias Trinox.Protocol
  alias Trinox.Result
  alias Trinox.Session
  alias Trinox.TestQuery

  setup do
    mock = MockTrino.start!()
    {:ok, mock: mock, opts: opts(mock)}
  end

  describe "connect/1" do
    test "opens a connection and keeps the resolved settings", %{mock: mock, opts: opts} do
      assert {:ok, state} = Protocol.connect(opts)

      assert state.scheme == :http
      assert state.hostname == "127.0.0.1"
      assert state.port == mock.port
      assert state.user == "alice"
      assert state.password == nil
      assert state.session == %Session{}
      assert HTTP.open?(state.conn)
    end

    test "keeps the password and the session defaults", %{opts: opts} do
      assert {:ok, state} =
               Protocol.connect(opts ++ [password: "secret", catalog: "tpch", schema: "sf1"])

      assert state.password == "secret"
      assert state.session == %Session{catalog: "tpch", schema: "sf1"}
    end

    test "keeps the request options a query may override", %{opts: opts} do
      assert {:ok, state} =
               Protocol.connect(opts ++ [receive_timeout: 5_000, poll_interval_ms: 20])

      assert Enum.sort(state.request_opts) == [poll_interval_ms: 20, receive_timeout: 5_000]
    end

    test "requires a username", %{opts: opts} do
      assert {:error, %ArgumentError{message: message}} =
               Protocol.connect(Keyword.delete(opts, :username))

      assert message =~ ":username"
    end

    test "returns the transport error when the coordinator is unreachable", %{mock: mock} do
      port = mock.port
      :ok = MockTrino.stop(mock)

      assert {:error, %Mint.TransportError{reason: :econnrefused}} =
               Protocol.connect(
                 scheme: :http,
                 hostname: "127.0.0.1",
                 port: port,
                 username: "alice"
               )
    end

    test "defaults to https, and passes :transport_opts through" do
      mock = MockTrino.start!(MockTrino.tls_opts())

      assert {:ok, state} =
               Protocol.connect(
                 hostname: "localhost",
                 port: mock.port,
                 username: "alice",
                 transport_opts: [cacertfile: MockTrino.ca_path()]
               )

      assert state.scheme == :https
      assert HTTP.open?(state.conn)
    end
  end

  describe "default_port/1" do
    test "matches Trino's defaults" do
      assert Protocol.default_port(:http) == 8080
      assert Protocol.default_port(:https) == 8443
    end
  end

  describe "ping/1" do
    test "asks the coordinator for /v1/info", %{mock: mock, opts: opts} do
      {:ok, state} = Protocol.connect(opts)

      assert {:ok, state} = Protocol.ping(state)
      assert {:ok, _state} = Protocol.ping(state)

      assert [first, second] = MockTrino.requests(mock)
      assert first.path == "/v1/info"
      assert second.path == "/v1/info"
      assert MockTrino.header(first, "x-trino-user") == "alice"
      assert MockTrino.header(first, "authorization") == nil
    end

    test "sends the Basic-auth header when a password is configured", %{mock: mock, opts: opts} do
      {:ok, state} = Protocol.connect(opts ++ [password: "secret"])

      assert {:ok, _state} = Protocol.ping(state)

      request = MockTrino.last_request(mock)

      assert MockTrino.header(request, "authorization") ==
               "Basic " <> Base.encode64("alice:secret")

      assert MockTrino.header(request, "x-trino-user") == "alice"
    end

    test "disconnects when the coordinator reports an unhealthy status" do
      mock = MockTrino.start!(info_status: 503)
      {:ok, state} = Protocol.connect(opts(mock))

      assert {:disconnect, %RuntimeError{message: message}, _state} = Protocol.ping(state)
      assert message =~ "503"
    end

    test "disconnects when the connection has gone away", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, conn} = HTTP.close(state.conn)

      assert {:disconnect, %Mint.HTTPError{reason: :closed}, _state} =
               Protocol.ping(%{state | conn: conn})
    end
  end

  describe "checkout/1" do
    test "hands over an open connection", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)

      assert {:ok, ^state} = Protocol.checkout(state)
    end

    test "disconnects a closed connection so the pool replaces it", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, conn} = HTTP.close(state.conn)

      assert {:disconnect, %RuntimeError{message: message}, _state} =
               Protocol.checkout(%{state | conn: conn})

      assert message =~ "closed"
    end
  end

  describe "disconnect/2" do
    test "closes the connection", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)

      assert :ok = Protocol.disconnect(%RuntimeError{message: "stopping"}, state)
    end
  end

  describe "handle_execute/4" do
    setup %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, state: state}
    end

    test "runs a statement to completion and decodes the result", %{mock: mock, state: state} do
      query = query(:single)

      assert {:ok, ^query, %Result{} = result, _state} =
               Protocol.handle_execute(query, [], [], state)

      assert result.columns == ["id", "name"]
      assert result.rows == [[1, "one"]]
      assert result.num_rows == 1
      assert result.query_id == MockTrino.query_id()
      assert result.stats["state"] == "FINISHED"

      assert [%{method: "POST", body: body}, %{method: "GET"}] = MockTrino.requests(mock)
      assert body == MockTrino.sql(:single)
    end

    test "sends the session as headers on every request", %{mock: mock, opts: opts} do
      {:ok, state} =
        Protocol.connect(opts ++ [password: "secret", catalog: "tpch", schema: "sf1"])

      assert {:ok, _query, %Result{}, _state} =
               Protocol.handle_execute(query(:multi_page), [], [], state)

      for request <- MockTrino.requests(mock) do
        assert MockTrino.header(request, "authorization") ==
                 "Basic " <> Base.encode64("alice:secret")

        assert MockTrino.header(request, "x-trino-user") == "alice"
        assert MockTrino.header(request, "x-trino-catalog") == "tpch"
        assert MockTrino.header(request, "x-trino-schema") == "sf1"
      end
    end

    test "folds the session headers of every page back into the state", %{state: state} do
      assert {:ok, _query, %Result{}, state} =
               Protocol.handle_execute(query(:session), [], [], state)

      # The catalog, schema and first property come from the POST response; the second
      # property only from the terminal page.
      assert state.session == %Session{
               catalog: "memory",
               schema: "default",
               properties: %{"mock_scenario" => "session", "mock_page_prop" => "2"}
             }
    end

    test "applies a session property the coordinator cleared", %{state: state} do
      state = %{state | session: %Session{properties: %{"mock_scenario" => "session"}}}

      assert {:ok, _query, %Result{}, state} =
               Protocol.handle_execute(query(:clear_session), [], [], state)

      assert state.session.properties == %{}
    end

    test "echoes the updated session on the next query", %{mock: mock, state: state} do
      {:ok, _query, %Result{}, state} = Protocol.handle_execute(query(:session), [], [], state)
      {:ok, _query, %Result{}, _state} = Protocol.handle_execute(query(:single), [], [], state)

      request = MockTrino.last_request(mock)

      assert MockTrino.header(request, "x-trino-catalog") == "memory"
      assert MockTrino.header(request, "x-trino-schema") == "default"

      assert MockTrino.header(request, "x-trino-session") ==
               "mock_page_prop=2,mock_scenario=session"
    end

    test "lets a per-query option override the connection's", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts ++ [receive_timeout: 5_000])

      # The connection would have waited five seconds; the query gives up after one
      # millisecond, and a half-read response leaves nothing to reuse the socket for.
      assert {:disconnect, %Mint.TransportError{reason: :timeout}, _state} =
               Protocol.handle_execute(query(:slow), [], [receive_timeout: 1], state)
    end
  end

  describe "handle_execute/4 failures" do
    setup %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, state: state}
    end

    test "returns a Trinox.Error for a query Trino refused", %{state: state} do
      assert {:error, %Error{} = error, state} =
               Protocol.handle_execute(query(:error), [], [], state)

      assert error.message =~ "Table 'mock.default.boom' does not exist"
      assert error.error_code == 44
      assert error.error_name == "TABLE_NOT_FOUND"
      assert error.error_type == "USER_ERROR"
      assert error.query_id == MockTrino.query_id()

      # A refused query is Trino's answer, not a broken connection.
      assert HTTP.open?(state.conn)
    end

    test "keeps the connection when the coordinator answers badly", %{state: state} do
      assert {:error, %RuntimeError{message: message}, state} =
               Protocol.handle_execute(query(:unavailable), [], [], state)

      assert message =~ "HTTP 503"
      assert HTTP.open?(state.conn)

      assert {:error, %Jason.DecodeError{}, state} =
               Protocol.handle_execute(query(:invalid_json), [], [], state)

      assert HTTP.open?(state.conn)
    end

    test "disconnects when the connection did not survive the query", %{state: state} do
      assert {:disconnect, %Mint.HTTPError{reason: :closed}, _state} =
               Protocol.handle_execute(query(:closing), [], [], state)
    end
  end

  describe "callbacks with nothing to do" do
    setup %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, state: state}
    end

    test "handle_prepare/3 hands the query straight back", %{state: state} do
      query = query(:single)

      assert {:ok, ^query, ^state} = Protocol.handle_prepare(query, [], state)
    end

    test "handle_close/3 has nothing to close", %{state: state} do
      assert {:ok, nil, ^state} = Protocol.handle_close(query(:single), [], state)
    end

    test "the cursor callbacks refuse with a Trinox.Error", %{state: state} do
      query = query(:single)

      results = [
        Protocol.handle_declare(query, [], [], state),
        Protocol.handle_fetch(query, :cursor, [], state),
        Protocol.handle_deallocate(query, :cursor, [], state)
      ]

      for result <- results do
        assert {:error, %Error{message: message}, ^state} = result
        assert message =~ "cursors"
      end
    end

    test "the transaction callbacks report the :error status", %{state: state} do
      assert {:error, ^state} = Protocol.handle_begin([], state)
      assert {:error, ^state} = Protocol.handle_commit([], state)
      assert {:error, ^state} = Protocol.handle_rollback([], state)
    end

    test "handle_status/2 always reports :idle", %{state: state} do
      assert {:idle, ^state} = Protocol.handle_status([], state)
    end
  end

  describe "under DBConnection" do
    test "starts a pool and pings the coordinator while idle", %{mock: mock, opts: opts} do
      {:ok, pool} =
        DBConnection.start_link(Protocol, opts ++ [pool_size: 1, idle_interval: 10])

      assert is_pid(pool)
      assert eventually(fn -> Enum.any?(MockTrino.requests(mock), &(&1.path == "/v1/info")) end)

      # Stop pinging before the mock goes away with the test process.
      :ok = GenServer.stop(pool)
    end

    test "executes a query and returns a result", %{mock: mock, opts: opts} do
      {:ok, pool} = DBConnection.start_link(Protocol, opts ++ [pool_size: 1])

      assert {:ok, _query, %Result{} = result} =
               DBConnection.execute(pool, query(:single), [])

      assert result.columns == ["id", "name"]
      assert result.rows == [[1, "one"]]
      assert [%{method: "POST"}, %{method: "GET"}] = MockTrino.requests(mock)

      :ok = GenServer.stop(pool)
    end

    test "prepares and executes in one call", %{mock: mock, opts: opts} do
      {:ok, pool} = DBConnection.start_link(Protocol, opts ++ [pool_size: 1])

      assert {:ok, _query, %Result{rows: [[1, "one"]]}} =
               DBConnection.prepare_execute(pool, query(:single), [])

      # Preparing costs nothing: the POST is still the first request the mock sees.
      assert [%{method: "POST"} | _polls] = MockTrino.requests(mock)

      :ok = GenServer.stop(pool)
    end

    test "returns a Trinox.Error for a query Trino refused", %{opts: opts} do
      {:ok, pool} = DBConnection.start_link(Protocol, opts ++ [pool_size: 1])

      assert {:error, %Error{error_name: "TABLE_NOT_FOUND"}} =
               DBConnection.execute(pool, query(:error), [])

      :ok = GenServer.stop(pool)
    end

    test "reports that transactions are not supported", %{opts: opts} do
      {:ok, pool} = DBConnection.start_link(Protocol, opts ++ [pool_size: 1])

      # handle_begin/2's :error status reaches the caller as a rolled-back transaction.
      assert {:error, :rollback} =
               DBConnection.transaction(pool, fn _conn -> :unreachable end)

      :ok = GenServer.stop(pool)
    end
  end

  defp query(scenario), do: %TestQuery{statement: MockTrino.sql(scenario)}

  defp opts(mock) do
    [scheme: :http, hostname: "127.0.0.1", port: mock.port, username: "alice"]
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(10) || eventually(fun, attempts - 1)
    end
  end
end
