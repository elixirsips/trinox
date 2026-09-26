defmodule Trinox.ProtocolTest do
  use ExUnit.Case, async: true

  alias Trinox.Error
  alias Trinox.HTTP
  alias Trinox.MockTrino
  alias Trinox.Protocol
  alias Trinox.Result
  alias Trinox.Session

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

  describe "open?/1" do
    test "reports whether the connection is still usable", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)

      assert Protocol.open?(state)

      {:ok, conn} = HTTP.close(state.conn)

      refute Protocol.open?(%{state | conn: conn})
    end
  end

  describe "close/1" do
    test "closes the connection", %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)

      assert :ok = Protocol.close(state)

      # Mint connections are immutable, so the struct we still hold says "open" while the
      # socket under it is gone; a request on it is what notices.
      assert {:error, _conn, %Mint.TransportError{reason: :closed}} =
               HTTP.request(state.conn, "GET", "/v1/info", [], nil)
    end
  end

  describe "execute/3" do
    setup %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, state: state}
    end

    test "runs a statement to completion and decodes the result", %{mock: mock, state: state} do
      assert {:ok, %Result{} = result, _state} =
               Protocol.execute(state, statement(:single), [])

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

      assert {:ok, %Result{}, _state} = Protocol.execute(state, statement(:multi_page), [])

      for request <- MockTrino.requests(mock) do
        assert MockTrino.header(request, "authorization") ==
                 "Basic " <> Base.encode64("alice:secret")

        assert MockTrino.header(request, "x-trino-user") == "alice"
        assert MockTrino.header(request, "x-trino-catalog") == "tpch"
        assert MockTrino.header(request, "x-trino-schema") == "sf1"
      end
    end

    test "folds the session headers of every page back into the state", %{state: state} do
      assert {:ok, %Result{}, state} = Protocol.execute(state, statement(:session), [])

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

      assert {:ok, %Result{}, state} = Protocol.execute(state, statement(:clear_session), [])

      assert state.session.properties == %{}
    end

    test "echoes the updated session on the next query", %{mock: mock, state: state} do
      {:ok, %Result{}, state} = Protocol.execute(state, statement(:session), [])
      {:ok, %Result{}, _state} = Protocol.execute(state, statement(:single), [])

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
               Protocol.execute(state, statement(:slow), receive_timeout: 1)
    end
  end

  describe "execute/3 failures" do
    setup %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, state: state}
    end

    test "returns a Trinox.Error for a query Trino refused", %{state: state} do
      assert {:error, %Error{} = error, state} =
               Protocol.execute(state, statement(:error), [])

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
               Protocol.execute(state, statement(:unavailable), [])

      assert message =~ "HTTP 503"
      assert HTTP.open?(state.conn)

      assert {:error, %Jason.DecodeError{}, state} =
               Protocol.execute(state, statement(:invalid_json), [])

      assert HTTP.open?(state.conn)
    end

    test "disconnects when the connection did not survive the query", %{state: state} do
      assert {:disconnect, %Mint.HTTPError{reason: :closed}, _state} =
               Protocol.execute(state, statement(:closing), [])
    end
  end

  defp statement(scenario), do: MockTrino.sql(scenario)

  defp opts(mock) do
    [scheme: :http, hostname: "127.0.0.1", port: mock.port, username: "alice"]
  end
end
