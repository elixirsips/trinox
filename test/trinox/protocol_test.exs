defmodule Trinox.ProtocolTest do
  use ExUnit.Case, async: true

  alias Trinox.HTTP
  alias Trinox.MockTrino
  alias Trinox.Protocol

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
      assert state.auth_header == nil
      assert state.catalog == nil
      assert state.schema == nil
      assert HTTP.open?(state.conn)
    end

    test "keeps session defaults and a Basic-auth header", %{opts: opts} do
      assert {:ok, state} =
               Protocol.connect(opts ++ [password: "secret", catalog: "tpch", schema: "sf1"])

      assert state.auth_header == "Basic " <> Base.encode64("alice:secret")
      assert state.catalog == "tpch"
      assert state.schema == "sf1"
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

  describe "callbacks that are not implemented yet" do
    setup %{opts: opts} do
      {:ok, state} = Protocol.connect(opts)
      {:ok, state: state}
    end

    test "every query and cursor callback returns a not-implemented error", %{state: state} do
      query = %{statement: "SELECT 1"}

      results = [
        Protocol.handle_prepare(query, [], state),
        Protocol.handle_execute(query, [], [], state),
        Protocol.handle_close(query, [], state),
        Protocol.handle_declare(query, [], [], state),
        Protocol.handle_fetch(query, :cursor, [], state),
        Protocol.handle_deallocate(query, :cursor, [], state)
      ]

      for result <- results do
        assert {:error, %RuntimeError{message: "not implemented"}, ^state} = result
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
  end

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
