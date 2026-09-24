defmodule Trinox.HTTPTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Trinox.HTTP
  alias Trinox.MockTrino

  describe "over http" do
    setup do
      mock = MockTrino.start!()
      {:ok, conn} = HTTP.connect(:http, "127.0.0.1", mock.port)
      {:ok, mock: mock, conn: conn}
    end

    test "returns the status, headers and body of a response", %{conn: conn} do
      assert {:ok, _conn, response} = HTTP.request(conn, "GET", "/v1/info", [], nil)

      assert response.status == 200
      assert {"content-type", "application/json; charset=utf-8"} in response.headers
      assert Jason.decode!(response.body)["coordinator"] == true
    end

    test "sends request headers and the request body", %{mock: mock, conn: conn} do
      headers = [{"x-trino-user", "alice"}, {"content-type", "text/plain"}]
      statement = MockTrino.sql(:single)

      assert {:ok, _conn, response} =
               HTTP.request(conn, "POST", "/v1/statement", headers, statement)

      assert response.status == 200
      assert Jason.decode!(response.body)["nextUri"] =~ "/v1/statement/single/"

      request = MockTrino.last_request(mock)
      assert request.method == "POST"
      assert request.body == statement
      assert MockTrino.header(request, "x-trino-user") == "alice"
    end

    test "reuses the connection for further requests", %{mock: mock, conn: conn} do
      {:ok, conn, first} = HTTP.request(conn, "POST", "/v1/statement", [], MockTrino.sql(:single))
      next_uri = URI.parse(Jason.decode!(first.body)["nextUri"])

      assert {:ok, conn, second} = HTTP.request(conn, "GET", next_uri.path, [], nil)
      assert second.status == 200
      assert Jason.decode!(second.body)["data"] == [[1, "one"]]
      assert HTTP.open?(conn)

      assert [%{method: "POST"}, %{method: "GET"}] = MockTrino.requests(mock)
    end

    test "passes non-200 responses through untouched", %{conn: conn} do
      assert {:ok, _conn, response} = HTTP.request(conn, "GET", "/nope", [], nil)

      assert response.status == 404
      assert response.body == "not found"
    end

    test "returns an error when the response does not arrive within :receive_timeout", %{
      conn: conn
    } do
      assert {:error, _conn, %Mint.TransportError{reason: :timeout}} =
               HTTP.request(conn, "GET", "/v1/slow", [], nil, receive_timeout: 1)
    end

    test "returns an error when the connection is already closed", %{conn: conn} do
      {:ok, conn} = HTTP.close(conn)
      refute HTTP.open?(conn)

      assert {:error, _conn, %Mint.HTTPError{module: Mint.HTTP1, reason: :closed}} =
               HTTP.request(conn, "GET", "/v1/info", [], nil)
    end
  end

  describe "over https" do
    setup do
      mock = MockTrino.start!(MockTrino.tls_opts())
      {:ok, mock: mock}
    end

    test "verifies the server certificate against the given CA", %{mock: mock} do
      {:ok, conn} = connect_tls(mock)

      assert {:ok, conn, response} = HTTP.request(conn, "GET", "/v1/info", [], nil)
      assert response.status == 200
      assert Jason.decode!(response.body)["coordinator"] == true
      assert Mint.HTTP.protocol(conn) == :http2
    end

    test "surfaces a reset stream as an error", %{mock: mock} do
      {:ok, conn} = connect_tls(mock)
      # RFC 9113 §8.2.2: connection-specific headers are forbidden over HTTP/2, so the
      # server resets the stream instead of answering.
      headers = [{"connection", "keep-alive"}]

      capture_log(fn ->
        assert {:error, _conn, reason} = HTTP.request(conn, "GET", "/v1/info", headers, nil)
        send(self(), {:reason, reason})
      end)

      assert_received {:reason, reason}
      assert %Mint.HTTPError{module: Mint.HTTP2, reason: {:server_closed_request, _code}} = reason
    end

    @tag :capture_log
    test "rejects the fixture certificate when only the default CAs are trusted", %{mock: mock} do
      assert {:error, %Mint.TransportError{reason: {:tls_alert, {alert, _detail}}}} =
               HTTP.connect(:https, "localhost", mock.port)

      assert alert in [:unknown_ca, :bad_certificate]
    end
  end

  describe "connect/4" do
    test "fails when nothing is listening" do
      mock = MockTrino.start!()
      port = mock.port
      :ok = MockTrino.stop(mock)

      assert {:error, %Mint.TransportError{reason: :econnrefused}} =
               HTTP.connect(:http, "127.0.0.1", port, connect_timeout: 1_000)
    end

    test "leaves a caller-supplied transport timeout alone" do
      mock = MockTrino.start!()

      assert {:ok, conn} =
               HTTP.connect(:http, "127.0.0.1", mock.port,
                 connect_timeout: 1,
                 transport_opts: [timeout: 5_000]
               )

      assert HTTP.open?(conn)
    end
  end

  defp connect_tls(mock) do
    HTTP.connect(:https, "localhost", mock.port,
      transport_opts: [cacertfile: MockTrino.ca_path()]
    )
  end
end
