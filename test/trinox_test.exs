defmodule TrinoxTest do
  use ExUnit.Case, async: true

  alias Trinox.Error
  alias Trinox.MockTrino
  alias Trinox.Query
  alias Trinox.Result

  doctest Trinox.Query

  setup do
    mock = MockTrino.start!()
    {:ok, mock: mock, opts: opts(mock)}
  end

  describe "start_link/1" do
    test "starts a connection that can be queried", %{opts: opts} do
      assert {:ok, conn} = Trinox.start_link(opts)
      assert is_pid(conn)

      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "passes the pool options on to DBConnection", %{opts: opts} do
      assert {:ok, conn} = Trinox.start_link(opts ++ [name: :trinox_named_test, pool_size: 2])

      assert {:ok, %Result{}} = Trinox.query(:trinox_named_test, MockTrino.sql(:single))

      stop(conn)
    end
  end

  describe "child_spec/1" do
    test "starts a named connection under a supervisor", %{opts: opts} do
      children = [{Trinox, opts ++ [name: :trinox_supervised_test]}]

      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

      assert {:ok, %Result{rows: [[1, "one"]]}} =
               Trinox.query(:trinox_supervised_test, MockTrino.sql(:single))

      :ok = Supervisor.stop(supervisor)
    end
  end

  describe "query/3" do
    test "runs a statement and returns the decoded result", %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert {:ok, %Result{} = result} = Trinox.query(conn, MockTrino.sql(:single))

      assert result.columns == ["id", "name"]
      assert result.rows == [[1, "one"]]
      assert result.num_rows == 1
      assert result.query_id == MockTrino.query_id()
      assert result.stats["state"] == "FINISHED"

      assert [%{method: "POST", path: "/v1/statement", body: body}, %{method: "GET"}] =
               MockTrino.requests(mock)

      assert body == MockTrino.sql(:single)

      stop(conn)
    end

    test "collects the rows of a query Trino paged", %{opts: opts} do
      conn = connect(opts)

      assert {:ok, %Result{} = result} = Trinox.query(conn, MockTrino.sql(:multi_page))

      assert result.columns == ["id", "name"]
      assert result.rows == [[1, "one"], [2, "two"], [3, "three"]]
      assert result.num_rows == 3

      stop(conn)
    end

    test "sends the connection's identity and session defaults", %{mock: mock, opts: opts} do
      conn = connect(opts, password: "secret", catalog: "tpch", schema: "sf1")

      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      for request <- MockTrino.requests(mock) do
        assert MockTrino.header(request, "authorization") ==
                 "Basic " <> Base.encode64("alice:secret")

        assert MockTrino.header(request, "x-trino-user") == "alice"
        assert MockTrino.header(request, "x-trino-catalog") == "tpch"
        assert MockTrino.header(request, "x-trino-schema") == "sf1"
      end

      stop(conn)
    end

    test "returns a Trinox.Error for a query Trino refused", %{opts: opts} do
      conn = connect(opts)

      assert {:error, %Error{} = error} = Trinox.query(conn, MockTrino.sql(:error))

      assert error.message =~ "Table 'mock.default.boom' does not exist"
      assert error.error_code == 44
      assert error.error_name == "TABLE_NOT_FOUND"
      assert error.error_type == "USER_ERROR"
      assert error.query_id == MockTrino.query_id()

      # A refused query is Trino's answer, not a broken connection: the next one works.
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "returns the error when the coordinator answers badly", %{opts: opts} do
      conn = connect(opts)

      assert {:error, %RuntimeError{message: message}} =
               Trinox.query(conn, MockTrino.sql(:unavailable))

      assert message =~ "HTTP 503"

      stop(conn)
    end

    test "lets an option override the connection's for one query", %{opts: opts} do
      conn = connect(opts, receive_timeout: 5_000)

      # The connection would have waited five seconds; this query gives up after one
      # millisecond, which it can only do if the option reached the protocol.
      assert {:error, %Mint.TransportError{reason: :timeout}} =
               Trinox.query(conn, MockTrino.sql(:slow), receive_timeout: 1)

      stop(conn)
    end
  end

  describe "query!/3" do
    test "returns the result", %{opts: opts} do
      conn = connect(opts)

      assert %Result{rows: [[1, "one"]]} = Trinox.query!(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "raises the error a query Trino refused reported", %{opts: opts} do
      conn = connect(opts)

      assert_raise Error, ~r/Table 'mock.default.boom' does not exist/, fn ->
        Trinox.query!(conn, MockTrino.sql(:error))
      end

      stop(conn)
    end
  end

  describe "session propagation" do
    test "echoes what one query set on the next query", %{mock: mock, opts: opts} do
      conn = connect(opts)

      # The mock sets a catalog, a schema and one property on the POST response, and a
      # second property on the terminal page.
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:session))
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      request = MockTrino.last_request(mock)

      assert MockTrino.header(request, "x-trino-catalog") == "memory"
      assert MockTrino.header(request, "x-trino-schema") == "default"

      assert MockTrino.header(request, "x-trino-session") ==
               "mock_page_prop=2,mock_scenario=session"

      stop(conn)
    end

    test "stops echoing a property the coordinator cleared", %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:session))
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:clear_session))
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      request = MockTrino.last_request(mock)

      assert MockTrino.header(request, "x-trino-catalog") == "memory"
      assert MockTrino.header(request, "x-trino-session") == "mock_page_prop=2"

      stop(conn)
    end
  end

  describe "Trinox.Query" do
    test "refuses parameters it cannot bind", %{opts: opts} do
      conn = connect(opts)
      query = %Query{statement: MockTrino.sql(:single)}

      assert_raise ArgumentError, ~r/does not support query parameters/, fn ->
        DBConnection.prepare_execute(conn, query, [1, 2])
      end

      stop(conn)
    end
  end

  # A pool of one, so that the session one query picked up is still there for the next.
  defp connect(opts, extra \\ []) do
    {:ok, conn} = Trinox.start_link(opts ++ [pool_size: 1] ++ extra)
    conn
  end

  # The pool goes down with the test process, and so does the mock it is talking to.
  # Stopping it here, while both are still alive, is what keeps an idle ping from
  # outliving the coordinator it would ping.
  defp stop(conn), do: :ok = GenServer.stop(conn)

  defp opts(mock) do
    [scheme: :http, hostname: "127.0.0.1", port: mock.port, username: "alice"]
  end
end
