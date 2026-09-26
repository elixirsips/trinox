defmodule TrinoxTest do
  use ExUnit.Case, async: true

  alias Trinox.Error
  alias Trinox.MockTrino
  alias Trinox.Result

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

    test "passes the process options on to the connection", %{opts: opts} do
      assert {:ok, conn} = Trinox.start_link(opts ++ [name: :trinox_named_test])

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

    test "starts several connections side by side", %{opts: opts} do
      # Concurrency here means more connections, so the specs of two of them have to be
      # able to live in one supervisor without being told apart by hand.
      children = [
        {Trinox, opts ++ [name: :trinox_side_by_side_a]},
        {Trinox, opts ++ [name: :trinox_side_by_side_b]}
      ]

      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

      assert {:ok, %Result{}} = Trinox.query(:trinox_side_by_side_a, MockTrino.sql(:single))
      assert {:ok, %Result{}} = Trinox.query(:trinox_side_by_side_b, MockTrino.sql(:single))

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

      # A half-read response leaves nothing to reuse the socket for, so the connection
      # stops itself once the caller has the error; there is nothing left to stop here.
      refute Process.alive?(conn)
    end
  end

  describe "query/3 timeouts" do
    test "cancels a query that outruns its :timeout and keeps the connection",
         %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert {:error, %Error{message: message}} =
               Trinox.query(conn, MockTrino.sql(:endless), poll_interval_ms: 1_000, timeout: 150)

      assert message =~ ":timeout"
      assert Enum.any?(MockTrino.requests(mock), &(&1.method == "DELETE"))

      # The caller gets an error rather than an exit, and the connection is still good.
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      stop(conn)
    end
  end

  describe "query!/3" do
    test "returns the result", %{opts: opts} do
      conn = connect(opts)

      assert %Result{rows: [[1, "one"]]} = Trinox.query!(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "raises when a query is cancelled for running too long", %{opts: opts} do
      conn = connect(opts)

      assert_raise Error, ~r/:timeout/, fn ->
        Trinox.query!(conn, MockTrino.sql(:endless), poll_interval_ms: 1_000, timeout: 150)
      end

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

  describe "transaction/3" do
    test "runs the statements inside one Trino transaction", %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert {:ok, :both_done} =
               Trinox.transaction(conn, fn conn ->
                 {:ok, _first} = Trinox.query(conn, MockTrino.sql(:single))
                 {:ok, _second} = Trinox.query(conn, MockTrino.sql(:single))
                 :both_done
               end)

      bodies = mock |> MockTrino.requests() |> Enum.map(& &1.body) |> Enum.reject(&(&1 == ""))
      assert MockTrino.sql(:begin) in bodies
      assert MockTrino.sql(:commit) in bodies

      # Every request between the two carried the id Trino handed back.
      inside =
        mock
        |> MockTrino.requests()
        |> Enum.filter(&(&1.body == MockTrino.sql(:single)))

      for request <- inside do
        assert MockTrino.header(request, "x-trino-transaction-id") == MockTrino.transaction_id()
      end

      # And the commit cleared it, so the next query is outside the transaction again.
      {:ok, _result} = Trinox.query(conn, MockTrino.sql(:single))
      assert MockTrino.header(MockTrino.last_request(mock), "x-trino-transaction-id") == nil

      stop(conn)
    end

    test "rolls back and re-raises what the function raised", %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert_raise RuntimeError, "no good", fn ->
        Trinox.transaction(conn, fn _conn -> raise "no good" end)
      end

      bodies = mock |> MockTrino.requests() |> Enum.map(& &1.body)
      assert MockTrino.sql(:rollback) in bodies
      refute MockTrino.sql(:commit) in bodies

      # The connection is released, so it still works.
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "rolls back when asked to, and reports the reason", %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert {:error, :changed_my_mind} =
               Trinox.transaction(conn, fn conn ->
                 Trinox.rollback(conn, :changed_my_mind)
               end)

      assert MockTrino.sql(:rollback) in Enum.map(MockTrino.requests(mock), & &1.body)
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "refuses to nest", %{opts: opts} do
      conn = connect(opts)

      assert {:ok, {:error, %Error{message: message}}} =
               Trinox.transaction(conn, fn conn ->
                 Trinox.transaction(conn, fn _conn -> :unreachable end)
               end)

      assert message =~ "already has an open transaction"

      stop(conn)
    end

    test "refuses a query from any other process", %{opts: opts} do
      conn = connect(opts)

      assert {:ok, {:error, %Error{message: message}}} =
               Trinox.transaction(conn, fn conn ->
                 task = Task.async(fn -> Trinox.query(conn, MockTrino.sql(:single)) end)
                 Task.await(task)
               end)

      assert message =~ "owned by another process"

      stop(conn)
    end

    test "reports a transaction that could not be started" do
      # A coordinator that refuses the START TRANSACTION itself.
      conn = MockTrino.start!(fail: [:begin]) |> opts() |> connect()

      assert {:error, %RuntimeError{message: message}} =
               Trinox.transaction(conn, fn _conn -> :unreachable end, max_attempts: 1)

      assert message =~ "HTTP 503"

      stop(conn)
    end

    test "reports a commit the coordinator refused, and still hands the connection back" do
      conn = MockTrino.start!(fail: [:commit]) |> opts() |> connect()

      assert {:error, %RuntimeError{message: message}} =
               Trinox.transaction(conn, fn _conn -> :done end, max_attempts: 1)

      assert message =~ "HTTP 503"

      # The transaction is over either way, so the connection is no longer reserved.
      assert {:ok, %Result{}} = Trinox.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "stops when the process holding the transaction dies", %{opts: opts} do
      Process.flag(:trap_exit, true)
      conn = connect(opts)

      # A process that opens a transaction and then abandons it.
      owner =
        spawn(fn ->
          :ok = Trinox.Connection.begin(conn)
          Process.sleep(:infinity)
        end)

      assert eventually(fn -> transaction_open?(conn) end)

      Process.exit(owner, :kill)

      assert eventually(fn -> not Process.alive?(conn) end)
    end
  end

  describe "ping/2" do
    test "checks the connection with the coordinator", %{mock: mock, opts: opts} do
      conn = connect(opts)

      assert :ok = Trinox.ping(conn)
      assert MockTrino.last_request(mock).path == "/v1/info"

      stop(conn)
    end
  end

  defp connect(opts, extra \\ []) do
    {:ok, conn} = Trinox.start_link(opts ++ extra)
    conn
  end

  # The connection goes down with the test process, and so does the mock it is talking
  # to. Stopping it here, while both are still alive, keeps the two in step.
  defp stop(conn), do: :ok = GenServer.stop(conn)

  defp opts(mock) do
    [scheme: :http, hostname: "127.0.0.1", port: mock.port, username: "alice"]
  end

  # A connection with an owner refuses a query from anyone else, which is how another
  # process can tell that the transaction has been opened.
  defp transaction_open?(conn) do
    match?({:error, %Error{}}, Trinox.query(conn, MockTrino.sql(:single)))
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(10) || eventually(fun, attempts - 1)
    end
  end
end
