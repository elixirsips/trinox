defmodule Trinox.ConnectionTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Trinox.Connection
  alias Trinox.Error
  alias Trinox.MockTrino
  alias Trinox.Result

  setup do
    mock = MockTrino.start!()
    {:ok, mock: mock, opts: opts(mock)}
  end

  describe "start_link/1" do
    test "opens a connection that can be queried", %{opts: opts} do
      assert {:ok, conn} = Connection.start_link(opts)
      assert is_pid(conn)

      assert {:ok, %Result{rows: [[1, "one"]]}} = Connection.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "names the process when asked to", %{opts: opts} do
      assert {:ok, conn} = Connection.start_link(opts ++ [name: :trinox_connection_named])

      assert {:ok, %Result{}} = Connection.query(:trinox_connection_named, MockTrino.sql(:single))

      stop(conn)
    end

    test "refuses to start when the connection cannot be opened", %{opts: opts} do
      Process.flag(:trap_exit, true)

      assert {:error, %ArgumentError{message: message}} =
               Connection.start_link(Keyword.delete(opts, :username))

      assert message =~ ":username"
    end

    test "refuses to start on an option that will never work", %{opts: opts} do
      Process.flag(:trap_exit, true)

      # A bad option has to be refused here rather than crashing once the process is up:
      # `start_link/1` has returned by then, and the crash would take the caller with it.
      assert {:error, %ArgumentError{message: message}} =
               Connection.start_link(Keyword.put(opts, :scheme, "http"))

      assert message =~ ":scheme"
      refute_receive {:EXIT, _pid, _reason}
    end
  end

  describe "query/3" do
    setup %{opts: opts} do
      {:ok, conn} = Connection.start_link(opts)

      {:ok, conn: conn}
    end

    test "returns the error of a query Trino refused and stays up", %{conn: conn} do
      assert {:error, %Error{error_name: "TABLE_NOT_FOUND"}} =
               Connection.query(conn, MockTrino.sql(:error))

      assert Process.alive?(conn)
      assert {:ok, %Result{}} = Connection.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "answers, then stops, when the connection did not survive", %{conn: conn} do
      Process.flag(:trap_exit, true)

      assert {:error, %Mint.HTTPError{reason: :closed}} =
               Connection.query(conn, MockTrino.sql(:closing))

      assert_receive {:EXIT, ^conn, :normal}
      refute Process.alive?(conn)
    end
  end

  describe "query/3 deadlines" do
    test "cancels a query that outlives its :timeout, and frees the connection",
         %{mock: mock, opts: opts} do
      {:ok, conn} = Connection.start_link(opts)

      # Warm the connection first: a deadline caps the timeout of the request it covers, so
      # a cold first POST on a loaded machine can eat the budget before there is a query to
      # cancel at all.
      assert {:ok, %Result{}} = Connection.query(conn, MockTrino.sql(:single))

      # :endless never reaches a terminal page, so only the deadline can end it. The poll
      # interval is longer than the deadline on purpose: it gets clamped to what is left,
      # so the run adds exactly the POST and the DELETE that cancels it.
      assert {:error, %Error{message: message}} =
               Connection.query(conn, MockTrino.sql(:endless),
                 poll_interval_ms: 60_000,
                 timeout: 500
               )

      assert message =~ ":timeout"

      # Trino was told to stop, rather than left running with nobody listening.
      assert Enum.any?(MockTrino.requests(mock), &(&1.method == "DELETE"))

      # And the connection is usable at once: the abandoned query is not still being
      # polled, which is what would make the next caller wait for it.
      assert {:ok, %Result{}} = Connection.query(conn, MockTrino.sql(:single))

      stop(conn)
    end

    test "answers the caller rather than making it give up", %{opts: opts} do
      Process.flag(:trap_exit, true)
      {:ok, conn} = Connection.start_link(opts)
      assert {:ok, %Result{}} = Connection.query(conn, MockTrino.sql(:single))

      # The connection enforces the deadline itself, so the caller's own call outlives it
      # and receives the error instead of exiting on a timeout of its own.
      assert {:error, %Error{}} =
               Connection.query(conn, MockTrino.sql(:endless),
                 poll_interval_ms: 60_000,
                 timeout: 500
               )

      stop(conn)
    end

    test "does not submit a query whose :timeout has already gone", %{mock: mock, opts: opts} do
      {:ok, conn} = Connection.start_link(opts)

      assert {:error, %Error{}} = Connection.query(conn, MockTrino.sql(:single), timeout: 0)
      assert MockTrino.requests(mock) == []

      stop(conn)
    end
  end

  describe "start_link/1 with an unreachable coordinator" do
    setup %{mock: mock} do
      port = mock.port
      :ok = MockTrino.stop(mock)

      {:ok, down_opts: [scheme: :http, hostname: "127.0.0.1", port: port, username: "alice"]}
    end

    test "starts anyway, and reports the failure to callers", %{down_opts: down_opts} do
      log =
        capture_log(fn ->
          assert {:ok, conn} = Connection.start_link(down_opts)
          assert Process.alive?(conn)

          assert {:error, %Mint.TransportError{reason: :econnrefused}} =
                   Connection.query(conn, "SELECT 1")

          assert {:error, %Mint.TransportError{reason: :econnrefused}} = Connection.ping(conn)

          stop(conn)
        end)

      assert log =~ "could not connect to Trino"
    end

    test "refuses to start a transaction it cannot start", %{down_opts: down_opts} do
      capture_log(fn ->
        {:ok, conn} = Connection.start_link(down_opts)

        assert {:error, %Mint.TransportError{reason: :econnrefused}} =
                 Trinox.transaction(conn, fn _conn -> :unreachable end)

        stop(conn)
      end)
    end

    test "keeps retrying, waiting longer each time", %{down_opts: down_opts} do
      log =
        capture_log(fn ->
          {:ok, conn} = Connection.start_link(down_opts)

          # The first attempt has already failed and scheduled a retry; drive the next two
          # by hand rather than waiting for the clock.
          send(conn, :reconnect)
          send(conn, :reconnect)

          # This query queues behind both, so by the time it answers they have run.
          assert {:error, %Mint.TransportError{}} = Connection.query(conn, "SELECT 1")

          stop(conn)
        end)

      assert log =~ "retrying in 200ms"
      assert log =~ "retrying in 400ms"
      assert log =~ "retrying in 800ms"
    end

    test "recovers when the coordinator turns up", %{down_opts: down_opts} do
      capture_log(fn ->
        {:ok, conn} = Connection.start_link(down_opts)
        assert {:error, %Mint.TransportError{}} = Connection.query(conn, "SELECT 1")

        back = MockTrino.start!(port: Keyword.fetch!(down_opts, :port))
        send(conn, :reconnect)

        assert {:ok, %Result{rows: [[1, "one"]]}} =
                 Connection.query(conn, MockTrino.sql(:single))

        stop(conn)
        MockTrino.stop(back)
      end)
    end
  end

  describe "commit/2 and rollback/2 outside a transaction" do
    test "refuse when there is nothing open", %{opts: opts} do
      {:ok, conn} = Connection.start_link(opts)

      assert {:error, %Error{message: message}} = Connection.commit(conn)
      assert message =~ "no open transaction"
      assert {:error, %Error{}} = Connection.rollback(conn)

      stop(conn)
    end

    test "stop the connection when committing breaks it", %{mock: mock, opts: opts} do
      Process.flag(:trap_exit, true)
      {:ok, conn} = Connection.start_link(opts)

      assert :ok = Connection.begin(conn)

      # The coordinator goes away with the transaction still open, so the COMMIT cannot be
      # delivered and there is nothing left to hand back.
      :ok = MockTrino.stop(mock)

      assert {:error, reason} = Connection.commit(conn)
      assert is_exception(reason)
      assert_receive {:EXIT, ^conn, :normal}
    end
  end

  describe "ping/2" do
    test "asks the coordinator for /v1/info", %{mock: mock, opts: opts} do
      {:ok, conn} = Connection.start_link(opts)

      assert :ok = Connection.ping(conn)
      assert MockTrino.last_request(mock).path == "/v1/info"

      stop(conn)
    end

    test "answers, then stops, when the coordinator is unhealthy" do
      Process.flag(:trap_exit, true)
      mock = MockTrino.start!(info_status: 503)
      {:ok, conn} = Connection.start_link(opts(mock))

      assert {:error, %RuntimeError{message: message}} = Connection.ping(conn)
      assert message =~ "503"

      assert_receive {:EXIT, ^conn, :normal}
    end
  end

  # The connection goes down with the test process, and so does the mock it is talking
  # to. Stopping it here, while both are still alive, keeps the two in step.
  defp stop(conn), do: :ok = GenServer.stop(conn)

  defp opts(mock) do
    [scheme: :http, hostname: "127.0.0.1", port: mock.port, username: "alice"]
  end
end
