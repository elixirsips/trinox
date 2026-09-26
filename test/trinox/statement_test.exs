defmodule Trinox.StatementTest do
  use ExUnit.Case, async: true

  alias Trinox.Error
  alias Trinox.HTTP
  alias Trinox.MockTrino
  alias Trinox.Statement

  @headers [{"x-trino-user", "alice"}]

  setup do
    mock = MockTrino.start!()
    {:ok, conn} = HTTP.connect(:http, "127.0.0.1", mock.port)
    {:ok, mock: mock, conn: conn}
  end

  describe "run/4" do
    test "polls a queued query through to its terminal page", %{mock: mock, conn: conn} do
      assert {:ok, _conn, [queued, final], _headers} =
               Statement.run(conn, MockTrino.sql(:single), @headers, [])

      assert queued["stats"]["state"] == "QUEUED"
      refute Map.has_key?(queued, "columns")

      assert final["stats"]["state"] == "FINISHED"
      assert final["data"] == [[1, "one"]]
      refute Map.has_key?(final, "nextUri")

      assert [%{method: "POST"}, %{method: "GET"}] = MockTrino.requests(mock)
    end

    test "accumulates every page of a multi-page query in order", %{mock: mock, conn: conn} do
      assert {:ok, _conn, pages, _headers} =
               Statement.run(conn, MockTrino.sql(:multi_page), @headers, [])

      assert length(pages) == 4

      assert Enum.flat_map(pages, &Map.get(&1, "data", [])) == [
               [1, "one"],
               [2, "two"],
               [3, "three"]
             ]

      # Columns arrive once, on the first page that has any.
      assert Enum.count(pages, &Map.has_key?(&1, "columns")) == 1
      refute Map.has_key?(List.last(pages), "nextUri")

      assert length(MockTrino.requests(mock)) == 4
    end

    test "follows a nextUri that carries a query string", %{mock: mock, conn: conn} do
      {:ok, _conn, [_queued, first | _rest], _headers} =
        Statement.run(conn, MockTrino.sql(:multi_page), @headers, [])

      assert first["nextUri"] =~ "?slug=mock"

      paths = mock |> MockTrino.requests() |> Enum.map(& &1.path) |> Enum.uniq()
      assert Enum.all?(paths, &(not String.contains?(&1, "?")))
      assert Enum.any?(paths, &String.starts_with?(&1, "/v1/statement/multi_page/"))
    end

    test "stops at a page that reports an error", %{conn: conn} do
      assert {:ok, _conn, pages, _headers} =
               Statement.run(conn, MockTrino.sql(:error), @headers, [])

      final = List.last(pages)
      assert final["error"]["errorName"] == "TABLE_NOT_FOUND"
      assert final["stats"]["state"] == "FAILED"
    end

    test "does not poll when the POST response is already terminal", %{mock: mock, conn: conn} do
      assert {:ok, _conn, [only_page], _headers} =
               Statement.run(conn, MockTrino.sql(:immediate), @headers, [])

      assert only_page["data"] == [[1, "one"]]
      assert [%{method: "POST"}] = MockTrino.requests(mock)
    end

    test "sends the given headers on the POST and on every poll", %{mock: mock, conn: conn} do
      headers = [{"x-trino-user", "alice"}, {"x-trino-catalog", "tpch"}]

      {:ok, _conn, _pages, _headers} =
        Statement.run(conn, MockTrino.sql(:multi_page), headers, [])

      for request <- MockTrino.requests(mock) do
        assert MockTrino.header(request, "x-trino-user") == "alice"
        assert MockTrino.header(request, "x-trino-catalog") == "tpch"
      end
    end

    test "sends the statement as the POST body", %{mock: mock, conn: conn} do
      statement = MockTrino.sql(:single)
      {:ok, _conn, _pages, _headers} = Statement.run(conn, statement, @headers, [])

      assert [post | _polls] = MockTrino.requests(mock)
      assert post.body == statement
    end

    test "waits between polls when :poll_interval_ms is set", %{conn: conn} do
      {elapsed, {:ok, _conn, pages, _headers}} =
        :timer.tc(
          fn ->
            Statement.run(conn, MockTrino.sql(:multi_page), @headers, poll_interval_ms: 20)
          end,
          :millisecond
        )

      assert length(pages) == 4
      # Three polls follow the POST, each waiting first.
      assert elapsed >= 60
    end

    test "passes :receive_timeout through to each request", %{conn: conn} do
      assert {:error, _conn, %Mint.TransportError{reason: :timeout}} =
               Statement.run(conn, MockTrino.sql(:slow), @headers, receive_timeout: 1)
    end

    test "returns every page's response headers in the order they arrived", %{conn: conn} do
      assert {:ok, _conn, _pages, headers} =
               Statement.run(conn, MockTrino.sql(:session), @headers, [])

      # The POST response sets catalog, schema and a property; the terminal page sets one
      # more, and a caller folding a session needs both.
      assert Enum.filter(headers, &match?({"x-trino-set-session", _value}, &1)) == [
               {"x-trino-set-session", "mock_scenario=session"},
               {"x-trino-set-session", "mock_page_prop=2"}
             ]

      assert {"x-trino-set-catalog", "memory"} in headers
      assert {"x-trino-set-schema", "default"} in headers
    end
  end

  describe "run/4 deadlines" do
    test "cancels the query and stops polling once the deadline passes",
         %{mock: mock, conn: conn} do
      assert {:error, conn, %Error{message: message}} =
               Statement.run(conn, MockTrino.sql(:endless), @headers,
                 poll_interval_ms: 1_000,
                 deadline: in_ms(150)
               )

      assert message =~ ":timeout"

      # The nextUri it had reached was DELETEd, which is how Trino is told to stop.
      assert [%{method: "DELETE", path: path} | _earlier] =
               mock |> MockTrino.requests() |> Enum.reverse()

      assert path =~ "/v1/statement/endless/"

      # The cancel is a complete request/response, so the connection lives on.
      assert HTTP.open?(conn)
    end

    test "submits nothing at all when the deadline has already passed",
         %{mock: mock, conn: conn} do
      assert {:error, _conn, %Error{}} =
               Statement.run(conn, MockTrino.sql(:single), @headers, deadline: in_ms(0))

      assert MockTrino.requests(mock) == []
    end

    test "caps :receive_timeout at the time the deadline has left", %{conn: conn} do
      # :slow answers after 100ms, and a receive timeout of a whole second would happily
      # wait for it — but the deadline says there are only 10ms to spend.
      assert {:error, _conn, reason} =
               Statement.run(conn, MockTrino.sql(:slow), @headers,
                 receive_timeout: 1_000,
                 deadline: in_ms(10)
               )

      assert %Mint.TransportError{reason: :timeout} = reason
    end

    test "does not wait out a poll interval that runs past the deadline", %{conn: conn} do
      started = System.monotonic_time(:millisecond)

      assert {:error, _conn, %Error{}} =
               Statement.run(conn, MockTrino.sql(:endless), @headers,
                 poll_interval_ms: 10_000,
                 deadline: in_ms(150)
               )

      # A full interval would have been ten seconds; the deadline cut it to fifty
      # milliseconds and a bit.
      assert System.monotonic_time(:millisecond) - started < 1_000
    end
  end

  describe "run/4 failures" do
    test "returns the transport error when the POST cannot be sent", %{conn: conn} do
      {:ok, conn} = HTTP.close(conn)

      assert {:error, _conn, %Mint.HTTPError{reason: :closed}} =
               Statement.run(conn, MockTrino.sql(:single), @headers, [])
    end

    test "returns the transport error when a poll fails", %{mock: mock, conn: conn} do
      # The POST is answered with `Connection: close`, so the coordinator is already gone
      # when the poll is sent — no sleeps and nothing to lose a race to. Only the POST
      # reaches the mock, which is what says the failure happened on the poll.
      assert {:error, _conn, %Mint.HTTPError{reason: :closed}} =
               Statement.run(conn, MockTrino.sql(:closing), @headers, [])

      assert [%{method: "POST"}] = MockTrino.requests(mock)
    end

    test "rejects a non-2xx answer", %{conn: conn} do
      assert {:error, _conn, %RuntimeError{message: message}} =
               Statement.run(conn, MockTrino.sql(:unavailable), @headers, [])

      assert message =~ "HTTP 503"
      assert message =~ "Service Unavailable"
    end

    test "rejects a body that is not JSON", %{conn: conn} do
      assert {:error, _conn, %Jason.DecodeError{}} =
               Statement.run(conn, MockTrino.sql(:invalid_json), @headers, [])
    end
  end

  describe "terminal?/1" do
    test "a page without a nextUri ends the query" do
      assert Statement.terminal?(%{"id" => "q", "data" => []})
    end

    test "a page with a nextUri does not" do
      refute Statement.terminal?(%{"id" => "q", "nextUri" => "http://host/v1/statement/q/1"})
    end

    test "an error ends the query even with a nextUri" do
      assert Statement.terminal?(%{
               "id" => "q",
               "nextUri" => "http://host/v1/statement/q/1",
               "error" => %{"message" => "boom"}
             })
    end
  end

  defp in_ms(milliseconds), do: System.monotonic_time(:millisecond) + milliseconds
end
