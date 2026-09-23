defmodule Trinox.MockTrinoTest do
  use ExUnit.Case, async: true

  alias Trinox.MockTrino

  setup do
    # `start!/1` links the mock to the test process, so it goes away with the test.
    {:ok, mock: MockTrino.start!()}
  end

  describe "POST /v1/statement" do
    test "answers with a queued page pointing at a nextUri", %{mock: mock} do
      {200, _headers, body} = submit(mock, MockTrino.sql(:single))

      assert body["id"] == MockTrino.query_id()
      assert body["stats"]["state"] == "QUEUED"
      refute Map.has_key?(body, "columns")
      refute Map.has_key?(body, "data")

      assert body["nextUri"] ==
               "#{MockTrino.base_url(mock)}/v1/statement/single/#{MockTrino.query_id()}/1"
    end

    test "unknown statements fall back to the single-page scenario", %{mock: mock} do
      {200, _headers, body} = submit(mock, "SELECT count(*) FROM anything")

      assert body["nextUri"] =~ "/v1/statement/single/"
    end

    test "the immediate scenario is already terminal", %{mock: mock} do
      {200, _headers, body} = submit(mock, MockTrino.sql(:immediate))

      refute Map.has_key?(body, "nextUri")
      assert body["stats"]["state"] == "FINISHED"
      assert body["data"] == [[1, "one"]]
    end
  end

  describe "polling nextUri" do
    test "the single scenario returns columns, one row and no nextUri", %{mock: mock} do
      {200, _headers, body} = mock |> submit(MockTrino.sql(:single)) |> follow()

      refute Map.has_key?(body, "nextUri")
      assert body["stats"]["state"] == "FINISHED"
      assert body["data"] == [[1, "one"]]
      assert Enum.map(body["columns"], & &1["name"]) == ["id", "name"]
      assert Enum.map(body["columns"], & &1["type"]) == ["bigint", "varchar(5)"]
    end

    test "the multi_page scenario pages three times, with columns only on the first", %{
      mock: mock
    } do
      first = mock |> submit(MockTrino.sql(:multi_page)) |> follow()
      second = follow(first)
      third = follow(second)

      assert {200, _, %{"columns" => _, "data" => [[1, "one"]]}} = first
      assert {200, _, %{"data" => [[2, "two"]]}} = second
      assert {200, _, %{"data" => [[3, "three"]]}} = third

      {_, _, second_body} = second
      refute Map.has_key?(second_body, "columns")

      {_, _, third_body} = third
      refute Map.has_key?(third_body, "nextUri")
      assert third_body["stats"]["state"] == "FINISHED"
    end

    test "the error scenario returns an error object and no nextUri", %{mock: mock} do
      {200, _headers, body} = mock |> submit(MockTrino.sql(:error)) |> follow()

      refute Map.has_key?(body, "nextUri")
      assert body["stats"]["state"] == "FAILED"

      assert body["error"] == %{
               "message" => "line 1:15: Table 'mock.default.boom' does not exist",
               "errorCode" => 44,
               "errorName" => "TABLE_NOT_FOUND",
               "errorType" => "USER_ERROR"
             }
    end
  end

  describe "session headers" do
    test "the session scenario sets catalog, schema and properties across pages", %{mock: mock} do
      {200, post_headers, _body} = submitted = submit(mock, MockTrino.sql(:session))

      assert resp_header(post_headers, "x-trino-set-catalog") == "memory"
      assert resp_header(post_headers, "x-trino-set-schema") == "default"
      assert resp_header(post_headers, "x-trino-set-session") == "mock_scenario=session"

      {200, final_headers, _body} = follow(submitted)
      assert resp_header(final_headers, "x-trino-set-session") == "mock_page_prop=2"
    end

    test "the clear_session scenario clears a property on the terminal page", %{mock: mock} do
      {200, post_headers, _body} = submitted = submit(mock, MockTrino.sql(:clear_session))
      assert resp_header(post_headers, "x-trino-set-session") == nil

      {200, final_headers, _body} = follow(submitted)
      assert resp_header(final_headers, "x-trino-clear-session") == "mock_scenario"
    end
  end

  describe "request recording" do
    test "records method, path, headers and body of every request in order", %{mock: mock} do
      headers = [
        {"authorization", "Basic " <> Base.encode64("alice:secret")},
        {"x-trino-user", "alice"},
        {"x-trino-catalog", "tpch"},
        {"x-trino-schema", "sf1"},
        {"x-trino-session", "query_max_run_time=10m"}
      ]

      mock |> submit(MockTrino.sql(:single), headers) |> follow()

      assert [post, get] = MockTrino.requests(mock)

      assert post.method == "POST"
      assert post.path == "/v1/statement"
      assert post.body == MockTrino.sql(:single)
      assert MockTrino.header(post, "authorization") == "Basic " <> Base.encode64("alice:secret")
      assert MockTrino.header(post, "x-trino-user") == "alice"
      assert MockTrino.header(post, "x-trino-catalog") == "tpch"
      assert MockTrino.header(post, "x-trino-schema") == "sf1"
      assert MockTrino.header(post, "x-trino-session") == "query_max_run_time=10m"
      assert MockTrino.header(post, "x-trino-role") == nil

      assert get.method == "GET"
      assert get.path == "/v1/statement/single/#{MockTrino.query_id()}/1"
      assert get.body == ""

      assert MockTrino.last_request(mock) == get
    end

    test "last_request/1 is nil before any request", %{mock: mock} do
      assert MockTrino.requests(mock) == []
      assert MockTrino.last_request(mock) == nil
    end
  end

  describe "other routes" do
    test "GET /v1/info reports a running coordinator", %{mock: mock} do
      {200, _headers, body} = get(MockTrino.base_url(mock) <> "/v1/info")

      assert body == %{
               "coordinator" => true,
               "starting" => false,
               "nodeVersion" => %{"version" => "mock"}
             }

      assert [%{method: "GET", path: "/v1/info"}] = MockTrino.requests(mock)
    end

    test "GET /v1/slow answers only after the configured delay", %{mock: mock} do
      {elapsed, {200, _headers, body}} =
        :timer.tc(fn -> get(MockTrino.base_url(mock) <> "/v1/slow") end, :millisecond)

      assert body == %{"slow" => true}
      assert elapsed >= MockTrino.slow_delay()
    end

    test "unknown paths are 404s", %{mock: mock} do
      assert {404, _headers, "not found"} = get(MockTrino.base_url(mock) <> "/nope")
    end
  end

  test "stop/1 shuts the listener down", %{mock: mock} do
    assert :ok = MockTrino.stop(mock)

    refute Process.alive?(mock.server)
    refute Process.alive?(mock.recorder)
  end

  defp submit(mock, statement, headers \\ []) do
    request(:post, MockTrino.base_url(mock) <> "/v1/statement", headers, statement)
  end

  defp follow({200, _headers, %{"nextUri" => next_uri}}), do: get(next_uri)

  defp get(url), do: request(:get, url, [], nil)

  defp request(method, url, headers, body) do
    headers = Enum.map(headers, fn {name, value} -> {to_charlist(name), to_charlist(value)} end)

    request =
      case body do
        nil -> {to_charlist(url), headers}
        body -> {to_charlist(url), headers, ~c"text/plain", body}
      end

    {:ok, {{_version, status, _reason}, resp_headers, resp_body}} =
      :httpc.request(method, request, [], body_format: :binary)

    resp_headers =
      Enum.map(resp_headers, fn {name, value} -> {to_string(name), to_string(value)} end)

    {status, resp_headers, decode(resp_headers, resp_body)}
  end

  defp decode(headers, body) do
    case resp_header(headers, "content-type") do
      "application/json" <> _rest -> Jason.decode!(body)
      _other -> body
    end
  end

  defp resp_header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end
end
