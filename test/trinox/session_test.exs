defmodule Trinox.SessionTest do
  use ExUnit.Case, async: true

  alias Trinox.Session

  doctest Trinox.Session

  describe "build_headers/2" do
    test "sends the user, and Basic auth when a password is configured" do
      headers = Session.build_headers(%Session{}, user: "alice", password: "s3cret")

      assert headers == [
               {"authorization", "Basic " <> Base.encode64("alice:s3cret")},
               {"x-trino-user", "alice"}
             ]
    end

    test "omits the authorization header when no password is configured" do
      assert Session.build_headers(%Session{}, user: "alice") == [{"x-trino-user", "alice"}]
    end

    test "sends an empty password as Basic auth rather than as no auth" do
      headers = Session.build_headers(%Session{}, user: "alice", password: "")

      assert {"authorization", "Basic " <> Base.encode64("alice:")} in headers
    end

    # {the session, the headers it adds on top of x-trino-user}
    @combinations [
      {%Session{}, []},
      {%Session{catalog: "tpch"}, [{"x-trino-catalog", "tpch"}]},
      {%Session{schema: "sf1"}, [{"x-trino-schema", "sf1"}]},
      {%Session{catalog: "tpch", schema: "sf1"},
       [{"x-trino-catalog", "tpch"}, {"x-trino-schema", "sf1"}]},
      {%Session{properties: %{"query_max_run_time" => "10m"}},
       [{"x-trino-session", "query_max_run_time=10m"}]},
      {%Session{catalog: "tpch", properties: %{"query_max_run_time" => "10m"}},
       [{"x-trino-catalog", "tpch"}, {"x-trino-session", "query_max_run_time=10m"}]},
      {%Session{schema: "sf1", properties: %{"query_max_run_time" => "10m"}},
       [{"x-trino-schema", "sf1"}, {"x-trino-session", "query_max_run_time=10m"}]},
      {%Session{catalog: "tpch", schema: "sf1", properties: %{"query_max_run_time" => "10m"}},
       [
         {"x-trino-catalog", "tpch"},
         {"x-trino-schema", "sf1"},
         {"x-trino-session", "query_max_run_time=10m"}
       ]}
    ]

    for {session, expected} <- @combinations do
      test "renders #{inspect(session)}" do
        assert Session.build_headers(unquote(Macro.escape(session)), user: "alice") ==
                 [{"x-trino-user", "alice"} | unquote(Macro.escape(expected))]
      end
    end

    test "joins several session properties into one header, sorted by name" do
      session = %Session{properties: %{"query_max_run_time" => "10m", "join_distribution" => "a"}}
      headers = Session.build_headers(session, user: "alice")

      assert {"x-trino-session", "join_distribution=a,query_max_run_time=10m"} in headers
    end

    test "url-encodes property values so separators inside them survive" do
      session = %Session{properties: %{"prop" => "a,b=c d"}}
      headers = Session.build_headers(session, user: "alice")

      assert {"x-trino-session", "prop=a%2Cb%3Dc+d"} in headers
    end

    test "requires a user" do
      assert_raise KeyError, fn -> Session.build_headers(%Session{}, []) end
    end
  end

  describe "build_headers/2 client identity" do
    test "names the client so a query is not anonymous in Trino" do
      headers =
        Session.build_headers(%Session{},
          user: "alice",
          source: "billing-etl",
          client_info: "run=2026-09-26"
        )

      assert {"x-trino-source", "billing-etl"} in headers
      assert {"x-trino-client-info", "run=2026-09-26"} in headers
    end

    test "pins the time zone so a timestamp means the same everywhere" do
      headers = Session.build_headers(%Session{}, user: "alice", time_zone: "Europe/Berlin")

      assert {"x-trino-time-zone", "Europe/Berlin"} in headers
    end

    test "sends none of them when they are not configured" do
      names = Enum.map(Session.build_headers(%Session{}, user: "alice"), &elem(&1, 0))

      refute "x-trino-source" in names
      refute "x-trino-client-info" in names
      refute "x-trino-time-zone" in names
    end
  end

  describe "build_headers/2 session state" do
    test "sends the SQL path" do
      headers = Session.build_headers(%Session{path: "tpch.sf1"}, user: "alice")

      assert {"x-trino-path", "tpch.sf1"} in headers
    end

    test "sends prepared statements, encoded like session properties" do
      session = %Session{prepared_statements: %{"by_id" => "SELECT * FROM t WHERE id = ?"}}

      headers = Session.build_headers(session, user: "alice")

      assert {"x-trino-prepared-statement", "by_id=SELECT+%2A+FROM+t+WHERE+id+%3D+%3F"} in headers
    end

    test "sends the id of the transaction in progress" do
      headers = Session.build_headers(%Session{transaction_id: "tx-1"}, user: "alice")

      assert {"x-trino-transaction-id", "tx-1"} in headers
    end
  end

  describe "apply_response_headers/2" do
    # {the response headers, the session they produce from an empty one}
    @responses [
      {[], %Session{}},
      {[{"x-trino-set-catalog", "tpch"}], %Session{catalog: "tpch"}},
      {[{"x-trino-set-schema", "sf1"}], %Session{schema: "sf1"}},
      {[{"x-trino-set-catalog", "tpch"}, {"x-trino-set-schema", "sf1"}],
       %Session{catalog: "tpch", schema: "sf1"}},
      {[{"x-trino-set-session", "query_max_run_time=10m"}],
       %Session{properties: %{"query_max_run_time" => "10m"}}},
      {[{"x-trino-set-session", "a=1"}, {"x-trino-set-session", "b=2"}],
       %Session{properties: %{"a" => "1", "b" => "2"}}},
      {[{"x-trino-set-session", "a=1,b=2"}], %Session{properties: %{"a" => "1", "b" => "2"}}},
      {[{"x-trino-set-session", "a=1, b=2"}], %Session{properties: %{"a" => "1", "b" => "2"}}},
      {[{"x-trino-clear-session", "a"}], %Session{}},
      {[{"x-trino-set-session", "a=1"}, {"x-trino-clear-session", "a"}], %Session{}},
      {[{"x-trino-set-catalog", "tpch"}, {"x-trino-set-catalog", "memory"}],
       %Session{catalog: "memory"}},
      {[{"content-type", "application/json"}], %Session{}}
    ]

    for {headers, expected} <- @responses do
      test "#{inspect(headers)} yields #{inspect(expected)}" do
        assert Session.apply_response_headers(%Session{}, unquote(Macro.escape(headers))) ==
                 unquote(Macro.escape(expected))
      end
    end

    test "leaves properties the response did not mention alone" do
      session = %Session{catalog: "tpch", properties: %{"a" => "1", "b" => "2"}}

      assert Session.apply_response_headers(session, [{"x-trino-clear-session", "a"}]) ==
               %Session{catalog: "tpch", properties: %{"b" => "2"}}
    end

    test "clears several properties from one header" do
      session = %Session{properties: %{"a" => "1", "b" => "2", "c" => "3"}}

      assert Session.apply_response_headers(session, [{"x-trino-clear-session", "a,b"}]) ==
               %Session{properties: %{"c" => "3"}}
    end

    test "clearing a property that was never set is not an error" do
      assert Session.apply_response_headers(%Session{}, [{"x-trino-clear-session", "nope"}]) ==
               %Session{}
    end

    test "a later value for the same property replaces the earlier one" do
      headers = [{"x-trino-set-session", "a=1"}, {"x-trino-set-session", "a=2"}]

      assert Session.apply_response_headers(%Session{}, headers) ==
               %Session{properties: %{"a" => "2"}}
    end

    test "url-decodes property values" do
      headers = [{"x-trino-set-session", "prop=a%2Cb%3Dc+d"}]

      assert Session.apply_response_headers(%Session{}, headers) ==
               %Session{properties: %{"prop" => "a,b=c d"}}
    end

    test "keeps a value whose encoding is broken rather than dropping the property" do
      headers = [{"x-trino-set-session", "prop=100%"}]

      assert Session.apply_response_headers(%Session{}, headers) ==
               %Session{properties: %{"prop" => "100%"}}
    end

    test "keeps a value that contains an equals sign" do
      headers = [{"x-trino-set-session", "prop=a=b"}]

      assert Session.apply_response_headers(%Session{}, headers) ==
               %Session{properties: %{"prop" => "a=b"}}
    end

    test "reads header names case-insensitively" do
      headers = [{"X-Trino-Set-Catalog", "tpch"}, {"X-Trino-Set-Session", "a=1"}]

      assert Session.apply_response_headers(%Session{}, headers) ==
               %Session{catalog: "tpch", properties: %{"a" => "1"}}
    end

    # Nothing a coordinator should send, but a malformed header must not take the
    # connection down mid-query.
    @malformed [
      {"x-trino-set-session", "nameless"},
      {"x-trino-set-session", "=1"},
      {"x-trino-set-session", " =1"},
      {"x-trino-set-session", ""},
      {"x-trino-set-session", ","},
      {"x-trino-clear-session", ""}
    ]

    for {name, value} <- @malformed do
      test "ignores #{inspect(name)}: #{inspect(value)}" do
        header = {unquote(name), unquote(value)}

        assert Session.apply_response_headers(%Session{}, [header]) == %Session{}
      end
    end

    test "a trailing comma does not add an empty property" do
      headers = [{"x-trino-set-session", "a=1,"}]

      assert Session.apply_response_headers(%Session{}, headers) ==
               %Session{properties: %{"a" => "1"}}
    end
  end

  describe "apply_response_headers/2 beyond catalog and schema" do
    test "sets the SQL path" do
      session = apply_headers([{"x-trino-set-path", "tpch.sf1"}])

      assert session.path == "tpch.sf1"
    end

    test "adds a prepared statement, decoding the SQL it arrived URL-encoded as" do
      session = apply_headers([{"x-trino-added-prepare", "by_id=SELECT+%3F"}])

      assert session.prepared_statements == %{"by_id" => "SELECT ?"}
    end

    test "deallocates a prepared statement" do
      session =
        Session.apply_response_headers(
          %Session{prepared_statements: %{"by_id" => "SELECT ?", "other" => "SELECT 1"}},
          [{"x-trino-deallocated-prepare", "by_id"}]
        )

      assert session.prepared_statements == %{"other" => "SELECT 1"}
    end

    test "records the transaction a statement started" do
      session = apply_headers([{"x-trino-started-transaction-id", "tx-1"}])

      assert session.transaction_id == "tx-1"
    end

    test "forgets the transaction when the coordinator clears it" do
      session =
        Session.apply_response_headers(%Session{transaction_id: "tx-1"}, [
          {"x-trino-clear-transaction-id", "true"}
        ])

      assert session.transaction_id == nil
    end
  end

  describe "round trip" do
    test "what a response set is what the next request sends" do
      response = [
        {"x-trino-set-catalog", "tpch"},
        {"x-trino-set-schema", "sf1"},
        {"x-trino-set-session", "query_max_run_time=10m"}
      ]

      session = Session.apply_response_headers(%Session{}, response)

      assert Session.build_headers(session, user: "alice") == [
               {"x-trino-user", "alice"},
               {"x-trino-catalog", "tpch"},
               {"x-trino-schema", "sf1"},
               {"x-trino-session", "query_max_run_time=10m"}
             ]
    end

    test "a value needing encoding survives a full round trip" do
      value = "a,b=c d%e"
      session = %Session{properties: %{"prop" => value}}

      [{"x-trino-session", encoded}] =
        Enum.filter(Session.build_headers(session, user: "alice"), fn {name, _value} ->
          name == "x-trino-session"
        end)

      "prop=" <> _rest = encoded

      assert Session.apply_response_headers(%Session{}, [{"x-trino-set-session", encoded}]) ==
               session
    end
  end

  defp apply_headers(headers), do: Session.apply_response_headers(%Session{}, headers)
end
