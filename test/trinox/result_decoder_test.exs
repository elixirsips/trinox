defmodule Trinox.ResultDecoderTest do
  use ExUnit.Case, async: true

  alias Trinox.Result
  alias Trinox.ResultDecoder

  doctest Trinox.ResultDecoder

  @query_id "20260924_120000_00000_mock"

  # {raw Trino type, the JSON value Trino sends, the term it decodes to}
  @scalars [
    {"boolean", true, true},
    {"boolean", false, false},
    {"tinyint", 7, 7},
    {"smallint", 300, 300},
    {"integer", 42, 42},
    {"bigint", 9_007_199_254_740_993, 9_007_199_254_740_993},
    {"real", 1.5, 1.5},
    {"double", -2.25, -2.25},
    {"decimal", "10.25", "10.25"},
    {"varchar", "hello", "hello"},
    {"char", "a    ", "a    "},
    {"varbinary", "AQID", <<1, 2, 3>>},
    {"json", ~s({"a":1}), ~s({"a":1})},
    {"uuid", "12151fd2-7586-11e9-8f9e-2a86e4085a59", "12151fd2-7586-11e9-8f9e-2a86e4085a59"},
    {"ipaddress", "10.0.0.1", "10.0.0.1"},
    {"date", "2026-09-24", ~D[2026-09-24]},
    {"time", "12:34:56.789", ~T[12:34:56.789]},
    {"time with time zone", "12:34:56.789+01:00", "12:34:56.789+01:00"},
    {"timestamp", "2026-09-24 12:34:56.789", ~N[2026-09-24 12:34:56.789]},
    {"timestamp with time zone", "2026-09-24 12:34:56.789 UTC", ~U[2026-09-24 12:34:56.789Z]},
    {"interval day to second", "3 12:00:00.000", "3 12:00:00.000"},
    {"interval year to month", "1-2", "1-2"},
    {"unknown", nil, nil}
  ]

  describe "decode/1" do
    test "builds a result from a single terminal page" do
      assert {:ok, %Result{} = result} = ResultDecoder.decode([page(data: [[1, "one"]])])

      assert result.columns == ["id", "name"]
      assert result.rows == [[1, "one"]]
      assert result.num_rows == 1
      assert result.query_id == @query_id
      assert result.stats == %{"state" => "FINISHED"}
    end

    test "concatenates the rows of every page, in page order" do
      pages = [
        queued(),
        page(data: [[1, "one"]], state: "RUNNING"),
        page(columns: nil, data: [[2, "two"]], state: "RUNNING"),
        page(columns: nil, data: [[3, "three"]])
      ]

      assert {:ok, result} = ResultDecoder.decode(pages)

      assert result.rows == [[1, "one"], [2, "two"], [3, "three"]]
      assert result.num_rows == 3
    end

    test "takes the columns from the first page that carries any" do
      pages = [queued(), page(data: [[1, "one"]])]

      assert {:ok, result} = ResultDecoder.decode(pages)
      assert result.columns == ["id", "name"]
    end

    test "takes the stats from the last page that has them" do
      pages = [queued(), page(data: [[1, "one"]])]

      assert {:ok, result} = ResultDecoder.decode(pages)
      assert result.stats == %{"state" => "FINISHED"}
    end

    test "a statement with no result set decodes to an empty result" do
      page = %{"id" => @query_id, "stats" => %{"state" => "FINISHED"}}

      assert {:ok, result} = ResultDecoder.decode([page])

      assert result.columns == []
      assert result.rows == []
      assert result.num_rows == 0
      assert result.query_id == @query_id
    end

    test "a query that returned zero rows keeps its columns" do
      assert {:ok, result} = ResultDecoder.decode([page(data: [])])

      assert result.columns == ["id", "name"]
      assert result.rows == []
      assert result.num_rows == 0
    end

    test "a page with no stats at all leaves stats empty" do
      assert {:ok, result} = ResultDecoder.decode([%{"id" => @query_id}])
      assert result.stats == %{}
    end
  end

  describe "decode/1 with a failed query" do
    test "returns Trino's error object" do
      error = %{"errorName" => "TABLE_NOT_FOUND", "message" => "does not exist"}
      pages = [queued(), Map.put(page(state: "FAILED"), "error", error)]

      assert {:error, ^error} = ResultDecoder.decode(pages)
    end

    test "finds an error reported on a page that is not the last" do
      error = %{"errorName" => "USER_CANCELLED"}
      pages = [Map.put(queued(), "error", error), page(data: [[1, "one"]])]

      assert {:error, ^error} = ResultDecoder.decode(pages)
    end
  end

  describe "scalar types" do
    for {raw_type, value, expected} <- @scalars do
      test "decodes #{raw_type} #{inspect(value)}" do
        assert decode_one(unquote(raw_type), unquote(Macro.escape(value))) ==
                 unquote(Macro.escape(expected))
      end
    end

    test "a null decodes to nil whatever the column type" do
      for {raw_type, _value, _expected} <- @scalars do
        assert decode_one(raw_type, nil) == nil
      end
    end

    test "decodes a double that JSON cannot express" do
      assert decode_one("double", "Infinity") == :infinity
      assert decode_one("double", "-Infinity") == :negative_infinity
      assert decode_one("double", "NaN") == :nan
    end

    test "widens an integral double to a float" do
      assert decode_one("double", 3) === 3.0
    end

    test "shifts a timestamp with an offset to UTC" do
      assert decode_one("timestamp with time zone", "2026-09-24 12:34:56.789 +01:00") ==
               ~U[2026-09-24 11:34:56.789Z]
    end

    test "keeps a timestamp in a named zone as the string Trino sent" do
      stamp = "2026-09-24 12:34:56.789 America/New_York"
      assert decode_one("timestamp with time zone", stamp) == stamp
    end

    test "keeps the value as sent for a type it does not know" do
      assert decode_one("kdb_tree", "something") == "something"
    end

    test "keeps the value as sent when it does not parse as its declared type" do
      assert decode_one("date", "not a date") == "not a date"
      assert decode_one("time", "nope") == "nope"
      assert decode_one("timestamp", "nope") == "nope"
      assert decode_one("timestamp with time zone", "nope nope Z") == "nope nope Z"
      assert decode_one("varbinary", "not base64!") == "not base64!"
    end

    test "keeps a value of an unexpected JSON shape as sent" do
      assert decode_one("date", 20_260_924) == 20_260_924
      assert decode_one("double", "3.5") == "3.5"
      assert decode_one("time", 123_456) == 123_456
      assert decode_one("timestamp", 20_260_924) == 20_260_924
      assert decode_one("varbinary", 1) == 1
      assert decode_one("timestamp with time zone", 1) == 1
    end

    test "keeps a timestamp with time zone that carries no zone as sent" do
      stamp = "2026-09-24T12:34:56.789Z"
      assert decode_one("timestamp with time zone", stamp) == stamp
    end
  end

  describe "parameterised types" do
    test "reads the raw type from a parameterised typeSignature" do
      column = %{
        "name" => "name",
        "type" => "varchar(5)",
        "typeSignature" => %{"rawType" => "varchar", "arguments" => [%{"kind" => "LONG"}]}
      }

      assert decode_column(column, "hello") == "hello"
    end

    test "falls back to the display type when there is no typeSignature" do
      assert decode_column(%{"name" => "d", "type" => "date"}, "2026-09-24") == ~D[2026-09-24]
    end

    test "strips the parameters off a display type" do
      assert decode_column(%{"name" => "t", "type" => "timestamp(3)"}, "2026-09-24 00:00:00") ==
               ~N[2026-09-24 00:00:00]
    end

    test "strips the parameters out of the middle of a display type" do
      column = %{"name" => "t", "type" => "timestamp(3) with time zone"}

      assert decode_column(column, "2026-09-24 12:34:56.789 UTC") == ~U[2026-09-24 12:34:56.789Z]
    end

    test "keeps the value as sent for a column with no type information at all" do
      assert decode_column(%{"name" => "mystery"}, "2026-09-24") == "2026-09-24"
    end
  end

  describe "array" do
    test "decodes each element" do
      assert decode_column(column("array", [type("date")]), ["2026-09-24", "2026-09-25"]) ==
               [~D[2026-09-24], ~D[2026-09-25]]
    end

    test "decodes a null element to nil" do
      assert decode_column(column("array", [type("date")]), ["2026-09-24", nil]) ==
               [~D[2026-09-24], nil]
    end

    test "decodes a nested array" do
      signature = column("array", [type("array", [type("bigint")])])

      assert decode_column(signature, [[1, 2], [3]]) == [[1, 2], [3]]
    end

    test "keeps the list as sent when the element type is missing" do
      assert decode_column(column("array", []), ["2026-09-24"]) == ["2026-09-24"]
    end

    test "keeps the list as sent when the only argument is not a type" do
      signature = column("array", [%{"kind" => "LONG", "value" => 5}])

      assert decode_column(signature, ["2026-09-24"]) == ["2026-09-24"]
    end

    test "keeps the list as sent when the signature carries no arguments at all" do
      assert decode_column(raw_column("array"), ["2026-09-24"]) == ["2026-09-24"]
    end
  end

  describe "map" do
    test "decodes the values and keeps the keys as Trino stringified them" do
      signature = column("map", [type("varchar"), type("date")])

      assert decode_column(signature, %{"a" => "2026-09-24"}) == %{"a" => ~D[2026-09-24]}
    end

    test "decodes a null value to nil" do
      signature = column("map", [type("varchar"), type("date")])

      assert decode_column(signature, %{"a" => nil}) == %{"a" => nil}
    end

    test "keeps the map as sent when the value type is missing" do
      assert decode_column(column("map", []), %{"a" => "2026-09-24"}) == %{"a" => "2026-09-24"}
    end
  end

  describe "row" do
    test "decodes named fields into a map" do
      signature = column("row", [named("when", "date"), named("what", "varchar")])

      assert decode_column(signature, ["2026-09-24", "launch"]) ==
               %{"when" => ~D[2026-09-24], "what" => "launch"}
    end

    test "decodes unnamed fields into a list, in field order" do
      signature = column("row", [type("date"), type("varchar")])

      assert decode_column(signature, ["2026-09-24", "launch"]) == [~D[2026-09-24], "launch"]
    end

    test "falls back to a list when only some fields are named" do
      signature = column("row", [named("when", "date"), type("varchar")])

      assert decode_column(signature, ["2026-09-24", "launch"]) == [~D[2026-09-24], "launch"]
    end

    test "decodes a row nested in an array" do
      signature = column("array", [type("row", [named("when", "date")])])

      assert decode_column(signature, [["2026-09-24"]]) == [%{"when" => ~D[2026-09-24]}]
    end

    test "keeps the value as sent when the fields are missing" do
      assert decode_column(column("row", []), ["2026-09-24"]) == ["2026-09-24"]
    end

    test "keeps the value as sent when the signature carries no arguments at all" do
      assert decode_column(raw_column("row"), ["2026-09-24"]) == ["2026-09-24"]
    end

    test "falls back to a list when a field is neither a named type nor a type" do
      signature = column("row", [named("when", "date"), %{"kind" => "LONG", "value" => 5}])

      assert decode_column(signature, ["2026-09-24", 5]) == [~D[2026-09-24], 5]
    end

    test "falls back to a list when a named field carries no field name" do
      anonymous = %{"kind" => "NAMED_TYPE", "value" => %{"typeSignature" => date_signature()}}
      signature = column("row", [named("when", "date"), anonymous])

      assert decode_column(signature, ["2026-09-24", "2026-09-25"]) ==
               [~D[2026-09-24], ~D[2026-09-25]]
    end
  end

  # Runs one value through the decoder for a column of the given raw Trino type.
  defp decode_one(raw_type, value), do: decode_column(column(raw_type), value)

  defp decode_column(column, value) do
    assert {:ok, %Result{rows: [[decoded]]}} =
             ResultDecoder.decode([
               %{"id" => @query_id, "columns" => [column], "data" => [[value]]}
             ])

    decoded
  end

  defp column(raw_type, arguments \\ []) do
    %{
      "name" => "value",
      "type" => raw_type,
      "typeSignature" => type(raw_type, arguments)["value"]
    }
  end

  # A `TYPE` argument of a type signature, which is also the shape of a signature itself.
  defp type(raw_type, arguments \\ []) do
    %{"kind" => "TYPE", "value" => %{"rawType" => raw_type, "arguments" => arguments}}
  end

  # A column whose typeSignature is only a raw type, with no arguments key at all.
  defp raw_column(raw_type) do
    %{"name" => "value", "type" => raw_type, "typeSignature" => %{"rawType" => raw_type}}
  end

  defp date_signature, do: %{"rawType" => "date", "arguments" => []}

  defp named(name, raw_type, arguments \\ []) do
    %{
      "kind" => "NAMED_TYPE",
      "value" => %{
        "fieldName" => %{"name" => name, "delimited" => false},
        "typeSignature" => %{"rawType" => raw_type, "arguments" => arguments}
      }
    }
  end

  defp queued do
    %{
      "id" => @query_id,
      "nextUri" => "http://localhost:8080/v1/statement/#{@query_id}/1",
      "stats" => %{"state" => "QUEUED"}
    }
  end

  defp page(opts) do
    page = %{"id" => @query_id, "stats" => %{"state" => Keyword.get(opts, :state, "FINISHED")}}

    page =
      case Keyword.get(opts, :columns, :default) do
        :default -> Map.put(page, "columns", default_columns())
        nil -> page
        columns -> Map.put(page, "columns", columns)
      end

    case Keyword.fetch(opts, :data) do
      {:ok, data} -> Map.put(page, "data", data)
      :error -> page
    end
  end

  defp default_columns do
    [
      Map.put(column("bigint"), "name", "id"),
      Map.put(column("varchar"), "name", "name")
    ]
  end
end
