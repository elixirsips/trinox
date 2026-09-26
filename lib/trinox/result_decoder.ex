defmodule Trinox.ResultDecoder do
  @moduledoc """
  Folds the JSON pages `Trinox.Statement` collected into a `Trinox.Result`.

  Two things make this more than a `Map.get/2`. The pages are a stream, not a record:
  `columns` arrives once, on the first page that carries any, while `data` dribbles in
  across all of them. And the values in `data` are JSON, so a `date` reaches us as
  `"2026-09-24"` and a `varbinary` as Base64 — the column's Trino type is the only thing
  that says which.

  So decoding builds one decoder function per column, from that column's type, and runs
  every page's rows through it. The function is built once per query rather than once per
  value, which is what keeps a wide result from re-parsing its own type signatures a
  million times.

  A statement that changes something rather than returning rows says so in `updateType`
  and `updateCount` instead of in `columns` and `data`; both are carried through to
  `Trinox.Result`.

  Everything here is a pure function over decoded JSON. Fetching the pages is
  `Trinox.Statement`'s job.

  ## Type mapping

  | Trino | Elixir |
  | --- | --- |
  | `boolean` | `true` / `false` |
  | `tinyint`, `smallint`, `integer`, `bigint` | `t:integer/0` |
  | `real`, `double` | `t:float/0`, or `:infinity` / `:negative_infinity` / `:nan` |
  | `decimal` | `t:String.t/0` — exact, and Elixir has no built-in arbitrary-precision decimal |
  | `varchar`, `char`, `json`, `uuid`, `ipaddress`, `interval ...` | `t:String.t/0` |
  | `varbinary` | `t:binary/0`, Base64-decoded |
  | `date` | `t:Date.t/0` |
  | `time` | `t:Time.t/0` |
  | `timestamp` | `t:NaiveDateTime.t/0` |
  | `timestamp with time zone` | `t:DateTime.t/0`, shifted to UTC |
  | `array(t)` | `t:list/0`, elements decoded |
  | `map(k, v)` | `t:map/0`, values decoded |
  | `row(...)` | `t:map/0` keyed by field name, or a list when the fields are unnamed |
  | anything else | as Trino sent it |

  A JSON `null` is `nil` at every depth, whatever the column type.

  Two cases deliberately keep the string Trino sent instead of failing the query: a
  `timestamp with time zone` in a named zone (`America/New_York`) rather than a numeric
  offset, since resolving one needs a time zone database this library does not depend on,
  and a value that does not parse as its declared type at all. `time with time zone` is
  always a string for the same reason — it carries an offset, and Elixir's `Time` has
  nowhere to put one.
  """

  alias Trinox.Result
  alias Trinox.Statement

  @passthrough ~w(boolean tinyint smallint integer bigint decimal varchar char json uuid ipaddress
                  unknown) ++
                 ["time with time zone", "interval day to second", "interval year to month"]

  @unknown %{"rawType" => "unknown", "arguments" => []}

  @doc """
  Decodes the pages of one query.

  Returns `{:error, error}` with Trino's own error object when any page reported one — a
  query that failed server-side has no rows to speak of. A later issue wraps that map in
  an exception; until then it is passed through as Trino sent it.

  Expects at least one page, which is what `Trinox.Statement.run/4` always returns.

      iex> page = %{
      ...>   "id" => "20260924_120000_00000_abcde",
      ...>   "columns" => [%{"name" => "d", "type" => "date"}],
      ...>   "data" => [["2026-09-24"]],
      ...>   "stats" => %{"state" => "FINISHED"}
      ...> }
      iex> {:ok, result} = Trinox.ResultDecoder.decode([page])
      iex> {result.columns, result.rows, result.num_rows}
      {["d"], [[~D[2026-09-24]]], 1}
  """
  @spec decode([Statement.page(), ...]) :: {:ok, Result.t()} | {:error, map()}
  def decode([_page | _rest] = pages) do
    case Enum.find_value(pages, & &1["error"]) do
      nil -> {:ok, build(pages)}
      error -> {:error, error}
    end
  end

  defp build(pages) do
    columns = Enum.find_value(pages, [], & &1["columns"])
    rows = rows(pages, Enum.map(columns, &column_decoder/1))

    %Result{
      columns: Enum.map(columns, & &1["name"]),
      rows: rows,
      num_rows: length(rows),
      query_id: Enum.find_value(pages, & &1["id"]),
      update_type: Enum.find_value(pages, & &1["updateType"]),
      update_count: Enum.find_value(pages, & &1["updateCount"]),
      stats: stats(pages)
    }
  end

  # The last page's stats are the query's final ones; earlier pages only ever said
  # QUEUED or RUNNING.
  defp stats(pages) do
    pages |> Enum.reverse() |> Enum.find_value(%{}, & &1["stats"])
  end

  defp rows(pages, decoders) do
    Enum.flat_map(pages, fn page ->
      Enum.map(page["data"] || [], &zip_decode(&1, decoders))
    end)
  end

  # A row Trino sent is positional, so it lines up with the column decoders by index.
  defp zip_decode(values, decoders) do
    Enum.zip_with(values, decoders, fn value, decode -> decode.(value) end)
  end

  defp column_decoder(column), do: column |> type_signature() |> decoder()

  # Every decoder is wrapped once here, so no individual one has to think about null.
  defp decoder(signature) do
    decode = raw_decoder(signature)

    fn
      nil -> nil
      value -> decode.(value)
    end
  end

  defp raw_decoder(%{"rawType" => raw}) when raw in @passthrough, do: &Function.identity/1
  defp raw_decoder(%{"rawType" => raw}) when raw in ~w(real double), do: &to_float/1
  defp raw_decoder(%{"rawType" => "varbinary"}), do: &to_binary/1
  defp raw_decoder(%{"rawType" => "date"}), do: &to_date/1
  defp raw_decoder(%{"rawType" => "time"}), do: &to_time/1
  defp raw_decoder(%{"rawType" => "timestamp"}), do: &to_naive_date_time/1
  defp raw_decoder(%{"rawType" => "timestamp with time zone"}), do: &to_date_time/1

  defp raw_decoder(%{"rawType" => "array"} = signature) do
    case type_arguments(signature) do
      [element] ->
        decode = decoder(element)
        &Enum.map(&1, decode)

      _missing ->
        &Function.identity/1
    end
  end

  # Trino writes a map as a JSON object, so its keys arrive stringified whatever the key
  # type is. They are left exactly as sent; only the values are decoded.
  defp raw_decoder(%{"rawType" => "map"} = signature) do
    case type_arguments(signature) do
      [_key, value] ->
        decode = decoder(value)
        &Map.new(&1, fn {name, field} -> {name, decode.(field)} end)

      _missing ->
        &Function.identity/1
    end
  end

  defp raw_decoder(%{"rawType" => "row"} = signature) do
    case row_fields(signature) do
      [] -> &Function.identity/1
      fields -> row_decoder(fields)
    end
  end

  defp raw_decoder(_signature), do: &Function.identity/1

  # A row's fields arrive as a positional JSON array. Named fields make a far more useful
  # map, but a row can be anonymous (`CAST(ROW(1, 2) AS ...)`), and then a list is all
  # there is to give.
  defp row_decoder(fields) do
    names = Enum.map(fields, fn {name, _signature} -> name end)
    decoders = Enum.map(fields, fn {_name, signature} -> decoder(signature) end)

    if Enum.all?(names, &is_binary/1) do
      fn values -> names |> Enum.zip(zip_decode(values, decoders)) |> Map.new() end
    else
      fn values -> zip_decode(values, decoders) end
    end
  end

  defp row_fields(%{"arguments" => arguments}) when is_list(arguments) do
    Enum.map(arguments, &row_field/1)
  end

  defp row_fields(_signature), do: []

  defp row_field(%{"kind" => "NAMED_TYPE", "value" => %{"typeSignature" => signature} = field}) do
    {field_name(field), signature}
  end

  defp row_field(%{"kind" => "TYPE", "value" => signature}), do: {nil, signature}
  defp row_field(_argument), do: {nil, @unknown}

  defp field_name(%{"fieldName" => %{"name" => name}}) when is_binary(name), do: name
  defp field_name(_field), do: nil

  defp type_arguments(%{"arguments" => arguments}) when is_list(arguments) do
    Enum.flat_map(arguments, fn
      %{"kind" => "TYPE", "value" => signature} -> [signature]
      _other -> []
    end)
  end

  defp type_arguments(_signature), do: []

  defp type_signature(%{"typeSignature" => %{"rawType" => _raw} = signature}), do: signature
  defp type_signature(%{"type" => type}) when is_binary(type), do: signature_from_type(type)
  defp type_signature(_column), do: @unknown

  # Without a typeSignature there is only the display type, `"varchar(5)"` or
  # `"timestamp(3) with time zone"`. Dropping its parameters leaves the raw type, which
  # is enough for every scalar — a parameterised one keeps no element types to recurse
  # into.
  defp signature_from_type(type) do
    raw =
      type
      |> String.replace(~r/\(.*\)/, "")
      |> String.replace(~r/\s+/, " ")
      |> String.trim()

    %{"rawType" => raw, "arguments" => []}
  end

  # Trino sends a double as a JSON number, except for the three values JSON cannot
  # express, which arrive as strings.
  defp to_float(value) when is_float(value), do: value
  defp to_float(value) when is_integer(value), do: value / 1
  defp to_float("Infinity"), do: :infinity
  defp to_float("-Infinity"), do: :negative_infinity
  defp to_float("NaN"), do: :nan
  defp to_float(value), do: value

  defp to_binary(value) when is_binary(value) do
    case Base.decode64(value) do
      {:ok, binary} -> binary
      :error -> value
    end
  end

  defp to_binary(value), do: value

  defp to_date(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _reason} -> value
    end
  end

  defp to_date(value), do: value

  defp to_time(value) when is_binary(value) do
    case Time.from_iso8601(value) do
      {:ok, time} -> time
      {:error, _reason} -> value
    end
  end

  defp to_time(value), do: value

  # Trino separates date and time with a space where ISO 8601 wants a T.
  defp to_naive_date_time(value) when is_binary(value) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, naive} -> naive
      {:error, _reason} -> value
    end
  end

  defp to_naive_date_time(value), do: value

  # `2026-09-24 12:34:56.789 +01:00`, or the same with a zone id in place of the offset.
  defp to_date_time(value) when is_binary(value) do
    case String.split(value, " ") do
      [date, time, zone] -> to_date_time(date <> "T" <> time, zone, value)
      _other -> value
    end
  end

  defp to_date_time(value), do: value

  defp to_date_time(stamp, zone, original) do
    case offset(zone) do
      nil -> original
      suffix -> from_iso8601(stamp <> suffix, original)
    end
  end

  defp offset(zone) when zone in ~w(UTC Z), do: "Z"
  defp offset(<<sign, _rest::binary>> = zone) when sign in [?+, ?-], do: zone
  defp offset(_zone), do: nil

  defp from_iso8601(stamp, original) do
    case DateTime.from_iso8601(stamp) do
      {:ok, date_time, _offset} -> date_time
      {:error, _reason} -> original
    end
  end
end
