defmodule Trinox.Session do
  @moduledoc """
  The session state a connection carries between requests, and the headers that carry it.

  Trino keeps no session on the socket the way Postgres does. Every request stands on its
  own, so the client has to say who it is and where it is pointed each time. And when a
  statement changes the session — `USE tpch.sf1`, `SET SESSION query_max_run_time = '10m'`,
  `PREPARE`, `START TRANSACTION` — the coordinator does not remember that either. It
  answers with an `X-Trino-Set-*` or `X-Trino-Added-*` header, and the client is expected
  to fold that into its own state and echo the result back from then on.

  This module is both halves of that loop and nothing else. `build_headers/2` renders a
  session into request headers; `apply_response_headers/2` folds a response's headers back
  into the session. Both are pure — `Trinox.Protocol` owns the struct, and
  `Trinox.Statement` threads it through every page of a query, not just the first.

  ## What is session state and what is not

  The struct holds only what the coordinator can change under us: the catalog, schema and
  SQL path, the session properties, the prepared statements, and the id of the transaction
  in progress. Who we are is not in it — Trino has no `X-Trino-Set-User` — and neither are
  the source, client info or time zone, which describe the client rather than the session.
  Those come in through `build_headers/2`'s options.

  ## Encoding

  Property values and prepared statement bodies are URL-encoded on the way out and decoded
  on the way back, which is what lets a value hold the `,` and `=` that separate entries.
  Names are not encoded; Trino restricts them to identifiers. A coordinator may send one
  header per entry or fold several into one comma-separated header — both are read the
  same way, which is safe precisely because any comma inside a value has been encoded away
  by then.
  """

  alias Trinox.HTTP

  defstruct catalog: nil,
            schema: nil,
            path: nil,
            properties: %{},
            prepared_statements: %{},
            transaction_id: nil

  @type t :: %__MODULE__{
          catalog: String.t() | nil,
          schema: String.t() | nil,
          path: String.t() | nil,
          properties: %{optional(String.t()) => String.t()},
          prepared_statements: %{optional(String.t()) => String.t()},
          transaction_id: String.t() | nil
        }

  @doc """
  Renders `session` as the headers to send with a request.

  Options:

    * `:user` — required; sent as `X-Trino-User` and used as the Basic-auth user.
    * `:password` — when given, adds `Authorization: Basic ...`. Trino only accepts
      Basic auth over TLS, so this belongs with `scheme: :https`.
    * `:source` — sent as `X-Trino-Source`. This is what an operator sees in the Trino UI
      and the query log next to a query, so a query without one is anonymous.
    * `:client_info` — sent as `X-Trino-Client-Info`, for whatever else is worth recording
      about the caller.
    * `:time_zone` — sent as `X-Trino-Time-Zone`, an IANA name like `"Europe/Berlin"`.
      Without it the coordinator falls back to its own default, which means the same query
      can answer differently against two clusters.

  Everything is only sent when set, since an empty `X-Trino-Catalog` is not the same to
  Trino as no catalog at all.

      iex> session = %Trinox.Session{catalog: "tpch", schema: "sf1"}
      iex> Trinox.Session.build_headers(session, user: "alice", password: "s3cret")
      [
        {"authorization", "Basic YWxpY2U6czNjcmV0"},
        {"x-trino-user", "alice"},
        {"x-trino-catalog", "tpch"},
        {"x-trino-schema", "sf1"}
      ]
  """
  @spec build_headers(t(), keyword()) :: HTTP.headers()
  def build_headers(%__MODULE__{} = session, opts) do
    user = Keyword.fetch!(opts, :user)

    headers = [
      {"authorization", basic_auth(user, Keyword.get(opts, :password))},
      {"x-trino-user", user},
      {"x-trino-source", Keyword.get(opts, :source)},
      {"x-trino-client-info", Keyword.get(opts, :client_info)},
      {"x-trino-time-zone", Keyword.get(opts, :time_zone)},
      {"x-trino-catalog", session.catalog},
      {"x-trino-schema", session.schema},
      {"x-trino-path", session.path},
      {"x-trino-session", encode_entries(session.properties)},
      {"x-trino-prepared-statement", encode_entries(session.prepared_statements)},
      {"x-trino-transaction-id", session.transaction_id}
    ]

    Enum.reject(headers, fn {_name, value} -> is_nil(value) end)
  end

  @doc """
  Folds a response's session headers into `session`.

  Reads `X-Trino-Set-Catalog`, `X-Trino-Set-Schema` and `X-Trino-Set-Path`;
  `X-Trino-Set-Session` and `X-Trino-Clear-Session`; `X-Trino-Added-Prepare` and
  `X-Trino-Deallocated-Prepare`; and `X-Trino-Started-Transaction-Id` and
  `X-Trino-Clear-Transaction-Id`. Headers are applied in the order they arrived, so a
  later one wins over an earlier one for the same thing. Anything else in `headers` is
  ignored.

      iex> headers = [
      ...>   {"x-trino-set-catalog", "tpch"},
      ...>   {"x-trino-set-session", "query_max_run_time=10m"}
      ...> ]
      iex> Trinox.Session.apply_response_headers(%Trinox.Session{}, headers)
      %Trinox.Session{catalog: "tpch", properties: %{"query_max_run_time" => "10m"}}
  """
  @spec apply_response_headers(t(), HTTP.headers()) :: t()
  def apply_response_headers(%__MODULE__{} = session, headers) do
    Enum.reduce(headers, session, &apply_header/2)
  end

  defp apply_header({name, value}, session) do
    apply_header(String.downcase(name), value, session)
  end

  defp apply_header("x-trino-set-catalog", value, session), do: %{session | catalog: value}
  defp apply_header("x-trino-set-schema", value, session), do: %{session | schema: value}
  defp apply_header("x-trino-set-path", value, session), do: %{session | path: value}

  defp apply_header("x-trino-set-session", value, session) do
    put_entries(session, :properties, value)
  end

  defp apply_header("x-trino-clear-session", value, session) do
    drop_entries(session, :properties, value)
  end

  # A PREPARE arrives as `name=<url-encoded sql>` and has to be echoed back on every later
  # request, since the coordinator is not holding it for us either.
  defp apply_header("x-trino-added-prepare", value, session) do
    put_entries(session, :prepared_statements, value)
  end

  defp apply_header("x-trino-deallocated-prepare", value, session) do
    drop_entries(session, :prepared_statements, value)
  end

  # `START TRANSACTION` is answered with the id, which every request from then on must
  # carry for the coordinator to place it in the transaction; COMMIT and ROLLBACK end it.
  defp apply_header("x-trino-started-transaction-id", value, session) do
    %{session | transaction_id: value}
  end

  defp apply_header("x-trino-clear-transaction-id", _value, session) do
    %{session | transaction_id: nil}
  end

  defp apply_header(_name, _value, session), do: session

  defp put_entries(session, field, value) do
    Enum.reduce(entries(value), session, &put_entry(&2, field, &1))
  end

  defp put_entry(session, field, entry) do
    case String.split(entry, "=", parts: 2) do
      [name, value] -> put_named(session, field, String.trim(name), decode(value))
      [_nameless] -> session
    end
  end

  defp put_named(session, _field, "", _value), do: session

  defp put_named(session, field, name, value) do
    Map.update!(session, field, &Map.put(&1, name, value))
  end

  defp drop_entries(session, field, value) do
    Enum.reduce(entries(value), session, fn name, acc ->
      Map.update!(acc, field, &Map.delete(&1, name))
    end)
  end

  # Both halves of every pair are comma-separated lists, of `name=value` when setting and
  # of bare names when clearing.
  defp entries(value) do
    value
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  # `URI.decode_www_form/1` is lenient: a `%` that is not the start of a valid escape
  # survives as itself. That is the behaviour to want here — a value Trino encoded in a
  # way we did not expect is still closer to what it meant than a dropped entry.
  defp decode(value), do: URI.decode_www_form(String.trim(value))

  defp basic_auth(_user, nil), do: nil
  defp basic_auth(user, password), do: "Basic " <> Base.encode64(user <> ":" <> password)

  defp encode_entries(entries) when map_size(entries) == 0, do: nil

  # Sorted so the header a given session produces is always the same string, which keeps
  # it readable in a request log and comparable in a test.
  defp encode_entries(entries) do
    entries
    |> Enum.sort()
    |> Enum.map_join(",", fn {name, value} -> name <> "=" <> URI.encode_www_form(value) end)
  end
end
