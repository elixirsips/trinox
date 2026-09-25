defmodule Trinox.Session do
  @moduledoc """
  The session state a connection carries between requests, and the headers that carry it.

  Trino keeps no session on the socket the way Postgres does. Every request stands on its
  own, so the client has to say who it is and where it is pointed each time:
  `X-Trino-User`, `X-Trino-Catalog`, `X-Trino-Schema` and `X-Trino-Session` go out with
  every request. And when a statement changes the session — `USE tpch.sf1`, `SET SESSION
  query_max_run_time = '10m'` — the coordinator does not remember that either. It answers
  with `X-Trino-Set-Catalog`, `X-Trino-Set-Schema`, `X-Trino-Set-Session` or
  `X-Trino-Clear-Session`, and the client is expected to fold those into its own state and
  echo the result back from then on.

  This module is both halves of that loop and nothing else. `build_headers/2` renders a
  session into request headers; `apply_response_headers/2` folds a response's headers back
  into the session. Both are pure — `Trinox.Protocol` owns the struct, and
  `Trinox.Statement` threads it through every page of a query, not just the first.

  The user is not part of the session: Trino has no `X-Trino-Set-User`, so who we are is
  connection config that `build_headers/2` takes in its options, while the catalog, schema
  and properties here are the parts the coordinator can change under us.

  ## Encoding

  Property values are URL-encoded on the way out and decoded on the way back, which is
  what lets a value hold the `,` and `=` that separate properties. Names are not encoded;
  Trino restricts them to identifiers. A coordinator may send one `X-Trino-Set-Session`
  header per property or fold several into one comma-separated header — both are read the
  same way, which is safe precisely because any comma inside a value has been encoded away
  by then.
  """

  alias Trinox.HTTP

  defstruct catalog: nil, schema: nil, properties: %{}

  @type t :: %__MODULE__{
          catalog: String.t() | nil,
          schema: String.t() | nil,
          properties: %{optional(String.t()) => String.t()}
        }

  @doc """
  Renders `session` as the headers to send with a request.

  Options:

    * `:user` — required; sent as `X-Trino-User` and used as the Basic-auth user.
    * `:password` — when given, adds `Authorization: Basic ...`. Trino only accepts
      Basic auth over TLS, so this belongs with `scheme: :https`.

  Catalog, schema and session properties are only sent when set, since an empty
  `X-Trino-Catalog` is not the same to Trino as no catalog at all.

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
      {"x-trino-catalog", session.catalog},
      {"x-trino-schema", session.schema},
      {"x-trino-session", properties_header(session.properties)}
    ]

    Enum.reject(headers, fn {_name, value} -> is_nil(value) end)
  end

  @doc """
  Folds a response's `X-Trino-Set-*` / `X-Trino-Clear-Session` headers into `session`.

  Headers are applied in the order they arrived, so a later one wins over an earlier one
  for the same catalog, schema or property. Anything else in `headers` is ignored.

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

  defp apply_header("x-trino-set-session", value, session) do
    Enum.reduce(entries(value), session, &set_property/2)
  end

  defp apply_header("x-trino-clear-session", value, session) do
    Enum.reduce(entries(value), session, &clear_property/2)
  end

  defp apply_header(_name, _value, session), do: session

  defp set_property(entry, session) do
    case String.split(entry, "=", parts: 2) do
      [name, value] -> put_property(session, String.trim(name), decode(value))
      [_nameless] -> session
    end
  end

  defp put_property(session, "", _value), do: session

  defp put_property(session, name, value) do
    %{session | properties: Map.put(session.properties, name, value)}
  end

  defp clear_property(name, session) do
    %{session | properties: Map.delete(session.properties, name)}
  end

  # Both session headers are comma-separated lists, of `name=value` when setting and of
  # bare names when clearing.
  defp entries(value) do
    value
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  # `URI.decode_www_form/1` is lenient: a `%` that is not the start of a valid escape
  # survives as itself. That is the behaviour to want here — a value Trino encoded in a
  # way we did not expect is still closer to what it meant than a dropped property.
  defp decode(value), do: URI.decode_www_form(String.trim(value))

  defp basic_auth(_user, nil), do: nil
  defp basic_auth(user, password), do: "Basic " <> Base.encode64(user <> ":" <> password)

  defp properties_header(properties) when map_size(properties) == 0, do: nil

  # Sorted so the header a given session produces is always the same string, which keeps
  # it readable in a request log and comparable in a test.
  defp properties_header(properties) do
    properties
    |> Enum.sort()
    |> Enum.map_join(",", fn {name, value} -> name <> "=" <> URI.encode_www_form(value) end)
  end
end
