defmodule Trinox.Statement do
  @moduledoc """
  Trino's submit-and-poll control flow, on top of `Trinox.HTTP`.

  Running a statement is not one request: the `POST` to `/v1/statement` only queues the
  query and hands back a `nextUri`. The client then follows `nextUri` — each `GET`
  returns another page, carrying `columns` once and `data` as rows become available —
  until a page arrives with no `nextUri` (the query finished) or with an `error` (it
  failed).

  This module does that walk and nothing else: it returns the raw decoded JSON pages in
  the order they arrived. Turning them into a result is `Trinox.ResultDecoder`'s job,
  and reading the `X-Trino-Set-*` response headers is `Trinox.Session`'s.

  A failed query is *not* an error here — it is a normal terminal page that happens to
  carry an `"error"` key, and the caller decides what to make of it. The `{:error, ...}`
  return means the conversation itself broke down: a transport failure, an HTTP status
  Trino shouldn't have sent, or a body that isn't JSON.

  Polling blocks the calling process. That is intended: Trino's `nextUri` `GET` is
  itself a long poll, so there is nothing to back off from, and `handle_execute/4`
  already runs in the connection process.
  """

  alias Trinox.HTTP

  @statement_path "/v1/statement"
  @default_poll_interval_ms 0

  @typedoc "One decoded JSON page as Trino sent it."
  @type page :: map()

  @doc """
  Submits `statement` and polls until the query reaches a terminal page.

  `headers` are sent on the initial `POST` and on every poll. Options:

    * `:poll_interval_ms` — wait this long before each poll (default `0`; Trino's
      `nextUri` already long-polls server-side).
    * `:receive_timeout` — per-request, passed to `Trinox.HTTP.request/6`.

  Returns every page, starting with the `POST` response.
  """
  @spec run(HTTP.conn(), String.t(), HTTP.headers(), keyword()) ::
          {:ok, HTTP.conn(), [page()]} | {:error, HTTP.conn(), Exception.t()}
  def run(conn, statement, headers, opts) do
    case HTTP.request(conn, "POST", @statement_path, headers, statement, request_opts(opts)) do
      {:ok, conn, response} -> collect(conn, response, headers, opts, [])
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  @doc """
  Whether a page ends the query.

  True when Trino stopped handing out a `nextUri`, and also when the page reports an
  `error` — a failed query must not be polled further even if a `nextUri` is present.
  """
  @spec terminal?(page()) :: boolean()
  def terminal?(page) do
    not Map.has_key?(page, "nextUri") or Map.has_key?(page, "error")
  end

  defp collect(conn, %{status: status} = response, headers, opts, pages)
       when status in 200..299 do
    case Jason.decode(response.body) do
      {:ok, page} -> advance(conn, page, headers, opts, pages)
      {:error, reason} -> {:error, conn, reason}
    end
  end

  defp collect(conn, response, _headers, _opts, _pages) do
    {:error, conn, status_error(response)}
  end

  defp advance(conn, page, headers, opts, pages) do
    pages = [page | pages]

    if terminal?(page) do
      {:ok, conn, Enum.reverse(pages)}
    else
      do_poll(conn, page["nextUri"], headers, opts, pages)
    end
  end

  defp do_poll(conn, next_uri, headers, opts, pages) do
    wait(opts)

    case HTTP.request(conn, "GET", next_path(next_uri), headers, nil, request_opts(opts)) do
      {:ok, conn, response} -> collect(conn, response, headers, opts, pages)
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  # The nextUri is absolute, but it points back at the coordinator we are already
  # connected to, so only its path and query are of use to us.
  defp next_path(next_uri) do
    case URI.parse(next_uri) do
      %URI{path: path, query: nil} -> path
      %URI{path: path, query: query} -> path <> "?" <> query
    end
  end

  defp wait(opts) do
    case Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms) do
      0 -> :ok
      milliseconds -> Process.sleep(milliseconds)
    end
  end

  defp request_opts(opts), do: Keyword.take(opts, [:receive_timeout])

  defp status_error(%{status: status, body: body}) do
    %RuntimeError{
      message:
        "Trino answered #{@statement_path} with HTTP #{status}: #{String.slice(body, 0, 500)}"
    }
  end
end
