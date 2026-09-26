defmodule Trinox.Statement do
  @moduledoc """
  Trino's submit-and-poll control flow, on top of `Trinox.HTTP`.

  Running a statement is not one request: the `POST` to `/v1/statement` only queues the
  query and hands back a `nextUri`. The client then follows `nextUri` — each `GET`
  returns another page, carrying `columns` once and `data` as rows become available —
  until a page arrives with no `nextUri` (the query finished) or with an `error` (it
  failed).

  This module does that walk and nothing else: it returns the raw decoded JSON pages in
  the order they arrived, along with the response headers that came with them. Turning
  the pages into a result is `Trinox.ResultDecoder`'s job, and reading the session headers
  is `Trinox.Session`'s.

  A failed query is *not* an error here — it is a normal terminal page that happens to
  carry an `"error"` key, and the caller decides what to make of it. The `{:error, ...}`
  return means the conversation itself broke down: a transport failure, an HTTP status
  Trino shouldn't have sent, or a body that isn't JSON.

  Polling blocks the calling process. That is intended: Trino's `nextUri` `GET` is
  itself a long poll, so there is nothing to back off from, and `Trinox.Protocol.execute/3`
  already runs in the process that owns the connection.

  ## Retrying

  A coordinator with nothing spare to give answers `502`, `503` or `504` and expects to be
  asked again — the official Trino clients all treat those as "come back shortly", not as
  failure. So does this: the same request is re-sent, waiting #{50}ms and doubling up to
  #{1_000}ms, for `:max_attempts` tries in all. Re-sending a `POST` is safe precisely
  because a rejected one queued nothing, so there is no query to duplicate.

  Any other status is Trino saying something a retry will not change, and is returned.

  ## Deadlines

  A query that never finishes would otherwise be polled forever, holding its connection
  against every later caller. `:deadline` ends that: past it, polling stops, the query is
  cancelled with a `DELETE` of its `nextUri` — which is how Trino is told to stop work —
  and the run returns a `Trinox.Error`. The connection itself is untouched and usable for
  the next query, which is the whole point of cancelling rather than hanging up.
  """

  alias Trinox.Error
  alias Trinox.HTTP

  @statement_path "/v1/statement"
  @default_poll_interval_ms 0

  # The statuses Trino uses to mean "busy, ask again" rather than "no".
  @retry_statuses [502, 503, 504]
  @default_max_attempts 5
  @retry_backoff_min_ms 50
  @retry_backoff_max_ms 1_000

  # A cancel is a courtesy to the coordinator on the way out; nobody is waiting on it.
  @cancel_timeout_ms 5_000

  @typedoc "One decoded JSON page as Trino sent it."
  @type page :: map()

  # What it takes to send one request again: everything that does not change across the
  # pages of a single run.
  @typep context :: %{headers: HTTP.headers(), opts: keyword()}
  @typep request :: {String.t(), String.t(), iodata() | nil}

  @doc """
  Submits `statement` and polls until the query reaches a terminal page.

  `headers` are sent on the initial `POST` and on every poll. Options:

    * `:poll_interval_ms` — wait this long before each poll (default `0`; Trino's
      `nextUri` already long-polls server-side).
    * `:receive_timeout` — per-request, passed to `Trinox.HTTP.request/6`. A `:deadline`
      caps it, since no single request may outlive the query it belongs to.
    * `:max_attempts` — how many times to send one request before giving up on a
      `502`/`503`/`504` (default `#{@default_max_attempts}`; `1` disables retrying).
    * `:deadline` — an absolute `System.monotonic_time(:millisecond)` after which the
      query is cancelled rather than polled again (default `:infinity`).

  Returns every page, starting with the `POST` response, and every page's response
  headers concatenated in the order they arrived. Concatenating them loses nothing that
  `Trinox.Session.apply_response_headers/2` needs: it folds headers in order, so folding
  the whole run at once says exactly what folding page by page would have.
  """
  @spec run(HTTP.conn(), String.t(), HTTP.headers(), keyword()) ::
          {:ok, HTTP.conn(), [page()], HTTP.headers()} | {:error, HTTP.conn(), Exception.t()}
  def run(conn, statement, headers, opts) do
    context = %{headers: headers, opts: opts}

    attempt(conn, context, {"POST", @statement_path, statement}, [], 1)
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

  @spec attempt(HTTP.conn(), context(), request(), list(), pos_integer()) ::
          {:ok, HTTP.conn(), [page()], HTTP.headers()} | {:error, HTTP.conn(), Exception.t()}
  defp attempt(conn, context, request, pages, tries) do
    if expired?(context.opts) do
      expire(conn, context, request)
    else
      send_request(conn, context, request, pages, tries)
    end
  end

  defp send_request(conn, context, {method, path, body} = request, pages, tries) do
    case HTTP.request(conn, method, path, context.headers, body, request_opts(context.opts)) do
      {:ok, conn, response} -> received(conn, context, request, pages, tries, response)
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  defp received(conn, context, _request, pages, _tries, %{status: status} = response)
       when status in 200..299 do
    case Jason.decode(response.body) do
      {:ok, page} -> advance(conn, context, {page, response.headers}, pages)
      {:error, reason} -> {:error, conn, reason}
    end
  end

  defp received(conn, context, request, pages, tries, %{status: status} = response)
       when status in @retry_statuses do
    retry(conn, context, request, pages, tries, response)
  end

  defp received(conn, _context, request, _pages, _tries, response) do
    {:error, conn, status_error(response, request)}
  end

  defp retry(conn, context, request, pages, tries, response) do
    if tries >= max_attempts(context.opts) do
      {:error, conn, status_error(response, request)}
    else
      # Capped by whatever the deadline has left, so backing off cannot outlast the query.
      Process.sleep(cap(retry_backoff(tries), remaining(context.opts)))

      attempt(conn, context, request, pages, tries + 1)
    end
  end

  # Pages are accumulated with the headers they arrived with, and split apart at the end,
  # so that a caller threading a session gets the headers of every page rather than only
  # the last one's.
  defp advance(conn, context, {page, _response_headers} = arrival, pages) do
    pages = [arrival | pages]

    if terminal?(page) do
      finish(conn, pages)
    else
      do_poll(conn, context, page["nextUri"], pages)
    end
  end

  defp finish(conn, pages) do
    {pages, headers} = pages |> Enum.reverse() |> Enum.unzip()
    {:ok, conn, pages, Enum.concat(headers)}
  end

  # The wait happens before the deadline is looked at, because it is part of the time the
  # query was given; `attempt/5` is what notices the deadline has gone.
  defp do_poll(conn, context, next_uri, pages) do
    wait(context.opts)

    attempt(conn, context, {"GET", next_path(next_uri), nil}, pages, 1)
  end

  # A `POST` that ran out of time queued nothing, so there is nothing to call off. A poll
  # that ran out of time is a query still working for a caller who has stopped waiting.
  defp expire(conn, _context, {"POST", _path, _body}), do: {:error, conn, timeout_error()}
  defp expire(conn, context, {"GET", path, _body}), do: cancel(conn, context, path)

  # Trino stops a running query when its `nextUri` is `DELETE`d. The outcome is of no
  # interest — the query is being abandoned either way, and a coordinator that will not
  # take the cancel does not change that — but the connection it leaves behind is, so it
  # is threaded out for the next query to use.
  defp cancel(conn, context, path) do
    {_outcome, conn, _rest} =
      HTTP.request(conn, "DELETE", path, context.headers, nil,
        receive_timeout: @cancel_timeout_ms
      )

    {:error, conn, timeout_error()}
  end

  # The nextUri is absolute, but it points back at the coordinator we are already
  # connected to, so only its path and query are of use to us.
  defp next_path(next_uri) do
    case URI.parse(next_uri) do
      %URI{path: path, query: nil} -> path
      %URI{path: path, query: query} -> path <> "?" <> query
    end
  end

  # Never sleep past the deadline: the wait between polls is a throttle, not a reason to
  # miss the moment the caller stopped waiting.
  defp wait(opts) do
    interval = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)

    case cap(interval, remaining(opts)) do
      0 -> :ok
      milliseconds -> Process.sleep(milliseconds)
    end
  end

  defp request_opts(opts) do
    case remaining(opts) do
      :infinity ->
        Keyword.take(opts, [:receive_timeout])

      remaining ->
        [receive_timeout: cap(Keyword.get(opts, :receive_timeout, remaining), remaining)]
    end
  end

  defp max_attempts(opts), do: Keyword.get(opts, :max_attempts, @default_max_attempts)

  defp retry_backoff(tries) do
    min(@retry_backoff_min_ms * 2 ** (tries - 1), @retry_backoff_max_ms)
  end

  defp cap(milliseconds, :infinity), do: milliseconds
  defp cap(milliseconds, remaining), do: min(milliseconds, remaining)

  defp expired?(opts), do: remaining(opts) == 0

  defp remaining(opts) do
    case Keyword.get(opts, :deadline, :infinity) do
      :infinity -> :infinity
      deadline -> max(deadline - System.monotonic_time(:millisecond), 0)
    end
  end

  defp timeout_error do
    %Error{message: "Trinox cancelled the query: it did not finish within its :timeout"}
  end

  defp status_error(%{status: status, body: body}, {method, path, _body}) do
    %RuntimeError{
      message:
        "Trino answered #{method} #{path} with HTTP #{status}: #{String.slice(body, 0, 500)}"
    }
  end
end
