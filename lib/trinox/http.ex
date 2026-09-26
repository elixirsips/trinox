defmodule Trinox.HTTP do
  @moduledoc """
  A thin synchronous wrapper around `Mint.HTTP`.

  This module is Trino-agnostic: it knows nothing about statements, JSON or session
  headers. It opens a connection, sends one request on it and blocks until the whole
  response has arrived, which is what `Trinox.Protocol` needs — it runs inside the
  connection process, and `Trinox.query/3` is a blocking call.

  Connections are opened in `:passive` mode so that response data never lands in the
  owner's mailbox and `request/6` can drive `Mint.HTTP.recv/3` itself.

      {:ok, conn} = Trinox.HTTP.connect(:https, "trino.example.com", 8443)
      {:ok, conn, response} = Trinox.HTTP.request(conn, "GET", "/v1/info", [], nil)
      response.status #=> 200
  """

  @default_connect_timeout 15_000
  @default_receive_timeout 30_000

  @type conn :: Mint.HTTP.t()
  @type headers :: [{String.t(), String.t()}]
  @type response :: %{status: non_neg_integer(), headers: headers(), body: binary()}

  @doc """
  Opens a connection to `hostname`.

  `opts` are `Mint.HTTP.connect/4` options — `:transport_opts` in particular carries
  the TLS configuration — with two additions:

    * `:mode` is always forced to `:passive`.
    * `:connect_timeout` (default `#{@default_connect_timeout}` ms) is a convenience
      for `transport_opts: [timeout: ...]`.

  For `:https`, the CA bundle shipped by `CAStore` is used unless the caller supplies
  its own `:cacerts` or `:cacertfile`, so verification also works on systems without
  an OS CA store.
  """
  @spec connect(Mint.Types.scheme(), String.t(), :inet.port_number(), keyword()) ::
          {:ok, conn()} | {:error, Mint.Types.error()}
  def connect(scheme, hostname, port, opts \\ []) do
    {connect_timeout, opts} = Keyword.pop(opts, :connect_timeout, @default_connect_timeout)

    transport_opts =
      opts
      |> Keyword.get(:transport_opts, [])
      |> Keyword.put_new(:timeout, connect_timeout)
      |> put_ca_certs(scheme)

    Mint.HTTP.connect(
      scheme,
      hostname,
      port,
      Keyword.merge(opts, mode: :passive, transport_opts: transport_opts)
    )
  end

  @doc """
  Sends one request and blocks until the full response has been received.

  `body` is `nil` for requests without one. Supports `:receive_timeout`
  (default `#{@default_receive_timeout}` ms), which applies to each `Mint.HTTP.recv/3`
  call rather than to the response as a whole.

  The returned connection must be used for any further request — Mint connections are
  immutable, and the one passed in is stale afterwards.
  """
  @spec request(conn(), String.t(), String.t(), headers(), iodata() | nil, keyword()) ::
          {:ok, conn(), response()} | {:error, conn(), Mint.Types.error()}
  def request(conn, method, path, headers, body, opts \\ []) do
    timeout = Keyword.get(opts, :receive_timeout, @default_receive_timeout)

    case Mint.HTTP.request(conn, method, path, headers, body) do
      {:ok, conn, ref} -> recv_response(conn, ref, timeout, %{status: nil, headers: [], body: []})
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  @doc "Closes the connection."
  @spec close(conn()) :: {:ok, conn()}
  def close(conn), do: Mint.HTTP.close(conn)

  @doc "Whether the connection is still usable."
  @spec open?(conn()) :: boolean()
  def open?(conn), do: Mint.HTTP.open?(conn)

  defp put_ca_certs(transport_opts, :https) do
    if Keyword.has_key?(transport_opts, :cacerts) or Keyword.has_key?(transport_opts, :cacertfile) do
      transport_opts
    else
      Keyword.put(transport_opts, :cacertfile, CAStore.file_path())
    end
  end

  defp put_ca_certs(transport_opts, :http), do: transport_opts

  defp recv_response(conn, ref, timeout, acc) do
    case Mint.HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} -> handle_responses(conn, ref, timeout, responses, acc)
      {:error, conn, reason, _responses} -> {:error, conn, reason}
    end
  end

  defp handle_responses(conn, ref, timeout, responses, acc) do
    case apply_responses(responses, ref, acc) do
      {:cont, acc} -> recv_response(conn, ref, timeout, acc)
      {:done, response} -> {:ok, conn, response}
      {:error, reason} -> {:error, conn, reason}
    end
  end

  defp apply_responses([], _ref, acc), do: {:cont, acc}

  defp apply_responses([{:status, ref, status} | rest], ref, acc) do
    apply_responses(rest, ref, %{acc | status: status})
  end

  defp apply_responses([{:headers, ref, headers} | rest], ref, acc) do
    apply_responses(rest, ref, %{acc | headers: acc.headers ++ headers})
  end

  defp apply_responses([{:data, ref, data} | rest], ref, acc) do
    apply_responses(rest, ref, %{acc | body: [data | acc.body]})
  end

  defp apply_responses([{:done, ref} | _rest], ref, acc) do
    {:done, %{acc | body: acc.body |> Enum.reverse() |> IO.iodata_to_binary()}}
  end

  defp apply_responses([{:error, ref, reason} | _rest], ref, _acc), do: {:error, reason}
end
