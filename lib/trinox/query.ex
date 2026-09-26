defmodule Trinox.Query do
  @moduledoc """
  One SQL statement on its way to Trino.

  Trino takes a statement as text and has no prepare step to describe it ahead of time,
  so this struct is barely more than the SQL it wraps. What earns it a module of its own
  is the `DBConnection.Query` implementation it carries: `DBConnection` encodes a query's
  parameters and decodes its result through that protocol, and refuses to run anything
  that does not implement it.

  `Trinox.query/3` builds one of these for you. Build it yourself to reach
  `DBConnection.execute/4`, `DBConnection.prepare_execute/4` and friends directly:

      DBConnection.execute(conn, %Trinox.Query{statement: "SELECT 1"}, [])

  ## Parameters

  There are none yet. Trino has prepared statements, but this milestone does not use
  them, so `encode/3` accepts an empty parameter list and raises on anything else rather
  than quietly dropping values a caller expected to be bound.

      iex> to_string(%Trinox.Query{statement: "SELECT 1"})
      "SELECT 1"
  """

  defstruct [:statement]

  @type t :: %__MODULE__{statement: String.t()}

  defimpl DBConnection.Query do
    alias Trinox.Query

    # Nothing to parse or describe: the statement is sent as text, and Trino says what
    # the columns are in the result rather than ahead of time.
    def parse(query, _opts), do: query
    def describe(query, _opts), do: query

    def encode(_query, [], _opts), do: []

    def encode(%Query{statement: statement}, params, _opts) do
      raise ArgumentError,
            "Trinox does not support query parameters yet, got #{inspect(params)} " <>
              "for #{inspect(statement)}"
    end

    # `Trinox.ResultDecoder` already decoded every value on the way out of the protocol.
    def decode(_query, result, _opts), do: result
  end

  defimpl String.Chars do
    def to_string(%Trinox.Query{statement: statement}), do: statement
  end
end
