defmodule Trinox.Result do
  @moduledoc """
  The outcome of one finished Trino query.

  Trino hands a query's rows back over many HTTP pages; this is what those pages add up
  to once `Trinox.ResultDecoder` has folded them together. The shape mirrors
  `Postgrex.Result` — `columns`, `rows`, `num_rows` — plus the two things that are
  Trino's own: the coordinator's `query_id`, and the `stats` map it reports alongside
  every page.

  Rows are lists in column order, holding Elixir terms rather than the raw JSON Trino
  sent; `Trinox.ResultDecoder` documents the type mapping.
  """

  defstruct columns: [], rows: [], num_rows: 0, query_id: nil, stats: %{}

  @type t :: %__MODULE__{
          columns: [String.t()],
          rows: [[term()]],
          num_rows: non_neg_integer(),
          query_id: String.t() | nil,
          stats: map()
        }
end
