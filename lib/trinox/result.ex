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

  ## Statements that change things rather than return them

  An `INSERT`, `DELETE`, `CREATE TABLE` or `USE` has no rows to give back, and reporting it
  as an empty result would lose the only thing it did say. `update_type` is Trino's name
  for what ran (`"INSERT"`, `"CREATE TABLE"`, ...) and `update_count` is how many rows it
  touched, where that is a meaningful question.

  `num_rows` is kept for what it says — how many rows came back — so it is `0` for these.
  The two are separate questions and a statement can answer both.
  """

  defstruct columns: [],
            rows: [],
            num_rows: 0,
            query_id: nil,
            update_type: nil,
            update_count: nil,
            stats: %{}

  @type t :: %__MODULE__{
          columns: [String.t()],
          rows: [[term()]],
          num_rows: non_neg_integer(),
          query_id: String.t() | nil,
          update_type: String.t() | nil,
          update_count: non_neg_integer() | nil,
          stats: map()
        }
end
