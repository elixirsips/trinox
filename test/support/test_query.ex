defmodule Trinox.TestQuery do
  @moduledoc """
  The smallest query that `DBConnection.execute/4` will accept.

  `Trinox.Protocol.handle_execute/4` asks a query for nothing but its `:statement`, but
  `DBConnection` itself insists on a `DBConnection.Query` implementation to encode
  params and decode results with. `Trinox.Query` takes that over when the public API
  lands; until then this is what lets a test drive the driver through `DBConnection`
  rather than by calling the callbacks directly.
  """

  defstruct [:statement]

  @type t :: %__MODULE__{statement: String.t()}

  defimpl DBConnection.Query do
    def parse(query, _opts), do: query
    def describe(query, _opts), do: query
    def encode(_query, params, _opts), do: params
    def decode(_query, result, _opts), do: result
  end
end
