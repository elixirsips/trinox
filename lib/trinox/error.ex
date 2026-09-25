defmodule Trinox.Error do
  @moduledoc """
  An error Trino reported, or one the driver reports in Trino's name.

  A query failing server-side is not an HTTP failure: Trino answers `200` with a terminal
  page carrying an `error` object, and this exception is that object. `error_name` is the
  stable thing to match on (`"TABLE_NOT_FOUND"`), `error_code` is its numeric twin, and
  `error_type` says whose fault it was — `"USER_ERROR"`, `"INTERNAL_ERROR"`,
  `"INSUFFICIENT_RESOURCES"`. `query_id` is the coordinator's id for the query, which is
  what a Trino admin needs to find it in the web UI or the query log.

  The same struct carries the driver's own refusals — cursors, which `Trinox.Protocol`
  does not implement — and those have nothing but a `message`.
  """

  defexception [:message, :error_code, :error_name, :error_type, :query_id]

  @type t :: %__MODULE__{
          message: String.t(),
          error_code: integer() | nil,
          error_name: String.t() | nil,
          error_type: String.t() | nil,
          query_id: String.t() | nil
        }

  @fallback_message "Trino reported an error without a message"

  @doc """
  Builds an error from the `error` object of a Trino page, and the query's id.

  Every field but the message is optional — an error object is whatever the coordinator
  chose to send, and a missing `errorName` is not worth failing over on a path that is
  already reporting a failure.

      iex> error = %{
      ...>   "message" => "line 1:15: Table 'mock.default.boom' does not exist",
      ...>   "errorCode" => 44,
      ...>   "errorName" => "TABLE_NOT_FOUND",
      ...>   "errorType" => "USER_ERROR"
      ...> }
      iex> Trinox.Error.from_page(error, "20260923_120000_00000_mock")
      %Trinox.Error{
        message: "line 1:15: Table 'mock.default.boom' does not exist",
        error_code: 44,
        error_name: "TABLE_NOT_FOUND",
        error_type: "USER_ERROR",
        query_id: "20260923_120000_00000_mock"
      }
  """
  @spec from_page(map(), String.t() | nil) :: t()
  def from_page(error, query_id) do
    %__MODULE__{
      message: Map.get(error, "message") || @fallback_message,
      error_code: error["errorCode"],
      error_name: error["errorName"],
      error_type: error["errorType"],
      query_id: query_id
    }
  end
end
