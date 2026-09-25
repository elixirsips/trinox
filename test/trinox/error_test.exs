defmodule Trinox.ErrorTest do
  use ExUnit.Case, async: true

  alias Trinox.Error

  doctest Trinox.Error

  describe "from_page/2" do
    test "keeps every field Trino sent, and the query id" do
      page_error = %{
        "message" => "line 1:15: Table 'mock.default.boom' does not exist",
        "errorCode" => 44,
        "errorName" => "TABLE_NOT_FOUND",
        "errorType" => "USER_ERROR"
      }

      error = Error.from_page(page_error, "20260923_120000_00000_mock")

      assert error.message == "line 1:15: Table 'mock.default.boom' does not exist"
      assert error.error_code == 44
      assert error.error_name == "TABLE_NOT_FOUND"
      assert error.error_type == "USER_ERROR"
      assert error.query_id == "20260923_120000_00000_mock"
    end

    test "stands in for a message the coordinator did not send" do
      error = Error.from_page(%{"errorName" => "GENERIC_INTERNAL_ERROR"}, nil)

      assert Exception.message(error) =~ "without a message"
      assert error.error_name == "GENERIC_INTERNAL_ERROR"
      assert error.error_code == nil
      assert error.query_id == nil
    end
  end

  test "is raisable with a message of its own" do
    assert_raise Error, "cursors are not supported", fn ->
      raise Error, "cursors are not supported"
    end
  end
end
