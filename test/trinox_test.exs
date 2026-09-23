defmodule TrinoxTest do
  use ExUnit.Case
  doctest Trinox

  test "greets the world" do
    assert Trinox.hello() == :world
  end
end
