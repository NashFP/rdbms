defmodule TinyRdbmsTest do
  use ExUnit.Case
  doctest TinyRdbms

  test "the truth" do
    assert 1 + 1 == 2
  end
end

defmodule SqlTest do
  use ExUnit.Case
  doctest Sql
end
