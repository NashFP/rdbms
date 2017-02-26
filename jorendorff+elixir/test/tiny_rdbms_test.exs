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

  test "select examples" do
    dir = Path.expand(Path.join(__DIR__, "../../tests/select"))
    for f <- File.ls!(dir) do
      if String.match?(f, ~r/^[0-9][0-9]-.*\.md$/) do
        IO.puts(f)
        text = File.read!(Path.join(dir, f))
        [query, answer] = Regex.run(
          ~r/^.*\n\n## Query\n\n((?:    .*\n)+)\n## Answer\n\n((?:    .*\n)*)$/,
          text,
          capture: :all_but_first)

      end
    end
  end
end
