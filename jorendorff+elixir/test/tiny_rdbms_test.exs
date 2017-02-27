defmodule TinyRdbmsTest do
  use ExUnit.Case
  doctest TinyRdbms

  test "select examples" do
    tables_dir = Path.expand(Path.join(__DIR__, "../../tests/tables"))
    database = TinyRdbms.load(tables_dir)

    dir = Path.expand(Path.join(__DIR__, "../../tests/select"))
    for f <- File.ls!(dir) do
      if String.match?(f, ~r/^[0-9][0-9]-.*\.md$/) do
        IO.puts(f)
        text = File.read!(Path.join(dir, f))
        [query, answer_str] = Regex.run(
          ~r/^.*\n\n## Query\n\n((?:    .*\n)+)\n## Answer\n\n((?:    .*\n)*)$/,
          text,
          capture: :all_but_first)

        expected_answer =
          Regex.scan(~r/    (.*\n)/, answer_str, capture: :all_but_first) |>
          Enum.map(fn([line]) -> line end) |>
          CSV.decode() |>
          Enum.to_list()

        actual_answer = TinyRdbms.run_query(database, query)

        assert actual_answer == expected_answer
      end
    end
  end
end

defmodule SqlTest do
  use ExUnit.Case
  doctest Sql
end
