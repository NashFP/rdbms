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

        row_set = TinyRdbms.run_query(database, query)
        columns = RowSet.columns(row_set)
        actual_answer = RowSet.rows(row_set)

        # Parse expected answer. Note that the last step here relies on type
        # information obtained by actually running the query. Since the CSV
        # file doesn't say anything about the column types, we can't check them.
        expected_answer =
          Regex.scan(~r/    (.*\n)/, answer_str, capture: :all_but_first)
          |> Enum.map(fn([line]) -> line end)
          |> CSV.decode()
          |> Enum.map(fn row -> Columns.row_from_strings(columns, row) end)

        assert actual_answer == expected_answer
      end
    end
  end
end

defmodule SqlTest do
  use ExUnit.Case
  doctest SqlParser
  doctest SqlValue
  doctest SqlExpr

  test "boolean expression compilation" do
    row = quote(do: var!(row))

    # This should produce a quoted Elixir expression, effectively the same as
    # `quote(do: SqlValue.equals?(elem(row, 0), 256))`.
    q = SqlExpr.compile_expr(
      Columns.new([{0, "Album", "AlbumId", :int}, {1, "Album", "Title", :str}]),
      row,
      {:=, {:identifier, "AlbumId"}, {:number, 256}})

    {result, _} = Code.eval_quoted(q, [row: {256, "Speak of the Devil"}])
    assert result == true

    {result, _} = Code.eval_quoted(q, [row: {238, "War"}])
    assert result == false
  end
end

