defmodule RowSet do
  @moduledoc """
  Rows of data (tuples), plus column metadata and optional indexes.

  A RowSet may be either plain or grouped.
  If it's plain, it's basically a list of tuples, plus metadata.
  If it's grouped, it's a list of groups (lists of tuples), plus metadata.

  Plain RowSets can be converted to grouped RowSets using group_1() or group_by().
  Grouped RowSets can be converted to plain RowSets using aggregate().
  """

  def new(columns, rows, indexes \\ %{}) do
    {:RowSet, columns, rows, indexes}
  end

  def one() do
    new(Columns.new([]), [{}])
  end

  def columns({:RowSet, columns, _, _}), do: columns
  def rows({:RowSet, _, rows, _}), do: rows

  def load!(table_name, filename) do
    raw_rows = File.stream!(filename) |> CSV.decode() |> Enum.to_list
    columns = hd(raw_rows)
      |> Enum.with_index()
      |> Enum.map(fn {name, index} ->
           # hack: guess type from column name
           type = cond do
             String.ends_with?(name, "Id") -> :int
             String.ends_with?(name, "Price") -> :num
             String.ends_with?(name, "Total") -> :num
             true -> :str
           end
           {index, table_name, name, type}
         end)
      |> Columns.new()

    rows = tl(raw_rows)
      |> Enum.map(fn row -> Columns.row_from_strings(columns, row) end)

    with_indexes(new(columns, rows))
  end

  def apply_alias({:RowSet, columns, rows, indexes}, name) do
    new(Columns.apply_table_alias(columns, name), rows, indexes)
  end

  @doc """
  Dump a RowSet to stdout.
  """
  def inspect(row_set) do
    headings = Columns.names(columns(row_set))
    IO.inspect(headings)
    IO.puts("----")
    rows(row_set)
      |> Enum.map(&IO.inspect/1)
  end

  def product(rs1, rs2) do
    cond do
      rows(rs1) == [{}] -> rs2  # optimization: 1 * A = A
      rows(rs2) == [{}] -> rs1  # optimization: A * 1 = A
      true ->
        {:RowSet, cols1, rows1, _} = rs1
        {:RowSet, cols2, rows2, _} = rs2
        cols = Columns.concat(cols1, cols2)
        rows =
          for a <- rows1, b <- rows2 do
            List.to_tuple(Tuple.to_list(a) ++ Tuple.to_list(b))
          end
        new(cols, rows)
    end
  end

  @doc """
  Return a RowSet with the same columns and data as this one,
  plus an index for each integer column.
  """
  def with_indexes({:RowSet, columns, rows, indexes}) do
    indexes =
      Columns.list(columns)
      |> Enum.reduce(indexes, fn (col, acc) ->
          {i, _, _, ty} = col
          if ty == :int && !Map.has_key?(acc, i) do
            col_index = rows |> Enum.group_by(fn(row) -> elem(row, i) end)
            Map.put(acc, i, col_index)
          else
            acc
          end
        end)
      {:RowSet, columns, rows, indexes}
  end

  defp get_hash_index({:RowSet, columns, rows, indexes}, column_name) do
    column_index = Columns.get_index(columns, column_name)
    case indexes[column_name] do
      nil ->
        rows |> Enum.group_by(fn(row) -> elem(row, column_index) end)
      hash_index ->
        hash_index
    end
  end

  def join(rs1, col1, rs2, col2) do
    {:RowSet, cols1, rows1, _} = rs1
    {:RowSet, cols2, _, _} = rs2
    col1_index = Columns.get_index(cols1, col1)
    rs2 = get_hash_index(rs2, col2)
    cols = Columns.concat(cols1, cols2)
    rows =
      for a <- rows1, b <- Map.get(rs2, elem(a, col1_index), []) do
        List.to_tuple(Tuple.to_list(a) ++ Tuple.to_list(b))
      end
    new(cols, rows)
  end

  # Evaluate WHERE clause.
  def filter(row_set, true) do
    row_set
  end
  def filter({:RowSet, columns, rows, _}, cond_expr) do
    # -------------------------------------------------------------------------
    # Use only one of the next two lines (the other should be commented out)
    #f = fn (row) -> SqlExpr.eval(columns, row, cond_expr) end  # INTERPRETER
    f = SqlExpr.compile(columns, cond_expr)                    # COMPILER
    # -------------------------------------------------------------------------
    filtered_rows = Enum.filter(rows, fn(row) ->
      SqlValue.is_truthy?(f.(row))
    end)
    new(columns, filtered_rows)
  end

  # Filter to rows where the given column has the given value. This special
  # case of `filter()` is very fast, assuming the index already exists.
  def filter_by_index(row_set, column_name, value) do
    filtered_rows = Map.get(get_hash_index(row_set, column_name), value, [])
    new(columns(row_set), filtered_rows)
  end

  # Evaluate SELECT clause (projection).
  def map({:RowSet, columns, rows, _}, exprs) do
    selected_columns =
      exprs
      |> Enum.with_index()
      |> Enum.map(fn {expr, index} -> SqlExpr.column_info(expr, columns, index) end)
      |> Columns.new()

    projected_rows =
      Enum.map(rows, fn(row) ->
        # For each row, evaluate each selected expression.
        Enum.map(exprs, fn(expr) -> SqlExpr.eval(columns, row, expr) end)
        |> List.to_tuple()
      end)

    new(selected_columns, projected_rows)
  end

  # Evaluate SELECT clause on a grouped rowset.
  #
  # A plain map() can't use aggregate functions like COUNT and SUM; this can.
  def aggregate({:RowSet, columns, groups, _}, exprs) do
    selected_columns =
      exprs
      |> Enum.with_index()
      |> Enum.map(fn {expr, index} -> SqlExpr.column_info(expr, columns, index) end)
      |> Columns.new()

    projected_rows =
      for rows <- groups do
        Enum.map(exprs, fn(expr) -> SqlExpr.eval_aggregate(columns, rows, expr) end)
        |> List.to_tuple()
      end

    new(selected_columns, projected_rows)
  end

  # Apply ORDER BY clause to the result set.
  def order_by({:RowSet, columns, rows, _}, order) do
    sorted_rows = Enum.sort_by(rows, fn row ->
      Enum.map(order, fn expr ->
        SqlExpr.eval(columns, row, expr)
      end)
    end)
    new(columns, sorted_rows)
  end

  # Apply ORDER BY clause to the groups in a grouped result set.
  def order_groups_by({:RowSet, columns, groups, _}, order) do
    sorted_rows = Enum.sort_by(groups, fn group ->
      Enum.map(order, fn expr ->
        SqlExpr.eval_aggregate(columns, group, expr)
      end)
    end)
    new(columns, sorted_rows)
  end

  # Apply LIMIT clause to the result set.
  #
  # Works on both regular and grouped result sets.
  def limit({:RowSet, columns, rows, _}, n) do
    new(columns, Enum.take(rows, n))
  end

  # Perform a trivial GROUP operation, putting the all rows in a single group.
  def group_1({:RowSet, columns, rows, _}) do
    new(columns, [rows])
  end

  # Apply GROUP BY clause, collecting rows into groups.
  #
  # The result is a grouped RowSet; the "rows" element is a list of groups,
  # which are lists of tuples.
  def group_by({:RowSet, columns, rows, _}, exprs) do
    groups =
      Enum.group_by(rows, fn row ->
        Enum.map(exprs, fn expr -> SqlExpr.eval(columns, row, expr) end)
        |> List.to_tuple()
      end)
      |> Map.values()
    new(columns, groups)
  end
end
