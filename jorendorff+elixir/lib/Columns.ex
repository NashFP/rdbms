defmodule Columns do
  @moduledoc "Metadata about RowSet columns: their index, table name, column name, and type."

  def new(columns) do
    by_name =
      columns
      |> Enum.reduce(%{}, fn (col, acc) ->
        {_, table_name, column_name, _} = col
        if column_name == nil do
          acc
        else
          column_name = String.downcase(column_name)
          acc =
            if Map.get(acc, column_name) do
              Map.put(acc, column_name, nil)
            else
              Map.put(acc, column_name, col)
            end
          if table_name != nil do
            table_name = String.downcase(table_name)
            Map.put(acc, {:., table_name, column_name}, col)
          else
            acc
          end
        end
      end)
    {List.to_tuple(columns), by_name}
  end

  def get({_, by_name}, name) do
    by_name[name]
  end

  def get_index(columns, name) do
    elem(get(columns, name), 0)
  end

  def get_type(columns, name) do
    elem(get(columns, name), 3)
  end

  def names(columns) do
    list(columns)
      |> Enum.map(fn {_, _, name, _} -> name end)
      |> List.to_tuple()
  end

  def list({columns, _}) do
    Tuple.to_list(columns)
  end

  def row_from_strings(columns, strings) do
    Enum.zip(list(columns), strings)
      |> Enum.map(fn {{_, _, _, type}, s} -> SqlValue.from_string(type, s) end)
      |> List.to_tuple()
  end

  def concat(columns1, columns2) do
    columns1 = list(columns1)
    n = length(columns1)
    columns2 =
      for {index, t, c, ty} <- list(columns2) do
        {n + index, t, c, ty}
      end
    Columns.new(columns1 ++ columns2)
  end

  def apply_table_alias(columns, alias_name) do
    list(columns)
    |> Enum.map(fn {i, _, c, ty} -> {i, alias_name, c, ty} end)
    |> Columns.new()
  end
end
