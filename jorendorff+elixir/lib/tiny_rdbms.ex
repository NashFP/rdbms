defmodule TinyRdbms do
  @moduledoc """
  Documentation for TinyRdbms.
  """

  def load(dir) do
    File.ls!(dir)
      |> Enum.filter(fn(f) -> String.ends_with?(f, ".csv") end)
      |> Enum.map(fn(f) ->
        table_name = String.downcase(Path.basename(f, ".csv"))
        {table_name,
         RowSet.load!(table_name, Path.join(dir, f))}
      end)
      |> Enum.into(%{})
  end

  def repl(dir) do
    db = load(dir)
    run_repl(db)
  end

  defp run_repl(db) do
    case IO.gets("sql> ") do
      {:error, reason} -> {:error, reason}
      :eof -> :eof
      query ->
        RowSet.inspect(run_query(db, query))
        run_repl(db)
    end
  end

  def run_query(database, query) do
    ast = SqlParser.parse_select_stmt!(query)
    #IO.inspect(ast)
    plan = QueryPlan.plan_query(database, ast)
    #IO.inspect(plan)
    QueryPlan.run(database, plan)
  end

  def table!(database, table_name) do
    t = Map.get(database, String.downcase(table_name))
    if t == nil do
      raise ArgumentError, message: "no such table: #{table_name}"
    end
    t
  end

  def table_column_info(database, {:alias, table_name, alias_name}) do
    table_column_info(database, table_name)
    |> Columns.apply_table_alias(alias_name)
  end
  def table_column_info(database, table_name) do
    RowSet.columns(table!(database, table_name))
  end
end
