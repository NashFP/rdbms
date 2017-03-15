defmodule QueryPlan do
  @moduledoc "An executable plan to execute a given query."

  defp product(1, x), do: x
  defp product(x, 1), do: x
  defp product(x, y), do: {:product, x, y}

  defp filter(true, x), do: x
  defp filter(c, x), do: {:filter, c, x}

  # If any expressions in `conditions` refer only to tables in `plan_tables`,
  # apply them to the given `plan`.
  #
  # Returns either `{plan, conditions}`
  # or `{filter(selected_conditions, plan), remaining_conditions}`.
  defp apply_filters(plan, conditions, all_column_info, plan_tables) do
    # Impl note: tables_used is computed many times, making the overall alg O(n^3) or so.
    # It would be easy to cache the result.
    partitioned =
      conditions
      |> Enum.group_by(fn(c) ->
        SqlExpr.tables_used(c, all_column_info)
        |> Enum.all?(&Enum.member?(plan_tables, String.downcase(&1)))
      end)

    selected_conds = Map.get(partitioned, true, [])
    remaining_conds = Map.get(partitioned, false, [])

    {plan_filters(all_column_info, plan, selected_conds), remaining_conds}
  end

  # The fallback plan for filtering a query by multiple conditions, when
  # `plan_filters` doesn't find a way to optimize.
  defp filter_multi(plan, conds) do
    filter(SqlExpr.join_and(conds), plan)
  end

  # Figure out how best to apply a filter.
  #
  # Worst case, this returns a `:filter` plan, i.e. a linear scan of all rows
  # produced by `plan`.  However, in some fairly common cases it returns a
  # `:filter_by_index` plan, which runs much faster.
  defp plan_filters(all_column_info, plan, conds) do
    case {plan, conds} do
      {{:table, t, a}, [{:=, left, right} | rest]} ->
        a = String.downcase(a)
        case {expand_column_ref(all_column_info, left), right} do
          {{:., ^a, c}, {:number, v}} ->
            c = String.downcase(c)
            filter(SqlExpr.join_and(rest), {:filter_by_index, t, a, c, v})
          {{:., ^a, c}, {:string, v}} ->
            c = String.downcase(c)
            filter(SqlExpr.join_and(rest), {:filter_by_index, t, a, c, v})
          _ ->
            case {left, expand_column_ref(all_column_info, right)} do
              {{:number, v}, {:., ^a, c}} ->
                c = String.downcase(c)
                filter(SqlExpr.join_and(rest), {:filter_by_index, t, a, c, v})
              {{:string, v}, {:., ^a, c}} ->
                c = String.downcase(c)
                filter(SqlExpr.join_and(rest), {:filter_by_index, t, a, c, v})
              _ -> filter_multi(plan, conds)
            end
        end
      {{:table, t, a}, [{:is_null, col_expr} | rest]} ->
        a = String.downcase(a)
        case expand_column_ref(all_column_info, col_expr) do
          {:., ^a, c} ->
            c = String.downcase(c)
            filter(SqlExpr.join_and(rest), {:filter_by_index, t, a, c, nil})
          _ -> filter_multi(plan, conds)
        end
      _ -> filter_multi(plan, conds)
    end
  end

  defp expand_column_ref(_, {:., t, c}) do
    {:., String.downcase(t), String.downcase(c)}
  end
  defp expand_column_ref(columns, {:identifier, c}) do
    c = String.downcase(c)
    {_, t, _, _} = Columns.get(columns, c)
    {:., String.downcase(t), c}
  end
  defp expand_column_ref(_, _), do: nil

  # See if the condition given as the second argument indicates a join on the
  # two given disjoint sets of tables. If so, return `{i, col1, col2}`.
  # If not, return nil.
  defp condition_as_join(i, {:=, lhs, rhs}, tables1, tables2, all_column_info) do
    case {expand_column_ref(all_column_info, lhs),
          expand_column_ref(all_column_info, rhs)} do
      {{:., t1, c1}, {:., t2, c2}} ->
        cond do
          Enum.member?(tables1, t1) && Enum.member?(tables2, t2) ->
            {i, {:., t1, c1}, {:., t2, c2}}
          Enum.member?(tables1, t2) && Enum.member?(tables2, t1) ->
            {i, {:., t2, c2}, {:., t1, c1}}
          true -> nil
        end
      _ -> nil
    end
  end
  defp condition_as_join(_, _, _, _, _), do: nil

  defp plan_join(1, _, plan2, _, _, conds) do
    {plan2, conds}
  end
  defp plan_join(plan1, tables1, plan2, tables2, all_column_info, conds) do
    join =
      Enum.with_index(conds)
      |> Enum.find_value(fn {c, i} ->
        condition_as_join(i, c, tables1, tables2, all_column_info)
      end)

    case join do
      nil ->
        {product(plan1, plan2), conds}
      {i, col1, col2} ->
        {{:join, plan1, col1, plan2, col2}, List.delete_at(conds, i)}
    end
  end

  def plan_query(database, ast) do
    # Some WHERE conditions, like `Genre.GenreId = 6`, apply to a single
    # table. We can save time by checking these conditions as early as possible,
    # before joining tables.

    conds =
      case ast.where do
        nil -> []
        expr -> SqlExpr.split_and(expr)
      end

    # In order to identify these "easy" conditions we need column info for all
    # tables involved in the query.
    all_column_info =
      ast.from
      |> Enum.map(&TinyRdbms.table_column_info(database, &1))
      |> Enum.reduce(&Columns.concat/2)

    # Construct the basic plan by starting with the trivial plan called `1`
    # and adding tables one by one (using a `reduce`).
    {plan, _, []} =
      ast.from
      |> Enum.reduce({1, [], conds}, fn(table_ast, acc) ->
        # Add one table to the plan. First, unpack the work we've already done.
        {plan_so_far, tables_so_far, remaining_conds} = acc

        # Now figure out some basics about the new table we're adding.
        {table_name, alias_name} =
          case table_ast do
            {:alias, t, a} -> {t, a}
            t -> {t, t}
          end
        plan_for_table = {:table, table_name, alias_name}

        # Filter this table as much as possible before joining.
        {plan_for_table, remaining_conds} =
          apply_filters(plan_for_table, remaining_conds, all_column_info,
            [String.downcase(alias_name)])

        # Join.
        {plan_so_far, remaining_conds} =
          plan_join(
            plan_so_far, tables_so_far,
            plan_for_table, [String.downcase(alias_name)],
            all_column_info, remaining_conds)
        tables_so_far = [String.downcase(alias_name) | tables_so_far]

        # Filter again after joining.
        {plan_so_far, remaining_conds} =
          apply_filters(plan_so_far, remaining_conds, all_column_info, tables_so_far)

        {plan_so_far, tables_so_far, remaining_conds}
      end)

    # ORDER BY clause
    plan =
      case ast.order do
        nil -> plan
        cols -> {:sort, cols, plan}
      end

    # SELECT clause
    {:map, ast.select, plan}
  end

  def run(database, plan) do
    case plan do
      1 ->
        RowSet.one()
      {:table, table_name, alias_name} ->
        TinyRdbms.table!(database, table_name)
        |> RowSet.apply_alias(alias_name)
      {:product, a, b} ->
        RowSet.product(run(database, a), run(database, b))
      {:filter, expr, subplan} ->
        run(database, subplan) |> RowSet.filter(expr)
      {:filter_by_index, t, a, c, v} ->
        TinyRdbms.table!(database, t)
        |> RowSet.apply_alias(a)
        |> RowSet.filter_by_index(c, v)
      {:sort, cols, subplan} ->
        run(database, subplan) |> RowSet.order_by(cols)
      {:map, exprs, subplan} ->
        run(database, subplan) |> RowSet.map(exprs)
      {:join, t1, c1, t2, c2} ->
        RowSet.join(run(database, t1), c1, run(database, t2), c2)
    end
  end
end
