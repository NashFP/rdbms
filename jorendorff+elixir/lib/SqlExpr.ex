defmodule SqlExpr do
  @moduledoc "Functions that inspect, compile, and evaluate parsed SQL expressions."

  require Decimal

  @doc """
  True if the expression is an aggregate (like `COUNT(*)`).

  Aggregate-ness matters because it can turn on grouping even without a `GROUP BY` clause.
  If a query `SELECT`s any aggregates, and `GROUP BY` is not present,
  then all matching rows are collected into on big group.
  """
  def is_aggregate?(expr) do
    case expr do
      {:apply, fnname, args} ->
        if Enum.member?(["AVG", "COUNT", "MAX", "MIN", "SUM"], fnname) do
          true
        else
          Enum.any?(args, &is_aggregate?/1)
        end
      _ -> false
    end
  end

  @doc """
  Return the list of tables used by the given expression.

  Since an identifier like `GenreId` might refer to columns with that name in
  several tables, the caller has to provide column info for the context where
  the expression appears.

  Raises an error if `expr` uses any identifiers that aren't in
  `columns`, or if `expr` uses `*` (except in `COUNT(*)`).
  """
  def tables_used(expr, columns) do
    case expr do
      {:identifier, name} ->
        {_, t, _, _} = Columns.get(columns, String.downcase(name))
        [t]
      {:., table_name, column_name} ->
        key = {:., String.downcase(table_name), String.downcase(column_name)}
        {_, t, _, _} = Columns.get(columns, key)
        [t]
      {:number, _} -> []
      {:apply, "COUNT", _} -> []  # for now, the only function is COUNT()
      {:apply, _, args} ->
        Enum.flat_map(args, &tables_used(&1, columns))
        |> Enum.uniq()
      {:string, _} -> []
      {:is_null, x} -> tables_used(x, columns)
      {_, lhs, rhs} -> tables_used(lhs, columns) ++ tables_used(rhs, columns)
      :* -> raise ArgumentError, message: "* used in WHERE clause"
    end
  end

  @doc """
  Infer column info for the given expression, `expr`.

  `columns` provides information about all columns available for use in `expr`
  (columns of tables mentioned in the `FROM` clause).

  `index` isn't used to compute anything; rather, it's part of the *answer*,
  the column number of `expr` within the `SELECT` clause where it appears.
  """
  def column_info(expr, columns, index) do
    case expr do
      {:identifier, name} ->
        {_, t, c, ty} = Columns.get(columns, String.downcase(name))
        {index, t, c, ty}
      {:., table_name, column_name} ->
        key = {:., String.downcase(table_name), String.downcase(column_name)}
        {_, t, c, ty} = Columns.get(columns, key)
        {index, t, c, ty}
      _ ->
        type =
          case expr do
            {:number, x} ->
              if Decimal.is_decimal x do
                :num
              else
                :int
              end
            {:apply, fnname, [arg]} ->
              cond do
                fnname == "COUNT" ->
                  :int
                fnname == "AVG" || fnname == "ROUND" ->
                  require_numeric_type(arg, fnname, columns)
                Enum.member?(["MAX", "MIN", "SUM"], fnname) ->
                  numeric_aggregate_type(arg, fnname, columns)
                true ->
                  raise ArgumentError, message: "internal error: bad expr #{inspect(expr)}"
              end
            {:apply, "ROUND", [num, places]} ->
              require_type(places, columns, "ROUND", 2, :int)
              require_numeric_type(num, "ROUND", columns)
            {:string, _} -> :str
            {:is_null, _} -> :bool
            {:=, _, _} -> :bool
            {:<>, _, _} -> :bool
            {:and, _, _} -> :bool
            {:or, _, _} -> :bool
            :* -> :row
            _ -> raise ArgumentError, message: "internal error: bad expr #{inspect(expr)}"
          end
        {index, nil, nil, type}  # column has no name
    end
  end

  defp numeric_aggregate_type(arg_expr, fnname, columns) do
    t = column_info(arg_expr, columns, 0) |> elem(3)
    if t != :int && t != :num do
      raise ArgumentError, message: "#{fnname}() requires a numeric argument"
    end
    t
  end

  defp require_numeric_type(arg_expr, fnname, columns) do
    numeric_aggregate_type(arg_expr, fnname, columns)
    :num
  end

  defp require_type(arg_expr, columns, fnname, arg_num, expected_type) do
    t = column_info(arg_expr, columns, 0) |> elem(3)
    if t != expected_type do
      raise ArgumentError, message: "#{fnname}() requires a #{inspect(expected_type)} for argument #{arg_num}"
    end
    t
  end

  defp expr_type(expr, columns) do
    column_info(expr, columns, 0) |> elem(3)
  end

  @doc "Evaluate an aggregate expression."
  def eval_aggregate(columns, group, expr) do
    case expr do
      {:apply, fnname, arg_exprs} ->
        case String.upcase(fnname) do
          "AVG" ->
            [arg_expr] = arg_exprs
            type = expr_type(arg_expr, columns)
            case Enum.map(group, fn row -> eval(columns, row, arg_expr) end) do
              [] -> nil
              values ->
                total = SqlValue.sum(values, type)
                total = case type do
                          :int -> Decimal.new(total)
                          _ -> total
                        end
                Decimal.div(total, Decimal.new(length(values)))
            end
          "COUNT" ->
            _ = Enum.map(group, fn(row) ->
              Enum.map(arg_exprs, fn(expr) -> eval(columns, row, expr) end)
            end)
            length(group)
          "MIN" ->
            [arg_expr] = arg_exprs
            Enum.map(group, fn row -> eval(columns, row, arg_expr) end)
            |> Enum.min(fn -> nil end)
          "MAX" ->
            [arg_expr] = arg_exprs
            Enum.map(group, fn row -> eval(columns, row, arg_expr) end)
            |> Enum.max(fn -> nil end)
          "SUM" ->
            [arg_expr] = arg_exprs
            case Enum.map(group, fn row -> eval(columns, row, arg_expr) end) do
              [] -> nil  # not 0, for some reason
              values ->
                type = column_info(arg_expr, columns, 0) |> elem(3)
                SqlValue.sum(values, type)
            end
          "ROUND" ->
            args = Enum.map(arg_exprs, fn expr -> eval_aggregate(columns, group, expr) end)
            apply(SqlValue, :round, args)
        end

      _ ->
        # If expr isn't obviously an aggregate, just pick an arbitrary row from the group
        # and evaluate it as a normal expression. Two issues here:
        #
        # *   This fails to handle compound expressions that *contain* aggregates
        #     as subexpressions. Oops.
        #
        # *   This behavior too permissive; it allows nonsensical queries like
        #     `SELECT Name, COUNT(*) FROM Track` (returning a single row!) or
        #     `SELECT Name FROM Track GROUP BY UnitPrice`. I'd rather allow
        #     selecting only the `GROUP BY` columns themselves and other
        #     expressions that are uniquely determined by those values (such as
        #     a table's other columns, when we have grouped by its primary key
        #     column). But Sqlite does something very much like this. I don't
        #     know what the SQL standard or other databases say.
        eval(columns, hd(group), expr)
    end
  end

  defp compile_column_ref(columns, row, key) do
    i = Columns.get_index(columns, key)
    quote(do: elem(unquote(row), unquote(i)))
  end

  @doc """
  Convert a parsed SQL expression `expr` to a quoted Elixir expression.

  The resulting expression can be evaluated in a loop to process several rows of data.

  `row` is a quoted Elixir expression, a variable name; when the expression is
  actually executed, that variable will contain a tuple (the current row of SQL
  data to process).
  """
  def compile_expr(columns, row, expr) do
    case expr do
      {:identifier, x} ->
        compile_column_ref(columns, row, String.downcase(x))
      {:., t, c} ->
        key = {:., String.downcase(t), String.downcase(c)}
        compile_column_ref(columns, row, key)
      {:number, n} ->
        if Decimal.is_decimal(n) do
          quote(do: unquote(Decimal).new(unquote(Decimal.to_string(n))))
        else
          n
        end
      {:string, s} -> s
      {:is_null, subexpr} ->
        subexpr_ex = compile_expr(columns, row, subexpr)
        quote(do: unquote(SqlValue).is_null?(unquote(subexpr_ex)))
      {:apply, "ROUND", args} ->
        {num_expr, places_expr} =
          case args do
            [n] -> {n, {:number, 0}}
            [n, p] -> {n, p}
          end
        num_ex = compile_expr(columns, row, num_expr)
        places_ex = compile_expr(columns, row, places_expr)
        if places_ex == 0 do
          quote(do: unquote(SqlValue).round(unquote(num_ex)))
        else
          quote(do: unquote(SqlValue).round(unquote(num_ex), unquote(places_ex)))
        end
      {binary_op, left_expr, right_expr} ->
        left_ex = compile_expr(columns, row, left_expr)
        right_ex = compile_expr(columns, row, right_expr)
        case binary_op do
          := -> quote(do: unquote(SqlValue).equals?(unquote(left_ex), unquote(right_ex)))
          :'<>' -> quote(do: unquote(SqlValue).logical_not(
                             unquote(SqlValue).equals?(unquote(left_ex), unquote(right_ex))))
          :and -> quote(do: unquote(SqlValue).logical_and(unquote(left_ex), unquote(right_ex)))
          :or -> quote(do: unquote(SqlValue).logical_or(unquote(left_ex), unquote(right_ex)))
          _ -> raise ArgumentError, message: "internal error: bad op #{inspect(binary_op)}"
        end
      :* -> row
      _ -> raise ArgumentError, message: "internal error: unrecognized expr #{inspect(expr)}"
    end
  end

  # This has the unfortunate side effect of obliterating any previously compiled function.
  def compile(columns, expr) do
    row = {:row, [], SqlExpr}
    body = compile_expr(columns, row, expr)

    :code.delete(SqlExpr.Tmp)
    :code.purge(SqlExpr.Tmp)

    # The quoted form of the module to compile.
    code = quote do
      def get_fun() do
        fn (unquote(row)) -> unquote(body) end
      end
    end

    # Compile it and return the compiled fn.
    {:module, tmp, _, _} = Module.create(SqlExpr.Tmp, code, Macro.Env.location(__ENV__))
    tmp.get_fun()
  end

  def eval(columns, row, expr) do
    case expr do
      {:identifier, x} ->
        elem(row, Columns.get_index(columns, String.downcase(x)))
      {:., t, c} ->
        key = {:., String.downcase(t), String.downcase(c)}
        elem(row, Columns.get_index(columns, key))
      {:number, n} -> n
      {:string, s} -> s
      {:is_null, subexpr} ->
        val = eval(columns, row, subexpr)
        SqlValue.is_null?(val)
      {:apply, fnname, arg_exprs} ->
        args = Enum.map(arg_exprs, fn expr -> eval(columns, row, expr) end)
        case fnname do
          "ROUND" -> apply(SqlValue, :round, args)
          _ -> raise ArgumentError, message: "internal error: bad function #{fnname}"
        end
      {binary_op, left_expr, right_expr} ->
        left_val = eval(columns, row, left_expr)
        right_val = eval(columns, row, right_expr)
        case binary_op do
          := -> SqlValue.equals?(left_val, right_val)
          :'<>' -> SqlValue.logical_not(SqlValue.equals?(left_val, right_val))
          :and -> SqlValue.logical_and(left_val, right_val)
          :or -> SqlValue.logical_or(left_val, right_val)
          _ -> raise ArgumentError, message: "internal error: bad op #{inspect(binary_op)}"
        end
      :* -> row
      _ -> raise ArgumentError, message: "internal error: unrecognized expr #{inspect(expr)}"
    end
  end

  @doc """
  Evaluate several expressions for the same row of data.

  `exprs` is a list of SqlExprs. Returns a tuple with one value per expr.
  """
  def eval_several(exprs, columns, row) do
    Enum.map(exprs, &SqlExpr.eval(columns, row, &1))
    |> List.to_tuple()
  end

  @doc """
  Evaluate several aggregate expressions for the same group of rows.

  `exprs` is a list of SqlExprs. Returns a tuple with one value per expr.
  """
  def eval_aggregate_several(exprs, columns, group) do
    Enum.map(exprs, &SqlExpr.eval_aggregate(columns, group, &1))
    |> List.to_tuple()
  end

  @doc """
  Break a boolean expression into a list of conditions which must all be met.

  If the argument is an AND expression, return a list of the operands of AND.
  Otherwise, return a list containing just the argument expression.
  """
  def split_and({:and, lhs, rhs}) do
    split_and(lhs) ++ split_and(rhs)
  end
  def split_and(expr) do
    [expr]
  end

  @doc """
  Combine a list of boolean expressions with AND.
  """
  def join_and([]), do: true
  def join_and([expr]), do: expr
  def join_and([lhs|rhs]), do: {:and, lhs, join_and(rhs)}

end
