defmodule SqlExpr do
  @moduledoc "Functions that inspect, compile, and evaluate parsed SQL expressions."

  @doc """
  True if the expression is an aggregate (like `COUNT(*)`).

  Aggregate-ness matters because it can turn on grouping even without a `GROUP BY` clause.
  If a query `SELECT`s any aggregates, and `GROUP BY` is not present,
  then all matching rows are collected into on big group.
  """
  def is_aggregate?(expr) do
    case expr do
      {:apply, "COUNT", _} -> true
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
              if Decimal.decimal? x do
                :num
              else
                :int
              end
            {:apply, "COUNT", _} -> :int  # for now, the only function is COUNT()
            {:string, _} -> :str
            {:is_null, _} -> :bool
            {:=, _, _} -> :bool
            {:<>, _, _} -> :bool
            {:and, _, _} -> :bool
            {:or, _, _} -> :bool
            :* -> :row
            _ -> raise ArgumentError, message: "internal error: unrecognized expr #{inspect(expr)}"
          end
        {index, nil, nil, type}  # column has no name
    end
  end

  @doc "Evaluate an aggregate expression."
  def eval_aggregate(columns, group, expr) do
    case expr do
      {:apply, fnname, arg_exprs} ->
        _ = Enum.map(group, fn(row) ->
          Enum.map(arg_exprs, fn(expr) -> eval(columns, row, expr) end)
        end)
        case String.upcase(fnname) do
          "COUNT" -> length(group)
          _ -> raise ArgumentError, message: "internal error: unknown function #{inspect(fnname)}"
        end
      _ ->
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
        if Decimal.decimal?(n) do
          quote(do: unquote(Decimal).new(unquote(Decimal.to_string(n))))
        else
          n
        end
      {:string, s} -> s
      {:is_null, subexpr} ->
        subexpr_ex = compile_expr(columns, row, subexpr)
        quote(do: unquote(SqlValue).is_null?(unquote(subexpr_ex)))
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
