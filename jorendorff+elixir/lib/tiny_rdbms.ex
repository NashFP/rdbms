defmodule TinyRdbms do
  @moduledoc """
  Documentation for TinyRdbms.
  """

  def load(dir) do
    File.ls!(dir)
      |> Enum.filter(fn(f) -> String.ends_with?(f, ".csv") end)
      |> Enum.map(fn(f) ->
        table_name = String.downcase(String.slice(f, 0..-5))
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
    ast = Sql.parse_select_stmt!(query)
    #IO.inspect(ast)

    {conditions_by_table, leftover_condition_expr} = plan_query(database, ast)

    # Actually run the query at last!
    combine(database, ast.from, conditions_by_table)
      |> RowSet.filter(leftover_condition_expr)
      |> apply_order(ast.order)
      |> RowSet.map(ast.select)
  end

  defp table!(database, table_name) do
    t = Map.get(database, String.downcase(table_name))
    if t == nil do
      raise ArgumentError, message: "no such table: #{table_name}"
    end
    t
  end

  defp combine(_, [], _) do
    RowSet.one()
  end
  defp combine(database, [table_name|more_tables], conds_by_table) do
    table_cond =
      Map.get(conds_by_table, String.downcase(table_name), [])
      |> SqlExpr.join_and()

    table!(database, table_name)
    |> RowSet.filter(table_cond)
    |> RowSet.product(combine(database, more_tables, conds_by_table))
  end

  defp plan_query(database, ast) do
    # Some WHERE conditions, like `Genre.GenreId = 6`, apply to a single
    # table. We can save time by checking these conditions first, before
    # joining tables.

    # In order to identify these "easy" conditions we need column info for all
    # tables involved in the query.
    all_column_info =
      ast.from
      |> Enum.map(fn(name) -> RowSet.columns(table!(database, name)) end)
      |> Enum.reduce(&Columns.concat/2)

    # Now, for each table, find all "easy" conditions that apply only to that
    # table. SqlExpr.tables_used() does most of the work.
    conds_by_table =
      case ast.where do
        nil -> %{}  # No WHERE clause, skip this work
        expr ->
          SqlExpr.split_and(expr)  # break the query down into conditions
          |> Enum.group_by(fn(expr) ->
            case SqlExpr.tables_used(expr, all_column_info) do
              [table] -> table
              _ -> 0
            end
          end)
      end

    # Other conditions involve no tables or multiple tables; combine them back
    # into a single SQL expression.
    leftover_conds =
      Map.get(conds_by_table, 0, [])
      |> SqlExpr.join_and()

    {conds_by_table, leftover_conds}
  end

  # Apply ORDER BY clause to the result set.
  defp apply_order(data, nil) do
    data
  end
  defp apply_order({columns, rows}, order) do
    sorted_rows = Enum.sort_by(rows, fn row ->
      Enum.map(order, fn expr ->
        SqlExpr.eval(columns, row, expr)
      end)
    end)
    {columns, sorted_rows}
  end
end

defmodule Columns do
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

  def names({columns, _}) do
    Tuple.to_list(columns)
      |> Enum.map(fn {_, _, name, _} -> name end)
      |> List.to_tuple()
  end

  def row_from_strings(columns, strings) do
    {column_tuple, _} = columns

    Enum.zip(Tuple.to_list(column_tuple), strings)
      |> Enum.map(fn {{_, _, _, type}, s} -> SqlValue.from_string(type, s) end)
      |> List.to_tuple()
  end

  def concat({columns1, _}, {columns2, _}) do
    columns1 = Tuple.to_list(columns1)
    n = length(columns1)
    columns2 =
      for {index, t, c, ty} <- Tuple.to_list(columns2) do
        {n + index, t, c, ty}
      end
    Columns.new(columns1 ++ columns2)
  end
end

defmodule RowSet do
  def new(columns, rows) do
    {columns, rows}
  end

  def one() do
    RowSet.new(Columns.new([]), [{}])
  end

  def rows({_, rows}), do: rows
  def columns({columns, _}), do: columns

  def load!(table_name, filename) do
    raw_rows = File.stream!(filename) |> CSV.decode() |> Enum.to_list
    columns = hd(raw_rows)
      |> Enum.with_index()
      |> Enum.map(fn {name, index} ->
           # hack: guess type from column name
           type = cond do
             String.ends_with?(name, "Id") -> :int
             String.ends_with?(name, "Price") -> :num
             true -> :str
           end
           {index, table_name, name, type}
         end)
      |> Columns.new()

    rows = tl(raw_rows)
      |> Enum.map(fn row -> Columns.row_from_strings(columns, row) end)

    new(columns, rows)
  end

  @doc """
  Dump a RowSet to stdout.
  """
  def inspect(row_set) do
    headings = Columns.names(RowSet.columns(row_set))
    IO.inspect(headings)
    IO.puts("----")
    RowSet.rows(row_set)
      |> Enum.map(&IO.inspect/1)
  end

  def product(rs1, rs2) do
    cond do
      rows(rs1) == [{}] -> rs2  # optimization: 1 * A = A
      rows(rs2) == [{}] -> rs1  # optimization: A * 1 = A
      true ->
        {cols1, rows1} = rs1
        {cols2, rows2} = rs2
        cols = Columns.concat(cols1, cols2)
        rows =
          for a <- rows1, b <- rows2 do
            List.to_tuple(Tuple.to_list(a) ++ Tuple.to_list(b))
          end
        {cols, rows}
    end
  end

  # Evaluate WHERE clause.
  def filter(data, true) do
    data
  end
  def filter({columns, rows}, cond_expr) do
    # -------------------------------------------------------------------------
    # Use only one of the next two lines (the other should be commented out)
    #f = fn (row) -> SqlExpr.eval(columns, row, cond_expr) end  # INTERPRETER
    f = SqlExpr.compile(columns, cond_expr)                    # COMPILER
    # -------------------------------------------------------------------------
    filtered_rows = Enum.filter(rows, fn(row) ->
      SqlValue.is_truthy?(f.(row))
    end)
    {columns, filtered_rows}
  end

  # Evaluate SELECT clause (projection).
  def map({columns, rows}, exprs) do
    selected_columns =
      exprs
      |> Enum.with_index()
      |> Enum.map(fn {expr, index} -> SqlExpr.column_info(expr, columns, index) end)
      |> Columns.new()

    projected_rows =
      if Enum.any?(exprs, &SqlExpr.is_aggregate?/1) do
        # Implicitly group all rows into one big group, if selecting an aggregate.
        [
          Enum.map(exprs, fn(expr) -> SqlExpr.eval_aggregate(columns, rows, expr) end)
          |> List.to_tuple()
        ]
      else
        Enum.map(rows, fn(row) ->
          # For each row, evaluate each selected expression.
          Enum.map(exprs, fn(expr) -> SqlExpr.eval(columns, row, expr) end)
          |> List.to_tuple()
        end)
      end
    {selected_columns, projected_rows}
  end
end

defmodule SqlValue do
  @doc """
  True if `v` is a null value.

  Because the CSV files don't include any information about the column types,
  we lamely treat empty strings as nulls.
  """
  def is_null?(v) do
    v == :nil || v == ""
  end

  @doc """
  Return true if SQL considers the value `v` to be a true value.

  ## Examples

      iex> SqlValue.is_truthy?(true)
      true
      iex> SqlValue.is_truthy?(0)
      false
      iex> SqlValue.is_truthy?(2)
      true
      iex> SqlValue.is_truthy?(nil)
      nil

  """
  def is_truthy?(v) do
    case v do
      true -> true
      nil -> nil
      v -> is_integer(v) && v != 0
    end
  end

  @doc """
  Return true if `left` and `right` are considered equal.

  As the SQL standard requires, if either `left` or `right` is null, the answer
  is `nil` rather than `false` or `true`.

  ## Examples

      iex> SqlValue.equals?(3, 3)
      true
      iex> SqlValue.equals?(3, 5)
      false
      iex> SqlValue.equals?(3, "3")
      false
      iex> SqlValue.equals?(nil, "hello")
      nil
      iex> SqlValue.equals?(nil, nil)
      nil

  """
  def equals?(left, right) do
     if is_null?(left) || is_null?(right) do
       nil
     else
       left == right
     end
  end

  def logical_not(v) do
    case is_truthy?(v) do
      true -> false
      false -> true
      nil -> nil
    end
  end

  def logical_and(a, b) do
    case {is_truthy?(a), is_truthy?(b)} do
      {nil, _} -> nil
      {_, nil} -> nil
      {true, true} -> true
      _ -> false
    end
  end

  def logical_or(a, b) do
    case {is_truthy?(a), is_truthy?(b)} do
      {nil, _} -> nil
      {_, nil} -> nil
      {false, false} -> false
      _ -> true
    end
  end

  def from_string(_, ""), do: :nil
  def from_string(:int, string) do
    {n, ""} = Integer.parse(string)
    n
  end
  def from_string(:num, string), do: Decimal.new(string)
  def from_string(:str, string), do: string
  def from_string(:bool, "true"), do: true
  def from_string(:bool, "false"), do: false
  def from_string(:bool, "1"), do: true
  def from_string(:bool, "0"), do: false
end

defmodule SqlExpr do
  def is_aggregate?(expr) do
    case expr do
      {:apply, _, _} -> true
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


defmodule Sql do

  @doc """
  Break the given SQL string `s` into tokens.

  Return the list of tokens. SQL keywords, operators, etc. are represented
  as Elixir keywords. Identifiers and literals are represented as pairs,
  `{:token_type, value}`. The token types are `:identifier`, `:number`, and
  `:string`.

  ## Examples

      iex> Sql.tokenize("SELECT * FROM Student")
      [:select, :*, :from, {:identifier, "Student"}]
      iex> Sql.tokenize("WHERE name = '")
      [:where, {:identifier, "name"}, :=, {:error, "unrecognized character: '"}]
      iex> Sql.tokenize("1 <> 0.99")
      [{:number, 1}, :<>, {:number, Decimal.new("0.99")}]
  """
  def tokenize(s) do
    token_re = ~r/(?:\s*)([0-9](?:\w|\.)*|\w+|'(?:[^']|'')*'|>=|<=|<>|.)(?:\s*)/
    Regex.scan(token_re, s, capture: :all_but_first) |>
      Enum.map(&match_to_token/1)
  end

  # Convert a single token regex match into a token.
  defp match_to_token([token_str]) do
    case String.downcase(token_str) do
      "all" -> :all
      "and" -> :and
      "as" -> :as
      "asc" -> :asc
      "between" -> :between
      "by" -> :by
      "desc" -> :desc
      "distinct" -> :distinct
      "exists" -> :exists
      "from" -> :from
      "group" -> :group
      "having" -> :having
      "insert" -> :insert
      "is" -> :is
      "not" -> :not
      "null" -> :null
      "or" -> :or
      "order" -> :order
      "select" -> :select
      "set" -> :set
      "union" -> :union
      "update" -> :update
      "values" -> :values
      "where" -> :where
      "*" -> :*
      "." -> :.
      "," -> :','
      "(" -> :'('
      ")" -> :')'
      "=" -> :=
      ">=" -> :'>='
      "<=" -> :'<='
      ">" -> :'>'
      "<" -> :'<'
      "<>" -> :'<>'
      _ ->
        cond do
          String.match?(token_str, ~r/^[0-9]/) ->
            if String.contains?(token_str, ".") do
              {:number, Decimal.new(token_str)}
            else
              {n, ""} = Integer.parse(token_str)
              {:number, n}
            end
          String.match?(token_str, ~r/^[a-z]/i) ->
            {:identifier, token_str}
          String.match?(token_str, ~r/^'.*'$/) ->
            {:string, String.slice(token_str, 1..-2)} # TODO: handle doubled quotes
          true -> {:error, "unrecognized character: #{token_str}"}
        end
    end
  end

  def parse_select_stmt!(sql) do
    {%{}, tokenize(sql)} |>
      parse_clause!(:select, &parse_exprs!/1, required: true) |>
      parse_clause!(:from, &parse_tables!/1) |>
      parse_clause!(:where, &parse_expr!/1) |>
      parse_clause_2!(:group, :by, &parse_exprs!/1) |>
      parse_clause!(:having, &parse_expr!/1) |>
      parse_clause_2!(:order, :by, &parse_exprs!/1) |>
      check_done!()
  end

  defp parse_exprs!(sql) do
    {expr, tail} = parse_expr!(sql)
    case tail do
      [:',' | more] ->
        {exprs, rest} = parse_exprs!(more)
        {[expr | exprs], rest}
      _ -> {[expr], tail}
    end
  end

  defp parse_prim!(sql) do
    case sql do
      [{:identifier, fnname}, :'(' | rest] ->
        fnname_up = String.upcase(fnname)
        {args, rest} = parse_exprs!(rest)
        case fnname_up do
          "COUNT" ->
            if length(args) != 1 do
              raise ArgumentError, message: "COUNT() function expects 1 argument, got #{length(args)}"
            end
          _ -> raise ArgumentError, message: "unrecognized function #{inspect(fnname)}"
        end
        case rest do
          [:')' | rest] -> {{:apply, fnname_up, args}, rest}
          _ -> raise ArgumentError, message: "')' expected after function arguments"
        end
      [{:identifier, table_name}, :., {:identifier, column_name} | rest] ->
        {{:., table_name, column_name}, rest}
      [{:identifier, _} | rest] -> {hd(sql), rest}
      [{:number, _} | rest] -> {hd(sql), rest}
      [{:string, _} | rest] -> {hd(sql), rest}
      [:* | rest] -> {hd(sql), rest}
      _ -> raise ArgumentError, message: "identifier or literal expected"
    end
  end

  defp parse_expr!(sql) do
    {lhs, rest} = parse_and_expr!(sql)
    case rest do
      [:or | rest] ->
        {rhs, rest} = parse_expr!(rest)
        {{:or, lhs, rhs}, rest}
      _ -> {lhs, rest}
    end
  end

  defp parse_and_expr!(sql) do
    {lhs, rest} = parse_compare_expr!(sql)
    case rest do
      [:and |rest] ->
        {rhs, rest} = parse_and_expr!(rest)
        {{:and, lhs, rhs}, rest}
      _ -> {lhs, rest}
    end
  end

  defp parse_compare_expr!(sql) do
    {prim, rest} = parse_prim!(sql)
    case rest do
      [:= | rest] ->
        {rhs, rest} = parse_prim!(rest)
        {{:=, prim, rhs}, rest}
      [:'<>' | rest] ->
        {rhs, rest} = parse_prim!(rest)
        {{:'<>', prim, rhs}, rest}
      [:is, :null | rest] ->
        {{:is_null, prim}, rest}
      _ -> {prim, rest}
    end
  end

  defp parse_table!(sql) do
    case sql do
      [{:identifier, x} | rest] -> {x, rest}
      _ -> raise ArgumentError, message: "table name expected"
    end
  end

  defp parse_tables!(sql) do
    {table, rest} = parse_table!(sql)
    case rest do
      [:',' | rest] ->
        {tables, rest} = parse_tables!(rest)
        {[table | tables], rest}
      _ -> {[table], rest}
    end
  end

  defp parse_clause!({ast, sql}, keyword, parser, keywords \\ []) do
    case sql do
      [^keyword | rest] ->
        {clause_ast, rest} = parser.(rest)
        {Map.put(ast, keyword, clause_ast), rest}
      _ ->
        if Keyword.get(keywords, :required, false) do
          raise ArgumentError, message: "#{keyword} expected"
        else
          {Map.put(ast, keyword, nil), sql}
        end
    end
  end

  defp parse_clause_2!({ast, sql}, kw1, kw2, parser) do
    case sql do
      [^kw1, ^kw2 | rest] ->
        {clause_ast, rest} = parser.(rest)
        {Map.put(ast, kw1, clause_ast), rest}
      _ ->
        {Map.put(ast, kw1, :nil), sql}
    end
  end

  defp check_done!({ast, sql}) do
    case sql do
      [] -> ast
      _ ->
        ast |> inspect() |> IO.puts()
        sql |> inspect() |> IO.puts()
        raise ArgumentError, message: "extra stuff at end of SQL"
    end
  end
end
