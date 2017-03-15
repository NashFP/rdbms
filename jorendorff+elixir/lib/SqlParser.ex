defmodule SqlParser do
  @moduledoc "Turns strings of SQL into Elixir data."

  @doc """
  Break the given SQL string `s` into tokens.

  Return the list of tokens. SQL keywords, operators, etc. are represented
  as Elixir keywords. Identifiers and literals are represented as pairs,
  `{:token_type, value}`. The token types are `:identifier`, `:number`, and
  `:string`.

  ## Examples

      iex> SqlParser.tokenize("SELECT * FROM Student")
      [:select, :*, :from, {:identifier, "Student"}]
      iex> SqlParser.tokenize("WHERE name = '")
      [:where, {:identifier, "name"}, :=, {:error, "unrecognized character: '"}]
      iex> SqlParser.tokenize("1 <> 0.99")
      [{:number, 1}, :<>, {:number, Decimal.new("0.99")}]
  """
  def tokenize(s) do
    token_re = ~r/(?:\s*)([0-9](?:\w|\.)*|\w+|'(?:[^']|'')*'|>=|<=|<>|.)(?:\s*)/
    Regex.scan(token_re, s, capture: :all_but_first)
    |> Enum.map(&match_to_token/1)
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
      "limit" -> :limit
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

  @doc """
  Parse a SQL `SELECT` statement.

  The result is a Map with keys representing all the clauses of a `SELECT`:
  `:select`, `:from`, `:where`, `:group`, `:having`, `:order`.
  Most clauses are optional, so some of these keys may map to `nil`.

  For example, below, there's no `GROUP BY` clause in the input,
  so we have `group: nil` in the output.

      iex> SqlParser.parse_select_stmt!(\"""
      ...>   SELECT Title
      ...>   FROM Album
      ...>   WHERE ArtistId = 252
      ...>   ORDER BY Title
      ...> \""")
      %{
        select: [{:identifier, "Title"}],
        from: ["Album"],
        where: {:=, {:identifier, "ArtistId"}, {:number, 252}},
        group: nil,
        having: nil,
        order: [identifier: "Title"],
        limit: nil
      }

  Raises `ArgumentError` if the input string isn't a syntactically correct
  `SELECT` statement.

      iex> SqlParser.parse_select_stmt!("SELECT SELECT FROM SELECT WHERE SELECT")
      ** (ArgumentError) identifier or literal expected
  """
  def parse_select_stmt!(sql) do
    {%{}, tokenize(sql)}
    |> parse_clause!(:select, &parse_exprs!/1, required: true)
    |> parse_clause!(:from, &parse_tables!/1)
    |> parse_clause!(:where, &parse_expr!/1)
    |> parse_clause_2!(:group, :by, &parse_exprs!/1)
    |> parse_clause!(:having, &parse_expr!/1)
    |> parse_clause_2!(:order, :by, &parse_exprs!/1)
    |> parse_clause!(:limit, &parse_expr!/1)
    |> check_done!()
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
        expected_argc =
          cond do
            Enum.member?(["AVG", "COUNT", "MAX", "MIN", "SUM"], fnname_up) ->
              [1]
            fnname_up == "ROUND" ->
              1..2
            true ->
              raise ArgumentError, message: "unrecognized function #{inspect(fnname)}"
          end

        if !Enum.member?(expected_argc, length(args)) do
          raise ArgumentError, message: "#{fnname_up}() function expects #{expected_argc} argument(s), got #{length(args)}"
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
      [{:identifier, table_name}, {:identifier, alias_name} | rest] ->
        {{:alias, table_name, alias_name}, rest}
      [{:identifier, table_name} | rest] ->
        {table_name, rest}
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
