defmodule TinyRdbms do
  @moduledoc """
  Documentation for TinyRdbms.
  """

  @doc """
  Hello world.

  ## Examples

      iex> TinyRdbms.hello
      :world

  """
  def hello do
    :world
  end

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

  """
  def tokenize(s) do
    Enum.map(Regex.scan(~r/(?:\s*)(\w+|[0-9]\w+|'(?:[^']|'')*'|.)(?:\s*)/,
                        s, capture: :all_but_first),
             &Sql.match_to_token/1)
  end

  @doc """
  Examine a token regex match and turn it into a token.

  ## Examples
      iex> Sql.match_to_token(["Select"])
      :select
      iex> Sql.match_to_token(["Student"])
      {:identifier, "Student"}
      iex> Sql.match_to_token(["'"])
      {:error, "unrecognized character: '"}

  """
  def match_to_token([token_str]) do
    case String.downcase(token_str) do
      "select" -> :select
      "*" -> :*
      "," -> :','
      "(" -> :'('
      ")" -> :')'
      "from" -> :from
      "as" -> :as
      "where" -> :where
      "order" -> :order
      "by" -> :by
      "asc" -> :asc
      "desc" -> :desc
      "group" -> :group
      "having" -> :having
      "null" -> :null
      "is" -> :is
      "not" -> :not
      "and" -> :and
      "or" -> :or
      "between" -> :between
      _ ->
        cond do
          String.match?(token_str, ~r/^[0-9]/) -> {:number, token_str}
          String.match?(token_str, ~r/^[a-z]/i) -> {:identifier, token_str}
          String.match?(token_str, ~r/^'.*'$/) ->
            {:string, token_str} # TODO: parse string
          true -> {:error, "unrecognized character: #{token_str}"}
        end
    end
  end
end
