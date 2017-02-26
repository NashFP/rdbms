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
  Return the list of tokens, as strings.

  ## Examples

      iex> Sql.tokenize("SELECT * FROM Student")
      ["SELECT", "*", "FROM", "Student"]

  """
  def tokenize(s) do
    Enum.map(Regex.scan(~r/(?:\s*)(\w+|[0-9]\w+|'(?:[^']|'')*'|.)(?:\s*)/,
                        s, capture: :all_but_first),
             &hd/1)
  end

  @doc """
  Examine a token and tell what type it is.

  ## Examples
      iex> Sql.token_type("select")
      :select
      iex> Enum.map(Sql.tokenize("SELECT * FROM Student"), &Sql.token_type/1)
      [:select, :star, :from, :identifier]
  """
  def token_type(token) do
    case String.downcase(token) do
      "select" -> :select
      "*" -> :star
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
          String.match?(token, ~r/^[0-9]/) -> :number
          String.match?(token, ~r/^[a-z]/i) -> :identifier
          true -> :error
        end
    end
  end
end
