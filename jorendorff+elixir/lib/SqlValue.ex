defmodule SqlValue do
  @moduledoc """
  Implementations of SQL operators and functions (where they differ from Elixir).

  SQL is pretty weird, around `NULL` at least; we have to implement the weirdness
  somewhere.
  """

  require Decimal

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

  def add(nil, _), do: nil
  def add(_, nil), do: nil
  def add(a, b) do
    cond do
      Decimal.is_decimal(b) -> Decimal.add(a, b)
      true -> a + b
    end
  end

  def sum(values, type) do
    start = case type do
              :int -> 0
              :num -> Decimal.new("0")
            end
    Enum.reduce(values, start, &add/2)
  end

  def round(nil), do: nil
  def round(num) do
    Decimal.round(num, 0)
  end
  def round(nil, _), do: nil
  def round(_, nil), do: nil
  def round(num, places) do
    Decimal.round(num, places)
  end
end
