Rounding before and after adding gives different results.

## Query

    SELECT SUM(ROUND(Total)), ROUND(SUM(Total))
    FROM Invoice

## Answer

    2351,2329
