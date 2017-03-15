`ROUND()` can also be used in a `WHERE` clause, applied to each row.

## Query

    SELECT COUNT(*)
    FROM Invoice
    WHERE ROUND(Total) = ROUND(0.99)

## Answer

    55
