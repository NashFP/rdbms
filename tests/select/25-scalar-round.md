`ROUND()` can also be used in a `WHERE` clause, applied to each row.

## Query

    SELECT COUNT(*)
    FROM Invoice
    WHERE ROUND(Total) = 1

## Answer

    55
