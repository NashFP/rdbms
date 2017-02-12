Use the `<>` operator to count tracks that aren't MPEG files.

## Query

    SELECT COUNT(*)
    FROM Track
    WHERE MediaTypeId <> 1

## Answer

    469
