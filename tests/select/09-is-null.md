Use the `IS NULL` operator to select rows with null fields.

## Query

    SELECT FirstName, LastName, City, State, Country
    FROM Customer
    WHERE PostalCode IS NULL

## Answer

    João,Fernandes,Lisbon,,Portugal
    Madalena,Sampaio,Porto,,Portugal
    Hugh,O'Reilly,Dublin,Dublin,Ireland
    Luis,Rojas,Santiago,,Chile
