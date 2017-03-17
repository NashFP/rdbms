Use a GROUP BY clause to figure out how much revenue is produced from which countries.

## Query

    SELECT BillingCountry, SUM(Total)
    FROM Invoice
    GROUP BY BillingCountry
    ORDER BY BillingCountry

## Answer

    Argentina,37.62
    Australia,37.62
    Austria,42.62
    Belgium,37.62
    Brazil,190.10
    Canada,303.96
    Chile,46.62
    Czech Republic,90.24
    Denmark,37.62
    Finland,41.62
    France,195.10
    Germany,156.48
    Hungary,45.62
    India,75.26
    Ireland,45.62
    Italy,37.62
    Netherlands,40.62
    Norway,39.62
    Poland,37.62
    Portugal,77.24
    Spain,37.62
    Sweden,38.62
    USA,523.06
    United Kingdom,112.86
