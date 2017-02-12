Filter on a column that contains some `NULL`s.

## Query

    SELECT InvoiceId, BillingCity, BillingState, BillingCountry, Total
    FROM Invoice
    WHERE BillingState = 'ON'

## Answer

    48,Toronto,ON,Canada,0.99
    49,Ottawa,ON,Canada,1.98
    72,Ottawa,ON,Canada,3.96
    94,Ottawa,ON,Canada,5.94
    146,Ottawa,ON,Canada,0.99
    169,Toronto,ON,Canada,1.98
    180,Toronto,ON,Canada,13.86
    235,Toronto,ON,Canada,8.91
    267,Ottawa,ON,Canada,1.98
    278,Ottawa,ON,Canada,13.86
    333,Ottawa,ON,Canada,8.91
    364,Toronto,ON,Canada,1.98
    387,Toronto,ON,Canada,3.96
    409,Toronto,ON,Canada,5.94
