Query multiple fields of a row.

## Query

    SELECT BillingAddress,BillingCity,BillingState,BillingCountry,BillingPostalCode
    FROM Invoice
    WHERE InvoiceId = 170

## Answer

    194A Chain Lake Drive,Halifax,NS,Canada,B3S 1C5
