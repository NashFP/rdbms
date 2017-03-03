(include "prcc-maw.scm")

(use prcc-maw)

(define where (<r> "[Ww][Hh][Ee][Rr][Ee] "))
(define order (<r> "[Oo][Rr][Dd][Ee][Rr] "))
(define by (<r> "[Bb][Yy] "))

(define (keyword-check k)
  (or (string-ci= k "where") (string-ci= k "order") (string-ci= k "by")))

(define identifier (regexp-parser-with-check "[A-Za-z_][A-Za-z0-9_]*" keyword-check))

(define table-qualifier
  (seq_ identifier (<c> #\.)))

(define table-qualifier?
  (<?> table-qualifier))

(define select-column
  (<or>
    (seq_ table-qualifier (<c> #\*))
    (seq_ table-qualifier? identifier)))

(define select-columns 
  (<or>
    (<c> #\*)
    (join+_ select-column (<c> #\,))))

(define table
  (seq_ identifier (<?> identifier)))

(define tables
  (join+_ table (<c> #\,)))

(define column
  (seq_ table-qualifier? identifier))

(define columns
  (join+_ column (<c> #\,)))

(define constant
  (<or>
    (<r> "'[^']*'")
    (<r> "[0-9][0-9]*[.]?[0-9]*")))

(define expr
  (<or>
    column
    constant))

(define comp
  (<or>
    (<s> "=")
    (<s> "!=")
    (<s> ">=")
    (<s> "<=")
    (<s> ">")
    (<s> "<")))

(define comparison
  (seq_ expr comp expr))

(define where-expr
  (<or>
    comparison
    (seq_ (<c> #\() (lazy where-or-exprs) (<c> #\)))))

(define where-and-exprs
  (join+_ where-expr (<@> (<r> "[Aa][Nn][Dd] ") (lambda (x) "and "))))

(define where-or-exprs
  (join+_ where-and-exprs (<@> (<r> "[Oo][Rr] ") (lambda (x) "or "))))

(define where-clause where-or-exprs)

(define sql-parser
  (seq_
    (<r> "[Ss][Ee][Ll][Ee][Cc][Tt] ")
    select-columns
    (<r> "[Ff][Rr][Oo][Mm] ")
    tables
    (<?>
      (seq_ where where-clause))
    (<?>
      (seq_ order by columns))
    skip: (<or> (<s*>) (eof))))
