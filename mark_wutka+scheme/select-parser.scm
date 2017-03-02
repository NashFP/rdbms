(require-extension prcc)

;; This is a parser SQl queries using a parser-combinator library

(define identifier (<r> "[A-Za-z_][A-Za-z0-9_]*"))

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

(define tables
  (join+_ identifier (<c> #\,)))

(define column
  (seq_ table-qualifier? identifier))

(define columns
  (join+_ column (<c> #\,)))

(define constant
  (<or>
    (<r> "'[^']*'")))

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
      (seq_
        (<r> "[Ww][Hh][Ee][Rr][Ee] ")
        where-clause))
    (<?>
      (seq_
        (<r> "[Oo][Rr][Dd][Ee][Rr] ")
        (<r> "[Bb][Yy] ")
        columns))
    skip: (<or> (<s*>) (eof))))
