(require-extension utf8-srfi-13)
(require-extension extras)
(require-extension regex)

(include "select-parser.scm")
(include "database.scm")

;; Prepares the query by converting from the output format of the parser
;; to the input format of the database engine, checking table and column
;; names along the way
(define (prepare-query parsed-query)
  (let ((columns (second parsed-query))
         (tables (fourth parsed-query))
         (where (fifth parsed-query))
         (order-by (sixth parsed-query)))
    (check-tables columns tables where order-by)))

;; Check all the table names
(define (check-tables columns tables where orderby)
  (let ((checked-tables (map check-table-with-alias (delete "," tables))))
    (if (every identity checked-tables)
      (check-select-columns columns checked-tables where orderby))))

;; Check to see if this table exists, allow case-independent matching,
;; then use the name of the table as it in in the database
(define (check-table-with-alias table)
  (let* ((alias (if (equal? "" (second table)) (first table) (second table)))
        (table-name (first table))
        (checked-table (assoc table-name table-list string-ci=)))
    (if checked-table (list alias (car checked-table))
      (begin
        (format #t "Invalid table name: ~A~%" table)
        #f))))

;; Check the list of select columns
(define (check-select-columns columns checked-tables where order-by)
  (if (equal? columns "*")
    (check-columns-star checked-tables where order-by)
    (if (equal? columns "count(*)")
      (check-where '() checked-tables where order-by)
      (let ((checked-columns 
            (append-map 
              (lambda (col) (check-select-column col checked-tables))
              (delete "," columns))))
        (if (every identity checked-columns)
          (check-where checked-columns checked-tables where order-by)
          #f)))))

;; If the column list is just * and there is one table, use all
;; the columns from that one table
(define (check-columns-star checked-tables where order-by)
  (if (> (length checked-tables) 1)
    (begin
      (format #t "Unable to select * on more than one table~%")
      (list #f))
    (check-where (map (lambda (col) (cons (caar checked-tables) col))
           (table-columns (cadar checked-tables))) checked-tables where order-by)))

(define (check-table-alias table-alias checked-tables)
  (let ((table-spec (assoc table-alias checked-tables)))
    (if table-spec table-spec
      (begin
        (format #t "Invalid table name: ~A~%:" table-alias)))))

(define (is-column-in-table column table-spec)
  (find (lambda (col) (string-ci= column col)) (table-columns (cadr table-spec))))

(define (find-unique-column-table col-spec checked-tables)
  (let ((matching-tables (filter (lambda (t) (is-column-in-table col-spec t)) checked-tables)))
    (if matching-tables
      (if (= (length matching-tables) 1) (car matching-tables)
        (begin
          (format #t "Unqualified column ~A is ambiguous~%" col-spec)
          #f))
      (begin
          (format #t "Unqualified column ~A does not appear in any queried table~%" col-spec)
          #f))))

;; if there is no table qualifier on the column, make sure there
;; is only one table in the select
(define (check-select-column column checked-tables)
  (let ((table-spec (car column))
        (col-spec (cadr column)))
    (if (equal? table-spec "") 
      (let ((unique-table (find-unique-column-table col-spec checked-tables)))
        (if unique-table (check-select-column-table col-spec unique-table)
          (begin
            (format #t "Column ~A must have a table specifier (multiple tables)~%" col-spec)
            (list #f))))
      (let ((checked-table-spec (check-table-alias (car table-spec) checked-tables)))
        (if checked-table-spec (check-select-column-table col-spec checked-table-spec)
          (list #f))))))

;; Make sure the column is in the table. If the column name is *,
;; insert all the columns for that table
(define (check-select-column-table column checked-table)
  (if (equal? column "*")
    (map (lambda (col) (cons (car checked-table) col))
           (table-columns (cadr checked-table)))
    (let ((checked-column (find (lambda (col) (string-ci= column col)) (table-columns (cadr checked-table)))))
      (if checked-column (list (cons (car checked-table) checked-column))
        (begin
          (format #t "Invalid column ~A for table ~A~%" column checked-table)
          (list #f))))))

;; If the where clause exists, process it as if it is a list of expressions
;; to be or-ed together since that's how the parser returns them
(define (check-where checked-columns checked-tables where order-by)
  (if (equal? where "")
    (check-order-by checked-columns checked-tables (lambda (r) #t) order-by)
    (let ((checked-where 
            (check-where-or checked-tables (cadr where))))
      (if checked-where
        (check-order-by checked-columns checked-tables checked-where order-by)
        #f))))

(define (check-where-or checked-tables where)
  (let ((checked-where-and (map (lambda (w) (check-where-and checked-tables w)) (delete "or " where))))
    (if (every identity checked-where-and)
      (cons db-or checked-where-and)
      #f)))

;; Check a subset of where expressions as if they are a list of and-ed
;; expressions, because that's how the parser returns them
(define (check-where-and checked-tables where)
  (let ((checked-where-comps (map (lambda (w) (check-where-comp checked-tables w)) (delete "and " where))))
    (if (every identity checked-where-comps)
      (cons db-and checked-where-comps)
      #f)))

;; Check each side of a where comparison and the operator
(define (check-where-comp checked-tables where)
  (let ((first-expr (check-where-expr checked-tables (first where))))
    (if (equal? (second where) "isnull")
      (if first-expr
        (make-comparison (get-string-op "=") "=" first-expr "")
        #f)
      (let ((second-expr (check-where-expr checked-tables (cadadr where)))
            (comp-op (check-comp-op (caadr where))))
        (if (and first-expr second-expr comp-op)
          (make-comparison comp-op (caadr where) first-expr second-expr)
          #f)))))

;; If the expression is a string, strip the quotes
(define (check-where-expr checked-tables expr)
  (if (string? expr) 
    (if (= (string-prefix-length expr "'") 1)
      (substring/shared expr 1 (- (string-length expr) 1))
      expr)
    (check-column expr checked-tables)))

;; A list of comparison operators and their equivalent functions
(define string-op-to-function
  (list (cons "=" string=) (cons "!=" string<>) (cons "<" string<) (cons ">" string>)
    (cons ">=" string>=) (cons "<=" string<=)))

(define (ne a b) (not (= a b)))

(define numeric-op-to-function
  (list (cons "=" =) (cons "!=" ne) (cons "<" <) (cons ">" >)
    (cons ">=" >=) (cons "<=" <=)))

(define (check-comp-op op)
  (cdr (assoc op string-op-to-function)))

(define (get-string-op op)
  (cdr (assoc op string-op-to-function)))

(define (get-numeric-op op)
  (cdr (assoc op numeric-op-to-function)))

(define (make-comparison comp-op comp-op-name expr1 expr2)
  (let ((string-op (get-string-op comp-op-name))
        (numeric-op (get-numeric-op comp-op-name)))
    (list (lambda (a b) 
            (if (and (is-number a) (is-number b))
              (numeric-op (string->number a) (string->number b))
              (string-op a b))) expr1 expr2)))

;; If the column has no table qualifier, there must be only one table
(define (check-column column checked-tables)
  (let ((table-spec (car column))
        (col-spec (cadr column)))
    (if (equal? table-spec "") 
      (let ((unique-table (find-unique-column-table col-spec checked-tables)))
        (if unique-table (check-column-table col-spec unique-table)
          (begin
            (format #t "Column ~A must have a table specifier (multiple tables)~%" col-spec)
            #f)))
      (let ((checked-table-spec (check-table-alias (car table-spec) checked-tables)))
        (if checked-table-spec (check-column-table col-spec checked-table-spec)
          #f)))))

;; Make sure the column is in the table
(define (check-column-table column checked-table)
  (let ((checked-column (find (lambda (col) (string-ci= column col)) (table-columns (cadr checked-table)))))
    (if checked-column (cons (car checked-table) checked-column)
      (begin
        (format #t "Invalid column ~A for table ~A~%" column checked-table)
        #f))))

;; Check the column list in the order-by clause making sure they
;; are valid references
(define (check-order-by checked-columns checked-tables checked-where order-by)
  (if (equal? order-by "") (list checked-columns checked-tables checked-where '())
    (let ((checked-order-by (map (lambda (c) (check-column c checked-tables)) (delete "," (third order-by)))))
      (if (every identity checked-order-by)
        (list checked-columns checked-tables checked-where checked-order-by)
        #f))))

;; -------------------------------------------------------------
;; Formatting
;; -------------------------------------------------------------
;; This section is for formatting the output of the queries.

;; Compute the max widths of each column, starting with a base set
;; (either empty, or the widths of the column headers), then go through
;; each row in the result and update the max width needed to display
;; the column, although limit the max to a pre-set value (see col-max)
(define (max-column-widths query-result old-widths)
  (fold update-widths old-widths (map get-column-widths query-result)))

;; Compute each column width for a particular row
(define (get-column-widths row)
  (map string-length row))

;; Return the maximum of two column widths, but then have a max value of 40
(define (col-max a b)
  (min (max a b) 40))

;; Updates an existing list of max column widths with a set of values
;; from another row
(define (update-widths new-values old-values)
  (if (null? old-values)
    (if (null? new-values) '()
      new-values)
    (cons (col-max (car old-values) (car new-values))
          (update-widths (cdr new-values) (cdr old-values)))))

;; Formats a column specifier into table.column
(define (format-column col)
  (string-append (car col) "." (cdr col)))

;; Makes a list of dashes to create a separator between the column
;; heading and the data values
(define (make-dashes widths)
  (map (lambda (w) (xsubstring "-" 0 w)) widths))

;; Displays the results of a query
(define (display-results columns query-result)
  (if (= (length columns) 0) (begin (display (length query-result))(newline))
    (let* ((formatted-columns (map format-column columns))
          (col-widths (max-column-widths query-result (get-column-widths formatted-columns))))
      (display-row formatted-columns col-widths)
      (display-row (make-dashes col-widths) col-widths)
      (map (lambda (r) (display-row r col-widths)) query-result))))

;; Displays a row, separating each column by a |
(define (display-row row widths)
  (if (null? row) (begin (display "|")(newline))
    (begin
      (display "|")
      (display (string-pad-right (car row) (car widths)))
      (display-row (cdr row) (cdr widths)))))

;; Reads a query from the command-line and executes it
(define (read-loop)
  (display "Query: ")
  (let ((line (read-line)))
    (if (equal? line #!eof) #t
      (begin
        (let ((parsed (parse-string line sql-parser)))
          (if parsed 
            (let ((prepared (prepare-query parsed)))
              (if prepared
                (let ((results (apply do-query prepared)))
                  (display-results (car prepared) results))))))
        (read-loop)))))

;; Reads a query from a file and executes it
(define (read-sql-file filename)
  (let ((parsed (parse-file filename sql-parser)))
    (if parsed 
      (let ((prepared (prepare-query parsed)))
        (if prepared
          (let ((results (apply do-query prepared)))
                (display-results (car prepared) results)))))))

(define (load-file filename)
  (with-input-from-file filename
                        (lambda () (read-lines))))

(define (look-for-md-query lines curr-query curr-answer)
  (if (null? lines) (begin (format #t "Can't find ## Query~%") #f)
    (if (string= (string-trim-both (car lines)) "## Query")
      (read-md-query (cdr lines) curr-query curr-answer)
      (look-for-md-query (cdr lines) curr-query curr-answer))))

(define (read-md-query lines curr-query curr-answer)
  (if (null? lines) (begin (format #t "Can't find ## Answer~%") #f)
    (if (string= (string-trim-both (car lines)) "## Answer")
      (read-md-answer (cdr lines) curr-query curr-answer)
      (read-md-query (cdr lines) (string-trim-both (string-join (list curr-query (string-trim-both (car lines))) " ")) curr-answer))))

(define (read-md-answer lines curr-query curr-answer)
  (if (null? lines) (list curr-query (reverse curr-answer))
    (let ((trimmed (string-trim-both (car lines))))
      (if (> (string-length trimmed) 0)
        (read-md-answer (cdr lines) curr-query (cons trimmed curr-answer))
        (read-md-answer (cdr lines) curr-query curr-answer)))))

(define (load-md-file filename)
  (let* ((lines (load-file filename)))
    (look-for-md-query lines "" '())))

(define (add-quotes-if-needed s)
  (if (or (string-index s #\,) (string-contains s "\"\"")) (string-concatenate (list "\"" s "\"")) s))

(define (format-results-for-md is-count results)
  (if is-count (list (number->string (length results)))
    (map (lambda (r) (string-join (map add-quotes-if-needed r) ",")) results)))

(define (check-md-results is-count has-order-by results expected)
  (let ((comparison (if has-order-by
                      (equal? (format-results-for-md is-count results) expected)
                      (equal? (sort (format-results-for-md is-count results) string<) (sort expected string<)))))
    (if comparison
      (format #t "Passed.~%")
      (begin (format #t "Failed.~%~%Expected:~%")
            (map (lambda (l) (display l)(newline)) expected)
            (format #t "~%Got:~%")
            (map (lambda (l) (display l)(newline)) (format-results-for-md is-count results))))))
    
;; Reads a .md file, runs the query and checks the answer
(define (read-md-file filename)
  (let* ((md-query-data (load-md-file filename))
         (md-query (first md-query-data))
         (md-answer (second md-query-data))
         (parsed (parse-string md-query sql-parser)))
    (if parsed
      (let ((prepared (prepare-query parsed)))
        (if prepared
          (let ((results (apply do-query prepared)))
            (check-md-results (= (length (car prepared)) 0) 
                              (> (length (fourth prepared)) 0)
                              results md-answer)))))))


(if (> (length (argv)) 1)
  (let ((filename (second (argv))))
    (if (= (string-suffix-length filename ".md") 3)
      (read-md-file filename)
      (read-sql-file filename)))
  (read-loop))
