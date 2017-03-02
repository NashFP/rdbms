(require-extension lookup-table)
(require-extension utf8-srfi-13)
(require-extension data-structures)
(require-extension extras)
(require-extension csv)

(use csv-string)
;; This is the basic database engine
;;
;; Define a tag used to "poison" part of a where clause to
;; consider that part as being true
(define unavailable (gensym 'unav))

;; Load the table, splitting the CSV values and creating a list of dictionaries
;; where each row has a column->value dictionary
(define (load-table table-name)
  (with-input-from-file (string-concatenate (list "../tests/tables/" table-name ".csv"))
    (lambda()
      (let* ((parser (csv-parser))
             (lines (map (lambda (l) (csv-record->list (car (parser l)))) (read-lines)))
             (columns (car lines))
             (data (cdr lines)))
        (list columns
              (map (lambda (d) (alist->dict (map cons columns d) equal?)) data))))))

;; This is a specialized cartesian product that assumes that the first item can be a
;; multi-valued list, but the second only a single-valued list. It is used to progressively
;; join table columns, where the first two tables are joined, the results filtered, and
;; then the next table is joined to the previous resuls. A row is a list of (table . dict) values
;; where table is the table that the values came from, and dict is the dictionary of values
(define (product-1 a b all-b)
  (if (null? b) (product-1 (cdr a) all-b all-b)
    (if (null? a) '()
      (cons (cons (caar b) (car a)) (product-1 a (cdr b) all-b)))))
(define (product a b) (product-1 a b b))

;; Retrieve the dictionary for a named table from the row, or unavailable if the
;; row doesn't have values from that table yet
(define (table-by-name t row)
  (let ((tab (assoc t row)))
    (if tab (cdr tab)
      unavailable)))

;; Retrieve a value from a table, or unavailable if either the value isn't in the table
;; (although that shouldn't happen) or if the table is still unavailable
(define (value-by-name v tab)
  (if (equal? tab unavailable) unavailable
    (dict-ref tab v unavailable)))

;; Normally, if any of the args to a function are unavailable, the function result
;; is also unavailable. But, in order to properly filter, we need the db-and and db-or
;; functions to go ahead and evaluate. Since unavailable isn't #f, it is treated as true.
(define (apply-func func args)
  (if (or (eq? func db-and) (eq? func db-or))
    (apply func args)
    (if (member unavailable args) unavailable
        (apply func args))))

;; Evaluates a where expression. If it sees a pair with (table-name . column-name) it
;; retrieves the value from table-assoc, which is the current row. Otherwise, if it
;; sees a list that starts with a procedure, it evaluates all the arguments and then
;; calls the procedure. If any argument comes back as unavailable, the result of the
;; expression is unavailable.
(define (eval-expr expr table-assoc)
  (if (pair? expr)
    (if (procedure? (car expr))
      (let ((args (map (lambda (v) (eval-expr v table-assoc)) (cdr expr)))
            (func (car expr)))
          (apply-func func args))
      (let* ((t (car expr))
            (col (cdr expr))
            (tab (assoc t table-assoc)))
        (if tab
          (dict-ref (cdr tab) col unavailable)
          unavailable)))
    expr))

;; Load the tables

(define table-names '("album" "artist" "customer" "employee" "genre"
                      "invoice" "invoiceline" "mediatype" "playlist"
                      "playlisttrack" "track"))

(define loaded-tables
  (map (lambda (tn) (list tn (load-table tn))) table-names))

(define table-list
  (map (lambda (tn) (list tn (cadadr (assoc tn loaded-tables)))) table-names))

(define table-column-list
  (map (lambda (tn) (list tn (caadr (assoc tn loaded-tables)))) table-names))

(define (table-columns table)
  (cadr (assoc table table-column-list)))

;; Given a list of table names, return a list of (table-name table) pairs
(define (from tables)
  (map (lambda (t) (assoc t table-list)) tables))

;; Given a list of (table-name . column-name) values, retrieve the values from the row
(define (get-values vs row)
  (if (null? vs) '()
    (cons (value-by-name (cdar vs) (table-by-name (caar vs) row))
          (get-values (cdr vs) row))))

;; For a named table, load the data from the table, making each table row into
;; a pair (table-name . row)
(define (get-table-rows table-pair)
  (map (lambda (r) (list (cons (car table-pair) r))) (cadr table-pair)))

;; Remove any rows that don't match the where clause
(define (filter-result where tables)
  (filter (lambda (r) (eval-expr where r)) tables))

;; Given the current result and another table to add in, filter the new table
;; then do a cartesian product and then filter the results
(define (join-rows t1 t2 where)
  (filter-result where
    (product t1 (filter-result where (get-table-rows t2)))))

;; Returns all the rows that match the where clause for the list of tables
(define (get-rows tables where)
  (fold (lambda (t2 t1) (join-rows t1 t2 where))
        (filter-result where (get-table-rows (car tables))) (cdr tables)))

;; Compares two rows for sorting
(define (row-compare r1 r2 order-list)
  (if (null? order-list) #t
    (let* ((curr-col (car order-list))
           (r1-value (value-by-name (cdr curr-col) (table-by-name (car curr-col) r1)))
           (r2-value (value-by-name (cdr curr-col) (table-by-name (car curr-col) r2))))
      (if (string= r1-value r2-value) (row-compare r1 r2 (cdr order-list))
        (string< r1-value r2-value)))))

;; Performs a query and returns the values
(define (do-query vals tables where order-by)
  (map (lambda (r) (get-values vals r))
       (sort (get-rows (from tables) where)
             (lambda (r1 r2) (row-compare r1 r2 order-by)))))

;; Helper functions for queries
(define (db-and . l) (every (lambda (x) x) l))
(define (db-or . l) (any (lambda (x) x) l))

;; Here is an example query using this engine:
;; (do-query '(("album" . "Title")) '("artist" "album") (list db-and (list equal? '("artist" . "Name") "Led Zeppelin") (list equal? '("artist" . "ArtistId") '("album" . "ArtistId"))) '(("album" . "Title")))
;;
;; This query selects album.Title from the artist and album tables, looking for
;; artist.Name = "Led Zeppelin" and artist.ArtistId = album.ArtistId
;; then does an order-by album.Title
;; 
;; Here's a query that spans three tables and still returns very quickly:
;; (do-query '(("album" . "Title") ("track" . "Name")) '("artist" "album" "track") (list db-and (list equal? '("artist" . "Name") "AC/DC") (list equal? '("artist" . "ArtistId") '("album" . "ArtistId")) (list equal? '("track" . "AlbumId") '("album" . "AlbumId"))) '(("album" . "Title") ("track" . "Name")))
;;
;; The next task is to build a simple parser to take something
;; like a standard SQL command and generate the do-query expression that
;; would execute the query
