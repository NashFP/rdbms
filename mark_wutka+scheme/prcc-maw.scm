;;;;
;;  Copyright (C) 2012, Wei Hu
;;  All rights reserved.
;;
;;  Redistribution and use in source and binary forms, with or without
;;  modification, are permitted provided that the following conditions are met:
;;
;;  Redistributions of source code must retain the above copyright notice, this
;;  list of conditions and the following disclaimer. 
;;  Redistributions in binary form must reproduce the above copyright notice,
;;  this list of conditions and the following disclaimer in the documentation
;;  and/or other materials provided with the distribution. 
;;  Neither the name of the author nor the names of its contributors may be
;;  used to endorse or promote products derived from this software without
;;  specific prior written permission. 
;;
;;  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;;  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;;  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
;;  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
;;  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
;;  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
;;  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
;;  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
;;  POSSIBILITY OF SUCH DAMAGE.
;;;;


(module prcc-maw (char
              <c>
              seq
              <and>
              sel
              <or>
              one?
              <?>
              rep
              <*>
	      rep_
              <*_>
              rep+
              <+>
	      rep+_
              <+_>
              pred
              <&>
              pred!
              <&!>
              eof
              act
              <@>
	      lazy
              neg
              <^>
              regexp-parser
              regexp-parser-with-check
              <r>
              cached
              ;; helpers
              str
              <s>
              one-of
              join+
              join+_
              ind
              <#>
              <w>
              <space>
              <w*>
              <s*>
              <w+>
              <s+>
              even
              odd
              seq_
              <and_>
              ;;
              parse-file
              parse-string
              parse-port)
  
  (import chicken scheme)

  (use srfi-1)
  (use srfi-14)
  (use srfi-69)
  (use srfi-41)
  (use streams-utils)
  (use stack)
  (use type-checks)
  (use irregex)
  (use data-structures)
  (use utils)
  (use record-variants)

  (define-record-variant ctxt
    (inline)
    name
    input-stream
    stack
    pos
    line
    col
    err-pos
    err-line
    err-col
    err-msg
    caching?
    cache)

  (define (%make-ctxt n s #!optional (c? #f))
    (let ((ctxt (make-ctxt
                  n
                  s
                  (make-stack)
                  0
                  0
                  0
                  0
                  0
                  0
                  ""
                  c?
                  (make-hash-table))))
      ctxt))

  ;; cache computation results based on combintor and stream position
  (define-inline (combinator-cache p ctxt)
    (let ((c (ctxt-cache ctxt)))
      (if (not (hash-table-exists? c p))
        (hash-table-set! c p (make-hash-table)))
      (hash-table-ref c p)))

  (define-inline (apply-c p ctxt #!optional (c? #f))
    (if (or (ctxt-caching? ctxt) c?)
      (let* ((cache (combinator-cache p ctxt))
             (id (ctxt-pos ctxt)))
        (if (hash-table-exists? cache id)
          (let ((r (hash-table-ref cache id)))
            (read-chars (- (list-ref r 1) id) ctxt))
          (let ((r (p ctxt)))
            (hash-table-set! cache id
              (list r
                    (ctxt-pos ctxt)
                    (ctxt-err-pos ctxt)
                    (ctxt-err-line ctxt)
                    (ctxt-err-col ctxt)
                    (ctxt-err-msg ctxt)))))
        (let* ((r (hash-table-ref cache id))
               (rr (list-ref r 0)))
          (if (not rr)
            (begin
              (ctxt-err-pos-set! ctxt (list-ref r 2))
              (ctxt-err-line-set! ctxt (list-ref r 3))
              (ctxt-err-col-set! ctxt  (list-ref r 4))
              (ctxt-err-msg-set! ctxt  (list-ref r 5))))
          rr))
      (p ctxt)))

  ;; end of stream
  (define-inline (end-of-stream? ctxt)
    (stream-null? (ctxt-input-stream ctxt)))

  ;; error report
  (define (report-error ctxt)
    (display "parsing \'")
    (display (ctxt-name ctxt))
    (display "\' failed:\n\t")
    (display (ctxt-err-msg ctxt))
    (display "@(")
    (display (+ (ctxt-err-line ctxt) 1))
    (display ", ")
    (display (+ (ctxt-err-col ctxt) 1))
    (display ")\n"))

  (define-inline (record-error ctxt . msg)
    (ctxt-err-pos-set!  ctxt (ctxt-pos ctxt))
    (ctxt-err-line-set! ctxt (ctxt-line ctxt))
    (ctxt-err-col-set!  ctxt (ctxt-col ctxt))
    (ctxt-err-msg-set!  ctxt msg))

  ;; read/rewind pair for performance
  (define-inline (update-pos-line-col str ctxt #!optional (op +))
    (ctxt-pos-set! ctxt (op (ctxt-pos ctxt) (string-length str)))
    (let* ((ss (string-split str "\n" #t))
           (ssl (length ss))
           (ssll (string-length (car (reverse ss)))))
      (if (= ssl 1)
        (ctxt-col-set! ctxt (op (ctxt-col ctxt) ssll))
        (ctxt-col-set! ctxt ssll))
      (ctxt-line-set! ctxt (op (ctxt-line ctxt) (- ssl 1)))))

  (define-inline (read-chars n ctxt)
    (let* ((nc (stream-take n (ctxt-input-stream ctxt)))
           (str (list->string (stream->list nc))))
      (ctxt-input-stream-set! ctxt (stream-drop n (ctxt-input-stream ctxt)))
      (stack-push! (ctxt-stack ctxt) nc)
      (update-pos-line-col str ctxt)
      str))

  (define-inline (rewind n ctxt)
    (letrec ((l (lambda (i)
        (if (not (= i n))
          (let* ((nc (stack-pop! (ctxt-stack ctxt)))
                 (str (list->string (stream->list nc))))
            (ctxt-input-stream-set! ctxt (stream-append nc (ctxt-input-stream ctxt)))
            (update-pos-line-col str ctxt -)
            (l (+ i 1)))))))
      (l 0)))

  ;; parse a char
  (define (char c)
    (check-char 'char c)
    (lambda (ctxt)
      (if (end-of-stream? ctxt)
        (begin
          (record-error ctxt "end of stream")
          #f)
        (let ((ic (stream-car (ctxt-input-stream ctxt))))
          (if (equal? ic c)
             (read-chars 1 ctxt) 
             (begin
               (record-error ctxt "expect:" c ";but got:" ic)
               #f))))))
  (define <c> char)

  ;; seqence of parsers
  (define (seq fp . lst)
    (check-procedure 'seq fp)
    (for-each (lambda (p)
        (check-procedure 'seq p))
      lst)
    (lambda (ctxt)
      (let* ((csc (stack-count (ctxt-stack ctxt)))
             (fr (apply-c fp ctxt))
             (lr (fold (lambda (cp pr)
                   (if pr
                     (let ((cr (apply-c cp ctxt)))
                       (if cr
                         (append pr (list cr))
                         #f))
                     #f))
                   (if fr
                     (list fr)
                     #f)
                   lst)))
         (if lr
           lr
           (begin
             (rewind (- (stack-count (ctxt-stack ctxt)) csc) ctxt)
             #f)))))
  (define <and> seq)

  ;; ordered selective parsers
  (define (sel . lst)
    (for-each (lambda (p)
        (check-procedure 'seq p))
      lst)
    (lambda (ctxt)
      (fold (lambda (cp r)
              (if r
                r
                (let ((cr (apply-c cp ctxt)))
                    (if cr
                      cr
                      #f))))
        #f
        lst)))
  (define <or> sel)

  ;; repeat 0 - infinite times
  (define (rep p)
    (check-procedure 'rep p)
    (lambda (ctxt)
      (letrec ((lp (lambda (r)
                  (let ((rr (apply-c p ctxt)))
                    (if rr
                      (lp (append r (list rr)))
                      r)))))
        (lp `()))))
  (define <*> rep)

  ;; null
  (define (zero)
    (lambda (ctxt)
      ""))
  (define <null> zero)

  ;; appear once or zero
  (define (one? p)
    (check-procedure 'one? p)
    (sel p (zero)))
  (define <?> one?)

  ;; repeat 1 - infinite times
  (define (rep+ p)
    (check-procedure 'rep+ p)
    (act
      (seq p (rep p))
      (lambda (o)
        (cons (car o) (cadr o)))))
  (define <+> rep+)

  ;; predicate
  (define (pred p pd #!optional (n #f))
    (check-procedure 'pred p)
    (check-procedure 'pred pd)
    (lambda (ctxt)
      (let ((pr (apply-c p ctxt))
            (csc (stack-count (ctxt-stack ctxt))))
        (if pr
          (let ((pdr (apply-c pd ctxt)))
            (if (if n (not pdr) pdr)
              (begin
                (if (not n) (rewind (- (stack-count (ctxt-stack ctxt)) csc) ctxt))
                pr)
              (begin
                (if n (rewind (- (stack-count (ctxt-stack ctxt)) csc) ctxt))
                #f)))
          #f))))
  (define <&> pred)

  (define (pred! p pd)
    (check-procedure 'pred! p)
    (check-procedure 'pred! pd)
    (pred p pd #t))
  (define <&!> pred!)

  ;; end of file
  (define (eof)
    (lambda (ctxt)
      (if (end-of-stream? ctxt)
        ""
        (begin
          (record-error ctxt "expect: end of file")
          #f))))

  ;; neg
  (define (neg p)
    (check-procedure 'neg p)
    (lambda (ctxt)
      (let ((csc (stack-count (ctxt-stack ctxt)))
            (r (apply-c p ctxt)))
        (if r
          (begin
            (rewind (- (stack-count (ctxt-stack ctxt)) csc) ctxt)
            (record-error ctxt "expect: parsing failure")
            #f)
            (read-chars (- (+ (ctxt-err-pos ctxt) 1) (ctxt-pos ctxt)) ctxt)))))
  (define <^> neg)

  ;; add action for parser to process the output
  (define (act p #!optional (succ #f) (fail #f))
    (check-procedure 'act p)
    (if succ
      (check-procedure 'act succ))
    (if fail
      (check-procedure 'act fail))
    (lambda (ctxt)
      (let ((pr (apply-c p ctxt)))
        (if pr
	  (if succ
            (begin
              (succ pr))
	    pr)
	  (if fail
  	    (begin
              (fail (ctxt-err-msg ctxt)))
	    #f)))))
  (define <@> act)

  ;; lazy
  (define-syntax lazy
    (syntax-rules ()
      ((_ p)
       (lambda (ctxt)
         ((lambda (c)
            (p c)) ctxt)))))

  ;; regexp
  (define (regexp-parser r #!optional (cl 10))
    (check-string 'regexp-parser r)
    (lambda (ctxt)
      (if (not (end-of-stream? ctxt))
        (let ((str (ctxt-input-stream ctxt))
              (re (string-append "^" r))
              (rc (make-irregex-chunker
                    (lambda (str) (if (stream-null? (stream-cdr str)) #f (stream-cdr str)))
                    (lambda (str) (string (stream-car str))))))
          (let ((rr (irregex-search/chunked re rc str)))
            (if rr
              (let ((rrr (irregex-match-substring rr)))
                (read-chars (string-length rrr) ctxt))
              (begin
                (record-error ctxt "regexp \'" r "\' match failed")
                #f))))
         (begin
           (record-error ctxt "expect:" r ";but got: end of file")
           #f))))
  (define <r> regexp-parser)

  (define (regexp-parser-with-check r keyword-checker #!optional (cl 10))
    (lambda (ctxt)
      (if (not (end-of-stream? ctxt))
        (let ((str (ctxt-input-stream ctxt))
              (re (string-append "^" r))
              (rc (make-irregex-chunker
                    (lambda (str) (if (stream-null? (stream-cdr str)) #f (stream-cdr str)))
                    (lambda (str) (string (stream-car str))))))
          (let ((rr (irregex-search/chunked re rc str)))
            (if rr
              (let ((rrr (irregex-match-substring rr)))
                (if (not (keyword-checker rrr))
                  (read-chars (string-length rrr) ctxt)
                  (begin
                    (record-error ctxt "regexp \'" r "\' match found keyword")
                    #f)))
              (begin
                (record-error ctxt "regexp \'" r "\' match failed")
                #f))))
         (begin
           (record-error ctxt "expect:" r ";but got: end of file")
           #f))))

  (define (cached p)
    (lambda (ctxt)
      (apply-c p ctxt #t)))

  ;; helpers

  ;; a string
  (define (str s)
    (check-string 'str s)
    (lambda (ctxt)
      (let* ((sl (string-length s))
             (is (apply string
                   (stream->list sl
                                (ctxt-input-stream ctxt)))))
        (if (equal? is s)
           (read-chars sl ctxt)
           (begin
             (record-error ctxt "expect:" s ";but got:" is)
             #f)))))
  (define <s> str)

  ;; match one char in a string
  (define (one-of str)
    (check-string 'one-of str)
    (apply sel
      (map
        (lambda (c)
          (char c))
        (string->list str))))

  ;; join
  (define (join+ p0 p1)
    (check-procedure 'join+ p0)
    (check-procedure 'join+ p1)
    (act
      (seq p0 (act
                (rep (seq p1 p0))
                (lambda (o)
                  (apply append o))))
      (lambda (o)
        (cons (car o) (cadr o)))))

  ;; index
  (define (ind p index)
    (check-procedure 'ind p)
    (check-number 'ind index)
    (act
      p
      (lambda (o)
        (list-ref o index))))
  (define <#> ind)

  (define (<w>)
    (<r> "\\w"))

  (define (<space>)
    (<r> "\\s"))

  (define (<w*>)
    (<r> "\\w*"))

  (define (<s*>)
    (<r> "\\s*"))

  (define (<w+>)
    (<r> "\\w+"))

  (define (<s+>)
    (<r> "\\s+"))

  (define (even p)
    (check-procedure 'even p)
    (act p
      (lambda (o)
        (car (fold (lambda (oo i)
          (if (even? (cdr i))
            (cons (append (car i) (list oo)) (+ (cdr i) 1))
            (cons (car i) (+ (cdr i) 1))))
          (cons `() 0)
          o)))))

  (define (odd p)
    (check-procedure 'odd p)
    (act p
      (lambda (o)
        (car (fold (lambda (oo i)
          (if (odd? (cdr i))
            (cons (append (car i) (list oo)) (+ (cdr i) 1))
            (cons (car i) (+ (cdr i) 1))))
          (cons `() 0)
          o)))))

  (define (join+_ p0 p1 #!key (skip (sel (<s*>) (eof))))
    (check-procedure 'join+_ p0)
    (check-procedure 'join+_ p1)
    (check-procedure 'join+_ skip)
    (act (join+ p0 (seq skip p1 skip))
      (lambda (o)
        (car (fold (lambda (oo i)
            (if (even? (cdr i))
              (cons (append (car i) (list oo)) (+ (cdr i) 1))
              (cons (append (car i) (list (cadr oo))) (+ (cdr i) 1))))
          (cons `() 0)
          o)))))

  (define (seq_ #!rest lst #!key (skip (sel (<s*>) (eof))))
    (check-procedure 'seq_ skip)
    (let* ((nlst (car (fold (lambda (p i)
                     (if (cdr i)
                       (if (equal? p #:skip)
                         (cons (car i) #f)
                         (begin
			   ;(check-procedure 'seq_ p)
			   (cons (append (car i) (list p)) #t)))
                       (cons (car i) #t)))
                   (cons `() #t)
                   lst)))
           (l (fold (lambda (p i)
                  (if (equal? i `())
                    (list p)
                    (append i (list skip p))))
               `()
               nlst)))
      (even (apply seq l))))
  (define <and_> seq_)

  (define (rep+_ p #!key (skip (sel (<s*>) (eof))))
    (check-procedure 'rep+_ p)
    (check-procedure 'rep+_ skip)
    (even (join+ p skip)))
  (define <+_> rep+_)

  (define (rep_ p #!key (skip (sel (<s*>) (eof))))
    (check-procedure 'rep_ p)
    (check-procedure 'rep_ skip)
    (<or> (rep+_ p skip: skip)
	  (act (zero) (lambda (o) `()))))
  (define <*_> rep_)

  ;; parse
  (define (parse p n s #!optional (c? #f))
    (let* ((ctxt (%make-ctxt n s c?))
           (r (p ctxt)))
      (if r r
        (begin
          (report-error ctxt)
          #f))))

  ;; parse file
  (define (parse-file file p #!optional (c? #f))
    (check-string 'parse-file file)
    (check-procedure 'parse-file p)
    (parse p file (file->stream file) c?))

  ;; parse string
  (define (parse-string str p #!optional (c? #f))
    (check-string 'parse-string str)
    (check-procedure 'parse-string p)
    (parse p str (list->stream (string->list str)) c?))
  
  ;; parse from port
  (define (parse-port port p #!optional (c? #f))
    (check-input-port 'parse-port port)
    (check-procedure 'parse-port p)
    (parse p (port-name) (port->stream port) c?)))


