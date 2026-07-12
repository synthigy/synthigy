(ns synthigy.xsql.tokens
  "Tokenizer for the XSQL DSL.

   Single-pass linear scan over a source string. Produces a flat
   vector of tokens, including virtual structural tokens
   (`:indent`, `:dedent`, `:newline`, `:blank-line`, `:eof`) that
   encode block structure for the parser to consume.

   Token shape:
     {:type     :identifier|:string|:number|:arrow|:dash|:eq|:neq
                |:lt|:le|:gt|:ge|:colon|:comma|:dot|:lparen|:rparen
                |:param-ref
                |:indent|:dedent|:newline|:blank-line|:eof|:error
      :text     string slice from source (\"\" for zero-length tokens)
      :from     integer byte offset (inclusive)
      :to       integer byte offset (exclusive)
      :message  string  (only on :error tokens)}

   :param-ref tokens carry extra metadata for the parser/compiler:
      :param-name      placeholder name string (\"limit\")
      :param-type-raw  raw type token (\"int\") or nil
      :array?          true if the token ended in `[]`

   Indent semantics (Python-strict, per XSQL.md § Lexical rules):
     - At line start, count leading spaces+tabs as indent width.
     - Width > stack top         → emit :indent, push width.
     - Width < stack top         → emit zero-length :dedent and pop;
                                   repeat until width matches stack top.
     - Width == stack top        → no token, skip leading whitespace.
     - Line is empty / # comment → entire line consumed as :blank-line.
     - At EOF, synthesize :newline if last char wasn't \\n, then emit
       zero-length :dedent until stack drains to root, then :eof.")

;; ── Helpers ────────────────────────────────────────────────────────────────

;; XSQL identifiers are STRICT snake_case: [a-z_][a-z0-9_]* only.
;; `-` is a join sigil, never part of an ident. Uppercase is rejected.
;; Re-casing (kebab/camel) lives in the SDK/codegen layer, not XSQL source.
(def ^:private re-ident-start    #"[a-zA-Z_]")   ; allow uppercase to scan, then reject
(def ^:private re-ident-continue #"[a-zA-Z0-9_]") ; same — check in lex-ident
(def ^:private re-digit          #"[0-9]")

(defn- char-at
  "Return the 1-char string at position `i`, or nil for out-of-bounds."
  [^String s i]
  (when (and (>= i 0) (< i (count s)))
    (subs s i (inc i))))

(defn- ident-start?    [c] (and c (re-matches re-ident-start c)))
(defn- ident-continue? [c] (and c (re-matches re-ident-continue c)))
(defn- digit?          [c] (and c (re-matches re-digit c)))
(defn- ws?             [c] (or (= " " c) (= "\t" c)))

(defn- scan-while
  "Walk forward from `pos` while `pred?` holds for the current char.
   Return the index just past the last matching char."
  [s pos pred?]
  (let [n (count s)]
    (loop [i pos]
      (if (and (< i n) (pred? (char-at s i)))
        (recur (inc i))
        i))))

(defn- scan-indent
  "Count leading spaces/tabs from `pos`. Return the index after the
   leading whitespace. The width is `(- next-pos pos)`."
  [s pos]
  (scan-while s pos ws?))

(defn- find-eol
  "Walk forward from `pos` to the next \\n or EOF.
   Return the index of the \\n (or n if EOF reached first)."
  [s pos]
  (scan-while s pos #(and % (not= "\n" %))))

;; ── Content lexers (mid-line) ──────────────────────────────────────────────

(defn- lex-string
  "Quoted string literal starting at `pos`. Handles `\\\"` escapes.
   Returns one :string token; if EOF or \\n hit before closing quote,
   returns :error."
  [s pos]
  (let [n (count s)]
    (loop [i (inc pos)]
      (cond
        (>= i n)
        {:type :error :from pos :to n :text (subs s pos n)
         :message "unterminated string"}

        (= "\n" (char-at s i))
        {:type :error :from pos :to i :text (subs s pos i)
         :message "unterminated string (newline)"}

        (= "\\" (char-at s i))
        ;; skip the next char (escape)
        (recur (+ i 2))

        (= "\"" (char-at s i))
        {:type :string :from pos :to (inc i) :text (subs s pos (inc i))}

        :else
        (recur (inc i))))))

(defn- lex-number
  "Number literal — optional leading `-`, digits, optional `.digits`."
  [s pos]
  (let [start pos
        i (if (= "-" (char-at s pos)) (inc pos) pos)
        i (scan-while s i digit?)
        i (if (= "." (char-at s i))
            (scan-while s (inc i) digit?)
            i)]
    {:type :number :from start :to i :text (subs s start i)}))

(defn- lex-ident [s pos]
  (let [end  (scan-while s (inc pos) ident-continue?)
        text (subs s pos end)]
    (if (re-find #"[A-Z]" text)
      {:type :error :from pos :to end :text text
       :message (str "identifiers must be snake_case — \"" text "\" contains uppercase")}
      {:type :identifier :from pos :to end :text text})))

(defn- lex-param-ref
  "Lex `?name`, `?name:type`, `?name[]`, `?name:type[]`. The leading `?`
   is at `pos`. XSQL is named-only — a bare `?` or `?N` (positional)
   becomes an :error token; the linter surfaces a clear message.

   The token text includes the entire placeholder; metadata captures
   the parsed pieces so the parser and compiler don't have to re-scan."
  [s pos]
  (let [n     (count s)
        after (inc pos)
        nch   (char-at s after)]
    (cond
      ;; Bare `?` at EOF / whitespace / structural char — positional,
      ;; not supported in XSQL.
      (or (nil? nch) (not (ident-start? nch)))
      {:type :error :from pos :to after :text "?"
       :message "XSQL parameters must be named (?name[:type][])"}

      :else
      (let [name-end   (scan-while s after ident-continue?)
            param-name (subs s after name-end)
            ;; Optional `:type` — single colon. Reject a `::` cast so
            ;; downstream SQL casts in sql-template stay valid (XSQL
            ;; itself doesn't need them, but be consistent with sql_params).
            colon?     (and (= ":" (char-at s name-end))
                            (not= ":" (char-at s (inc name-end))))
            type-start (when colon? (inc name-end))
            type-end   (when type-start
                         (scan-while s type-start ident-continue?))
            type-raw   (when type-start
                         (subs s type-start type-end))
            ;; Optional trailing `[]` — must be adjacent, no whitespace.
            arr-pos    (or type-end name-end)
            array?     (and (= "[" (char-at s arr-pos))
                            (= "]" (char-at s (inc arr-pos))))
            final-end  (cond
                         array?     (+ arr-pos 2)
                         type-end   type-end
                         :else      name-end)]
        (cond
          ;; `?name:[]` — empty type between `:` and `[]`.
          (and colon? (= type-start type-end))
          {:type :error :from pos :to final-end
           :text (subs s pos final-end)
           :message "expected type token after `:`"}

          :else
          ;; Optional default — `=literal` IMMEDIATELY after (tight, no
          ;; whitespace, so it never collides with a predicate `=`). A quoted
          ;; string default scans to its closing quote; a bare literal (number /
          ;; true / false / word) scans to the next delimiter.
          (let [has-default? (= "=" (char-at s final-end))
                def-start    (inc final-end)
                def-end      (when has-default?
                               (cond
                                 ;; array literal `[…]` → scan to closing `]`
                                 (= "[" (char-at s def-start))
                                 (loop [i (inc def-start)]
                                   (cond (>= i n)               i
                                         (= "]" (char-at s i))  (inc i)
                                         :else                  (recur (inc i))))
                                 ;; quoted string → scan to closing quote
                                 (= "\"" (char-at s def-start))
                                 (loop [i (inc def-start)]
                                   (cond (>= i n)                i
                                         (= "\"" (char-at s i))  (inc i)
                                         :else                   (recur (inc i))))
                                 ;; bare literal → scan to delimiter
                                 :else
                                 (scan-while s def-start
                                             #(and % (not (#{" " "\t" "\n" "\r"
                                                             "," ")" "]"} %))))))
                end          (if has-default? def-end final-end)]
            (cond-> {:type :param-ref
                     :from pos :to end
                     :text (subs s pos end)
                     :param-name param-name
                     :param-type-raw type-raw
                     :array? array?}
              has-default? (assoc :param-default (subs s def-start def-end)))))))))

(defn- lex-content-token
  "Lex one token of mid-line content starting at `pos`.
   Pre: `(char-at s pos)` is non-nil and non-newline and non-whitespace."
  [s pos]
  (let [c  (char-at s pos)
        c2 (char-at s (inc pos))]
    (cond
      ;; Two-char operators
      (and (= "-" c) (= ">" c2)) {:type :arrow :from pos :to (+ pos 2) :text "->"}
      (and (= "!" c) (= "=" c2)) {:type :neq   :from pos :to (+ pos 2) :text "!="}
      (and (= "<" c) (= "=" c2)) {:type :le    :from pos :to (+ pos 2) :text "<="}
      (and (= ">" c) (= "=" c2)) {:type :ge    :from pos :to (+ pos 2) :text ">="}

      ;; Single-char operators / punct
      (= "=" c) {:type :eq     :from pos :to (inc pos) :text "="}
      (= "<" c) {:type :lt     :from pos :to (inc pos) :text "<"}
      (= ">" c) {:type :gt     :from pos :to (inc pos) :text ">"}
      (= ":" c) {:type :colon  :from pos :to (inc pos) :text ":"}
      (= "," c) {:type :comma  :from pos :to (inc pos) :text ","}
      (= "." c) {:type :dot    :from pos :to (inc pos) :text "."}
      (= "(" c) {:type :lparen :from pos :to (inc pos) :text "("}
      (= ")" c) {:type :rparen :from pos :to (inc pos) :text ")"}

      ;; Dash disambiguation:
      ;;  - digit after (`-5`)                     → negative number
      ;;  - word-char on BOTH sides (tight `a-b`)  → a kebab identifier attempt;
      ;;    XSQL is snake_case ONLY, so this is a hard error (not a silent
      ;;    `a` − `b` split). The clean "use snake_case" message guides the fix.
      ;;  - otherwise (`-roles`, `a - b`)          → the join marker.
      (= "-" c)
      (cond
        (digit? c2) (lex-number s pos)
        (and (ident-continue? (char-at s (dec pos))) (ident-start? c2))
        {:type :error :from pos :to (inc pos) :text "-"
         :message "identifiers must be snake_case (use '_' not '-')"}
        :else {:type :dash :from pos :to (inc pos) :text "-"})

      ;; String literal
      (= "\"" c) (lex-string s pos)

      ;; Named parameter placeholder
      (= "?" c) (lex-param-ref s pos)

      ;; Number
      (digit? c) (lex-number s pos)

      ;; Identifier (covers keywords too — parser specializes)
      (ident-start? c) (lex-ident s pos)

      ;; Mid-line `#` is invalid per spec — comments only at line start.
      (= "#" c) {:type :error :from pos :to (inc pos) :text "#"
                 :message "comments must start at the beginning of a line"}

      :else
      {:type :error :from pos :to (inc pos) :text c
       :message (str "unexpected character: " (pr-str c))})))

;; ── Main loop ──────────────────────────────────────────────────────────────

(defn tokenize
  "Tokenize an XSQL source string. Returns a vector of tokens
   ending in `{:type :eof …}`."
  [^String src]
  (let [n (count src)]
    (loop [pos             0
           stack           [0]            ; indent depth stack
           at-line-start?  true
           out             []]
      (cond
        ;; ── EOF ─────────────────────────────────────────────────────
        (>= pos n)
        (let [prev-char (when (pos? n) (char-at src (dec n)))
              ;; If the source ended without a trailing \n on a content
              ;; line, synthesize a zero-length :newline so the parser
              ;; sees a clean line terminator.
              out (cond-> out
                    (and (pos? n)
                         (not= "\n" prev-char)
                         (not at-line-start?))
                    (conj {:type :newline :from n :to n :text ""}))
              ;; Drain the indent stack with zero-length :dedent tokens.
              dedents (vec (repeat (dec (count stack))
                                   {:type :dedent :from n :to n :text ""}))
              out (into out dedents)]
          (conj out {:type :eof :from n :to n :text ""}))

        ;; ── At line start: handle indent / dedent / blank lines ─────
        at-line-start?
        (let [ind-end   (scan-indent src pos)
              width     (- ind-end pos)
              next-char (char-at src ind-end)]
          (cond
            ;; Empty line, comment line, or end-of-input after ws:
            ;; consume the whole line (incl. \n if any) as :blank-line.
            (or (nil? next-char) (= "\n" next-char) (= "#" next-char))
            (let [eol (find-eol src pos)
                  end (if (and (< eol n) (= "\n" (char-at src eol)))
                        (inc eol) eol)]
              (recur end stack true
                     (conj out {:type :blank-line
                                :from pos :to end
                                :text (subs src pos end)})))

            ;; Deeper indent: emit :indent, push, leave line-start state.
            (> width (peek stack))
            (recur ind-end (conj stack width) false
                   (conj out {:type :indent
                              :from pos :to ind-end
                              :text (subs src pos ind-end)}))

            ;; Shallower: emit one zero-length :dedent, pop, stay at
            ;; line-start so the next iteration re-checks (multi-pop).
            (< width (peek stack))
            (recur pos (pop stack) true
                   (conj out {:type :dedent :from pos :to pos :text ""}))

            ;; Same indent — skip leading ws and proceed mid-line.
            :else
            (recur ind-end stack false out)))

        ;; ── Mid-line ───────────────────────────────────────────────
        :else
        (let [c (char-at src pos)]
          (cond
            (= "\n" c)
            (recur (inc pos) stack true
                   (conj out {:type :newline :from pos :to (inc pos) :text "\n"}))

            (ws? c)
            ;; skip inline whitespace
            (recur (inc pos) stack false out)

            :else
            (let [tok (lex-content-token src pos)]
              (recur (:to tok) stack false (conj out tok)))))))))
