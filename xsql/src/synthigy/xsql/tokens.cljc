;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.xsql.tokens
  "Tokenizer for the XSQL DSL — single-pass linear scan producing a flat
   token vector, including virtual structural tokens (:indent :dedent
   :newline :blank-line :eof) that encode block structure. Token shape:
   `{:type :text :from :to :message?}`. Indent handling is Python-strict
   (XSQL.md § Lexical rules) — see docs for the full state machine."
  (:require [clojure.string :as str]))

;; ── Helpers ────────────────────────────────────────────────────────────────

;; XSQL identifiers are STRICT snake_case: [a-z_][a-z0-9_]* only.
;; `-` is a join sigil, never part of an ident. Uppercase is rejected.
(def ^:private re-ident-start    #"[a-zA-Z_]")   ; allow uppercase to scan, then reject
(def ^:private re-ident-continue #"[a-zA-Z0-9_]")
(def ^:private re-digit          #"[0-9]")

(defn char-at
  "1-char string at position `i`, or nil for out-of-bounds."
  [^String s i]
  (when (and (>= i 0) (< i (count s)))
    (subs s i (inc i))))

(defn ident-start?    [c] (and c (re-matches re-ident-start c)))
(defn ident-continue? [c] (and c (re-matches re-ident-continue c)))
(defn digit?          [c] (and c (re-matches re-digit c)))
(defn ws?             [c] (or (= " " c) (= "\t" c)))

(defn scan-while
  "Index just past the last char from `pos` for which `pred?` holds."
  [s pos pred?]
  (let [n (count s)]
    (loop [i pos]
      (if (and (< i n) (pred? (char-at s i)))
        (recur (inc i))
        i))))

(defn scan-indent
  "Index after leading spaces/tabs from `pos`."
  [s pos]
  (scan-while s pos ws?))

(defn find-eol
  "Index of the next \\n from `pos`, or n if EOF reached first."
  [s pos]
  (scan-while s pos #(and % (not= "\n" %))))

;; ── Content lexers (mid-line) ──────────────────────────────────────────────

(defn lex-string
  "Quoted string literal starting at `pos`; :error if EOF/newline hit
   before the closing quote."
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

(defn lex-number
  "Number literal — optional leading `-`, digits, optional `.digits`."
  [s pos]
  (let [start pos
        i (if (= "-" (char-at s pos)) (inc pos) pos)
        i (scan-while s i digit?)
        i (if (= "." (char-at s i))
            (scan-while s (inc i) digit?)
            i)]
    {:type :number :from start :to i :text (subs s start i)}))

(defn lex-ident [s pos]
  (let [end  (scan-while s (inc pos) ident-continue?)
        text (subs s pos end)]
    (if (re-find #"[A-Z]" text)
      {:type :error :from pos :to end :text text
       :message (str "identifiers must be snake_case — \"" text "\" contains uppercase")}
      {:type :identifier :from pos :to end :text text})))

(defn lex-param-ref
  "Lex `?name`, `?name:type`, `?name[]`, `?name:type[]` at `pos`. XSQL is
   named-only — a bare `?`/`?N` becomes an :error token. Metadata
   captures the parsed pieces so parser/compiler don't have to re-scan."
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
            ;; `?name?` — optional marker, tight after the name, before
            ;; `:type`. Absent param ⇒ the enclosing predicate is dropped
            ;; at compile instead of raising PARAM_MISSING.
            optional?  (= "?" (char-at s name-end))
            mark-end   (if optional? (inc name-end) name-end)
            ;; Optional `:type` — single colon. Reject a `::` cast so
            ;; downstream SQL casts in sql-template stay valid (XSQL
            ;; itself doesn't need them, but be consistent with sql_params).
            colon?     (and (= ":" (char-at s mark-end))
                            (not= ":" (char-at s (inc mark-end))))
            type-start (when colon? (inc mark-end))
            type-end   (when type-start
                         (scan-while s type-start ident-continue?))
            type-raw   (when type-start
                         (subs s type-start type-end))
            ;; Optional `(a, b, c)` restriction set — tight after the type
            ;; (`?sort:order(a, b)`) or, for untyped order params, tight
            ;; after the name (`?sort(a, b)`).
            paren-at   (cond
                         (and type-end (pos? (- type-end type-start))
                              (= "(" (char-at s type-end)))       type-end
                         (and (not colon?) (= "(" (char-at s mark-end))) mark-end)
            args-close (when paren-at
                         (loop [i (inc paren-at)]
                           (cond (>= i n)               nil
                                 (= ")" (char-at s i))  i
                                 (= "\n" (char-at s i)) nil
                                 :else                  (recur (inc i)))))
            type-args  (when args-close
                         (->> (str/split (subs s (inc paren-at) args-close) #",")
                              (map str/trim)
                              (remove empty?)
                              vec))
            args-end   (when args-close (inc args-close))
            ;; Optional trailing `[]` — must be adjacent, no whitespace.
            arr-pos    (or args-end type-end mark-end)
            array?     (and (= "[" (char-at s arr-pos))
                            (= "]" (char-at s (inc arr-pos))))
            final-end  (cond
                         array?     (+ arr-pos 2)
                         args-end   args-end
                         type-end   type-end
                         :else      mark-end)]
        (cond
          ;; `?name:[]` — empty type between `:` and `[]`.
          (and colon? (= type-start type-end))
          {:type :error :from pos :to final-end
           :text (subs s pos final-end)
           :message "expected type token after `:`"}

          ;; `?name:type(` with no closing paren on the same line.
          (and paren-at (nil? args-close))
          {:type :error :from pos :to (or type-end pos)
           :text (subs s pos (or type-end pos))
           :message "unterminated `(…)` restriction set on parameter type"}

          ;; `?name:type()` / non-identifier entries — empty or malformed set.
          (and args-close (or (empty? type-args)
                              (some #(not (re-matches #"[a-z_][a-z0-9_]*" %))
                                    type-args)))
          {:type :error :from pos :to final-end
           :text (subs s pos final-end)
           :message "restriction set must be comma-separated snake_case identifiers"}

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
              optional?    (assoc :optional? true)
              type-args    (assoc :param-type-args type-args)
              has-default? (assoc :param-default (subs s def-start def-end)))))))))

(defn lex-content-token
  "Lex one token of mid-line content starting at `pos` (non-nil,
   non-newline, non-whitespace)."
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

      ;; Dash: digit after → negative number; word-char tight on both
      ;; sides (`a-b`) → hard error (snake_case only, never silently
      ;; split as subtraction); otherwise the join marker.
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
           depth           0              ; open-paren depth — while > 0,
                                          ; newlines/indentation are plain
                                          ; whitespace (implicit line
                                          ; joining, so arg-lists can span
                                          ; lines)
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
              (recur end stack true depth
                     (conj out {:type :blank-line
                                :from pos :to end
                                :text (subs src pos end)})))

            ;; Deeper indent: emit :indent, push, leave line-start state.
            (> width (peek stack))
            (recur ind-end (conj stack width) false depth
                   (conj out {:type :indent
                              :from pos :to ind-end
                              :text (subs src pos ind-end)}))

            ;; Shallower: emit one zero-length :dedent, pop, stay at
            ;; line-start so the next iteration re-checks (multi-pop).
            (< width (peek stack))
            (recur pos (pop stack) true depth
                   (conj out {:type :dedent :from pos :to pos :text ""}))

            ;; Same indent — skip leading ws and proceed mid-line.
            :else
            (recur ind-end stack false depth out)))

        ;; ── Mid-line ───────────────────────────────────────────────
        :else
        (let [c (char-at src pos)]
          (cond
            (= "\n" c)
            (if (pos? depth)
              ;; Inside parens: the newline is just whitespace. Stay
              ;; mid-line so the continuation line's indentation never
              ;; reaches the indent stack.
              (recur (inc pos) stack false depth out)
              (recur (inc pos) stack true depth
                     (conj out {:type :newline :from pos :to (inc pos) :text "\n"})))

            (ws? c)
            ;; skip inline whitespace
            (recur (inc pos) stack false depth out)

            :else
            (let [tok (lex-content-token src pos)
                  depth (case (:type tok)
                          :lparen (inc depth)
                          :rparen (max 0 (dec depth))
                          depth)]
              (recur (:to tok) stack false depth (conj out tok)))))))))
