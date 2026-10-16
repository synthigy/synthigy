;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.xsql.tokens
  "Tokenizer for the XSQL DSL: a flat token vector with virtual structural tokens encoding block structure."
  (:require [clojure.string :as str]))

;; ── Helpers ────────────────────────────────────────────────────────────────

(def ^:private re-ident-start    #"[a-zA-Z_]")   ; allow uppercase to scan, then reject
(def ^:private re-ident-continue #"[a-zA-Z0-9_]")
(def ^:private re-digit          #"[0-9]")

(defn char-at
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
  [s pos]
  (scan-while s pos ws?))

(defn find-eol
  [s pos]
  (scan-while s pos #(and % (not= "\n" %))))

;; ── Content lexers (mid-line) ──────────────────────────────────────────────

(defn lex-string
  "Quoted string literal at `pos`; :error if EOF/newline comes before the closing quote."
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
  "Lex a named param-ref at `pos`; a bare `?`/`?N` becomes an :error token."
  [s pos]
  (let [n     (count s)
        after (inc pos)
        nch   (char-at s after)]
    (cond
      (or (nil? nch) (not (ident-start? nch)))
      {:type :error :from pos :to after :text "?"
       :message "XSQL parameters must be named (?name[:type][])"}

      :else
      (let [name-end   (scan-while s after ident-continue?)
            param-name (subs s after name-end)
            optional?  (= "?" (char-at s name-end))
            mark-end   (if optional? (inc name-end) name-end)
            colon?     (and (= ":" (char-at s mark-end))
                            (not= ":" (char-at s (inc mark-end))))
            type-start (when colon? (inc mark-end))
            type-end   (when type-start
                         (scan-while s type-start ident-continue?))
            type-raw   (when type-start
                         (subs s type-start type-end))
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
  "Lex one mid-line content token at `pos` (not newline or whitespace)."
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
  "Tokenize an XSQL source string into a token vector ending in `:eof`."
  [^String src]
  (let [n (count src)]
    (loop [pos             0
           stack           [0]            ; indent depth stack
           at-line-start?  true
           depth           0              ; open-paren depth
           out             []]
      (cond
        ;; ── EOF ─────────────────────────────────────────────────────
        (>= pos n)
        (let [prev-char (when (pos? n) (char-at src (dec n)))
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

            ;; Shallower: zero-length :dedent, pop, stay at line-start (multi-pop).
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
