(ns synthigy.dataset.sql.compose
  "Database-agnostic SQL query composition utilities.

  Provides:
  - Multiline query builders (ml, mlwa, mlf, lwa)
  - JOIN generation (relation-join)
  - Conditional joining (and-join, or-join)
  - Multimethods for prepare/execute (dispatches on DB type)"
  (:require
    [clojure.string :as str]
    [synthigy.dataset.core :refer [*return-type*]]
    [synthigy.dataset.sql.naming :as n]
    [synthigy.db :refer [*db*]]
    [synthigy.db.sql :as sql]))

(defn table [x] (n/table *db* x))
(defn relation [t x] (n/relation *db* t x))
(defn related-table [t x] (n/related-table *db* t x))
(defn relation-from-field [t x] (n/relation-from-field *db* t x))
(defn relation-to-field [t x] (n/relation-to-field *db* t x))

(defn ml
  "Multiline... Joins lines with newline. Removes empty lines"
  [& lines]
  (str/join "\n" (remove empty? lines)))

(defn mwa
  "Short for 'Multiline With Arguments'
  Expects bindings of form [query-part arg1 arg2 arg3] and returns
  final vector with first argument as final query in multiline form and
  rest of arguments that were provided in bindings"
  [& bindings]
  (reduce
    (fn [[query :as result] [part & data]]
      (if (empty? part) result
          (as-> result result
            (assoc result 0 (str query (when query \newline) part))
            (if (empty? data) result
                (into result data)))))
    []
    bindings))

(defmacro mlwa
  "Multiline format macro. This macro will take string and
  if it is followed by something other than string will assume
  that taken string is formated line and following are arguments.

  Repeats until next string end of line-or-arg input

  ```clojure
  (let [variable \"391092109\"]
    (mlwa
     \"Hi from macro\"
     \"with formated  ?  \" variable
     \"text on number ?\" 10292))
  ```"
  [& line-or-arg]
  (loop [[current & others] line-or-arg
         lines []
         args []]
    (let [[next] others]
      (cond
        ;; When there is no next and there are no others
        ;; return result
        (and (nil? current) (empty? others))
        `[(clojure.string/join "\n" ~lines) ~@args]
        ;; If current is string and there are no others, than join that line
        (and (string? current) (empty? others))
        `[(clojure.string/join "\n" ~(conj lines current)) ~@args]
        ;; If this is string and string follows than join
        ;; line in result and recur with others
        (and (string? current) (string? next))
        (recur others (conj lines current) args)
        ;; If current is string and next isn't string, than
        ;; this should be formated
        (and (string? current) (not (string? next)))
        (let [_args (take-while #(not (string? %)) others)]
          (recur
            (drop (count _args) others)
            (conj lines current)
            (into args _args)))))))

(defmacro mlf
  "Multiline format macro. This macro will take string and
  if it is followed by something other than string will assume
  that taken string is formated line and following are arguments.

  Repeats until next string end of line-or-arg input

  ```clojure
  (let [variable \"391092109\"]
    (mlf
     \"Hi from macro\"
     \"with formated  %s  \" variable
     \"text on number %d\" 10292))
  ```"
  [& line-or-arg]
  (loop [[current & others] line-or-arg
         result []]
    (let [[next] others]
      (cond
        ;; When there is no next and there are no others
        ;; return result
        (and (nil? current) (empty? others))
        `(clojure.string/join "\n" ~result)
        ;; If current is string and there are no others, than join that line
        (and (string? current) (empty? others))
        `(clojure.string/join "\n" ~(conj result current))
        ;; If this is string and string follows than join
        ;; line in result and recur with others
        (and (string? current) (string? next))
        (recur others (conj result current))
        ;; If current is string and next isn't string, than
        ;; this should be formated
        (and (string? current) (not (string? next)))
        (let [args (take-while #(not (string? %)) others)
              line `(~'format ~current ~@args)]
          (recur
            (drop (count args) others)
            (conj result line)))))))

(defn lwa
  "Short for 'Line with arguments'
  Expects bindings of form [query-part arg1 arg2 arg3] and returns
  final vector with first argument as space separated parts of query-parts
  and rest of arguments that were provided in bindings"
  [& bindings]
  (reduce
    (fn [[query :as result] [part & data]]
      (if (empty? part) result
          (as-> result result
            (assoc result 0 (str query " " part))
            (if (empty? data) result
                (into result data)))))
    []
    bindings))

(defn and-join [condition text]
  (if condition (str "and " text)
      text))

(defn or-join [condition text]
  (if condition (str "or " text)
      text))

(defn relation-join
  "Function will generate SQL for joining two tables based on
  start entity and outgoing label. Optionally you can specify
  join type (LEFT, INNER, RIGHT) as well as from-alias and to-alias.

  IMPORTANT: If you are using aliases outside of this join, then it
  is mandatory do specify them, otherwise SQL query will fail"
  [{:keys [join entity label
           from-alias to-alias]
    :or {join "inner"}}]
  (let [relation-table (relation entity label)
        from-table (table entity)
        link-alias (gensym "link")
        to-table (related-table entity label)]
    (ml
      (format
        "%s join \"%s\" %s on \"%s\"._eid=\"%s\".%s"
        join relation-table link-alias (or from-alias from-table) link-alias
        (relation-from-field entity label))
      (format
        "%s join \"%s\" %s on \"%s\".%s=\"%s\"._eid"
        join to-table (if to-alias to-alias "")
        link-alias
        (relation-to-field entity label)
        (or to-alias to-table)))))

(defmulti prepare
  "Multimethod for preparing queries based on database type"
  (fn dispatch
    ([id] [id (class *db*)])
    ([id _] [id (class *db*)])))

(defmulti execute!
  "Multimethod for executing queries based on database type"
  (fn [_] (class *db*)))

(defmethod execute! synthigy.db.Postgres
  [query-binding]
  (sql/execute! query-binding *return-type*))

(defmethod execute! synthigy.db.SQLite
  [query-binding]
  (sql/execute! query-binding *return-type*))
