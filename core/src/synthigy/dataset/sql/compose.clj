(ns synthigy.dataset.sql.compose
  "Database-agnostic SQL query composition utilities.

  Provides:
  - Table name resolution (table)
  - Multiline query builders (ml, mlf)
  - Multimethods for prepare/execute (dispatches on DB type)"
  (:require
   [clojure.string :as str]
   [synthigy.dataset.core :refer [*return-type*]]
   [synthigy.dataset.sql.naming :as n]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]))

(defn table [x] (n/table *db* x))

(defn ml
  "Multiline... Joins lines with newline. Removes empty lines"
  [& lines]
  (str/join "\n" (remove empty? lines)))

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
