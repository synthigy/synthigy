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

(ns synthigy.dataset.rls
  "Row-Level Security: injects WHERE conditions into queries from pre-compiled guard
   configs, filtering rows by relationship to IAM entities. Opt-in per entity via :rls
   in the compiled schema (resolved by model->schema at deploy time) — no global env var."
  (:require
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.dataset.access :as access]))

(defn should-apply-guards?
  "False when no principal is bound, principal is superuser, or (entity-aware arity)
   an RWDOB row-scope bypass grant applies. RLS is independent of RBAC."
  ([]
   (and (some? (access/current-principal))
        (not (access/superuser?))))
  ([entity-id operation]
   (and (should-apply-guards?)
        (not (access/rls-bypass? entity-id operation)))))

(defn get-session-values
  "Vector of _eid values projected from the current principal for the given match
   type (:user/:group/:role). Nil still fail-closes to deny-all downstream."
  [match]
  (case match
    :user  (when-let [uid (access/principal-eid)] [uid])
    :group (when-let [gs (seq (access/group-eids))] (vec gs))
    :role  (when-let [rs (seq (access/role-eids))] (vec rs))
    nil))

(defn build-ref-sql
  "SQL for a :ref condition: direct column match against session values."
  [main-alias column session-values]
  (if (= 1 (count session-values))
    {:sql (format "%s.\"%s\" = ?" main-alias column)
     :params session-values}
    {:sql (format "%s.\"%s\" IN (%s)" main-alias column
                  (str/join "," (repeat (count session-values) "?")))
     :params session-values}))

(defn build-relation-sql
  "SQL for a :relation condition: EXISTS subquery walking the hop chain to
   session values."
  [main-alias hops session-values]
  (let [first-hop (first hops)
        from-clause (format "\"%s\" l0" (:table first-hop))
        join-clauses (map-indexed
                      (fn [idx hop]
                        (let [prev-idx idx
                              curr-idx (inc idx)
                              prev-hop (nth hops idx)]
                          (format "JOIN \"%s\" l%d ON l%d.\"%s\" = l%d.\"%s\""
                                  (:table hop)
                                  curr-idx
                                  prev-idx
                                  (:to-field prev-hop)
                                  curr-idx
                                  (:from-field hop))))
                      (rest hops))

        join-sql (str/join " " join-clauses)
        final-idx (dec (count hops))
        final-hop (last hops)
        where-sql (if (= 1 (count session-values))
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" = ?"
                            (:from-field first-hop) main-alias
                            final-idx (:to-field final-hop))
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" IN (%s)"
                            (:from-field first-hop) main-alias
                            final-idx (:to-field final-hop)
                            (str/join "," (repeat (count session-values) "?"))))]
    {:sql (if (empty? join-clauses)
            (format "EXISTS (SELECT 1 FROM %s WHERE %s)" from-clause where-sql)
            (format "EXISTS (SELECT 1 FROM %s %s WHERE %s)" from-clause join-sql where-sql))
     :params session-values}))

(defn build-hybrid-sql
  "SQL for a :hybrid condition: relation hops then a final column match."
  [main-alias hops final-table final-column session-values]
  (let [first-hop (first hops)
        from-clause (format "\"%s\" l0" (:table first-hop))
        join-clauses (concat
                      (map-indexed
                       (fn [idx hop]
                         (let [prev-idx idx
                               curr-idx (inc idx)
                               prev-hop (nth hops idx)]
                           (format "JOIN \"%s\" l%d ON l%d.\"%s\" = l%d.\"%s\""
                                   (:table hop)
                                   curr-idx
                                   prev-idx
                                   (:to-field prev-hop)
                                   curr-idx
                                   (:from-field hop))))
                       (rest hops))
                      (let [last-idx (dec (count hops))
                            last-hop (last hops)
                            final-join-idx (count hops)]
                        [(format "JOIN \"%s\" l%d ON l%d.\"%s\" = l%d._eid"
                                 final-table
                                 final-join-idx
                                 last-idx
                                 (:to-field last-hop)
                                 final-join-idx)]))

        join-sql (str/join " " join-clauses)
        final-idx (count hops)

        where-sql (if (= 1 (count session-values))
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" = ?"
                            (:from-field first-hop) main-alias
                            final-idx final-column)
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" IN (%s)"
                            (:from-field first-hop) main-alias
                            final-idx final-column
                            (str/join "," (repeat (count session-values) "?"))))]
    {:sql (format "EXISTS (SELECT 1 FROM %s %s WHERE %s)" from-clause join-sql where-sql)
     :params session-values}))

(defn condition-to-sql
  "SQL for a single compiled condition (:ref/:relation/:hybrid); nil if no
   session values."
  [main-alias condition]
  (let [{:keys [type match column hops final-table final-column]} condition
        session-values (get-session-values match)]
    (when (seq session-values)
      (case type
        :ref (build-ref-sql main-alias column session-values)
        :relation (build-relation-sql main-alias hops session-values)
        :hybrid (build-hybrid-sql main-alias hops final-table final-column session-values)
        nil))))

(defn guard-to-sql
  "SQL for a single guard: AND of its conditions, nil if ANY condition has no
   session values — dropping a term would widen the guard, never narrow it."
  [main-alias guard]
  (let [{:keys [conditions]} guard
        condition-sqls (map #(condition-to-sql main-alias %) conditions)]
    (when (and (seq conditions) (every? some? condition-sqls))
      (if (= 1 (count condition-sqls))
        (first condition-sqls)
        {:sql (format "(%s)" (str/join " AND " (map :sql condition-sqls)))
         :params (vec (mapcat :params condition-sqls))}))))

(defn compile-guards-to-sql
  "SQL for all applicable guards on operation: OR of guards."
  [main-alias guards operation]
  (let [applicable-guards (filter #(contains? (:operation %) operation) guards)
        guard-sqls (keep #(guard-to-sql main-alias %) applicable-guards)]
    (when (seq guard-sqls)
      (if (= 1 (count guard-sqls))
        (first guard-sqls)
        {:sql (format "(%s)" (str/join " OR " (map :sql guard-sqls)))
         :params (vec (mapcat :params guard-sqls))}))))

(defn enhance-args
  "Injects compiled RLS conditions into [stack data] for schema's entity +
   operation."
  [schema [stack data] operation]
  (let [{:keys [entity rls]} schema
        as (or (:rls/as schema) (:entity/as schema))
        {:keys [enabled guards]} rls]
    (cond
      (not enabled)
      [stack data]

      (not (should-apply-guards? entity operation))
      [stack data]

      :else
      (let [{:keys [sql params]} (compile-guards-to-sql as guards operation)]
        (if sql
          (do
            (log/info {:id   :synthigy.dataset.rls/guard-active
                       :data {:action    :applied
                              :subject   :request
                              :entity    entity
                              :operation operation}}
                      "RLS guard applied")
            (log/debug {:id ::applying-guards
                        :data {:entity entity :operation operation :sql sql}}
                       "Applying RLS guards")
            [(conj stack [:and [sql]])
             (into (vec data) params)])
          ;; no applicable guards for this operation: fail-closed, deny all
          (do
            (log/info {:id   :synthigy.dataset.rls/guard-active
                       :data {:action    :denied
                              :subject   :request
                              :entity    entity
                              :operation operation}}
                      "RLS denied — no applicable guards")
            (log/warn {:id ::no-applicable-guards
                       :data {:entity entity :operation operation}}
                      "RLS denying access: no applicable guards for operation")
            [(conj stack [:and ["1=0"]])
             data]))))))
