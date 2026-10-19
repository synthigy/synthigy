;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.postgres.collation
  "The `human` ICU collation on PostgreSQL: creation, column conversion and ICU version upkeep."
  (:require
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.sql.naming :refer [normalize-name entity->table-name]]
   [synthigy.db.sql :as sql]
   [synthigy.log :as log]))

(def human-types #{"string" "enum"})

(def human-ddl "text collate human")

(defn ensure-human-collation!
  "Creates the `human` collation when missing; throws when the server has no ICU."
  []
  (when (zero? (:n (sql/execute-one! ["select count(*) as n from pg_collation where collprovider = 'i'"])))
    (throw (ex-info "PostgreSQL server has no ICU support; Synthigy needs ICU for its human collation"
                    {:type ::icu-missing})))
  (sql/execute-one! ["create collation if not exists human (provider = icu, locale = 'und-u-kn-true')"]))

(defn pending-columns
  "[table column] of every string/enum column in the deployed model not yet collated `human`."
  []
  (let [catalog (into {}
                      (map (juxt (juxt :table_name :column_name) :collation_name))
                      (sql/execute! ["select table_name, column_name, collation_name
                                      from information_schema.columns
                                      where table_schema = current_schema() and data_type = 'text'"]))]
    (vec
     (distinct
      (for [entity (core/get-entities (dataset/deployed-model))
            {:keys [name type]} (:attributes entity)
            :when (human-types type)
            :let [column [(entity->table-name entity) (normalize-name name)]]
            :when (contains? catalog column)
            :when (not= "human" (get catalog column))]
        column)))))

(defn collate-columns!
  "Collates every pending string/enum column `human`; returns the converted [table column] pairs."
  []
  (let [columns (pending-columns)]
    (doseq [[table column] columns]
      (sql/execute-one! [(format "alter table \"%s\" alter column \"%s\" type %s" table column human-ddl)])
      (log/debug {:id ::column-collated :data {:table table :column column}} "Collated column human"))
    columns))

(defn human-version
  "Recorded and live ICU version of the `human` collation, or nil when it is missing."
  []
  (sql/execute-one! ["select collversion as recorded, pg_collation_actual_version(oid) as actual
                      from pg_collation
                      where collname = 'human' and collnamespace = current_schema()::regnamespace"]))

(defn human-indexes
  "Indexes with at least one column collated `human`."
  []
  (mapv :idx (sql/execute! ["select i.indexrelid::regclass::text as idx
                             from pg_index i
                             join pg_collation c on c.oid = any(i.indcollation::oid[])
                             where c.collname = 'human' and c.collnamespace = current_schema()::regnamespace"])))

(defn index-intact?
  [index]
  (try
    (sql/execute-one! ["select count(*) as n from (select bt_index_check(?::regclass)) as checked" index])
    true
    (catch Exception e
      (log/warn {:id ::index-check-failed :data {:index index :error (ex-message e)}}
                "Index failed bt_index_check under the new ICU version")
      false)))

(defn refresh-human-version!
  "Checks every `human` index under the live ICU, rebuilds the ones that fail, then records the version."
  []
  (let [amcheck? (some? (sql/execute-one! ["select 1 as present from pg_extension where extname = 'amcheck'"]))
        indexes (human-indexes)
        rebuilt (filterv #(not (and amcheck? (index-intact? %))) indexes)]
    (doseq [index rebuilt]
      (sql/execute-one! [(format "reindex index concurrently %s" index)]))
    (sql/execute-one! ["alter collation human refresh version"])
    (log/info {:id ::version-refreshed
               :data {:action :refreshing :subject :collation
                      :indexes (count indexes) :rebuilt rebuilt :amcheck amcheck?}}
              "Human collation version refreshed")
    rebuilt))

(defn check-version!
  "Starts a background refresh when the server's ICU no longer matches the version `human` was recorded with."
  []
  (let [{:keys [recorded actual]} (human-version)]
    (when (and recorded actual (not= recorded actual))
      (log/warn {:id ::version-changed
                 :data {:action :verifying :subject :collation :recorded recorded :actual actual}}
                "ICU version of the human collation changed; verifying its indexes in the background")
      (future
        (try
          (refresh-human-version!)
          (catch Throwable e
            (log/error! {:id ::version-refresh-failed
                         :msg "Human collation version refresh failed"
                         :data {:action :refreshing :subject :collation}}
                        e)))))))
