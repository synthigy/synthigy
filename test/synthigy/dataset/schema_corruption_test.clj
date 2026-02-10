(ns synthigy.dataset.schema-corruption-test
  "Minimal test exposing schema corruption bug.

  BUG: After deploying a user dataset, the deployed schema loses
  the :versions relation on the Dataset entity. This breaks queries
  that use :versions relation (like last-deployed-version-per-dataset).

  To run: (clojure.test/run-tests 'synthigy.dataset.schema-corruption-test)"
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [environ.core :refer [env]]
    [next.jdbc]
    [patcho.lifecycle]
    [synthigy.dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.query :as query]
    [synthigy.dataset.test-helpers :refer [make-entity add-test-attribute]]
    [synthigy.db :refer [*db*]]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]))

;; Dynamic backend loading - same pattern as synthigy.core
(def ^:private backend-ns
  (let [db-type (or (env :synthigy-database-type)
                    (env :db-type)
                    "sqlite")]
    (symbol (str "synthigy.dataset." db-type))))

(require backend-ns)

(defn- deployed-versions []
  ((ns-resolve backend-ns 'deployed-versions)))

(defn- deployed-version-per-dataset-euuids []
  ;; Function name varies by backend
  (if-let [f (ns-resolve backend-ns 'deployed-version-per-dataset-euuids)]
    (f)
    ((ns-resolve backend-ns 'deployed-version-per-dataset-ids))))

(use-fixtures :once test-helper/system-fixture)

;;; ============================================================================
;;; Helpers
;;; ============================================================================

(defn dataset-schema-relations
  "Returns the relation keys for the Dataset entity in deployed schema."
  []
  (let [schema (query/deployed-schema)
        ds-entity (->> schema vals (filter #(= "Dataset" (:name %))) first)]
    (set (keys (:relations ds-entity)))))

(defn fix-schema!
  "Regenerate and deploy correct schema from model.

  NOTE: Uses deployed-model from DB (which is correct) rather than
  rebuild-global-model (which has the bug we're testing)."
  []
  (let [model (synthigy.dataset/deployed-model)
        ;; Force all relations to active (workaround for the bug)
        fixed-model (update model :relations
                            (fn [rels]
                              (reduce-kv
                                (fn [m k v] (assoc m k (assoc v :active true)))
                                {} rels)))
        schema (query/model->schema fixed-model)]
    (query/deploy-schema schema)))

(defn make-simple-model
  "Create a minimal ERD model with one entity.
  Includes at least one scalar attribute to satisfy Lacinia schema generation
  (enums with empty values fail spec validation).

  attr-id must be stable across test runs to avoid 'duplicate column' errors
  when re-deploying the same entity."
  [entity-name entity-id attr-id]
  (let [entity (-> (make-entity {(id/key) entity-id
                                 :name entity-name
                                 :type "strong"})
                   (add-test-attribute {(id/key) attr-id
                                        :name "name"
                                        :type "string"}))]
    (core/add-entity (core/map->ERDModel {}) entity)))


;;; ============================================================================
;;; Test Cases
;;; ============================================================================

(deftest test-schema-retains-versions-after-deploy
  (testing "Schema should retain :versions relation after deploying user datasets"

    ;; SETUP: Ensure schema is in known good state
    (fix-schema!)

    ;; PRECONDITION: Schema has :versions relation
    (is (contains? (dataset-schema-relations) :versions)
        "PRECONDITION FAILED: Schema should have :versions relation before test")

    ;; ACTION: Deploy a simple test dataset
    (let [test-version {(id/key) (id/data :test/schema-corruption-v1)
                        :name "SchemaCorruptionTest-v1"
                        :model (make-simple-model
                                 "SchemaCorruptionTestEntity"
                                 (id/data :test/schema-corruption-entity-1)
                                 (id/data :test/schema-corruption-attr-1))
                        :dataset {(id/key) (id/data :test/schema-corruption-dataset)
                                  :name "SchemaCorruptionTestDataset"}}]
      (core/deploy! *db* test-version))

    ;; ASSERTION: Schema should STILL have :versions relation
    (is (contains? (dataset-schema-relations) :versions)
        "BUG: Schema lost :versions relation after deploy!")))

(deftest test-schema-retains-versions-after-recall
  (testing "Schema should retain :versions relation after recalling a version"

    ;; SETUP: Ensure schema is in known good state and deploy test version
    (fix-schema!)
    (let [test-version {(id/key) (id/data :test/schema-corruption-v2)
                        :name "SchemaCorruptionTest-v2"
                        :model (make-simple-model
                                 "SchemaCorruptionTestEntity2"
                                 (id/data :test/schema-corruption-entity-2)
                                 (id/data :test/schema-corruption-attr-2))
                        :dataset {(id/key) (id/data :test/schema-corruption-dataset)
                                  :name "SchemaCorruptionTestDataset"}}]
      (core/deploy! *db* test-version)

      ;; Fix schema if broken by deploy
      (fix-schema!)

      ;; PRECONDITION: Schema has :versions relation
      (is (contains? (dataset-schema-relations) :versions)
          "PRECONDITION FAILED: Schema should have :versions relation before recall")

      ;; ACTION: Recall the version
      (core/recall! *db* {(id/key) (id/data :test/schema-corruption-v2)})

      ;; ASSERTION: Schema should STILL have :versions relation
      (is (contains? (dataset-schema-relations) :versions)
          "BUG: Schema lost :versions relation after recall!"))))

(deftest test-deployed-version-per-dataset-ids-not-empty
  (testing "deployed-version-per-dataset-ids should return non-empty set when versions exist"

    ;; SETUP: Ensure schema is correct
    (fix-schema!)

    ;; PRECONDITION: There are deployed versions in database
    (let [deployed-count (count (deployed-versions))]
      (is (pos? deployed-count)
          "PRECONDITION FAILED: Should have deployed versions in database"))

    ;; ASSERTION: deployed-version-per-dataset-ids should return them
    (let [ids (deployed-version-per-dataset-euuids)]
      (is (not-empty ids)
          "BUG: deployed-version-per-dataset-ids returned empty set despite deployed versions existing"))))

(deftest test-zzz-cleanup
  (testing "Cleanup test data after all tests"
    ;; Destroy test dataset (removes all versions)
    (try
      (core/destroy! *db* {(id/key) (id/data :test/schema-corruption-dataset)
                           :name "SchemaCorruptionTestDataset"})
      (catch Exception _))

    ;; Drop test tables
    (try
      (with-open [con (next.jdbc/get-connection (:datasource *db*))]
        (next.jdbc/execute! con ["DROP TABLE IF EXISTS schemacorruptiontestentity CASCADE"])
        (next.jdbc/execute! con ["DROP TABLE IF EXISTS schemacorruptiontestentity2 CASCADE"]))
      (catch Exception _))

    ;; Fix schema
    (fix-schema!)

    ;; Verify cleanup
    (is (contains? (dataset-schema-relations) :versions)
        "Schema should have :versions after cleanup")))
