(ns synthigy.dataset.euuid-mapping-unit-test
  "Unit tests for EUUID mapping logic in store-entity-records.

  These tests focus on the mapping reconstruction logic without requiring
  a full database setup. They test the core algorithm directly."
  (:require
   [clojure.test :refer [deftest is testing]]
   [synthigy.dataset.id :as id]
   [synthigy.test-data]))  ; Load test data registrations

;;; ============================================================================
;;; Helper Functions to Test Mapping Logic
;;; ============================================================================

;; Counter for cycling through test data IDs
(def ^:private test-id-counter (atom 0))

;; Available test data keys for unit tests
(def ^:private test-id-keys
  [:test/mapping-id-1 :test/mapping-id-2 :test/mapping-id-3 :test/mapping-id-4
   :test/mapping-id-5 :test/mapping-id-6 :test/mapping-id-7 :test/mapping-id-8])

(defn generate-id
  "Get next ID from test data (mimics the refactored code)"
  []
  (let [idx (swap! test-id-counter inc)
        data-key (nth test-id-keys (mod idx (count test-id-keys)))]
    (id/data data-key)))

(defn add-id-to-rows
  "Add ID to rows that don't have one (mimics Step 1 of refactoring)"
  [rows]
  (map (fn [[row-data tmp-id]]
         (let [entity-id (or (id/extract row-data) (generate-id))]
           [(assoc row-data (id/key) entity-id) tmp-id entity-id]))
       rows))

(defn build-id-mapping
  "Build id->tmpid mapping (mimics Step 2 of refactoring)"
  [rows-with-id]
  (into {} (map (fn [[_ tmpid entity-id]] [entity-id tmpid])
                rows-with-id)))

(defn build-constraint-mapping
  "Build constraint->tmpid mapping (mimics Step 2 of refactoring)"
  [rows-with-id constraint-keys]
  (reduce
   (fn [m [row tmpid _]]
     (let [cvals (select-keys row constraint-keys)]
        ;; Only add if all constraint values are non-NULL
        ;; Note: (vals {}) returns (), and (every? some? ()) is true (vacuous)
        ;; So we also check (not-empty cvals)
       (if (and (not-empty constraint-keys)
                (not-empty cvals)
                (every? some? (vals cvals)))
         (assoc m cvals tmpid)
         m)))
   {}
   rows-with-id))

(defn reconstruct-mapping
  "Reconstruct tmpid mapping from database results (mimics Step 7)"
  [id->tmpid constraint->tmpid constraint-keys result]
  (reduce
   (fn [m result-row]
     (let [entity-id (id/extract result-row)
           cvals (when (not-empty constraint-keys)
                   (select-keys result-row constraint-keys))
            ;; Try ID first, fallback to constraint
           tmp-id (or (get id->tmpid entity-id)
                      (get constraint->tmpid cvals))]
       (if tmp-id
         (assoc m tmp-id result-row)
         m)))
   {}
   result))

;;; ============================================================================
;;; Test Case 1: User Provides ID
;;; ============================================================================

(deftest test-mapping-with-provided-id
  (testing "Mapping works when user provides ID"
    (let [my-id (generate-id)
          rows [[(hash-map :name "Alice" (id/key) my-id) :tmp1]]
          constraint-keys [(id/key)]

          ;; Simulate refactoring steps
          rows-with-id (add-id-to-rows rows)
          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate database response (same ID returned)
          db-result [{:_eid 42, (id/key) my-id, :name "Alice"}]

          ;; Reconstruct mapping
          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result)]

      (is (= my-id (id/extract (first (first rows-with-id))))
          "ID should be preserved in processing")
      (is (= :tmp1 (get id->tmpid my-id))
          "ID->tmpid mapping should work")
      (is (= {:_eid 42, (id/key) my-id, :name "Alice"} (get final-mapping :tmp1))
          "Final mapping should be correct"))))

;;; ============================================================================
;;; Test Case 2: New Record Without ID
;;; ============================================================================

(deftest test-mapping-generates-id
  (testing "Mapping works when ID is generated"
    (let [rows [[(hash-map :name "Bob") :tmp1]]
          constraint-keys [(id/key)]

          ;; Simulate refactoring steps
          rows-with-id (add-id-to-rows rows)
          generated-id (id/extract (first (first rows-with-id)))
          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate database response (generated ID returned)
          db-result [{:_eid 43, (id/key) generated-id, :name "Bob"}]

          ;; Reconstruct mapping
          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result)]

      (is (some? generated-id)
          "ID should be generated")
      (is (= :tmp1 (get id->tmpid generated-id))
          "ID->tmpid mapping should work")
      (is (= {:_eid 43, (id/key) generated-id, :name "Bob"} (get final-mapping :tmp1))
          "Final mapping should be correct"))))

;;; ============================================================================
;;; Test Case 3: Update via Constraint (THE CRITICAL TEST)
;;; ============================================================================

(deftest test-mapping-with-constraint-fallback
  (testing "Mapping uses constraint fallback when ID doesn't match"
    (let [;; User doesn't provide ID, has email constraint
          rows [[(hash-map :name "Alice" :email "alice@test.com") :tmp1]]
          constraint-keys [:email]

          ;; Simulate refactoring steps
          rows-with-id (add-id-to-rows rows)
          generated-id (id/extract (first (first rows-with-id)))
          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate database response:
          ;; DB already had a record with this email, returns OLD ID (not generated one!)
          old-id (generate-id)
          db-result [{:_eid 44, (id/key) old-id, :email "alice@test.com", :name "Alice"}]

          ;; Reconstruct mapping
          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result)]

      (is (not= old-id generated-id)
          "Returned ID should differ from generated one (simulates existing record)")
      (is (nil? (get id->tmpid old-id))
          "ID mapping should NOT find the old ID")
      (is (= :tmp1 (get constraint->tmpid {:email "alice@test.com"}))
          "Constraint mapping SHOULD find the tmpid")
      (is (= {:_eid 44, (id/key) old-id, :email "alice@test.com", :name "Alice"}
             (get final-mapping :tmp1))
          "Final mapping should work via constraint fallback"))))

;;; ============================================================================
;;; Edge Case: Multiple Rows with NULL Constraints
;;; ============================================================================

(deftest test-mapping-with-null-constraints
  (testing "Rows with NULL constraints should map via ID only"
    (let [rows [[(hash-map :name "User 1") :tmp1]
                [(hash-map :name "User 2") :tmp2]]
          constraint-keys [:email] ;; email is constraint but both rows have nil

          ;; Simulate refactoring steps
          rows-with-id (add-id-to-rows rows)
          id-1 (id/extract (first (nth rows-with-id 0)))
          id-2 (id/extract (first (nth rows-with-id 1)))
          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate database response
          db-result [{:_eid 45, (id/key) id-1, :name "User 1"}
                     {:_eid 46, (id/key) id-2, :name "User 2"}]

          ;; Reconstruct mapping
          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result)]

      (is (empty? constraint->tmpid)
          "Constraint mapping should be empty (NULL values skipped)")
      (is (= :tmp1 (get id->tmpid id-1))
          "ID mapping should work for first row")
      (is (= :tmp2 (get id->tmpid id-2))
          "ID mapping should work for second row")
      (is (= 2 (count final-mapping))
          "Both rows should be mapped")
      (is (= {:_eid 45, (id/key) id-1, :name "User 1"} (get final-mapping :tmp1)))
      (is (= {:_eid 46, (id/key) id-2, :name "User 2"} (get final-mapping :tmp2))))))

;;; ============================================================================
;;; Edge Case: Order Independence
;;; ============================================================================

(deftest test-mapping-order-independence
  (testing "Mapping should work regardless of database return order"
    (let [id-1 (generate-id)
          id-2 (generate-id)
          id-3 (generate-id)
          rows [[(hash-map :name "First" (id/key) id-1) :tmp1]
                [(hash-map :name "Second" (id/key) id-2) :tmp2]
                [(hash-map :name "Third" (id/key) id-3) :tmp3]]
          constraint-keys [(id/key)]

          ;; Simulate refactoring steps
          rows-with-id (add-id-to-rows rows)
          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate database response IN REVERSE ORDER (simulates Cockroach/SQLite)
          db-result-reversed [{:_eid 103, (id/key) id-3, :name "Third"}
                              {:_eid 102, (id/key) id-2, :name "Second"}
                              {:_eid 101, (id/key) id-1, :name "First"}]

          ;; Reconstruct mapping
          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result-reversed)]

      (is (= 3 (count final-mapping))
          "All rows should be mapped")
      (is (= {:_eid 101, (id/key) id-1, :name "First"} (get final-mapping :tmp1))
          "First record should map correctly despite order")
      (is (= {:_eid 102, (id/key) id-2, :name "Second"} (get final-mapping :tmp2))
          "Second record should map correctly despite order")
      (is (= {:_eid 103, (id/key) id-3, :name "Third"} (get final-mapping :tmp3))
          "Third record should map correctly despite order"))))

;;; ============================================================================
;;; Edge Case: Mixed Batch (Some with ID, Some Without)
;;; ============================================================================

(deftest test-mapping-mixed-batch
  (testing "Batch with mixed ID provision should work"
    (let [provided-id (generate-id)
          rows [[(hash-map :name "Provided" (id/key) provided-id) :tmp1]
                [(hash-map :name "Generated") :tmp2]]
          constraint-keys [(id/key)]

          ;; Simulate refactoring steps
          rows-with-id (add-id-to-rows rows)
          generated-id (id/extract (first (nth rows-with-id 1)))
          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate database response
          db-result [{:_eid 201, (id/key) provided-id, :name "Provided"}
                     {:_eid 202, (id/key) generated-id, :name "Generated"}]

          ;; Reconstruct mapping
          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result)]

      (is (= provided-id (id/extract (first (nth rows-with-id 0))))
          "Provided ID should be preserved")
      (is (some? generated-id)
          "Second row should get generated ID")
      (is (not= provided-id generated-id)
          "IDs should be different")
      (is (= 2 (count final-mapping))
          "Both rows should be mapped")
      (is (= {:_eid 201, (id/key) provided-id, :name "Provided"} (get final-mapping :tmp1)))
      (is (= {:_eid 202, (id/key) generated-id, :name "Generated"} (get final-mapping :tmp2))))))

;;; ============================================================================
;;; Summary Test: All Logic Works Together
;;; ============================================================================

(deftest test-complete-mapping-pipeline
  (testing "Complete mapping pipeline works end-to-end"
    (let [;; Complex scenario: mix of provided IDs, generated IDs, and constraints
          provided-id (generate-id)
          rows [[(hash-map :name "User 1" (id/key) provided-id) :tmp1]
                [(hash-map :name "User 2" :email "user2@test.com") :tmp2]
                [(hash-map :name "User 3") :tmp3]]
          constraint-keys [:email]

          rows-with-id (add-id-to-rows rows)
          id-1 (id/extract (first (nth rows-with-id 0)))
          id-2-generated (id/extract (first (nth rows-with-id 1)))
          id-3 (id/extract (first (nth rows-with-id 2)))

          id->tmpid (build-id-mapping rows-with-id)
          constraint->tmpid (build-constraint-mapping rows-with-id constraint-keys)

          ;; Simulate complex DB response:
          ;; - Row 1: Returns provided ID (maps via ID)
          ;; - Row 2: Returns old ID (maps via constraint fallback)
          ;; - Row 3: Returns generated ID (maps via ID)
          old-id-2 (generate-id)
          db-result [{:_eid 301, (id/key) id-1, :name "User 1"}
                     {:_eid 302, (id/key) old-id-2, :email "user2@test.com", :name "User 2"}
                     {:_eid 303, (id/key) id-3, :name "User 3"}]

          final-mapping (reconstruct-mapping id->tmpid constraint->tmpid constraint-keys db-result)]

      (is (= 3 (count final-mapping)) "All rows should be mapped")
      (is (= {:_eid 301, (id/key) id-1, :name "User 1"} (get final-mapping :tmp1))
          "Row 1 maps via ID")
      (is (= {:_eid 302, (id/key) old-id-2, :email "user2@test.com", :name "User 2"}
             (get final-mapping :tmp2))
          "Row 2 maps via constraint fallback")
      (is (= {:_eid 303, (id/key) id-3, :name "User 3"} (get final-mapping :tmp3))
          "Row 3 maps via ID"))))
