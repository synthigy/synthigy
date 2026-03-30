(ns synthigy.dataset.sql.template-test
  "Tests for SQL template parser, resolver, and executor.

  Tests are organized by:
  - Parser: placeholder syntax parsing
  - Resolver: entity/relation name → physical SQL resolution
  - Validation: SELECT-only enforcement, unknown entities
  - Execution: end-to-end template queries with parameters
  - Edge cases: empty results, multiple params, special characters"
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.string :as str]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.template :as tmpl]
    [synthigy.db :refer [*db*]]))

;;; ============================================================================
;;; Parser Tests (unit — no DB needed)
;;; ============================================================================

(deftest parse-entity-placeholder
  (is (= {:type :entity :entity "User"}
         (#'tmpl/parse-placeholder "User")))
  (is (= {:type :entity :entity "User Role"}
         (#'tmpl/parse-placeholder "User Role")))
  (is (= {:type :entity :entity "Dataset Version"}
         (#'tmpl/parse-placeholder "Dataset Version"))))

(deftest parse-field-placeholder
  (is (= {:type :field :entity "User" :field "name"}
         (#'tmpl/parse-placeholder "User.name")))
  (is (= {:type :field :entity "User" :field "active"}
         (#'tmpl/parse-placeholder "User.active")))
  (is (= {:type :field :entity "User" :field "_eid"}
         (#'tmpl/parse-placeholder "User._eid"))))

(deftest parse-relation-placeholder
  (testing "Inner join with dash"
    (is (= {:type :relation :entity "User" :path ["roles"]
            :joins [{:label "roles" :join "INNER"}]}
           (#'tmpl/parse-placeholder "User - roles"))))
  (testing "Left join with arrow"
    (is (= {:type :relation :entity "User" :path ["groups"]
            :joins [{:label "groups" :join "LEFT"}]}
           (#'tmpl/parse-placeholder "User -> groups"))))
  (testing "Right join"
    (is (= {:type :relation :entity "User" :path ["roles"]
            :joins [{:label "roles" :join "RIGHT"}]}
           (#'tmpl/parse-placeholder "User <- roles")))))

(deftest parse-relation-field-placeholder
  (testing "Inner join field"
    (is (= {:type :relation-field :entity "User" :path ["roles"] :field "name"
            :joins [{:label "roles" :join "INNER"}]}
           (#'tmpl/parse-placeholder "User - roles.name"))))
  (testing "Left join field"
    (is (= {:type :relation-field :entity "User" :path ["roles"] :field "name"
            :joins [{:label "roles" :join "LEFT"}]}
           (#'tmpl/parse-placeholder "User -> roles.name")))))

(deftest parse-multi-hop-relation
  (testing "Chained inner joins"
    (is (= {:type :relation :entity "User" :path ["groups" "projects"]
            :joins [{:label "groups" :join "INNER"} {:label "projects" :join "INNER"}]}
           (#'tmpl/parse-placeholder "User - groups - projects"))))
  (testing "Mixed join types"
    (is (= {:type :relation-field :entity "User" :path ["groups" "projects"] :field "name"
            :joins [{:label "groups" :join "LEFT"} {:label "projects" :join "INNER"}]}
           (#'tmpl/parse-placeholder "User -> groups - projects.name")))))

(deftest extract-multiple-placeholders
  (let [template "SELECT {User.name}, {User.active} FROM {User}"
        placeholders (#'tmpl/extract-placeholders template)]
    (is (= 3 (count placeholders)))
    (is (= :field (get-in placeholders [0 :parsed :type])))
    (is (= :field (get-in placeholders [1 :parsed :type])))
    (is (= :entity (get-in placeholders [2 :parsed :type])))))

(deftest extract-mixed-placeholders
  (let [template "SELECT {User.name}, COUNT({User -> roles}._eid) FROM {User}"
        placeholders (#'tmpl/extract-placeholders template)]
    (is (= 3 (count placeholders)))
    (is (= :field (get-in placeholders [0 :parsed :type])))
    (is (= :relation (get-in placeholders [1 :parsed :type])))
    (is (= :entity (get-in placeholders [2 :parsed :type])))))

;;; ============================================================================
;;; Resolver Tests (needs deployed schema)
;;; ============================================================================

(deftest resolve-simple-entity
  (testing "Field placeholder resolves and auto-generates FROM"
    (let [{:keys [sql entities]} (tmpl/resolve-template
                                    "SELECT {User.name}")]
      (is (str/includes? sql "\"user\""))
      (is (re-find #"e\d+\.name" sql))
      (is (str/includes? sql "FROM"))
      (is (= 1 (count entities))))))

(deftest resolve-field-reference
  (testing "Field placeholder resolves to aliased column"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User.name}")]
      (is (re-find #"e\d+\.name" sql))
      (is (str/includes? sql "\"user\"")))))

(deftest resolve-auto-from-placement
  (testing "FROM is injected before WHERE"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User.name} WHERE {User.active} = ?")]
      (is (str/includes? sql "FROM"))
      (let [from-idx (str/index-of sql "FROM")
            where-idx (str/index-of sql "WHERE")]
        (is (< from-idx where-idx)))))

  (testing "FROM is injected before GROUP BY"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User.name} GROUP BY {User.name}")]
      (let [from-idx (str/index-of sql "FROM")
            group-idx (str/index-of sql "GROUP")]
        (is (< from-idx group-idx)))))

  (testing "FROM is appended when no keywords follow SELECT"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User.name}, {User.active}")]
      (is (str/includes? sql "FROM")))))

(deftest resolve-relation-generates-join
  (testing "Inner join (dash) generates INNER JOIN"
    (let [{:keys [sql entities]} (tmpl/resolve-template
                                    "SELECT {User.name}, {User - roles.name}")]
      (is (str/includes? sql "INNER JOIN"))
      (is (str/includes? sql "\"user\""))
      (is (str/includes? sql "\"user_role\""))
      (is (= 2 (count entities)))))
  (testing "Left join (arrow) generates LEFT JOIN"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User.name}, {User -> roles.name}")]
      (is (str/includes? sql "LEFT JOIN")))))

(deftest resolve-relation-only-reference
  (testing "Relation without .field resolves to target alias"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT COUNT({User - roles}._eid)")]
      (is (str/includes? sql "INNER JOIN"))
      (is (re-find #"e\d+\._eid" sql)))))

(deftest resolve-duplicate-relation-single-join
  (testing "Same relation referenced multiple times generates only one JOIN"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User - roles.name}, {User - roles._eid}")]
      (is (= 2 (count (re-seq #"INNER JOIN" sql)))))))

(deftest resolve-multiple-relations
  (testing "Different relations generate separate JOINs"
    (let [{:keys [sql]} (tmpl/resolve-template
                          "SELECT {User - roles.name}, {User - groups.name}")]
      (is (= 4 (count (re-seq #"INNER JOIN" sql)))))))

(deftest resolve-unknown-entity-error
  (testing "Unknown entity name produces error with position"
    (let [{:keys [errors]} (tmpl/resolve-template
                             "SELECT {NonExistent.name}")]
      (is (some? errors))
      (is (str/includes? (:error (first errors)) "Unknown entity"))
      (is (number? (:position (first errors)))))))

(deftest resolve-unknown-relation-error
  (testing "Unknown relation label produces error"
    (let [{:keys [errors]} (tmpl/resolve-template
                             "SELECT {User - nonexistent.name}")]
      (is (some? errors))
      (is (str/includes? (:error (first errors)) "Unknown relation")))))

;;; ============================================================================
;;; Validation Tests
;;; ============================================================================

(deftest validate-select-only
  (testing "SELECT is allowed"
    (is (nil? (tmpl/validate-select-only! "SELECT {User.name} FROM {User}")))
    (is (nil? (tmpl/validate-select-only! "  select {User.name} FROM {User}")))
    (is (nil? (tmpl/validate-select-only! "\n  SELECT * FROM {User}"))))

  (testing "Non-SELECT is rejected"
    (is (thrown-with-msg? Exception #"Only SELECT"
          (tmpl/validate-select-only! "DELETE FROM {User}")))
    (is (thrown-with-msg? Exception #"Only SELECT"
          (tmpl/validate-select-only! "UPDATE {User} SET name = ?")))
    (is (thrown-with-msg? Exception #"Only SELECT"
          (tmpl/validate-select-only! "INSERT INTO {User} VALUES (?)")))
    (is (thrown-with-msg? Exception #"Only SELECT"
          (tmpl/validate-select-only! "DROP TABLE {User}")))))

;;; ============================================================================
;;; Execution Tests (needs running DB with test data)
;;; ============================================================================

(deftest execute-simple-query
  (testing "Simple SELECT with parameter — no FROM needed"
    (let [result (tmpl/execute-template
                   "SELECT {User.name} WHERE {User.active} = ?"
                   [true])]
      (is (vector? result))
      (is (every? :name result))
      (is (pos? (count result))))))

(deftest execute-query-without-params
  (testing "SELECT without parameters"
    (let [result (tmpl/execute-template
                   "SELECT {User.name}"
                   nil)]
      (is (vector? result))
      (is (pos? (count result))))))

(deftest execute-query-with-multiple-params
  (testing "Multiple ? parameters bind correctly"
    (let [result (tmpl/execute-template
                   "SELECT {User.name} WHERE {User.active} = ? AND {User.type} = ?"
                   [true "PERSON"])]
      (is (vector? result))
      (is (every? :name result)))))

(deftest execute-group-by-with-count
  (testing "GROUP BY with COUNT on relation (inner join)"
    (let [result (tmpl/execute-template
                   "SELECT {User.name}, COUNT({User - roles._eid}) as role_count WHERE {User.active} = ? GROUP BY {User.name}"
                   [true])]
      (is (vector? result))
      (is (every? #(contains? % :role-count) result))
      ;; Inner join — only users WITH roles
      (is (every? #(pos? (:role-count %)) result))))

  (testing "GROUP BY with COUNT on relation (left join includes zero-role users)"
    (let [result (tmpl/execute-template
                   "SELECT {User.name}, COUNT({User -> roles._eid}) as role_count WHERE {User.active} = ? GROUP BY {User.name}"
                   [true])]
      (is (vector? result))
      ;; Left join — includes users with 0 roles
      (is (some #(zero? (:role-count %)) result)))))

(deftest execute-query-no-results
  (testing "Query that matches nothing returns empty vector"
    (let [result (tmpl/execute-template
                   "SELECT {User.name} WHERE {User.name} = ?"
                   ["NonExistentUserXYZ"])]
      (is (vector? result))
      (is (zero? (count result))))))

(deftest execute-with-limit
  (testing "LIMIT parameter works"
    (let [result (tmpl/execute-template
                   "SELECT {User.name} LIMIT ?"
                   [1])]
      (is (= 1 (count result))))))

(deftest execute-with-order-by
  (testing "ORDER BY works"
    (let [result (tmpl/execute-template
                   "SELECT {User.name} WHERE {User.active} = ? ORDER BY {User.name} ASC"
                   [true])]
      (is (= (sort (map :name result))
             (map :name result))))))

(deftest execute-multi-relation-join
  (testing "Query joining multiple relations"
    (let [result (tmpl/execute-template
                   "SELECT DISTINCT {User.name}, {User - roles.name} as role_name WHERE {User.name} = ?"
                   ["Alice"])]
      (is (pos? (count result)))
      (is (every? #(= "Alice" (:name %)) result))
      (is (every? :role-name result)))))

(deftest execute-backward-compat-with-from
  (testing "Explicit FROM still works (backward compat)"
    (let [result (tmpl/execute-template
                   "SELECT {User.name} FROM {User} WHERE {User.active} = ?"
                   [true])]
      (is (vector? result))
      (is (pos? (count result))))))

(deftest execute-reject-delete
  (testing "DELETE template throws"
    (is (thrown-with-msg? Exception #"Only SELECT"
          (tmpl/execute-template
            "DELETE FROM {User} WHERE {User.active} = ?"
            [false])))))

(deftest execute-reject-update
  (testing "UPDATE template throws"
    (is (thrown-with-msg? Exception #"Only SELECT"
          (tmpl/execute-template
            "UPDATE {User} SET {User.name} = ? WHERE {User._eid} = ?"
            ["hacked" 1])))))

(deftest execute-unknown-entity-throws
  (testing "Template with unknown entity includes position in error"
    (is (thrown-with-msg? Exception #"position"
          (tmpl/execute-template
            "SELECT {Bogus.name}"
            nil)))))
