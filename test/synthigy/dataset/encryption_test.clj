(ns synthigy.dataset.encryption-test
  "Tests for dataset encryption functionality.

  Dataset encryption is NOT managed by lifecycle - tests manually start/stop encryption.
  The system fixture ensures database and transit are initialized."
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [synthigy.dataset.encryption :as encryption]
   [synthigy.dataset.shamir :as shamir]
   [synthigy.test-helper :as helper]))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(use-fixtures :once helper/system-fixture)

(defn encryption-fixture
  "Ensure encryption is stopped and DEK table is clean before and after each test."
  [f]
  (try
    ;; Clean state before test
    (encryption/stop)
    ;; Drop and recreate DEK table for clean slate
    (when (encryption/dek-table-exists?)
      (encryption/drop-dek-table))
    (f)
    (finally
      ;; Clean up after test
      (encryption/stop))))

(use-fixtures :each encryption-fixture)

;;; ============================================================================
;;; Basic Encryption Tests
;;; ============================================================================

(deftest test-dek-table-operations
  (testing "DEK table creation and existence check"
    ;; Clean state
    (encryption/drop-dek-table)
    (is (false? (encryption/dek-table-exists?)) "Table should not exist initially")

    ;; Create table
    (encryption/create-dek-table)
    (is (true? (encryption/dek-table-exists?)) "Table should exist after creation")

    ;; Cleanup
    (encryption/drop-dek-table)))

(deftest test-encryption-lifecycle
  (testing "Start and stop encryption with master key"
    (let [master-key (encryption/random-master)]
      ;; Ensure DEK table exists
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      ;; Start encryption
      (encryption/start master-key)
      (is (some? encryption/*master-key*) "Master key should be set")
      (is (some? encryption/*dek*) "DEK should be initialized")
      (is (map? @encryption/deks) "DEKs atom should be initialized")

      ;; Stop encryption
      (encryption/stop)
      (is (nil? @encryption/deks) "DEKs should be cleared after stop"))))

(deftest test-key-generation
  (testing "Generate AES-256 key"
    (let [key (encryption/generate-key 256)]
      (is (instance? javax.crypto.spec.SecretKeySpec key) "Should be a SecretKeySpec")
      (is (= 32 (count (.getEncoded key))) "Should be 32 bytes (256 bits)")
      (is (= "AES" (.getAlgorithm key)) "Should be AES algorithm"))))

(deftest test-gen-key-from-string
  (testing "Generate key from numeric string using gen-key"
    (let [master-string "316714828082109243757432512254285214989459387048765934065582062858114433024"
          key1 (encryption/gen-key master-string)
          key2 (encryption/gen-key master-string)]
      (is (instance? javax.crypto.spec.SecretKeySpec key1))
      (is (= (seq (.getEncoded key1)) (seq (.getEncoded key2)))
          "Same numeric string should generate same key"))))

(deftest test-gen-key-deterministic
  (testing "gen-key should be deterministic for same input"
    (let [master "123456789012345678901234567890"
          key1 (encryption/gen-key master)
          key2 (encryption/gen-key master)
          key3 (encryption/gen-key master)]
      (is (= (seq (.getEncoded key1))
             (seq (.getEncoded key2))
             (seq (.getEncoded key3)))
          "Same input should always generate same key"))))

;;; ============================================================================
;;; DEK Encryption/Decryption Tests
;;; ============================================================================

(deftest test-encrypt-decrypt-dek
  (testing "Encrypt and decrypt DEK with master key"
    (let [master-key (encryption/generate-key 256)
          dek (encryption/generate-key 256)]
      (binding [encryption/*master-key* master-key]
        (let [encrypted (encryption/encrypt-dek dek)
              decrypted (encryption/decrypt-dek encrypted)]
          (is (map? encrypted) "Encrypted should be a map")
          (is (:key encrypted) "Should have encrypted key")
          (is (:iv encrypted) "Should have IV")
          (is (= (seq (.getEncoded dek)) (seq decrypted))
              "Decrypted should match original DEK"))))))

(deftest test-decrypt-dek-wrong-key
  (testing "Decrypting DEK with wrong master key should fail"
    (let [correct-key (encryption/generate-key 256)
          wrong-key (encryption/generate-key 256)
          dek (encryption/generate-key 256)]
      (binding [encryption/*master-key* correct-key]
        (let [encrypted (encryption/encrypt-dek dek)]
          (binding [encryption/*master-key* wrong-key]
            (is (thrown? Exception (encryption/decrypt-dek encrypted))
                "Should throw exception with wrong key")))))))

;;; ============================================================================
;;; Data Encryption/Decryption Tests
;;; ============================================================================

(deftest test-encrypt-decrypt-data
  (testing "Encrypt and decrypt data round-trip"
    (let [master-key (encryption/random-master)
          test-data "Sensitive information that needs encryption"]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [encrypted (encryption/encrypt-data test-data)
            decrypted (encryption/decrypt-data encrypted)]
        (is (map? encrypted) "Encrypted should be a map")
        (is (:data encrypted) "Should have encrypted data field")
        (is (:dek encrypted) "Should have DEK ID field")
        (is (:iv encrypted) "Should have IV field")
        (is (= test-data decrypted) "Decrypted should match original data"))

      (encryption/stop))))

(deftest test-encrypt-without-initialization
  (testing "Encrypting without initialization should throw"
    (encryption/stop)  ; Ensure not initialized
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"not initialized"
         (encryption/encrypt-data "test data"))
        "Should throw when encryption not initialized")))

(deftest test-encrypt-different-data
  (testing "Different data produces different ciphertext"
    (let [master-key (encryption/random-master)]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [data1 "First message"
            data2 "Second message"
            encrypted1 (encryption/encrypt-data data1)
            encrypted2 (encryption/encrypt-data data2)]
        (is (not= (:data encrypted1) (:data encrypted2))
            "Different data should produce different ciphertext"))

      (encryption/stop))))

(deftest test-encrypt-same-data-different-iv
  (testing "Same data encrypted twice produces different ciphertext due to random IV"
    (let [master-key (encryption/random-master)
          data "Same message"]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [encrypted1 (encryption/encrypt-data data)
            encrypted2 (encryption/encrypt-data data)]
        (is (not= (:data encrypted1) (:data encrypted2))
            "Same data should produce different ciphertext due to random IV")
        (is (not= (:iv encrypted1) (:iv encrypted2))
            "IVs should be different"))

      (encryption/stop))))

;;; ============================================================================
;;; DEK Management Tests
;;; ============================================================================

(deftest test-create-and-manage-dek
  (testing "Create and manage DEK in database"
    (let [master-key (encryption/random-master)]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [initial-dek encryption/*dek*
            _ (encryption/create-dek)
            new-dek encryption/*dek*]
        (is (some? initial-dek) "Initial DEK should exist")
        (is (some? new-dek) "New DEK should be created")
        (is (not= initial-dek new-dek) "Should create different DEK"))

      (encryption/stop))))

(deftest test-decrypt-with-old-dek
  (testing "Data encrypted with old DEK should still decrypt after rotation"
    (let [master-key (encryption/random-master)
          test-data "Data to encrypt"]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      ;; Encrypt data with first DEK
      (let [encrypted-with-old (encryption/encrypt-data test-data)
            _ (encryption/create-dek)  ; Rotate to new DEK
            decrypted (encryption/decrypt-data encrypted-with-old)]
        (is (= test-data decrypted)
            "Should still decrypt data encrypted with old DEK"))

      (encryption/stop))))

;;; ============================================================================
;;; Edge Cases
;;; ============================================================================

(deftest test-encrypt-empty-string
  (testing "Encrypt empty string"
    (let [master-key (encryption/random-master)]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [encrypted (encryption/encrypt-data "")
            decrypted (encryption/decrypt-data encrypted)]
        (is (= "" decrypted) "Should handle empty string"))

      (encryption/stop))))

(deftest test-encrypt-large-data
  (testing "Encrypt large data"
    (let [master-key (encryption/random-master)
          large-data (apply str (repeat 10000 "A"))]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [encrypted (encryption/encrypt-data large-data)
            decrypted (encryption/decrypt-data encrypted)]
        (is (= large-data decrypted) "Should handle large data"))

      (encryption/stop))))

(deftest test-encrypt-unicode
  (testing "Encrypt Unicode data"
    (let [master-key (encryption/random-master)
          unicode-data "Hello 世界 🌍 Здравствуй مرحبا"]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)

      (let [encrypted (encryption/encrypt-data unicode-data)
            decrypted (encryption/decrypt-data encrypted)]
        (is (= unicode-data decrypted) "Should handle Unicode correctly"))

      (encryption/stop))))

(deftest test-dek-persistence
  (testing "DEKs should persist to database and reload on restart"
    (let [master-key (encryption/random-master)]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      ;; Start encryption (creates first DEK)
      (encryption/start master-key)
      (let [first-dek-id encryption/*dek*]
        (is (some? first-dek-id) "Should have created initial DEK")

        ;; Stop and restart encryption
        (encryption/stop)
        (encryption/start master-key)

        ;; Should reload the same DEK
        (is (= first-dek-id encryption/*dek*)
            "Should reload same DEK after restart"))

      (encryption/stop))))

(deftest test-encryption-stop
  (testing "Stop should clear all encryption state"
    (let [master-key (encryption/random-master)]
      (when-not (encryption/dek-table-exists?)
        (encryption/create-dek-table))

      (encryption/start master-key)
      (is (some? encryption/*master-key*) "Master key should be set")
      (is (some? encryption/*dek*) "DEK should be set")

      (encryption/stop)
      (is (nil? encryption/*master-key*) "Master key should be cleared")
      (is (nil? encryption/*dek*) "DEK should be cleared")
      (is (nil? @encryption/deks) "DEKs atom should be cleared"))))

(deftest test-get-nonexistent-dek
  (testing "Getting non-existent DEK should return nil"
    (reset! encryption/deks {1 (.getEncoded (encryption/generate-key 256))})
    (is (nil? (encryption/get-dek 999))
        "Should return nil for non-existent DEK ID")))

;;; ============================================================================
;;; Shamir Secret Sharing Tests
;;; ============================================================================

(deftest test-create-shares
  (testing "Create Shamir secret shares"
    (let [secret (java.math.BigInteger. "12345678901234567890")
          shares (shamir/create-shares secret 5 3)]
      (is (sequential? shares) "Should return sequential collection of shares")
      (is (= 5 (count shares)) "Should create 5 shares")
      (is (every? (fn [[id value]] (and (number? id) (instance? java.math.BigInteger value)))
                  shares)
          "Each share should be [id BigInteger] pair"))))

(deftest test-reconstruct-secret
  (testing "Reconstruct secret from threshold shares"
    (let [secret (java.math.BigInteger. "98765432109876543210")
          shares (shamir/create-shares secret 5 3)
          ;; Use any 3 shares
          subset (take 3 shares)
          reconstructed (shamir/reconstruct-secret subset)]
      (is (= secret reconstructed) "Should reconstruct original secret from threshold shares"))))

(deftest test-reconstruct-with-different-shares
  (testing "Different combinations of threshold shares should work"
    (let [secret (java.math.BigInteger. "11111111111111111111")
          shares (shamir/create-shares secret 5 3)]
      ;; Try different combinations
      (is (= secret (shamir/reconstruct-secret (take 3 shares))))
      (is (= secret (shamir/reconstruct-secret (drop 2 shares))))
      (is (= secret (shamir/reconstruct-secret [(first shares) (nth shares 2) (nth shares 4)]))))))

(deftest test-insufficient-shares
  (testing "Reconstructing with insufficient shares should fail"
    (let [secret (java.math.BigInteger. "99999999999999999999")
          shares (shamir/create-shares secret 5 3)
          ;; Only take 2 shares (below threshold of 3)
          insufficient (take 2 shares)
          reconstructed (shamir/reconstruct-secret insufficient)]
      ;; Should get wrong secret with insufficient shares
      (is (not= secret reconstructed)
          "Should not reconstruct correct secret with insufficient shares"))))

(deftest test-shares-uniqueness
  (testing "Each share should be unique"
    (let [secret (java.math.BigInteger. "88888888888888888888")
          shares (shamir/create-shares secret 5 3)
          share-values (map second shares)]
      (is (= 5 (count (distinct share-values)))
          "All shares should be unique"))))
