(ns synthigy.iam.encryption-test
  "Tests for EncryptionProvider protocol and implementations."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [synthigy.iam.encryption :as encryption]
   [synthigy.iam.events :as events]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def test-provider (atom nil))

(defn provider-fixture [f]
  "Setup/teardown test provider for each test"
  (let [provider (encryption/->AtomEncryptionProvider (atom '()))]
    (reset! test-provider provider)
    (encryption/start! provider)
    (f)
    (encryption/stop! provider)))

(use-fixtures :each provider-fixture)

;; =============================================================================
;; Protocol Implementation Tests
;; =============================================================================

(deftest test-sign-and-verify-jwt
  (testing "Sign and verify JWT with AtomEncryptionProvider"
    (let [provider @test-provider
          keypair (encryption/rotate-keypair provider)
          data {:sub "test-user" :iat 123456 :exp 999999}]

      (testing "Sign JWT successfully"
        (let [token (encryption/sign-jwt provider data {:alg :rs256})]
          (is (string? token))
          (is (re-matches #"[^.]+\.[^.]+\.[^.]+" token) "JWT has three parts")))

      (testing "Verify JWT successfully"
        (let [token (encryption/sign-jwt provider data {:alg :rs256})
              verified (encryption/verify-jwt provider token)]
          (is (map? verified))
          (is (= "test-user" (:sub verified)))))

      (testing "Verify fails with invalid token"
        (is (nil? (encryption/verify-jwt provider "invalid.token.here"))))

      (testing "Verify fails with tampered token"
        (let [token (encryption/sign-jwt provider data {:alg :rs256})
              tampered (str token "tampered")]
          (is (nil? (encryption/verify-jwt provider tampered))))))))

(deftest test-no-keypairs-error
  (testing "Sign JWT throws error when no keypairs"
    (let [provider (encryption/->AtomEncryptionProvider (atom '()))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"No encryption keypairs available"
           (encryption/sign-jwt provider {:sub "test"} {:alg :rs256}))))))

(deftest test-keypair-rotation
  (testing "Keypair rotation and management"
    (let [provider @test-provider]

      (testing "Rotate keypair generates new keypair"
        (let [keypair (encryption/rotate-keypair provider)]
          (is (map? keypair))
          (is (:kid keypair))
          (is (:public keypair))
          (is (:private keypair))))

      (testing "List keypairs shows added keypair"
        (let [keypairs (encryption/list-keypairs provider)]
          (is (= 1 (count keypairs)))))

      (testing "Get keypair by kid works"
        (let [keypair (first (encryption/list-keypairs provider))
              found (encryption/get-keypair-by-kid provider (:kid keypair))]
          (is (= keypair found))))

      (testing "Get keypair by invalid kid returns nil"
        (is (nil? (encryption/get-keypair-by-kid provider "invalid-kid")))))))

(deftest test-max-3-keypairs
  (testing "Max 3 keypairs enforcement (FIFO rotation)"
    (let [provider @test-provider]

      ;; Add 4 keypairs
      (let [kp1 (encryption/rotate-keypair provider)
            kp2 (encryption/rotate-keypair provider)
            kp3 (encryption/rotate-keypair provider)
            kp4 (encryption/rotate-keypair provider)
            keypairs (encryption/list-keypairs provider)]

        (testing "Only 3 keypairs retained"
          (is (= 3 (count keypairs))))

        (testing "Oldest keypair (kp1) was evicted"
          (is (nil? (encryption/get-keypair-by-kid provider (:kid kp1)))))

        (testing "Newest keypairs (kp2, kp3, kp4) retained"
          (is (encryption/get-keypair-by-kid provider (:kid kp2)))
          (is (encryption/get-keypair-by-kid provider (:kid kp3)))
          (is (encryption/get-keypair-by-kid provider (:kid kp4))))

        (testing "Keypairs in reverse chronological order (newest first)"
          (is (= (:kid kp4) (:kid (first keypairs))))
          (is (= (:kid kp3) (:kid (second keypairs))))
          (is (= (:kid kp2) (:kid (nth keypairs 2)))))))))

(deftest test-verify-with-old-keypair
  (testing "Verify JWT signed with old (evicted) keypair fails"
    (let [provider @test-provider
          kp1 (encryption/rotate-keypair provider)
          token (encryption/sign-jwt provider {:sub "old"} {:alg :rs256})]

      ;; Add 3 more keypairs to evict kp1
      (encryption/rotate-keypair provider)
      (encryption/rotate-keypair provider)
      (encryption/rotate-keypair provider)

      (testing "Old keypair evicted"
        (is (nil? (encryption/get-keypair-by-kid provider (:kid kp1)))))

      (testing "Cannot verify token signed with evicted keypair"
        (is (nil? (encryption/verify-jwt provider token)))))))

;; =============================================================================
;; RSAEncryptionProvider-Specific Tests
;; =============================================================================

(deftest test-rsa-provider-event-publishing
  (testing "RSAEncryptionProvider publishes events"
    (let [captured-events (atom [])
          provider (encryption/->RSAEncryptionProvider)]

      ;; Mock publish to capture events
      (with-redefs [events/publish (fn [topic data]
                                     (swap! captured-events conj {:topic topic :data data}))]
        (testing "Start provider"
          (encryption/start! provider))

        (testing "Add keypair publishes :keypair/added"
          (let [keypair (encryption/generate-key-pair)]
            (encryption/add-keypair provider keypair)
            (is (some #(= (:topic %) :keypair/added) @captured-events))
            (is (some #(= (:key-pair (:data %)) keypair) @captured-events))))

        (testing "Evicting keypair publishes :keypair/removed"
          (reset! captured-events [])
          ;; Add 4 keypairs to trigger eviction
          (dotimes [_ 4]
            (encryption/rotate-keypair provider))
          (is (some #(= (:topic %) :keypair/removed) @captured-events)))

        (testing "Stop provider"
          (encryption/stop! provider))))))

(deftest test-rsa-provider-validation
  (testing "RSAEncryptionProvider validates keypairs"
    (let [provider (encryption/->RSAEncryptionProvider)]
      (encryption/start! provider)

      (testing "Rejects invalid public key"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Unacceptable public key"
             (encryption/add-keypair provider {:public "invalid" :private nil}))))

      (testing "Rejects invalid private key"
        (let [keypair (encryption/generate-key-pair)]
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               #"Unacceptable private key"
               (encryption/add-keypair provider {:public (:public keypair) :private "invalid"})))))

      (encryption/stop! provider))))

;; =============================================================================
;; Global Provider Management Tests
;; =============================================================================

(deftest test-set-encryption-provider
  (testing "set-encryption-provider! lifecycle management"
    (let [provider1 (encryption/->AtomEncryptionProvider (atom '()))
          provider2 (encryption/->AtomEncryptionProvider (atom '()))
          old-provider encryption/*encryption-provider*]

      (try
        (testing "Set first provider"
          (encryption/set-encryption-provider! provider1)
          (is (= provider1 encryption/*encryption-provider*)))

        (testing "Set second provider (stops old, starts new)"
          (encryption/set-encryption-provider! provider2)
          (is (= provider2 encryption/*encryption-provider*)))

        (finally
          ;; Restore original provider
          (when old-provider
            (alter-var-root #'encryption/*encryption-provider* (constantly old-provider))))))))

(deftest test-with-encryption-provider
  (testing "with-encryption-provider temporary override"
    (let [global-provider (encryption/->AtomEncryptionProvider (atom '()))
          temp-provider (encryption/->AtomEncryptionProvider (atom '()))
          old-provider encryption/*encryption-provider*]

      (try
        (encryption/set-encryption-provider! global-provider)

        (testing "Global provider is active"
          (is (= global-provider encryption/*encryption-provider*)))

        (testing "Temporary override works"
          (encryption/with-encryption-provider temp-provider
            (is (= temp-provider encryption/*encryption-provider*))))

        (testing "Global provider restored after override"
          (is (= global-provider encryption/*encryption-provider*)))

        (finally
          ;; Restore original provider
          (when old-provider
            (alter-var-root #'encryption/*encryption-provider* (constantly old-provider))))))))

;; =============================================================================
;; Backward Compatibility Tests
;; =============================================================================

(deftest test-backward-compatible-sign-data
  (testing "sign-data wrapper delegates to provider"
    (let [provider @test-provider
          old-provider encryption/*encryption-provider*]

      (try
        (encryption/set-encryption-provider! provider)
        (encryption/rotate-keypair provider)

        (testing "sign-data works with 1-arity"
          (let [token (encryption/sign-data {:sub "test"})]
            (is (string? token))
            (is (re-matches #"[^.]+\.[^.]+\.[^.]+" token))))

        (testing "sign-data works with 2-arity"
          (let [token (encryption/sign-data {:sub "test"} {:alg :rs256})]
            (is (string? token))))

        (finally
          ;; Restore original provider
          (when old-provider
            (alter-var-root #'encryption/*encryption-provider* (constantly old-provider))))))))

(deftest test-backward-compatible-unsign-data
  (testing "unsign-data wrapper delegates to provider"
    (let [provider @test-provider
          old-provider encryption/*encryption-provider*]

      (try
        (encryption/set-encryption-provider! provider)
        (encryption/rotate-keypair provider)

        (let [data {:sub "test-user" :iat 123456}
              token (encryption/sign-data data)]

          (testing "unsign-data verifies token"
            (let [verified (encryption/unsign-data token)]
              (is (map? verified))
              (is (= "test-user" (:sub verified)))))

          (testing "unsign-data returns nil for invalid token"
            (is (nil? (encryption/unsign-data "invalid.token.here")))))

        (finally
          ;; Restore original provider
          (when old-provider
            (alter-var-root #'encryption/*encryption-provider* (constantly old-provider))))))))

(deftest test-no-provider-configured-error
  (testing "Functions throw when no provider configured"
    (let [old-provider encryption/*encryption-provider*]

      (try
        (alter-var-root #'encryption/*encryption-provider* (constantly nil))

        (testing "sign-data throws IllegalStateException"
          (is (thrown? IllegalStateException
                       (encryption/sign-data {:sub "test"}))))

        (testing "unsign-data throws IllegalStateException"
          (is (thrown? IllegalStateException
                       (encryption/unsign-data "token"))))

        (testing "init-default-encryption throws IllegalStateException"
          (is (thrown? IllegalStateException
                       (encryption/init-default-encryption))))

        (finally
          ;; Restore original provider
          (when old-provider
            (alter-var-root #'encryption/*encryption-provider* (constantly old-provider))))))))

;; =============================================================================
;; Helper Function Tests
;; =============================================================================

(deftest test-generate-key-pair
  (testing "generate-key-pair creates valid RSA keypair"
    (let [keypair (encryption/generate-key-pair)]

      (testing "Keypair has required keys"
        (is (:kid keypair))
        (is (:public keypair))
        (is (:private keypair)))

      (testing "Public key is valid RSA public key"
        (is (instance? java.security.interfaces.RSAPublicKey (:public keypair))))

      (testing "Private key is valid RSA private key"
        (is (instance? java.security.interfaces.RSAPrivateKey (:private keypair))))

      (testing "kid is non-empty string"
        (is (string? (:kid keypair)))
        (is (not-empty (:kid keypair)))))))

(deftest test-encode-rsa-key
  (testing "encode-rsa-key produces JWK format"
    (let [keypair (encryption/generate-key-pair)
          jwk (encryption/encode-rsa-key (:public keypair))]

      (testing "JWK has required fields"
        (is (= "RSA" (:kty jwk)))
        (is (= "sig" (:use jwk)))
        (is (= "RS256" (:alg jwk)))
        (is (:kid jwk))
        (is (:n jwk))
        (is (:e jwk)))

      (testing "kid is consistent"
        (is (= (:kid keypair) (:kid jwk))))

      (testing "n and e are base64-url encoded (no padding)"
        (is (not (re-find #"=" (:n jwk))))
        (is (not (re-find #"=" (:e jwk))))))))

(deftest test-base64-url-encode
  (testing "base64-url-encode removes padding"
    (let [input (.getBytes "test data")
          encoded (encryption/base64-url-encode input)]

      (testing "Result is string"
        (is (string? encoded)))

      (testing "No padding characters"
        (is (not (re-find #"=" encoded)))))))
