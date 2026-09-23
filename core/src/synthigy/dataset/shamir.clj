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

(ns synthigy.dataset.shamir
  "Shamir Secret Sharing implementation.

  Pure cryptographic utility for splitting secrets into shares using
  Shamir's Secret Sharing algorithm. No dependencies on IAM or other layers.

  DEPRECATED as a first-class initialization mode: `:env`/`:file`/pluggable
  KeyWrapProvider (see synthigy.dataset.encryption) cover normal operation.
  Kept ONLY for break-glass master-key recovery on air-gapped installs where
  no provider/network is available — do not build new features on this."
  (:import
   (java.security SecureRandom)
   (java.util Random)
   (java.math BigInteger)))

(def prime-field (BigInteger. "340282366920938463463374607431768211297"))
; (def prime-field (BigInteger. "115792089237316195423570985008687907853269984665640564039457584007913129639937"))

(defn secure-random-int
  [upper-bound]
  (let [random-bytes (byte-array 32)
        _ (.nextBytes (SecureRandom.) random-bytes)
        random-int (BigInteger. 1 random-bytes)]
    (.mod random-int upper-bound)))

(defn random-coefficients
  "Generates secure random coefficients for a polynomial of degree t-1."
  [secret t]
  (cons secret (repeatedly (dec t) #(secure-random-int prime-field))))

(defn evaluate-polynomial
  "Evaluates the polynomial at a given x-value within the prime field."
  [coefficients x]
  (reduce
   (fn [acc [i coeff]]
     (.mod (.add acc (.multiply coeff (.pow (BigInteger/valueOf x) (long i)))) prime-field))
   BigInteger/ZERO
   (map-indexed vector coefficients)))

(defn create-shares
  "Creates shares from the secret with a minimum threshold of t shares."
  [secret n t]
  (let [coefficients (random-coefficients secret t)]
    (map #(vector % (evaluate-polynomial coefficients %)) (range 1 (inc n)))))

(defn modular-inverse
  "Calculates the modular inverse of a number within the prime field."
  [n prime-field]
  (.modPow n (.subtract prime-field BigInteger/TWO) prime-field))

(defn lagrange-interpolation
  "Reconstructs the secret using Lagrange interpolation within the prime field."
  [shares x]
  (reduce
   (fn [sum [xi yi]]
     (.mod (.add sum
                 (.multiply yi
                            (reduce
                             (fn [prod [xj _]]
                               (if (= xi xj)
                                 prod
                                 (let [numinator (.subtract (BigInteger/valueOf x) (BigInteger/valueOf xj))
                                       denuminator (.subtract (BigInteger/valueOf xi) (BigInteger/valueOf xj))
                                       inverse-denuminator (modular-inverse denuminator prime-field)]
                                   (.mod
                                    #_(.multiply prod (.modInverse denuminator prime-field))
                                    (.multiply prod (.mod (.multiply numinator inverse-denuminator) prime-field))
                                    prime-field))))
                             BigInteger/ONE
                             shares)))
           prime-field))
   BigInteger/ZERO
   shares))

(defn reconstruct-secret
  "Reconstructs the secret from the shares."
  [shares]
  (lagrange-interpolation shares BigInteger/ZERO))

;; Usage Example
(comment
  (let [secret (BigInteger. 128 (Random.)) ;; Large integer secret
        n 5                                ;; Total number of shares
        t 3]                               ;; Minimum threshold of shares

    (def shares (create-shares secret n t))
    (println "Generated Shares:" shares)

    ;; Select a subset of shares for reconstruction
    (def selected-shares (take t shares))
    (def reconstructed-secret (reconstruct-secret (list (nth shares 0) (nth shares 3) (nth shares 4))))
    (println "Reconstructed Secret:" (= reconstructed-secret secret))))
