(ns synthigy.util
  "Shared, dependency-free utility helpers for the Synthigy platform.")

;; ── Time durations ──────────────────────────────────────────────────────
;;
;; These helpers convert a count of time units into a number of MILLISECONDS,
;; the same unit used by `(now)` / `System/currentTimeMillis`.
;;
;; They are pure DURATIONS (offsets), not timestamps — they carry no timezone
;; and can be added directly to an epoch-millis instant, e.g.
;;
;;   (+ (now) (minutes 5))  ; => instant 5 minutes from now
;;
;; `(now)` returns milliseconds since the Unix epoch (1970-01-01T00:00:00Z),
;; which is an absolute instant and therefore inherently UTC.

(defn milliseconds [n] n)
(defn seconds [n] (* n 1000))
(defn minutes [n] (* n 60 1000))
(defn hours   [n] (* n 60 60 1000))
(defn days    [n] (* n 24 60 60 1000))

(defn now
  "Current instant as milliseconds since the Unix epoch
   (1970-01-01T00:00:00Z, UTC)."
  []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))
