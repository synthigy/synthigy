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

(ns synthigy.timestamp
  "Timestamps are UTC instants: one grammar, one normalization, every backend."
  (:require [clojure.string]))

(def pattern
  #"(\d{4})(?:-(\d{2})(?:-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?(Z|[+-]\d{2}:?\d{2})?)?)?)?")

(defn invalid
  [value]
  (ex-info (str "Invalid timestamp " (pr-str value)
                " — use ISO-8601, UTC unless an offset is given: \"2026\", \"2026-09\","
                " \"2026-09-01\", \"2026-09-01 14:30\", \"2026-09-01T14:30:00.5Z\","
                " \"2026-09-01T14:30:00+02:00\"")
           {:code "INVALID_TIMESTAMP" :value value}))

(defn int-or [s default]
  (if s #?(:clj (Long/parseLong s) :cljs (js/parseInt s 10)) default))

(defn offset-minutes [o]
  (if (or (nil? o) (= "Z" o))
    0
    (let [sign (if (= \- (first o)) -1 1)
          digits (apply str (remove #{\: \+ \-} o))]
      (* sign (+ (* 60 (int-or (subs digits 0 2) 0)) (int-or (subs digits 2 4) 0))))))

(defn fields
  "The parsed components of a timestamp literal, or nil when it doesn't match the grammar."
  [s]
  (when-let [[_ y mo d h mi sec frac off] (re-matches pattern s)]
    {:year (int-or y 0) :month (int-or mo 1) :day (int-or d 1)
     :hour (int-or h 0) :minute (int-or mi 0) :second (int-or sec 0)
     :nanos (if frac (int-or (subs (str frac "000000000") 0 9) 0) 0)
     :offset (offset-minutes off)}))

#?(:clj
   (defn parse
     "Parse a timestamp literal to a java.time.Instant; a partial date is the start of its period."
     ^java.time.Instant [value]
     (let [f (fields (clojure.string/trim (str value)))]
       (when-not f (throw (invalid value)))
       (try
         (let [{:keys [year month day hour minute second nanos offset]} f]
           (-> (java.time.LocalDateTime/of (int year) (int month) (int day)
                                           (int hour) (int minute) (int second) (int nanos))
               (.toInstant (java.time.ZoneOffset/ofTotalSeconds (* 60 offset)))))
         (catch java.time.DateTimeException _ (throw (invalid value))))))
   :cljs
   (defn parse
     "Parse a timestamp literal to a js/Date; a partial date is the start of its period."
     [value]
     (let [f (fields (clojure.string/trim (str value)))]
       (when-not f (throw (invalid value)))
       (let [{:keys [year month day hour minute second nanos offset]} f
             d (js/Date. (js/Date.UTC year (dec month) day hour minute second
                                      (quot nanos 1000000)))]
         (when-not (and (= year (.getUTCFullYear d)) (= (dec month) (.getUTCMonth d))
                        (= day (.getUTCDate d)) (= hour (.getUTCHours d))
                        (= minute (.getUTCMinutes d)) (= second (.getUTCSeconds d)))
           (throw (invalid value)))
         (js/Date. (- (.getTime d) (* 60000 offset)))))))

(defn valid?
  [value]
  (try (parse value) true (catch #?(:clj Exception :cljs :default) _ false)))

#?(:clj
   (defn ->instant
     "Any temporal value (or literal) as a UTC instant; zone-less values are UTC."
     ^java.time.Instant [value]
     (cond
       (instance? java.time.Instant value) value
       ;; before Date: Timestamp IS a Date, and its epoch is JVM-zone-shifted
       (instance? java.sql.Timestamp value) (.toInstant (.toLocalDateTime ^java.sql.Timestamp value)
                                                        java.time.ZoneOffset/UTC)
       (instance? java.util.Date value) (.toInstant ^java.util.Date value)
       (instance? java.time.LocalDateTime value) (.toInstant ^java.time.LocalDateTime value
                                                             java.time.ZoneOffset/UTC)
       (instance? java.time.LocalDate value) (.toInstant (.atStartOfDay ^java.time.LocalDate value)
                                                         java.time.ZoneOffset/UTC)
       (instance? java.time.OffsetDateTime value) (.toInstant ^java.time.OffsetDateTime value)
       (instance? java.time.ZonedDateTime value) (.toInstant ^java.time.ZonedDateTime value)
       (string? value) (parse value)
       :else (throw (invalid value)))))

#?(:clj
   (defn ->date
     ^java.util.Date [value]
     (java.util.Date/from (->instant value))))

#?(:clj
   (defn ->utc-local
     "UTC wall-clock value for a zone-less timestamp column."
     ^java.time.LocalDateTime [value]
     (java.time.LocalDateTime/ofInstant (->instant value) java.time.ZoneOffset/UTC)))

#?(:clj
   (def sortable-format
     (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")))

#?(:clj
   (defn ->sortable-text
     "UTC text in SQLite CURRENT_TIMESTAMP's shape, so string order is time order."
     [value]
     (let [s (.format ^java.time.format.DateTimeFormatter sortable-format (->utc-local value))]
       (if (.endsWith s ".000") (subs s 0 19) s))))
