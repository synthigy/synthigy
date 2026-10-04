;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.classification
  "Data classification tags on entity attributes — metadata, not enforcement."
  (:require [clojure.string :as str]))

(def base-vocabulary
  "Documented base tags; an open vocabulary, not enforced."
  {:pii      "Personally identifiable — identifies a natural person alone or in combination (name, email, phone, national id, date of birth)."
   :phi      "Protected health information — health data linked to an identifiable person. HIPAA scope."
   :secret   "Credential or key material — tokens, passwords, private keys, API secrets."
   :internal "Confidential but not personal — commercially sensitive, not subject to personal-data regimes."})

(def redacting-tags
  "Tags whose values the audit trail withholds; the change itself is still recorded."
  #{:secret})

(defn classification
  "Set of classification tags on `attribute`. Empty set when untagged."
  [attribute]
  (or (get-in attribute [:configuration :classification]) #{}))

(defn set-classification
  "Replace the classification tag set on `attribute`."
  [attribute tags]
  (assoc-in attribute [:configuration :classification] (set tags)))

(defn toggle-tag
  "Add `tag` if absent, remove it if present."
  [attribute tag]
  (let [current (classification attribute)]
    (set-classification attribute
                        (if (contains? current tag)
                          (disj current tag)
                          (conj current tag)))))

(defn classified?
  "True when `attribute` carries any classification tag."
  [attribute]
  (boolean (seq (classification attribute))))

(defn redact?
  "True when `attribute` carries at least one redacting tag."
  [attribute]
  (boolean (seq (filter redacting-tags (classification attribute)))))

(defn redaction-marker
  "Stable placeholder naming the tags that caused a value's redaction."
  [attribute]
  (let [tags (sort (filter redacting-tags (classification attribute)))]
    (str "[redacted:" (str/join "," (map name tags)) "]")))
