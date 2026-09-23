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
  "Tags whose values must never reach the audit plug or log sinks."
  #{:pii :phi :secret})

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
