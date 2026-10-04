;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.data
  "Well-known system data records with stable, provider-agnostic identifiers."
  (:require [synthigy.dataset.id :refer [defdata] :as id]))

(defdata :data/synthigy-user
  :euuid #uuid "c5a67922-351e-4ca3-95c2-fa52a7a3e2b5" :xid "RQb935cnLog5tiFsXgfMhv")

(defdata :data/root-role
  :euuid #uuid "601ee98d-796b-43f3-ac1f-881851407f34" :xid "CsRfQHNu3RyCgbpQQdanbd")

(defdata :data/public-role
  :euuid #uuid "746a7348-4daf-4b5a-921a-efc8bd476d88" :xid "FNnFqhZghyvTxtsfXP1wwM")

(defdata :data/public-user
  :euuid #uuid "762d9076-4b78-4918-9eec-262a56a94e95" :xid "FbQG7q3EYXeR7N3oBhd2Gk")

(def synthigy-user-name "Synthigy")

(def ^:dynamic *SYNTHIGY*
  {:euuid (id/data :data/synthigy-user :euuid)
   :xid (id/data :data/synthigy-user :xid)
   :name synthigy-user-name
   :type :SERVICE
   :active true
   :modified_by {:euuid #uuid "c5a67922-351e-4ca3-95c2-fa52a7a3e2b5" :xid "RQb935cnLog5tiFsXgfMhv"}})

(def ^:dynamic *ROOT*
  {:euuid (id/data :data/root-role :euuid)
   :xid (id/data :data/root-role :xid)
   :name "SUPERUSER"
   :description "System role. Bypasses every access rule: all data, the data model and the whole IAM. Hold it only for break-glass administration."
   :active true})

(def ^:dynamic *PUBLIC_ROLE*
  {:euuid (id/data :data/public-role :euuid)
   :xid (id/data :data/public-role :xid)
   :name "Public"
   :description "System role of the anonymous __public__ user. Grant it on an entity to make that entity reachable without a token; takes effect only while public access is enabled."})

(def ^:dynamic *PUBLIC_USER*
  {:euuid (id/data :data/public-user :euuid)
   :xid (id/data :data/public-user :xid)
   :name "__public__"
   :active false})
