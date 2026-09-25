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
   :active true})

(def ^:dynamic *PUBLIC_ROLE*
  {:euuid (id/data :data/public-role :euuid)
   :xid (id/data :data/public-role :xid)
   :name "Public"})

(def ^:dynamic *PUBLIC_USER*
  {:euuid (id/data :data/public-user :euuid)
   :xid (id/data :data/public-user :xid)
   :name "__public__"
   :active false})
