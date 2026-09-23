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

(ns synthigy.info
  "Public discovery data — is auth required, and if so, where."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataset :as dataset]
   [synthigy.engine :as engine]
   [synthigy.iam.access :as access]))

(def modeler-public-client-id
  "ZMETOIKCJISOUMBAXRLOXOLKNSTVSMSGUZLMZOAFPIBJGCSF")

(defn model-info
  "Deploy timestamps only — version NAMES are human labels and must not leak
   from this unauthenticated, CORS-`*` response. They ship on `/schema`."
  []
  (when (lifecycle/started? :synthigy/dataset)
    (not-empty (update-vals (dataset/deployed-versions) :deployed-at))))

(defn entity-grant-fields []
  (when (lifecycle/started? :synthigy/iam)
    (reduce-kv
     (fn [r rule relation]
       (if-let [field (access/grant-field relation)]
         (assoc r rule field)
         r))
     nil
     access/entity-grants)))

(defn discovery-body []
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        datasets    (model-info)
        grants      (entity-grant-fields)]
    (cond-> {:service "synthigy"
             :auth {:required iam-active?}}
      iam-active?
      (assoc-in [:auth :oidc]
                {:discovery "/.well-known/openid-configuration"
                 :client_id modeler-public-client-id
                 :audience engine/platform-audience})

      datasets
      (assoc :datasets datasets)

      grants
      (assoc :grants grants))))
