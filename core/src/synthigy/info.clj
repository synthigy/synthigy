;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.info
  "Public discovery data — is auth required, and if so, where."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataset :as dataset]
   [synthigy.engine :as engine]
   [synthigy.iam.access :as access]))

(def modeler-public-client-id
  "ZMETOIKCJISOUMBAXRLOXOLKNSTVSMSGUZLMZOAFPIBJGCSF")

(def desktop-public-client-id
  "IFGSBNADPUUWVSRWUOXHPEARTSTXMAHZIMSLHERFZJZEWYNF")

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
                 :desktop_client_id desktop-public-client-id
                 :audience engine/platform-audience})

      datasets
      (assoc :datasets datasets)

      grants
      (assoc :grants grants))))
