(ns synthigy.dataset.access
  "Access control convenience functions for dataset operations.

  This namespace provides convenience functions that delegate to the current
  access control implementation. The actual protocol is defined in
  synthigy.dataset.access.protocol.

  This design allows:
  - Datasets to work without IAM (using default allow-all implementation)
  - IAM to plug in access control when available
  - Testing with mock access control implementations
  - No circular dependency between datasets and IAM"
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [synthigy.dataset.access.protocol :as p]
   [synthigy.dataset.core :as dataset]
   [synthigy.dataset.id :as id]))

;;; ============================================================================
;;; Default Implementation (Allow All)
;;; ============================================================================

(defrecord AllowAllAccess []
  p/AccessControl
  (entity-allows?   [_ _ _] true)
  (relation-allows? [_ _ _] true)
  (relation-allows? [_ _ _ _] true)
  (scope-allowed?   [_ _] true)
  (roles-allowed?   [_ _] true)
  (superuser?       [_] true)
  (get-principal    [_] nil)
  (principal-eid    [_] nil)
  (role-ids         [_] #{})
  (group-eids       [_] #{}))

;;; ============================================================================
;;; Dynamic Context
;;; ============================================================================

;; Dynamic var holding the current access control implementation.
;; Defaults to AllowAllAccess which permits all operations.
;; IAM layer binds this to an IAM-aware implementation during request processing.
(defonce ^:dynamic *access-control* (->AllowAllAccess))

;;; ============================================================================
;;; Convenience Functions
;;; ============================================================================

(defn entity-allows? [entity-id operations]
  (p/entity-allows? *access-control* entity-id operations))

(defn relation-allows?
  ([relation-id operations]
   (p/relation-allows? *access-control* relation-id operations))
  ([relation-id from-to operations]
   (p/relation-allows? *access-control* relation-id from-to operations)))

(defn scope-allowed? [scope]
  (p/scope-allowed? *access-control* scope))

(defn roles-allowed? [role-ids]
  (p/roles-allowed? *access-control* role-ids))

(defn superuser? []
  (p/superuser? *access-control*))

(defn current-principal []
  (p/get-principal *access-control*))

(defn principal-eid []
  (p/principal-eid *access-control*))

(defn role-ids
  "Set of role id-keys (xids) for the current principal. RBAC."
  []
  (p/role-ids *access-control*))

(defn group-eids
  "Set of group :_eid values for the current principal. RLS."
  []
  (p/group-eids *access-control*))

;;; ============================================================================
;;; Model Protection
;;; ============================================================================

(defn protect-model
  "Filters a dataset model based on IAM permissions.
   Removes entities and relations the current user cannot access."
  [model]
  (when model
    (as-> model m
      (reduce
        (fn [m entity]
          (if (entity-allows? (id/extract entity) #{:read :write})
            m
            (dataset/remove-entity m entity)))
        m
        (dataset/get-entities m))
      (reduce
        (fn [m {from :from to :to :as relation}]
          (let [from-id (id/extract from)
                to-id (id/extract to)
                relation-id (id/extract relation)]
            (if (or
                  (relation-allows? relation-id [from-id to-id] #{:read :write})
                  (relation-allows? relation-id [to-id from-id] #{:read :write}))
              m
              (dataset/remove-relation m relation))))
        m
        (dataset/get-relations m)))))

;;; ============================================================================
;;; Client Schema Projection
;;; ============================================================================

(defn- ->kebab-str [^String n]
  (when n (-> n str/trim csk/->kebab-case-string)))

(defn- ->snake-str [^String n]
  (when n (-> n str/trim csk/->snake_case_string)))

(defn- ->camel-str [^String n]
  (when n (-> n str/trim csk/->camelCaseString)))

;; Output casing for client-schema NAMES (entities/attributes/relations/labels).
;; `/schema` is JSON *data*, not the strict XSQL grammar — so casing here is the
;; CONSUMER's preference, not a contract. Default snake_case (matches the wire's
;; default `key_format`); the `/schema` handler rebinds this per `?key_format=`
;; so codegen/SDKs pick the casing convenient for their language.
(def ^:dynamic *name-fn* ->snake-str)

(def name-fn-for
  "key_format (string/keyword) → name-casing fn. Default falls back to snake."
  {"snake" ->snake-str :snake ->snake-str
   "kebab" ->kebab-str :kebab ->kebab-str
   "camel" ->camel-str :camel ->camel-str})

(defn- collapse-cardinality
  "Project ERD cardinality strings onto the two values the client cares
   about — `one` vs `many` — from the *from* side's perspective. The
   model's relations are already focused to the current entity by
   `focus-entity-relations`."
  [c]
  (case c
    ("o2m" "m2m") "many"
    ("o2o" "m2o") "one"
    "many"))

(defn- attribute->client
  "Return the thin client projection of an entity attribute (or nil when
   the attribute is inactive). Key is the attribute name kebab-cased; the
   value is `{:type, :nullable?, :enum?, :write-only?}`.

   Codegen-facing facts, all read straight off the ERD attribute record
   (`ERDEntityAttribute [.. constraint type configuration ..]`) — no
   derivation lives anywhere but here, so every language consumes the same
   flattened JSON instead of re-deriving from raw model internals:
     - :nullable    — false only for mandatory constraints (`query.clj`
                      treats `mandatory`/`unique+mandatory` as NOT NULL).
                      Omitted (⇒ nullable) otherwise to keep the common case terse.
     - :enum        — member value names, ALL of them. The deploy DDL builds
                      the Postgres enum type from every value's `:name` with no
                      `:active` filter (`postgres.clj`), so the DB accepts all;
                      filtering here would wrongly emit empty unions.
     - :write-only  — `hashed` only (no plaintext read-back). `encrypted`
                      reads back as ciphertext, so it is NOT write-only.

   JSON keys stay kebab-case to match the rest of the schema envelope
   (`id-key`, `version-id`): `synthigy.json` emits the keyword name verbatim,
   so `:write-only` ⇒ \"write-only\"."
  [{:keys [name type constraint configuration] :as attribute}]
  (when (:active attribute)
    [(*name-fn* name)
     (cond-> {:type (or type "string")}
       (#{"mandatory" "unique+mandatory"} constraint) (assoc :nullable false)
       (= "enum" type)   (assoc :enum (mapv :name (:values configuration)))
       (= "hashed" type) (assoc :write-only true))]))

(defn- attribute->xid-pair
  "Return [kebab-name xid-string] for an active attribute; nil otherwise.
   Used to build the `:xids :attributes` map in [[entity->client]] so SDK
   consumers can resolve substrate envelopes (which key `before`/`after`
   by attribute xid) into attribute names."
  [attribute]
  (when (and (:active attribute) (:xid attribute))
    [(*name-fn* (:name attribute)) (str (:xid attribute))]))

(defn- entity-unique-constraints->client
  "Resolve constraint groups (stored as attribute ids) to kebab-cased
   attribute name strings for the current entity. Mirrors
   `get-entity-unique-constraints` but emits names, not ids."
  [entity]
  (let [id->name (into {} (map (juxt id/extract #(*name-fn* (:name %)))) (:attributes entity))
        id-key-name (name (id/key))
        raw (get-in entity [:configuration :constraints :unique])
        named (reduce
                (fn [acc group]
                  (let [ng (keep id->name group)]
                    (if (seq ng) (conj acc (vec ng)) acc)))
                []
                raw)]
    ;; xid/euuid is always a unique constraint even when not stored explicitly
    (if (some #(= % [id-key-name]) named)
      named
      (into [[id-key-name]] named))))

(defn- entity->client
  "Project an ERD entity + its focused relations onto the client-schema
   shape:

     {:name, :xid, :attributes, :relations, :constraints, :xids}

   `:xids` is `{:attributes {kebab-name xid-string}, :relations {label xid-string}}`,
   added so SDK consumers can resolve substrate delta envelopes (which key
   `before`/`after` by attribute xid, and `:element` to relation xid)
   back to human-readable names. The relation xid is the same on both
   sides of the edge — Movie's `\"ratings\"` and UserRating's `\"movie\"`
   share one identity. See `focus-entity-relations` (inverts relations so
   the focused entity is always on the from-side; labels are this
   entity's outgoing perspective)."
  [model entity]
  (let [active-attrs (:attributes entity)
        attrs        (into {} (keep attribute->client) active-attrs)
        attr-xids    (into {} (keep attribute->xid-pair) active-attrs)
        ;; Walk relations once; emit both the existing projection and the
        ;; parallel xid map. Inactive / unlabelled relations are skipped
        ;; (same filter as the legacy shape).
        rel-rows  (keep (fn [{:keys [xid to to-label cardinality active]}]
                          (when (and active (seq to-label))
                            (let [label (*name-fn* to-label)]
                              [label
                               {:to (*name-fn* (:name to))
                                :cardinality (collapse-cardinality cardinality)}
                               (when xid (str xid))])))
                        (dataset/focus-entity-relations model entity))
        rels      (into {} (map (fn [[label v _]] [label v])) rel-rows)
        rel-xids  (into {} (keep (fn [[label _ xid]] (when xid [label xid]))) rel-rows)]
    {:name (:name entity)
     :xid  (when-let [x (:xid entity)] (str x))
     :attributes attrs
     :relations rels
     :constraints {:unique (entity-unique-constraints->client entity)}
     :xids {:attributes attr-xids
            :relations rel-xids}}))

(defn schema
  "Thin projection of an IAM-filtered model for client consumption.

   Shape:
     {:id-key  \"xid\" | \"euuid\"
      :entities {\"user\" {:name, :xid, :attributes, :relations,
                           :constraints, :xids},
                 ...}}

   Each entity carries `:xid` (the entity's identity) and `:xids` —
   `{:attributes {kebab-name xid}, :relations {label xid}}` — so SDK
   consumers can resolve substrate delta envelopes back to names. The
   relation xid is the same on both sides of an edge (Movie's `\"ratings\"`
   and UserRating's `\"movie\"` share one identity).

   `model` must already be IAM-filtered (e.g. via [[protect-model]]); this
   function does the *projection*, not the access check. Passing the model
   in rather than fetching it avoids a circular dependency between this
   namespace and `synthigy.dataset`.

   `entity-names` is optional — when supplied (as a collection of
   kebab-case entity names), the result is restricted to that subset.

   This shape is intentionally narrow: no layout, no icons, no AI hints,
   no module metadata. Only what the transaction buffer needs to
   normalise and diff."
  ([model] (schema model nil))
  ([model entity-names]
   (let [allowed  (dataset/get-entities model)
         ;; Filter matches on canonical snake (independent of output casing) so
         ;; `?entities=` works whatever form the caller passes or requests.
         wanted   (when (seq entity-names) (set (map #(->snake-str (str %)) entity-names)))
         filtered (if wanted
                    (filter #(contains? wanted (->snake-str (:name %))) allowed)
                    allowed)]
     {:id-key (name (id/key))
      :entities (into {}
                      (map (fn [e]
                             [(*name-fn* (:name e))
                              (entity->client model e)]))
                      filtered)})))
