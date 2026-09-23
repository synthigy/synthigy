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

(ns synthigy.dataset.projection
  "ERDModelProjectionProtocol implementations. Diff/projection state lives in
   metadata (:added?/:removed?/:diff under :dataset/projection), never in the record."
  (:require
    [clojure.data]
    [synthigy.dataset.core :as dataset
     :refer [ERDModelProjectionProtocol
             get-attribute get-entity get-entities get-relation get-relations
             set-entity set-relation
             normalize-relation focus-entity-relations
             entity-changed? attribute-changed?
             projection-data]]
    [synthigy.dataset.id :as id]))

(extend-protocol ERDModelProjectionProtocol
  ;; ENTITY ATTRIBUTE
  #?(:clj synthigy.dataset.core.ERDEntityAttribute
     :cljs synthigy.dataset.core/ERDEntityAttribute)
  (mark-added [this] (vary-meta (assoc this :active true) assoc-in [:dataset/projection :added?] true))
  (mark-removed [this] (vary-meta (assoc this :active false) assoc-in [:dataset/projection :removed?] true))
  (mark-diff [this diff] (vary-meta this assoc-in [:dataset/projection :diff] diff))
  (added? [this] (boolean (:added? (projection-data this))))
  (removed? [this] (boolean (:removed? (projection-data this))))
  (diff? [this] (boolean (not-empty (:diff (projection-data this)))))
  (diff [this] (:diff (projection-data this)))
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (when-let [this' (cond
                       (dataset/added? this) nil
                       (dataset/diff? this) (merge this (dataset/diff this))
                       :else this)]
      (with-meta this' nil)))
  (project
    [this that]
    {:pre [(or
             (nil? that)
             (instance? synthigy.dataset.core.ERDEntityAttribute that))]}
    ;; FIXME: configuration should implement this protocol itself (or a
    ;; type-dispatched multimethod)
    (if (nil? that)
      (dataset/mark-removed this)
      (let [this-id (id/extract this)
            that-id (id/extract that)]
        (when (not= this-id that-id)
          (throw
            (ex-info "Can't project this on that for different entity attributes!"
                     {:this this
                      :that that})))
        (letfn [(focus-attribute [attribute]
                  (select-keys attribute [:name :type :constraint :configuration :active]))]
          (let [[{config :configuration} n _]
                (clojure.data/diff
                  (focus-attribute that)
                  (focus-attribute this))]
            (if (or (some? n) (not-empty config))
              ;; wrapped under :configuration so postgres.clj can extract
              ;; dconfig from the diff
              (dataset/mark-diff that (or n (when config {:configuration config})))
              that))))))

  ;; ENTITY
  #?(:clj synthigy.dataset.core.ERDEntity
     :cljs synthigy.dataset.core/ERDEntity)
  (mark-added [this]
    (vary-meta
      (update this :attributes #(mapv dataset/mark-added %))
      assoc-in [:dataset/projection :added?] true))
  (mark-removed [this]
    (vary-meta
      (update this :attributes #(mapv dataset/mark-removed %))
      assoc-in [:dataset/projection :removed?] true))
  (mark-diff [this diff] (vary-meta this assoc-in [:dataset/projection :diff] diff))
  (added? [this] (boolean (:added? (projection-data this))))
  (removed? [this] (boolean (:removed? (projection-data this))))
  (diff? [this]
    (let [{:keys [diff added?]} (projection-data this)]
      (and
        (not added?)
        (or
          (not-empty (dissoc diff :width :height))
          (some attribute-changed? (:attributes this))))))
  (diff [this] (:diff (projection-data this)))
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (when-let [this'
               (cond
                 (dataset/added? this) nil
                 ;;
                 (dataset/diff? this)
                 (->
                   this
                   (merge this (dissoc (dataset/diff this) :attributes))
                   (update :attributes
                           (fn [as]
                             (vec
                               (remove nil? (map dataset/suppress as))))))
                 :else this)]
      (with-meta this' nil)))
  (project
    [this that]
    {:pre [(or
             (nil? that)
             (instance? synthigy.dataset.core.ERDEntity that))]}
    (if (nil? that)
      (dataset/mark-removed this)
      (let [this-id (id/extract this)
            that-id (id/extract that)]
        (when (not= this-id that-id)
          (throw
            (ex-info "Can't project this on that for different entities!"
                     {:this this
                      :that that})))
        (let [that-ids (set (map id/extract (:attributes that)))
              this-ids (set (map id/extract (:attributes this)))
              [oid nid sid] (clojure.data/diff this-ids that-ids)
              [o _ _] (when (and this that)
                        (clojure.data/diff
                          (select-keys this [:name])
                          (select-keys that [:name])))
              that-attributes (:attributes that)
              that-attribute-ids (set (map id/extract that-attributes))
              this-attributes (remove (comp that-attribute-ids id/extract) (:attributes this))
              attributes' (reduce
                            (fn [as attribute]
                              (let [id (id/extract attribute)]
                                (conj
                                  as
                                  (cond-> attribute
                                    (and
                                      (not-empty nid)
                                      (nid id))
                                    dataset/mark-added

                                    (and
                                      (set? sid)
                                      (sid id))
                                    ;; entity rename may affect enum spec — mark
                                    ;; :entity/name diff unless the attribute
                                    ;; already has one
                                    (as-> a (dataset/project (get-attribute this id) a)
                                      (if (and (:name o) (not (dataset/diff? a)))
                                        (dataset/mark-diff a {:entity/name (:name o)})
                                        a))))))
                            []
                            (concat
                              that-attributes
                              this-attributes))
              ;; whole-configuration diff, minus :rls (which has its own
              ;; dual-base projection) —
              ;; storing the complete old config, not just the changed key,
              ;; fixes suppress too
              config-old (dissoc (:configuration this) :rls)
              config-new (dissoc (:configuration that) :rls)
              changed-attributes (vec (filter attribute-changed? attributes'))]
          (cond->
            (assoc that :attributes attributes')
            ;; TODO: enum-typed attributes should be force-marked diffed on
            ;; entity rename
            (some? o)
            (vary-meta assoc-in [:dataset/projection :diff] o)

            (not= config-old config-new)
            (vary-meta assoc-in [:dataset/projection :diff :configuration] config-old)

            (not-empty changed-attributes)
            (vary-meta assoc-in [:dataset/projection :diff :attributes] changed-attributes))))))

  ;; RELATION
  #?(:clj synthigy.dataset.core.ERDRelation
     :cljs synthigy.dataset.core/ERDRelation)
  (mark-added [this] (vary-meta this assoc-in [:dataset/projection :added?] true))
  (mark-removed [this] (vary-meta this assoc-in [:dataset/projection :removed?] true))
  (mark-diff [this diff] (vary-meta this assoc-in [:dataset/projection :diff] diff))
  (added? [this] (boolean (:added? (projection-data this))))
  (removed? [this] (boolean (:removed? (projection-data this))))
  (diff? [this] (boolean (not-empty (:diff (projection-data this)))))
  (diff [this] (:diff (projection-data this)))
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (when-let [this'
               (cond
                 (dataset/added? this) nil
                 (dataset/diff? this) (->
                                        this
                                        (merge (dissoc (dataset/diff this) :from :to))
                                        (update :from dataset/suppress)
                                        (update :to dataset/suppress)
                                        (with-meta nil))
                 :else this)]
      (with-meta this' nil)))
  (project
    [this that]
    {:pre [(or
             (nil? that)
             (and
               (instance? synthigy.dataset.core.ERDRelation that)
               (= (id/extract this) (id/extract that))))]}
    (if (some? that)
      (let [ks [:from-label :to-label :cardinality]
            this (normalize-relation this)
            that (normalize-relation that)
            [o _] (clojure.data/diff
                    (select-keys this ks)
                    (select-keys that ks))
            from-projection (when (not=
                                    (:name (:from this))
                                    (:name (:from that)))
                              {:name (:name (:from that))})
            to-projection (when (not=
                                  (:name (:to this))
                                  (:name (:to that)))
                            {:name (:name (:to that))})
            ;; whole-configuration diff — relation RBAC lives at [:configuration
            ;; :rbac <direction> :enabled]
            config-old (:configuration this)
            config-new (:configuration that)
            o' (cond-> o
                 from-projection (assoc :from from-projection)
                 to-projection (assoc :to to-projection)
                 (not= config-old config-new) (assoc :configuration config-old))]
        (if (some? o')
          (dataset/mark-diff that o')
          that))
      (dataset/mark-removed this)))

  ;; MODEL
  #?(:clj synthigy.dataset.core.ERDModel
     :cljs synthigy.dataset.core/ERDModel)
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (with-meta
      (reduce
        (fn [m r]
          (->
            m
            (set-relation (dataset/suppress r))
            (with-meta nil)))
        (reduce
          (fn [m e]
            (->
              m
              (set-entity (dataset/suppress e))
              (with-meta nil)))
          this
          (get-entities this))
        (get-relations this))
      nil))
  (project
    [this that]
    (as-> that projection
      (reduce
        (fn [m e]
          (set-entity m (dataset/project (get-entity this (id/extract e)) e)))
        projection
        (get-entities projection))
      (reduce
        (fn [m r]
          (set-relation m (dataset/project (get-relation this (id/extract r)) r)))
        projection
        (get-relations projection))
      ;; second pass: relations missing in `that` whose endpoint entities
      ;; changed in `that`
      (let [that-relations (get-relations projection)
            this-relations (distinct
                             (mapcat
                               (comp normalize-relation #(focus-entity-relations this %))
                               (filter entity-changed? (get-entities projection))))
            that-relation-ids (set (map id/extract that-relations))
            target-relations (remove
                               (comp that-relation-ids id/extract)
                               this-relations)]
        (reduce
          (fn [m {from :from
                  to :to
                  :as r}]
            (let [id (id/extract r)
                  from-id (id/extract from)
                  to-id (id/extract to)
                  from (get-entity projection from-id)
                  to (get-entity projection to-id)]
              (if (and from to)
                (let [from-name-change (:name (dataset/diff from))
                      to-name-change (:name (dataset/diff to))
                      relation-projection (dataset/project (get-relation this id)
                                                           (-> r
                                                               (assoc :from from)
                                                               (assoc :to to)))
                      final-relation (if (or from-name-change to-name-change)
                                       (dataset/mark-diff
                                         relation-projection
                                         (merge (dataset/diff relation-projection)
                                                {:entity.to/change {:name to-name-change}
                                                 :entity.from/change {:name from-name-change}}))
                                       r)]
                  (dataset/set-relation m final-relation))
                m)))
          projection
          target-relations))))

  ;; NIL
  nil
  (mark-removed [_] nil)
  (mark-added [_] nil)
  (mark-diff [_ _] nil)
  (project [_ that] (when that (dataset/mark-added that))))
