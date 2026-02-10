(ns synthigy.dataset.projection
  "Projection protocol implementations for dataset records.

  Implements ERDModelProjectionProtocol for:
  - ERDEntityAttribute
  - ERDEntity
  - ERDRelation
  - ERDModel

  Projections track changes between models using metadata:
  - :added? - element was added
  - :removed? - element was removed
  - :diff - differences between versions"
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

;; Extend ERDModelProjectionProtocol for ERDEntityAttribute

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
    ;; FIXME configuration should also implement this protocol or
    ;; at least some multimethod that would return configuration diff
    ;; based on attribute type
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
                  (select-keys attribute [:name :type :constraint :configuration]))]
          (let [[{config :configuration} n _]
                (clojure.data/diff
                  (focus-attribute that)
                  (focus-attribute this))]
            ;; 1. Check configuration has been extended and that contains more
            ;;    information than this
            ;; 2. Check if some existing attribute changes were made
            (if (or (some? n) (not-empty config))
              ;; Wrap config under :configuration key so postgres.clj can find it
              ;; when extracting dconfig via (let [{... dconfig :configuration} diff])
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
    ;; If that exists
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
              ;; Separate new ids from old and same ids
              [oid nid sid] (clojure.data/diff this-ids that-ids)
              ;; Check if name has changed
              [o _ _] (when (and this that)
                        (clojure.data/diff
                          (select-keys this [:name])
                          (select-keys that [:name])))
              that-attributes (:attributes that)
              that-attribute-ids (set (map id/extract that-attributes))
              this-attributes (remove (comp that-attribute-ids id/extract) (:attributes this))
              attributes' (reduce
                            ;; Reduce attributes
                            (fn [as attribute]
                              (let [id (id/extract attribute)]
                                (conj
                                  as
                                  (cond-> attribute
                                    (and
                                      (not-empty nid)
                                      (nid id))
                                    dataset/mark-added
                                    ;;
                                    (and
                                      (set? sid)
                                      (sid id))
                                    ;; Project this attribute to that attribute
                                    (as-> a (dataset/project (get-attribute this id) a)
                                      ;; if entity name has changed that might affect enum
                                      ;; specification... So check if attribute already has diff
                                      ;; if it does do nothing
                                      ;; if it doesn't mark attribute diff for :entity/name
                                      (if (and (:name o) (not (dataset/diff? a)))
                                        (dataset/mark-diff a {:entity/name (:name o)})
                                        a))))))
                            []
                            (concat
                              that-attributes
                              this-attributes))
              cso (get-in this [:configuration :constraints :unique])
              csn (get-in that [:configuration :constraints :unique])
              changed-attributes (vec (filter attribute-changed? attributes'))]
          (cond->
            (assoc that :attributes attributes')
            ;; TODO - ENUMs are affected when entity name changes as well... we should mark
            ;; attribute of type enum as diffed so that those attributes are then
            ;; renamed!
            (some? o)
            (vary-meta assoc-in [:dataset/projection :diff] o)
            ;;
            (not= cso csn)
            (vary-meta assoc-in [:dataset/projection :diff :configuration :constraints :unique] cso)
            ;;
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
    ;; If that exists
    (if (some? that)
      ;; Check if relations are the same
      (let [ks [:from-label :to-label :cardinality]
            this (normalize-relation this)
            that (normalize-relation that)
            ;; Compute difference between this and that
            [o _] (clojure.data/diff
                    (select-keys this ks)
                    (select-keys that ks))
            ;; Check only entity names since that might
            ;; affect relation
            from-projection (when (not=
                                    (:name (:from this))
                                    (:name (:from that)))
                              {:name (:name (:from that))})
            to-projection (when (not=
                                  (:name (:to this))
                                  (:name (:to that)))
                            {:name (:name (:to that))})
            o' (cond-> o
                 from-projection (assoc :from from-projection)
                 to-projection (assoc :to to-projection))]
        ;; And if there is some difference than
        (if (some? o')
          ;; return that with projected difference
          (dataset/mark-diff that o')
          ;; otherwise return that
          that))
      ;; If that does't exist than return this with projected removed metadata
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
      ;; Take into account relations that are missing
      ;; in that and entites have been changed in that
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
