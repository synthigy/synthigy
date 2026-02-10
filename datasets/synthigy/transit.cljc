(ns synthigy.transit
  "Transit serialization with custom handlers for Synthigy records.

  Supports both:
  - Synthigy transit records (synthigy.dataset.core.*)
  - EYWA transit records (neyho.eywa.dataset.core.*) → converted to Synthigy"
  (:require
    #?(:clj [patcho.lifecycle :as lifecycle])
    #?(:clj [patcho.patch :as patch])
    [cognitect.transit :as transit]
    synthigy.dataset.core
    synthigy.modeling.core)
  #?(:clj
     (:import
       [com.cognitect.transit WriteHandler ReadHandler]
       [java.io ByteArrayInputStream ByteArrayOutputStream])))

(def ^:dynamic *format* :json-verbose)
(def ^:dynamic *read-transit* (constantly nil))
(def ^:dynamic *write-transit* (constantly nil))

(defn ->transit [data] (*write-transit* data))
(defn <-transit [data] (*read-transit* data))

(defn init-transit-handlers
  "Initialize transit read/write handlers"
  [{:keys [write read]}]
  #?(:clj
     (alter-var-root
       #'*write-transit*
       (fn [_]
         (fn writer [data]
           (when data
             (with-open [baos (ByteArrayOutputStream.)]
               (let [w (transit/writer
                         baos
                         :json-verbose
                         {:handlers write})
                     _ (transit/write w data)
                     ret (.toString baos)]
                 (.reset baos)
                 ret))))))
     :cljs
     (set! *write-transit*
           (fn writer [data]
             (when data
               (let [wr (transit/writer
                          :json-verbose
                          {:handlers write})]
                 (transit/write wr data))))))
  #?(:clj
     (alter-var-root
       #'*read-transit*
       (fn [_]
         (fn reader [data]
           (when data
             (with-open [in (ByteArrayInputStream. (.getBytes data))]
               (transit/read (transit/reader in :json-verbose {:handlers read})))))))
     :cljs
     (set! *read-transit*
           (fn reader [data]
             (when data
               (let [reader (transit/reader :json-verbose {:handlers read})]
                 (transit/read reader data)))))))

(defn record-write-handler
  "Create a transit write handler for a record type"
  [type ks]
  (reify #?(:clj WriteHandler :cljs Object)
    (tag [_ _]
      type)
    (rep [_ rec]
      (transit/tagged-value "map" (select-keys rec ks)))
    (stringRep [_ _] nil)
    (getVerboseHandler [_] nil)))

(defn record-read-handler
  "Create a transit read handler for a record constructor"
  [ctor]
  #?(:clj
     (reify ReadHandler
       (fromRep [_ m] (ctor m)))
     :cljs
     (fn [rep] (ctor rep))))

;; Transit handler registration

(letfn [(synthigy-write-handlers []
          "Write handlers for Synthigy records"
          {synthigy.dataset.core.ERDRelation
           (record-write-handler
             "synthigy.dataset.core.ERDRelation"
             (keys (synthigy.dataset.core/map->ERDRelation nil)))

           synthigy.dataset.core.ERDEntityAttribute
           (record-write-handler
             "synthigy.dataset.core.ERDEntityAttribute"
             (keys (synthigy.dataset.core/map->ERDEntityAttribute nil)))

           synthigy.dataset.core.ERDEntity
           (record-write-handler
             "synthigy.dataset.core.ERDEntity"
             (keys (synthigy.dataset.core/map->ERDEntity nil)))

           synthigy.dataset.core.ERDModel
           (record-write-handler
             "synthigy.dataset.core.ERDModel"
             (keys (synthigy.dataset.core/map->ERDModel nil)))

           synthigy.modeling.core.Coordinate
           (record-write-handler
             "synthigy.modeling.core.Coordinate"
             (keys (synthigy.modeling.core/map->Coordinate nil)))

           synthigy.modeling.core.PathSegment
           (record-write-handler
             "synthigy.modeling.core.PathSegment"
             (keys (synthigy.modeling.core/map->PathSegment nil)))

           synthigy.modeling.core.Path
           (record-write-handler
             "synthigy.modeling.core.Path"
             (keys (synthigy.modeling.core/map->Path nil)))})

        (synthigy-read-handlers []
          "Read handlers for Synthigy records"
          {"synthigy.dataset.core.ERDRelation"
           (record-read-handler synthigy.dataset.core/map->ERDRelation)

           "synthigy.dataset.core.ERDEntityAttribute"
           (record-read-handler synthigy.dataset.core/map->ERDEntityAttribute)

           "synthigy.dataset.core.ERDEntity"
           (record-read-handler synthigy.dataset.core/map->ERDEntity)

           "synthigy.dataset.core.ERDModel"
           (record-read-handler synthigy.dataset.core/map->ERDModel)

           "synthigy.modeling.core.Coordinate"
           (record-read-handler synthigy.modeling.core/map->Coordinate)

           "synthigy.modeling.core.PathSegment"
           (record-read-handler synthigy.modeling.core/map->PathSegment)

           "synthigy.modeling.core.Path"
           (record-read-handler synthigy.modeling.core/map->Path)})

        (eywa-read-handlers []
          "Read handlers for EYWA records → Convert to Synthigy records"
          {"neyho.eywa.dataset.core.ERDRelation"
           (record-read-handler synthigy.dataset.core/map->ERDRelation)

           "neyho.eywa.dataset.core.ERDEntityAttribute"
           (record-read-handler synthigy.dataset.core/map->ERDEntityAttribute)

           "neyho.eywa.dataset.core.ERDEntity"
           (record-read-handler synthigy.dataset.core/map->ERDEntity)

           "neyho.eywa.dataset.core.ERDModel"
           (record-read-handler synthigy.dataset.core/map->ERDModel)

           "neyho.eywa.modeling.core.Coordinate"
           (record-read-handler synthigy.modeling.core/map->Coordinate)

           "neyho.eywa.modeling.core.PathSegment"
           (record-read-handler synthigy.modeling.core/map->PathSegment)

           "neyho.eywa.modeling.core.Path"
           (record-read-handler synthigy.modeling.core/map->Path)})]

  (defn init
    "Initialize transit handlers.

    Supports:
    - Writing Synthigy records with synthigy.* tags
    - Reading Synthigy records (synthigy.* tags)
    - Reading EYWA records (neyho.eywa.* tags) → Converted to Synthigy"
    []
    (init-transit-handlers
      {:write (synthigy-write-handlers)
       :read (merge (synthigy-read-handlers)
                    (eywa-read-handlers))})))


#? (:clj
    (patch/current-version :synthigy/transit "0.1.0"))

(comment
  (init))

;;; ============================================================================
;;; Lifecycle Registration
;;; ============================================================================

#?(:clj
   (lifecycle/register-module!
     :synthigy/transit
     {:start (fn [] (init))}))
