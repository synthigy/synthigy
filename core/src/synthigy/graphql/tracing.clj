(ns synthigy.graphql.tracing
  "Apollo tracing support for GraphQL execution.

   Captures timing at:
   - Request start
   - Parsing
   - Validation
   - Execution

   Follows Apollo Tracing spec for compatibility with GraphQL tooling."
  (:require
    [com.walmartlabs.lacinia.tracing :as lacinia-tracing])
  (:import
    [java.time Instant]))

;;; ============================================================================
;;; Timing Capture
;;; ============================================================================

(defn create-timing
  "Create timing context for a request.
   Call at the very start of request processing."
  []
  {::start-time (System/nanoTime)
   ::start-instant (Instant/now)
   ::phases []})

(defn record-phase
  "Record timing for a phase (parse, validate, execute).
   Returns updated timing context."
  [timing phase-name start-nanos]
  (when timing
    (let [duration (- (System/nanoTime) start-nanos)]
      (update timing ::phases conj
              {:phase phase-name
               :startOffset (- start-nanos (::start-time timing))
               :duration duration}))))

;;; ============================================================================
;;; Tracing Context
;;; ============================================================================

(defn start-phase
  "Mark the start of a phase. Returns start nanos for later recording."
  []
  (System/nanoTime))

(defn tracing-enabled?
  "Check if tracing is enabled in context."
  [context]
  (boolean (:com.walmartlabs.lacinia/enable-tracing? context)))

;;; ============================================================================
;;; Result Extension
;;; ============================================================================

(defn- find-phase
  "Find a phase by name in timing phases."
  [phases phase-name]
  (first (filter #(= phase-name (:phase %)) phases)))

(defn add-tracing-extension
  "Add Apollo tracing extension to result if tracing was enabled."
  [result timing]
  (if (and timing (seq (::phases timing)))
    (let [end-time (System/nanoTime)
          duration (- end-time (::start-time timing))
          phases (::phases timing)
          tracing {:version 1
                   :startTime (str (::start-instant timing))
                   :endTime (str (Instant/now))
                   :duration duration
                   :parsing (find-phase phases :parse)
                   :validation (find-phase phases :validate)
                   :execution (merge
                                (find-phase phases :execute)
                                {:resolvers []})}]  ;; Resolver tracing handled by Lacinia
      (assoc-in result [:extensions :tracing] tracing))
    result))

;;; ============================================================================
;;; Integration with Lacinia Tracing
;;; ============================================================================

(defn create-lacinia-timing-start
  "Create Lacinia-compatible timing start for parser integration."
  []
  (lacinia-tracing/create-timing-start))
