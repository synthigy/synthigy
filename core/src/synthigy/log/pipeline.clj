(ns synthigy.log.pipeline
  "Single signal-transform pipeline for `synthigy.log`.

  A pipeline is a sequence of stages composed into one fn:

      stage1 → stage2 → … → stageN

  Each stage is `(fn [signal]) -> signal-or-nil`. Returning nil drops the
  signal entirely — subsequent stages and all destination handlers are
  skipped for that signal.

  The composed pipeline is installed as Telemere's global `:xfn` via
  `t/set-xfn!`. This guarantees the pipeline runs exactly **once** per
  signal, regardless of how many destination handlers are registered.
  Telemere then dispatches the (post-pipeline) signal to each handler.

  Built-in stages live below. `synthigy.log/install!` composes them in
  the locked default order; advanced operators may build custom
  pipelines via `compose-stages`.

  ## Stage authoring contract

  - Stages SHOULD be pure when possible.
  - Stages MAY return nil to drop the signal (sampling, denylist, etc.).
  - Stages MUST NOT throw. A throwing stage breaks every signal afterwards;
    the failure is global. If a stage hits an unexpected condition, print
    to `*err*` and return the signal unchanged.
  - Stages SHOULD be cheap. The pipeline runs synchronously before
    handler dispatch — every microsecond is paid by every signal.

  ## Stages

  Installed in the default pipeline (by `synthigy.log/install!`, in order):
  `enrich-host` → `enrich-ctx` → `redact` → `serialize`. The structural win
  is moving serialization ahead of dispatch so it runs once per signal.

  `sample-stage` is a ⚗️ FUTURE-FEATURE EXPERIMENT: complete and tested but
  deliberately NOT in the default pipeline and with no config on-ramp yet —
  latent capability for when log volume/cost becomes a driver. See its
  docstring. Don't wire it up reflexively; the per-subsystem debug need is
  already met by the cockpit debug toggle (which RAISES verbosity via
  `:ns-overrides`, the opposite of sampling)."
  )

(defn compose-stages
  "Compose `stages` (a seq of `(fn [signal] -> signal-or-nil)`) into a
   single pipeline function suitable for `taoensso.telemere/set-xfn!`.

   - nil entries in `stages` are dropped
   - an empty pipeline is `identity`
   - a single-stage pipeline returns that stage directly (no wrapper)
   - any stage returning nil short-circuits the rest of the pipeline"
  [stages]
  (let [non-empty (vec (remove nil? stages))]
    (case (count non-empty)
      0 identity
      1 (first non-empty)
      (fn pipeline-fn [signal]
        (reduce (fn [sig stage]
                  (if-let [next-sig (stage sig)]
                    next-sig
                    (reduced nil)))
                signal
                non-empty)))))

;;; ============================================================================
;;; Built-in stages
;;; ============================================================================

(def line-key
  "Signal key under which the serialized JSON wire String is cached.
   Destination handlers should read this and fall back to direct
   serialization only if missing (e.g. when no pipeline is installed)."
  :_synthigy/line)

(def redacted-marker
  "String substituted for redacted values in :data and :ctx."
  "<redacted>")

(defn- walk-redact
  "Walk `x`, replacing values for any key in `keys-to-redact` with
   `redacted-marker`. Recurses into nested maps and into seqs (vectors,
   lists, lazy-seqs). Records are treated as maps. Leaves anything else
   untouched.

   Notes:
   - Redaction is *by key name only*. String contents are never inspected
     (no regex for JWTs, etc.) — that's a separate concern.
   - When a key matches, its value is replaced wholesale — the recursion
     does NOT descend into a redacted value. This matters when a
     credential is itself a structured object: the whole thing is replaced
     by `<redacted>`, not partially walked."
  [keys-to-redact x]
  (cond
    (record? x)
    (reduce-kv (fn [m k v]
                 (assoc m k (if (contains? keys-to-redact k)
                              redacted-marker
                              (walk-redact keys-to-redact v))))
               x x)

    (map? x)
    (reduce-kv (fn [m k v]
                 (assoc m k (if (contains? keys-to-redact k)
                              redacted-marker
                              (walk-redact keys-to-redact v))))
               (empty x) x)

    (sequential? x)
    (mapv #(walk-redact keys-to-redact %) x)

    :else x))

(def default-promoted-ctx-keys
  "Context keys that the default pipeline promotes from `:ctx` to top-level
   signal fields. These align with the wire schema's named columns."
  [:request-id :user-xid :tenant])

(defn enrich-host-stage
  "Pipeline stage: normalize the signal's `:host` to a String, falling
   back to `host-fn` (0-arg fn returning host String, typically a deref
   of a hostname delay) when no usable host is present.

   Three input shapes:
   1. `:host` is a String          → preserve as-is (call sites that set
                                      their own host stay in control)
   2. `:host` is a Telemere host    → extract `:name` (Telemere's signal
      map {:name s :ip s}             constructor auto-populates this map
                                      shape; the wire schema wants a String)
   3. `:host` is missing or nil    → call `host-fn`

   After this stage, downstream stages and the serializer see a String
   (or nil if `host-fn` fails). The wire schema's `host` column is a
   String — never a map."
  [host-fn]
  (fn enrich-host [signal]
    (let [existing (:host signal)]
      (cond
        (string? existing)
        signal

        (and (map? existing) (string? (:name existing)))
        (assoc signal :host (:name existing))

        :else
        (assoc signal :host (try (host-fn) (catch Throwable _ nil)))))))

(defn enrich-ctx-stage
  "Pipeline stage: promote keys from `:ctx` to top-level signal fields.
   Default keys: `:request-id`, `:user-xid`, `:tenant`.

   Behaviour:
   - Each key is moved from `:ctx` to top-level only if the signal does
     NOT already carry it at top level (top-level wins; call sites that
     set their own field stay in control).
   - Once promoted, the key is removed from `:ctx` to avoid double-emit
     when the serializer renders `:ctx` as a JSON object.
   - Signals with no `:ctx` pass through unchanged.

   This stage exists so that the serializer can be a dumb projection of
   the signal's top-level fields — no more `(get ctx-map :request-id)`
   embedded in the serialization path."
  ([] (enrich-ctx-stage default-promoted-ctx-keys))
  ([keys-to-promote]
   (let [keys-vec (vec keys-to-promote)
         keys-set (set keys-to-promote)]
     (fn enrich-ctx [signal]
       (if-let [ctx (:ctx signal)]
         (let [promoted-signal
               (reduce (fn [sig k]
                         (if (or (contains? sig k)
                                 (not (contains? ctx k)))
                           sig
                           (assoc sig k (get ctx k))))
                       signal
                       keys-vec)
               residual-ctx (reduce dissoc ctx keys-set)]
           (assoc promoted-signal :ctx residual-ctx))
         signal)))))

(defn sample-stage
  "⚗️ FUTURE-FEATURE EXPERIMENT — latent, intentionally unwired. ⚗️

   This stage is complete and tested but is deliberately NOT installed in
   the default pipeline and has no config on-ramp yet. It is kept as latent
   capability for the day log VOLUME/COST becomes a problem (drop a fraction
   of high-volume, low-value signals before they reach any handler).

   Do NOT mistake its absence from `synthigy.log/install!` for a bug or a
   TODO to wire up reflexively. The per-subsystem *debug* need (turn a
   lens's namespaces UP to TRACE) is already met by the cockpit debug toggle
   → `:ns-overrides` → `set-min-level!` — that RAISES verbosity. Sampling is
   the OPPOSITE direction (volume reduction). Only wire this when there's a
   real noise/cost driver; the natural home is a `SYNTHIGY_LOG_SAMPLE` spec
   read in `synthigy.log.config` and spliced ahead of the enrich stages.

   ---

   Pipeline stage: probabilistically drop signals for which `pred` returns
   truthy. `pred` is `(fn [signal]) -> bool`. `rate` is in [0.0, 1.0] —
   the probability of *keeping* a matched signal.

       rate 1.0 → keep every matched signal (no-op)
       rate 0.5 → keep ~50% of matched signals
       rate 0.0 → drop every matched signal

   Signals where `pred` returns falsy pass through unchanged. Compose
   multiple `sample-stage` instances for multiple sampling rules.

   This stage is NOT installed in the default pipeline — sampling is
   opt-in. Custom pipelines splice it in via `compose-stages`.

   Example: keep 1% of Hikari signals at INFO or below; keep 10% of
   internal SQL trace; pass everything else through.

       (compose-stages
         [(sample-stage
            (fn [{:keys [ns level]}]
              (and (clojure.string/starts-with? (str ns) \"com.zaxxer.hikari\")
                   (#{:trace :debug :info} level)))
            0.01)
          (sample-stage
            (fn [{:keys [ns level]}]
              (and (clojure.string/starts-with? (str ns) \"synthigy.dataset.sql\")
                   (= :trace level)))
            0.10)
          (enrich-host-stage host-fn)
          ...])

   Note on ordering: cheap dropping stages SHOULD run before expensive
   stages (enrich, redact). Dropping a signal in `sample-stage` skips
   all subsequent work — that's the whole point.

   Returns nil to drop; never throws (exceptions in `pred` are caught
   and the signal is preserved — fail-safe over fail-secure for
   sampling)."
  [pred rate]
  (let [keep-rate (double rate)]
    (fn sample [signal]
      (let [matched? (try (pred signal) (catch Throwable _ false))]
        (if (and matched? (> (rand) keep-rate))
          nil
          signal)))))

(defn redact-stage
  "Pipeline stage: replace values for any key in `keys-to-redact` with
   `redacted-marker` (the literal String '<redacted>') anywhere they
   appear in the signal's `:data` or `:ctx` — top-level OR nested in
   maps/seqs.

   This is the always-on security floor. Callsites SHOULD still avoid
   dumping raw credential-bearing structures — see
   `feedback_log_callsite_discipline` — but the pipeline now catches
   accidental leaks instead of relying on per-callsite vigilance.

   `keys-to-redact` is a set of keywords. Matching is by `contains?`;
   pass kebab-case keywords (`:client-secret`), not snake_case strings
   (the snake_case conversion happens at JSON serialization).

   The stage never throws. If `walk-redact` fails, the signal passes
   through unchanged with an error line to *err* — surfacing the bug
   without breaking the log stream."
  [keys-to-redact]
  (let [ks (set keys-to-redact)]
    (fn redact [signal]
      (try
        (cond-> signal
          (:data signal) (update :data #(walk-redact ks %))
          (:ctx signal)  (update :ctx  #(walk-redact ks %)))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.log.pipeline/redact-stage threw: "
                          (some-> t .getClass .getName) ": "
                          (.getMessage t))))
          signal)))))

(defn enrich-topics-stage
  "Pipeline stage: assoc `:topics` — the set of topic keywords this signal
   classifies to — onto the signal, computed by `classify-fn` (a
   `(signal → #{topics})`, typically `synthigy.log.topics/classify`). Passed
   in rather than required directly so this namespace stays dependency-free.

   Topics are an axis ORTHOGONAL to level: they describe what a signal is
   ABOUT (its audience/lens), not how severe it is. Downstream the serializer
   emits them as a wire array and stores filter them with the `:has` op.

   Never throws — on failure the signal passes through with no `:topics`."
  [classify-fn]
  (fn enrich-topics [signal]
    (try
      (assoc signal :topics (classify-fn signal))
      (catch Throwable t
        (binding [*out* *err*]
          (println (str "synthigy.log.pipeline/enrich-topics-stage threw: "
                        (some-> t .getClass .getName) ": " (.getMessage t))))
        signal))))

(defn serialize-stage
  "Stage that runs `serializer` over the signal and stashes the resulting
   String at `line-key`. `serializer` is typically
   `synthigy.log/signal->json-line` — passed in by `synthigy.log/install!`
   to avoid a circular namespace dependency.

   If `serializer` throws, the failure is printed to `*err*` and the
   signal passes through unchanged (with no cached line). Destination
   handlers will fall back to direct serialization in that case."
  [serializer]
  (fn serialize [signal]
    (try
      (assoc signal line-key (serializer signal))
      (catch Throwable t
        (binding [*out* *err*]
          (println (str "synthigy.log.pipeline/serialize-stage threw: "
                        (some-> t .getClass .getName) ": "
                        (.getMessage t))))
        signal))))
