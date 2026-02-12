(ns synthigy.iam.patch
  "IAM component version patches and leveling.

  Manages version patches for the core IAM component (separate from IAM model).
  The IAM component handles authentication, authorization infrastructure.

  To level this component:
    (require '[patcho.patch :as patch])
    (patch/level! :synthigy/iam)"
  (:require
    [clojure.tools.logging :as log]
    [patcho.patch :as patch]
    [synthigy.db :refer [*db*]]))

;; ============================================================================
;; Component Version Registration
;; ============================================================================

(patch/current-version :synthigy/iam "1.0.0")
;; ============================================================================
;; Version Patches
;; ============================================================================

(patch/upgrade :synthigy/iam "1.0.0" (log/info "[IAM] Component initialized at v1.0.0"))
