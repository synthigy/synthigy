(ns synthigy.iam.events
  "Event publishing infrastructure for IAM components.

  Provides async pub/sub mechanism for IAM events like keypair rotation,
  user changes, etc. Used by oauth.store for database persistence."
  (:require
    [clojure.core.async :as async]))

;; =============================================================================
;; Event Publishing Infrastructure
;; =============================================================================

(defonce ^{:doc "Main subscription channel for all IAM events.

  Events flow through this channel to the publisher for topic-based routing."}
  subscription

  (async/chan (async/sliding-buffer 10000)))

(defonce ^{:doc "Topic-based publisher for IAM events.

  Routes events to subscribers based on :topic key.
  Default topic is ::broadcast if not specified."}
  publisher

  (async/pub
    subscription
    (fn [{:keys [topic]
          :or {topic ::broadcast}}]
      topic)))

(defn publish
  "Publish an event to the IAM event system.

  Args:
    topic - Keyword identifying the event type (e.g., :keypair/added, :user/updated)
    data - Event data map (will be assoc'd with :topic)

  Returns: nil

  The event is routed to all subscribers listening to this topic.
  Used primarily by oauth.store for persisting changes to database.

  Common topics:
  - :keypair/added - New encryption keypair added
  - :keypair/removed - Encryption keypairs evicted (max 3 limit)
  - :user/updated - User data modified
  - :client/registered - New OAuth client registered

  Example:
    (publish :keypair/added {:key-pair {:kid \"abc\" :public ...}})"
  [topic data]
  (async/put! subscription (assoc data :topic topic)))
