;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.client-address
  "Decides a request's client address and builds its log context."
  (:require
   [clojure.string :as str]
   [nano-id.core :refer [nano-id]]
   [synthigy.env :as env]
   [synthigy.log :as log])
  (:import
   [java.net Inet6Address InetAddress]))

(def ipv4-re
  #"(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}")

(defn parse-ip
  "InetAddress for an IPv4/IPv6 literal, nil for anything else; never resolves a name."
  ^InetAddress [s]
  (when (and (string? s)
             (or (re-matches ipv4-re s)
                 ;; a ':' makes getByName parse-or-throw, never look up
                 (and (str/includes? s ":") (re-matches #"[0-9A-Fa-f:.]+" s))))
    (try (InetAddress/getByName s) (catch Exception _ nil))))

(defn compress-v6
  [s]
  (let [groups (str/split s #":")
        runs   (->> (map-indexed vector groups)
                    (partition-by (fn [[_ g]] (= "0" g)))
                    (filter #(and (= "0" (second (first %))) (< 1 (count %)))))]
    (if (empty? runs)
      s
      (let [run   (apply max-key count (reverse runs))
            start (ffirst run)]
        (str (str/join ":" (subvec groups 0 start))
             "::"
             (str/join ":" (subvec groups (+ start (count run)))))))))

(defn ->text
  [^InetAddress a]
  (let [s (.getHostAddress a)]
    (if (instance? Inet6Address a) (compress-v6 s) s)))

(defn parse-cidr
  [s]
  (let [[addr bits] (str/split (str/trim s) #"/" 2)]
    (when-let [a (parse-ip addr)]
      (let [net (.getAddress a)
            n   (if bits (parse-long bits) (* 8 (alength net)))]
        (when (and n (<= 0 n (* 8 (alength net))))
          [net n])))))

(defn in-cidr?
  [^InetAddress a [^bytes net bits]]
  (let [b (.getAddress a)]
    (and (= (alength b) (alength net))
         (let [full (quot bits 8)
               mask (bit-and 0xff (bit-shift-left 0xff (- 8 (mod bits 8))))]
           (and (every? #(= (aget b %) (aget net %)) (range full))
                (or (zero? (mod bits 8))
                    (= (bit-and (aget b full) mask) (bit-and (aget net full) mask))))))))

(defn parse-trusted
  [s]
  (into []
        (keep (fn [entry]
                (or (parse-cidr entry)
                    (do (log/warn {:id   ::trusted-proxy-ignored
                                   :data {:action :rejected :subject :trust-proxy :entry entry}}
                                  "SYNTHIGY_SERVER_TRUST_PROXY entry is not an IP or CIDR; ignored")
                        nil))))
        (remove str/blank? (map str/trim (str/split (or s "") #",")))))

(def trusted-proxies
  (delay (parse-trusted env/trust-proxy)))

(defn trusted?
  [trusted ^InetAddress a]
  (boolean (some #(in-cidr? a %) trusted)))

(defn strip-port
  [s]
  (cond
    (str/starts-with? s "[")        (second (re-matches #"\[([^\]]+)\](?::\d+)?" s))
    (re-matches #"[^:]+:\d+" s)     (subs s 0 (str/index-of s ":"))
    :else                           s))

(defn forwarded-for
  [request]
  (some->> (get-in request [:headers "x-forwarded-for"])
           (#(str/split % #","))
           (map str/trim)
           (remove str/blank?)))

(defn client-address
  "Client IP of a Ring request: the socket peer, or, when the peer is a trusted
   proxy, the nearest X-Forwarded-For hop that is not one."
  ([request] (client-address request @trusted-proxies))
  ([request trusted]
   (let [peer (parse-ip (:remote-addr request))]
     (cond
       (nil? peer)                    (:remote-addr request)
       (not (trusted? trusted peer))  (->text peer)
       :else
       (loop [[hop & more] (reverse (forwarded-for request))
              nearest      peer]
         (let [a (some-> hop strip-port parse-ip)]
           (cond
             (nil? a)              (->text nearest)
             (trusted? trusted a)  (recur more a)
             :else                 (->text a))))))))

(def request-id-re #"[A-Za-z0-9._:-]{1,128}")

(defn truncate
  [s n]
  (when s (subs s 0 (min n (count s)))))

(defn request-context
  "Log context for one request: its id, client address and the client's own claims."
  ([request] (request-context request @trusted-proxies))
  ([request trusted]
   (let [peer       (parse-ip (:remote-addr request))
         from-proxy (and peer (trusted? trusted peer))
         ip         (client-address request trusted)
         inbound    (some-> (get-in request [:headers "x-request-id"]) str/trim
                            (#(when (re-matches request-id-re %) %)))
         ua         (get-in request [:headers "user-agent"])
         xff        (get-in request [:headers "x-forwarded-for"])]
     (cond-> {:request-id (if (and from-proxy inbound) inbound (nano-id 10))}
       ip                                      (assoc :ip ip)
       (and peer (not= (->text peer) ip))      (assoc :peer (->text peer))
       (and inbound (not from-proxy))          (assoc :client-request-id inbound)
       (not-empty ua)                          (assoc :user-agent (truncate ua 512))
       (not-empty xff)                         (assoc :forwarded-for (truncate xff 512))))))
