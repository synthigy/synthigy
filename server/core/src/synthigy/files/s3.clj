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

(ns synthigy.files.s3
  "S3-compatible object store client: SigV4 over java.net.http, five calls."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [synthigy.files.storage :as storage]
   [synthigy.json :as json])
  (:import
   [java.io InputStream]
   [java.net URI URLEncoder]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers
    HttpResponse HttpResponse$BodyHandlers]
   [java.nio.charset StandardCharsets]
   [java.nio.file Path]
   [java.security MessageDigest]
   [java.time Duration Instant ZoneOffset]
   [java.time.format DateTimeFormatter]
   [java.util HexFormat]
   [javax.crypto Mac]
   [javax.crypto.spec SecretKeySpec]))

(def empty-sha256 "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

(def client
  (delay (-> (HttpClient/newBuilder)
             (.version HttpClient$Version/HTTP_1_1)
             (.connectTimeout (Duration/ofSeconds 10))
             (.build))))

(defn hex [^bytes b] (.formatHex (HexFormat/of) b))

(defn sha256-hex [^String s]
  (hex (.digest (MessageDigest/getInstance "SHA-256") (.getBytes s StandardCharsets/UTF_8))))

(defn hmac ^bytes [^bytes k ^String data]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. k "HmacSHA256"))
    (.doFinal mac (.getBytes data StandardCharsets/UTF_8))))

(defn uri-encode
  "RFC 3986 encoding as SigV4 wants it; keeps `/` when `path?`."
  [^String s path?]
  (let [e (-> (URLEncoder/encode s StandardCharsets/UTF_8)
              (str/replace "+" "%20")
              (str/replace "*" "%2A")
              (str/replace "%7E" "~"))]
    (if path? (str/replace e "%2F" "/") e)))

(def amz-date-fmt (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'") ZoneOffset/UTC))

(defn host-header
  "Host as java.net.http sends it: default ports are left out."
  [^URI uri]
  (let [port (.getPort uri)
        default (case (.getScheme uri) "https" 443 "http" 80 nil)]
    (cond-> (.getHost uri)
      (and (not= -1 port) (not= default port)) (str ":" port))))

(defn sign
  "SigV4 headers for one request; `headers` are lowercase names already on the request."
  [{:keys [region]} {:keys [access-key secret-key token]} method ^URI uri headers payload-hash now]
  (let [amz-date (.format amz-date-fmt now)
        date (subs amz-date 0 8)
        headers (cond-> (assoc headers
                               "host" (host-header uri)
                               "x-amz-date" amz-date
                               "x-amz-content-sha256" payload-hash)
                  token (assoc "x-amz-security-token" token))
        names (sort (keys headers))
        canonical-headers (apply str (map #(str % ":" (str/replace (str/trim (str (get headers %))) #"\s+" " ") "\n") names))
        signed (str/join ";" names)
        canonical (str/join "\n" [method
                                  (or (not-empty (.getRawPath uri)) "/")
                                  (or (.getRawQuery uri) "")
                                  canonical-headers
                                  signed
                                  payload-hash])
        scope (str date "/" region "/s3/aws4_request")
        to-sign (str/join "\n" ["AWS4-HMAC-SHA256" amz-date scope (sha256-hex canonical)])
        k (-> (.getBytes (str "AWS4" secret-key) StandardCharsets/UTF_8)
              (hmac date) (hmac region) (hmac "s3") (hmac "aws4_request"))]
    (-> (dissoc headers "host")
        (assoc "authorization"
               (str "AWS4-HMAC-SHA256 Credential=" access-key "/" scope
                    ", SignedHeaders=" signed
                    ", Signature=" (hex (hmac k to-sign)))))))

(defn endpoint-uri
  "The endpoint as a URI with scheme and host, else throw :storage-unavailable."
  ^URI [{:keys [endpoint region]}]
  (let [u (try (URI. (or endpoint (str "https://s3." region ".amazonaws.com")))
               (catch Exception _ nil))]
    (when-not (and u (#{"http" "https"} (.getScheme u)) (.getHost u))
      (throw (ex-info (str "SYNTHIGY_FILES_S3_ENDPOINT must be an http(s) URL, got " endpoint)
                      {:code :storage-unavailable})))
    u))

(defn object-uri
  ^URI [{:keys [bucket path-style?] :as cfg} key]
  (let [base (endpoint-uri cfg)
        prefix (str/replace (or (.getRawPath base) "") #"/+$" "")
        k (when key (str "/" (uri-encode key true)))
        authority (cond-> (.getHost base)
                    (not= -1 (.getPort base)) (str ":" (.getPort base)))]
    (if (or path-style? (str/includes? bucket "."))
      (URI. (str (.getScheme base) "://" authority prefix "/" (uri-encode bucket false) k))
      (URI. (str (.getScheme base) "://" bucket "." authority prefix (or k "/"))))))

(defn error-info [status ^String body]
  (let [field #(second (re-find (re-pattern (str "<" % ">([^<]*)</" % ">")) (or body "")))]
    {:code :storage-error :status status :s3-code (field "Code") :message (field "Message")}))

(defn fail! [what status body]
  (let [info (error-info status body)]
    (throw (ex-info (str "S3 " what " failed: " status " " (or (:s3-code info) "") " " (or (:message info) ""))
                    info))))

(defn send!
  "Signed request; returns the java HttpResponse."
  ^HttpResponse [{:keys [credentials] :as cfg} method key {:keys [headers publisher payload-hash handler timeout]}]
  (let [uri (object-uri cfg key)
        signed (sign cfg (credentials) method uri (or headers {})
                     (or payload-hash empty-sha256) (Instant/now))
        b (reduce-kv (fn [^java.net.http.HttpRequest$Builder b k v] (.header b k v))
                     (HttpRequest/newBuilder uri)
                     signed)
        b (cond-> (.method b method (or publisher (HttpRequest$BodyPublishers/noBody)))
            timeout (.timeout (Duration/ofSeconds timeout)))]
    (.send ^HttpClient @client (.build b) (or handler (HttpResponse$BodyHandlers/ofString)))))

(defn body-string
  "At most 64 KB of an error body."
  [^HttpResponse r]
  (let [b (.body r)]
    (if (instance? InputStream b)
      (with-open [^InputStream in b]
        (String. (.readNBytes in 65536) StandardCharsets/UTF_8))
      b)))

(defn put-object!
  [cfg key ^Path path {:keys [sha256 content-type]}]
  (let [r (send! cfg "PUT" key {:headers (cond-> {} content-type (assoc "content-type" content-type))
                                :publisher (HttpRequest$BodyPublishers/ofFile path)
                                :payload-hash sha256
                                :timeout 600})]
    (when-not (<= 200 (.statusCode r) 299)
      (fail! "PUT" (.statusCode r) (.body r)))
    nil))

(defn get-object
  "InputStream over the object (or bytes start..end inclusive), nil on 404."
  [cfg key [start end :as range]]
  (let [r (send! cfg "GET" key {:headers (cond-> {} range (assoc "range" (str "bytes=" start "-" end)))
                                :handler (HttpResponse$BodyHandlers/ofInputStream)
                                :timeout 60})
        status (.statusCode r)
        ^InputStream body (.body r)]
    (cond
      (and range (= 206 status)) (storage/limit-stream body (inc (- end start)))
      (and range (= 200 status)) (do (.skipNBytes body start)
                                     (storage/limit-stream body (inc (- end start))))
      (= 200 status) body
      (= 404 status) (do (.close body) nil)
      (= 416 status) (do (.close body)
                         (throw (ex-info "Range not satisfiable" {:code :range-not-satisfiable :start start :end end})))
      :else (fail! "GET" status (body-string r)))))

(defn head-object [cfg key]
  (let [r (send! cfg "HEAD" key {:timeout 30})
        status (.statusCode r)]
    (cond
      (<= 200 status 299) {:size (some-> (.firstValue (.headers r) "content-length") (.orElse nil) parse-long)}
      (= 404 status) nil
      :else (fail! "HEAD" status nil))))

(defn copy-object! [{:keys [bucket] :as cfg} from to]
  (let [r (send! cfg "PUT" to {:headers {"x-amz-copy-source" (str "/" (uri-encode bucket false) "/" (uri-encode from true))}
                               :timeout 300})
        body (.body r)]
    (when (or (not (<= 200 (.statusCode r) 299)) (str/includes? (or body "") "<Error>"))
      (fail! "COPY" (.statusCode r) body))
    nil))

(defn delete-object! [cfg key]
  (let [r (send! cfg "DELETE" key {:timeout 30})]
    (when-not (or (<= 200 (.statusCode r) 299) (= 404 (.statusCode r)))
      (fail! "DELETE" (.statusCode r) (.body r)))
    nil))

(defn list-query
  "ListObjectsV2 query, parameters in the sorted order SigV4 signs."
  [token]
  (str (when token (str "continuation-token=" (uri-encode token false) "&")) "list-type=2&max-keys=1000"))

(defn list-page
  "One ListObjectsV2 page: {:blobs [{:key :modified}] :next token-or-nil}."
  [cfg token]
  (let [q (list-query token)
        uri (URI. (str (object-uri cfg nil) (if (str/ends-with? (str (object-uri cfg nil)) "/") "" "/") "?" q))
        signed (sign cfg ((:credentials cfg)) "GET" uri {} empty-sha256 (Instant/now))
        b (reduce-kv (fn [^java.net.http.HttpRequest$Builder b k v] (.header b k v))
                     (-> (HttpRequest/newBuilder uri) (.timeout (Duration/ofSeconds 60)))
                     signed)
        r (.send ^HttpClient @client (.build (.GET b)) (HttpResponse$BodyHandlers/ofString))
        body (.body r)]
    (when-not (<= 200 (.statusCode r) 299) (fail! "LIST" (.statusCode r) body))
    {:blobs (vec (for [[_ c] (re-seq #"<Contents>(.*?)</Contents>" body)
                       :let [k (second (re-find #"<Key>([^<]*)</Key>" c))
                             m (second (re-find #"<LastModified>([^<]*)</LastModified>" c))]]
                   {:key k :modified (some-> m Instant/parse)}))
     :next (when (str/includes? body "<IsTruncated>true</IsTruncated>")
             (second (re-find #"<NextContinuationToken>([^<]*)</NextContinuationToken>" body)))}))

(defn list-objects
  "Every object in the bucket as {:key :modified}, lazily, page by page."
  [cfg]
  ((fn step [token]
     (lazy-seq
      (let [{:keys [blobs next]} (list-page cfg token)]
        (concat blobs (when next (step next))))))
   nil))

(def imds "http://169.254.169.254")

(defn imds-credentials
  "Instance-role credentials from EC2 IMDSv2, or nil off EC2."
  []
  (try
    (let [c (-> (HttpClient/newBuilder) (.connectTimeout (Duration/ofSeconds 2)) (.build))
          req (fn [method path headers]
                (let [b (reduce-kv (fn [^java.net.http.HttpRequest$Builder b k v] (.header b k v))
                                   (-> (HttpRequest/newBuilder (URI. (str imds path)))
                                       (.timeout (Duration/ofSeconds 2)))
                                   headers)]
                  (.body (.send c (.build (.method b method (HttpRequest$BodyPublishers/noBody)))
                                (HttpResponse$BodyHandlers/ofString)))))
          token (req "PUT" "/latest/api/token" {"X-aws-ec2-metadata-token-ttl-seconds" "21600"})
          h {"X-aws-ec2-metadata-token" token}
          role (first (str/split-lines (req "GET" "/latest/meta-data/iam/security-credentials/" h)))
          m (json/read-str (req "GET" (str "/latest/meta-data/iam/security-credentials/" role) h))]
      {:access-key (:AccessKeyId m)
       :secret-key (:SecretAccessKey m)
       :token (:Token m)
       :expires (some-> (:Expiration m) Instant/parse)})
    (catch Exception _ nil)))

(defn credentials-fn
  "Static keys when given, else AWS_* env, else a refreshing EC2 instance role."
  [{:keys [access-key secret-key]}]
  (cond
    (and access-key secret-key)
    (constantly {:access-key access-key :secret-key secret-key})

    (and (env :aws-access-key-id) (env :aws-secret-access-key))
    (constantly {:access-key (env :aws-access-key-id)
                 :secret-key (env :aws-secret-access-key)
                 :token (not-empty (env :aws-session-token))})

    :else
    (let [cache (atom nil)
          failed-at (atom nil)
          fresh? (fn [c secs] (and c (:expires c) (.isAfter ^Instant (:expires c) (.plusSeconds (Instant/now) secs))))]
      (fn []
        (if (fresh? @cache 300)
          @cache
          (locking cache
            (let [c @cache]
              (if (fresh? c 300)
                c
                (if-let [n (when-not (some-> @failed-at (.isAfter (.minusSeconds (Instant/now) 30)))
                             (or (imds-credentials) (do (reset! failed-at (Instant/now)) nil)))]
                  (do (reset! failed-at nil) (reset! cache n))
                  (if (fresh? c 0)
                    c
                    (throw (ex-info "No S3 credentials: set SYNTHIGY_FILES_S3_ACCESS_KEY_ID/SECRET_ACCESS_KEY or run with an instance role"
                                    {:code :storage-unavailable}))))))))))))
