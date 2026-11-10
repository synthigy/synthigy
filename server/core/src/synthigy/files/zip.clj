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

(ns synthigy.files.zip
  "Zip many stored files into one streamed download."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [synthigy.files.storage :as storage]
   [synthigy.log :as log])
  (:import
   [java.io InputStream PipedInputStream PipedOutputStream]
   [java.nio.charset StandardCharsets]
   [java.util.zip Deflater ZipEntry ZipOutputStream]))

(defn env-long [k default]
  (or (some-> (env k) str/trim not-empty parse-long) default))

(defn max-bytes [] (env-long :synthigy-files-zip-max-bytes (* 2 1024 1024 1024)))

(defn max-entries [] (env-long :synthigy-files-zip-max-entries 10000))

(defn stored?
  "Whether bytes of this type go in without deflate: already compressed, or opaque binary that rarely shrinks."
  [content-type]
  (let [t (some-> content-type str/lower-case)]
    (boolean
     (and t
          (or (= t "application/octet-stream")
              (and (str/starts-with? t "image/") (not (str/includes? t "svg")))
              (str/starts-with? t "video/")
              (str/starts-with? t "audio/")
              (re-find #"zip|gzip|x-7z|x-rar|x-bzip|x-xz|zstd|pdf|openxmlformats|opendocument|epub" t))))))

(defn numbered [entry n]
  (let [cut (str/last-index-of entry "/")
        dir (if cut (subs entry 0 (inc cut)) "")
        base (if cut (subs entry (inc cut)) entry)
        dot (str/last-index-of base ".")
        [stem ext] (if (and dot (pos? dot)) [(subs base 0 dot) (subs base dot)] [base ""])]
    (str dir stem " (" n ")" ext)))

(defn unique-names
  "Give every item a distinct :entry, suffixing repeats like uploads do: name (2).ext."
  [items]
  (first
   (reduce (fn [[out taken] {:keys [entry] :as item}]
             (let [lower #(str/lower-case %)
                   free (if (taken (lower entry))
                          (some #(when-not (taken (lower %)) %) (map #(numbered entry %) (iterate inc 2)))
                          entry)]
               [(conj out (assoc item :entry free)) (conj taken (lower free))]))
           [[] #{}]
           items)))

(defn skipped-text [skipped]
  (str (str/join "\n" (map (fn [{:keys [ref reason]}] (str ref "\t" reason)) skipped)) "\n"))

(defn write!
  "Write `items` ({:entry :key :content-type}) from `store` as a zip to `out`; returns the skips added on the way."
  [store items skipped ^java.io.OutputStream out]
  (with-open [zip (ZipOutputStream. out)]
    (let [late (volatile! [])]
      (doseq [{:keys [entry key content-type ref]} items]
        (if-let [^InputStream in (storage/open-blob store key)]
          (with-open [in in]
            (.setLevel zip (if (stored? content-type) Deflater/NO_COMPRESSION Deflater/DEFAULT_COMPRESSION))
            (.putNextEntry zip (ZipEntry. ^String entry))
            (.transferTo in zip)
            (.closeEntry zip))
          (vswap! late conj {:ref ref :reason "bytes no longer stored"})))
      (let [all (into (vec skipped) @late)]
        (when (seq all)
          (.setLevel zip Deflater/DEFAULT_COMPRESSION)
          (.putNextEntry zip (ZipEntry. "_skipped.txt"))
          (.write zip (.getBytes ^String (skipped-text all) StandardCharsets/UTF_8))
          (.closeEntry zip))
        @late))))

(defn stream
  "An InputStream of the zip, written on its own virtual thread as the server reads it."
  ^InputStream [store items skipped]
  (let [in (PipedInputStream. (* 1024 1024))
        out (PipedOutputStream. in)]
    (Thread/startVirtualThread
     (fn []
       (try
         (write! store items skipped out)
         (catch Throwable t
           (log/warn {:id ::stream-failed :data {:action :failed :subject :zip :entries (count items)}}
                     (str "Zip download stopped: " (ex-message t)))
           (try (.close out) (catch Throwable _ nil))))))
    in))
