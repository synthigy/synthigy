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

(ns synthigy.files.storage
  "Blob storage contract and the spooling that measures every upload."
  (:require
   [babashka.fs :as fs]
   [synthigy.env :as env])
  (:import
   [java.io File FileOutputStream InputStream]
   [java.nio.file Files LinkOption Path]
   [java.nio.file.attribute PosixFilePermissions]
   [java.security MessageDigest]
   [java.util HexFormat]))

(defprotocol Storage
  (put-file! [this key file meta]
    "Store spooled `file` under `key`; `meta` carries :size, :sha256 and :content-type. Always consumes the file.")
  (open-blob [this key] [this key start end]
    "InputStream over the blob, or over bytes start..end inclusive; nil when the key is absent.")
  (head-blob [this key]
    "{:size n} for a stored key, nil when absent.")
  (copy-blob! [this from to]
    "Copy blob `from` to the fresh key `to` inside the same store.")
  (delete-blob! [this key]
    "Remove a blob; absent keys are not an error.")
  (list-blobs [this]
    "Every stored blob as {:key :modified Instant}, lazily."))

(defprotocol FileBacked
  (blob-file [this key]
    "The blob as a java.io.File the HTTP server can send without copying, nil when the key is absent."))

(def key-pattern #"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")

(defn check-key
  "Return `key` when it is a safe storage key, else throw."
  [key]
  (if (and (string? key) (re-matches key-pattern key))
    key
    (throw (ex-info "Invalid storage key" {:code :invalid-storage-key :key key}))))

(defn check-range
  [start end]
  (when-not (and (int? start) (int? end) (<= 0 start end))
    (throw (ex-info "Invalid byte range" {:code :range-not-satisfiable :start start :end end}))))

(defn limit-stream
  "InputStream yielding at most `n` bytes of `in`; closing it closes `in`."
  ^InputStream [^InputStream in n]
  (let [remaining (volatile! n)]
    (proxy [InputStream] []
      (read
        ([]
         (if (pos? @remaining)
           (let [b (.read in)]
             (when-not (neg? b) (vswap! remaining dec))
             b)
           -1))
        ([^bytes buf]
         (.read ^InputStream this buf 0 (alength buf)))
        ([^bytes buf off len]
         (cond
           (zero? len) 0
           (pos? @remaining) (let [n (.read in buf off (int (min len @remaining)))]
                               (when (pos? n) (vswap! remaining - n))
                               n)
           :else -1)))
      (close [] (.close in)))))

(defn private!
  "Owner-only permissions where the filesystem supports POSIX modes."
  [path mode]
  (let [p (fs/path path)]
    (when (Files/exists p (make-array LinkOption 0))
      (try
        (Files/setPosixFilePermissions ^Path p (PosixFilePermissions/fromString mode))
        (catch UnsupportedOperationException _ nil)))
    path))

(defn private-dirs!
  [dir]
  (fs/create-dirs dir)
  (private! dir "rwx------"))

(def spool-dir (str env/home "/files/.spool"))

(defn spool!
  "Copy `in` to a temp file while hashing; returns {:file :size :sha256}."
  [^InputStream in]
  (private-dirs! spool-dir)
  (let [f (File/createTempFile "up-" ".part" (File. ^String spool-dir))
        md (MessageDigest/getInstance "SHA-256")
        buf (byte-array 65536)]
    (try
      (private! (.getPath f) "rw-------")
      (with-open [out (FileOutputStream. f)]
        (loop [total 0]
          (let [n (.read in buf)]
            (if (neg? n)
              {:file f
               :size total
               :sha256 (.formatHex (HexFormat/of) (.digest md))}
              (do (.update md buf 0 n)
                  (.write out buf 0 n)
                  (recur (+ total n)))))))
      (catch Throwable t
        (.delete f)
        (throw t)))))

(defn discard!
  [{:keys [^File file]}]
  (when file (.delete file)))
