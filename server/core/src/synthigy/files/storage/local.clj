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

(ns synthigy.files.storage.local
  "Blob store on local disk under <home>/files/."
  (:require
   [babashka.fs :as fs]
   [clojure.string :as str]
   [synthigy.env :as env]
   [synthigy.files.storage :as storage])
  (:import
   [java.io File]
   [java.nio.channels Channels FileChannel]
   [java.nio.file AtomicMoveNotSupportedException CopyOption DirectoryNotEmptyException Files NoSuchFileException
    OpenOption Path StandardCopyOption StandardOpenOption]))

(def root (str env/home "/files"))

(defn blob-path
  ^Path [root key]
  (fs/path root (storage/check-key key)))

(defn spool-name [key suffix]
  (str (str/replace key "/" "_") suffix))

(def atomic (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))

(defn move-into-place!
  [^Path source ^Path target root key]
  (try
    (Files/move source target ^"[Ljava.nio.file.CopyOption;" atomic)
    (catch AtomicMoveNotSupportedException _
      (let [staged (fs/path root ".spool" (spool-name key ".part"))]
        (storage/private-dirs! (fs/parent staged))
        (try
          (Files/move source staged (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))
          (Files/move staged target ^"[Ljava.nio.file.CopyOption;" atomic)
          (finally (fs/delete-if-exists staged)))))))

(defrecord LocalStorage [root]
  storage/Storage
  (put-file! [_ key file _]
    (let [source (.toPath ^File file)]
      (try
        (let [target (blob-path root key)]
          (try
            (storage/private-dirs! (fs/parent target))
            (move-into-place! source target root key)
            (catch NoSuchFileException _
              (storage/private-dirs! (fs/parent target))
              (move-into-place! source target root key))))
        (finally (fs/delete-if-exists source)))
      nil))
  (open-blob [_ key]
    (try
      (Files/newInputStream (blob-path root key) (make-array OpenOption 0))
      (catch NoSuchFileException _ nil)))
  (open-blob [_ key start end]
    (storage/check-range start end)
    (let [p (blob-path root key)
          ch (try (FileChannel/open p (into-array OpenOption [StandardOpenOption/READ]))
                  (catch NoSuchFileException _ nil))]
      (when ch
        (try
          (storage/limit-stream (Channels/newInputStream (.position ch (long start)))
                                (inc (- end start)))
          (catch Throwable t
            (.close ch)
            (throw t))))))
  (head-blob [_ key]
    (try
      {:size (Files/size (blob-path root key))}
      (catch NoSuchFileException _ nil)))
  (copy-blob! [_ from to]
    (let [source (blob-path root from)
          target (blob-path root to)
          tmp (fs/path root ".spool" (spool-name to ".copy"))]
      (storage/private-dirs! (fs/parent tmp))
      (storage/private-dirs! (fs/parent target))
      (try
        (Files/copy source tmp (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))
        (Files/move tmp target ^"[Ljava.nio.file.CopyOption;" atomic)
        (finally (fs/delete-if-exists tmp)))
      nil))
  (delete-blob! [_ key]
    (let [p (blob-path root key)
          dir (fs/parent p)]
      (fs/delete-if-exists p)
      (when (not= dir (fs/path root))
        (try (fs/delete-if-exists dir) (catch DirectoryNotEmptyException _))))
    nil)
  (list-blobs [_]
    (letfn [(blob [key p] {:key key :modified (.toInstant (fs/last-modified-time p))})]
      (for [entry (fs/list-dir root)
            :let [n (str (fs/file-name entry))]
            :when (not (str/starts-with? n "."))
            b (cond
                (fs/regular-file? entry) [(blob n entry)]
                (fs/directory? entry) (for [v (fs/list-dir entry) :when (fs/regular-file? v)]
                                        (blob (str n "/" (fs/file-name v)) v)))]
        b)))
  storage/FileBacked
  (blob-file [_ key]
    (let [p (blob-path root key)]
      (when (fs/regular-file? p) (.toFile p)))))

(defn create
  ([] (create root))
  ([root]
   (storage/private-dirs! root)
   (->LocalStorage root)))
