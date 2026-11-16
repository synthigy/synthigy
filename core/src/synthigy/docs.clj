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

(ns synthigy.docs
  "The public manual (docs/*.md) as data: pages by topic, sections by heading slug, and which error rules it documents."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]))

(defn topic->file [topic]
  (if (= "brief" topic) "AGENT_BRIEF.md" (str (str/upper-case topic) ".md")))

(defn file->topic [file]
  (let [base (str/replace file #"\.md$" "")]
    (if (= "AGENT_BRIEF" base) "brief" (str/lower-case base))))

(defn checkout-docs
  "docs/ of the source checkout this namespace was loaded from; nil when it comes from a jar."
  []
  (when-let [^java.net.URL u (io/resource "synthigy/docs.clj")]
    (when (= "file" (.getProtocol u))
      (-> (io/file (.toURI u)) .getParentFile .getParentFile .getParentFile .getParentFile (io/file "docs")))))

(defn source
  "The markdown of `file`, from the jar's synthigy/docs/ or the checkout's docs/."
  [file]
  (when (re-matches #"[A-Z_]+\.md" file)
    (or (some-> (io/resource (str "synthigy/docs/" file)) slurp)
        (when-let [dir (checkout-docs)]
          (let [f (io/file dir file)] (when (.isFile f) (slurp f)))))))

(defn topics
  "Topic ids: every page the README links, and the brief."
  []
  (->> (re-seq #"\]\(([A-Z_]+)\.md" (or (source "README.md") ""))
       (map #(file->topic (str (second %) ".md")))
       (concat (when (source "AGENT_BRIEF.md") ["brief"]))
       distinct sort vec))

(defn slug
  "A heading's anchor as GitHub and Studio make it."
  [heading]
  (-> (str/trim heading)
      (.toLowerCase java.util.Locale/ROOT)
      (str/replace #"[^\p{IsAlphabetic}\p{N} _-]" "")
      (str/replace " " "-")))

(defn headings
  "`[level slug index]` of each heading outside code fences."
  [lines]
  (first
   (reduce (fn [[out fence?] [i l]]
             (cond
               (str/starts-with? (str/triml l) "```") [out (not fence?)]
               fence? [out true]
               :else (if-let [[_ hashes text] (re-matches #"(#{1,6}) (.*)" l)]
                       [(conj out [(count hashes) (slug text) i]) false]
                       [out false])))
           [[] false]
           (map-indexed vector lines))))

(defn section-ids [md]
  (into [] (comp (filter #(>= (first %) 2)) (map second)) (headings (str/split-lines md))))

(defn section
  "The section of `md` whose heading slug is `id`, with its subsections, or nil."
  [md id]
  (let [lines (vec (str/split-lines md))
        hs (headings lines)]
    (when-let [[level _ start] (first (filter (fn [[l s]] (and (>= l 2) (= id s))) hs))]
      (let [end (or (some (fn [[l _ i]] (when (and (> i start) (<= l level)) i)) hs) (count lines))]
        (str/trimr (str/join "\n" (subvec lines start end)))))))

(defn rewrite-links
  "Links between manual pages as /docs paths, so a reader can follow them on the engine."
  [md topic]
  (-> md
      (str/replace #"\]\(README\.md\)" "](/docs)")
      (str/replace #"\]\(([A-Z_]+)\.md(?:#([^)\s]+))?\)"
                   (fn [[_ f anchor]] (str "](/docs/" (file->topic (str f ".md")) (when anchor (str "/" anchor)) ")")))
      (cond-> topic (str/replace #"\]\(#([^)\s]+)\)" (fn [[_ anchor]] (str "](/docs/" topic "/" anchor ")"))))))

(def documented-rules
  "Rules with a section in ERRORS.md, read once."
  (delay (set (some-> (source "ERRORS.md") section-ids))))

(defn rule-doc
  "The doc id an error with `rule` points to, when the manual documents the rule."
  [rule]
  (when (and (string? rule) (contains? @documented-rules (slug rule)))
    (str "errors/" (slug rule))))
