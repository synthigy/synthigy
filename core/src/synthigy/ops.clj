;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.ops
  "One-shot operator verbs straight against the database — the recovery
   floor for deployments with no portal binary."
  (:require
    [patcho.lifecycle :as lifecycle]
    [synthigy.json :as json]
    synthigy.core
    [synthigy.supervisor.verbs :as verbs]))

(defn fail! [msg]
  (binding [*out* *err*] (println msg))
  (System/exit 1))

(defn usage! []
  (fail! (str "usage: ... clojure.main -m synthigy.ops <verb>\n"
              "  init                              create tables, deploy schemas\n"
              "  superuser add <user> <pw>         create/update a ROOT user (recovery)\n"
              "  superuser list                    list ROOT users\n"
              "  iam add-client <name> <id> <type> [opts] [role ...]\n"
              "                                    create an OAuth client + role/API grants,\n"
              "                                    no server; type is public|confidential;\n"
              "                                    trailing bare words are role names.\n"
              "                                    opts: --secret <s>  pin the client secret\n"
              "                                          --api <name>  link an API (repeatable;\n"
              "                                                        confidential only — this\n"
              "                                                        is how a token gets the\n"
              "                                                        audience /data checks)\n"
              "                                          --grant <g>   add to allowed-grants\n"
              "                                                        (repeatable, e.g.\n"
              "                                                        client_credentials)\n"
              "                                          --trusted     allow acting_as\n"
              "                                    prints {id secret type roles apis}")))

(defn take-value! [flag more]
  (when (empty? more)
    (throw (ex-info (str flag " needs a value") {:code -32602})))
  [(first more) (rest more)])

(defn parse-client-opts
  "Pulls --secret/--api/--grant/--trusted out of a trailing arg list,
   wherever they appear; everything left over is positional (role names).
   --api and --grant are repeatable. Returns {:secret :apis :grants
   :trusted? :roles}."
  [args]
  (loop [args (seq args)
         out {:apis [] :grants [] :roles [] :trusted? false}]
    (if (empty? args)
      out
      (let [a (first args)
            more (rest args)]
        (case a
          "--secret" (let [[v more] (take-value! a more)]
                       (recur more (assoc out :secret v)))
          "--api" (let [[v more] (take-value! a more)]
                    (recur more (update out :apis conj v)))
          "--grant" (let [[v more] (take-value! a more)]
                      (recur more (update out :grants conj v)))
          "--trusted" (recur more (assoc out :trusted? true))
          (recur more (update out :roles conj a)))))))

(defn -main [& [verb & args]]
  (try
    (case verb
      "init"
      (println (json/write-str (verbs/run-init!)))

      "superuser"
      (let [[sub username password] args]
        (lifecycle/start! :synthigy/iam)
        (case sub
          "add" (println (json/write-str (verbs/set-superuser! username password)))
          "list" (println (json/write-str {:superusers (verbs/superusers)}))
          (usage!)))

      "iam"
      (let [[sub client-name client-id client-type & rest-args] args]
        (lifecycle/start! :synthigy/iam)
        (case sub
          "add-client"
          (let [{:keys [secret apis grants trusted? roles]} (parse-client-opts rest-args)
                settings (cond-> {}
                           (seq grants) (assoc "allowed-grants" grants)
                           trusted? (assoc "trusted" true))]
            (println (json/write-str
                      (verbs/add-client! {:name client-name :id client-id
                                          :type client-type :secret secret
                                          :apis apis :roles roles
                                          :settings (when (seq settings) settings)}))))
          (usage!)))

      (usage!))
    (System/exit 0)
    (catch Throwable t
      (fail! (or (ex-message t) (str t))))))
