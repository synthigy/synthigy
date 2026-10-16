# Embedding Synthigy

An embedded app is **one jar**: your code plus the engine, pulled from Clojars
as a bundle (`com.synthigy/sqlite-bundle`, `postgres-bundle`,
`postgres-clickhouse-bundle`). The same jar runs two ways: on its own
(`java -jar`), or supervised by the `synthigy` CLI, which adds the portal:
first superuser, Stop/Start/Restart, logs, versions and module health.

## 1. Your app is a module

```clojure
;; deps.edn
{:deps {com.synthigy/sqlite-bundle {:mvn/version "0.3.0"}}}
```

Register a patcho (`dev.gersak/patcho`) lifecycle module. Depend on
what you use. The engine starts dependencies first and stops dependents first.

```clojure
(ns my.app
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.server.routes :as routes]))

(defn home [request]
  (when (= :get (:request-method request))
    {:status 200 :headers {"Content-Type" "text/plain"} :body "hello"}))

(lifecycle/register-module!
 :my/app
 {:depends-on [:synthigy/server]
  :start (fn [] (routes/register-prefix! ::home "/" #'home))
  :stop  (fn [] (routes/unregister-prefix! ::home))})
```

Ship exactly **one** bundle on the classpath. Two engines are refused at boot.

A route registered this way runs inside the engine's request context: every
log line it produces carries the request's id and the client's `ip`, and every
audit row the request's id. Work that starts anywhere else — your own HTTP
server, a scheduler — attaches the same context itself:

```clojure
(require '[synthigy.log :as log])

(log/with-ctx {:request-id "import-2026-10-06" :ip "198.51.100.7"}
  (import-orders!))
```

## 2. Run it standalone

```clojure
(ns my.main
  (:gen-class)
  (:require
   [synthigy.boot :as boot]
   synthigy.core
   synthigy.server
   synthigy.server.console
   my.app))

(def modules [:my/app :synthigy/console])

(defn -main [& _]
  (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable #(boot/stop! modules)))
  (boot/start! modules)
  @(promise))
```

A module that fails to start does not stop the others. `boot/start!` starts
every module it can, skips the ones that need the failed one, starts audit and
logs, and then throws an error naming each module that did not start.

Configuration is environment variables (`SYNTHIGY_SERVER_PORT`, ...). The
SQLite files live in `$SYNTHIGY_HOME/db/`: `synthigy.db`, `audit.db` and
`logs.db`. The engine refuses to start without a master key for its
[encryption](ENCRYPTION.md). `synthigy up` writes one into `.env` the first
time it starts an engine; before that, set your own as
`SYNTHIGY_ENCRYPTION_MASTER_KEY=b64:` followed by the output of
`openssl rand -base64 32`. `synthigy exec` loads the project's
`.synthigy/.env` and runs your jar with the project's `.synthigy/` as its home:

```bash
synthigy exec -- java -jar target/my-app.jar
# first superuser, from the same jar
synthigy exec -- java -cp target/my-app.jar clojure.main -m synthigy.ops superuser add admin <password>
```

## 3. Run it under the portal

Put the jar and your modules in the project's `.synthigy/.env`:

```bash
SYNTHIGY_BUNDLE=sqlite                     # the bundle your jar carries
SYNTHIGY_JAR=/abs/path/target/my-app.jar   # absolute path
SYNTHIGY_REQUIRE=my.app                    # namespaces to load, comma separated
SYNTHIGY_MODULES=:my/app,:synthigy/console # modules to start, comma separated
```

The database files are in the instance's `db/` folder, next to that `.env`.

```bash
synthigy up      # portal at http://127.0.0.1:7888
synthigy down
```

- `SYNTHIGY_MODULES` **replaces** the default start list (`:synthigy/server`,
  `:synthigy/console`). Dependencies start on their own. List
  `:synthigy/console` when your module does not depend on it and you want the
  console at `/console`.
- Unset, the engine starts the default list and your code is not loaded.
- Stop stops the server and every listed module. Start brings them back.
- The Engine panel shows the version your jar's engine reports and the state of
  your modules. A namespace or module that fails to load is shown there with
  its error. The other modules and the console still start, and the engine
  stays up so you can fix `.env` and restart.
- Your `-main` is not used here. The portal boots the engine itself and starts
  your modules through the same lifecycle.
