(ns kafka.test.fixtures.service
  "Test fixtures for kafka based apps"
  (:require
   [clojure.tools.logging :as log]
   [kafka.admin :as admin]
   [kafka.core :as kafka]
   [kafka.test.zk :as zk]
   [kafka.test.kafka :as broker]))

(defn embedded-fixtures? []
  (System/getenv "EMBEDDED_FIXTURES"))


(defn broker
  "A kafka test broker fixture.

   Start up a kafka broker with the supplied config before running the
   test `t`"
  [config]
  (when-not (get config "log.dirs")
    (throw (ex-info "Invalid config: missing required field 'log.dirs'"
                    {:config config})))

  (fn [t]
    (if-not (embedded-fixtures?)
      (t)
      (let [log-dirs (get config "log.dirs")
            kafka (broker/start! {:config config})]
        (try
          (t)
          (finally
            (broker/stop! kafka)))))))
