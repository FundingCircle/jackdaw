(ns kafka.test.test-helper
  (:require
   [clojure.test :refer :all]
   [environ.core :refer [env]]
   [kafka.test.config :as config]
   [kafka.test.fixtures :as fix]))

(defn embedded-fixtures? []
  (read-string (env :embedded-fixtures)))

(defn test-harness [producers log-seqs]
  (if (embedded-fixtures?)
    (join-fixtures [(fix/kafka-platform config/broker)
                    producers
                    log-seqs])

    (join-fixtures (vector producers log-seqs))))
