(ns kafka.test.config
  (:require
   [clojure.string :as str]
   [environ.core :as env]
   [ragtime.jdbc :as jdbc]
   [ragtime.repl :as repl]))

(defn host-port [host-str]
  (try
    (let [[host port] (str/split host-str #"\:")]
      {:host host
       :port (Integer/parseInt port)})
    (catch Exception e
      (throw (ex-info "invalid host string: " {:host-str host-str} e)))))

(defn multi-config
  "Convenience function for generating multi-config functions compatible with the
   multi-broker fixture

   Pass in a base config and this will convert it into a multi-config by making node
   specific updates to the following keys

    broker.id
    port
    advertised.port
    log.dirs
  "
  [base]
  (fn [n]
    (let [port (+ n (Integer/parseInt (or (get base "port")
                                          "9092")))]
      (assoc base
             "broker.id" (str n)
             "port" (str port)
             "advertised.port" (str port)
             "log.dirs" (str (get base "log.dirs")
                             "-"
                             n)))))

(def db-conf
  {:dbtype "postgresql"
   :host (:db-host env/env)
   :user (:db-user env/env)
   :password (:db-password env/env)
   :dbname (:db-name env/env)
   :port (:db-port env/env)})

(def ragtime-database-conf
    {:datastore
     (jdbc/sql-database
       db-conf)
   :migrations (jdbc/load-resources "migrations")})

(defn migrate [] (repl/migrate ragtime-database-conf))

(defn rollback [] (repl/rollback ragtime-database-conf))
