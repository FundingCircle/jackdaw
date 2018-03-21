(ns jackdaw.test.fixtures-integration-test
  (:require
   [circle.wait-for :refer [wait-for]]
   [clj-http.client :as http]
   [clj-time.core :as t]
   [clj-time.coerce :as c]
   [clojure.data.json :as json]
   [clojure.java.jdbc :as jdbc]
   [clojure.test :refer :all]
   [environ.core :as env]
   [jackdaw.client :as client]
   [jackdaw.test.config :as config]
   [jackdaw.test.fs :as fs]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.test.test-config :as test-config])
  (:import
   (java.util UUID)))

(def db-spec
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname "/tmp/jackdaw-test/"})

(defn kafka-connect-source-data-table
  [f]
  (let [table-ddl (jdbc/create-table-ddl :kafka_connect_source_data
                                         [[:id "integer" "primary key"]
                                          [:foreign_id :text "not null"]
                                          [:updated_at :timestamp "default current_timestamp"]])]
    (jdbc/execute! db-spec ["drop table if exists kafka_connect_source_data"])
    (jdbc/execute! db-spec [table-ddl])
    (f)))

(use-fixtures :each kafka-connect-source-data-table)

(def poll-timeout-ms 1000)
(def consumer-timeout-ms 5000)

(defn fuse
  "Returns a function that throws an exception when called after some time has passed."
  [millis]
  (let [end (+ millis (System/currentTimeMillis))]
    (fn []
      (if (< end (System/currentTimeMillis))
        (throw (ex-info "Timer expired" {:millis millis}))
        true))))

(deftest ^:integration integration-test
  (let [topic {:jackdaw.topic/topic-name "foo"}]
    (with-open [producer (client/producer test-config/producer)
                consumer (-> (client/consumer test-config/consumer)
                             (client/subscribe topic))]

    (testing "publish!"
      (let [result (client/send! producer (client/producer-record "foo" "1" "bar"))]
        (are [key] (get (client/metadata @result) key)
          :offset
          :topic
          :toString
          :partition
          :checksum
          :serializedKeySize
          :serializedValueSize
          :timestamp)))

      (testing "consume!"
        (let [[key val] (-> (client/log-messages consumer
                                                 poll-timeout-ms
                                                 (fuse consumer-timeout-ms))
                            first)]
          (is (= ["1" "bar"] [key val])))))))

(deftest ^:integration kafka-connect-source-connector-test
  (let [fix (fix/kafka-connect test-config/kafka-connect-worker-config)]
    (testing "kafka-connect source connector"
      (fix
       (fn []
         (let [foreign-id (UUID/randomUUID)
               date (t/now)
               repayment {:foreign_id foreign-id
                          :updated_at (c/to-sql-date date)}
               kc-query "SELECT foreign_id, id, updated_at FROM kafka_connect_source_data"
               task-config {:name "kafka-connect-source"
                            :config {:mode "incrementing"
                                     :timestamp.column.name "updated_at"
                                     :incrementing.column.name "id"
                                     :connector.class "io.confluent.connect.jdbc.JdbcSourceConnector"
                                     :connection.url "jdbc:sqlite://tmp/jackdaw-test"
                                     :name "kafka-connect-source"
                                     :query kc-query
                                     :topic.prefix "kafka-connect-source"
                                     :topics "kafka-connect-source"}}]

           (http/post (format "http://%s:%s/connectors"
                              test-config/kafka-connect-host
                              test-config/kafka-connect-port)
                      {:content-type :json
                       :body (json/write-str task-config)})

           (jdbc/insert! db-spec
                         :kafka_connect_source_data
                         [:foreign_id :updated_at]
                         [foreign-id (c/to-sql-date date)])

           (with-open [consumer (-> (client/consumer (assoc test-config/consumer "group.id"
                                                            (str "kafka-connect-test-" (UUID/randomUUID))))
                                    (client/subscribe {:jackdaw.topic/topic-name "kafka-connect-source"}))]

             (is (wait-for {:sleep (t/seconds 1)
                            :timeout (t/seconds 60)}
                           #(some-> (client/poll consumer 60000)
                                    first
                                    :value
                                    (json/read-str)
                                    (get "foreign_id")
                                    (= (str foreign-id))))))))))))
