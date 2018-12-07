(ns kafka.test.kafka-test
  (:require
   [clojure.test :refer :all]
   [com.stuartsierra.component :as component]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [kafka.test.fs :as fs]
   [kafka.test.zk :as zk]
   [kafka.test.kafka :as kafka])
  (:import
   (java.util.concurrent CountDownLatch)))

(def broker-config
  {"zookeeper.connect"            "localhost:2181"
   "bootstrap.servers"            "localhost:9092"
   "broker.id"                    "0"
   "advertised.host.name"         "localhost"
   "auto.create.topics.enable"    "true"
   "offsets.topic.num.partitions" "1"
   "log.dirs"                     (fs/tmp-dir "kafka-log")})

(defn system-under-test [config]
  (component/system-map
   :zookeeper (zk/server config)
   :kafka (component/using
           (kafka/server config)
           [:zookeeper])))

(deftest kafka-tests
  (testing "kafka lifecycle"
    (let [sut (-> (system-under-test broker-config)
                  (atom))]

      (is (= broker-config (get-in @sut [:kafka :config])))

      (testing "start!"
        (swap! sut component/start-system)
        (is (instance? CountDownLatch (get-in @sut [:kafka :latch]))))

      (testing "stop!"
        (swap! sut component/stop-system)
        (is 0 (.getCount (get-in @sut [:kafka :latch])))))))
