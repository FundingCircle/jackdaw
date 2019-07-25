(ns jackdaw.test.transports.kafka-test
  (:require
   [clojure.tools.logging :as log]
   [clojure.test :refer :all]
   [jackdaw.streams :as k]
   [jackdaw.test :as jd.test]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.test.journal :refer [with-journal watch-for]]
   [jackdaw.test.serde :as serde]
   [jackdaw.test.transports.kafka]
   [manifold.stream :as s])
  (:import
   (java.util Properties)))

(def kafka-config {"bootstrap.servers" "localhost:9092"
                   "group.id" "kafka-write-test"})

(defn kstream-config
  [app app-id]
  {:topology app
   :config {"bootstrap.servers" "localhost:9092"
            "application.id" app-id}})

(defn echo-stream
  "Makes a dummy stream processor that reads some topic and then
   promptly ignores it"
  [in out]
  (fn [builder]
    (let [in (-> (k/kstream builder in)
                 (k/map (fn [[k v]]
                          [k v])))]
      (k/to in out)
      builder)))

(def test-in
  (serde/resolver {:topic-name "test-in"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :long
                   :value-serde :edn}))

(def test-out
  (serde/resolver {:topic-name "test-out"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :long
                   :value-serde :edn}))

(def topic-config {"test-in" test-in
                   "test-out" test-out})

(defn with-kafka-transport
  [{:keys [transport app app-id]} f]
  (fix/with-fixtures [(fix/topic-fixture kafka-config topic-config)
                      (fix/skip-to-end {:topic test-in
                                        :config kafka-config})
                      (fix/kstream-fixture (kstream-config app app-id))]

    (with-open [machine (jd.test/test-machine (transport))]
      (log/info "started" app-id)
      (let [result (f machine)]
        (log/info "completed" app-id)
        result))))

(deftest test-transport-exit-hooks
  (with-kafka-transport {:app-id "test-exit-hooks"
                         :transport (fn []
                                      (jd.test/kafka-transport kafka-config topic-config))
                         :app (echo-stream test-in test-out)}
    (fn [t]
      (is (coll? (:exit-hooks t)))
      (is (instance? clojure.lang.Agent (:journal t))))))

(deftest test-kafka-transport
  (with-kafka-transport {:app-id "test-kafka-transport"
                         :transport (fn []
                                      (jd.test/kafka-transport kafka-config topic-config))
                         :app (echo-stream test-in test-out)}
    (fn [t]
      (log/info "testing kafka-transport")
      (let [msg {:id 1 :payload "foo"}
            topic test-in
            messages (get-in t [:producer :messages])
            serdes (get-in t [:serdes])
            ack (promise)
            msg-key (:id msg)]

        (log/info "feed: " msg)
        (s/put! messages
                {:topic topic
                 :key msg-key
                 :value msg
                 :timestamp (System/currentTimeMillis)
                 :ack ack})

        (let [result (deref ack 1000 {:error :timeout})]
          (is (= "test-in" (:topic-name result)))
          (is (integer? (:partition result)))
          (is (integer? (:offset result))))))))

(deftest test-kafka-transport-with-journal
  (with-kafka-transport {:app-id "test-kafka-transport-with-journal"
                         :transport (fn []
                                      (jd.test/kafka-transport kafka-config topic-config))
                         :app (echo-stream test-in test-out)}
    (fn [t]
      (let [msg {:id 2 :payload "foo"}
            topic test-in
            messages (get-in t [:producer :messages])
            serdes (get-in t [:serdes])
            ack (promise)
            msg-key (:id msg)]

        (log/info "feed: " msg)
        (s/put! messages
                {:topic topic
                 :key msg-key
                 :value msg
                 :timestamp (System/currentTimeMillis)
                 :ack ack})

        (testing "the write is acknowledged"
          (let [result (deref ack 1000 {:error :timeout})]
            (is (= "test-in" (:topic-name result)))
            (is (integer? (:partition result)))
            (is (integer? (:offset result)))))

        (testing "the journal is updated"
          (let [result (-> (watch-for t (fn [journal]
                                          (->> (get-in journal [:topics "test-out"])
                                               (filter (fn [m]
                                                         (= 2 (get-in m [:value :id]))))
                                               first))
                                      1000
                                      "failed to find test-out=2")
                           :info)]

            (is (= "test-out" (:topic result)))
            (is (= 2 (:key result)))
            (is (= {:id 2 :payload "foo"} (:value result)))))))))
