(ns jackdaw.test.transports.mock-test
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer :all]
   [clojure.tools.logging :as log]
   [jackdaw.streams :as k]
   [jackdaw.test.journal :refer [with-journal watch-for]]
   [jackdaw.test.transports.mock :as mock]
   [jackdaw.test :as jd.test]
   [jackdaw.test-config :refer [test-config]]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.serde :as serde])
  (:import
   (java.util Properties)
   (org.apache.kafka.streams TopologyTestDriver)))

(defmethod print-method TopologyTestDriver [x writer]
  (print-simple x writer))

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

(defn test-driver
  [f app-config]
  (let [builder (k/streams-builder)
        topology (let [builder (f builder)]
                   (-> (k/streams-builder* builder)
                       (.build)))]
    (TopologyTestDriver. topology
                         (let [props (Properties.)]
                           (doseq [[k v] app-config]
                             (.setProperty props k v))
                           props))))

(defn mock-transport
  []
  (trns/transport {:type :mock
                   :driver (test-driver (echo-stream test-in test-out)
                                        {"bootstrap.servers" (format "%s:%s"
                                                                     (get-in (test-config) [:broker :host])
                                                                     (get-in (test-config) [:broker :port]))
                                         "application.id" "test-echo-stream"})
                   :topics {"test-in" test-in
                            "test-out" test-out}}))

(defn with-mock-transport
  [{:keys [test-id]} f]
  (with-open [machine (jd.test/test-machine (mock-transport))]
    (log/info "started" test-id)
    (let [result (f machine)]
      (log/info "completed" test-id)
      result)))

(deftest test-mock-transport
  (with-mock-transport {:test-id "test-mock-transport"}
    (fn [t]
      (let [msg {:id 1 :payload "foo"}
            topic test-in
            messages (get-in t [:producer :messages])
            serdes (get-in t [:serdes])
            ack (promise)
            msg-key (:id msg)]

        (async/put! messages
                    {:topic topic
                     :key msg-key
                     :value msg
                     :timestamp (System/currentTimeMillis)
                     :ack ack})

        (let [result (deref ack 1000 {:error :timeout})]
          (is (= "test-in" (:topic result)))
          (is (integer? (:partition result)))
          (is (integer? (:offset result))))))))

(deftest test-mock-transport-with-journal
  (with-mock-transport {:test-id "test-mock-transport-with-journal"}
    (fn [t]
      (let [msg {:id 1 :payload "foo"}
            topic test-in
            messages (get-in t [:producer :messages])
            serdes (get-in t [:serdes])
            ack (promise)
            msg-key (:id msg)]

        (async/put! messages
                    {:topic topic
                     :key msg-key
                     :value msg
                     :timestamp (System/currentTimeMillis)
                     :ack ack})

        (testing "the write is acknowledged"
          (let [result (deref ack 1000 {:error :timeout})]
            (is (= "test-in" (:topic result)))
            (is (integer? (:partition result)))
            (is (integer? (:offset result)))))

        (testing "the journal is updated"
          (let [result (watch-for t (fn [journal]
                                      (->> (get-in journal [:topics "test-out"])
                                           (filter (fn [m]
                                                     (= 1 (get-in m [:value :id]))))
                                           first))
                                  1000
                                  "failed to find test-out=2")]

            (is (= "test-out" (:topic result)))
            (is (= 1 (:key result)))
            (is (= {:id 1 :payload "foo"} (:value result)))))))))
