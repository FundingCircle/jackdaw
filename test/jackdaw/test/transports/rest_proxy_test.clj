(ns jackdaw.test.transports.rest-proxy-test
  (:require
   [byte-streams :as bs]
   [clojure.tools.logging :as log]
   [clojure.test :refer [deftest is testing]]
   [clojure.data.json :as json]
   [jackdaw.streams :as k]
   [jackdaw.test :as jd.test]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.test.serde :as serde]
   [jackdaw.test.journal :refer [with-journal watch-for]]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.transports.rest-proxy :as proxy]
   [manifold.stream :as s]
   [manifold.deferred :as d]))

(set! *warn-on-reflection* false)

(def kafka-config {"bootstrap.servers" "localhost:9092"
                   "group.id" "kafka-write-test"})

(def +real-rest-proxy-url+
  "http://localhost:8082")

(defn rest-proxy-config
  [group-id]
  {:bootstrap-uri +real-rest-proxy-url+
   :group-id group-id})

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

(def topic-config {:test-in test-in
                   :test-out test-out})

(defn rest-proxy-transport
  [config topics]
  (trns/transport {:type :confluent-rest-proxy
                   :config config
                   :topics topics}))

(defn with-rest-proxy-transport
  [{:keys [transport app app-id]} f]
  (fix/with-fixtures [(fix/topic-fixture kafka-config topic-config)
                      (fix/kstream-fixture (kstream-config app app-id))
                      (fix/service-ready? {:http-url +real-rest-proxy-url+
                                           :http-timeout 5000})]
    (with-open [machine (jd.test/test-machine (transport))]
      (log/info "begin" app-id)
      (let [result (f machine)]
        (log/info "end" app-id)
        result))))


(deftest test-rest-proxy-transport
  (with-rest-proxy-transport {:app-id "echo-for-exit-hooks"
                              :transport (fn []
                                           (rest-proxy-transport (rest-proxy-config "test-exit-hooks") topic-config))
                              :app (echo-stream test-in test-out)}
    (fn [t]
      (is (get-in t [:consumer :messages]))
      (is (get-in t [:producer :messages]))
      (is (coll? (:exit-hooks t)))
      (is (instance? clojure.lang.Agent (:journal t))))))

(deftest test-rest-proxy-write
  (with-rest-proxy-transport {:app-id "echo-for-write"
                              :transport (fn []
                                           (rest-proxy-transport (rest-proxy-config "test-rest-proxy-write") topic-config))
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

        (let [result @ack]
          (is (= "test-in" (:topic result)))
          (is (integer? (:partition result)))
          (is (integer? (:offset result))))))))

(deftest test-rest-proxy-read
  (with-rest-proxy-transport {:app-id "echo-for-read"
                              :transport (fn []
                                           (rest-proxy-transport (rest-proxy-config "test-rest-proxy-read") topic-config))
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
            (is (= "test-in" (:topic result)))
            (is (integer? (:partition result)))
            (is (integer? (:offset result)))))

        (testing "the journal is updated"
          (let [result (-> (watch-for t (fn [journal]
                                          (log/info "jrnl:" journal)
                                          (->> (get-in journal [:topics :test-out])
                                               (filter (fn [m]
                                                         (= 2 (get-in m [:value :id]))))
                                               first))
                                      20000
                                      "failed to find test-out=2")
                           :info)]
            (is (= :test-out (:topic result)))
            (is (= 2 (:key result)))
            (is (= {:id 2 :payload "foo"} (:value result)))))))))

(defn mock-http-client
  [req-atom]
  (fn [url req-options]
    (swap! req-atom conj [url req-options])
    (d/future {:status 200
               :body (bs/to-input-stream (json/write-str {:instance-id "yolo"}))})))

(deftest test-rest-proxy-group-config
  (let [http-reqs (atom [])]
    (binding [proxy/*http-client* {:post (mock-http-client http-reqs)}]
      (let [client (-> (proxy/rest-proxy-client (-> (rest-proxy-config "test-group-config")
                                                    (assoc :group-config {:auto.offset.reset "earliest"
                                                                          :fetch.min.bytes 100
                                                                          :consumer.fetch.timeout.ms 200})))
                       (proxy/with-consumer))
            [url options] (first @http-reqs)]
        (is (= "http://localhost:8082/consumers/test-group-config" url))
        (is (= {"Accept" "application/vnd.kafka.v2+json"
                "Content-Type" "application/vnd.kafka.v2+json"}
               (:headers options)))
        (is (= {"auto.offset.reset" "earliest"
                "fetch.min.bytes" 100
                "consumer.fetch.timeout.ms" 200}
               (-> (:body options)
                   (json/read-str)
                   (select-keys ["auto.offset.reset"
                                 "fetch.min.bytes"
                                 "consumer.fetch.timeout.ms"]))))))))
