(ns jackdaw.test.transports.rest-proxy-test
  (:require
   [aleph.http :as http]
   [clojure.tools.logging :as log]
   [clojure.core.async :as async]
   [clojure.test :refer :all]
   [jackdaw.streams :as k]
   [jackdaw.test :as jd.test]
   [jackdaw.test-config :refer [test-config]]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.test.serde :as serde]
   [jackdaw.test.journal :refer [with-journal watch-for]]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.transports.rest-proxy :as rest-proxy])
  (:import
   (java.util Properties)))

(def kafka-config {"bootstrap.servers" (format "%s:%s"
                                               (get-in (test-config) [:broker :host])
                                               (get-in (test-config) [:broker :port]))
                   "group.id" "kafka-write-test"})

(defn rest-proxy-config
  [group-id]
  {:bootstrap-uri (format "http://%s:%s"
                          (get-in (test-config) [:kafka-rest :host])
                          (get-in (test-config) [:kafka-rest :port]))
   :group-id group-id})

(defn kstream-config
  [app app-id]
  {:topology app
   :config {"bootstrap.servers" (format "%s:%s"
                                        (get-in (test-config) [:broker :host])
                                        (get-in (test-config) [:broker :port]))
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

(defn rest-proxy-transport
  [config topics]
  (trns/transport {:type :confluent-rest-proxy
                   :config config
                   :topics topics}))

(defn with-rest-proxy-transport
  [{:keys [transport app app-id]} f]
  (fix/with-fixtures [(fix/topic-fixture kafka-config topic-config)
                      (fix/skip-to-end {:topic test-in
                                        :config kafka-config})
                      (fix/kstream-fixture (kstream-config app app-id))]

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
        (async/put! messages
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
                                      (log/info "jrnl:" journal)
                                      (->> (get-in journal [:topics "test-out"])
                                           (filter (fn [m]
                                                     (= 2 (get-in m [:value :id]))))
                                           first))
                                  10000
                                  "failed to find test-out=2")]

            (is (= "test-out" (:topic result)))
            (is (= 2 (:key result)))
            (is (= {:id 2 :payload "foo"} (:value result)))))))))
