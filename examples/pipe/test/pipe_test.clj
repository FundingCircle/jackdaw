(ns pipe-test
  (:gen-class)
  (:require [clojure.test :refer [deftest is]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.protocols :as jsp]
            [jackdaw.streams.xform :as jxf]
            [jackdaw.test :as jt]
            [jackdaw.test.fixtures :as jt.fix]
            [pipe])
  (:import java.util.Properties
           org.apache.kafka.streams.TopologyTestDriver))

(deftest pipe-unit-test
  (let [input [["1" "foo"] ["2" "bar"]]
        output (transduce (pipe/xf nil nil) concat input)]
    (is (= "foo" (get (into {} output) "1")))
    (is (= "bar" (get (into {} output) "2")))))

(def topic-metadata
  {:input
   {:topic-name "input"
    :partition-count 15
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}

   :output
   {:topic-name "output"
    :partition-count 15
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}})

(def test-config
  {:broker-config {"bootstrap.servers" "localhost:9092"}
   :topic-metadata topic-metadata
   :app-config pipe/streams-config
   :enable? (System/getenv "BOOTSTRAP_SERVERS")})

(defn topology-builder
  [topic-metadata]
  (pipe/topology-builder topic-metadata {:pipe/xf #(pipe/xf % jxf/kv-store-swap-fn)}))

(defn props-for
  [x]
  (doto (Properties.)
    (.putAll (reduce-kv (fn [m k v]
                          (assoc m (str k) (str v)))
                        {}
                        x))))

(defn mock-transport-config
  []
  {:driver (let [streams-builder (j/streams-builder)
                 topology ((topology-builder (:topic-metadata test-config)) streams-builder)]
             (TopologyTestDriver. (.build (jsp/streams-builder* topology))
                                  (props-for (:app-config test-config))))})

(defn test-transport
  [{:keys [topic-metadata] :as test-config}]
  (jt/mock-transport (mock-transport-config) topic-metadata))

(defn done?
  [journal]
  (= 2 (count (get-in journal [:topics :output]))))

(def commands
  [[:write! :input "foo" {:key-fn (constantly "1")}]
   [:write! :input "bar" {:key-fn (constantly "2")}]
   [:watch done? {:timeout 2000}]])

(defn pipe
  [journal k]
  (->> (get-in journal [:topics :output])
       (filter (fn [x] (= k (:key x))))
       last
       :value))

(deftest simple-ledger-end-to-end-test
  (jt.fix/with-fixtures [(jt.fix/integration-fixture topology-builder test-config)]
    (jackdaw.test/with-test-machine (test-transport test-config)
      (fn [machine]
        (let [{:keys [results journal]} (jackdaw.test/run-test machine commands)]
          (is (= "foo" (pipe journal "1")))
          (is (= "bar" (pipe journal "2"))))))))
