(ns xf-word-count-test
  (:gen-class)
  (:require [clojure.test :refer [deftest is]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.protocols :as jsp]
            [jackdaw.streams.xform :as jxf]
            [jackdaw.test :as jt]
            [jackdaw.test.fixtures :as jt.fix]
            [xf-word-count :as xfwc])
  (:import java.util.Properties
           org.apache.kafka.streams.TopologyTestDriver))

(def topic-metadata
  {:input
   {:topic-name "input"
    :partition-count 1
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}

   :output
   {:topic-name "output"
    :partition-count 1
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}})

(def test-config
  {:broker-config {"bootstrap.servers" "localhost:9092"}
   :topic-metadata topic-metadata
   :app-config xfwc/streams-config
   :enable? (System/getenv "BOOTSTRAP_SERVERS")})

(defn topology-builder
  [topic-metadata]
  (xfwc/topology-builder topic-metadata #(xfwc/xf % jxf/kv-store-swap-fn)))

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
  (= 12 (count (get-in journal [:topics :output]))))

(def commands
  [[:write! :input "inside every large program" {:key-fn (constantly "")}]
   [:write! :input "is a small program" {:key-fn (constantly "")}]
   [:write! :input "struggling to get out" {:key-fn (constantly "")}]
   [:watch done? {:timeout 2000}]])

(defn word-count
  [journal word]
  (->> (get-in journal [:topics :output])
       (filter (fn [x] (= word (:key x))))
       last
       :value))

(deftest test-xf-word-count
  (jt.fix/with-fixtures [(jt.fix/integration-fixture topology-builder test-config)]
    (jackdaw.test/with-test-machine (test-transport test-config)
      (fn [machine]
        (let [{:keys [results journal]} (jackdaw.test/run-test machine commands)]

          (is (= 1 (word-count journal "large")))
          (is (= 2 (word-count journal "program"))))))))
