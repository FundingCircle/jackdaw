(ns xf-word-count-test
  (:gen-class)
  (:require [clojure.test :refer [deftest is]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.protocols :as jsp]
            [jackdaw.test :as jt]
            [jackdaw.test.fixtures :as jtf]
            [xf-word-count :as xfwc])
  (:import java.util.Properties
           org.apache.kafka.streams.TopologyTestDriver))

(def test-config
  {:broker-config {"bootstrap.servers" "localhost:9092"}
   :topic-metadata {:input
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
                     :value-serde (js/edn-serde)}}
  :app-config xfwc/streams-config
  :enable? (System/getenv "BOOTSTRAP_SERVERS")})

(defn topology-builder
  [topic-metadata]
  (xfwc/topology-builder topic-metadata #(xfwc/xf % xfwc/kv-store-swap-fn)))

(defn props-for
  [x]
  (doto (Properties.)
    (.putAll (reduce-kv (fn [m k v]
                          (assoc m (str k) (str v)))
                        {}
                        x))))

(def mock-transport-config
  {:driver (let [streams-builder (j/streams-builder)
                 topology ((topology-builder (:topic-metadata test-config)) streams-builder)]
             (TopologyTestDriver. (.build (j/streams-builder* topology))
                                  (props-for (:app-config test-config))))})

(def test-transport
  (jt/mock-transport mock-transport-config (:topic-metadata test-config)))

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
  (jtf/with-fixtures [(jtf/integration-fixture topology-builder test-config)]
    (jackdaw.test/with-test-machine test-transport
      (fn [machine]
        (let [{:keys [results journal]} (jackdaw.test/run-test machine commands)]

          (is (= 1 (word-count journal "large")))
          (is (= 2 (word-count journal "program"))))))))
