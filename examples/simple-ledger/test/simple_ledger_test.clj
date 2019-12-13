(ns simple-ledger-test
  (:gen-class)
  (:require [clojure.test :refer [deftest is]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.protocols :as jsp]
            [jackdaw.streams.xform :as jxf]
            [jackdaw.test :as jt]
            [jackdaw.test.fixtures :as jt.fix]
            [simple-ledger :as sl])
  (:import java.util.Properties
           org.apache.kafka.streams.TopologyTestDriver))

(deftest simple-ledger-unit-test
  (let [entries
        [["1" {:debit-account "tech"
               :credit-account "cash"
               :amount 1000}]
         ["2" {:debit-account "cash"
               :credit-account "sales"
               :amount 2000}]]
        transactions (->> entries
                          (transduce (sl/split-entries nil nil) concat)
                          (transduce (sl/running-balances (atom {}) swap!) concat))]
    (is (= -1000 (:after-balance (get (into {} transactions) "tech"))))
    (is (= -1000 (:after-balance (get (into {} transactions) "cash"))))
    (is (=  2000 (:after-balance (get (into {} transactions) "sales"))))))

(def topic-metadata
  {:entry-pending
   {:topic-name "entry-pending"
    :partition-count 15
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}

   :transaction-pending
   {:topic-name "transaction-pending"
    :partition-count 15
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}

   :transaction-added
   {:topic-name "transaction-added"
    :partition-count 15
    :replication-factor 1
    :key-serde (js/edn-serde)
    :value-serde (js/edn-serde)}})

(def test-config
  {:broker-config {"bootstrap.servers" "localhost:9092"}
   :topic-metadata topic-metadata
   :app-config sl/streams-config
   :enable? (System/getenv "BOOTSTRAP_SERVERS")})

(defn topology-builder
  [topic-metadata]
  (sl/topology-builder topic-metadata
                       {::sl/split-entries #(sl/split-entries % nil)
                        ::sl/running-balances #(sl/running-balances % jxf/kv-store-swap-fn)}))

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
  (= 4 (count (get-in journal [:topics :transaction-added]))))

(def commands
  [[:write!
    :entry-pending
    {:debit-account "tech" :credit-account "cash" :amount 1000}
    {:key-fn (constantly "1")}]
   [:write!
    :entry-pending
    {:debit-account "cash" :credit-account "sales" :amount 2000}
    {:key-fn (constantly "2")}]
   [:watch done? {:timeout 2000}]])

(defn simple-ledger
  [journal account-name]
  (->> (get-in journal [:topics :transaction-added])
       (filter (fn [x] (= account-name (:account-name (:value x)))))
       last
       :value))

(deftest simple-ledger-end-to-end-test
  (jt.fix/with-fixtures [(jt.fix/integration-fixture topology-builder test-config)]
    (jackdaw.test/with-test-machine (test-transport test-config)
      (fn [machine]
        (let [{:keys [results journal]} (jackdaw.test/run-test machine commands)]
          (is (= -1000 (:after-balance (simple-ledger journal "tech"))))
          (is (= -1000 (:after-balance (simple-ledger journal "cash"))))
          (is (=  2000 (:after-balance (simple-ledger journal "sales")))))))))
