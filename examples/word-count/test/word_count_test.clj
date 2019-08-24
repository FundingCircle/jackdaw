(ns word-count-test
  (:require
   [word-count :as wc]
   [jackdaw.streams :as k]
   [jackdaw.streams.protocols :as proto]
   [jackdaw.test :as jd.test]
   [jackdaw.test.fixtures :as fix]
   [clojure.test :as t :refer [deftest is testing]])
  (:import
   (java.util Properties)
   (org.apache.kafka.streams TopologyTestDriver)))

(def broker-config
  {"bootstrap.servers" "localhost:9092"})

(defn test-consumer-config
  [broker-config]
  (assoc broker-config
         "group.id" "word-count-test"))

(defn input-writer
  "Helper for generating input commands. For each line, we return a `:write!`
   command that will produces a record when executed by the test-machine
   (in this case with k = v = line)"
  [line]
  [:write! :input line {:key-fn identity}])

(defn word-watcher
  "Builds a test-command that blocks until the supplied word appears"
  [word]
  [:watch (fn [journal]
            (some #(= word (:key %))
                  (get-in journal [:topics :output]))) 2000])

(defn word-count
  "A simple helper to extract the latest value from the word-count ktable
   as observed by the test-consumer.

   The journal collects all records as a vector of maps representing
   ConsumerRecords for each topic. Since we're inspecting a mutating
   table, we want to get the `last` matching record for `word`."
  [journal word]
  (->> (get-in journal [:topics :output])
       (filter (fn [r]
                 (= (:key r) word)))
       last
       :value))

(defn get-env [k]
  (get (System/getenv) k))

(def test-config
  {:broker-config broker-config
   :topic-metadata wc/topic-metadata
   :app-config (assoc wc/app-config "cache.max.bytes.buffering" "0")
   :enable? (get-env "BOOTSTRAP_SERVERS")})

(defn props-for [x]
  (doto (Properties.)
    (.putAll (reduce-kv (fn [m k v]
                          (assoc m (str k) (str v)))
                        {}
                        x))))

(defn mock-transport-config
  []
  {:driver (let [builder (k/streams-builder)
                 app (wc/topology-builder wc/topic-metadata)
                 topology (.build (proto/streams-builder* (app builder)))]
             (TopologyTestDriver.
              topology
              (props-for (:app-config test-config))))})

(defn test-transport
  [topics]
  (cond
    (get-env "REST_PROXY_URL")
    (let [rest-proxy-url (get-env "REST_PROXY_URL")]
      (jd.test/rest-proxy-transport {:bootstrap-uri rest-proxy-url
                                     :group-id (get (test-consumer-config {}) "group.id")}
                                    topics))

    (get-env "BOOTSTRAP_SERVERS")
    (let [broker (get-env "BOOTSTRAP_SERVERS")]
      (jd.test/kafka-transport (test-consumer-config {"bootstrap.servers" broker})
                               topics))

    :else
    (jd.test/mock-transport (mock-transport-config) wc/topic-metadata)))

(deftest test-word-count-example
  (fix/with-fixtures [(fix/integration-fixture wc/topology-builder test-config)]
    (jd.test/with-test-machine (test-transport wc/topic-metadata)
      (fn [machine]
        (let [lines ["As Gregor Samsa awoke one morning from uneasy dreams"
                     "he found himself transformed in his bed into an enormous insect"
                     "What a fate: to be condemned to work for a firm where the"
                     "slightest negligence at once gave rise to the gravest suspicion"
                     "How about if I sleep a little bit longer and forget all this nonsense"
                     "I cannot make you understand"]
              commands (->> (concat
                             (map input-writer lines)
                             [(word-watcher "understand")]))

              {:keys [results journal]} (jd.test/run-test machine commands)]

          (is (every? #(= :ok (:status %)) results))

          (is (= 1 (word-count journal "understand")))
          (is (= 2 (word-count journal "i")))
          (is (= 3 (word-count journal "to"))))))))
