(ns jackdaw.word-count-test
  (:require
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.java.shell :as sh]
   [clojure.edn :as edn]
   [clojure.test :refer :all]

   [manifold.deferred :as d]

   [jackdaw.serdes.avro.schema-registry :as reg]
   [jackdaw.streams :as k]
   [jackdaw.test :as jd.test]
   [jackdaw.test.commands :as cmd]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.test.serde :as serde]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.transports.kafka]
   [jackdaw.test.transports.mock]
   [jackdaw.test.middleware :refer [with-status]])
  (:import
   (java.util Properties)
   (java.io File)
   (org.apache.kafka.streams TopologyTestDriver)
   (org.apache.kafka.common.serialization Serdes)))

;;; Example of using the test-machine ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Here we take the word-count example from confluent's examples repo
;;
;;   https://github.com/confluentinc/kafka-streams-examples
;;
;; and provide an implementation in Clojure, and test it using the
;; test-machine.
;;
;; First the app....

(defn parse-line
  [line]
  (let [line (-> (.toLowerCase line (java.util.Locale/getDefault)))]
    (->> (.split line "\\W+")
         (into []))))

(defn word-count
  [in out]
  (fn [builder]
    (let [counts (-> (k/kstream builder in)
                     (k/flat-map-values (fn [line]
                                          (parse-line line)))
                     (k/group-by (fn [[k v]]
                                   v))
                     (k/count)
                     (k/to-kstream))]
      (k/to counts out)
      builder)))

;; In order to use the app builder defined above, we need to provide
;; it with topic definitions for the input/output topics

(def input
  {:topic-name "streams-plaintext-input"
   :replication-factor 1
   :partition-count 1
   :key-serde (Serdes/String)
   :value-serde (Serdes/String)})

(def output
  {:topic-name "streams-plaintext-output"
   :replication-factor 1
   :partition-count 1
   :key-serde (Serdes/String)
   :value-serde (Serdes/Long)})

(def word-count-topics
  {:input input
   :output output})

;; In order to *run* the app, we need to provide a test configuration

(def broker-config
  {"bootstrap.servers" "localhost:9092"})

(def app-config
  (assoc broker-config
   "cache.max.bytes.buffering" "0"
   "application.id" "streams-word-count"
   "default.key.serde" "org.apache.kafka.common.serialization.Serdes$StringSerde"
   "default.value.serde" "org.apache.kafka.common.serialization.Serdes$StringSerde"))

(def test-config
  (assoc broker-config
    "group.id" "word-count-test"))

;; Now we're ready to build a test

(defn write-lines
  "Helper for generating input commands. For each line, we return a `:write!`
   command that will produces a record when executed by the test-machine
   (in this case with k = v = line)"
  []
  (let [lines ["As Gregor Samsa awoke one morning from uneasy dreams"
               "he found himself transformed in his bed into an enormous insect"
               "What a fate: to be condemned to work for a firm where the"
               "slightest negligence at once gave rise to the gravest suspicion"
               "How about if I sleep a little bit longer and forget all this nonsense"
               "I cannot make you understand"]]
    (map (fn [line]
           [:write! :input line {:key-fn identity}]) lines)))

(defn watch-for-output
  "Builds a test-command that blocks until the supplied word appears"
  [word]
  [[:watch (fn [journal]
             (some #(= word (:key %))
                   (get-in journal [:topics :output]))) 2000]])

(deftest test-word-count-demo
  (fix/with-fixtures [(fix/topic-fixture broker-config word-count-topics
                                         {:timeout-ms 10000
                                          :delete-first? false})
                      (fix/reset-application app-config)
                      (fix/kstream-fixture {:topology (word-count input output)
                                            :config app-config})]
    ;; The fixtures above ensure that
    ;;
    ;;   * the necessary topics are created
    ;;   * the application reset tool is run to delete any state left-over from
    ;;     prior test-runs
    ;;   * the word-count app is started (and then stopped after the test is complete)

    (with-open [machine (-> (jd.test/kafka-transport test-config word-count-topics)
                            (jd.test/test-machine))]

      ;; We now have a test-machine (in this case, running against our local dev cluster)
      ;; and can use `run-tests` to execute test commands and see their effect on the journal

      (let [{:keys [results journal]} (->> (concat
                                            (write-lines)
                                            (watch-for-output "understand"))
                                           (jd.test/run-test machine))
            wc (fn [word]
                 ;; A helper to extract the latest value from the word-count ktable
                 ;; as observed by the test-consumer
                 (->> (get-in journal [:topics :output])
                      (filter (fn [r]
                                (= (:key r) word)))
                      last
                      :value))]

        (is (every? #(= :ok (:status %)) results))

        (is (= 1 (wc "understand")))
        (is (= 2 (wc "i")))
        (is (= 3 (wc "to")))))))
