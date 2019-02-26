(ns jackdaw.word-count-test
  (:require [clojure.test :refer :all]
            [jackdaw.streams :as k]
            [jackdaw.test :as jd.test]
            [jackdaw.test.fixtures :as fix])
  (:import org.apache.kafka.common.serialization.Serdes))

;;; Example of using the test-machine ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Here we take the word-count example from confluent's examples repo
;;
;;   https://github.com/confluentinc/kafka-streams-examples
;;
;; and provide an implementation in Clojure, and test it using the
;; test-machine.
;;
;; First the app. All pretty simple stuff.
;;
;;   - Parse each line into a list of words
;;   - Group By each unique word
;;   - Count the records in each group
;;   - Write the counts to the output topic

(defn parse-line
  [line]
  (let [line (-> (.toLowerCase line (java.util.Locale/getDefault)))]
    (->> (.split line "\\W+")
         (into []))))

(defn word-count
  [{:keys [input output]}]
  (fn [builder]
    (let [counts (-> (k/kstream builder input)
                     (k/flat-map-values (fn [line]
                                          (parse-line line)))
                     (k/group-by (fn [[k v]]
                                   v))
                     (k/count)
                     (k/to-kstream))]
      (k/to counts output)
      builder)))

;; In order to use the app builder defined above, we need to provide
;; it with topic definitions for the input/output topics. In a real app, you
;; might consider loading this type of configuration from an EDN file

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

;; For testing, we need to configure a few elements
;;
;;  - the "broker config" is shared by both the app and test configs and is
;;    needed to connect to kafka
;;  - the "app config" is a StreamsConfig and is supplied when building
;;    the app topology
;;  - the "test config" is a ConsumerConfig and is supplied when creating the
;;    consumer used by the test machine

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

(def integration-fixtures
  "The integration fixtures ensure that

     * the necessary topics are created (after deleting them if necessary)
     * the application reset tool is run to delete any intermediate topics
       left-over from prior test-runs
     * the word-count app is started (and then stopped after the test is complete)"
  [(fix/topic-fixture broker-config word-count-topics {:delete-first? true})
   (fix/reset-application-fixture app-config)
   (fix/kstream-fixture {:topology (word-count word-count-topics)
                         :config app-config
                         :cleanup-first? true})])


(deftest test-word-count-demo
  (fix/with-fixtures integration-fixtures
    (with-open [machine (-> (jd.test/kafka-transport test-config word-count-topics)

                            ;; The input to the test-machine is a "transport". This
                            ;; allows us to use different transports for different
                            ;; scenarios. In this case, the `kafka-transport`
                            ;; connects directly to kafka brokers to read/write
                            ;; messages.

                            (jd.test/test-machine))]

      ;; We now have a test-machine (in this case, running against our local dev cluster)
      ;; and can use `run-tests` to execute test commands and see their effect on the journal.
      ;; In this case, the last command is a `:watch` for the word 'understand'. We know
      ;; based on the test-input that this should be the last word to appear in the output
      ;; so when it shows up, we can start to make assertions against the journal

      (let [{:keys [results journal]} (->> (concat
                                            (write-lines)
                                            (watch-for-output "understand"))
                                           (jd.test/run-test machine))
            wc (fn [word]
                 ;; A simple helper to extract the latest value from the word-count ktable
                 ;; as observed by the test-consumer.
                 ;;
                 ;; The journal collects all records as a vector of maps representing
                 ;; ConsumerRecords for each topic. Since we're inspecting a mutating
                 ;; table, we want to get the `last` matching record for `word`.
                 (->> (get-in journal [:topics :output])
                      (filter (fn [r]
                                (= (:key r) word)))
                      last
                      :value))]

        (is (every? #(= :ok (:status %)) results))

        (is (= 1 (wc "understand")))
        (is (= 2 (wc "i")))
        (is (= 3 (wc "to")))))))
