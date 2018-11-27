(ns simple-ledger
  "This tutorial contains a simple stream processing application using
  Jackdaw and Kafka Streams.

  It begins with Pipe which is then extended using an interactive
  workflow. The result is a simple ledger."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :refer [info]]
            [clj-uuid :as uuid]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import [org.apache.kafka.common.serialization Serdes]))


;;; Topic Configuration
;;;

(defn topic-config
  "Takes a topic name and (optionally) a key and value serde and
  returns a topic configuration map, which may be used to create a
  topic or produce/consume records."
  ([topic-name]
   (topic-config topic-name (jse/serde) (jse/serde)))

  ([topic-name key-serde value-serde]
   {:topic-name topic-name
    :partition-count 1
    :replication-factor 1
    :topic-config {}
    :key-serde key-serde
    :value-serde value-serde}))


;;; App Template
;;;

(defn app-config
  "Returns the application config."
  []
  {"application.id" "simple-ledger"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  "Returns a topology builder.

  WARNING: This is just a stub. Before publishing to the input topic,
  evaluate one of the `build-topology` functions in the comment forms
  below."
  [builder]
  (-> (j/kstream builder (topic-config "ledger-entries-requested"))
      (j/peek (fn [[k v]]
                (info (str {:key k :value v}))))
      (j/to (topic-config "ledger-transaction-added")))
  builder)

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "simple-ledger is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "simple-ledger is down"))


(defn -main
  [& _]
  (start-app (app-config)))


(comment
  ;;; Start
  ;;;

  ;; Needed to invoke the forms from this namespace. When typing
  ;; directly in the REPL, skip this step.
  (require '[user :refer :all :exclude [topic-config]])


  ;; Start ZooKeeper and Kafka.
  ;; This requires the Confluent Platform CLI which may be obtained
  ;; from `https://www.confluent.io/download/`. If ZooKeeper and Kafka
  ;; are already running, skip this step.
  (confluent/start)


  ;; Create the topics, and start the app.
  (start)


  ;; Write to the input stream.
  (publish (topic-config "ledger-entries-requested")
           nil
           "this is a pipe")


  ;; Read from the output stream.
  (get-keyvals (topic-config "ledger-transaction-added"))


  )


(comment
  ;;; Add input validation
  ;;;

  (do (s/def ::ledger-entries-requested-value
        (s/keys :req-un [::id
                         ::entries]))

      (s/def ::id uuid?)
      (s/def ::entries (s/+ ::entry))

      (s/def ::entry
        (s/and (s/keys :req-un [::debit-account-name
                                ::credit-account-name
                                ::amount])
               #(not= (:debit-account-name %)
                      (:credit-account-name %))))

      (s/def ::debit-account-name string?)
      (s/def ::credit-account-name string?)
      (s/def ::amount pos-int?))


  (defn valid-input?
    [[_ v]]
    (s/valid? ::ledger-entries-requested-value v))


  (defn log-bad-input
    [[k v]]
    (info (str "Bad input: " {:key k :value v})))


  (defn build-topology
    [builder]
    (let [input (j/kstream builder
                           (topic-config "ledger-entries-requested"))

          [valid invalid] (j/branch input [valid-input?
                                           (constantly true)])

          _ (j/peek invalid log-bad-input)]

      (-> valid
          (j/to (topic-config "ledger-transaction-added")))

      builder))


  ;; Reset the app.
  (reset)


  ;; Write valid input.
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream.
  (get-keyvals (topic-config "ledger-transaction-added"))


  ;; Write invalid input.
  (publish (topic-config "ledger-entries-requested")
           nil
           "this is not a pipe")


  ;; Read from the output stream.
  (get-keyvals (topic-config "ledger-transaction-added"))


  )


(comment
  ;;; Split `ledger-entries-requested` events into ledger
  ;;; transactions (aka debits and credits).
  ;;;

  (defn entry-sides
    [{:keys [debit-account-name
             credit-account-name
             amount]}]
    [[debit-account-name
      {:account-name debit-account-name
       :amount (- amount)}]
     [credit-account-name
      {:account-name credit-account-name
       :amount amount}]])


  (defn entries->transactions
    [[_ v]]
    (reduce #(concat %1 (entry-sides %2)) [] (:entries v)))


  (defn build-topology
    [builder]
    (let [input (j/kstream builder
                           (topic-config "ledger-entries-requested"))

          [valid invalid] (j/branch input [valid-input?
                                           (constantly true)])

          _ (j/peek invalid log-bad-input)

          transactions (j/flat-map valid entries->transactions)]

      (-> transactions
          (j/to (topic-config "ledger-transaction-added")))

      builder))


  ;; Reset the app.
  (reset)


  ;; Write valid input.
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream.
  (get-keyvals (topic-config "ledger-transaction-added"))


  ;; Write valid input (again).
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream (again).
  (get-keyvals (topic-config "ledger-transaction-added"))


  )


(comment
  ;;; Add unique identifiers and reference IDs.
  ;;;

  (defn entries->transactions
    [[_ v]]
    (->> (:entries v)
         (reduce #(concat %1 (entry-sides %2)) [])
         (map-indexed #(assoc-in %2 [1 :id] (uuid/v5 (:id v) %1)))
         (map #(assoc-in %1 [1 :causation-id] (:id v)))))


  ;; Reset the app.
  (reset)


  ;; Write valid input.
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream.
  (get-keyvals (topic-config "ledger-transaction-added"))


  ;; Write valid input (again).
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream (again).
  (get-keyvals (topic-config "ledger-transaction-added"))


  ;; Read from the output stream ("foo" only).
  (->> (get-keyvals (topic-config "ledger-transaction-added"))
       (filter (fn [[k v]] (= "foo" k))))


  )


(comment
  ;;; Keep track of running balances.
  ;;;

  (defn account-balance-reducer
    [x y]
    (let [starting-balance (:current-balance x)]
      (merge y {:starting-balance starting-balance
                :current-balance (+ starting-balance (:amount y))})))


  (defn build-topology
    [builder]
    (let [input (j/kstream builder
                           (topic-config "ledger-entries-requested"))

          [valid invalid] (j/branch input [valid-input?
                                           (constantly true)])

          _ (j/peek invalid log-bad-input)

          transactions (j/flat-map valid entries->transactions)

          balances
          (-> transactions
              (j/map-values (fn [v]
                              (merge v
                                     {:starting-balance 0
                                      :current-balance (:amount v)})))
              (j/group-by-key (topic-config nil (Serdes/String)
                                            (j.s.edn/serde)))
              (j/reduce account-balance-reducer
                        (topic-config "balances")))]

      (-> balances
          (j/to-kstream)
          (j/to (topic-config "ledger-transaction-added")))

      builder))


  ;; Reset the app.
  (reset)


  ;; Write valid input.
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream.
  (get-keyvals (topic-config "ledger-transaction-added"))


  ;; Write valid input (again).
  (publish (topic-config "ledger-entries-requested")
           nil
           {:id (java.util.UUID/randomUUID)
            :entries [{:debit-account-name "foo"
                       :credit-account-name "bar"
                       :amount 10}
                      {:debit-account-name "foo"
                       :credit-account-name "qux"
                       :amount 20}]})


  ;; Read from the output stream (again).
  (get-keyvals (topic-config "ledger-transaction-added"))


  ;; Read from the output stream ("foo" only).
  (->> (get-keyvals (topic-config "ledger-transaction-added"))
       (filter (fn [[k v]] (= "foo" k))))


  )
