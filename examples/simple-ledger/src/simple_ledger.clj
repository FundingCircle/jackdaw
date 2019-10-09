(ns simple-ledger
  "This example creates a simple accounting ledger using transducers."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.xform :as jxf]
            [jackdaw.streams.xform.fakes :as fakes]))


(defn split-entries
  [_ _]
  (map (fn [[k {:keys [debit-account credit-account amount] :as entry}]]
         [[debit-account
           {:account-name debit-account
            :debit-credit-indicator :dr
            :amount amount}]
          [credit-account
           {:account-name credit-account
            :debit-credit-indicator :cr
            :amount amount}]])))

(defn next-balances
  [starting-balances {:keys [account-name debit-credit-indicator amount]
                      :as transaction}]
  (let [op (if (= :dr debit-credit-indicator) - +)]
    (update starting-balances account-name (fnil op 0) amount)))

(defn running-balances
  [state swap-fn]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (let [[k v] input
             {:keys [account-name debit-credit-indicator amount] :as entry} v
             next (as-> entry %
                    (swap-fn state next-balances %)
                    (select-keys % [account-name])
                    ((juxt (comp first keys) (comp first vals)) %)
                    (zipmap [:account-name :after-balance] %)
                    (assoc % :before-balance (if (= :dr debit-credit-indicator)
                                                 (+ amount (:after-balance %))
                                                 (- amount (:after-balance %))))
                    (vector k %)
                    (vector %))]
         (rf result next))))))


(comment
  ;; Use this comment block to explore the Simple Ledger using Clojure
  ;; transducers.

  ;; Launch a Clojure REPL:
  ;; ```
  ;; cd <path-to-jackdaw>/examples/simple-ledger
  ;; clj -A:dev
  ;; ```

  ;; Emacs users: Open a project file, e.g. this one, and enter
  ;; `M-x cider-jack-in`.

  ;; Evaluate the form:
  (def coll
    [["1" {:debit-account "tech"
           :credit-account "cash"
           :amount 1000}]
     ["2" {:debit-account "cash"
           :credit-account "sales"
           :amount 2000}]])

  ;; Let's record the entries. Evaluate the form:
  (->> coll
       (transduce (split-entries nil nil) concat)
       (transduce (running-balances (atom {}) swap!) concat))

  ;; You should see output like the following:

  ;; (["tech"
  ;;   {:account-name "tech"
  ;;    :before-balance 0
  ;;    :after-balance -1000}]
  ;;  ["cash"
  ;;   {:account-name "cash"
  ;;    :before-balance 0
  ;;    :after-balance 1000}]
  ;;  ["cash"
  ;;   {:account-name "cash"
  ;;    :before-balance 1000
  ;;    :after-balance -1000}]
  ;;  ["sales"
  ;;   {:account-name "sales"
  ;;    :before-balance 0
  ;;    :after-balance 2000}])


  ;; This time, let's count the words using
  ;; `jackdaw.streams.xform.fakes/fake-kv-store` which implements the
  ;; KeyValueStore interface with overrides for get and put."

  ;; Evaluate the form:
  (->> coll
       (transduce (split-entries nil nil) concat)
       (transduce (running-balances (fakes/fake-kv-store {})
                                    jxf/kv-store-swap-fn) concat))

  ;; You should see the same output.
  )


(def streams-config
  {"application.id" "simple-ledger"
   "bootstrap.servers" (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092")
   "cache.max.bytes.buffering" "0"})

(defn topology-builder
  [{:keys [entry-pending
           transaction-pending
           transaction-added] :as topics} xforms]
  (fn [builder]
    (jxf/add-state-store! builder)
    (-> (j/kstream builder entry-pending)
        (jxf/transduce-kstream (::split-entries xforms))
        (j/through transaction-pending)
        (jxf/transduce-kstream (::running-balances xforms))
        (j/to transaction-added))
    builder))


(comment
  ;; Use this comment block to explore the Simple Ledger as a stream
  ;; processing application.

  ;; For more details on dynamic development, see the comment block in
  ;; <path-to-jackdaw>/examples/word-count/src/word_count.clj

  ;; Start ZooKeeper and Kafka:
  ;; ```
  ;; <path-to-directory>/bin/confluent local start kafka
  ;; ```

  ;; Launch a Clojure REPL:
  ;; ```
  ;; cd <path-to-jackdaw>/examples/simple-ledger
  ;; clj -A:dev
  ;; ```

  ;; Emacs users: Open a project file, e.g. this one, and enter
  ;; `M-x cider-jack-in`.

  ;; Evaluate the form:
  (reset)

  ;; Evaluate the form:
  (let [coll [{:debit-account "tech"
               :credit-account "cash"
               :amount 1000}
              {:debit-account "cash"
               :credit-account "sales"
               :amount 2000}]]
    (doseq [x coll]
      (publish (:entry-pending topic-metadata) nil x)))

  ;; Evaluate the form:
  (get-keyvals (:transaction-added topic-metadata))

  ;; You should see output like the following. Notice transaction
  ;; order is not preserved:

  ;; (["sales"
  ;;   {:account-name "sales"
  ;;    :before-balance 0
  ;;    :after-balance 2000}]
  ;;  ["tech"
  ;;   {:account-name "tech"
  ;;    :before-balance 0
  ;;    :after-balance -1000}]
  ;;  ["cash"
  ;;   {:account-name "cash"
  ;;    :before-balance 0
  ;;    :after-balance 1000}]
  ;;  ["cash"
  ;;   {:account-name "cash"
  ;;    :before-balance 1000
  ;;    :after-balance -1000}])


  ;; The `transaction-added` topic has 15 partitions. Let's see how
  ;; the records distributed. Evaluate the form:
  (->> (get-records (:transaction-added topic-metadata))
       (map (fn [x]
              (select-keys x [:key :offset :partition :value]))))

  ;; You should see output like the following. The balances are spread
  ;; across partitions 0, 11, and 14. Transaction order is preserved
  ;; only for each account. There is no global order.

  ;; ({:key "sales"
  ;;   :offset 0
  ;;   :partition 0
  ;;   :value
  ;;   {:account-name "sales"
  ;;    :before-balance 0
  ;;    :after-balance 2000}}
  ;;  {:key "tech"
  ;;   :offset 0
  ;;   :partition 11
  ;;   :value
  ;;   {:account-name "tech"
  ;;    :before-balance 0
  ;;    :after-balance -1000}}
  ;;  {:key "cash"
  ;;   :offset 0
  ;;   :partition 14
  ;;   :value
  ;;   {:account-name "cash"
  ;;    :before-balance 0
  ;;    :after-balance 1000}}
  ;;  {:key "cash"
  ;;   :offset 1
  ;;   :partition 14
  ;;   :value
  ;;   {:account-name "cash"
  ;;    :before-balance 1000
  ;;    :after-balance -1000}})
  )
