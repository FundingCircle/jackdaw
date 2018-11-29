(ns simple-ledger-test
  (:require [simple-ledger :as sut]
            [jackdaw.streams.mock :as jsm]
            [clojure.test :refer :all]))


(deftest build-topology-unit-test
  (testing "simple ledger unit test"
    (let [driver (jsm/build-driver sut/build-topology)
          publish (partial jsm/publish driver)
          get-keyvals (partial jsm/get-keyvals driver)]

      (publish (sut/topic-config "ledger-entries-requested")
               nil
               {:id (java.util.UUID/randomUUID)
                :entries [{:debit-account-name "foo"
                           :credit-account-name "bar"
                           :amount 2}
                          {:debit-account-name "foo"
                           :credit-account-name "qux"
                           :amount 3}]})

      (publish (sut/topic-config "ledger-entries-requested")
               nil
               {:id (java.util.UUID/randomUUID)
                :entries [{:debit-account-name "foo"
                           :credit-account-name "bar"
                           :amount 5}
                          {:debit-account-name "foo"
                           :credit-account-name "qux"
                           :amount 7}]})

      (let [keyvals (get-keyvals
                     (sut/topic-config "ledger-transaction-added"))]

        (is (= [-2 -3 -5 -7]
               (->> (filter (fn [[k _]] (= "foo" k)) keyvals)
                    (map second)
                    (map :amount))))

        (is (= [0 -2 -5 -10]
               (->> (filter (fn [[k _]] (= "foo" k)) keyvals)
                    (map second)
                    (map :starting-balance))))

        (is (= [-2 -5 -10 -17]
               (->> (filter (fn [[k _]] (= "foo" k)) keyvals)
                    (map second)
                    (map :current-balance))))

        (is (= [2 5]
               (->> (filter (fn [[k _]] (= "bar" k)) keyvals)
                    (map second)
                    (map :amount))))

        (is (= [0 2]
               (->> (filter (fn [[k _]] (= "bar" k)) keyvals)
                    (map second)
                    (map :starting-balance))))

        (is (= [2 7]
               (->> (filter (fn [[k _]] (= "bar" k)) keyvals)
                    (map second)
                    (map :current-balance))))))))
