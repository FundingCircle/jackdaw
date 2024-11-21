(ns streams.stack-calculator-test
  (:require
    [clj-uuid :as uuid]
    [clojure.test :refer :all]
    [jackdaw.test.journal :as j]
    [streams.stack-calculator :as sut]
    [streams.core :as app]
    [streams.framework.integration-test :as itc]))

;; alter this to run against a local kafka broker etc.
(reset! itc/test-machine-mode :mock)

(use-fixtures :once (itc/integration-fixture sut/build-stream
                                             (sut/configure-topology (app/load-config))))


(deftest stack-reducer-fn-test
  (testing "the stack calculator topologies reducer fn"
    (let [to-topic (fn [l]
                     (map (fn [v]
                            [:k v]) l))]
      (is (= [3 2 1] (reduce sut/stack-reducer [] (to-topic [1 2 3]))))
      (is (= [3] (reduce sut/stack-reducer [] (to-topic [1 2 "+"]))))
      (is (= [-2] (reduce sut/stack-reducer [] (to-topic [2 4 "-"]))))
      (is (= [8] (reduce sut/stack-reducer [] (to-topic [2 4 "*"]))))
      (is (= [1/2] (reduce sut/stack-reducer [] (to-topic [2 4 "/"]))))
      (is (= [6] (reduce sut/stack-reducer [] (to-topic [1 2 3 "+" "+"])))))))

(deftest calculation-test
  (testing "Test the stack calculator"
    (let [test-id (str (uuid/v4))
          {:keys [results journal]}
          (itc/run-test
           [[:write! :input 1 {:key test-id}]
            [:write! :input 2 {:key test-id}]
            [:write! :input "+" {:key test-id}]

            [:write! :input 3 {:key test-id}]
            [:write! :input 4 {:key test-id}]
            [:write! :input "+" {:key test-id}]

            [:write! :input "*" {:key test-id}]
            [:watch (itc/watch-msg-count-for-key :output test-id 7)]])]
      (is (itc/result-ok? results))
      (is (= [21] (-> journal
                    (j/messages :output)
                    (last)))))))

;; TODO more dynamic key mapping / counts
(deftest regression-test
  (testing "Assert the calculator using a journal from a previous run"
    (let [test-id-1 (str (uuid/v4))
          test-id-2 (str (uuid/v4))
          input-journal (itc/slurp-journal "test/resources/journal.edn")
          {:keys [results journal]}
          (itc/run-test
            ;; Build the input from the loaded journals input topic
            ;; alter the keys though so they are unique to this test run
            (concat
              (map (fn [v]
                     [:write! :input (:value v) {:key test-id-1}])
                   (itc/msg-for-key input-journal :input "key-1"))
              (map (fn [v]
                     [:write! :input (:value v) {:key test-id-2}])
                   (itc/msg-for-key input-journal :input "key-2"))
              [[:watch (itc/watch-msg-count-for-key :output test-id-1 3)]
               [:watch (itc/watch-msg-count-for-key :output test-id-2 3)]]))]
      (is (itc/result-ok? results))
      ;; assert the output is the same as the loaded journals output
      (is (= (map :value (itc/msg-for-key input-journal :output "key-1"))
             (map :value (itc/msg-for-key journal :output test-id-1))))
      (is (= (map :value (itc/msg-for-key input-journal :output "key-2"))
             (map :value (itc/msg-for-key journal :output test-id-2)))))))
