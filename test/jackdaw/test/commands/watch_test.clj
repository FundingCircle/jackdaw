(ns jackdaw.test.commands.watch-test
  (:require
   [jackdaw.test.commands.watch :as watch]
   [manifold.deferred :as d]
   [clojure.test :refer [deftest testing is]]))

(set! *warn-on-reflection* false)

;; An example of the problem the watcher is trying to solve
;; might help.
;;
;; Lets say some process is adding messages representing
;; orders, each consisting of a pair (qty, amt). We want
;; to "watch" the journal until $100 of revenue has been
;; received and return immediately when it does.
;;
;; In the test, we submit the messages ourselves. We want
;; to assert that `handle-cmd!` only returns *after*
;; sufficient messages have been sent to satisfy the $100
;; revenue.

(defn run-watch-cmd [watcher cmd-list]
  (let [journal (agent [])
        machine {:journal journal}
        result-d (d/future (watch/handle-watch-cmd machine [watcher]))]

    (doseq [cmd (butlast cmd-list)]
      (send journal conj cmd)
      (is (not (d/realized? result-d))))

    (send journal conj (last cmd-list))

    @result-d))

(deftest test-watch-command
  (testing "watch for $100 revenue"
    (let [rev (fn [coll]
                (reduce + 0 (map (fn [[qty amt]]
                                   (* qty amt))
                                 coll)))]
      (run-watch-cmd (fn [j]
                       (<= 100 (rev j)))
                     [[2 10]
                      [5 10]
                      [3 10]
                      [1 10]]))))
