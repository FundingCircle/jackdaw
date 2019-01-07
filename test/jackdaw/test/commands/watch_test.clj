(ns jackdaw.test.commands.watch-test
  (:require
   [clojure.core.async :as async]
   [jackdaw.test.commands.watch :as watch]
   [clojure.test :refer :all]))

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
        result-ch (async/thread
                    (watch/handle-watch-cmd machine :jackdaw.test.commands/watch! [watcher]))]

    (doseq [cmd (butlast cmd-list)]
      (send journal conj cmd)
      (let [result (async/poll! result-ch)]
        (is (nil? result) result)))
    
    (send journal conj (last cmd-list))

    (async/<!! result-ch)))

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
