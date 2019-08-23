(ns jackdaw.test.commands-test
  (:require
   [clojure.test :refer :all]
   [jackdaw.test.commands :as cmd]))

(deftest test-command-handler
  (testing "input cmd and params added into result"
    (let [test-cmd [:stop]]
      (doseq [k [:cmd :params]]
        (is (contains? (-> (cmd/command-handler {} test-cmd)
                           keys
                           set)
                       k)))))

  (testing "Passing an unknown command throws an exception"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown command: :not-found"
                          (cmd/command-handler {} [:not-found])))))
