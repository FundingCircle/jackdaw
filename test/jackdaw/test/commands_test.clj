(ns jackdaw.test.commands-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer :all]
   [jackdaw.test.commands :as cmd]))

(set! *warn-on-reflection* false)

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

(defn valid-command?
  [cmd]
  (s/valid? ::cmd/test-event cmd))

(deftest test-command-specs
  (testing "base commands"
    (is (not (valid-command? [:yolo])))
    (is (valid-command? (cmd/do #(println "yolo" %))))
    (is (valid-command? (cmd/do! #(println "yolo" %))))
    (is (valid-command? [:stop]))
    (is (valid-command? [:println "yolo"]))
    (is (valid-command? [:pprint {:foo "yolo"}]))
    (is (valid-command? [:sleep 420])))

  (testing "write commands"
    (is (valid-command? (cmd/write! :foo {:id 1 :payload "yolo"})))

    (is (valid-command? (cmd/write! :foo {:id 1 :payload "yolo"}
                                    {:key 1
                                     :partition 1})))

    (is (valid-command? (cmd/write! :foo {:id 1 :payload "yolo"}
                                    {:key-fn :id
                                     :partition-fn (constantly 1)})))

    (is (not (valid-command? [:write! :foo {:id 1 :payload "yolo"}
                              {:key-fn "not a fn"
                               :partition-fn "not a fn"}]))))

  (testing "watch commands"
    (is (valid-command? (cmd/watch #(= % :expected))))
    (is (valid-command? (cmd/watch #(= % :expected)
                                   {:info "error hint"})))
    (is (valid-command? (cmd/watch #(= % :expected)
                                   {:timeout 420})))

    (is (not (valid-command? [:watch #(= % :expected)
                              {:timeout "not an int"}])))
    (is (not (valid-command? [:watch #(= % :expected)
                              {:info :not-a-string}])))))
