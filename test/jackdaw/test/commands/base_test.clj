(ns jackdaw.test.commands.base-test
  (:require
   [jackdaw.test.commands.base :as cmd]
   [clojure.pprint :as pprint]
   [clojure.test :refer :all]))

(deftest test-base-commands
  (testing "stop"
    (is ((cmd/command-map :stop) {})))

  (testing "sleep"
    (let [start (System/currentTimeMillis)
          _ ((cmd/command-map :sleep) {} [1000])
          end (System/currentTimeMillis)]
      (is (<= 1000 (- end start)))))

  (testing "print"
    (is (= "yolo\n"
           (with-out-str ((cmd/command-map :println) {} ["yolo"])))))

  (testing "pprint"
    (let [d {:a-very-long-key-name-in-a-map
             "with some value which is a string which makes sure pprint will add newlines"
             :b 2 :c [1 2 3]}
          r (with-out-str (pprint/pprint d))]
      (is (= r
             (with-out-str ((cmd/command-map :pprint) {} d))))))

  (testing "do"
    (let [journal (agent {})
          machine {:journal journal}
          do-fn (fn [j]
                  (is (= j @journal)))]
      ((cmd/command-map :do) machine [do-fn])))

  (testing "do!"
    (let [journal (agent {})
          machine {:journal journal}
          do-fn (fn [j]
                  (is (= j journal)))]
      ((cmd/command-map :do!) machine [do-fn])))

  (testing "inspect"
    (let [journal (agent {})
          machine {:journal journal}
          do-fn (fn [m]
                  (is (= m machine)))]
      ((cmd/command-map :inspect) machine [do-fn]))))
