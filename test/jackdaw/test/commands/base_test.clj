(ns jackdaw.test.commands.base-test
  (:require
   [jackdaw.test.commands.base :as cmd]
   [clojure.test :refer :all]))

(deftest test-base-commands
  (testing "stop"
    (is ((cmd/command-map ::cmd/stop) {})))

  (testing "sleep"
    (let [start (System/currentTimeMillis)
          _ ((cmd/command-map ::cmd/sleep) {} nil [1000])
          end (System/currentTimeMillis)]
      (is (<= 1000 (- end start)))))

  (testing "print"
    (is (= "yolo\n"
           (with-out-str ((cmd/command-map ::cmd/println) {} nil ["yolo"])))))

  (testing "do"
    (let [journal (agent {})
          machine {:journal journal}
          do-fn (fn [j]
                  (is (= j @journal)))]
      ((cmd/command-map ::cmd/do) machine nil [do-fn]))))

