(ns jackdaw.test.journal-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [jackdaw.test.journal :as jrnl]))

(set! *warn-on-reflection* false)

(deftest journal-access-tests
  (let [j {:topics {"foo" [{:key 1
                            :value {:id 1 :v 99}}
                           {:key 2
                            :value {:id 2 :v 88}}
                           {:key 3
                            :value {:id 3 :v 77}}]}}]

    (testing "by-key"
      (is (= {:id 1 :v 99} ((jrnl/by-key "foo" [:id] 1) j)))
      (is (= {:id 2 :v 88} ((jrnl/by-key "foo" [:v] 88) j))))

    (testing "by-keys"
      (is (= [{:id 1 :v 99} {:id 2 :v 88}]
             ((jrnl/by-keys "foo" [:id] [1 2]) j))))

    (testing "by-id"
      (is (= {:id 2 :v 88} ((jrnl/by-id "foo" 2) j))))

    (testing "all-keys-present"
      (is ((jrnl/all-keys-present "foo" [:id] [1 2]) j))
      (is (false? ((jrnl/all-keys-present "foo" [:id] [1 2 4]) j))))))
