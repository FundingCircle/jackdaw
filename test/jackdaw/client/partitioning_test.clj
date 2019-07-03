(ns jackdaw.client-test
  (:require
   [clojure.test :refer :all]
   [jackdaw.client.partitioning :as part]))

(deftest test-record-key->key-fn
  (let [test-key-fn (fn [key-str]
                      (-> (part/record-key->key-fn {:record-key key-str})
                          :jackdaw.client.partitioning/key-fn))]

    (testing "dollar prefix"
      (is (= 42 ((test-key-fn "$.foo") {:foo 42}))))

    (testing "hyphenated"
      (is (= 42 ((test-key-fn "foo_bar") {:foo-bar 42}))))

    (testing "dotted"
      (is (= 42 ((test-key-fn "foo.bar") {:foo {:bar 42}}))))))
