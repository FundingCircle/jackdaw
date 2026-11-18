(ns streams.core-test
  (:require
    [clj-uuid :as uuid]
    [clojure.test :refer :all]
    [jackdaw.test.journal :as j]
    [streams.core :as core]
    [streams.framework.integration-test :as itc]))

(deftest props-for-test
  (testing "Empty map to java Properties"
    (let [p (core/props-for {})]
      (is (= java.util.Properties (type p)))
      (is (.isEmpty p))))
  (testing "Config map to java Properties"
    (let [p (core/props-for {:name-one 42
                             :name_two "foo"
                             "string-key" true
                             "key_string" 0
                             :foo.bar :baz
                             "hello.there" 12})]
      (is (= java.util.Properties (type p)))
      (is (not (.isEmpty p)))

      (is (= "42" (.getProperty p "name.one")))
      (is (= "foo" (.getProperty p "name_two")))
      (is (= "true" (.getProperty p "string.key")))
      (is (= "0" (.getProperty p "key_string")))
      (is (= ":baz" (.getProperty p "foo.bar")))
      (is (= "12" (.getProperty p "hello.there")))
      (is (nil? (.getProperty p "not.found"))))))

(deftest journal-test
  (let [j (itc/slurp-journal "test/resources/journal.edn")]
    (testing "Journal messages by key"
      (let [mi (map :value (itc/msg-for-key j :input "key-1"))
            mo (map :value (itc/msg-for-key j :output "key-2"))]
        (is (= [1 2 "+"] mi))
        (is (= [[3] [4 3] [7]] mo))))
    (testing "Journal watchers"
      (is (true? ((itc/watch-msg-count :input 6) j)))
      (is (false? ((itc/watch-msg-count :input 5) j)))
      (is (true? ((itc/watch-msg-count-for-key :output "key-2" 3) j)))
      (is (false? ((itc/watch-msg-count-for-key :output "key-2" 4) j))))))
