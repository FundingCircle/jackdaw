(ns kafka.client-test
  (:require [clojure.test :refer :all]
            [kafka.client :as client]))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(def consumer-config
  {"bootstrap.servers"     "localhost:9092"
   "group.id"              "test"
   "key.deserializer"      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"    "org.apache.kafka.common.serialization.StringDeserializer"
   "metadata.max.age.ms"   "1000" ;; usually this is 5 minutes
   "auto.offset.reset"     "earliest"
   "enable.auto.commit"    "true"})

(deftest callback-test
  (testing "producer callbacks"
    (testing "success"
      (let [result (promise)
            cb (client/callback (fn [meta ex]
                                  (if ex
                                    (deliver result ex)
                                    (deliver result :ok))))]

        (.onCompletion cb nil nil)
        (is (= :ok @result ))))

    (testing "failure"
      (let [result (promise)
            cb (client/callback (fn [meta ex]
                                  (if ex
                                    (deliver result ex)
                                    (deliver result :ok))))
            ex (Exception. "failed write :-(")]
        (.onCompletion cb nil ex)
        (is (= ex @result))))))

(deftest select-methods-test
  (testing "object methods"
    (let [o (Object.)]
      (is (= {:getClass Object}
             (client/select-methods o [:getClass]))))))
