(ns jackdaw.serdes-test
  (:require [clojure.test :refer :all]
            [jackdaw.serdes :as serdes]))

(deftest lookup-serde

  (testing "String serde from config"
    (let [serde (serdes/serde {:topic-name "stringy"
                               :jackdaw.serdes/type :jackdaw.serdes/string})]
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde (.getClass serde)))))

  (testing "String serde from keyword"
    (let [serde (serdes/serde :jackdaw.serdes/string)]
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde (.getClass serde)))))

  (testing "Unset serde"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Failed to find a legal mapping"
                          (serdes/serde {:topic-name "no-serde"}))))

  (testing "Unknown serde from config"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unable to resolve serde for topic"
                          (serdes/serde {:topic-name "bad-serde"
                                         :jackdaw.serdes/type :unknown}))))

  (testing "Unknown serde from keyword"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unable to resolve serde for topic"
                          (serdes/serde :unknown))))

  (testing "My Custom serde"
    ;; its not a proper serde (an instance of org.apache.kafka.common.serialization.Serde)
    ;; but it tests the machanics and clojure lets you get away with it
    (let [serde (serdes/serde (merge serdes/+default-serdes+
                                     {:blackhole-sun (fn [conf]
                                                       (fn [v]
                                                         (str v (:blackhole-serde-config conf))))})
                              {:topic-name "stringy"
                               :blackhole-serde-config " got spaghettified!"
                               :jackdaw.serdes/type :blackhole-sun})]
      (is (fn? serde))
      (is (= "Matt got spaghettified!" (serde "Matt"))))))


(deftest resolve-serde

  (testing "Resolving string serde for key and value"
    (let [serdefied (serdes/resolve {:topic-name "stringy"
                                     :key-serde :jackdaw.serdes/string
                                     :value-serde :jackdaw.serdes/string})]
      (is (map? serdefied))
      ;; things that shouldn't change don't change
      (is (= "stringy" (:topic-name serdefied)))
      ;; key and value serde keys are overwritten with the resolved impls
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde
             (.getClass (:key-serde serdefied))))
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde
             (.getClass (:value-serde serdefied))))))

  (testing "Resolving string key and ByteArray value serdes"
    (let [serdefied (serdes/resolve {:topic-name "stringy"
                                     :key-serde :jackdaw.serdes/string
                                     :value-serde :jackdaw.serdes/byte-array})]
      (is (map? serdefied))
      ;; things that shouldn't change don't change
      (is (= "stringy" (:topic-name serdefied)))
      ;; key and value serde keys are overwritten with the resolved impls
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde
             (.getClass (:key-serde serdefied))))
      (is (= org.apache.kafka.common.serialization.Serdes$ByteArraySerde
             (.getClass (:value-serde serdefied))))))

  (testing "No serde specified"
    (is (thrown-with-msg? IllegalArgumentException
                          #":key-serde is required in the topic config"
                          (serdes/resolve {})))
    (is (thrown-with-msg? IllegalArgumentException
                          #":value-serde is required in the topic config"
                          (serdes/resolve {:key-serde :jackdaw.serdes/string})))))
