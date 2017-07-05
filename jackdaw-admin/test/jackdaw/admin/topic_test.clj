(ns jackdaw.admin.topic-test
  (:require [clojure.test :refer :all]
            [jackdaw.admin
             [config :as config]
             [fixture :as fixture]
             [topic :as topic]
             [zk :as zk]])
  (:import kafka.admin.AdminUtils
           kafka.server.ConfigType))

(fixture/kafka)

(defn build-topics-metadata
   "Receives Vector of topic names and returns topic.metadata for the topics"
   [topic-names]
   (let [topic-name-key :topic.metadata/name
         metadata       {:topic.metadata/status             :pre-release
                         :topic.metadata/record-key         :unique-key
                         :topic.metadata/value-serde        :kafka.serdes/json
                         :topic.metadata/partitions         1
                         :topic.metadata/version            2
                         :topic.metadata/replication-factor 1
                         :topic.metadata/config             {:topic.metadata/cleanup.policy "compact"}
                         :topic.metadata/value-schema-name  "loan-unkeyed-pre-release"
                         :topic.metadata/type               :topic.metadata/table
                         :topic.metadata/unique-key         "id"
                         :topic.metadata/key-serde          :kafka.serdes/string}]
     (map (fn [topic-name]
            (merge {topic-name-key topic-name} metadata)) topic-names)))

(deftest create-test
  (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
    (let [topic-name (str (java.util.UUID/randomUUID))
          create-result (topic/create! zk-utils topic-name 1 1 {})]
      (testing "returns a topic name"
        (is (= topic-name create-result)))
      (testing "creates a kafka topic"
        (is (true? (topic/exists? zk-utils topic-name)))))))

(deftest delete-test
  (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
    (let [topic-name (str (java.util.UUID/randomUUID))]
      (topic/create! zk-utils topic-name 1 1 {})
      (testing "returns a topic name"
        (is (= topic-name (topic/delete! zk-utils topic-name))))
      (testing "deletes a topic within 10 tries, 1s apart"
        (let [tries (atom 10)]
          (while (and (topic/exists? zk-utils topic-name)
                      (pos? @tries))
            (Thread/sleep 1000)
            (topic/delete! zk-utils topic-name)
            (swap! tries dec)))
        (is (false? (topic/exists? zk-utils topic-name)))))))

 (deftest exists?-test
   (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
     (let [topic-name (str (java.util.UUID/randomUUID))
           _ (topic/create! zk-utils topic-name 1 1 {})]
       (is (true? (topic/exists? zk-utils topic-name))))))

 (deftest retry-exists?-test
   (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
     (let [topic-name (str (java.util.UUID/randomUUID))]
       (testing "returns false if topic does not exists"
         (is (false? (topic/retry-exists? zk-utils topic-name))))
       (testing "returns true if topic does exists"
         (topic/create! zk-utils topic-name 1 1 {})
         (is (true? (topic/retry-exists? zk-utils topic-name)))))))

 (deftest create-topics!-test
   (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
     (let [topic-names (map str [(java.util.UUID/randomUUID) (java.util.UUID/randomUUID)])
           topics-metadata (build-topics-metadata topic-names)
           cleanup-policy (-> topics-metadata
                              first
                              :topic.metadata/config
                              :topic.metadata/cleanup.policy)]
       (testing "creates new topics"
         (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
           (topic/create-topics! zk-utils topics-metadata)
           (map (fn [topic-name]
                  (is (true? (topic/retry-exists? zk-utils topic-name))))
                topic-names)))
       (testing "creates topic with config"
         (is (= cleanup-policy
                (get (AdminUtils/fetchEntityConfig zk-utils (ConfigType/Topic) (first topic-names)) "cleanup.policy")))))))

 (deftest fetch-topic-config-test
   (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
     (let [config {"cleanup.policy" "compact"}
           topic-name (str (java.util.UUID/randomUUID))]
       (testing "returns topic config"
         (topic/create! zk-utils topic-name 1 1 config)
         (topic/retry-exists? zk-utils topic-name)
         (is (= config (topic/fetch-config zk-utils topic-name)))))))

(deftest change-config-test
  (testing "without topic metadata"
    (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
      (let [config {"cleanup.policy" "compact"}
            topic-name (str (java.util.UUID/randomUUID))]
        (topic/create! zk-utils topic-name 1 1 {})
        (topic/change-config! zk-utils topic-name config)
        (is (= config (AdminUtils/fetchEntityConfig zk-utils (ConfigType/Topic) topic-name))))))
  (testing "with topic metadata"
    (with-open [zk-utils (zk/zk-utils (:connect-string config/common))]
      (let [topic-name (str (java.util.UUID/randomUUID))
            config {"cleanup.policy" "compact"}
            metadata (first (build-topics-metadata [topic-name]))]
        (topic/create! zk-utils topic-name 1 1 {})
        (topic/change-config! zk-utils metadata)
        (is (= config (AdminUtils/fetchEntityConfig zk-utils (ConfigType/Topic) topic-name)))))))
