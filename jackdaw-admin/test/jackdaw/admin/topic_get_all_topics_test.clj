(ns jackdaw.admin.topic-get-all-topics-test
  (:require [clojure.test :refer :all]
            [jackdaw.admin.config :as config]
            [jackdaw.admin.fixture :as fixture]
            [jackdaw.admin.topic :as topic]
            [jackdaw.admin.zk :as zk]))

(fixture/kafka)

(deftest fetch-topic-config-test
  (with-open [zk-utils (zk/zk-utils (get config/common "zookeeper.connect"))]
    (let [config {"cleanup.policy" "compact"}
          topic-name (str (java.util.UUID/randomUUID))]
      (testing "returns topic config"
        (topic/create! zk-utils topic-name 1 1 config)
        (topic/retry-exists? zk-utils topic-name)
        (is (= config (topic/fetch-config zk-utils topic-name)))))))

