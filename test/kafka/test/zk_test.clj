(ns kafka.test.zk-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [kafka.test.zk :as zk]
   [com.stuartsierra.component :as component])
  (:import
   (org.I0Itec.zkclient ZkClient ZkConnection)
   (org.apache.zookeeper.server ZooKeeperServer)))

(def zk-config {"zookeeper.connect" "localhost:32181"})

(defn call-with-zk [f config]
  (let [zk (atom (zk/server config))]
    (try
      (swap! zk component/start)
      (finally
        (swap! zk component/stop)))))

(defmacro with-zk [config & body]
  `(call-with-zk (fn []
                   ~@body) ~config))

(deftest zookeeper-tests
  (testing "zookeeper port"
    (is (= 32181 (zk/port "localhost:32181"))))

  (testing "zk lifecycle"
    (let [zk (atom (zk/server zk-config))]

      (is (= zk-config (:config @zk)))

        (testing "start!"
          (swap! zk component/start)
          (is (instance? ZooKeeperServer (:zk @zk)))
          (is (:factory @zk))
          (is (d/deferred? (:thread @zk))))

        (testing "acquire client connection"
          (let [client (zk/client zk-config)]
            (try
              (is (instance? ZkClient client))
              (finally
                (.close client)))))

        (testing "stop!"
          (swap! zk component/stop)
          (is (d/realized? (:thread @zk)))
          (is (nil? (:zk @zk)))
          (is (nil? (:factory @zk)))))))
