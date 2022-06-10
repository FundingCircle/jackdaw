(ns jackdaw.test.middleware-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.tools.logging :as log]
   [jackdaw.test.middleware :as middle]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.transports.identity]
   [jackdaw.test :as jd.test]))

(set! *warn-on-reflection* false)

(defn identity-transport
  []
  (trns/transport {:type :identity}))

(defn with-identity-transport
  [{:keys [test-id transport]} f]
  (with-open [machine (jd.test/test-machine (transport))]
    (when test-id (log/info "begin" test-id))
    (f machine)
    (when test-id (log/info "end" test-id))))


(deftest test-with-status
  (with-identity-transport {:test-id "test-with-status"
                            :transport #(identity-transport)}
    (fn [t]
      (let [status-for (fn [response]
                         (-> ((middle/with-status (fn [_ _]
                                                    response))
                              t response)
                             :status))]
        (testing "status = ok if a command is executed successfully"
          (is (= :ok
                 (status-for {:payload :ok})))
          (is (= :ok
                 (status-for {:payload :abc})))

          (testing "status = error if a command returns an :error key"
            (is (= :error
                   (status-for {:result {:error "Oh noes!"}})))))))))

(deftest test-with-timing
  (with-identity-transport {:test-id "test-with-timing"
                            :transport #(identity-transport)}
    (fn [t]
      (let [timing-for (fn [delay-ms]
                         (-> ((middle/with-timing (fn [_ _]
                                                    (Thread/sleep delay-ms)
                                                    {:payload :ok}))
                              t [:yolo 1 2])))]

        (testing "with-timing records duration"
          (is (<= 1000 (:duration (timing-for 1000)))))

        (testing "with-timing records started-at"
          (is (contains? (timing-for 0) :started-at)))

        (testing "with-timing records finished-at"
          (is (contains? (timing-for 0) :finished-at)))))))


(deftest test-journal-snapshots
  (with-identity-transport {:test-id "test-journal-snapshots"
                            :transport #(identity-transport)}
    (fn [t]
      (let [snapshots-for (fn [cmd]
                            ((middle/with-journal-snapshots (fn [_ _]
                                                              (send (:journal t) assoc-in [:topics (:topic cmd)] cmd)
                                                              (await (:journal t))))
                             t cmd))]

        (testing "with-journal-snapshots records journal-before"
          (is (= {:topics {}}
                 (:journal-before (snapshots-for {:topic :foo :value {:payload :abc}}))))

         (testing "with-journal-snapshots records journal-after"
           (is (= {:topics {:foo {:topic :foo
                                  :value {:payload :abc}}}}
                  (:journal-after (snapshots-for {:topic :foo :value {:payload :abc}}))))))))))
