(ns kafka.test.fs
  (:require
   [clojure.java.io :as io]))

(defn tmp-dir [& parts]
  (let [gen-dir (fn [root]
                  (apply io/file root "embedded-kafka" parts))]
    (-> (System/getProperty "java.io.tmpdir")
        (gen-dir)
        (.getPath))))

(defn try-delete! [dir]
  (try
    (doseq [f (file-seq dir)]
      (.delete f))
    (.delete dir)
    (catch Exception e
      (println "failed to delete logs: " e))))
