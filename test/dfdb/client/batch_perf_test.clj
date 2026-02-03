(ns dfdb.client.batch-perf-test
  "Performance test to investigate progressively slower batch transactions"
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]
            [dfdb.client.http :as http]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(defn generate-batch
  "Generate a batch of n entities with a unique prefix"
  [batch-num n]
  (let [base-id (* batch-num 1000000)]
    (vec (for [i (range n)]
           {:db/id (+ base-id i)
            :perf/batch batch-num
            :perf/index i
            :perf/name (str "Entity-" batch-num "-" i)
            :perf/value (* i 100)}))))

(deftest test-batch-timing
  (when (server-running?)
    (testing "Measure timing of successive identical batches"
      (let [conn (dfdb/connect :base-url test-server-url)
            batch-size 100
            num-batches 10
            timings (atom [])]

        (println "\nBatch timing test: " batch-size "entities per batch," num-batches "batches")
        (println "Batch | Time (ms)")
        (println "------|----------")

        (doseq [batch-num (range num-batches)]
          (let [batch (generate-batch batch-num batch-size)
                start (System/nanoTime)
                _ (dfdb/transact! conn batch)
                end (System/nanoTime)
                duration-ms (/ (- end start) 1000000.0)]
            (swap! timings conj duration-ms)
            (printf "%5d | %8.2f%n" batch-num duration-ms)
            (flush)))

        (let [times @timings
              first-half (take (/ num-batches 2) times)
              second-half (drop (/ num-batches 2) times)
              avg-first (/ (reduce + first-half) (count first-half))
              avg-second (/ (reduce + second-half) (count second-half))
              slowdown-ratio (/ avg-second avg-first)]

          (println "\nSummary:")
          (printf "  First %d batches avg: %.2f ms%n" (count first-half) avg-first)
          (printf "  Last %d batches avg:  %.2f ms%n" (count second-half) avg-second)
          (printf "  Slowdown ratio: %.2fx%n" slowdown-ratio)

          ;; Flag if there's significant slowdown (> 1.5x)
          (when (> slowdown-ratio 1.5)
            (println "\n*** WARNING: Significant slowdown detected! ***")))))))

(deftest test-transit-encoding-isolation
  (when (server-running?)
    (testing "Check if Transit encoding is getting slower"
      (let [batch-size 100
            num-batches 10
            timings (atom [])]

        (println "\nTransit encoding timing test")
        (println "Batch | Encode Time (ms)")
        (println "------|------------------")

        (doseq [batch-num (range num-batches)]
          (let [batch (generate-batch batch-num batch-size)
                ;; We can't directly call transit-encode since it's private
                ;; But we can measure the full request cycle
                request-body {:tx-data batch}
                start (System/nanoTime)
                ;; Encode to transit
                out (java.io.ByteArrayOutputStream.)
                _ (cognitect.transit/write
                   (cognitect.transit/writer out :json) request-body)
                _ (.toString out "UTF-8")
                end (System/nanoTime)
                duration-ms (/ (- end start) 1000000.0)]
            (swap! timings conj duration-ms)
            (printf "%5d | %8.4f%n" batch-num duration-ms)
            (flush)))

        (let [times @timings
              first-half (take (/ num-batches 2) times)
              second-half (drop (/ num-batches 2) times)]
          (printf "%nFirst half avg: %.4f ms%n" (/ (reduce + first-half) (count first-half)))
          (printf "Second half avg: %.4f ms%n" (/ (reduce + second-half) (count second-half))))))))

(deftest test-reused-vs-fresh-connection
  (when (server-running?)
    (testing "Compare reused connection vs fresh connection per batch"
      (let [batch-size 100
            num-batches 5]

        (println "\nReused connection timing:")
        (let [conn (dfdb/connect :base-url test-server-url)
              reused-times (for [batch-num (range num-batches)]
                             (let [batch (generate-batch (+ 100 batch-num) batch-size)
                                   start (System/nanoTime)
                                   _ (dfdb/transact! conn batch)
                                   end (System/nanoTime)]
                               (/ (- end start) 1000000.0)))]
          (doseq [[i t] (map-indexed vector reused-times)]
            (printf "  Batch %d: %.2f ms%n" i t)))

        (println "\nFresh connection per batch timing:")
        (let [fresh-times (for [batch-num (range num-batches)]
                            (let [conn (dfdb/connect :base-url test-server-url)
                                  batch (generate-batch (+ 200 batch-num) batch-size)
                                  start (System/nanoTime)
                                  _ (dfdb/transact! conn batch)
                                  end (System/nanoTime)]
                              (/ (- end start) 1000000.0)))]
          (doseq [[i t] (map-indexed vector fresh-times)]
            (printf "  Batch %d: %.2f ms%n" i t)))))))

(deftest test-single-connection-many-small-batches
  (when (server-running?)
    (testing "Many small batches on single connection"
      (let [conn (dfdb/connect :base-url test-server-url)
            batch-size 10
            num-batches 50
            timings (atom [])]

        (println "\nSmall batch test:" batch-size "entities," num-batches "batches")

        (doseq [batch-num (range num-batches)]
          (let [batch (generate-batch (+ 300 batch-num) batch-size)
                start (System/nanoTime)
                _ (dfdb/transact! conn batch)
                end (System/nanoTime)
                duration-ms (/ (- end start) 1000000.0)]
            (swap! timings conj duration-ms)))

        (let [times @timings
              quartiles (partition-all (/ num-batches 4) times)]
          (doseq [[i q] (map-indexed vector quartiles)]
            (printf "Q%d avg: %.2f ms%n" (inc i) (/ (reduce + q) (count q)))))))))
