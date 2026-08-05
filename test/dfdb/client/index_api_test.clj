(ns dfdb.client.index-api-test
  "The index and entity API: datoms, index-range, tx-range, entid, attribute.

  These reads had no client functions, so asking for the datoms of an attribute
  or the entity behind an ident meant writing a query that approximated it, and
  the transaction log could not be read at all."
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(deftest index-api-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          base (+ 900000 (rand-int 90000))
          attr (keyword "idxapi" (str "name" base))
          age  (keyword "idxapi" (str "age" base))]

      (dfdb/transact! conn [{:db/id (+ base 1) attr "Alice" age 30}
                            {:db/id (+ base 2) attr "Bob"   age 25}])

      (testing "datoms of an attribute"
        (let [result (dfdb/datoms conn :aevt :components [attr])]
          (is (= 2 (count (:datoms result)))
              (str "expected both datoms of " attr ", got " result))))

      (testing "index-range over values"
        (let [result (dfdb/index-range conn age :start 26 :end 40)]
          (is (= 1 (count (:datoms result)))
              (str "expected the one age in [26,40), got " result))))

      (testing "tx-range reads the log"
        (let [result (dfdb/tx-range conn 0 100000)]
          (is (pos? (count (:transactions result)))
              (str "expected transactions, got " result))))

      (testing "attribute reports what is known"
        ;; Transit round-trips the name back as a keyword, not a string.
        (let [result (dfdb/attribute conn age)]
          (is (= age (:name result))
              (str "expected the attribute's own name, got " result))
          (is (= 2 (:datoms result))
              (str "expected two datoms for " age ", got " result)))))))
