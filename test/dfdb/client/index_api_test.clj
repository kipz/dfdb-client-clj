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

(deftest seek-datoms-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          base (+ 900000 (rand-int 90000))
          attr (keyword "seekapi" (str "name" base))]

      (dfdb/transact! conn [{:db/id (+ base 1) attr "Alice"}
                            {:db/id (+ base 2) attr "Bob"}])

      (testing "seek runs on past the components it starts at"
        (let [sought (dfdb/seek-datoms conn :aevt :components [attr])
              bounded (dfdb/datoms conn :aevt :components [attr])]
          (is (>= (count (:datoms sought)) (count (:datoms bounded)))
              (str "a seek cannot see fewer datoms than the bounded scan, got " sought))
          (is (= attr (:a (first (:datoms sought))))
              (str "the seek should start at " attr ", got " sought))))

      (testing "a limit bounds the seek"
        (let [result (dfdb/seek-datoms conn :aevt :components [attr] :limit 1)]
          (is (= 1 (count (:datoms result)))
              (str "expected one datom, got " result)))))))

(deftest ident-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          eid (+ 900000 (rand-int 90000))
          name-kw (keyword "identapi" (str "thing" eid))]

      (dfdb/transact! conn [{:db/id eid :db/ident name-kw}])

      (testing "ident names the entity entid resolves"
        (is (= eid (dfdb/entid conn name-kw)))
        (is (= (str name-kw) (str (dfdb/ident conn eid)))))

      (testing "an entity with no :db/ident has no name"
        (is (= "" (str (dfdb/ident conn (+ eid 1)))))))))

(deftest stats-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          eid (+ 900000 (rand-int 90000))
          attr (keyword "statsapi" (str "name" eid))]

      (dfdb/transact! conn [{:db/id eid attr "Alice"}])

      (testing "db-stats counts the database value"
        (let [result (dfdb/db-stats conn)]
          (is (pos? (:datoms result)) (str "expected datoms, got " result))
          (is (pos? (:attributes result)) (str "expected attributes, got " result))
          (is (pos? (:basis-t result)) (str "expected a basis, got " result))))

      (testing "db-stats reads at a basis"
        (let [now (:basis-t (dfdb/db-stats conn))
              before (dfdb/db-stats conn :as-of {:db/tx 1})]
          (is (<= (:basis-t before) now)
              (str "an as-of read cannot be ahead of the current basis, got " before))))

      (testing "stats reports the optimizer's cache"
        (let [result (dfdb/stats conn)]
          (is (some? (:total-datoms result)) (str "expected a datom count, got " result))
          (is (sequential? (:attributes result))
              (str "expected per-attribute statistics, got " result)))))))
