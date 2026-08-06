(ns dfdb.client.filters-test
  "Reading through a named filter.

  A filter is a predicate over datoms, and a predicate is a closure, so it
  cannot cross the wire. The server registers filters by name and a request
  chooses among them — which also means a client cannot invent one."
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(deftest filters-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn  (dfdb/connect :base-url test-server-url)
          base  (+ 800000 (rand-int 90000))
          nm    (keyword "filt" (str "name" base))
          secret (keyword "filt" (str "secret" base))
          alice (+ base 1)]

      (dfdb/transact! conn [{:db/id alice nm "Alice" secret "hunter2"}])

      (testing "without a filter both attributes are readable"
        (let [e (dfdb/entity conn alice)]
          (is (= "Alice" (get e nm)))
          (is (= "hunter2" (get e secret)))))

      (testing "attr-not-in hides an attribute from entity"
        (let [e (dfdb/entity conn alice :filters [[:attr-not-in (str secret)]])]
          (is (= "Alice" (get e nm)) (str "expected the name to survive, got " e))
          (is (nil? (get e secret))
              (str "the filtered attribute should not be readable, got " e))))

      (testing "attr-not-in hides an attribute from pull"
        (let [r (dfdb/pull conn [nm secret] alice
                           :filters [[:attr-not-in (str secret)]])]
          (is (= "Alice" (get r nm)))
          (is (nil? (get r secret)) (str "expected no secret, got " r))))

      (testing "a query reads through the filter"
        (let [r (dfdb/query conn
                            [:find '?v :where ['?e secret '?v]]
                            :filters [[:attr-not-in (str secret)]])]
          (is (empty? (:bindings r))
              (str "the filtered attribute should match nothing, got " r))))

      (testing "an unregistered filter is refused, not ignored"
        (is (thrown? Exception
                     (dfdb/entity conn alice :filters [[:no-such-filter]]))
            "naming a filter the server does not have must fail loudly")))))
