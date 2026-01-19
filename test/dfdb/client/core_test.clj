(ns dfdb.client.core-test
  "Integration tests for dfdb-client-clj

  These tests require a running dfdb-go server on localhost:8080.
  To run the server:
    cd /Users/james.carnegie/scm/dfdb-go
    go run cmd/server/main.go"
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]))

(def test-server-url "http://localhost:8081")

(defn server-running?
  "Check if the dfdb server is running"
  []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _
      false)))

(defn with-connection [f]
  (when-not (server-running?)
    (println "\nWARNING: dfdb-go server not running on" test-server-url)
    (println "Skipping integration tests. Start the server with:")
    (println "  cd /Users/james.carnegie/scm/dfdb-go")
    (println "  go run cmd/server/main.go\n"))
  (f))

(use-fixtures :once with-connection)

(deftest test-health-check
  (when (server-running?)
    (testing "Health check returns ok status"
      (let [conn (dfdb/connect :base-url test-server-url)
            result (dfdb/health conn)]
        (is (= "ok" (:status result)))
        (is (number? (:time result)))))))

(deftest test-transaction-map-notation
  (when (server-running?)
    (testing "Transaction with map notation"
      (let [conn (dfdb/connect :base-url test-server-url)
            result (dfdb/transact! conn [{:db/id 1
                                          :user/name "Alice"
                                          :user/age 30}])]
        (is (number? (:tx-id result)))
        (is (number? (:tx-time result)))
        (is (vector? (:deltas result)))))))

(deftest test-transaction-tuple-notation
  (when (server-running?)
    (testing "Transaction with tuple notation"
      (let [conn (dfdb/connect :base-url test-server-url)
            result (dfdb/transact! conn [[:db/add 2 :user/name "Bob"]
                                         [:db/add 2 :user/age 25]])]
        (is (number? (:tx-id result)))
        (is (number? (:tx-time result)))))))

(deftest test-basic-query
  (when (server-running?)
    (testing "Basic query returns bindings"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; First add some data
        (dfdb/transact! conn [{:db/id 3 :user/name "Charlie" :user/age 35}])

        ;; Query for the data
        (let [result (dfdb/query conn '[:find ?name
                                        :where [?e :user/name ?name]])]
          (is (vector? (:bindings result)))
          (is (some #{"Charlie"} (map :?name (:bindings result)))))))))

(deftest test-query-with-parameters
  (when (server-running?)
    (testing "Query with parameters"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add test data
        (dfdb/transact! conn [{:db/id 4 :user/name "David" :user/age 40}
                              {:db/id 5 :user/name "Eve" :user/age 20}])

        ;; Query with parameter
        (let [result (dfdb/query conn
                                 '[:find ?name ?age
                                   :in $ ?min-age
                                   :where
                                   [?e :user/name ?name]
                                   [?e :user/age ?age]
                                   [(>= ?age ?min-age)]]
                                 :params {"?min-age" 35})]
          (is (vector? (:bindings result)))
          ;; Should find David (40) but not Eve (20)
          (is (some #{"David"} (map :?name (:bindings result)))))))))

(deftest test-entity-lookup-with-pull
  (when (server-running?)
    (testing "Entity lookup via pull pattern"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add entity with unique email
        (dfdb/transact! conn [{:db/id 6
                               :user/name "Frank"
                               :user/age 45
                               :user/email "frank-unique@example.com"}])

        ;; Look up entity by email and pull
        (let [result (dfdb/query conn
                                 '[:find (pull ?e [:user/name :user/age])
                                   :where [?e :user/email "frank-unique@example.com"]])]
          (is (vector? (:bindings result)))
          ;; Pull result is under the variable name (:?e in this case)
          (let [entity (-> result :bindings first :?e)]
            (is (= "Frank" (:user/name entity)))
            (is (= 45 (:user/age entity)))))))))

(deftest test-pull-pattern-query
  (when (server-running?)
    (testing "Query with pull pattern"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add entity
        (dfdb/transact! conn [{:db/id 7
                               :user/name "Grace"
                               :user/age 28
                               :user/email "grace@example.com"}])

        ;; Query with pull pattern
        (let [result (dfdb/query conn
                                 '[:find (pull ?e [:user/name :user/age])
                                   :where [?e :user/email "grace@example.com"]])]
          (is (vector? (:bindings result)))
          ;; Pull result is under the variable name (:?e in this case)
          (let [entity (-> result :bindings first :?e)]
            (is (= "Grace" (:user/name entity)))
            (is (= 28 (:user/age entity)))))))))

(deftest test-entity-helper-function
  (when (server-running?)
    (testing "Entity helper function via pull"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add entity with unique ID and name
        (dfdb/transact! conn [{:db/id 80008
                               :user/name "HenryUnique"
                               :user/age 50}])

        ;; Use pull to get all attributes
        (let [result (dfdb/query conn '[:find (pull ?e [*])
                                        :where [?e :user/name "HenryUnique"]])
              entity (-> result :bindings first :?e)]
          (is (map? entity))
          (is (= "HenryUnique" (:user/name entity)))
          (is (= 50 (:user/age entity))))))))

(deftest test-pull-helper-function
  (when (server-running?)
    (testing "Pull helper function with specific attributes"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add entity with unique ID and email
        (dfdb/transact! conn [{:db/id 90009
                               :user/name "IvyUnique"
                               :user/age 33
                               :user/email "ivy-unique-test@example.com"}])

        ;; Pull only specific attributes
        (let [result (dfdb/query conn '[:find (pull ?e [:user/name :user/age])
                                        :where [?e :user/email "ivy-unique-test@example.com"]])
              entity (-> result :bindings first :?e)]
          (is (map? entity))
          (is (= "IvyUnique" (:user/name entity)))
          (is (= 33 (:user/age entity)))
          ;; Email should not be included in pull result
          (is (nil? (:user/email entity))))))))

(deftest test-transaction-with-time-dimensions
  (when (server-running?)
    (testing "Transaction with time dimensions"
      (let [conn (dfdb/connect :base-url test-server-url)
            valid-time 1000
            result (dfdb/transact! conn
                                   [{:db/id 10 :user/name "Jack"}]
                                   :time-dimensions {:time/valid valid-time})]
        (is (number? (:tx-id result)))
        (is (number? (:tx-time result)))
        ;; Check that deltas include time dimensions
        (when (seq (:deltas result))
          (is (map? (:dimensions (first (:deltas result))))))))))

(deftest test-temp-id-resolution
  (when (server-running?)
    (testing "Temporary ID resolution"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Use negative ID as temp ID
            result (dfdb/transact! conn [{:db/id -1
                                          :user/name "Kelly"
                                          :user/age 27}])]
        (is (map? (:temp-id-map result)))
        (is (number? (:tx-id result)))))))

(deftest test-get-all-entity-attributes
  (when (server-running?)
    (testing "Get all attributes for an entity"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add entity with multiple attributes
        (dfdb/transact! conn [{:db/id 11
                               :user/name "Liam"
                               :user/age 42
                               :user/email "liam@example.com"}])

        ;; Get all attributes
        (let [result (dfdb/query conn
                                 '[:find ?a ?v
                                   :in $ ?e
                                   :where [?e ?a ?v]]
                                 :params {"?e" 11})]
          (is (vector? (:bindings result)))
          (is (> (count (:bindings result)) 0))
          ;; Should have bindings with ?a and ?v
          (is (every? #(and (:?a %) (:?v %)) (:bindings result))))))))

(deftest test-connection-timeout
  (when (server-running?)
    (testing "Connection with custom timeout"
      (let [conn (dfdb/connect :base-url test-server-url
                               :timeout 5000
                               :max-retries 1)]
        (is (= 5000 (:timeout conn)))
        (is (= 1 (:max-retries conn)))

        ;; Should still work
        (let [result (dfdb/health conn)]
          (is (= "ok" (:status result))))))))
