(ns dfdb.client.subscription-test
  "Integration tests for subscription API with materialized views"
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(defn with-connection [f]
  (if (server-running?)
    (f)
    (do
      (println "\nWARNING: dfdb-go server not running on" test-server-url)
      (println "Start the server with: scripts/start-server.sh\n"))))

(use-fixtures :once with-connection)

;; ============================================================================
;; Subscription CRUD Tests
;; ============================================================================

(deftest test-subscription-create
  (when (server-running?)
    (testing "Create a subscription"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; First add some data
            _ (dfdb/transact! conn
                              [{:db/id 1001 :person/name "Alice" :person/age 30}
                               {:db/id 1002 :person/name "Bob" :person/age 25}
                               {:db/id 1003 :person/name "Charlie" :person/age 35}])
            ;; Create subscription
            sub (dfdb/create-subscription conn "test-people"
                                          "[:find ?e ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]")]
        (is (some? (:id sub)) "Subscription should have an ID")
        (is (= "test-people" (:name sub)) "Subscription name should match")
        (is (:active sub) "Subscription should be active")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-subscription-list
  (when (server-running?)
    (testing "List subscriptions"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Create a subscription
            sub (dfdb/create-subscription conn "list-test"
                                          "[:find ?e :where [?e :person/name _]]")
            ;; List subscriptions
            result (dfdb/list-subscriptions conn)]
        (is (vector? (:subscriptions result)) "Should return subscriptions vector")
        (is (some #(= (:id sub) (:id %)) (:subscriptions result))
            "Created subscription should be in list")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-subscription-get
  (when (server-running?)
    (testing "Get subscription by ID"
      (let [conn (dfdb/connect :base-url test-server-url)
            sub (dfdb/create-subscription conn "get-test"
                                          "[:find ?e :where [?e :person/name _]]")
            fetched (dfdb/get-subscription conn (:id sub))]
        (is (= (:id sub) (:id fetched)) "IDs should match")
        (is (= "get-test" (:name fetched)) "Names should match")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-subscription-update
  (when (server-running?)
    (testing "Update subscription query"
      (let [conn (dfdb/connect :base-url test-server-url)
            sub (dfdb/create-subscription conn "update-test"
                                          "[:find ?e :where [?e :person/name _]]")
            updated (dfdb/update-subscription conn (:id sub)
                                              "[:find ?e ?age :where [?e :person/age ?age]]")]
        (is (= (:id sub) (:id updated)) "ID should remain the same")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-subscription-delete
  (when (server-running?)
    (testing "Delete subscription"
      (let [conn (dfdb/connect :base-url test-server-url)
            sub (dfdb/create-subscription conn "delete-test"
                                          "[:find ?e :where [?e :person/name _]]")
            _ (dfdb/delete-subscription conn (:id sub))]
        ;; Verify it's deleted by trying to get it
        (is (thrown? Exception (dfdb/get-subscription conn (:id sub)))
            "Getting deleted subscription should throw")))))

;; ============================================================================
;; Materialized View Query Tests
;; ============================================================================

(deftest test-query-view-data-before-subscription
  (when (server-running?)
    (testing "Subscription sees data that existed before subscription was created"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Add test data FIRST
            _ (dfdb/transact! conn
                              [{:db/id 2001 :item/name "Apple" :item/price 100}
                               {:db/id 2002 :item/name "Banana" :item/price 50}
                               {:db/id 2003 :item/name "Cherry" :item/price 150}])
            ;; Create subscription AFTER data exists
            sub (dfdb/create-subscription conn "items-view-before"
                                          "[:find ?e ?name ?price :where [?e :item/name ?name] [?e :item/price ?price]]")
            ;; Query the view - should see pre-existing data
            result (dfdb/query-view conn (:id sub))]
        (is (some? (:results result)) "Should have results")
        (is (>= (count (:results result)) 3) "Should have at least 3 items from pre-existing data")
        (is (some? (:total result)) "Should have total count")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-query-view-data-after-subscription
  (when (server-running?)
    (testing "Subscription sees data added after subscription was created"
      ;; Use unique namespace with timestamp to avoid test pollution
      (let [unique-ns (str "after-test-" (System/currentTimeMillis))
            name-attr (keyword unique-ns "name")
            price-attr (keyword unique-ns "price")
            conn (dfdb/connect :base-url test-server-url)
            ;; Create subscription FIRST with no matching data
            query-str (str "[:find ?e ?name ?price :where "
                           "[?e :" unique-ns "/name ?name] "
                           "[?e :" unique-ns "/price ?price]]")
            sub (dfdb/create-subscription conn "items-view-after" query-str)
            ;; Verify initially empty
            initial-result (dfdb/query-view conn (:id sub))
            _ (is (= 0 (count (:results initial-result))) "Should start with no results")
            ;; Add test data AFTER subscription exists
            _ (dfdb/transact! conn
                              [{:db/id -1 name-attr "Gizmo1" price-attr 100}
                               {:db/id -2 name-attr "Gizmo2" price-attr 200}])
            ;; Query the view - should see newly added data
            result (dfdb/query-view conn (:id sub))]
        (is (some? (:results result)) "Should have results")
        (is (= 2 (count (:results result))) "Should have 2 items from newly added data")
        ;; Verify actual values
        (let [prices (set (map :?price (:results result)))]
          (is (= #{100 200} prices) "Should have correct prices"))
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-query-view-with-filter
  (when (server-running?)
    (testing "Query view with filter"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Add test data
            _ (dfdb/transact! conn
                              [{:db/id 3001 :product/name "Widget" :product/price 100}
                               {:db/id 3002 :product/name "Gadget" :product/price 200}
                               {:db/id 3003 :product/name "Gizmo" :product/price 300}])
            ;; Create subscription
            sub (dfdb/create-subscription conn "products-view"
                                          "[:find ?e ?name ?price :where [?e :product/name ?name] [?e :product/price ?price]]")
            ;; Query with filter: price > 150
            result (dfdb/query-view conn (:id sub)
                                    :filter {"?price" {">" 150}})]
        (is (some? (:results result)) "Should have results")
        ;; All results should have price > 150
        (doseq [r (:results result)]
          (is (> (:?price r) 150) "All prices should be > 150"))
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-query-view-with-sort
  (when (server-running?)
    (testing "Query view with sorting"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Add test data
            _ (dfdb/transact! conn
                              [{:db/id 4001 :fruit/name "Apple" :fruit/quantity 10}
                               {:db/id 4002 :fruit/name "Banana" :fruit/quantity 5}
                               {:db/id 4003 :fruit/name "Cherry" :fruit/quantity 20}])
            ;; Create subscription
            sub (dfdb/create-subscription conn "fruits-view"
                                          "[:find ?e ?name ?qty :where [?e :fruit/name ?name] [?e :fruit/quantity ?qty]]")
            ;; Query with descending sort
            result (dfdb/query-view conn (:id sub)
                                    :sort ["-?qty"])]
        (is (some? (:results result)) "Should have results")
        (when (>= (count (:results result)) 2)
          (let [quantities (map :?qty (:results result))]
            ;; Check descending order
            (is (apply >= (filter some? quantities))
                "Results should be sorted by quantity descending")))
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-query-view-with-pagination
  (when (server-running?)
    (testing "Query view with pagination"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Add test data
            _ (dfdb/transact! conn
                              [{:db/id 5001 :book/title "Book A"}
                               {:db/id 5002 :book/title "Book B"}
                               {:db/id 5003 :book/title "Book C"}
                               {:db/id 5004 :book/title "Book D"}
                               {:db/id 5005 :book/title "Book E"}])
            ;; Create subscription
            sub (dfdb/create-subscription conn "books-view"
                                          "[:find ?e ?title :where [?e :book/title ?title]]")
            ;; Query with limit
            result1 (dfdb/query-view conn (:id sub) :limit 2)
            ;; Query with offset
            result2 (dfdb/query-view conn (:id sub) :limit 2 :offset 2)]
        (is (<= (count (:results result1)) 2) "First page should have at most 2 results")
        (is (<= (count (:results result2)) 2) "Second page should have at most 2 results")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

(deftest test-query-view-combined-options
  (when (server-running?)
    (testing "Query view with filter, sort, and pagination combined"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Add test data
            _ (dfdb/transact! conn
                              [{:db/id 6001 :employee/name "Alice" :employee/salary 50000}
                               {:db/id 6002 :employee/name "Bob" :employee/salary 60000}
                               {:db/id 6003 :employee/name "Charlie" :employee/salary 70000}
                               {:db/id 6004 :employee/name "Diana" :employee/salary 80000}
                               {:db/id 6005 :employee/name "Eve" :employee/salary 55000}])
            ;; Create subscription
            sub (dfdb/create-subscription conn "employees-view"
                                          "[:find ?e ?name ?salary :where [?e :employee/name ?name] [?e :employee/salary ?salary]]")
            ;; Query: salary >= 55000, sorted descending, limit 3
            result (dfdb/query-view conn (:id sub)
                                    :filter {"?salary" {">=" 55000}}
                                    :sort ["-?salary"]
                                    :limit 3)]
        (is (some? (:results result)) "Should have results")
        (is (<= (count (:results result)) 3) "Should respect limit")
        ;; Clean up
        (dfdb/delete-subscription conn (:id sub))))))

;; ============================================================================
;; Error Handling Tests
;; ============================================================================

(deftest test-get-nonexistent-subscription
  (when (server-running?)
    (testing "Getting nonexistent subscription throws"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (is (thrown? Exception (dfdb/get-subscription conn "nonexistent-id"))
            "Should throw for nonexistent subscription")))))

(deftest test-query-view-nonexistent-subscription
  (when (server-running?)
    (testing "Querying view of nonexistent subscription throws"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (is (thrown? Exception (dfdb/query-view conn "nonexistent-id"))
            "Should throw for nonexistent subscription")))))
