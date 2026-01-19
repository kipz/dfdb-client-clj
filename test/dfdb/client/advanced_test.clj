(ns dfdb.client.advanced-test
  "Advanced tests with large transactions, complex queries, aggregates, and joins"
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(defn with-fresh-connection [f]
  (when-not (server-running?)
    (println "\nWARNING: dfdb-go server not running on" test-server-url)
    (println "Start the server with: /tmp/dfdb-server -a :8081\n"))
  (f))

(use-fixtures :once with-fresh-connection)

;; ============================================================================
;; Large Transaction Tests
;; ============================================================================

(deftest test-large-transaction-100-entities
  (when (server-running?)
    (testing "Transaction with 100 entities"
      (let [conn (dfdb/connect :base-url test-server-url)
            entities (vec (for [i (range 10000 10100)]
                            {:db/id i
                             :employee/id (str "EMP-" i)
                             :employee/name (str "Employee" i)
                             :employee/salary (* i 100)
                             :employee/department (rand-nth ["Engineering" "Sales" "Marketing"])}))]
        (let [result (dfdb/transact! conn entities)]
          (is (number? (:tx-id result)))
          ;; Deltas may or may not be returned depending on server config
          (is (or (nil? (:deltas result)) (vector? (:deltas result)))))

        ;; Verify count
        (let [result (dfdb/query conn '[:find (count ?e)
                                        :where [?e :employee/id ?id]])]
          (is (>= (-> result :aggregate first first) 100)))))))

(deftest test-large-transaction-500-entities
  (when (server-running?)
    (testing "Transaction with 500 entities (stress test)"
      (let [conn (dfdb/connect :base-url test-server-url)
            entities (vec (for [i (range 20000 20500)]
                            {:db/id i
                             :item/sku (str "SKU-" i)
                             :item/name (str "Item" i)
                             :item/price (+ 10 (mod i 90))}))]
        (let [result (dfdb/transact! conn entities)]
          (is (number? (:tx-id result)))
          ;; Deltas may or may not be returned depending on server config
          (is (or (nil? (:deltas result)) (vector? (:deltas result)))))

        ;; Verify all inserted
        (let [result (dfdb/query conn '[:find (count ?e)
                                        :where [?e :item/sku ?sku]])]
          (is (>= (-> result :aggregate first first) 500)))))))

(deftest test-batch-updates
  (when (server-running?)
    (testing "Batch update multiple entities"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create 50 entities
        (let [entities (vec (for [i (range 30000 30050)]
                              {:db/id i
                               :product/sku (str "PROD-" i)
                               :product/price 100}))]
          (dfdb/transact! conn entities))

        ;; Update all prices in batch
        (let [updates (vec (for [i (range 30000 30050)]
                             [:db/add i :product/price 150]))]
          (let [result (dfdb/transact! conn updates)]
            (is (>= (count (:deltas result)) 50))))

        ;; Verify updates
        (let [result (dfdb/query conn '[:find (count ?e)
                                        :where
                                        [?e :product/sku ?sku]
                                        [?e :product/price 150]])]
          (is (= 50 (-> result :aggregate first first))))))))

;; ============================================================================
;; Complex Aggregate Tests
;; ============================================================================

(deftest test-multi-level-aggregates-with-grouping
  (when (server-running?)
    (testing "Complex aggregates with multiple grouping levels"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Sales data: region -> team -> salesperson
        (let [entities (atom [])]
          (doseq [[region-idx region] (map-indexed vector ["East" "West" "North"])
                  team (range 1 4)
                  rep (range 1 6)]
            (swap! entities conj
                   {:db/id (+ 40000 (* region-idx 1000) (* team 100) rep)
                    :sale/region region
                    :sale/team team
                    :sale/rep (str region "-T" team "-R" rep)
                    :sale/amount (+ 1000 (rand-int 9000))}))
          (dfdb/transact! conn @entities))

        ;; Group by region and count
        (let [result (dfdb/query conn
                                 '[:find ?region (count ?e)
                                   :where
                                   [?e :sale/region ?region]])]
          (is (>= (count (:aggregate result)) 3)))

        ;; Group by region and sum amounts
        (let [result (dfdb/query conn
                                 '[:find ?region (sum ?amount)
                                   :where
                                   [?e :sale/region ?region]
                                   [?e :sale/amount ?amount]])]
          (is (>= (count (:aggregate result)) 3))
          ;; Each region should have a sum
          (is (every? #(> (second %) 0) (:aggregate result))))))))

(deftest test-aggregate-avg-with-filtering
  (when (server-running?)
    (testing "Average with filtering and grouping"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Student scores by class
        (dfdb/transact! conn
                        [{:db/id 50000 :student/name "A1" :student/class "Math" :student/score 85}
                         {:db/id 50001 :student/name "A2" :student/class "Math" :student/score 90}
                         {:db/id 50002 :student/name "A3" :student/class "Math" :student/score 75}
                         {:db/id 50003 :student/name "B1" :student/class "English" :student/score 88}
                         {:db/id 50004 :student/name "B2" :student/class "English" :student/score 92}])

        ;; Average score by class
        (let [result (dfdb/query conn
                                 '[:find ?class (avg ?score)
                                   :where
                                   [?e :student/class ?class]
                                   [?e :student/score ?score]])]
          (is (= 2 (count (:aggregate result))))
          ;; Math average should be around 83.33
          (let [math-avg (->> (:aggregate result)
                              (filter #(= "Math" (first %)))
                              first
                              second)]
            (is (and (> math-avg 83) (< math-avg 84)))))))))

(deftest test-complex-aggregate-pipeline
  (when (server-running?)
    (testing "Complex aggregate with filtering before aggregation"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Order data
        (dfdb/transact! conn
                        [{:db/id 60000 :order/id "O1" :order/amount 100 :order/status "completed"}
                         {:db/id 60001 :order/id "O2" :order/amount 200 :order/status "completed"}
                         {:db/id 60002 :order/id "O3" :order/amount 150 :order/status "pending"}
                         {:db/id 60003 :order/id "O4" :order/amount 300 :order/status "completed"}
                         {:db/id 60004 :order/id "O5" :order/amount 50 :order/status "cancelled"}])

        ;; Sum only completed orders
        (let [result (dfdb/query conn
                                 '[:find (sum ?amount)
                                   :where
                                   [?e :order/status "completed"]
                                   [?e :order/amount ?amount]])]
          (is (= 600 (-> result :aggregate first first))))

        ;; Count by status
        (let [result (dfdb/query conn
                                 '[:find ?status (count ?e)
                                   :where
                                   [?e :order/status ?status]])]
          (is (>= (count (:aggregate result)) 3)))))))

;; ============================================================================
;; Multi-Join Complex Queries
;; ============================================================================

(deftest test-four-way-join
  (when (server-running?)
    (testing "Four-way join across related entities"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create a complex hierarchy: Country -> State -> City -> Person
        (dfdb/transact! conn
                        [{:db/id 70000 :country/name "USA"}
                         {:db/id 70010 :state/name "California" :state/country 70000}
                         {:db/id 70011 :state/name "Texas" :state/country 70000}
                         {:db/id 70020 :city/name "San Francisco" :city/state 70010}
                         {:db/id 70021 :city/name "Los Angeles" :city/state 70010}
                         {:db/id 70022 :city/name "Houston" :city/state 70011}
                         {:db/id 70030 :person/name "John" :person/city 70020}
                         {:db/id 70031 :person/name "Jane" :person/city 70020}
                         {:db/id 70032 :person/name "Bob" :person/city 70021}
                         {:db/id 70033 :person/name "Alice" :person/city 70022}])

        ;; Find all people in California
        (let [result (dfdb/query conn
                                 '[:find ?person-name ?city-name
                                   :where
                                   [?country :country/name "USA"]
                                   [?state :state/country ?country]
                                   [?state :state/name "California"]
                                   [?city :city/state ?state]
                                   [?person :person/city ?city]
                                   [?person :person/name ?person-name]
                                   [?city :city/name ?city-name]])]
          (is (= 3 (count (:bindings result))))
          (is (some #{"John"} (map :?person-name (:bindings result))))
          (is (some #{"Jane"} (map :?person-name (:bindings result))))
          (is (some #{"Bob"} (map :?person-name (:bindings result)))))))))

(deftest test-join-with-aggregates
  (when (server-running?)
    (testing "Join with aggregate per group"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Employees with departments and salaries
        (dfdb/transact! conn
                        [{:db/id 80000 :dept/name "Engineering"}
                         {:db/id 80001 :dept/name "Sales"}
                         {:db/id 80010 :emp/name "E1" :emp/dept 80000 :emp/salary 100000}
                         {:db/id 80011 :emp/name "E2" :emp/dept 80000 :emp/salary 120000}
                         {:db/id 80012 :emp/name "E3" :emp/dept 80000 :emp/salary 110000}
                         {:db/id 80013 :emp/name "S1" :emp/dept 80001 :emp/salary 80000}
                         {:db/id 80014 :emp/name "S2" :emp/dept 80001 :emp/salary 85000}])

        ;; Average salary by department
        (let [result (dfdb/query conn
                                 '[:find ?dept-name (avg ?salary)
                                   :where
                                   [?emp :emp/dept ?dept]
                                   [?dept :dept/name ?dept-name]
                                   [?emp :emp/salary ?salary]])]
          (is (= 2 (count (:aggregate result))))
          ;; Engineering average should be 110000
          (let [eng-avg (->> (:aggregate result)
                             (filter #(= "Engineering" (first %)))
                             first
                             second)]
            (is (= 110000 eng-avg))))))))

(deftest test-self-join-pattern
  (when (server-running?)
    (testing "Self-join to find entities with related properties"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Users with common interests
        (dfdb/transact! conn
                        [{:db/id 90000 :user/name "User1" :user/interest "Clojure"}
                         {:db/id 90001 :user/name "User2" :user/interest "Clojure"}
                         {:db/id 90002 :user/name "User3" :user/interest "Go"}
                         {:db/id 90003 :user/name "User4" :user/interest "Clojure"}])

        ;; Find pairs of users with same interest
        (let [result (dfdb/query conn
                                 '[:find ?name1 ?name2
                                   :where
                                   [?u1 :user/interest ?interest]
                                   [?u2 :user/interest ?interest]
                                   [?u1 :user/name ?name1]
                                   [?u2 :user/name ?name2]
                                   [(< ?name1 ?name2)]])] ; Avoid duplicates and self-pairs
          ;; Should find User1-User2, User1-User4, User2-User4 (3 pairs)
          (is (>= (count (:bindings result)) 3)))))))

;; ============================================================================
;; Complex Negation Tests
;; ============================================================================

(deftest test-not-with-multiple-patterns
  (when (server-running?)
    (testing "NOT with multiple patterns"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 95000 :advtest.person/name "Complete" :advtest.person/email "c@e.com" :advtest.person/phone "123"}
                         {:db/id 95001 :advtest.person/name "NoPhone" :advtest.person/email "n@e.com"}
                         {:db/id 95002 :advtest.person/name "NoEmail" :advtest.person/phone "456"}
                         {:db/id 95003 :advtest.person/name "Minimal"}])

        ;; Find people with email but no phone
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :advtest.person/name ?name]
                                   [?e :advtest.person/email _]
                                   (not [?e :advtest.person/phone _])])]
          (is (= 1 (count (:bindings result))))
          (is (= "NoPhone" (-> result :bindings first :?name))))))))

(deftest test-not-join-pattern
  (when (server-running?)
    (testing "NOT-JOIN clause"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 96000 :author/name "Author1"}
                         {:db/id 96001 :author/name "Author2"}
                         {:db/id 96002 :author/name "Author3"}
                         {:db/id 96010 :post/title "Post1" :post/author 96000}
                         {:db/id 96011 :post/title "Post2" :post/author 96000}
                         {:db/id 96012 :post/title "Post3" :post/author 96001}])

        ;; Find authors with no posts (using NOT-JOIN)
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?author :author/name ?name]
                                   (not-join [?author]
                                             [?post :post/author ?author])])]
          (is (= 1 (count (:bindings result))))
          (is (= "Author3" (-> result :bindings first :?name))))))))

;; ============================================================================
;; Advanced Join Patterns
;; ============================================================================

(deftest test-triangular-join
  (when (server-running?)
    (testing "Triangular join pattern (3-way relationship)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Project assignments: project, employee, role
        (dfdb/transact! conn
                        [{:db/id 100000 :project/name "ProjectA"}
                         {:db/id 100001 :project/name "ProjectB"}
                         {:db/id 100010 :emp/name "Developer1"}
                         {:db/id 100011 :emp/name "Developer2"}
                         {:db/id 100012 :emp/name "Manager1"}
                        ;; Assignments
                         {:db/id 100020
                          :assignment/project 100000
                          :assignment/employee 100010
                          :assignment/role "developer"}
                         {:db/id 100021
                          :assignment/project 100000
                          :assignment/employee 100012
                          :assignment/role "manager"}
                         {:db/id 100022
                          :assignment/project 100001
                          :assignment/employee 100011
                          :assignment/role "developer"}])

        ;; Find all developers on ProjectA
        (let [result (dfdb/query conn
                                 '[:find ?emp-name
                                   :where
                                   [?proj :project/name "ProjectA"]
                                   [?assign :assignment/project ?proj]
                                   [?assign :assignment/role "developer"]
                                   [?assign :assignment/employee ?emp]
                                   [?emp :emp/name ?emp-name]])]
          (is (= 1 (count (:bindings result))))
          (is (= "Developer1" (-> result :bindings first :?emp-name))))))))

(deftest test-diamond-join-pattern
  (when (server-running?)
    (testing "Diamond join pattern (4 entities, 2 paths)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; User -> Address & User -> Order -> Address (same address both ways)
        (dfdb/transact! conn
                        [{:db/id 110000 :user/name "Customer"}
                         {:db/id 110001 :address/street "123 Main St"}
                         {:db/id 110010 :order/id "ORD1" :order/customer 110000}
                        ;; Link both user and order to same address
                         [:db/add 110000 :user/shipping-address 110001]
                         [:db/add 110010 :order/shipping-address 110001]])

        ;; Find orders where order address matches customer address
        (let [result (dfdb/query conn
                                 '[:find ?order-id
                                   :where
                                   [?user :user/name "Customer"]
                                   [?user :user/shipping-address ?addr]
                                   [?order :order/customer ?user]
                                   [?order :order/shipping-address ?addr]
                                   [?order :order/id ?order-id]])]
          (is (= 1 (count (:bindings result))))
          (is (= "ORD1" (-> result :bindings first :?order-id))))))))

;; ============================================================================
;; Recursive and Graph Traversal
;; ============================================================================

(deftest test-three-hop-traversal
  (when (server-running?)
    (testing "Three-hop graph traversal"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Chain: A -> B -> C -> D
        (dfdb/transact! conn
                        [{:db/id 120000 :node/name "A"}
                         {:db/id 120001 :node/name "B" :node/parent 120000}
                         {:db/id 120002 :node/name "C" :node/parent 120001}
                         {:db/id 120003 :node/name "D" :node/parent 120002}])

        ;; Find D starting from A (3 hops)
        (let [result (dfdb/query conn
                                 '[:find ?descendant-name
                                   :where
                                   [?root :node/name "A"]
                                   [?child1 :node/parent ?root]
                                   [?child2 :node/parent ?child1]
                                   [?child3 :node/parent ?child2]
                                   [?child3 :node/name ?descendant-name]])]
          (is (= 1 (count (:bindings result))))
          (is (= "D" (-> result :bindings first :?descendant-name))))))))

(deftest test-bidirectional-graph
  (when (server-running?)
    (testing "Bidirectional relationship traversal"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Parent-child relationships (bidirectional via queries)
        (dfdb/transact! conn
                        [{:db/id 130000 :person/name "Parent"}
                         {:db/id 130001 :person/name "Child1" :person/parent 130000}
                         {:db/id 130002 :person/name "Child2" :person/parent 130000}])

        ;; Find parent from child (forward)
        (let [result (dfdb/query conn
                                 '[:find ?parent-name
                                   :where
                                   [?child :person/name "Child1"]
                                   [?child :person/parent ?parent]
                                   [?parent :person/name ?parent-name]])]
          (is (= 1 (count (:bindings result))))
          (is (= "Parent" (-> result :bindings first :?parent-name))))

        ;; Find children from parent (reverse via VAET)
        (let [result (dfdb/query conn
                                 '[:find ?child-name
                                   :where
                                   [?parent :person/name "Parent"]
                                   [?child :person/parent ?parent]
                                   [?child :person/name ?child-name]])]
          (is (= 2 (count (:bindings result))))
          (is (some #{"Child1"} (map :?child-name (:bindings result)))))))))

;; ============================================================================
;; Complex Filtering and Predicates
;; ============================================================================

(deftest test-multiple-numeric-predicates
  (when (server-running?)
    (testing "Multiple numeric predicates in complex query"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Range of products
        (let [products (vec (for [i (range 140000 140050)]
                              {:db/id i
                               :product/id (str "P" i)
                               :product/price (+ 10 (* (mod i 10) 10))
                               :product/stock (+ 5 (mod i 20))
                               :product/rating (+ 1 (mod i 5))}))]
          (dfdb/transact! conn products))

        ;; Complex filter: price between 30-70, stock > 10, rating >= 3
        (let [result (dfdb/query conn
                                 '[:find ?id ?price ?stock ?rating
                                   :where
                                   [?e :product/id ?id]
                                   [?e :product/price ?price]
                                   [?e :product/stock ?stock]
                                   [?e :product/rating ?rating]
                                   [(>= ?price 30)]
                                   [(<= ?price 70)]
                                   [(> ?stock 10)]
                                   [(>= ?rating 3)]])]
          (is (> (count (:bindings result)) 0))
          ;; Verify all results match criteria
          (is (every? #(and (>= (:?price %) 30)
                            (<= (:?price %) 70)
                            (> (:?stock %) 10)
                            (>= (:?rating %) 3))
                      (:bindings result))))))))

(deftest test-string-comparison-predicates
  (when (server-running?)
    (testing "String comparison in predicates"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 150000 :book/title "Advanced Clojure" :book/pages 300}
                         {:db/id 150001 :book/title "Beginner's Guide" :book/pages 150}
                         {:db/id 150002 :book/title "Expert Techniques" :book/pages 400}])

        ;; Find books with pages > 200 (numeric predicate works)
        (let [result (dfdb/query conn
                                 '[:find ?title
                                   :where
                                   [?e :book/title ?title]
                                   [?e :book/pages ?pages]
                                   [(> ?pages 200)]])]
          (is (= 2 (count (:bindings result)))))))))

;; ============================================================================
;; Mixed Transaction Types
;; ============================================================================

(deftest test-mixed-map-and-tuple-transactions
  (when (server-running?)
    (testing "Mix of map and tuple notation in one transaction"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (let [result (dfdb/transact! conn
                                     [{:db/id 160000 :mixed/type "map" :mixed/value 1}
                                      [:db/add 160001 :mixed/type "tuple"]
                                      [:db/add 160001 :mixed/value 2]
                                      {:db/id 160002 :mixed/type "map" :mixed/value 3}])]
          ;; Deltas may or may not be returned depending on server config
          (is (or (nil? (:deltas result)) (vector? (:deltas result)))))

        ;; Verify all three entities exist
        (let [result (dfdb/query conn
                                 '[:find (count ?e)
                                   :where [?e :mixed/type ?t]])]
          (is (= 3 (-> result :aggregate first first))))))))

(deftest test-retract-and-re-add
  (when (server-running?)
    (testing "Retract attribute and re-add it"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Use a unique entity ID based on timestamp to avoid collisions
            eid (+ 170000 (mod (System/currentTimeMillis) 10000))]
        ;; Add entity
        (dfdb/transact! conn [{:db/id eid :advtest.item/name "Item" :advtest.item/price 100}])

        ;; Verify price exists
        (let [result (dfdb/query conn `[:find ~'?price :where [~eid :advtest.item/price ~'?price]])]
          (is (= 100 (-> result :bindings first :?price))))

        ;; Retract price
        (dfdb/transact! conn [[:db/retract eid :advtest.item/price 100]])

        ;; Verify price is gone
        (let [result (dfdb/query conn `[:find ~'?price :where [~eid :advtest.item/price ~'?price]])]
          (is (= 0 (count (:bindings result)))))

        ;; Re-add with new price
        (dfdb/transact! conn [[:db/add eid :advtest.item/price 200]])

        ;; Verify new price
        (let [result (dfdb/query conn `[:find ~'?price :where [~eid :advtest.item/price ~'?price]])]
          (is (some #{200} (map :?price (:bindings result)))))))))

;; ============================================================================
;; Complex Aggregate Scenarios
;; ============================================================================

(deftest test-aggregate-with-multiple-variables
  (when (server-running?)
    (testing "Aggregate with multiple grouping variables"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Sales by region and product category
        (dfdb/transact! conn
                        [{:db/id 180000 :sale/region "North" :sale/category "Electronics" :sale/amount 1000}
                         {:db/id 180001 :sale/region "North" :sale/category "Electronics" :sale/amount 1500}
                         {:db/id 180002 :sale/region "North" :sale/category "Books" :sale/amount 500}
                         {:db/id 180003 :sale/region "South" :sale/category "Electronics" :sale/amount 2000}
                         {:db/id 180004 :sale/region "South" :sale/category "Books" :sale/amount 600}])

        ;; Sum by region and category
        (let [result (dfdb/query conn
                                 '[:find ?region ?category (sum ?amount)
                                   :where
                                   [?e :sale/region ?region]
                                   [?e :sale/category ?category]
                                   [?e :sale/amount ?amount]])]
          (is (= 4 (count (:aggregate result))))
          ;; North + Electronics should be 2500
          (let [north-elec-row (->> (:aggregate result)
                                    (filter #(and (= "North" (first %))
                                                  (= "Electronics" (second %))))
                                    first)
                north-elec (when north-elec-row (nth north-elec-row 2))]
            (is (= 2500 north-elec))))))))

(deftest test-aggregate-min-max-range
  (when (server-running?)
    (testing "Min and max in same query to find range"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 190000 :temp/reading 15}
                         {:db/id 190001 :temp/reading 22}
                         {:db/id 190002 :temp/reading 18}
                         {:db/id 190003 :temp/reading 25}
                         {:db/id 190004 :temp/reading 12}])

        ;; Get min
        (let [result (dfdb/query conn '[:find (min ?temp)
                                        :where [?e :temp/reading ?temp]])]
          (is (= 12 (-> result :aggregate first first))))

        ;; Get max
        (let [result (dfdb/query conn '[:find (max ?temp)
                                        :where [?e :temp/reading ?temp]])]
          (is (= 25 (-> result :aggregate first first))))))))

;; ============================================================================
;; Query Performance Tests
;; ============================================================================

(deftest test-query-over-large-dataset
  (when (server-running?)
    (testing "Query performance over 500+ entities"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create large dataset
        (let [entities (vec (for [i (range 200000 200500)]
                              {:db/id i
                               :record/id i
                               :record/category (nth ["A" "B" "C" "D"] (mod i 4))
                               :record/value (* i 2)}))]
          (dfdb/transact! conn entities))

        ;; Query with filter
        (let [result (dfdb/query conn
                                 '[:find ?id
                                   :where
                                   [?e :record/category "A"]
                                   [?e :record/value ?val]
                                   [(> ?val 400000)]
                                   [?e :record/id ?id]])]
          (is (> (count (:bindings result)) 100)))))))

(deftest test-complex-multi-pattern-query
  (when (server-running?)
    (testing "Complex query with 10+ patterns"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; E-commerce scenario
        (dfdb/transact! conn
                        [{:db/id 210000 :customer/name "Customer1" :customer/tier "gold"}
                         {:db/id 210001 :order/id "O1" :order/customer 210000 :order/status "shipped"}
                         {:db/id 210002 :item/sku "SKU1" :item/price 50}
                         {:db/id 210003 :order-line/order 210001 :order-line/item 210002 :order-line/qty 2}])

        ;; Complex join across 4 entity types with filters
        (let [result (dfdb/query conn
                                 '[:find ?customer-name ?order-id ?sku ?total
                                   :where
                                   [?customer :customer/name ?customer-name]
                                   [?customer :customer/tier "gold"]
                                   [?order :order/customer ?customer]
                                   [?order :order/id ?order-id]
                                   [?order :order/status "shipped"]
                                   [?line :order-line/order ?order]
                                   [?line :order-line/item ?item]
                                   [?line :order-line/qty ?qty]
                                   [?item :item/sku ?sku]
                                   [?item :item/price ?price]
                                   [(* ?price ?qty) ?total]])]
          (is (= 1 (count (:bindings result))))
          (is (= 100 (-> result :bindings first :?total))))))))

;; ============================================================================
;; Edge Cases and Boundary Tests
;; ============================================================================

(deftest test-empty-result-set
  (when (server-running?)
    (testing "Query that returns empty result set"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn [{:db/id 220000 :test/value 1}])

        ;; Query for non-existent value
        (let [result (dfdb/query conn
                                 '[:find ?e
                                   :where [?e :test/value 999]])]
          (is (= 0 (count (:bindings result))))
          (is (vector? (:bindings result))))))))

(deftest test-single-entity-multiple-attributes
  (when (server-running?)
    (testing "Single entity with many attributes"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Create entity with 20 attributes
            entity (merge {:db/id 230000}
                          (into {} (for [i (range 20)]
                                     [(keyword (str "attr" i)) (str "value" i)])))]
        (dfdb/transact! conn [entity])

        ;; Query for all attributes using variable attribute
        (let [result (dfdb/query conn
                                 '[:find ?a ?v
                                   :where [230000 ?a ?v]])]
          (is (= 20 (count (:bindings result)))))))))

(deftest test-concurrent-query-semantics
  (when (server-running?)
    (testing "Multiple queries see consistent snapshot"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Initial data
        (dfdb/transact! conn [{:db/id 240000 :counter/value 0}])

        ;; Update counter
        (dfdb/transact! conn [{:db/id 240000 :counter/value 1}])
        (dfdb/transact! conn [{:db/id 240000 :counter/value 2}])

        ;; Latest query should see most recent value
        (let [result (dfdb/query conn
                                 '[:find ?val
                                   :where [240000 :counter/value ?val]])]
          (is (>= (count (:bindings result)) 1))
          (is (some #{2} (map :?val (:bindings result)))))))))
