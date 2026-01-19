(ns dfdb.client.comprehensive-test
  "Comprehensive tests representative of Datalevin, Datomic, and Datascript usage patterns"
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
  (when-not (server-running?)
    (println "\nWARNING: dfdb-go server not running on" test-server-url)
    (println "Start the server with: /tmp/dfdb-server -a :8081\n"))
  (f))

(use-fixtures :once with-connection)

;; ============================================================================
;; Schema Setup Tests
;; ============================================================================

(deftest test-multi-entity-type-schema
  (when (server-running?)
    (testing "Multiple entity types with relationships"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create a blog-like schema
        (dfdb/transact! conn
                        [;; Users
                         {:db/id 100 :user/name "Alice" :user/email "alice@example.com"}
                         {:db/id 101 :user/name "Bob" :user/email "bob@example.com"}

                        ;; Posts
                         {:db/id 200
                          :post/title "First Post"
                          :post/content "Hello World"
                          :post/author 100}
                         {:db/id 201
                          :post/title "Second Post"
                          :post/content "Another post"
                          :post/author 101}

                        ;; Comments
                         {:db/id 300
                          :comment/text "Great post!"
                          :comment/author 101
                          :comment/post 200}])

        ;; Query users
        (let [result (dfdb/query conn '[:find ?name
                                        :where [?e :user/name ?name]])]
          (is (>= (count (:bindings result)) 2))
          (is (some #{"Alice"} (map :?name (:bindings result)))))

        ;; Query posts
        (let [result (dfdb/query conn '[:find ?title
                                        :where [?e :post/title ?title]])]
          (is (>= (count (:bindings result)) 2)))))))

;; ============================================================================
;; Join and Relationship Tests
;; ============================================================================

(deftest test-join-across-entities
  (when (server-running?)
    (testing "Join posts with authors"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Setup data
        (dfdb/transact! conn
                        [{:db/id 110 :user/name "Author1"}
                         {:db/id 111 :user/name "Author2"}
                         {:db/id 210 :post/title "Post1" :post/author 110}
                         {:db/id 211 :post/title "Post2" :post/author 111}])

        ;; Query: find post titles with author names
        (let [result (dfdb/query conn
                                 '[:find ?title ?author-name
                                   :where
                                   [?post :post/title ?title]
                                   [?post :post/author ?author]
                                   [?author :user/name ?author-name]])]
          (is (>= (count (:bindings result)) 2))
          (is (some #{"Author1"} (map :?author-name (:bindings result))))
          (is (some #{"Post1"} (map :?title (:bindings result)))))))))

(deftest test-multi-hop-traversal
  (when (server-running?)
    (testing "Multi-hop graph traversal (comment -> post -> author)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Setup: comment -> post -> author chain
        (dfdb/transact! conn
                        [{:db/id 120 :user/name "OriginalAuthor"}
                         {:db/id 220 :post/title "OriginalPost" :post/author 120}
                         {:db/id 320 :comment/text "Nice!" :comment/post 220}])

        ;; Find original author from comment
        (let [result (dfdb/query conn
                                 '[:find ?author-name
                                   :where
                                   [?comment :comment/text "Nice!"]
                                   [?comment :comment/post ?post]
                                   [?post :post/author ?author]
                                   [?author :user/name ?author-name]])]
          (is (= 1 (count (:bindings result))))
          (is (= "OriginalAuthor" (-> result :bindings first :?author-name))))))))

;; ============================================================================
;; Aggregate Tests
;; ============================================================================

(deftest test-aggregates
  (when (server-running?)
    (testing "Various aggregate functions"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Insert test data
        (dfdb/transact! conn
                        [{:db/id 400 :product/name "Widget" :product/price 10}
                         {:db/id 401 :product/name "Gadget" :product/price 20}
                         {:db/id 402 :product/name "Doohickey" :product/price 30}])

        ;; Count products
        (let [result (dfdb/query conn '[:find (count ?e)
                                        :where [?e :product/name ?name]])]
          (is (>= (-> result :aggregate first first) 3)))

        ;; Sum prices
        (let [result (dfdb/query conn '[:find (sum ?price)
                                        :where [?e :product/price ?price]])]
          (is (>= (-> result :aggregate first first) 60)))

        ;; Max price
        (let [result (dfdb/query conn '[:find (max ?price)
                                        :where [?e :product/price ?price]])]
          (is (>= (-> result :aggregate first first) 30)))

        ;; Min price
        (let [result (dfdb/query conn '[:find (min ?price)
                                        :where [?e :product/price ?price]])]
          (is (<= (-> result :aggregate first first) 10)))

        ;; Average price
        (let [result (dfdb/query conn '[:find (avg ?price)
                                        :where [?e :product/price ?price]])]
          (is (number? (-> result :aggregate first first))))))))

(deftest test-aggregate-with-grouping
  (when (server-running?)
    (testing "Aggregate with grouping by category"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Products with categories using unique namespace
        (dfdb/transact! conn
                        [{:db/id 410 :comptest.product/name "Widget1" :comptest.product/category "Tools" :comptest.product/price 10}
                         {:db/id 411 :comptest.product/name "Widget2" :comptest.product/category "Tools" :comptest.product/price 15}
                         {:db/id 412 :comptest.product/name "Book1" :comptest.product/category "Books" :comptest.product/price 20}])

        ;; Count by category
        (let [result (dfdb/query conn
                                 '[:find ?category (count ?e)
                                   :where
                                   [?e :comptest.product/category ?category]])]
          (is (= 2 (count (:aggregate result))))
          ;; Should have Tools: 2, Books: 1
          (is (some (fn [row] (and (= "Tools" (first row)) (= 2 (second row))))
                    (:aggregate result))))))))

;; ============================================================================
;; Predicate and Function Tests
;; ============================================================================

(deftest test-comparison-predicates
  (when (server-running?)
    (testing "Comparison predicates in queries"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 500 :item/name "Item1" :item/quantity 5}
                         {:db/id 501 :item/name "Item2" :item/quantity 15}
                         {:db/id 502 :item/name "Item3" :item/quantity 25}])

        ;; Greater than
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :item/name ?name]
                                   [?e :item/quantity ?qty]
                                   [(> ?qty 10)]])]
          (is (>= (count (:bindings result)) 2)))

        ;; Between (using >= and <)
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :item/name ?name]
                                   [?e :item/quantity ?qty]
                                   [(>= ?qty 10)]
                                   [(< ?qty 20)]])]
          (is (= 1 (count (:bindings result))))
          (is (= "Item2" (-> result :bindings first :?name))))))))

;; ============================================================================
;; Cardinality-Many and Set Tests
;; ============================================================================

(deftest test-cardinality-many-tags
  (when (server-running?)
    (testing "Cardinality-many attributes (tags)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add entity with multiple tags (simulate cardinality-many)
        (dfdb/transact! conn
                        [{:db/id 600 :article/title "Article1"}
                         [:db/add 600 :article/tag "clojure"]
                         [:db/add 600 :article/tag "database"]
                         [:db/add 600 :article/tag "datalog"]])

        ;; Query for articles with specific tag
        (let [result (dfdb/query conn
                                 '[:find ?title
                                   :where
                                   [?e :article/title ?title]
                                   [?e :article/tag "clojure"]])]
          (is (= 1 (count (:bindings result))))
          (is (= "Article1" (-> result :bindings first :?title))))))))

;; ============================================================================
;; Retraction Tests
;; ============================================================================

(deftest test-retract-attribute
  (when (server-running?)
    (testing "Retract an attribute value"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add data
        (dfdb/transact! conn [{:db/id 700 :person/name "John" :person/age 30}])

        ;; Verify it exists
        (let [result (dfdb/query conn
                                 '[:find ?age
                                   :where
                                   [?e :person/name "John"]
                                   [?e :person/age ?age]])]
          (is (= 30 (-> result :bindings first :?age))))

        ;; Retract the age
        (dfdb/transact! conn [[:db/retract 700 :person/age 30]])

        ;; Verify it's gone
        (let [result (dfdb/query conn
                                 '[:find ?age
                                   :where
                                   [?e :person/name "John"]
                                   [?e :person/age ?age]])]
          (is (= 0 (count (:bindings result)))))))))

;; ============================================================================
;; Temp ID and Reference Tests
;; ============================================================================

(deftest test-temp-ids-with-references
  (when (server-running?)
    (testing "Temp IDs with references between entities"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create entities with references using temp IDs
        (let [result (dfdb/transact! conn
                                     [{:db/id -1 :user/name "TempUser"}
                                      {:db/id -2
                                       :post/title "TempPost"
                                       :post/author -1}])]
          (is (map? (:temp-id-map result)))
          (is (contains? (:temp-id-map result) -1))
          (is (contains? (:temp-id-map result) -2)))))))

(deftest test-lookup-refs
  (when (server-running?)
    (testing "Lookup refs in transactions"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create user with unique email
        (dfdb/transact! conn [{:db/id 800
                               :user/name "LookupUser"
                               :user/email "lookup@example.com"}])

        ;; Use lookup ref to update the user
        (dfdb/transact! conn
                        [[:db/add [:user/email "lookup@example.com"]
                          :user/age 25]])

        ;; Verify the age was added
        (let [result (dfdb/query conn
                                 '[:find ?age
                                   :where
                                   [?e :user/email "lookup@example.com"]
                                   [?e :user/age ?age]])]
          (is (= 1 (count (:bindings result))))
          (is (= 25 (-> result :bindings first :?age))))))))

;; ============================================================================
;; Complex Query Patterns
;; ============================================================================

(deftest test-not-clause
  (when (server-running?)
    (testing "NOT clause in queries"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 900 :person/name "Has Email" :person/email "has@example.com"}
                         {:db/id 901 :person/name "No Email"}])

        ;; Find persons without email
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :person/name ?name]
                                   (not [?e :person/email _])])]
          (is (>= (count (:bindings result)) 1))
          (is (some #{"No Email"} (map :?name (:bindings result)))))))))

(deftest test-or-clause
  (when (server-running?)
    (testing "OR clause in queries"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 1000 :vehicle/type "car" :vehicle/name "Sedan"}
                         {:db/id 1001 :vehicle/type "truck" :vehicle/name "Pickup"}
                         {:db/id 1002 :vehicle/type "bicycle" :vehicle/name "Mountain"}])

        ;; Find cars OR trucks
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :vehicle/name ?name]
                                   (or [?e :vehicle/type "car"]
                                       [?e :vehicle/type "truck"])])]
          (is (>= (count (:bindings result)) 2))
          (is (some #{"Sedan"} (map :?name (:bindings result))))
          (is (some #{"Pickup"} (map :?name (:bindings result)))))))))

(deftest test-multiple-joins
  (when (server-running?)
    (testing "Multiple joins across entities"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Company -> Department -> Employee hierarchy
        (dfdb/transact! conn
                        [{:db/id 1100 :company/name "ACME Corp"}
                         {:db/id 1110 :dept/name "Engineering" :dept/company 1100}
                         {:db/id 1111 :dept/name "Sales" :dept/company 1100}
                         {:db/id 1120 :employee/name "Emp1" :employee/dept 1110}
                         {:db/id 1121 :employee/name "Emp2" :employee/dept 1110}
                         {:db/id 1122 :employee/name "Emp3" :employee/dept 1111}])

        ;; Find employees in Engineering department of ACME Corp
        (let [result (dfdb/query conn
                                 '[:find ?emp-name
                                   :where
                                   [?company :company/name "ACME Corp"]
                                   [?dept :dept/company ?company]
                                   [?dept :dept/name "Engineering"]
                                   [?emp :employee/dept ?dept]
                                   [?emp :employee/name ?emp-name]])]
          (is (>= (count (:bindings result)) 2))
          (is (some #{"Emp1"} (map :?emp-name (:bindings result)))))))))

;; ============================================================================
;; Pull Expression Tests
;; ============================================================================

(deftest test-pull-nested-references
  (when (server-running?)
    (testing "Pull expression with nested references"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 1200 :user/name "PullUser"}
                         {:db/id 1210
                          :post/title "PullPost"
                          :post/author 1200
                          :post/content "Content"}])

        ;; Pull post with author name
        (let [result (dfdb/query conn
                                 '[:find (pull ?post [:post/title :post/content])
                                   :where [?post :post/title "PullPost"]])]
          (is (= 1 (count (:bindings result))))
          (let [post (-> result :bindings first :?post)]
            (is (= "PullPost" (:post/title post)))
            (is (= "Content" (:post/content post)))))))))

(deftest test-pull-wildcard
  (when (server-running?)
    (testing "Pull with wildcard [*] returns all attributes"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 1300
                          :user/name "WildcardUser"
                          :user/email "wildcard@example.com"
                          :user/age 35}])

        ;; Pull all attributes
        (let [result (dfdb/query conn
                                 '[:find (pull ?e [*])
                                   :where [?e :user/email "wildcard@example.com"]])]
          (is (= 1 (count (:bindings result))))
          (let [entity (-> result :bindings first :?e)]
            (is (= "WildcardUser" (:user/name entity)))
            (is (= 35 (:user/age entity)))
            (is (= "wildcard@example.com" (:user/email entity)))))))))

;; ============================================================================
;; Time-based Query Tests
;; ============================================================================

(deftest test-temporal-queries
  (when (server-running?)
    (testing "Time-based queries with :as-of"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Add initial data
        (let [tx1 (dfdb/transact! conn [{:db/id 1400 :person/name "OldName"}])]

          ;; Update name
          (dfdb/transact! conn [{:db/id 1400 :person/name "NewName"}])

          ;; Query current state
          (let [result (dfdb/query conn
                                   '[:find ?name
                                     :where [?e :person/name ?name]
                                     [1400 :person/name ?name]])]
            (is (some #{"NewName"} (map :?name (:bindings result)))))

          ;; Query as-of old time
          (let [result (dfdb/query conn
                                   '[:find ?name
                                     :where [1400 :person/name ?name]]
                                   :as-of {:time/system (:tx-time tx1)})]
            (is (>= (count (:bindings result)) 1))
            (is (some #{"OldName"} (map :?name (:bindings result))))))))))

;; ============================================================================
;; Complex Filtering Tests
;; ============================================================================

(deftest test-complex-filtering
  (when (server-running?)
    (testing "Complex multi-predicate filtering"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 1500 :person/name "Young" :person/age 20 :person/score 85}
                         {:db/id 1501 :person/name "Middle" :person/age 35 :person/score 75}
                         {:db/id 1502 :person/name "Old" :person/age 50 :person/score 95}])

        ;; Age between 25-45 AND score > 70
        (let [result (dfdb/query conn
                                 '[:find ?name ?age ?score
                                   :where
                                   [?e :person/name ?name]
                                   [?e :person/age ?age]
                                   [?e :person/score ?score]
                                   [(>= ?age 25)]
                                   [(<= ?age 45)]
                                   [(> ?score 70)]])]
          (is (= 1 (count (:bindings result))))
          (is (= "Middle" (-> result :bindings first :?name))))))))

;; ============================================================================
;; Batch Transaction Tests
;; ============================================================================

(deftest test-large-batch-transaction
  (when (server-running?)
    (testing "Large batch transaction performance"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Create 50 entities in one transaction
            entities (vec (for [i (range 1600 1650)]
                            {:db/id i
                             :batch/index i
                             :batch/name (str "Entity" i)}))]
        (let [result (dfdb/transact! conn entities)]
          (is (number? (:tx-id result)))
          (is (pos? (count (:deltas result))))) ; Deltas returned (count varies by implementation)

        ;; Verify all were inserted
        (let [result (dfdb/query conn
                                 '[:find (count ?e)
                                   :where [?e :batch/index ?i]])]
          (is (>= (-> result :aggregate first first) 50)))))))

;; ============================================================================
;; Update and Upsert Tests
;; ============================================================================

(deftest test-entity-updates
  (when (server-running?)
    (testing "Entity updates and history"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Initial entity
        (dfdb/transact! conn [{:db/id 1700
                               :person/name "Original"
                               :person/age 25}])

        ;; Update name (cardinality-one replacement)
        (dfdb/transact! conn [{:db/id 1700 :person/name "Updated"}])

        ;; Verify new name exists
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where [1700 :person/name ?name]])]
          (is (>= (count (:bindings result)) 1))
          (is (some #{"Updated"} (map :?name (:bindings result)))))

        ;; Age should still be there
        (let [result (dfdb/query conn
                                 '[:find ?age
                                   :where [1700 :person/age ?age]])]
          (is (= 25 (-> result :bindings first :?age))))))))

;; ============================================================================
;; Graph Pattern Tests
;; ============================================================================

(deftest test-social-graph-friends
  (when (server-running?)
    (testing "Social graph - friends relationships"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Create social graph
        (dfdb/transact! conn
                        [{:db/id 1800 :person/name "Alice"}
                         {:db/id 1801 :person/name "Bob"}
                         {:db/id 1802 :person/name "Charlie"}
                         [:db/add 1800 :friend 1801]  ; Alice -> Bob
                         [:db/add 1800 :friend 1802]  ; Alice -> Charlie
                         [:db/add 1801 :friend 1802]]) ; Bob -> Charlie

        ;; Find Alice's friends
        (let [result (dfdb/query conn
                                 '[:find ?friend-name
                                   :where
                                   [?alice :person/name "Alice"]
                                   [?alice :friend ?friend]
                                   [?friend :person/name ?friend-name]])]
          (is (>= (count (:bindings result)) 2))
          (is (some #{"Bob"} (map :?friend-name (:bindings result))))
          (is (some #{"Charlie"} (map :?friend-name (:bindings result)))))))))

(deftest test-mutual-friends
  (when (server-running?)
    (testing "Find mutual friends (friends of friends)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 1810 :person/name "A"}
                         {:db/id 1811 :person/name "B"}
                         {:db/id 1812 :person/name "C"}
                         {:db/id 1813 :person/name "D"}
                         [:db/add 1810 :friend 1811]  ; A -> B
                         [:db/add 1811 :friend 1812]  ; B -> C
                         [:db/add 1811 :friend 1813]]) ; B -> D

        ;; Find friends of A's friends (B's friends: C and D)
        (let [result (dfdb/query conn
                                 '[:find ?fof-name
                                   :where
                                   [?person :person/name "A"]
                                   [?person :friend ?friend]
                                   [?friend :friend ?fof]
                                   [?fof :person/name ?fof-name]])]
          (is (>= (count (:bindings result)) 2))
          (is (some #{"C"} (map :?fof-name (:bindings result))))
          (is (some #{"D"} (map :?fof-name (:bindings result)))))))))

;; ============================================================================
;; Variable Attribute Tests
;; ============================================================================

(deftest test-variable-attribute-all-facts
  (when (server-running?)
    (testing "Get all facts (EAV) for an entity"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn [{:db/id 1900
                               :person/name "FullPerson"
                               :person/age 40
                               :person/email "full@example.com"
                               :person/city "NYC"}])

        ;; Get all attributes and values
        (let [result (dfdb/query conn
                                 '[:find ?attr ?val
                                   :in $ ?eid
                                   :where [?eid ?attr ?val]]
                                 :params {"?eid" 1900})]
          (is (>= (count (:bindings result)) 4))
          ;; Should have all four attributes
          (let [attrs (set (map :?attr (:bindings result)))]
            (is (contains? attrs :person/name))
            (is (contains? attrs :person/age))
            (is (contains? attrs :person/email))
            (is (contains? attrs :person/city))))))))

(deftest test-find-all-entities-with-attribute
  (when (server-running?)
    (testing "Find all entities that have a specific attribute"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 2000 :item/name "Item1" :item/price 10}
                         {:db/id 2001 :item/name "Item2" :item/price 20}
                         {:db/id 2002 :item/name "Item3"}]) ; No price

        ;; Find all entities with price attribute
        (let [result (dfdb/query conn
                                 '[:find ?e ?price
                                   :where [?e :item/price ?price]])]
          (is (>= (count (:bindings result)) 2)))))))

;; ============================================================================
;; Parametrized Query Tests
;; ============================================================================

(deftest test-multiple-parameters
  (when (server-running?)
    (testing "Queries with multiple parameters"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 2100 :product/name "P1" :product/price 15 :product/category "Cat1"}
                         {:db/id 2101 :product/name "P2" :product/price 25 :product/category "Cat1"}
                         {:db/id 2102 :product/name "P3" :product/price 35 :product/category "Cat2"}])

        ;; Query with category and min price parameters
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :in $ ?cat ?min-price
                                   :where
                                   [?e :product/name ?name]
                                   [?e :product/category ?cat]
                                   [?e :product/price ?price]
                                   [(>= ?price ?min-price)]]
                                 :params {"?cat" "Cat1" "?min-price" 20})]
          (is (= 1 (count (:bindings result))))
          (is (= "P2" (-> result :bindings first :?name))))))))

;; ============================================================================
;; Pattern Matching Tests
;; ============================================================================

(deftest test-wildcard-patterns
  (when (server-running?)
    (testing "Wildcard _ in patterns"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 2200 :task/name "Task1" :task/status "done"}
                         {:db/id 2201 :task/name "Task2" :task/status "pending"}
                         {:db/id 2202 :task/name "Task3" :task/status "done"}])

        ;; Find all entities that have a :task/status (any value)
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :task/name ?name]
                                   [?e :task/status _]])]
          (is (= 3 (count (:bindings result)))))))))

(deftest test-same-variable-multiple-patterns
  (when (server-running?)
    (testing "Same variable in multiple patterns (implicit join)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 2300 :comptest.person/name "Complete" :comptest.person/age 30 :comptest.person/email "c@example.com"}
                         {:db/id 2301 :comptest.person/name "Incomplete" :comptest.person/age 25}])

        ;; Find persons with both age AND email
        (let [result (dfdb/query conn
                                 '[:find ?name
                                   :where
                                   [?e :comptest.person/name ?name]
                                   [?e :comptest.person/age _]
                                   [?e :comptest.person/email _]])]
          (is (= 1 (count (:bindings result))))
          (is (= "Complete" (-> result :bindings first :?name))))))))

;; ============================================================================
;; Aggregate Edge Cases
;; ============================================================================

(deftest test-count-distinct
  (when (server-running?)
    (testing "Count entities (distinct)"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 2400 :item/tag "tag1" :item/name "I1"}
                         {:db/id 2401 :item/tag "tag1" :item/name "I2"}
                         {:db/id 2402 :item/tag "tag2" :item/name "I3"}])

        ;; Count unique items
        (let [result (dfdb/query conn
                                 '[:find (count ?e)
                                   :where [?e :item/name ?name]])]
          (is (>= (-> result :aggregate first first) 3)))))))

;; ============================================================================
;; Complex Data Structure Tests
;; ============================================================================

(deftest test-nested-data
  (when (server-running?)
    (testing "Nested data structures in transactions"
      (let [conn (dfdb/connect :base-url test-server-url)]
        ;; Transaction with nested entity - use unique namespace
        (dfdb/transact! conn
                        [{:db/id 2500
                          :comptest.order/id "ORD-123"
                          :comptest.order/customer 2510}
                         {:db/id 2510
                          :comptest.customer/name "Customer1"
                          :comptest.customer/email "cust1@example.com"}])

        ;; Query across the reference
        (let [result (dfdb/query conn
                                 '[:find ?order-id ?customer-name
                                   :where
                                   [?order :comptest.order/id ?order-id]
                                   [?order :comptest.order/customer ?customer]
                                   [?customer :comptest.customer/name ?customer-name]])]
          (is (= 1 (count (:bindings result))))
          (is (= "ORD-123" (-> result :bindings first :?order-id)))
          (is (= "Customer1" (-> result :bindings first :?customer-name))))))))

;; ============================================================================
;; Query Result Format Tests
;; ============================================================================

(deftest test-find-single-vs-collection
  (when (server-running?)
    (testing "Find returns collection of bindings"
      (let [conn (dfdb/connect :base-url test-server-url)]
        (dfdb/transact! conn
                        [{:db/id 2600 :item/name "Single1" :item/value 1}
                         {:db/id 2601 :item/name "Single2" :item/value 2}])

        ;; Find should return collection
        (let [result (dfdb/query conn
                                 '[:find ?name ?value
                                   :where
                                   [?e :item/name ?name]
                                   [?e :item/value ?value]])]
          (is (vector? (:bindings result)))
          (is (>= (count (:bindings result)) 2))
          ;; Each binding should have both variables
          (is (every? #(and (:?name %) (:?value %)) (:bindings result))))))))
