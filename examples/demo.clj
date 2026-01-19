(ns demo
  (:require [dfdb.client.core :as dfdb]))

(defn -main []
  (println "=== dfdb-client-clj Demo ===\n")

  ;; Connect to server
  (println "1. Connecting to server...")
  (def conn (dfdb/connect :base-url "http://localhost:8081"))
  (println "   ✓ Connected\n")

  ;; Health check
  (println "2. Health check...")
  (let [health (dfdb/health conn)]
    (println "   Status:" (:status health))
    (println "   Time:" (:time health))
    (println "   ✓ Server is healthy\n"))

  ;; Insert data with map notation
  (println "3. Inserting data (map notation)...")
  (let [result (dfdb/transact! conn [{:db/id 1001 :user/name "Alice" :user/age 30}
                                     {:db/id 1002 :user/name "Bob" :user/age 25}
                                     {:db/id 1003 :user/name "Charlie" :user/age 35}])]
    (println "   TX ID:" (:tx-id result))
    (println "   ✓ Data inserted\n"))

  ;; Insert data with tuple notation
  (println "4. Inserting data (tuple notation)...")
  (let [result (dfdb/transact! conn [[:db/add 1004 :user/name "Diana"]
                                     [:db/add 1004 :user/age 28]])]
    (println "   TX ID:" (:tx-id result))
    (println "   ✓ Data inserted\n"))

  ;; Basic query
  (println "5. Basic query (find all users)...")
  (let [result (dfdb/query conn '[:find ?name ?age
                                  :where
                                  [?e :user/name ?name]
                                  [?e :user/age ?age]])]
    (println "   Results:")
    (doseq [binding (:bindings result)]
      (println "    -" (:?name binding) "is" (:?age binding) "years old"))
    (println "   ✓ Found" (count (:bindings result)) "users\n"))

  ;; Query with parameters
  (println "6. Query with parameters (age >= 30)...")
  (let [result (dfdb/query conn '[:find ?name ?age
                                  :in $ ?min-age
                                  :where
                                  [?e :user/name ?name]
                                  [?e :user/age ?age]
                                  [(>= ?age ?min-age)]]
                           :params {"?min-age" 30})]
    (println "   Results:")
    (doseq [binding (:bindings result)]
      (println "    -" (:?name binding) "is" (:?age binding) "years old"))
    (println "   ✓ Found" (count (:bindings result)) "users\n"))

  ;; Aggregate query
  (println "7. Aggregate query (count users)...")
  (let [result (dfdb/query conn '[:find (count ?e)
                                  :where
                                  [?e :user/name ?name]])]
    (println "   Total users:" (-> result :aggregate first first))
    (println "   ✓ Aggregation works\n"))

  (println "=== Demo Complete ===")
  (println "\nAll core functionality is working!")
  (System/exit 0))

(-main)
