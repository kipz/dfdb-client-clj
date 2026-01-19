(ns dfdb.client.websocket-integration-test
  "Full integration tests for WebSocket delta streaming against a real dfdb-go server.
   These tests verify the complete flow: subscribe, transact, receive deltas, query views."
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]
            [dfdb.client.websocket :as ws]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(defn websocket-available? []
  (when (server-running?)
    (let [conn (dfdb/connect :base-url test-server-url)
          stream (dfdb/stream-connect conn)]
      (if (and stream (dfdb/stream-connected? stream))
        (do
          (dfdb/stream-close! stream)
          true)
        false))))

(defn with-server [f]
  (if (server-running?)
    (if (websocket-available?)
      (f)
      (do
        (println "\nWARNING: WebSocket endpoint not available on" test-server-url)
        (println "Ensure dfdb-go is running with WebSocket support enabled\n")))
    (do
      (println "\nWARNING: dfdb-go server not running on" test-server-url)
      (println "Start the server with: scripts/start-server.sh\n"))))

(use-fixtures :once with-server)

;; ============================================================================
;; Full Integration Test: Complete Workflow
;; ============================================================================

(deftest test-full-websocket-workflow
  (when (websocket-available?)
    (testing "Complete WebSocket workflow: create subscription, stream deltas, query view"
      (let [;; Use unique namespace to avoid test pollution
            test-id (System/currentTimeMillis)
            ns-prefix (str "wsint" test-id)
            name-attr (keyword ns-prefix "name")
            email-attr (keyword ns-prefix "email")
            status-attr (keyword ns-prefix "status")

            conn (dfdb/connect :base-url test-server-url)

            ;; Create query for subscription
            query-str (str "[:find ?e ?name ?email ?status "
                           ":where [?e :" ns-prefix "/name ?name] "
                           "[?e :" ns-prefix "/email ?email] "
                           "[?e :" ns-prefix "/status ?status]]")

            ;; Track all received deltas
            received-deltas (atom [])
            received-acks (atom [])
            received-errors (atom [])

            ;; Create subscription
            sub (dfdb/create-subscription conn (str "ws-integration-" test-id) query-str)]

        (try
          ;; Step 1: Connect to WebSocket stream
          (let [stream (dfdb/stream-connect conn
                                            :on-ack (fn [ack] (swap! received-acks conj ack))
                                            :on-error (fn [err] (swap! received-errors conj err)))]

            (is (some? stream) "Should connect to WebSocket")
            (is (dfdb/stream-connected? stream) "Stream should be connected")

            (try
              ;; Step 2: Subscribe to delta updates
              (dfdb/stream-subscribe! stream [(:id sub)]
                                      (fn [delta]
                                        (swap! received-deltas conj delta)))

              ;; Wait for subscription ack
              (Thread/sleep 300)

              (is (pos? (count @received-acks)) "Should receive subscription ack")
              (when (pos? (count @received-acks))
                (let [ack (first @received-acks)]
                  (is (= "subscribed" (:action ack)) "Ack action should be 'subscribed'")
                  (is (contains? (set (:subscription-ids ack)) (:id sub))
                      "Ack should include our subscription ID")))

              ;; Step 3: Transact initial data - should trigger delta
              (reset! received-deltas [])

              (dfdb/transact! conn [{:db/id -1
                                     name-attr "Alice"
                                     email-attr "alice@example.com"
                                     status-attr "active"}
                                    {:db/id -2
                                     name-attr "Bob"
                                     email-attr "bob@example.com"
                                     status-attr "active"}])

              ;; Wait for delta
              (Thread/sleep 500)

              ;; Verify delta received
              (is (pos? (count @received-deltas)) "Should receive delta after transaction")

              (when (pos? (count @received-deltas))
                (let [delta (first @received-deltas)]
                  (is (= (:id sub) (:subscription-id delta))
                      "Delta subscription ID should match")
                  (is (some? (:additions delta)) "Delta should have additions")
                  (is (some? (:timestamp delta)) "Delta should have timestamp")

                  ;; Check additions contain our data
                  (let [additions (:additions delta)
                        names (set (map #(get % (keyword "?name")) additions))]
                    (is (or (contains? names "Alice")
                            (contains? names "Bob"))
                        "Additions should contain transacted names"))))

              ;; Step 4: Query the materialized view - should match delta data
              (let [view-result (dfdb/query-view conn (:id sub))]
                (is (some? (:results view-result)) "View should have results")
                (is (>= (count (:results view-result)) 2)
                    "View should have at least 2 results")

                ;; Verify view contains our entities
                (let [names (set (map #(get % (keyword "?name")) (:results view-result)))]
                  (is (contains? names "Alice") "View should contain Alice")
                  (is (contains? names "Bob") "View should contain Bob")))

              ;; Step 5: Update data - should trigger delta with retraction + addition
              (reset! received-deltas [])

              ;; Find Alice's entity ID from the view
              (let [view-result (dfdb/query-view conn (:id sub))
                    alice-row (first (filter #(= "Alice" (get % (keyword "?name")))
                                             (:results view-result)))
                    alice-eid (get alice-row (keyword "?e"))]

                (when alice-eid
                  ;; Update Alice's status
                  (dfdb/transact! conn [{:db/id alice-eid status-attr "inactive"}])

                  ;; Wait for delta
                  (Thread/sleep 500)

                  ;; Should receive delta with the update
                  (is (pos? (count @received-deltas))
                      "Should receive delta after update")

                  (when (pos? (count @received-deltas))
                    (let [delta (first @received-deltas)]
                      ;; Update should show as retraction (old value) + addition (new value)
                      (is (or (pos? (count (:additions delta)))
                              (pos? (count (:retractions delta))))
                          "Update delta should have additions or retractions")))))

              ;; Step 6: Query view with filter
              (let [filtered (dfdb/query-view conn (:id sub)
                                              :filter {(keyword "?status") "active"})]
                (is (some? (:results filtered)) "Filtered view should have results"))

              ;; Step 7: Query view with sort and pagination
              (let [sorted (dfdb/query-view conn (:id sub)
                                            :sort [(str "-?" "name")]
                                            :limit 1)]
                (is (<= (count (:results sorted)) 1) "Should respect limit"))

              ;; Step 8: Add more data and verify delta streaming continues
              (reset! received-deltas [])

              (dfdb/transact! conn [{:db/id -3
                                     name-attr "Charlie"
                                     email-attr "charlie@example.com"
                                     status-attr "pending"}])

              (Thread/sleep 500)

              (is (pos? (count @received-deltas))
                  "Should continue receiving deltas for new transactions")

              ;; Step 9: Unsubscribe and verify no more deltas
              (reset! received-acks [])
              (dfdb/stream-unsubscribe! stream [(:id sub)])

              (Thread/sleep 300)

              (is (pos? (count @received-acks)) "Should receive unsubscribe ack")
              (when (pos? (count @received-acks))
                (is (= "unsubscribed" (:action (first @received-acks)))
                    "Ack should confirm unsubscription"))

              ;; Verify we're no longer tracking this subscription
              (is (not (contains? (ws/subscribed-ids stream) (:id sub)))
                  "Should no longer be subscribed")

              ;; Transact more data - should NOT receive delta
              (reset! received-deltas [])
              (dfdb/transact! conn [{:db/id -4
                                     name-attr "Diana"
                                     email-attr "diana@example.com"
                                     status-attr "active"}])

              (Thread/sleep 300)

              (is (zero? (count @received-deltas))
                  "Should NOT receive deltas after unsubscribing")

              ;; Step 10: Verify no errors occurred
              (is (zero? (count @received-errors))
                  "Should have no errors during the workflow")

              (finally
                (dfdb/stream-close! stream))))

          (finally
            ;; Cleanup: delete subscription
            (dfdb/delete-subscription conn (:id sub))))))))

;; ============================================================================
;; Integration Test: Multiple Subscriptions
;; ============================================================================

(deftest test-multiple-subscriptions-streaming
  (when (websocket-available?)
    (testing "Stream deltas from multiple subscriptions simultaneously"
      (let [test-id (System/currentTimeMillis)
            ns1 (str "multi1-" test-id)
            ns2 (str "multi2-" test-id)
            attr1 (keyword ns1 "value")
            attr2 (keyword ns2 "value")

            conn (dfdb/connect :base-url test-server-url)

            ;; Create two subscriptions with different queries
            query1 (str "[:find ?e ?v :where [?e :" ns1 "/value ?v]]")
            query2 (str "[:find ?e ?v :where [?e :" ns2 "/value ?v]]")

            sub1 (dfdb/create-subscription conn (str "multi-sub1-" test-id) query1)
            sub2 (dfdb/create-subscription conn (str "multi-sub2-" test-id) query2)

            ;; Track deltas per subscription
            deltas-sub1 (atom [])
            deltas-sub2 (atom [])]

        (try
          (let [stream (dfdb/stream-connect conn)]
            (try
              ;; Subscribe to both with different callbacks
              (dfdb/stream-subscribe! stream [(:id sub1)]
                                      (fn [delta] (swap! deltas-sub1 conj delta)))
              (dfdb/stream-subscribe! stream [(:id sub2)]
                                      (fn [delta] (swap! deltas-sub2 conj delta)))

              (Thread/sleep 300)

              ;; Verify both subscriptions are tracked
              (is (= #{(:id sub1) (:id sub2)} (ws/subscribed-ids stream))
                  "Should be subscribed to both")

              ;; Transact data for first subscription only
              (dfdb/transact! conn [{:db/id -1 attr1 "value1"}])
              (Thread/sleep 500)

              (is (pos? (count @deltas-sub1)) "Sub1 should receive delta")
              (is (zero? (count @deltas-sub2)) "Sub2 should NOT receive delta for sub1 data")

              ;; Transact data for second subscription only
              (reset! deltas-sub1 [])
              (dfdb/transact! conn [{:db/id -2 attr2 "value2"}])
              (Thread/sleep 500)

              (is (zero? (count @deltas-sub1)) "Sub1 should NOT receive delta for sub2 data")
              (is (pos? (count @deltas-sub2)) "Sub2 should receive delta")

              ;; Transact data for both
              (reset! deltas-sub1 [])
              (reset! deltas-sub2 [])
              (dfdb/transact! conn [{:db/id -3 attr1 "value3"}
                                    {:db/id -4 attr2 "value4"}])
              (Thread/sleep 500)

              (is (pos? (count @deltas-sub1)) "Sub1 should receive delta")
              (is (pos? (count @deltas-sub2)) "Sub2 should receive delta")

              (finally
                (dfdb/stream-close! stream))))

          (finally
            (dfdb/delete-subscription conn (:id sub1))
            (dfdb/delete-subscription conn (:id sub2))))))))

;; ============================================================================
;; Integration Test: Reconnection Scenario
;; ============================================================================

(deftest test-reconnection-scenario
  (when (websocket-available?)
    (testing "Can reconnect and resubscribe after disconnect"
      (let [test-id (System/currentTimeMillis)
            ns-prefix (str "reconn" test-id)
            value-attr (keyword ns-prefix "value")

            conn (dfdb/connect :base-url test-server-url)
            query-str (str "[:find ?e ?v :where [?e :" ns-prefix "/value ?v]]")
            sub (dfdb/create-subscription conn (str "reconn-sub-" test-id) query-str)

            deltas (atom [])]

        (try
          ;; First connection
          (let [stream1 (dfdb/stream-connect conn)]
            (dfdb/stream-subscribe! stream1 [(:id sub)]
                                    (fn [delta] (swap! deltas conj delta)))
            (Thread/sleep 200)

            ;; Transact and verify
            (dfdb/transact! conn [{:db/id -1 value-attr "first"}])
            (Thread/sleep 500)
            (is (pos? (count @deltas)) "Should receive delta on first connection")

            ;; Close connection
            (dfdb/stream-close! stream1)
            (Thread/sleep 100)
            (is (not (dfdb/stream-connected? stream1)) "First stream should be closed"))

          ;; Second connection (reconnect)
          (reset! deltas [])
          (let [stream2 (dfdb/stream-connect conn)]
            (is (dfdb/stream-connected? stream2) "Should reconnect successfully")

            (dfdb/stream-subscribe! stream2 [(:id sub)]
                                    (fn [delta] (swap! deltas conj delta)))
            (Thread/sleep 200)

            ;; Transact and verify deltas work on new connection
            (dfdb/transact! conn [{:db/id -2 value-attr "second"}])
            (Thread/sleep 500)
            (is (pos? (count @deltas)) "Should receive delta on reconnected stream")

            (dfdb/stream-close! stream2))

          (finally
            (dfdb/delete-subscription conn (:id sub))))))))

;; ============================================================================
;; Integration Test: High Volume Deltas
;; ============================================================================

(deftest test-high-volume-deltas
  (when (websocket-available?)
    (testing "Handle multiple rapid transactions"
      (let [test-id (System/currentTimeMillis)
            ns-prefix (str "highvol" test-id)
            value-attr (keyword ns-prefix "value")

            conn (dfdb/connect :base-url test-server-url)
            query-str (str "[:find ?e ?v :where [?e :" ns-prefix "/value ?v]]")
            sub (dfdb/create-subscription conn (str "highvol-sub-" test-id) query-str)

            deltas (atom [])
            tx-count 10]

        (try
          (let [stream (dfdb/stream-connect conn)]
            (try
              (dfdb/stream-subscribe! stream [(:id sub)]
                                      (fn [delta] (swap! deltas conj delta)))
              (Thread/sleep 200)

              ;; Rapidly transact multiple times
              (doseq [i (range tx-count)]
                (dfdb/transact! conn [{:db/id (- -100 i) value-attr (str "value-" i)}]))

              ;; Wait for all deltas to arrive
              (Thread/sleep 2000)

              ;; Should have received deltas (may be batched)
              (is (pos? (count @deltas)) "Should receive deltas for rapid transactions")

              ;; Verify view has all data
              (let [view (dfdb/query-view conn (:id sub))]
                (is (= tx-count (count (:results view)))
                    "View should contain all transacted entities"))

              (finally
                (dfdb/stream-close! stream))))

          (finally
            (dfdb/delete-subscription conn (:id sub))))))))

;; ============================================================================
;; Integration Test: Error Recovery
;; ============================================================================

(deftest test-error-recovery
  (when (websocket-available?)
    (testing "Handle errors gracefully and continue operating"
      (let [conn (dfdb/connect :base-url test-server-url)
            errors (atom [])
            acks (atom [])]

        (let [stream (dfdb/stream-connect conn
                                          :on-error (fn [err] (swap! errors conj err))
                                          :on-ack (fn [ack] (swap! acks conj ack)))]
          (try
            ;; Try to subscribe to non-existent subscription
            (dfdb/stream-subscribe! stream ["nonexistent-subscription-xyz"]
                                    (fn [_] nil))
            (Thread/sleep 300)

            ;; Should receive error
            (is (pos? (count @errors)) "Should receive error for nonexistent subscription")

            ;; Now create a real subscription and verify stream still works
            (let [test-id (System/currentTimeMillis)
                  ns-prefix (str "recovery" test-id)
                  value-attr (keyword ns-prefix "value")
                  query-str (str "[:find ?e ?v :where [?e :" ns-prefix "/value ?v]]")
                  sub (dfdb/create-subscription conn (str "recovery-sub-" test-id) query-str)
                  deltas (atom [])]

              (try
                (reset! acks [])
                (dfdb/stream-subscribe! stream [(:id sub)]
                                        (fn [delta] (swap! deltas conj delta)))
                (Thread/sleep 300)

                ;; Should receive ack for valid subscription
                (is (pos? (count @acks)) "Should receive ack after error recovery")

                ;; Transact and verify stream still works
                (dfdb/transact! conn [{:db/id -1 value-attr "recovered"}])
                (Thread/sleep 500)

                (is (pos? (count @deltas)) "Should receive deltas after error recovery")

                (finally
                  (dfdb/delete-subscription conn (:id sub)))))

            (finally
              (dfdb/stream-close! stream))))))))

;; ============================================================================
;; Integration Test: View Consistency
;; ============================================================================

(deftest test-view-consistency-with-deltas
  (when (websocket-available?)
    (testing "Materialized view stays consistent with streamed deltas"
      (let [test-id (System/currentTimeMillis)
            ns-prefix (str "consist" test-id)
            name-attr (keyword ns-prefix "name")
            count-attr (keyword ns-prefix "count")

            conn (dfdb/connect :base-url test-server-url)
            query-str (str "[:find ?e ?name ?count "
                           ":where [?e :" ns-prefix "/name ?name] "
                           "[?e :" ns-prefix "/count ?count]]")
            sub (dfdb/create-subscription conn (str "consist-sub-" test-id) query-str)

            all-additions (atom [])
            all-retractions (atom [])]

        (try
          (let [stream (dfdb/stream-connect conn)]
            (try
              (dfdb/stream-subscribe! stream [(:id sub)]
                                      (fn [{:keys [additions retractions]}]
                                        (swap! all-additions into (or additions []))
                                        (swap! all-retractions into (or retractions []))))
              (Thread/sleep 200)

              ;; Initial insert
              (dfdb/transact! conn [{:db/id -1 name-attr "Counter" count-attr 0}])
              (Thread/sleep 500)

              ;; Get entity ID
              (let [view (dfdb/query-view conn (:id sub))
                    eid (get (first (:results view)) (keyword "?e"))]

                ;; Multiple updates
                (dotimes [i 5]
                  (dfdb/transact! conn [{:db/id eid count-attr (inc i)}])
                  (Thread/sleep 200))

                (Thread/sleep 500)

                ;; Query final view state
                (let [final-view (dfdb/query-view conn (:id sub))
                      final-count (get (first (:results final-view)) (keyword "?count"))]

                  ;; View should show final state
                  (is (= 5 final-count) "View should show final count value")

                  ;; We should have received multiple deltas
                  (is (pos? (count @all-additions)) "Should have additions from deltas")

                  ;; Net result from deltas should match view
                  ;; (This is a simplified check - full delta reconciliation would be more complex)
                  (is (= 1 (count (:results final-view)))
                      "Should have exactly one entity in view")))

              (finally
                (dfdb/stream-close! stream))))

          (finally
            (dfdb/delete-subscription conn (:id sub))))))))
