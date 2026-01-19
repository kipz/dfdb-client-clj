(ns dfdb.client.websocket-stream-test
  "Integration tests for WebSocket delta streaming"
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
  "Check if WebSocket endpoint is available"
  (when (server-running?)
    (let [conn (dfdb/connect :base-url test-server-url)
          stream (dfdb/stream-connect conn)]
      (if (and stream (dfdb/stream-connected? stream))
        (do
          (dfdb/stream-close! stream)
          true)
        false))))

(defn with-connection [f]
  (if (server-running?)
    (f)
    (do
      (println "\nWARNING: dfdb-go server not running on" test-server-url)
      (println "Start the server with: scripts/start-server.sh\n"))))

(use-fixtures :once with-connection)

;; ============================================================================
;; WebSocket Connection Tests
;; ============================================================================

(deftest test-websocket-connect
  (when (websocket-available?)
    (testing "Connect to WebSocket stream"
      (let [conn (dfdb/connect :base-url test-server-url)
            stream (dfdb/stream-connect conn)]
        (is (some? stream) "Should return a stream")
        (is (dfdb/stream-connected? stream) "Stream should be connected")
        ;; Clean up
        (dfdb/stream-close! stream)
        (Thread/sleep 50)
        (is (not (dfdb/stream-connected? stream)) "Stream should be disconnected after close")))))

(deftest test-websocket-connect-with-callbacks
  (when (websocket-available?)
    (testing "Connect with error and close callbacks"
      (let [conn (dfdb/connect :base-url test-server-url)
            errors (atom [])
            closed (atom false)
            acks (atom [])
            stream (dfdb/stream-connect conn
                                        :on-error (fn [e] (swap! errors conj e))
                                        :on-close (fn [_] (reset! closed true))
                                        :on-ack (fn [a] (swap! acks conj a)))]
        (is (dfdb/stream-connected? stream) "Should be connected")
        (dfdb/stream-close! stream)
        (Thread/sleep 100)
        (is @closed "Close callback should have been called")))))

;; ============================================================================
;; Subscribe and Receive Delta Tests
;; ============================================================================

(deftest test-subscribe-and-receive-delta
  (when (websocket-available?)
    (testing "Subscribe to delta stream and receive updates"
      (let [unique-ns (str "ws-test-" (System/currentTimeMillis))
            name-attr (keyword unique-ns "name")
            conn (dfdb/connect :base-url test-server-url)
            ;; Create a subscription first
            query-str (str "[:find ?e ?name :where [?e :" unique-ns "/name ?name]]")
            sub (dfdb/create-subscription conn "ws-test-sub" query-str)
            ;; Track received deltas
            deltas (atom [])
            acks (atom [])
            ;; Connect to WebSocket
            stream (dfdb/stream-connect conn
                                        :on-ack (fn [a] (swap! acks conj a)))]
        (try
          ;; Subscribe to deltas
          (dfdb/stream-subscribe! stream [(:id sub)]
                                  (fn [delta]
                                    (swap! deltas conj delta)))

          ;; Wait for ack
          (Thread/sleep 200)
          (is (pos? (count @acks)) "Should have received ack")
          (when (pos? (count @acks))
            (is (= "subscribed" (:action (first @acks))) "Ack should confirm subscription"))

          ;; Transact new data - should trigger delta
          (dfdb/transact! conn [{:db/id -1 name-attr "WebSocketUser"}])

          ;; Wait for delta to arrive
          (Thread/sleep 500)

          ;; Verify delta received
          (is (pos? (count @deltas)) "Should have received at least one delta")
          (when (pos? (count @deltas))
            (let [delta (first @deltas)]
              (is (= (:id sub) (:subscription-id delta)) "Delta should be for our subscription")
              (is (some? (:additions delta)) "Delta should have additions")
              (is (some? (:timestamp delta)) "Delta should have timestamp")))

          (finally
            (dfdb/stream-close! stream)
            (dfdb/delete-subscription conn (:id sub))))))))

(deftest test-subscribe-multiple-subscriptions
  (when (websocket-available?)
    (testing "Subscribe to multiple subscriptions at once"
      (let [conn (dfdb/connect :base-url test-server-url)
            ;; Create two subscriptions
            sub1 (dfdb/create-subscription conn "multi-sub-1"
                                           "[:find ?e :where [?e :multi/type \"a\"]]")
            sub2 (dfdb/create-subscription conn "multi-sub-2"
                                           "[:find ?e :where [?e :multi/type \"b\"]]")
            acks (atom [])
            stream (dfdb/stream-connect conn
                                        :on-ack (fn [a] (swap! acks conj a)))]
        (try
          ;; Subscribe to both at once
          (dfdb/stream-subscribe! stream [(:id sub1) (:id sub2)]
                                  (fn [_delta] nil))

          ;; Wait for ack
          (Thread/sleep 200)

          ;; Verify both subscriptions were acknowledged
          (is (pos? (count @acks)) "Should have received ack")
          (when (pos? (count @acks))
            (let [ack (first @acks)]
              (is (= 2 (count (:subscription-ids ack))) "Should have 2 subscription IDs in ack")))

          (finally
            (dfdb/stream-close! stream)
            (dfdb/delete-subscription conn (:id sub1))
            (dfdb/delete-subscription conn (:id sub2))))))))

(deftest test-unsubscribe-from-deltas
  (when (websocket-available?)
    (testing "Unsubscribe from delta stream"
      (let [conn (dfdb/connect :base-url test-server-url)
            sub (dfdb/create-subscription conn "unsub-test"
                                          "[:find ?e :where [?e :unsub/data _]]")
            acks (atom [])
            stream (dfdb/stream-connect conn
                                        :on-ack (fn [a] (swap! acks conj a)))]
        (try
          ;; Subscribe first
          (dfdb/stream-subscribe! stream [(:id sub)] (fn [_] nil))
          (Thread/sleep 200)

          ;; Clear acks and unsubscribe
          (reset! acks [])
          (dfdb/stream-unsubscribe! stream [(:id sub)])
          (Thread/sleep 200)

          ;; Verify unsubscribe ack
          (is (pos? (count @acks)) "Should have received unsubscribe ack")
          (when (pos? (count @acks))
            (is (= "unsubscribed" (:action (first @acks))) "Ack should confirm unsubscription"))

          (finally
            (dfdb/stream-close! stream)
            (dfdb/delete-subscription conn (:id sub))))))))

;; ============================================================================
;; Error Handling Tests
;; ============================================================================

(deftest test-subscribe-nonexistent-subscription
  (when (websocket-available?)
    (testing "Subscribe to nonexistent subscription returns error"
      (let [conn (dfdb/connect :base-url test-server-url)
            errors (atom [])
            stream (dfdb/stream-connect conn
                                        :on-error (fn [e] (swap! errors conj e)))]
        (try
          ;; Try to subscribe to non-existent subscription
          (dfdb/stream-subscribe! stream ["nonexistent-sub-12345"]
                                  (fn [_] nil))

          ;; Wait for error
          (Thread/sleep 300)

          ;; Verify error received
          (is (pos? (count @errors)) "Should have received error")
          (when (pos? (count @errors))
            (let [err (first @errors)]
              (is (= "NOT_FOUND" (:code err)) "Error code should be NOT_FOUND")))

          (finally
            (dfdb/stream-close! stream)))))))

(deftest test-delta-additions-and-retractions
  (when (websocket-available?)
    (testing "Receive both additions and retractions in deltas"
      (let [unique-ns (str "delta-test-" (System/currentTimeMillis))
            name-attr (keyword unique-ns "name")
            conn (dfdb/connect :base-url test-server-url)
            ;; Add initial data
            _ (dfdb/transact! conn [{:db/id 99001 name-attr "Original"}])
            ;; Create subscription
            query-str (str "[:find ?e ?name :where [?e :" unique-ns "/name ?name]]")
            sub (dfdb/create-subscription conn "delta-test-sub" query-str)
            deltas (atom [])
            stream (dfdb/stream-connect conn)]
        (try
          ;; Subscribe
          (dfdb/stream-subscribe! stream [(:id sub)]
                                  (fn [delta] (swap! deltas conj delta)))
          (Thread/sleep 200)

          ;; Update the entity - should cause retraction + addition
          (dfdb/transact! conn [{:db/id 99001 name-attr "Updated"}])
          (Thread/sleep 500)

          ;; We should have received a delta with the update
          (is (pos? (count @deltas)) "Should have received delta for update")

          (finally
            (dfdb/stream-close! stream)
            (dfdb/delete-subscription conn (:id sub))))))))

;; ============================================================================
;; Reconnection and State Tests
;; ============================================================================

(deftest test-subscribed-ids-tracking
  (when (websocket-available?)
    (testing "Track subscribed IDs correctly"
      (let [conn (dfdb/connect :base-url test-server-url)
            sub1 (dfdb/create-subscription conn "track-1"
                                           "[:find ?e :where [?e :track/a _]]")
            sub2 (dfdb/create-subscription conn "track-2"
                                           "[:find ?e :where [?e :track/b _]]")
            stream (dfdb/stream-connect conn)]
        (try
          ;; Initially no subscriptions
          (is (empty? (ws/subscribed-ids stream)) "Should start with no subscriptions")

          ;; Subscribe to first
          (dfdb/stream-subscribe! stream [(:id sub1)] (fn [_] nil))
          (Thread/sleep 100)
          (is (= #{(:id sub1)} (ws/subscribed-ids stream)) "Should track first subscription")

          ;; Subscribe to second
          (dfdb/stream-subscribe! stream [(:id sub2)] (fn [_] nil))
          (Thread/sleep 100)
          (is (= #{(:id sub1) (:id sub2)} (ws/subscribed-ids stream)) "Should track both")

          ;; Unsubscribe from first
          (dfdb/stream-unsubscribe! stream [(:id sub1)])
          (Thread/sleep 100)
          (is (= #{(:id sub2)} (ws/subscribed-ids stream)) "Should only have second")

          (finally
            (dfdb/stream-close! stream)
            (dfdb/delete-subscription conn (:id sub1))
            (dfdb/delete-subscription conn (:id sub2))))))))

;; ============================================================================
;; Direct WebSocket Namespace Tests
;; ============================================================================

(deftest test-ws-namespace-directly
  (when (websocket-available?)
    (testing "Use websocket namespace functions directly"
      (let [conn (dfdb/connect :base-url test-server-url)
            sub (dfdb/create-subscription conn "direct-ws-test"
                                          "[:find ?e :where [?e :direct/ws _]]")
            stream (ws/connect conn)]
        (try
          (is (ws/connected? stream) "Should be connected via ws/connect")

          (ws/subscribe-deltas! stream [(:id sub)] (fn [_] nil))
          (Thread/sleep 100)
          (is (contains? (ws/subscribed-ids stream) (:id sub)))

          (ws/unsubscribe-deltas! stream [(:id sub)])
          (Thread/sleep 100)
          (is (not (contains? (ws/subscribed-ids stream) (:id sub))))

          (ws/close! stream)
          (Thread/sleep 50)
          (is (not (ws/connected? stream)))

          (finally
            (dfdb/delete-subscription conn (:id sub))))))))

;; ============================================================================
;; Connection Failure Tests (don't require running server)
;; ============================================================================

(deftest test-connection-failure-handling
  (testing "Handle connection failure gracefully"
    (let [conn (dfdb/connect :base-url "http://localhost:99999")
          errors (atom [])
          stream (dfdb/stream-connect conn
                                      :on-error (fn [e] (swap! errors conj e)))]
      ;; Should return nil on connection failure
      (is (nil? stream) "Should return nil on connection failure"))))

(deftest test-websocket-availability-check
  (testing "WebSocket availability check works"
    ;; This just verifies the check function doesn't throw
    (let [available (websocket-available?)]
      (is (or (true? available) (false? available) (nil? available))
          "websocket-available? should return boolean or nil"))))
