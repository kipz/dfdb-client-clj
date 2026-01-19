(ns dfdb.client.websocket
  "WebSocket client for streaming subscription deltas from dfdb-go"
  (:require [hato.websocket :as hato-ws]
            [cheshire.core :as json]
            [clojure.string :as str]))

(defrecord DeltaStream [ws-client subscriptions callbacks state])

(defn- ws-url
  "Convert HTTP URL to WebSocket URL"
  [base-url]
  (-> base-url
      (str/replace #"^http://" "ws://")
      (str/replace #"^https://" "wss://")
      (str "/api/subscriptions/stream")))

(defn- send-message!
  "Send a JSON message over WebSocket"
  [ws-client msg]
  (when ws-client
    (hato-ws/send! ws-client (json/generate-string msg))))

(defn- handle-message
  "Handle incoming WebSocket message"
  [stream raw-msg]
  (try
    (let [msg (json/parse-string (str raw-msg) true)
          msg-type (:type msg)]
      (case msg-type
        "delta"
        (let [sub-id (:subscription-id msg)
              callbacks @(:callbacks stream)]
          (when-let [cb (get callbacks sub-id)]
            (cb {:subscription-id sub-id
                 :additions (:additions msg)
                 :retractions (:retractions msg)
                 :timestamp (:timestamp msg)})))

        "ack"
        (let [action (:action msg)
              sub-ids (:subscription-ids msg)]
          (when-let [on-ack (get @(:callbacks stream) :on-ack)]
            (on-ack {:action action :subscription-ids sub-ids})))

        "error"
        (let [error-msg (:message msg)
              code (:code msg)]
          (when-let [on-error (get @(:callbacks stream) :on-error)]
            (on-error {:message error-msg :code code})))

        ;; Unknown message type - ignore
        nil))
    (catch Exception e
      (when-let [on-error (get @(:callbacks stream) :on-error)]
        (on-error {:message (str "Failed to parse message: " (.getMessage e))
                   :code "PARSE_ERROR"})))))

(defn connect
  "Connect to the WebSocket stream for subscription deltas

  Args:
    conn - Connection created with `dfdb.client.core/connect`

  Options:
    :on-error - Callback for error messages (fn [{:keys [message code]}])
    :on-close - Callback when connection closes (fn [status])
    :on-ack   - Callback for ack messages (fn [{:keys [action subscription-ids]}])

  Returns:
    DeltaStream record that can be used with subscribe-deltas!, unsubscribe-deltas!, etc.
    Returns nil if connection fails.

  Example:
    (def stream (ws/connect conn
                  :on-error (fn [e] (println \"Error:\" (:message e)))
                  :on-close (fn [s] (println \"Closed with status:\" s))))"
  [conn & {:keys [on-error on-close on-ack]}]
  (let [url (ws-url (:base-url conn))
        callbacks (atom (cond-> {}
                          on-error (assoc :on-error on-error)
                          on-ack (assoc :on-ack on-ack)))
        subscriptions (atom #{})
        state (atom :connecting)
        stream (->DeltaStream (atom nil) subscriptions callbacks state)]
    (try
      (let [ws-future (hato-ws/websocket url
                                         {:on-message (fn [_ws data _last?]
                                                        (handle-message stream data))
                                          :on-close (fn [_ws status-code _reason]
                                                      (reset! state :closed)
                                                      (when on-close
                                                        (on-close status-code)))
                                          :on-error (fn [_ws err]
                                                      (when on-error
                                                        (on-error {:message (str err)
                                                                   :code "CONNECTION_ERROR"})))
                                          :on-open (fn [_ws]
                                                     (reset! state :connected))})
            ;; Wait for connection (with timeout)
            ws (deref ws-future 5000 nil)]
        (if ws
          (do
            (reset! (:ws-client stream) ws)
            stream)
          (do
            (reset! state :failed)
            (when on-error
              (on-error {:message "WebSocket connection timeout"
                         :code "TIMEOUT"}))
            nil)))
      (catch Exception e
        (reset! state :failed)
        (when on-error
          (on-error {:message (str "WebSocket connection failed: " (.getMessage e))
                     :code "CONNECTION_FAILED"}))
        nil))))

(defn subscribe-deltas!
  "Subscribe to delta updates for one or more subscriptions

  Args:
    stream - DeltaStream from `connect`
    subscription-ids - Vector of subscription IDs to subscribe to
    callback - Function to call when deltas arrive (fn [{:keys [subscription-id additions retractions timestamp]}])

  Example:
    (ws/subscribe-deltas! stream [(:id sub)]
      (fn [delta]
        (println \"Additions:\" (:additions delta))
        (println \"Retractions:\" (:retractions delta))))"
  [stream subscription-ids callback]
  (let [ids (if (string? subscription-ids) [subscription-ids] subscription-ids)]
    ;; Register callbacks
    (doseq [id ids]
      (swap! (:callbacks stream) assoc id callback)
      (swap! (:subscriptions stream) conj id))
    ;; Send subscribe message
    (send-message! @(:ws-client stream)
                   {:type "subscribe"
                    :subscription-ids ids})))

(defn unsubscribe-deltas!
  "Unsubscribe from delta updates

  Args:
    stream - DeltaStream from `connect`
    subscription-ids - Vector of subscription IDs to unsubscribe from

  Example:
    (ws/unsubscribe-deltas! stream [(:id sub)])"
  [stream subscription-ids]
  (let [ids (if (string? subscription-ids) [subscription-ids] subscription-ids)]
    ;; Remove callbacks
    (doseq [id ids]
      (swap! (:callbacks stream) dissoc id)
      (swap! (:subscriptions stream) disj id))
    ;; Send unsubscribe message
    (send-message! @(:ws-client stream)
                   {:type "unsubscribe"
                    :subscription-ids ids})))

(defn close!
  "Close the WebSocket connection

  Args:
    stream - DeltaStream from `connect`

  Example:
    (ws/close! stream)"
  [stream]
  (when-let [ws @(:ws-client stream)]
    (hato-ws/close! ws))
  (reset! (:state stream) :closed))

(defn connected?
  "Check if the WebSocket is connected

  Args:
    stream - DeltaStream from `connect`

  Returns:
    true if connected, false otherwise"
  [stream]
  (= @(:state stream) :connected))

(defn subscribed-ids
  "Get the set of subscription IDs this stream is subscribed to

  Args:
    stream - DeltaStream from `connect`

  Returns:
    Set of subscription IDs"
  [stream]
  @(:subscriptions stream))
