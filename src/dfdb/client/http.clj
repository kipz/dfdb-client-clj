(ns dfdb.client.http
  "HTTP/2 client with retry logic for dfdb-go server"
  (:require [hato.client :as hc]
            [cognitect.transit :as transit]
            [cheshire.core :as json])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.net.http HttpClient$Version]))

(defn- transit-encode
  "Encode Clojure data to Transit JSON"
  [data]
  (let [out (ByteArrayOutputStream.)]
    (transit/write (transit/writer out :json) data)
    (.toString out "UTF-8")))

(defn- convert-temp-id-map
  "Normalize temp-id-map keys to strings (temp-IDs are now strings, not integers)"
  [response]
  (if-let [temp-id-map (:temp-id-map response)]
    (try
      ;; Handle Transit TaggedValueImpl - convert to map
      (let [converted-map (if (map? temp-id-map)
                            temp-id-map
                           ;; If it's a TaggedValueImpl, it might be a transit map
                           ;; Try to convert via into {}
                            (try
                              (into {} temp-id-map)
                              (catch Exception _
                               ;; Fallback: it's likely empty or invalid
                                {})))]
        ;; Keys are now strings (temp-IDs), values are resolved integer entity IDs
        (assoc response :temp-id-map
               (into {} (map (fn [[k v]]
                               [(if (keyword? k)
                                  (name k)
                                  (str k))
                                v])
                             converted-map))))
      (catch Exception e
        ;; If conversion fails, just return original
        response))
    response))

(defn- transit-decode
  "Decode Transit JSON to Clojure data"
  [^String json-str]
  ;; Handle empty responses (e.g., from DELETE)
  (when (and json-str (pos? (count json-str)))
    (let [in (ByteArrayInputStream. (.getBytes json-str "UTF-8"))
          data (transit/read (transit/reader in :json))]
      ;; Post-process to convert temp-id-map keys if present
      (if (map? data)
        (convert-temp-id-map data)
        data))))

(defn- should-retry?
  "Determine if a request should be retried based on status code"
  [status]
  (or (>= status 500)  ; Server errors
      (= status 429))) ; Too many requests

(defn- exponential-backoff
  "Calculate exponential backoff delay in milliseconds"
  [attempt]
  (long (* 1000 (Math/pow 2 attempt))))

;; Create a shared HTTP client (HTTP/1.1 - dfdb doesn't support HTTP/2)
(defonce ^:private http-client
  (hc/build-http-client {:connect-timeout 10000}))

(defn request
  "Make an HTTP/2 request with retry logic

  Options:
    :method - HTTP method (:get, :post, etc.)
    :url - Full URL
    :body - Request body (will be Transit encoded)
    :max-retries - Maximum number of retries (default: 3)
    :timeout - Request timeout in milliseconds (default: 30000)"
  [{:keys [method url body max-retries timeout]
    :or {max-retries 3 timeout 30000}}]
  (loop [attempt 0]
    (let [encoded-body (when body
                         (try
                           (transit-encode body)
                           (catch Exception e
                             (println "[TRANSIT ENCODE ERROR]" (ex-message e))
                             (println "[TRANSIT ENCODE ERROR] Body type:" (type body))
                             (println "[TRANSIT ENCODE ERROR] Body sample:" (pr-str (take 2 body)))
                             (throw e))))
          opts {:http-client http-client
                :request-method method
                :url url
                :headers {"Content-Type" "application/transit+json"
                          "Accept" "application/transit+json"}
                :body encoded-body
                :timeout timeout
                :as :string}
          _ (println "[HTTP] Attempting request to" url "with body length" (count encoded-body))
          {:keys [status body] :as response}
          (try
            (hc/request opts)
            (catch Exception e
              (println "[HTTP ERROR]" (ex-message e))
              (println "[HTTP ERROR] Exception type:" (type e))
              {:status 0 :error (ex-message e)}))]
      (println "[HTTP] Got response status:" status)

      (cond
        ;; Success
        (and (nil? (:error response)) (< status 400))
        {:success true
         :status status
         :body (when body (transit-decode body))}

        ;; Error that should be retried
        (and (or (:error response) (should-retry? status))
             (< attempt max-retries))
        (do
          (Thread/sleep (exponential-backoff attempt))
          (recur (inc attempt)))

        ;; Error that should not be retried or max retries exceeded
        :else
        {:success false
         :status status
         :body (when body
                 (try
                   (transit-decode body)
                   (catch Exception _
                     ;; If Transit decode fails, try JSON
                     (try
                       (json/parse-string body true)
                       (catch Exception _
                         body)))))
         :error (or (:error response) (str "HTTP " status))}))))

(defn post
  "Make a POST request with Transit JSON encoding"
  [url body & {:keys [max-retries timeout]
               :or {max-retries 3 timeout 30000}}]
  (request {:method :post
            :url url
            :body body
            :max-retries max-retries
            :timeout timeout}))

(defn get-request
  "Make a GET request"
  [url & {:keys [max-retries timeout]
          :or {max-retries 3 timeout 30000}}]
  (request {:method :get
            :url url
            :max-retries max-retries
            :timeout timeout}))

(defn put
  "Make a PUT request with Transit JSON encoding"
  [url body & {:keys [max-retries timeout]
               :or {max-retries 3 timeout 30000}}]
  (request {:method :put
            :url url
            :body body
            :max-retries max-retries
            :timeout timeout}))

(defn delete
  "Make a DELETE request"
  [url & {:keys [max-retries timeout]
          :or {max-retries 3 timeout 30000}}]
  (request {:method :delete
            :url url
            :max-retries max-retries
            :timeout timeout}))
