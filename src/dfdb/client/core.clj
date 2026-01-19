(ns dfdb.client.core
  "Clojure client for dfdb-go remote API"
  (:require [dfdb.client.http :as http]))

(defrecord Connection [base-url timeout max-retries])

(defn connect
  "Create a connection to a dfdb-go server

  Options:
    :base-url - Base URL of the server (e.g., \"http://localhost:8080\")
    :timeout - Request timeout in milliseconds (default: 30000)
    :max-retries - Maximum number of retries (default: 3)

  Example:
    (connect :base-url \"http://localhost:8080\")"
  [& {:keys [base-url timeout max-retries]
      :or {timeout 30000 max-retries 3}
      :as opts}]
  (when-not base-url
    (throw (IllegalArgumentException. ":base-url is required")))
  (map->Connection {:base-url base-url
                    :timeout timeout
                    :max-retries max-retries}))

(defn- format-query
  "Format a query for transmission.
  Queries can be strings or vectors (EDN format)"
  [query]
  (if (string? query)
    query
    (pr-str query)))

(defn transact!
  "Execute a transaction on the server

  Args:
    conn - Connection created with `connect`
    tx-data - Transaction data (vector of maps or tuples)

  Options:
    :time-dimensions - Map of time dimension names to values
    :meta - Transaction metadata

  Returns:
    Map with keys:
      :tx-id - Transaction ID
      :tx-time - Transaction timestamp
      :deltas - Vector of deltas (changes made)
      :temp-id-map - Map of temporary IDs to resolved IDs

  Examples:
    ;; Map notation
    (transact! conn [{:db/id 1 :user/name \"Alice\" :user/age 30}])

    ;; Tuple notation
    (transact! conn [[:db/add 1 :user/name \"Alice\"]
                     [:db/add 1 :user/age 30]])

    ;; With time dimensions
    (transact! conn
      [{:db/id 1 :user/name \"Alice\"}]
      :time-dimensions {:time/valid 1000})"
  [conn tx-data & {:keys [time-dimensions meta]}]
  (let [url (str (:base-url conn) "/api/transact")
        request-body (cond-> {:tx-data tx-data}
                       time-dimensions (assoc :time-dimensions time-dimensions)
                       meta (assoc :meta meta))
        response (http/post url
                            request-body
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Transaction failed: " (:error response))
                      {:response response})))))

(defn query
  "Execute a query on the server

  Args:
    conn - Connection created with `connect`
    query - Query string or vector (EDN format)

  Options:
    :params - Map of parameter bindings (e.g., {\"?age\" 30})
    :as-of - Map of time dimension names to timestamps

  Returns:
    Map with keys:
      :bindings - Vector of binding maps (for non-aggregate queries)
      :aggregate - Vector of aggregate results (for aggregate queries)

  Examples:
    ;; Basic query
    (query conn '[:find ?name :where [?e :user/name ?name]])

    ;; With parameters
    (query conn
      '[:find ?name :in $ ?min-age
        :where [?e :user/name ?name]
               [?e :user/age ?age]
               [(>= ?age ?min-age)]]
      :params {\"?min-age\" 25})

    ;; Entity lookup via pull
    (query conn
      '[:find (pull ?e [*]) :in $ ?id :where [?e :db/id ?id]]
      :params {\"?id\" 1})

    ;; Pull pattern
    (query conn
      '[:find (pull ?e [:user/name :user/age])
        :where [?e :user/email \"alice@example.com\"]])

    ;; Get all attributes for entity
    (query conn
      '[:find ?a ?v :in $ ?e :where [?e ?a ?v]]
      :params {\"?e\" 1})

    ;; With as-of time
    (query conn
      '[:find ?name :where [?e :user/name ?name]]
      :as-of {:time/system 1000})"
  [conn query & {:keys [params as-of]}]
  (let [url (str (:base-url conn) "/api/query")
        request-body (cond-> {:query (format-query query)}
                       params (assoc :params params)
                       as-of (assoc :as-of as-of))
        response (http/post url
                            request-body
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Query failed: " (:error response))
                      {:response response})))))

(defn health
  "Check server health

  Args:
    conn - Connection created with `connect`

  Returns:
    Map with keys:
      :status - Health status (\"ok\" if healthy)
      :time - Server timestamp

  Example:
    (health conn)
    ;; => {:status \"ok\", :time 1234567890}"
  [conn]
  (let [url (str (:base-url conn) "/api/health")
        response (http/get-request url
                                   :timeout (:timeout conn)
                                   :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Health check failed: " (:error response))
                      {:response response})))))

;; Helper functions for common operations

(defn entity
  "Get all attributes of an entity by ID

  Args:
    conn - Connection
    entity-id - Entity ID

  Options:
    :as-of - Map of time dimension names to timestamps

  Returns:
    Entity map with all attributes

  Example:
    (entity conn 1)
    ;; => {:db/id 1, :user/name \"Alice\", :user/age 30}"
  [conn entity-id & {:keys [as-of]}]
  (let [result (query conn
                      '[:find (pull ?e [*]) :in $ ?id :where [?e :db/id ?id]]
                      :params {"?id" entity-id}
                      :as-of as-of)]
    (-> result :bindings first first)))

(defn pull
  "Execute a pull pattern on an entity

  Args:
    conn - Connection
    pattern - Pull pattern (vector of attribute keywords)
    entity-id - Entity ID

  Options:
    :as-of - Map of time dimension names to timestamps

  Returns:
    Entity map with requested attributes

  Example:
    (pull conn [:user/name :user/age] 1)
    ;; => {:user/name \"Alice\", :user/age 30}"
  [conn pattern entity-id & {:keys [as-of]}]
  (let [result (query conn
                      `[:find (~'pull ~'?e ~pattern)
                        :in ~'$ ~'?id
                        :where [~'?e :db/id ~'?id]]
                      :params {"?id" entity-id}
                      :as-of as-of)]
    (-> result :bindings first first)))

;; Subscription API - Materialized Views

(defn create-subscription
  "Create a subscription with a materialized view

  Args:
    conn - Connection created with `connect`
    name - Human-readable name for the subscription
    query - Datalog query that defines the materialized view

  Returns:
    Map with subscription details including :id

  Example:
    (create-subscription conn \"active-users\"
      '[:find ?e ?name :where [?e :user/active true] [?e :user/name ?name]])"
  [conn name query]
  (let [url (str (:base-url conn) "/api/subscriptions")
        request-body {:name name
                      :query (format-query query)}
        response (http/post url
                            request-body
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Create subscription failed: " (:error response))
                      {:response response})))))

(defn list-subscriptions
  "List all subscriptions

  Args:
    conn - Connection created with `connect`

  Returns:
    Map with :subscriptions vector of subscription metadata

  Example:
    (list-subscriptions conn)
    ;; => {:subscriptions [{:id \"sub-1\" :name \"active-users\" ...}]}"
  [conn]
  (let [url (str (:base-url conn) "/api/subscriptions")
        response (http/get-request url
                                   :timeout (:timeout conn)
                                   :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "List subscriptions failed: " (:error response))
                      {:response response})))))

(defn get-subscription
  "Get a subscription by ID

  Args:
    conn - Connection created with `connect`
    id - Subscription ID

  Returns:
    Subscription metadata map

  Example:
    (get-subscription conn \"sub-123\")"
  [conn id]
  (let [url (str (:base-url conn) "/api/subscriptions/" id)
        response (http/get-request url
                                   :timeout (:timeout conn)
                                   :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Get subscription failed: " (:error response))
                      {:response response})))))

(defn update-subscription
  "Update a subscription's query

  Args:
    conn - Connection created with `connect`
    id - Subscription ID
    query - New Datalog query

  Returns:
    Updated subscription metadata

  Example:
    (update-subscription conn \"sub-123\"
      '[:find ?e :where [?e :user/active false]])"
  [conn id query]
  (let [url (str (:base-url conn) "/api/subscriptions/" id)
        request-body {:query (format-query query)}
        response (http/put url
                           request-body
                           :timeout (:timeout conn)
                           :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Update subscription failed: " (:error response))
                      {:response response})))))

(defn delete-subscription
  "Delete a subscription

  Args:
    conn - Connection created with `connect`
    id - Subscription ID

  Returns:
    nil on success

  Example:
    (delete-subscription conn \"sub-123\")"
  [conn id]
  (let [url (str (:base-url conn) "/api/subscriptions/" id)
        response (http/delete url
                              :timeout (:timeout conn)
                              :max-retries (:max-retries conn))]
    (if (:success response)
      nil
      (throw (ex-info (str "Delete subscription failed: " (:error response))
                      {:response response})))))

(defn query-view
  "Query a subscription's materialized view

  Args:
    conn - Connection created with `connect`
    subscription-id - ID of the subscription

  Options:
    :filter - Map of variable to filter criteria
              e.g., {\"?age\" {\">\" 30}} or {\"?name\" \"Alice\"}
    :sort   - Vector of sort fields, prefix with - for descending
              e.g., [\"?age\"] or [\"-?age\" \"?name\"]
    :limit  - Maximum number of results
    :offset - Number of results to skip

  Returns:
    Map with :results (vector of bindings) and :total (total count)

  Examples:
    ;; Get all results
    (query-view conn \"sub-123\")

    ;; With pagination
    (query-view conn \"sub-123\" :limit 10 :offset 0)

    ;; With filter and sort
    (query-view conn \"sub-123\"
      :filter {\"?age\" {\">\" 30}}
      :sort [\"-?age\"]
      :limit 20)"
  [conn subscription-id & {:keys [filter sort limit offset]}]
  (let [url (str (:base-url conn) "/api/subscriptions/" subscription-id "/view")
        has-opts? (or filter sort limit offset)]
    (if has-opts?
      ;; POST with options
      (let [request-body (cond-> {}
                           filter (assoc :filter filter)
                           sort (assoc :sort sort)
                           limit (assoc :limit limit)
                           offset (assoc :offset offset))
            response (http/post url
                                request-body
                                :timeout (:timeout conn)
                                :max-retries (:max-retries conn))]
        (if (:success response)
          (:body response)
          (throw (ex-info (str "Query view failed: " (:error response))
                          {:response response}))))
      ;; GET without options
      (let [response (http/get-request url
                                       :timeout (:timeout conn)
                                       :max-retries (:max-retries conn))]
        (if (:success response)
          (:body response)
          (throw (ex-info (str "Query view failed: " (:error response))
                          {:response response})))))))
