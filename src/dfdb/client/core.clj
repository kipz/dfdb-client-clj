(ns dfdb.client.core
  "Clojure client for dfdb-go remote API"
  (:require [dfdb.client.http :as http]
            [dfdb.client.websocket :as ws]))

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

(defn- parse-query
  "Parse a query to extract its structure.
  Returns the query as a vector (parsing string queries if needed)"
  [query]
  (cond
    (vector? query) query
    (string? query) (try
                      (read-string query)
                      (catch Exception _
                        ;; If parsing fails, return nil (caller will handle)
                        nil))
    :else nil))

(defn- extract-in-clause
  "Extract the :in clause from a parsed query vector.
  Returns a vector of input bindings, or nil if no :in clause"
  [parsed-query]
  (when (vector? parsed-query)
    (let [in-idx (.indexOf parsed-query :in)]
      (when (>= in-idx 0)
        (let [where-idx (.indexOf parsed-query :where)
              end-idx (if (>= where-idx 0) where-idx (count parsed-query))]
          (vec (subvec parsed-query (inc in-idx) end-idx)))))))

(defn- collection-binding?
  "Check if a binding is a collection binding: [?var ...]"
  [binding]
  (and (vector? binding)
       (>= (count binding) 2)
       (= '... (last binding))))

(defn- tuple-binding?
  "Check if a binding is a tuple binding: [?var1 ?var2 ...]"
  [binding]
  (and (vector? binding)
       (not= '... (last binding))
       (> (count binding) 1)))

(defn- relation-binding?
  "Check if a binding is a relation binding: [[?var1 ?var2 ...]]"
  [binding]
  (and (vector? binding)
       (= 1 (count binding))
       (vector? (first binding))))

(defn- non-scalar-binding?
  "Check if a binding is non-scalar (collection, tuple, or relation)"
  [binding]
  (or (collection-binding? binding)
      (tuple-binding? binding)
      (relation-binding? binding)))

(defn- binding-param-key
  "Get the parameter key for a binding.
  For scalars: '?var' -> '?var'
  For collections: '[?var ...]' -> '[?var ...]'"
  [binding]
  (cond
    (symbol? binding) (str binding)
    (vector? binding) (pr-str binding)
    :else (str binding)))

(defn- convert-params-to-positional
  "Convert named params map to positional array for non-scalar bindings.
  Returns params in the order specified by the :in clause"
  [params in-clause]
  (when (and params in-clause (map? params))
    ;; Skip $ (database) which is always first in :in clause
    (let [bindings (if (= '$ (first in-clause))
                     (rest in-clause)
                     in-clause)]
      (mapv (fn [binding]
              (let [key (binding-param-key binding)]
                (get params key)))
            bindings))))

(defn- should-use-positional-params?
  "Check if params should be converted to positional array.
  Returns true if query has non-scalar bindings and params is a map"
  [query params]
  (when (and (map? params) (not-empty params))
    (let [parsed (parse-query query)
          in-clause (extract-in-clause parsed)]
      (when in-clause
        ;; Check if any binding (except $) is non-scalar
        (let [bindings (if (= '$ (first in-clause))
                         (rest in-clause)
                         in-clause)]
          (some non-scalar-binding? bindings))))))

(defn- format-filters
  "Normalise :filters into the wire's list of {:name ... :args [...]}.

  A filter is named rather than passed, because a predicate is a closure and
  a closure cannot cross the wire: the server decides what may be selected,
  and the request only chooses among them. Accepts a bare name, a name with
  arguments, or a map."
  [filters]
  (mapv (fn [f]
          (cond
            (map? f) (cond-> {:name (name (or (:name f) (get f "name")))}
                       (seq (:args f)) (assoc :args (vec (:args f))))
            (sequential? f) {:name (name (first f)) :args (vec (rest f))}
            :else {:name (name f)}))
        filters))

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
    :limit - Maximum number of results to return
    :offset - Number of results to skip
    :rules - Rule definitions the query may invoke

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
      :as-of {:time/system 1000})

    ;; With rules
    (query conn
      '[:find ?name :where (ancestor 1 ?a) [?a :person/name ?name]]
      :rules '[[(ancestor ?c ?a) [?c :person/parent ?a]]
               [(ancestor ?c ?a) [?c :person/parent ?p] (ancestor ?p ?a)]])"
  [conn query & {:keys [params as-of limit offset rules filters]}]
  (let [url (str (:base-url conn) "/api/query")
        ;; Convert params to positional array if query has non-scalar bindings
        converted-params (if (should-use-positional-params? query params)
                           (let [parsed (parse-query query)
                                 in-clause (extract-in-clause parsed)]
                             (convert-params-to-positional params in-clause))
                           params)
        request-body (cond-> {:query (format-query query)}
                       converted-params (assoc :params converted-params)
                       rules (assoc :rules (format-query rules))
                       as-of (assoc :as-of as-of)
                       (seq filters) (assoc :filters (format-filters filters))
                       limit (assoc :limit limit)
                       offset (assoc :offset offset))
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

(defn define-schema
  "Define schema attributes on the server

  This enables unique identity constraints for upsert behavior,
  cardinality-many attributes, component relationships, and other
  schema features.

  Args:
    conn - Connection created with `connect`
    schema - EDN schema definition (string or vector)

  Returns:
    Map with keys:
      :status - \"ok\" on success
      :count - Number of attributes defined

  Examples:
    ;; Define unique identity for upsert
    (define-schema conn
      \"[{:db/ident :image/digest
         :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one
         :db/unique :db.unique/identity}]\")

    ;; Define multiple attributes
    (define-schema conn
      [{:db/ident :package/purl
        :db/valueType :db.type/string
        :db/cardinality :db.cardinality/one
        :db/unique :db.unique/identity}
       {:db/ident :package/tags
        :db/valueType :db.type/string
        :db/cardinality :db.cardinality/many}])

  Schema attribute options:
    :db/ident       - Attribute name (required)
    :db/valueType   - :db.type/string, :db.type/long, :db.type/boolean,
                      :db.type/float, :db.type/double, :db.type/ref,
                      :db.type/instant, :db.type/uuid, :db.type/bytes
    :db/cardinality - :db.cardinality/one (default) or :db.cardinality/many
    :db/unique      - :db.unique/identity (enables upsert) or :db.unique/value
    :db/index       - true to create dedicated index
    :db/fulltext    - true to enable fulltext search (strings only)
    :db/isComponent - true for component entities (cascade delete)
    :db/doc         - Documentation string"
  [conn schema]
  (let [url (str (:base-url conn) "/api/schema")
        schema-str (if (string? schema)
                     schema
                     (pr-str schema))
        request-body {:schema schema-str}
        response (http/post url
                            request-body
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Define schema failed: " (:error response))
                      {:response response})))))

;; Helper functions for common operations

(defn- post-api
  "POST to an API path, returning the body or throwing with the server's error."
  [conn path body]
  (let [response (http/post (str (:base-url conn) path)
                            body
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Request to " path " failed: " (:error response))
                      {:response response})))))

(defn- get-api
  "GET an API path, returning the body or throwing with the server's error."
  [conn path]
  (let [response (http/get-request (str (:base-url conn) path)
                                   :timeout (:timeout conn)
                                   :max-retries (:max-retries conn))]
    (if (:success response)
      (:body response)
      (throw (ex-info (str "Request to " path " failed: " (:error response))
                      {:response response})))))

(defn- basis-request
  "Add the temporal part of a read to a request body.

  Every read endpoint takes the same three: an as-of map naming a
  transaction, an instant or a custom dimension; a since bound; and history.
  Filters ride along the same way, since the server resolves both together."
  ([body as-of since history]
   (basis-request body as-of since history nil))
  ([body as-of since history filters]
   (cond-> body
     (seq as-of) (assoc :as-of as-of)
     since (assoc :since since)
     history (assoc :history true)
     (seq filters) (assoc :filters (format-filters filters)))))

(defn entity
  "Get all attributes of an entity by ID

  Args:
    conn - Connection
    entity-id - Entity ID

  Options:
    :as-of   - Map of time dimension names to bounds
    :since   - Exclude everything asserted at or before this transaction
    :history - Show retractions, and every value an attribute has held

  Returns:
    Entity map with all attributes, or an empty map if nothing is written
    under that ID.

  Example:
    (entity conn 1)
    ;; => {:db/id 1, :user/name \"Alice\", :user/age 30}"
  [conn entity-id & {:keys [as-of since history filters]}]
  (:entity (post-api conn "/api/entity"
                     (basis-request {:entity entity-id} as-of since history filters))))

(defn pull
  "Execute a pull pattern on an entity, or on several

  The pattern is Datomic's: attributes, `*`, nested maps, recursion by `...`
  or a depth, and attribute expressions such as (:user/name :as :name),
  (:user/tags :limit 5) and (:user/nick :default \"none\").

  Args:
    conn - Connection
    pattern - Pull pattern
    entity-id - Entity ID, or a collection of them

  Options:
    :as-of   - Map of time dimension names to bounds
    :since   - Exclude everything asserted at or before this transaction
    :history - Show retractions, and every value an attribute has held

  Returns:
    One entity map, or — given a collection — one per entity in the order
    asked.

  Example:
    (pull conn [:user/name :user/age] 1)
    ;; => {:db/id 1 :user/name \"Alice\", :user/age 30}
    (pull conn [:user/name] [1 2])
    ;; => [{:db/id 1 :user/name \"Alice\"} {:db/id 2 :user/name \"Bob\"}]"
  [conn pattern entity-id & {:keys [as-of since history filters]}]
  (let [many? (and (coll? entity-id) (not (map? entity-id)))
        body (basis-request
               (if many?
                 {:entities (vec entity-id) :pattern (vec pattern)}
                 {:entity entity-id :pattern (vec pattern)})
               as-of since history filters)
        result (post-api conn "/api/pull" body)]
    (if many? (:results result) (:result result))))

;; Subscription API - Materialized Views

(defn create-subscription
  "Create a subscription with a materialized view

  Args:
    conn - Connection created with `connect`
    name - Human-readable name for the subscription
    query - Datalog query that defines the materialized view

  Options:
    :rules - Rule definitions the query may invoke. A subscription on a rule
             query recomputes after each transaction rather than updating
             incrementally, because a rule's answer set is a fixpoint.

  Returns:
    Map with subscription details including :id

  Example:
    (create-subscription conn \"active-users\"
      '[:find ?e ?name :where [?e :user/active true] [?e :user/name ?name]])"
  [conn name query & {:keys [rules]}]
  (let [url (str (:base-url conn) "/api/subscriptions")
        request-body (cond-> {:name name
                              :query (format-query query)}
                       rules (assoc :rules (format-query rules)))
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

;; WebSocket Delta Streaming API

(defn stream-connect
  "Connect to the WebSocket delta stream

  Args:
    conn - Connection created with `connect`

  Options:
    :on-error - Callback for error messages (fn [{:keys [message code]}])
    :on-close - Callback when connection closes (fn [status])
    :on-ack   - Callback for ack messages (fn [{:keys [action subscription-ids]}])

  Returns:
    DeltaStream that can be used with stream-subscribe!, stream-unsubscribe!, etc.

  Example:
    (def stream (dfdb/stream-connect conn
                  :on-error (fn [e] (println \"Error:\" (:message e)))))"
  [conn & {:keys [on-error on-close on-ack]}]
  (ws/connect conn :on-error on-error :on-close on-close :on-ack on-ack))

(defn stream-subscribe!
  "Subscribe to delta updates for subscriptions via WebSocket

  Args:
    stream - DeltaStream from `stream-connect`
    subscription-ids - Subscription ID or vector of IDs
    callback - Function called when deltas arrive
               (fn [{:keys [subscription-id additions retractions timestamp]}])

  Example:
    (dfdb/stream-subscribe! stream [(:id sub)]
      (fn [{:keys [additions retractions]}]
        (println \"Added:\" (count additions) \"Removed:\" (count retractions))))"
  [stream subscription-ids callback]
  (ws/subscribe-deltas! stream subscription-ids callback))

(defn stream-unsubscribe!
  "Unsubscribe from delta updates

  Args:
    stream - DeltaStream from `stream-connect`
    subscription-ids - Subscription ID or vector of IDs

  Example:
    (dfdb/stream-unsubscribe! stream [(:id sub)])"
  [stream subscription-ids]
  (ws/unsubscribe-deltas! stream subscription-ids))

(defn stream-close!
  "Close the WebSocket stream connection

  Args:
    stream - DeltaStream from `stream-connect`

  Example:
    (dfdb/stream-close! stream)"
  [stream]
  (ws/close! stream))

(defn stream-connected?
  "Check if the WebSocket stream is connected

  Args:
    stream - DeltaStream from `stream-connect`

  Returns:
    true if connected, false otherwise"
  [stream]
  (ws/connected? stream))

;; Index and entity API
;;
;; These reads had no client functions, so asking for the datoms of an
;; attribute, the entity behind an ident, or the transactions in a range meant
;; writing a query that approximated it — or, for the transaction log, could not
;; be done at all.

(defn- scan-body
  "The request body the two index walks share; they differ only in path."
  [index components limit as-of since history filters]
  (basis-request (cond-> {:index (name index)}
                   (seq components) (assoc :components (vec components))
                   limit (assoc :limit limit))
                 as-of since history filters))

(defn datoms
  "Return datoms from an index, narrowed by leading components.

  The components are the index's own order: entity then attribute then value for
  :eavt, attribute then entity for :aevt, attribute then value for :avet, value
  then attribute for :vaet. Fewer components means a wider range.

  Args:
    conn - Connection
    index - :eavt, :aevt, :avet or :vaet

  Options:
    :components - Leading components to narrow by
    :limit - Maximum datoms to return

  Example:
    (datoms conn :aevt :components [:user/name])
    ;; => {:datoms [{:e 1 :a \":user/name\" :v \"Alice\" :tx 1 :added true}]}"
  [conn index & {:keys [components limit as-of since history filters]}]
  (post-api conn "/api/datoms"
            (scan-body index components limit as-of since history filters)))

(defn seek-datoms
  "Return datoms from an index, starting at the components and running on.

  Unlike `datoms` the components are a starting point rather than a range to
  stay within, so the scan continues past them to the end of the index.

  Example:
    (seek-datoms conn :aevt :components [:user/name])"
  [conn index & {:keys [components limit as-of since history filters]}]
  (post-api conn "/api/seek-datoms"
            (scan-body index components limit as-of since history filters)))

(defn index-range
  "Return an attribute's datoms whose value lies in [start, end).

  Only meaningful for an attribute carrying an AVET index; without one the
  server refuses rather than scanning and sorting, which would answer while
  hiding that the index is missing.

  Example:
    (index-range conn :user/age :start 25 :end 40)"
  [conn attribute & {:keys [start end limit as-of since history filters]}]
  (post-api conn "/api/index-range"
            (basis-request (cond-> {:attribute (str attribute)}
                             (some? start) (assoc :start start)
                             (some? end) (assoc :end end)
                             limit (assoc :limit limit))
                           as-of since history filters)))

(defn tx-range
  "Return the transactions in [start, end), each with its datoms.

  Example:
    (tx-range conn 0 10)"
  [conn start end]
  (post-api conn "/api/tx-range" {:start start :end end}))

(defn entid
  "Resolve a :db/ident keyword to the entity that carries it."
  [conn ident]
  (:entity (post-api conn "/api/entid" {:ident (str ident)})))

(defn ident
  "Name an entity, the reverse of `entid`.

  An entity carrying no :db/ident has no name, which comes back as the empty
  string rather than an error."
  [conn entity-id & {:keys [as-of since history filters]}]
  (:ident (post-api conn "/api/ident"
                    (basis-request {:entity entity-id} as-of since history filters))))

(defn attribute
  "Report what the database knows about an attribute: whether it is indexed,
  whether its values are references, and how many datoms it has."
  [conn attr]
  (post-api conn "/api/attribute" {:attribute (str attr)}))

(defn db-stats
  "Summarise the database value at a basis: its datom count, its attribute
  count, and the transaction it is read at.

  Distinct from `stats`, which reports the query optimizer's cache."
  [conn & {:keys [as-of since history filters]}]
  (post-api conn "/api/db-stats"
            (basis-request {} as-of since history filters)))

(defn stats
  "Return the query optimizer's statistics: index sizes and, per attribute,
  cardinality, datom count and selectivity."
  [conn]
  (get-api conn "/api/stats"))
