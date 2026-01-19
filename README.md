# dfdb-client-clj

[![CI](https://github.com/kipz/dfdb-client-clj/actions/workflows/ci.yml/badge.svg)](https://github.com/kipz/dfdb-client-clj/actions/workflows/ci.yml)

Clojure client library for [dfdb-go](https://github.com/kipz/dfdb-go) remote API.

## Installation

Add to your `deps.edn`:

```clojure
io.github.kipz/dfdb-client-clj {:git/url "https://github.com/kipz/dfdb-client-clj"
                                 :git/sha "LATEST_SHA"}
```

## Usage

### Connecting to the server

```clojure
(require '[dfdb.client.core :as dfdb])

(def conn (dfdb/connect :base-url "http://localhost:8080"))
```

### Transacting data

```clojure
;; Map notation
(dfdb/transact! conn [{:db/id 1 :user/name "Alice" :user/age 30}])

;; Tuple notation
(dfdb/transact! conn [[:db/add 1 :user/name "Alice"]
                      [:db/add 1 :user/age 30]])

;; With time dimensions
(dfdb/transact! conn
  [{:db/id 1 :user/name "Alice"}]
  :time-dimensions {:time/valid 1000})
```

### Querying data

```clojure
;; Basic query
(dfdb/query conn '[:find ?name :where [?e :user/name ?name]])

;; With parameters
(dfdb/query conn
  '[:find ?name :in $ ?min-age :where [?e :user/name ?name] [?e :user/age ?age] [(>= ?age ?min-age)]]
  :params {"?min-age" 25})

;; Entity lookup via pull
(dfdb/query conn
  '[:find (pull ?e [*]) :in $ ?id :where [?e :db/id ?id]]
  :params {"?id" 1})

;; Pull pattern
(dfdb/query conn
  '[:find (pull ?e [:user/name :user/age])
    :where [?e :user/email "alice@example.com"]])

;; Get all attributes for entity
(dfdb/query conn
  '[:find ?a ?v :in $ ?e :where [?e ?a ?v]]
  :params {"?e" 1})

;; With as-of time
(dfdb/query conn
  '[:find ?name :where [?e :user/name ?name]]
  :as-of {:time/system 1000})
```

### Helper functions

```clojure
;; Get entity by ID
(dfdb/entity conn 1)
;; => {:db/id 1, :user/name "Alice", :user/age 30}

;; Pull specific attributes
(dfdb/pull conn [:user/name :user/age] 1)
;; => {:user/name "Alice", :user/age 30}
```

### Health check

```clojure
(dfdb/health conn)
;; => {:status "ok", :time 1234567890}
```

### Subscriptions (Materialized Views)

Subscriptions allow you to create materialized views that are automatically updated when underlying data changes.

#### Creating a subscription

```clojure
(def sub (dfdb/create-subscription conn "active-users"
           '[:find ?e ?name ?age
             :where [?e :user/name ?name]
                    [?e :user/age ?age]]))
;; => {:id "sub-123" :name "active-users" :active true ...}
```

#### Querying a materialized view

```clojure
;; Get all results
(dfdb/query-view conn (:id sub))
;; => {:results [{:?e 1 :?name "Alice" :?age 30} ...] :total 10}

;; With pagination
(dfdb/query-view conn (:id sub) :limit 10 :offset 0)

;; With filtering
(dfdb/query-view conn (:id sub)
  :filter {"?age" {">" 25}})

;; With sorting (prefix with - for descending)
(dfdb/query-view conn (:id sub)
  :sort ["-?age" "?name"])

;; Combined options
(dfdb/query-view conn (:id sub)
  :filter {"?age" {">=" 21}}
  :sort ["-?age"]
  :limit 20
  :offset 0)
```

#### Managing subscriptions

```clojure
;; List all subscriptions
(dfdb/list-subscriptions conn)
;; => {:subscriptions [{:id "sub-123" :name "active-users" ...}]}

;; Get a subscription by ID
(dfdb/get-subscription conn "sub-123")

;; Update a subscription's query
(dfdb/update-subscription conn "sub-123"
  '[:find ?e ?name :where [?e :user/name ?name]])

;; Delete a subscription
(dfdb/delete-subscription conn "sub-123")
```

## Development

Run tests (requires dfdb-go server running on localhost:8081):

```bash
clj -M:test
```

Start a REPL:

```bash
clj -M:dev
```

## License

MIT
