# dfdb-client-clj

Clojure client library for dfdb-go remote API.

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

### Health check

```clojure
(dfdb/health conn)
;; => {:status "ok", :time 1234567890}
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
