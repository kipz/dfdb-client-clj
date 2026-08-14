(ns dfdb.client.rego
  "Rego policies maintained by dfdb.

  A registered bundle is compiled, not evaluated: each entrypoint that can be
  maintained from deltas becomes an ordinary dfdb subscription, delivered over
  the same websocket as any other. An entrypoint that cannot be maintained is
  refused with the construct named, so a policy is never silently recomputed and
  never silently answered wrongly."
  (:require [dfdb.client.http :as http]))

(defn- result [response what]
  (if (:success response)
    (:body response)
    (throw (ex-info (str what " failed: " (or (get-in response [:body :message])
                                              (get-in response [:body :error :message])
                                              (:error response)))
                    {:response response}))))

(defn register-policy!
  "Register a Rego bundle and report what can be maintained.

  Args:
    conn - Connection created with `dfdb.client.core/connect`
    modules - Map of file name to Rego source, the file names a refusal points into

  Options:
    :name - Human-readable name for the bundle
    :input-anchor - Attribute every input entity has, which grounds a policy body
                    that reads nothing of the input

  Returns:
    Map with :id and :entrypoints, one entry per rule: :maintained true with its
    :kind, or :maintained false with the :construct refused and where it is.

  Example:
    (register-policy! conn {\"p.rego\" \"package example\\n\\nallow if input.role == \\\"admin\\\"\\n\"})"
  [conn modules & {:keys [name input-anchor]}]
  (result (http/post (str (:base-url conn) "/api/rego/policies")
                     (cond-> {:modules modules}
                       name (assoc :name name)
                       input-anchor (assoc :input-anchor input-anchor))
                     :timeout (:timeout conn)
                     :max-retries (:max-retries conn))
          "Register policy"))

(defn list-policies
  "List the registered bundles and their entrypoints"
  [conn]
  (result (http/get-request (str (:base-url conn) "/api/rego/policies")
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))
          "List policies"))

(defn subscribe!
  "Subscribe to one entrypoint of a registered bundle.

  Args:
    conn - Connection created with `dfdb.client.core/connect`
    policy - Bundle id from `register-policy!`
    entrypoint - Rule path, e.g. \"data.example.allow\"

  Options:
    :name - Human-readable name for the subscription

  Returns:
    Map with :subscription, the dfdb subscription id to stream deltas from, and
    :embedded, one entry per collection the entrypoint's value holds: each is a
    subscription of its own, and :key is the key of the value it fills.

  Example:
    (subscribe! conn (:id policy) \"data.example.result\")"
  [conn policy entrypoint & {:keys [name]}]
  (result (http/post (str (:base-url conn) "/api/rego/subscriptions")
                     (cond-> {:policy policy :entrypoint entrypoint}
                       name (assoc :name name))
                     :timeout (:timeout conn)
                     :max-retries (:max-retries conn))
          "Subscribe to entrypoint"))

(defn list-subscriptions
  "List the entrypoint subscriptions this server is maintaining"
  [conn]
  (result (http/get-request (str (:base-url conn) "/api/rego/subscriptions")
                            :timeout (:timeout conn)
                            :max-retries (:max-retries conn))
          "List Rego subscriptions"))
