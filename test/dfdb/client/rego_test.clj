(ns dfdb.client.rego-test
  "Integration tests for the Rego API, which is served by cmd/dfdb-rego"
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]
            [dfdb.client.rego :as rego]))

(def test-server-url "http://localhost:8081")

(def bundle
  "package example

violations contains \"not an admin\" if input.role != \"admin\"

default success := false

success if count(violations) == 0

result := {\"success\": success, \"violations\": violations}
")

(defn rego-served?
  "The Rego endpoints are in a binary of their own, so a plain dfdb server
  answers everything here with 404 and these tests have nothing to run against."
  []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      (rego/list-policies conn)
      true)
    (catch Exception _ false)))

(defn warn-if-absent [f]
  (if (rego-served?)
    (f)
    (println "\nWARNING: no dfdb server with Rego on" test-server-url
             "\nStart one with: go build -o /tmp/dfdb-server ./rego/cmd/dfdb-rego && scripts/start-server.sh\n")))

(use-fixtures :once warn-if-absent)

(deftest test-register-reports-every-entrypoint
  (when (rego-served?)
    (testing "each entrypoint is reported maintained with its shape"
      (let [conn (dfdb/connect :base-url test-server-url)
            policy (rego/register-policy! conn {"p.rego" bundle} :name "example" :input-anchor ":input/role")
            by-path (into {} (map (juxt :path identity) (:entrypoints policy)))]
        (is (some? (:id policy)))
        (is (:maintained (get by-path "data.example.result")))
        (is (= "complete" (:kind (get by-path "data.example.result"))))
        (is (= "set" (:kind (get by-path "data.example.violations"))))
        ;; The verdict holds the whole set of violations, so the set is an
        ;; entrypoint of its own and the response says which key it fills.
        (is (= [{:key "violations" :entrypoint "data.example.violations"}]
               (map #(select-keys % [:key :entrypoint])
                    (:embedded (get by-path "data.example.result")))))))))

(deftest test-refusal-names-the-construct
  (when (rego-served?)
    (testing "a policy outside the fragment is refused, naming what it used"
      (let [conn (dfdb/connect :base-url test-server-url)
            thrown (try
                     (rego/register-policy!
                      conn {"p.rego" "package example\n\nallow if time.now_ns() > 0\n"})
                     nil
                     (catch Exception e e))]
        (is (some? thrown) "a bundle that cannot be maintained must not register")
        (is (re-find #"time\.now_ns" (ex-message thrown)))))))

(defn- rows [conn id]
  (count (:results (dfdb/query-view conn id))))

(deftest test-subscribe-and-maintain
  (when (rego-served?)
    (testing "an input arriving moves the verdict and its violations"
      (let [conn (dfdb/connect :base-url test-server-url)
            policy (rego/register-policy! conn {"p.rego" bundle} :input-anchor ":input/role")
            sub (rego/subscribe! conn (:id policy) "data.example.result")
            violations (:subscription (first (:embedded sub)))
            ;; Counted as deltas from what is already there: the server outlives
            ;; the test run, so any input another test left has a verdict too.
            verdicts-before (rows conn (:subscription sub))
            violations-before (rows conn violations)
            eid (System/currentTimeMillis)]
        (is (some? (:subscription sub)))
        (is (some? violations) "the embedded collection is maintained too")

        (dfdb/transact! conn [{:db/id eid :input/role "admin"}])
        (is (= (inc verdicts-before) (rows conn (:subscription sub)))
            "the admin input has a verdict")
        (is (= violations-before (rows conn violations))
            "and violates nothing")

        (dfdb/transact! conn [{:db/id (inc eid) :input/role "viewer"}])
        (is (= (+ 2 verdicts-before) (rows conn (:subscription sub)))
            "both inputs have a verdict")
        (is (= (inc violations-before) (rows conn violations))
            "the viewer input violates the admin rule")))))

(deftest test-listing
  (when (rego-served?)
    (testing "what was registered can be found again"
      (let [conn (dfdb/connect :base-url test-server-url)
            policy (rego/register-policy! conn {"p.rego" bundle} :input-anchor ":input/role")
            _ (rego/subscribe! conn (:id policy) "data.example.violations")]
        (is (some #(= (:id policy) (:id %)) (:policies (rego/list-policies conn))))
        (is (some #(and (= (:id policy) (:policy %))
                        (= "data.example.violations" (:entrypoint %)))
                  (:subscriptions (rego/list-subscriptions conn))))))))
