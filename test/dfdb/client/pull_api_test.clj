(ns dfdb.client.pull-api-test
  "The pull and entity API.

  Both used to be a query in disguise: the client wrote `[:find (pull ?e …)]`
  and read the first binding out of the result. That cannot ask for an entity
  no pattern matches, cannot pull several entities in one request, and gave
  the pattern no way to carry :as, :limit or a nested spec."
  (:require [clojure.test :refer :all]
            [dfdb.client.core :as dfdb]))

(def test-server-url "http://localhost:8081")

(defn server-running? []
  (try
    (let [conn (dfdb/connect :base-url test-server-url)]
      (dfdb/health conn)
      true)
    (catch Exception _ false)))

(deftest pull-api-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          base (+ 700000 (rand-int 90000))
          nm   (keyword "pullapi" (str "name" base))
          fr   (keyword "pullapi" (str "friend" base))
          alice (+ base 1)
          bob   (+ base 2)]

      (dfdb/transact! conn [{:db/id alice nm "Alice" fr bob}
                            {:db/id bob   nm "Bob"}])

      (testing "pull one entity"
        (let [result (dfdb/pull conn [nm] alice)]
          (is (= "Alice" (get result nm))
              (str "expected Alice, got " result))))

      (testing "pull a nested pattern"
        (let [result (dfdb/pull conn [nm {fr [nm]}] alice)
              friend (get result fr)]
          (is (map? friend) (str "expected a nested map, got " result))
          (is (= "Bob" (get friend nm))
              (str "expected the friend's name, got " friend))))

      (testing "pull several entities, in the order asked"
        (let [results (dfdb/pull conn [nm] [bob alice])]
          (is (= 2 (count results)) (str "expected two results, got " results))
          (is (= "Bob" (get (first results) nm))
              (str "expected Bob first, got " results))))

      (testing "an attribute expression renames what it pulls"
        (let [result (dfdb/pull conn [(list nm :as :name)] alice)]
          (is (= "Alice" (:name result))
              (str "expected the value under :name, got " result))))

      (testing "a default stands in for what is absent"
        (let [missing (keyword "pullapi" (str "nick" base))
              result (dfdb/pull conn [(list missing :default "none")] alice)]
          (is (= "none" (get result missing))
              (str "expected the default, got " result))))

      (testing "entity reads every attribute"
        (let [result (dfdb/entity conn bob)]
          (is (= "Bob" (get result nm))
              (str "expected Bob, got " result))))

      (testing "an entity nobody has written is nothing, not an error"
        (let [result (dfdb/entity conn (+ base 9999))]
          (is (or (nil? result) (empty? (dissoc result :db/id)))
              (str "expected nothing, got " result)))))))

(deftest pull-basis-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          base (+ 800000 (rand-int 90000))
          nm   (keyword "pullbasis" (str "name" base))
          eid  (+ base 1)
          first-tx (:tx-id (dfdb/transact! conn [{:db/id eid nm "before"}]))]

      (dfdb/transact! conn [{:db/id eid nm "after"}])

      (testing "the current value"
        (is (= "after" (get (dfdb/pull conn [nm] eid) nm))))

      (testing "as of the first transaction"
        (let [result (dfdb/pull conn [nm] eid :as-of {:tx first-tx})]
          (is (= "before" (get result nm))
              (str "expected the value at that transaction, got " result))))

      (testing "entity as of the first transaction"
        (let [result (dfdb/entity conn eid :as-of {:tx first-tx})]
          (is (= "before" (get result nm))
              (str "expected the value at that transaction, got " result)))))))

(deftest subscription-rules-test
  (if-not (server-running?)
    (println "\nSKIP: dfdb server not running on" test-server-url "\n")
    (let [conn (dfdb/connect :base-url test-server-url)
          base (+ 600000 (rand-int 90000))
          nm     (keyword "subrule" (str "name" base))
          parent (keyword "subrule" (str "parent" base))
          rules  [[(list 'ancestor '?c '?a) ['?c parent '?a]]
                  [(list 'ancestor '?c '?a) ['?c parent '?p] (list 'ancestor '?p '?a)]]]

      (dfdb/transact! conn [{:db/id (+ base 1) nm "one"}
                            {:db/id (+ base 2) nm "two"   parent (+ base 1)}
                            {:db/id (+ base 3) nm "three" parent (+ base 2)}])

      (testing "a subscription can name the rules its query invokes"
        (let [sub (dfdb/create-subscription
                    conn (str "ancestors-" base)
                    [:find '?a :where (list 'ancestor (+ base 3) '?a)]
                    :rules rules)]
          (try
            (is (:id sub) (str "expected a subscription, got " sub))
            (let [view (dfdb/query-view conn (:id sub))]
              (is (= 2 (count (:results view)))
                  (str "expected both ancestors, got " view)))
            (finally
              (dfdb/delete-subscription conn (:id sub)))))))))
