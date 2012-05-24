;; Copyright (C) 2011~2012, Eduardo Julián. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the 
;; Eclipse Public License 1.0
;; (http://opensource.org/licenses/eclipse-1.0.php) which can be found
;; in the file epl-v10.html at the root of this distribution.
;;
;; By using this software in any fashion, you are agreeing to be bound
;; by the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns ^{:author "Eduardo Julián <eduardoejp@gmail.com>",
      :doc "A whole lotta tests."}
  clj-orient.test
  (:use clojure.test)
  (:require [clj-orient.core :as oc]
            [clj-orient.graph :as og]
            [clj-orient.query :as oq]
            [clj-orient.script :as os]
            [clj-orient.schema :as osch])
  (:import clj_orient.core.CljODoc))

; <Tests>
(deftest document-test
  (prn "<TEST START>" 'document-test)
  (let [foo (oc/save! (oc/document :core/foo {:a 1, :b 2.0,
                                              :c "3", :d :4,
                                              :e {:A 1, :B 2}, :f [1 2 3 4]
                                              :g '(1 2 3 4), :h #{1 2 3 4}}))]
    (are [x y] (= x y)
         clj_orient.core.CljODoc (type foo)
         #{:a :b :c :d :e :f :g :h} (set (keys foo))
         foo (->> foo :#rid oc/cluster-pos (vector :core/foo) oc/load)
         :core/foo (:#class foo)
         :core/foo (:#class (oc/load [:core/foo 0])))
    (is (map? foo))
    (is (:#rid foo))
    (is (>= (:#version foo) 0))
    (let [foo (oc/save! (with-meta foo {:meta-1 1, :meta-2 2}))]
      (are [x y] (= x y)
           #{:a :b :c :d :e :f :g :h :__meta__} (set (keys foo))
           #{:meta-1 :meta-2} (set (keys (meta foo))))
      (is (keyword? (:#class foo)))
      (is (> (:#version foo) 0))
      (is (map? (meta foo))))
    ))

(deftest classes-and-clusters
  (prn "<TEST START>" 'classes-and-clusters)
  (are [x y] (= x y)
       (oc/browse-class :core/foo) (oc/browse-cluster :core/foo)
       (oc/count-class :core/foo) (oc/count-cluster :core/foo)
       (oc/count-cluster :core/foo) (oc/count-cluster (oc/cluster-id :core/foo))
       (oc/cluster-name (oc/cluster-id :core/foo)) :core/foo
       (oc/db-closed?) false
       (oc/db-open?) true
       (oc/db-exists?) true
       (:name (oc/db-info)) "test"
       (:url (oc/db-info)) "memory:test"
       )
  (is ((set (oc/cluster-names)) :core/foo))
  )

(deftest transactions
  (prn "<TEST START>" 'transactions)
  (oc/with-tx
    (let [f1 (oc/save! (oc/document :core/foo))
          f2 (oc/save! (oc/document :core/foo))]
      (is (-> f1 :#rid oc/cluster-pos neg?))
      (is (-> f2 :#rid oc/cluster-pos neg?))
      ))
  (let [f1 (oc/load [:core/foo 1])
        f2 (oc/load [:core/foo 2])]
    (is (-> f1 :#rid oc/cluster-pos pos?))
    (is (-> f2 :#rid oc/cluster-pos pos?))
    )
  (doall (map (comp oc/delete! oc/load (partial vector :core/foo)) [1 2]))
  (are [x y] (= x y)
       (oc/count-class :core/foo) 1
       (oc/count-cluster :core/foo) 1
       ))

(deftest orecord-id
  (prn "<TEST START>" 'orecord-id)
  (let [d (oc/load [:core/foo 0])]
    (are [x y] (= x y)
       (-> d :#rid oc/orid->vec) [(oc/cluster-id :core/foo) 0]
       (-> d :#rid) (-> d .odoc .getIdentity)
       (-> d :#rid oc/orid->vec oc/orid) (-> d .odoc .getIdentity)
       (oc/doc->map d) {:a 1, :b 2.0,
                        :c "3", :d :4,
                        :e {:A 1, :B 2}, :f [1 2 3 4]
                        :g (list 1 2 3 4), :h #{1 2 3 4},
                        :__meta__ {:meta-2 2, :meta-1 1}}
       ))
  )

(deftest oclasses
  (prn "<TEST START>" 'oclasses)
  (osch/defoclass core/bar core/foo
    [^:mandatory ^:unique id :long]
    [name :string {:regex #"(\w+)+"}]
    [age :short {:min 0, :max 1000}]
    [^:nullable foo [:link core/foo]])
  (osch/defoclass core/baz core/foo)
  (osch/install-oclasses!)
  ;(oc/derive! (oc/create-class! :core/bar) :core/foo)
  ;(oc/create-class! :core/baz :core/foo)
  ;(oc/save-schema!)
  
  (is (oc/oclass? (oc/oclass :core/foo)))
  (is ((set (oc/oclasses)) (oc/oclass :core/foo)))
  (are [x y] (= x y)
    (oc/class-name (oc/oclass :core/foo)) :core/foo
    (oc/sub-classes :core/foo) #{:core/bar :core/baz})
  (is (oc/exists-class? :core/bar))
  (is (not (oc/exists-class? :noob)))
  (is (oc/superclass? :core/foo :core/bar))
  (is (oc/subclass? :core/bar :core/foo))
  (is (every? (-> (oc/schema-info) :classes)
              [:core/foo :core/bar :core/baz]))
  (oc/save! (oc/document :core/baz {:Ima :baz}))
  (is (not (empty? (oc/browse-class :core/baz))))
  (oc/truncate-class! :core/baz)
  (is (empty? (oc/browse-class :core/baz))))

(deftest schema-properties
  (prn "<TEST START>" 'schema-properties)
  ;(is (oc/create-prop! :core/bar :id :long {:mandatory? true, :index :unique}))
  ;(is (oc/create-prop! :core/bar :name :string {:regex #"(\w+)+"}))
  ;(is (oc/create-prop! :core/bar :age :short {:min 0, :max 1000}))
  ;(is (oc/create-prop! :core/bar :foo [:link :core/foo], {:nullable? true}))
  ;(oc/save-schema!)
  (is (oc/exists-prop? :core/bar :foo))
  (is (not (oc/exists-prop? :core/bar :bar)))
  (are [x y] (= x y)
       #{:id :name :age :foo} (set (oc/props :core/bar))
       1 (count (oc/class-indexes :core/bar))
       ;3 (count (oc/class-cluster-ids :core/foo))
       "0" (:min (oc/prop-info :core/bar :age))
       "1000" (:max (oc/prop-info :core/bar :age))
       (str #"(\w+)+") (str (:regex (oc/prop-info :core/bar :name)))
       true (:mandatory? (oc/prop-info :core/bar :id))
       true (:nullable? (oc/prop-info :core/bar :foo))
       )
  (oc/drop-prop! :core/bar :foo)
  (oc/save-schema!)
  (is (not (oc/exists-prop? :core/bar :foo)))
  (is (oc/indexed? :core/bar [:id]))
  (is (not (oc/indexed? :core/bar [:name :age])))
  )

; It seems like, regardless of what is done, 'before' operations cause exceptions...
(deftest hooks
  (prn "<TEST START>" 'hooks)
  (oc/defhook test-hook "Doc-string."
    (before-create [x] (prn 'before-create x) true)
    (before-read [x] (prn 'before-read x) true)
    (before-update [x] (prn 'before-update x) true)
    (before-delete [x] (prn 'before-delete x) true)
    (after-create [x] (prn 'after-create x))
    (after-read [x] (prn 'after-read x))
    (after-update [x] (prn 'after-update x))
    (after-delete [x] (prn 'after-delete x)))
  
  (oc/add-hook! test-hook)
  (let [x (oc/save! (oc/document :core/foo {:hello :world}))]
    (oc/load [:core/foo 1])
    (oc/save! (assoc x :welcome :back))
    (oc/delete! x))
  (oc/remove-hook! test-hook))

(deftest orecord-bytes
  (prn "<TEST START>" 'orecord-bytes)
  (let [orb (oc/record-bytes (.getBytes "Secret Message"))]
    (is (= "Secret Message" (String. (oc/->bytes orb))))))

(deftest script
  (prn "<TEST START>" 'script)
  (os/run-script! "for(i = 0; i < 1000; i++){ db.query( 'insert into core_foo (count) values ('+i+')' ); }")
  (is (= 1001 (oc/count-class :core/foo)))
  (doall (map oc/delete! (rest (oc/browse-class :core/foo)))))

(deftest graph-test
  (prn "<TEST START>" 'graph-test)
  (og/create-vertex-type! :person)
  (og/create-edge-type! :is-friend)
  (oc/save-schema!)
  
  (let [mankind (oc/save! (og/vertex))
        u1 (oc/save! (og/vertex :person {:name "Bill"}))
        u2 (oc/save! (og/vertex :person {:name "Bob"}))
        rel (oc/save! (og/link! u1 :is-friend {:date (java.util.Date.)} u2))]
    (is (and u1 u2 rel mankind))
    (is (every? #(>= % 0) (map (comp oc/cluster-pos :#rid) [u1 u2 rel mankind])))
    (is (og/add-root! :mankind mankind))
    (is (og/get-root :mankind))
    (is (oc/save! (og/link! u1 mankind)))
    (is (oc/save! (og/link! u2 mankind)))
    (are [x y] (= x y)
         1 (count (og/browse-vertices))
         3 (og/count-vertices)
         3 (count (og/browse-vertices true))
         2 (count (og/browse-edges))
         3 (og/count-edges)
         3 (count (og/browse-edges true))
         6 (og/count-elements)
         u1 (og/get-vertex (first (og/get-links u1 u2)) :out)
         u2 (og/get-vertex (first (og/get-links u1 u2)) :in)
         (og/get-edges u2 :in) (og/get-links u1 u2)
         ;(set (og/get-ends u1 :out)) #{u2 mankind}
         (og/get-ends u1 :out :is-friend) [u2]
         ;(set (og/get-ends u2 :in)) #{u1}
         (og/get-ends u2 :in :is-friend) [u1]
         )
    (is (og/linked? u1 u2))
    (is (og/linked? u1 u2 [:is-friend]))
    (is (og/linked? u1 u2 [:is-friend] [:is-friend]))
    (og/unlink! u1 u2)
    (oc/save! u1) (oc/save! u2)
    (is (not (og/linked? u1 u2)))
    )
  )

(deftest native-query-test
  (prn "<TEST START>" 'native-query-test)
  (are [x y] (= x y)
       1 (count (oq/native-query :person {:name "Bob"}))
       1 (count (oq/native-query :person {:name "Bill"}))
       2 (count (oq/native-query :person {}))
       ))

(deftest sql-test
  (prn "<TEST START>" 'sql-test)
  (oq/defsqlfn notPass [] false)
  (oq/defsqlfn allowBob [n] (= n "Bob"))
  (oq/install-sql-fns!)
  (are [x y] (= x y)
       ;1 (count (oq/sql-query "SELECT FROM person WHERE name = ?" ["Bob"]))
       ;1 (count (oq/sql-query "SELECT FROM person WHERE name = :name" {:name "Bill"}))
       ;2 (count (oq/sql-query "SELECT FROM person" nil))
       ;0 (count (oq/sql-query "SELECT FROM person WHERE notPass()" nil))
       1 (count (oq/clj-query '{:from person :where [(= name ?)]} ["Bob"]))
       1 (count (oq/clj-query '{:from person :where [(= name ?name)]} {:name "Bill"}))
       2 (count (oq/clj-query '{:from person} nil))
       0 (count (oq/clj-query '{:from person :where [(notPass)]} nil))
       "Bob" (:name (first (oq/sql-query "SELECT FROM person WHERE allowBob(name)" nil)))
       ))

(defn test-ns-hook []
  (oc/create-db! "memory:test")
  ;(binding [*test-out* (clojure.java.io/writer (java.io.FileOutputStream. "clj-orient-test-report"))]
    (oc/with-db (og/open-graph-db! "memory:test" "admin" "admin")
      ;(with-test-out
        (document-test)
        (classes-and-clusters)
        (transactions)
        (orecord-id)
        (oclasses)
        (schema-properties)
        (hooks)
        (orecord-bytes)
        (graph-test)
        (native-query-test)
        (sql-test)
        ;(script) ; It's commented out because I'm using an in-memory DB.
        ;)
      )
    ;)
  )

;(run-tests)
