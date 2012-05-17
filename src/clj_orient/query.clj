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

(ns ^{:author "Eduardo Julian <eduardoejp@gmail.com>",
       :doc "This namespace wraps the querying functionality, both for native queries and SQL queries."}
  clj-orient.query
  (:refer-clojure :exclude [load])
  (:use (clj-orient core))
  (:import (java.util HashMap)
    (com.orientechnologies.orient.core.query.nativ ONativeSynchQuery OQueryContextNativeSchema)
    (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)
    (com.orientechnologies.orient.core.db ODatabaseComplex)
    (clj_orient.core CljODoc)))

; Native Queries
(def +n-operators+ #{:$= :$not= :$< :$<= :$> :$>= :$like :$matches})
(def op->meth {:$= '.eq, :$not= '.different, :$like '.like, :$matches '.matches,
               :$< '.minor, :$<= '.minorEq, :$> '.major, :$>= '.majorEq})

(defn- _special-cases "Adds the operator methods to the hash-map fn." [v]
  (if (+n-operators+ (first v))
    (list (op->meth (first v)) (second v))
    (list '.eq v)))

(defn- map->fn "Constructs the filter fn from the passed hash-map."
  [kvs]
  (->> (reduce (fn [f [k v]]
                 (conj f (list '.field (name k))
                       (if (vector? v)
                         (_special-cases v)
                         (list '.eq v))
                       '.and))
               '(% ->) kvs)
    rest
    (cons '.go)
    reverse
    (list 'fn '[%])
    eval))

(defn ->native-query
  "Takes either a function or a hash-map and returns an ONativeSynchQuery object.

When provided a filtering function, you will have to make your own query using the available Java methods
for the OQueryContextNativeSchema instance you will be given.

When provided a hash-map, matching will be done like this:
{:field1 val1
 :field2 [<command> val2]}

e.g.
{:country \"USA\",
 :age [:$>= 20]
 :last-name [:$not= \"Smith\"]}

Available operators:
:$=, :$not=, :$<, :$<=, :$>, :$>=, :$like, :$matches

When not provided a command, it works like :$= (.equals/.eq)."
  [kclass fn-kvs]
  (let [f (if (fn? fn-kvs)
            fn-kvs
            (if (empty? fn-kvs)
              (fn [_] true)
              (map->fn fn-kvs)))]
    (proxy [com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery]
      [*db*, (kw->oclass-name kclass), (OQueryContextNativeSchema.)]
      (filter [*record*] (f *record*)))))

(defn native-query
  "Executes a native query that filters results by the class of the documents (as a keyword) and a filtering function.
It takes either an ONativeSynchQuery object, a function or a hash-map.
Returns results as a lazy-seq of CljODoc objects."
  [klass query]
  (let [query (if (instance? ONativeSynchQuery query) query (->native-query klass query))]
    (map #(CljODoc. %) (.query *db* query (to-array nil)))))

; SQL Queries
(defn- map->hmap [m]
  (let [hmap (HashMap.)]
    (doseq [[k v] m] (.put hmap (name k) (prop-in v)))
    hmap))

(defn- prep-args [args]
  (to-array (if (map? args)
              [(map->hmap args)]
              (map prop-in args))))

(defn- paginate [qry args orid]
  (let [res (.query *db* (OSQLSynchQuery. (str qry " RANGE " orid))
              (prep-args args))]
    (if-not (empty? res) (lazy-cat (map #(CljODoc. %) res) (paginate qry args (-> res last .getIdentity .next))))))

(defn sql-query
  "Runs the given SQL query with the given parameters (as a Clojure vector or hash-map) and the option to paginate results.
When using positional parameters (?), use a vector.
When using named parameters (:named), use a hash-map."
  [qry args & [paginate?]]
  (let [sqry (OSQLSynchQuery. qry)
        res (.query *db* sqry (prep-args args))]
    (if paginate?
      (lazy-cat (map #(CljODoc. %) res) (paginate qry args (-> res last .getIdentity .next)))
      (map #(CljODoc. %) res))))

(defn run-sql-command! "Runs the given SQL command."
  [comm] (-> ^ODatabaseComplex *db* (.command (OCommandSQL. comm))) nil)

(defmacro defsqlfn
  "Defines a new SQL function and installs it on the SQL engine."
  [sym args & body]
  `(let [sqlfn# (proxy [com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract]
                  [~(name sym) ~(count args) ~(count args)]
                  (~'getSyntax [] ~(str sym "(" (apply str (interpose ", " (rest args))) ")"))
                  (~'execute [~'*document* args# ~'*request*] (let [~args args#] ~@body))
                  )]
     (-> (com.orientechnologies.orient.core.sql.OSQLEngine/getInstance) (.registerFunction ~(name sym) sqlfn#))
     (def ~sym sqlfn#)))
