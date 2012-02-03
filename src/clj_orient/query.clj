;; Copyright (C) 2011, Eduardo Julián. All rights reserved.
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
  (:use (clj-orient core))
  (:import (java.util HashMap)
    (com.orientechnologies.orient.core.query.nativ ONativeSynchQuery OQueryContextNativeSchema)
    (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)
    (com.orientechnologies.orient.core.db ODatabaseComplex)))

; Native Queries
(defn- _special-cases [^OQueryContextNativeSchema d v]
  (case (first v)
    :$= (.eq d (second v))
    :$not= (.different d (second v))
    :$< (.minor d (second v))
    :$<= (.minorEq d (second v))
    :$> (.major d (second v))
    :$>= (.majorEq d (second v))
    :$like (.like d (second v))
    :$matches (.matches d (second v))
    (.eq d v)))

(defn- map->nquery [qdoc kvs]
  (reduce (fn [^OQueryContextNativeSchema d [k v]]
            (let [d (.field d (name k))
                  d (if (vector? v)
                      (_special-cases d v)
                      (.eq d v))]
              (.and d)))
          qdoc kvs))

(defn native-query
  "Executes a native query that filters results by the class of the documents (as a keyword) and a filtering function.

When provided a filtering function, you will have to make your own query using the available Java methods
for the OQueryContextNativeSchema instance you will be given.

When provided a hash-map, matching will be done like this:
{:field1 val1
 :field2 [<command> val2]}
e.g.
{:country \"USA\",
 :age [:$>= 20]
 :last-name [:$not= \"Smith\"]}

Available commands:
:$=, :$not=, :$<, :$<=, :$>, :$>=, :$like, :$matches

When not provided a command, it works like :$= (equal)."
  [kclass filter-fn]
  (let [qry (proxy [com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery]
              [*db*, (name kclass), (OQueryContextNativeSchema.)]
              (filter [*record*] (.go ^OQueryContextNativeSchema (if (map? filter-fn)
                                                                    (map->nquery *record* filter-fn)
                                                                    (filter-fn *record*)))))]
    (seq (.query *db* qry (to-array nil)))))

; SQL Queries
(defn- prep-arg [v]
  (if (or (keyword? v) (ratio? v))
    (str v)
    v))

(defn- map->hmap [m]
  (let [hmap (HashMap.)]
    (doseq [[k v] m] (.put hmap (name k) (prep-arg v)))
    hmap))

(defn- prep-args [args]
  (to-array (if (map? args)
              [(map->hmap args)]
              (map prep-arg args))))

(defn- paginate [qry args orid]
  (try
    (let [res (.query *db* (OSQLSynchQuery. (str qry " RANGE " orid))
                      (to-array (prep-args args)))]
      (when-not (empty? res)
        (lazy-cat res (paginate qry args (-> res last .getIdentity .next)))))
    (catch com.orientechnologies.orient.core.exception.OQueryParsingException e
      '())))

(defn sql-query
  "Runs the given SQL query with the given parameters (as a Clojure vector or hash-map) and the option to paginate results.
When using positional parameters (?), use a vector.
When using named parameters (:named), use a hash-map."
  ([qry args paginate?]
   (try
     (let [sqry (OSQLSynchQuery. qry)
           res (.query *db* sqry (prep-args args))]
       (if paginate?
         (lazy-cat res (paginate qry args (-> res last .getIdentity .next)))
         (seq res)))
     (catch com.orientechnologies.orient.core.exception.OQueryParsingException e
       '())))
  ([qry args] (sql-query qry args false)))

(defn run-sql-command! "Runs the given SQL command."
  [comm] (-> ^ODatabaseComplex *db* (.command (OCommandSQL. comm))))

(defmacro defsqlfn
  "Defines a new SQL function and installs it on the SQL engine."
  [sym args & body]
  `(let [sqlfn# (proxy [com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract]
                       [~(name sym) ~(count args) ~(count args)]
                  (~'getSyntax [] ~(str sym "(" (apply str (interpose ", " args)) ")"))
                  (~'execute [~'*document* ~args] ~@body)
                  )]
     (-> (com.orientechnologies.orient.core.sql.OSQLEngine/getInstance) (.registerFunction ~(name sym) sqlfn#))
     (def ~sym sqlfn#)))
