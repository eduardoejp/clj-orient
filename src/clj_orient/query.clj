
(ns clj-orient.query
  "This namespace wraps the querying functionality, both for native queries and SQL queries."
  (:use (clj-orient core))
  (:import (com.orientechnologies.orient.core.query.nativ ONativeSynchQuery OQueryContextNativeSchema)
    (java.util HashMap)
    (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)
    (com.orientechnologies.orient.core.db.graph ODatabaseGraphTx)))

; Native Queries
(defn native-query
  "Executes a native query that filters results by the class of the documents (as a keyword) and a filtering function."
  [kclass filter-fn]
  (let [qry (proxy [com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery]
              [*db*, (name kclass), (OQueryContextNativeSchema.)]
              (filter [*record*] (filter-fn *record*)))]
    (.query *db* qry (to-array nil))))

; SQL Queries
(defn- map->hmap [m]
  (let [hmap (HashMap.)]
    (doall (for [k (keys m)] (.put hmap (name k) (get m k))))
    hmap))

(defn run-query
  "Runs the given SQL query with the given parameters (as a Clojure hash-map) and with the given limit."
  ([qry args limit] (-> *db* (.command (OSQLSynchQuery. qry limit)) (.execute (to-array (map->hmap args)))))
  ([qry extra]
    (cond
      (integer? extra) (-> *db* (.command (OSQLSynchQuery. qry extra)) (.execute (to-array nil)))
      (or (vector? extra) (seq? extra)) (-> *db* (.command (OSQLSynchQuery. qry)) (.execute (to-array extra)))
      (map? extra) (-> *db* (.command (OSQLSynchQuery. qry)) (.execute (to-array [(map->hmap extra)])))))
  ([qry] (-> *db* (.command (OSQLSynchQuery. qry)) (.execute (to-array nil)))))

(defn run-command! "Runs the given SQL command."
  [db comm] (-> (.command db (OCommandSQL. comm)) (.execute (to-array nil))))

(defmacro defsqlfn
  "Defines a new SQL function and installs it on the DB."
  [sym syntax args & forms]
  `(let [sqlfn# (proxy [com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract]
                       [~(name sym) ~@(if (integer? args) [args args] [(first args) (second args)])]
                  (~'getSyntax [] ~syntax)
                  ~(if (and (seq? (first forms)) (vector? (ffirst forms)))
                     (let [cnd `[cond]
                           pairs (for [f forms] (if (or (= (count (first f)) 1)
                                                      (and (>= (count (first f)) 2)
                                                        (not= '& ((first f)(- (count (first f)) 2)))))
                                                  `[(= (count ~'*args*) ~(count (first f))) (let [~(first f) ~'*args*] ~@(rest f))]
                                                  `[:else (let [~(first f) ~'*args*] ~@(rest f))]))
                           cnd (seq (reduce #(conj %1 (first %2) (second %2)) cnd pairs))]
                       `(~'execute [~'*record* ~'*args*] ~cnd))
                     `(~'execute [~'*record* ~'*args*] ~@forms)))]
     (-> com.orientechnologies.orient.core.sql.OSQLEngine/getInstance (.registerFunction ~(name sym) sqlfn#))))
