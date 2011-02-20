
(ns clj-orient.sql
  #^{:doc "This namespace implements the SQL interface for OrientDB."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)
    (com.orientechnologies.orient.core.db.graph ODatabaseGraphTx)))

(defn query
  [db qry]
  (seq (if (instance? ODatabaseGraphTx db)
         (->> (OSQLSynchQuery. qry) (.query (.getUnderlying db)))
         (->> (OSQLSynchQuery. qry) (.query db)))))

(defn run-command!
  [db comm]
  (->> (OCommandSQL. comm) (.command db) .execute))

(defmacro defsqlfn
  [sym argsv syntax & forms]
  `(let [sqlfn# (proxy [com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract]
                       [~(name sym) ~@(if (integer? argsv) [argsv argsv] [(first argsv) (second argsv)])]
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
