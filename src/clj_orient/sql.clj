
(ns clj-orient.sql
  #^{:doc "This namespace implements the SQL interface for OrientDB."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (java.util HashMap)
    (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)
    (com.orientechnologies.orient.core.db.graph ODatabaseGraphTx)))

(defn run-query
  ([db qry limit args]
    (let [params (HashMap.)]
      (doall (for [k (keys args)] (.put params (name k) (get args k))))
      (-> (.command db (OSQLSynchQuery. qry limit)) (.execute (to-array args)))))
  ([db qry extra]
   (let [params (HashMap.)]
     (when (map? extra) (doall (for [k (keys extra)] (.put params (name k) (get extra k)))))
     (cond
       (integer? extra) (-> (.command db (OSQLSynchQuery. qry extra)) (.execute (to-array nil)))
       (seq? extra) (-> (.command db (OSQLSynchQuery. qry)) (.execute (to-array extra)))
       (map? extra) (-> (.command db (OSQLSynchQuery. qry)) (.execute (to-array [params]))))
     ))
  ([db qry] (-> (.command db (OSQLSynchQuery. qry)) (.execute (to-array nil)))))

(defn run-command! [db comm] (-> (.command db (OCommandSQL. comm)) (.execute (to-array nil))))

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

