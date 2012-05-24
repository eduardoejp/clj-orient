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
  (:import (com.orientechnologies.orient.core.query.nativ ONativeSynchQuery OQueryContextNativeSchema)
    com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
    com.orientechnologies.orient.core.sql.OCommandSQL
    com.orientechnologies.orient.core.db.ODatabaseComplex
    com.orientechnologies.orient.core.command.traverse.OTraverse
    (com.orientechnologies.orient.core.command OCommandPredicate OCommandContext)
    clj_orient.core.CljODoc))

(declare sym->sql item->sql map->sql)

; <Native Queries>
(def ^:private +n-operators+ #{:$= :$not= :$< :$<= :$> :$>= :$like :$matches})
(def ^:private op->meth {:$= '.eq, :$not= '.different, :$like '.like, :$matches '.matches,
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

When not provided a command, it works like :$= (.eq)."
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

; <API Graph Traversals>
(defn- $var "Wraps the OCommandContext object to mediate access to the context variables."
  [ctx k]
  (let [k (name k)
        v (.getVariable ctx k)]
    (if (= k "history")
      (set (map wrap-odoc v))
      v)))

(defn traverse
  "fields = vector of keywords.
target = oclass keyword or ORID.
pred = function that takes a CljODoc and a function that takes a keyword for accessing context variables.
(optional) limit = an integer.

Returns results as a lazy-seq of CljODoc objects."
  [fields target pred & [limit]]
  (-> (OTraverse.)
    (.fields (map name fields))
    (.target (map #(.odoc %) target))
    (.limit (or limit 0))
    (.predicate (reify OCommandPredicate
                  (evaluate [self odoc ctx]
                    (pred (wrap-odoc odoc) (partial $var ctx)))))
    (->> (map wrap-odoc))))

; <SQL Queries>
(def sql-fns (atom [])) ; This atom holds all the defined OSQLFunctions.
(defn install-sql-fns! "Installs the previously defined SQL functions in the database."
  []
  (doseq [[name f] @sql-fns]
    (-> (com.orientechnologies.orient.core.sql.OSQLEngine/getInstance) (.registerFunction name f))))

(defn- map->hmap [m]
  (let [hmap (java.util.HashMap.)]
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
  [qry & [args paginate?]]
  (let [sqry (OSQLSynchQuery. qry)
        res (.query *db* sqry (prep-args args))]
    (if paginate?
      (lazy-cat (map #(CljODoc. %) res) (paginate qry args (-> res last .getIdentity .next)))
      (map #(CljODoc. %) res))))

(defn sql-command! "Runs the given SQL command."
  [comm] (-> ^ODatabaseComplex *db* (.command (OCommandSQL. comm))) nil)

(defmacro defsqlfn
  "Defines a new SQL function and installs it on the SQL engine.
Besides the arguments passed to the function, it will also receive the hidden params *document* and *request*,
of types ODocument and OCommandRequest respectively.

If the function does not access within it's body the hidden params *document* and *request*, a local (Clojure) version of the
function will also be defined."
  [sym args & body]
  (let [[doc-str args body] (if (string? args) [args (first body) (rest body)] [nil args body])
        doc-str (str doc-str "\n\n Available as an OrientDB SQL function.")]
    `(let [sqlfn# (proxy [com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract]
                    [~(name sym) ~(count args) ~(count args)]
                    (~'getSyntax [] ~(str sym "(" (apply str (interpose ", " (rest args))) ")"))
                    (~'execute [~'*document* args# ~'*request*] (let [~args args#] ~@body))
                    )]
       (swap! sql-fns conj [~(name sym) sqlfn#])
       ~(if-not (some #(or (= % '*document*) (= % '*request*)) (flatten body))
          `(defn ~sym ~doc-str ~args ~@body))
       )))

; <Clojure Data-Structure Queries>
(def ^:private in-op->sql
  {"+" "+"
   "-" "-"
   "*" "*"
   "/" "/"
   "mod" "%"
   "=" "="
   "not=" "<>"
   "<" "<"
   "<=" "<=" 
   ">" ">"
   ">=" ">="
   "in?" "in"
   "like?" "like"
   "contains-key?" "CONTAINSKEY"
   "contains-val?" "CONTAINSVALUE"
   "contains-text?" "CONTAINSTEXT"
   "matches?" "matches"
   })

(def ^:private group-in-ops->sql
  {"and" " AND "
   "or" " OR "})

(def ^:private post-ops->sql
  {"nil?" " IS null"
   "not-nil?" " IS NOT null"})

(def ^:private command->sql
  {:create "CREATE"
   :insert "INSERT"
   :update "UPDATE"
   :delete "DELETE"
   :find-refs "FIND REFERENCES"})

(def ^:private truncate->sql {:class "CLASS", :cluster "CLUSTER", :record "RECORD"})
(def ^:private type->sql {:link-set "LINKSET", :link-list "LINKLIST"})
(def ^:private order->sql {:asc "ASC", :desc "DESC"})

(def ^:private special-op? #{"instance?" "aget" "between?" "contains?" "contains-all?" "gremlin" "traverse"})

(def ^:private permission->sql
  {:none "NONE"
   :create "CREATE"
   :read "READ"
   :update "UPDATE"
   :delete "DELETE"
   :all "ALL"})

(defn- special-ops->sql [[op & [_1 _2 _3 _4 & _rest] :as form]]
  (case (name op)
    "instance?" (str _2 " INSTANCEOF " (pr-str (sym->sql _1)))
    "aget" (str _1 "[" (cond (list? _2) (item->sql _2)
                             (vector? _2) (apply str (interpose "," _2))
                             (and (integer? _2) (integer? _3)) (str _2 "-" _3)
                             :else _2)
                "]")
    "between?" (str (sym->sql _1) " BETWEEN " _2 " AND " _3)
    "contains?" (str (sym->sql _1) " CONTAINS "
                     (let [x (item->sql _2)]
                       (cond (.startsWith x "(") x
                             
                             (and (>= (.indexOf x " ") 0)
                                  (not (and (.startsWith x "\"")
                                            (.endsWith x "\""))))
                             (str "(" x ")")
                             
                             :else x)))
    "contains-all?" (str (sym->sql _1) " CONTAINSALL " (let [x (item->sql _2)] (if (.startsWith x "(") x (str "(" x ")"))))
    "gremlin" (str "GREMLIN(" (pr-str _1) ")")
    "traverse" (str "TRAVERSE(" (or _1 0) (str "," (or _2 -1))
                    (if _3 (->> _3 (map name) (interpose ",") (apply str) pr-str (str ",")))
                    (if _4
                      (str "," (let [res (item->sql _4)]
                                 (if (.startsWidth res "(")
                                   res
                                   (str "(" res ")")))))
                    )
    nil
    ))

(defn- sym->sql [k]
  (let [n (name k)]
    (cond
      (.startsWith n "#") (str \@ (.substring n 1))
      (and (.startsWith n "?") (not= n "?")) (str ":" (.substring n 1))
      :else n)))
(defn- form->sql [[op & body :as form]]
  (let [op (name op)]
    (cond (in-op->sql op) (str (item->sql (first body)) " " (in-op->sql op) " " (item->sql (second body)))
          (group-in-ops->sql op) (str "(" (apply str (interpose (group-in-ops->sql op) (map item->sql body))) ")")
          (post-ops->sql op) (str (item->sql (first body)) (post-ops->sql op))
          (special-op? op) (special-ops->sql form)
          (.startsWith op ".") (str (item->sql (first body)) op "(" (apply str (interpose "," (map item->sql (rest body)))) ")")
          :else (str (sym->sql op) "(" (apply str (interpose "," (map item->sql body))) ")")
          )))
(defn- item->sql [x]
  (cond
    (or (symbol? x) (keyword? x)) (sym->sql x)
    (seq? x) (form->sql x)
    (vector? x) (str "[" (apply str (interpose "," (map item->sql x))) "]")
    (map? x) (str "{"
                  (apply str
                         (interpose ", "
                           (map #(apply str (interpose ":" %))
                                (partition 2
                                           (interleave (map #(pr-str (name %)) (keys x))
                                                       (map item->sql (vals x)))))))
                  "}")
    (instance? java.util.regex.Pattern x) (str x)
    (ratio? x) (double x)
    (orid? x) (str x)
    :else (pr-str x)))

(defn- select->sql [s]
  (map (fn [x] (if (vector? x)
                 (str (sym->sql (first x)) " AS " (sym->sql (second x)))
                 (item->sql x)))
       s))
(defn- where->sql [s]
  (apply str
         (interpose " AND "
                    (map #(cond (symbol? %) (sym->sql %)
                                (seq? %) (form->sql %))
                         (filter (complement nil?) s)
                         ))))

(defn- into->sql [f]
  (cond (orid? f) (str f)
        (or (symbol? f) (keyword? f)) (kw->oclass-name f)
        (vector? f) (str "[" (apply str (interpose "," (map item->sql f))) "]")
        :else nil))

(defn- from->sql [f] (if (map? f) (str "(" (map->sql f) ")") (into->sql f)))

(defn- map->sql [{:keys [select traverse update command truncate ; Types of commands/queries
                         grant revoke ; Granting & revoking rights
                         set put add remove ; Ways to update
                         link type ; For document links
                         from into target ; Where to do things
                         classes fields fields* on to ; Extra data for certain operations
                         where ; Tests
                         inverse? order-by limit skip]}] ; Miscellaneous
  (str (cond
         traverse (apply str "TRAVERSE " (interpose "," (map name traverse)))
         update (str "UPDATE " (from->sql update))
         command (command->sql command)
         truncate (str "TRUNCATE " (truncate->sql truncate))
         grant (str "GRANT " (permission->sql grant))
         revoke (str "REVOKE " (permission->sql revoke))
         link (str "CREATE LINK " (name link))
         :else (str "SELECT" (if select (apply str " " (interpose "," (select->sql select))))))
       (cond set (apply str " " (interpose ", " (map (fn [[k v]] (str "SET " (name k) " = " (item->sql v))) set)))
             put (apply str " " (interpose ", " (map (fn [[f [k v]]] (str "PUT " (name f) " = " (item->sql k) "," (item->sql v))) put)))
             add (apply str " " (interpose ", " (map (fn [[k v]] (str "ADD " (name k) " = " (item->sql v))) add)))
             remove (apply str " " (interpose ", " (map (fn [i] (if (coll? i)
                                                                  (str "REMOVE " (name (first i)) " = " (item->sql (second i)))
                                                                  (str "REMOVE " (item->sql i))))
                                                        remove)))
             )
       (if type (str " TYPE " (type->sql type)))
       (cond from (str " FROM " (from->sql from))
             into (str " INTO " (into->sql into))
             target (str " " (from->sql target)))
       (if classes (str " [" (apply str (interpose " " (map kw->oclass-name classes))) "]"))
       (cond fields (str "(" (apply str (interpose "," (map name (keys fields)))) ")"
                         " VALUES "
                         "(" (apply str (interpose "," (map item->sql (vals fields)))) ")")
             fields* (str "(" (apply str (interpose "," (map name (keys fields*)))) ")"
                          " VALUES "
                          (->> fields* vals (apply interleave) (partition (count fields*))
                            (map #(map item->sql %))
                            (map #(interpose "," %))
                            (map #(apply str %))
                            (map #(str "(" % ")"))
                            (interpose ",")
                            (apply str)))
             )
       (if on (str " ON " (kw->oclass-name on)))
       (if to (str " TO " (kw->oclass-name to)))
       (if where (str " WHERE " (where->sql where)))
       (if inverse? " INVERSE")
       (if order-by (apply str " ORDER BY " (interpose " " (map (fn [[n o]] (str (name n) " " (order->sql o))) (partition 2 order-by)))))
       (if limit (str " LIMIT " limit))
       (if skip (str " SKIP " skip))
    )
  )

(defn clj-query "Does a SQL query against the database written as a Clojure map."
  [query-map args] (sql-query (map->sql query-map) args))

(defmacro clj-query* "Same as clj-query, but transforms the map into a SQL string at compile time."
  [query-map args] `(sql-query ~(map->sql query-map) ~args))

(defn clj-command! "Runs a SQL command against the database written as a Clojure map."
  [query-map args] (sql-command! (map->sql query-map)))
