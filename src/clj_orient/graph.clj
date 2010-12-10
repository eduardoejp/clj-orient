
(ns clj-orient.graph
  #^{:doc "This namespace implements the graph oriented part of the DB."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.db.graph ODatabaseGraphTx
                                                       OGraphVertex OGraphEdge)
    (com.orientechnologies.orient.core.record.impl ODocument)
    (com.orientechnologies.orient.client.remote OServerAdmin))
  (:use (clj-orient core)))

;(set! *warn-on-reflection* true)

(defn create-graph-db!
  "Given the DBs location as either :local, :remote or :memory and the path, creates the desired DB."
  ([db-loc dbpath]
   (.create (ODatabaseGraphTx. (str (name db-loc) ":" dbpath))))
  ([db-loc dbpath storage]
   (-> (OServerAdmin. (str "remote:" dbpath)) .connect (.createDatabase (name storage)))))

(defn open-graph-db!
"Given the DBs location as either :local, :remote or :memory and the path (with optional admin and password data), opens
the desired DB."
  ([db-loc dbpath mode]
    (-> (ODatabaseGraphTx. (str (name db-loc) dbpath)) (.open (name mode) (name mode))))
  ([db-loc dbpath admin passw]
   (-> (ODatabaseGraphTx. (str (name db-loc) dbpath)) (.open admin passw))))

(defn as-vertex
  [#^ODatabaseGraphTx db d]
  (if (map? d)
    (OGraphVertex. db (as-document d))
    (OGraphVertex. db d)))

(defn add-root
  ([#^ODatabaseGraphTx db root-name]
    (let [rt (.createVertex db)]
      (.setRoot db root-name rt)))
  ([#^ODatabaseGraphTx db root-name d]
    (.setRoot db root-name (as-vertex d))))

(defn link
  ([#^OGraphVertex n1 #^OGraphVertex n2]
   (.link n1 n2))
  ([#^OGraphVertex n1 edge-type #^OGraphVertex n2]
   (.link n1 n2 (name edge-type))))

(defn unlink
  ([#^ODatabaseGraphTx db #^ODocument n1 #^ODocument n2] (OGraphVertex/unlink db n1 n2))
  ([#^OGraphVertex n1 #^OGraphVertex n2] (.unlink n1 n2)))

(defn get-root
  [#^ODatabaseGraphTx db root-name]
  (.getRoot db root-name))
