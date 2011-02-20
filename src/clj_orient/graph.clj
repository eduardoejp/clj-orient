
(ns clj-orient.graph
  #^{:doc "This namespace implements the graph oriented part of the DB."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.db.graph ODatabaseGraphTx
                                                       OGraphElement OGraphVertex OGraphEdge)
    (com.orientechnologies.orient.core.record.impl ODocument)
    (com.orientechnologies.orient.client.remote OServerAdmin))
  (:use (clj-orient core)))

;(set! *warn-on-reflection* true)

(defn open-graph-db!
  [db-loc dbpath user pass]
  (-> (ODatabaseGraphTx. (str (name db-loc) ":" dbpath)) (.open user pass)))

(defn browse-vertexes [#^ODatabaseGraphTx db] (seq (.browseVertexes db)))

(defn vertex
  ([#^ODatabaseGraphTx db] (OGraphVertex. db))
  ([#^ODatabaseGraphTx db #^ODocument d] (OGraphVertex. db d))
  ([#^ODatabaseGraphTx db dclass m]
   (let [vtx (OGraphVertex. db (name dclass))]
     (doall (for [k (keys m)] (.set vtx (name k) (m k))))
     vtx)))

(defn add-root!
  [#^ODatabaseGraphTx db root-name #^OGraphVertex v]
  (.setRoot db (name root-name) v))

(defn get-root [#^ODatabaseGraphTx db root-name] (.getRoot db (name root-name)))

(defn link!
  ([#^OGraphVertex n1 #^OGraphVertex n2]
   (.link n1 n2))
  ([#^OGraphVertex n1 edge-data #^OGraphVertex n2]
   (if (keyword? edge-data)
     (.link n1 n2 (name edge-data))
     (let [edg (.link n1 n2)]
       (doall (for [k (keys edge-data)] (.set edg (name k) (edge-data k))))
       edg)))
  ([#^OGraphVertex n1 edge-type props #^OGraphVertex n2]
   (let [edg (.link n1 n2 (name edge-type))]
     (doall (for [k (keys props)] (.set edg (name k) (props k))))
     edg)))

(defn unlink!
  ([#^ODatabaseGraphTx db #^ODocument n1 #^ODocument n2] (OGraphVertex/unlink db n1 n2))
  ([#^OGraphVertex n1 #^OGraphVertex n2] (.unlink n1 n2)))

(defn related?
  ([n1 n2]
    (if (or (some #(= (.getIn %) n2) (.getOutEdges n1))
          (some #(= (.getIn %) n1) (.getOutEdges n2)))
      true false))
  ([n1 edgtype n2]
    (if (or (some #(and (= (.getIn %) n2) (= (.getClassName (.getDocument %)) (name edgtype))) (.getOutEdges n1))
          (some #(and (= (.getIn %) n1) (= (.getClassName (.getDocument %)) (name edgtype))) (.getOutEdges n2)))
      true false))
  ([n1 edgtype direction n2]
   (cond
     (= :-> direction) (if (some #(and (= (.getIn %) n2) (= (.getClassName (.getDocument %)) (name edgtype))) (.getOutEdges n1)) true false)
     (= :<- direction) (if (some #(and (= (.getIn %) n1) (= (.getClassName (.getDocument %)) (name edgtype))) (.getOutEdges n2)) true false)
     )))
