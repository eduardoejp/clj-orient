
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
  ([#^ODatabaseGraphTx db, d]
    (cond
      (instance? ODocument d) (OGraphVertex. db d)
      (keyword? d) (OGraphVertex. db (name d))
      (map? d) (let [vtx (OGraphVertex. db)] (doall (for [k (keys d)] (.set vtx (name k) (d k)))) vtx)))
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
       (doall (for [k (keys edge-data)] (.set edg (name k) (get edge-data k))))
       edg)))
  ([#^OGraphVertex n1 edge-type props #^OGraphVertex n2]
   (let [edg (.link n1 n2 (name edge-type))]
     (doall (for [k (keys props)] (.set edg (name k) (props k))))
     edg)))

(defn unlink!
  ([#^ODatabaseGraphTx db #^ODocument n1 #^ODocument n2] (OGraphVertex/unlink db n1 n2))
  ([#^OGraphVertex n1 #^OGraphVertex n2] (.unlink n1 n2)))

(defn linked?
  ([n1 n2]
    (if (or (some #(= (.getIn %) n2) (.getOutEdges n1))
          (some #(= (.getIn %) n1) (.getOutEdges n2)))
      true false))
  ([n1 edgtype n2]
    (if (or (some #(and (= (.getIn %) n2) (= (get-class-name %) (name edgtype))) (.getOutEdges n1))
          (some #(and (= (.getIn %) n1) (= (get-class-name %) (name edgtype))) (.getOutEdges n2)))
      true false))
  ([n1 edgtype direction n2]
   (cond
     (= :-> direction) (if (some #(and (= (.getIn %) n2) (= (get-class-name %) (name edgtype))) (.getOutEdges n1)) true false)
     (= :<- direction) (if (some #(and (= (.getIn %) n1) (= (get-class-name %) (name edgtype))) (.getOutEdges n2)) true false)))
  )

(defn get-edges
  ([vertex dir] (case dir :in (.getInEdges vertex), :out (.getOutEdges vertex), :both (concat (.getInEdges vertex) (.getOutEdges vertex))))
  ([vertex dir data]
    (let [edges (get-edges vertex dir)]
      (if (keyword? data)
        (filter #(= (name data) (get-class-name %)) edges)
        (filter #(reduce (fn [b1 b2] (and b1 b2)) (for [k (keys data)] (= (field % k) (get k data)))) edges)))))

(defn get-vertex [edge dir] (case dir :in (.getIn edge), :out (.getOut edge)))

(defn get-ends
  ([vertex dir]
   (if (= :both dir)
     (concat (get-ends vertex :in) (get-ends vertex :out))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir))))
  ([vertex dir data]
   (if (= :both dir)
     (concat (get-ends vertex :in data) (get-ends vertex :out data))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir data)))))




