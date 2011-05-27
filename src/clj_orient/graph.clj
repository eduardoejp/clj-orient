
(ns clj-orient.graph
  "This namespace wraps the GraphDB part of OrientDB."
  (:import (com.orientechnologies.orient.core.db.graph OGraphDatabase))
  (:use (clj-orient core)))

(defn open-graph-db! "Opens a new OGraphDatabase and binds it to the *db* var. It then returns the OGraphDatabase."
  [db-loc user pass]
  (set-db! (.open (OGraphDatabase. db-loc) user pass))
  *db*)

(defn browse-vertices "" [] (.browseVertices *db*))
(defn browse-edges "" [] (.browseEdges *db*))
(defn browse-elements "" [] (.browseElements *db*))

(defn count-vertices "" [] (.countVertexes *db*))
(defn count-edges "" [] (.countEdges *db*))

(defn make-vertex ""
  ([] (.createVertex *db*))
  ([kclass-or-doc]
    (cond
      (keyword? kclass-or-doc) (.createVertex *db* (name kclass-or-doc))
      (map? kclass-or-doc) (update! (.createVertex *db*) kclass-or-doc)))
  ([kclass odoc] (let [vtx (make-vertex kclass)] (update! vtx odoc) vtx)))

(defn get-root "" [root-name] (.getRoot *db* (name root-name)))

(defn add-root! "" [root-name vertex] (.setRoot *db* (name root-name) vertex))

(defn link!
"Creates an edge between 2 vertexes. An optional edge-type (as a :keyword), ODocument or hash-map can be passed to set the
type and properties."
  ([v1 v2] (.createEdge *db* v1 v2))
  ([v1 edge-data v2]
   (cond
     (keyword? edge-data) (.createEdge *db* v1 v2 (name edge-data))
     (map? edge-data) (update! (.createEdge *db* v1 v2) edge-data)))
  ([v1 edge-type props v2] (update! (.createEdge *db* v1 v2 (name edge-type)) props)))

(defn remove-vertex "" [vertex] (.removeVertex *db* vertex))
(defn remove-edge! "" [edge] (.removeEdge *db* edge))

(defn get-links ""
  ([v1 v2] (.getEdgesBetweenVertexes v1 v2))
  ([v1 v2 labels] (.getEdgesBetweenVertexes v1 v2 (to-array labels)))
  ([v1 v2 labels edge-types] (.getEdgesBetweenVertexes v1 v2 (to-array labels) (to-array (map keyword edge-types)))))

(defn linked? "Returns whether 2 vertexes are linked."
  [v1 v2] (not (.isEmpty (get-links v1 v2))))

(defn unlink "Removes all the edges between 2 vertices"
  [v1 v2] (doall (for [e (get-links v1 v2)] (remove-edge! e))))

(defn get-edges "Gets the :in edges, the :out edges or :both edges from a vertex."
  ([vertex dir] (case dir :in (.getInEdges *db* vertex), :out (.getOutEdges *db* vertex), :both (concat (.getInEdges *db* vertex) (.getOutEdges *db* vertex))))
  ([vertex dir label-or-hmap]
    (if (keyword? label-or-hmap)
      (case dir
        :in (.getInEdges *db* vertex (name label-or-hmap)),
        :out (.getOutEdges *db* vertex (name label-or-hmap)),
        :both (concat (.getInEdges *db* vertex (name label-or-hmap)) (.getOutEdges *db* vertex (name label-or-hmap))))
      (filter #(reduce (fn [b1 b2] (and b1 b2)) (for [k (keys label-or-hmap)] (= (pget % k) (get label-or-hmap k))))
        (get-edges vertex dir))))
  ([vertex dir label hmap]
    (filter #(reduce (fn [b1 b2] (and b1 b2)) (for [k (keys label hmap)] (= (pget % k) (get label hmap k))))
      (get-edges vertex dir label))))

(defn get-vertex "Gets th :in or the :out vertex of an edge."
  [edge dir] (case dir :in (.getInVertex *db* edge), :out (.getOutVertex *db* edge)))

(defn get-ends
  "Gets the :in edges, the :out edges or :both edges from a vertex."
  ([vertex dir]
   (if (= :both dir)
     (concat (get-ends vertex :in) (get-ends vertex :out))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir))))
  ([vertex dir label-or-hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in label-or-hmap) (get-ends vertex :out label-or-hmap))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir label-or-hmap))))
  ([vertex dir label hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in label hmap) (get-ends vertex :out label hmap))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir label hmap)))))

(defn create-vertex-type ""
  ([ktype] (.createVertexType *db* (name ktype)))
  ([ktype superclass] (.createVertexType *db* (name ktype) (if (keyword? superclass) (name superclass) superclass))))

(defn create-edge-type ""
  ([ktype] (.createEdgeType *db* (name ktype)))
  ([ktype superclass] (.createEdgeType *db* (name ktype) (if (keyword? superclass) (name superclass) superclass))))

(defn get-vertex-base-class "" [] (.getVertexBaseClass *db*))
(defn get-edge-base-class "" [] (.getEdgeBaseClass *db*))
