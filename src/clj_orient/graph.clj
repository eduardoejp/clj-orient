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

(ns #^{:author "Eduardo Julián",
       :doc "This namespace wraps the GraphDB part of OrientDB."}
  clj-orient.graph
  (:import (com.orientechnologies.orient.core.db.graph OGraphDatabase))
  (:use (clj-orient core))
  (:require [clojure.walk :as walk]
            [clojure.set :as set]))

; Graph-DB fns
(defopener open-graph-db! OGraphDatabase
  "Opens a new OGraphDatabase and binds it to the *db* var. It then returns the OGraphDatabase.")

(defn browse-vertices ""
  ([] (iterator-seq (.browseVertices *db*)))
  ([polymorphic?] (iterator-seq (.browseVertices *db* polymorphic?))))
(defn browse-edges ""
  ([] (iterator-seq (.browseEdges *db*)))
  ([polymorphic?] (iterator-seq (.browseEdges *db* polymorphic?))))
(defn browse-elements "" [kclass polymorphic?] (iterator-seq (.browseElements *db* (name kclass) polymorphic?)))

(defn count-vertices "" [] (.countVertexes *db*))
(defn count-edges "" [] (.countEdges *db*))
(defn count-elements "" [] (+ (count-vertices) (count-edges)))

; Vertices
(defn get-vertex-base-class "" [] (.getVertexBaseClass *db*))

(defn create-vertex-type! ""
  ([kclass] (.createVertexType *db* (name kclass)))
  ([kclass ksuperclass] (.createVertexType *db* (name kclass) (name ksuperclass))))

(defn vertex ""
  ([] (.createVertex *db*))
  ([kclass-or-map]
    (cond
      (keyword? kclass-or-map) (.createVertex *db* (name kclass-or-map))
      (map? kclass-or-map) (update! (.createVertex *db*) kclass-or-map)))
  ([kclass hmap] (let [vtx (vertex kclass)] (update! vtx hmap) vtx)))

(defn remove-vertex! "" [vertex] (.removeVertex *db* vertex))

(defn get-root "Returns the root node of the given name (as a keyword)."
  [root-name]
  (.getRoot *db* (name root-name)))

(defn add-root! "Sets up a root node with the given name (as a keyword)."
  [root-name vertex]
  (.setRoot *db* (name root-name) vertex))

; Edges
(defn get-edge-base-class "" [] (.getEdgeBaseClass *db*))

(defn create-edge-type! ""
  ([kclass] (.createEdgeType *db* (name kclass)))
  ([kclass ksuperclass] (.createEdgeType *db* (name kclass) (name ksuperclass))))

(defn link!
"Creates an edge between 2 vertices. An optional edge-type (as a :keyword) or hash-map can be passed to set the type and properties."
  ([v1 v2] (.createEdge *db* v1 v2))
  ([v1 edge-data v2]
   (cond
     (keyword? edge-data) (.createEdge *db* v1 v2 (name edge-data))
     (map? edge-data) (update! (.createEdge *db* v1 v2) edge-data)))
  ([v1 edge-type props v2] (update! (link! v1 (name edge-type) v2) props)))

(defn remove-edge! "" [edge] (.removeEdge *db* edge))

(defn get-links "Returns the set of edges between 2 vertices."
  ([v1 v2] (set (.getEdgesBetweenVertexes *db* v1 v2)))
  ([v1 v2 labels] (set (.getEdgesBetweenVertexes *db* v1 v2 (into-array String (map name labels)))))
  ([v1 v2 labels edge-types] (set (.getEdgesBetweenVertexes *db* v1 v2
                                                            (into-array String (map name labels))
                                                            (into-array String (map name edge-types))))))

(defn linked? "Returns whether 2 vertexes are linked."
  [v1 v2] (not (empty? (get-links v1 v2))))

(defn unlink! "Removes all the edges between 2 vertices"
  [v1 v2] (dorun (map #(remove-edge! %) (get-links v1 v2))))

(defn get-vertex "Gets the :in or the :out vertex of an edge."
  [edge dir] (case dir :in (.getInVertex *db* edge), :out (.getOutVertex *db* edge)))

(defn get-edges "Gets the :in edges, the :out edges or :both edges from a vertex."
  ([vertex dir] (case dir
                  :in (set (.getInEdges *db* vertex)),
                  :out (set (.getOutEdges *db* vertex)),
                  :both (set/union (get-edges vertex :in) (get-edges vertex :out))))
  ([vertex dir ktype-or-hmap]
   (if (= :both dir)
     (set/union (get-edges vertex :in ktype-or-hmap) (get-edges vertex :out ktype-or-hmap))
     (if (keyword? ktype-or-hmap)
       (case dir
         :in (set (.getInEdges *db* vertex (name ktype-or-hmap))),
         :out (set (.getOutEdges *db* vertex (name ktype-or-hmap))))
       (case dir
         :in (set (.getInEdgesHavingProperties *db* vertex (walk/stringify-keys ktype-or-hmap)))
         :out (set (.getOutEdgesHavingProperties *db* vertex (walk/stringify-keys ktype-or-hmap)))))
     )))

(defn get-ends
  "Gets the :in edges, the :out edges or :both edges from a vertex."
  ([vertex dir]
   (if (= :both dir)
     (concat (get-ends vertex :in) (get-ends vertex :out))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir))))
  ([vertex dir ktype-or-hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in ktype-or-hmap) (get-ends vertex :out ktype-or-hmap))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir ktype-or-hmap))))
  ([vertex dir ktype hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in ktype hmap) (get-ends vertex :out ktype hmap))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir ktype hmap)))))
