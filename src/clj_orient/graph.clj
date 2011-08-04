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

(ns #^{:author "Eduardo Julian <eduardoejp@gmail.com>",
       :doc "This namespace wraps the GraphDB part of OrientDB."}
  clj-orient.graph
  (:import (com.orientechnologies.orient.core.db.graph OGraphDatabase OGraphDatabasePool))
  (:use (clj-orient core))
  (:require [clojure.walk :as walk]
            [clojure.set :as set]))

; Graph-DB fns
(defopener open-graph-db! OGraphDatabasePool
  "Opens and returns a new OGraphDatabase.")

(defn browse-vertices ""
  ([] (browse-vertices false))
  ([polymorphic?] (iterator-seq (.browseVertices #^OGraphDatabase *db* polymorphic?))))
(defn browse-edges ""
  ([] (browse-edges false))
  ([polymorphic?] (iterator-seq (.browseEdges #^OGraphDatabase *db* polymorphic?))))
(defn browse-elements "" [kclass polymorphic?] (iterator-seq (.browseElements #^OGraphDatabase *db* (name kclass) polymorphic?)))

(defn count-vertices "" [] (.countVertexes #^OGraphDatabase *db*))
(defn count-edges "" [] (.countEdges #^OGraphDatabase *db*))
(defn count-elements "" [] (+ (count-vertices) (count-edges)))

; Vertices
(defn get-vertex-base-class "" [] (.getVertexBaseClass #^OGraphDatabase *db*))

(defn create-vertex-type! ""
  ([kclass] (.createVertexType #^OGraphDatabase *db* (name kclass)))
  ([kclass ksuperclass] (.createVertexType #^OGraphDatabase *db* (name kclass) (name ksuperclass))))

(defn vertex "Creates a new vertex. Works just like 'document'."
  ([] (.createVertex #^OGraphDatabase *db*))
  ([kclass-or-map]
    (cond
      (keyword? kclass-or-map) (.createVertex #^OGraphDatabase *db* (name kclass-or-map))
      (map? kclass-or-map) (apply passoc! (.createVertex #^OGraphDatabase *db*) (mapcat identity kclass-or-map))))
  ([kclass hmap] (let [vtx (vertex kclass)] (apply passoc! vtx (mapcat identity hmap)) vtx)))

(defn delete-vertex! "" [vertex] (.removeVertex #^OGraphDatabase *db* vertex))

(defn get-root "Returns the root node of the given name (as a keyword)."
  [root-name]
  (.getRoot #^OGraphDatabase *db* (name root-name)))

(defn add-root! "Sets up a root node with the given name (as a keyword)."
  [root-name vertex]
  (.setRoot #^OGraphDatabase *db* (name root-name) vertex))

; Edges
(defn get-edge-base-class "" [] (.getEdgeBaseClass #^OGraphDatabase *db*))

(defn create-edge-type! ""
  ([kclass] (.createEdgeType #^OGraphDatabase *db* (name kclass)))
  ([kclass ksuperclass] (.createEdgeType #^OGraphDatabase *db* (name kclass) (name ksuperclass))))

(defn link!
"Creates an edge between 2 vertices. An optional edge-type (as a :keyword) or hash-map can be passed to set the type and properties."
  ([v1 v2] (.createEdge #^OGraphDatabase *db* v1 v2))
  ([v1 edge-data v2]
   (cond
     (keyword? edge-data) (.createEdge #^OGraphDatabase *db* v1 v2 (name edge-data))
     (map? edge-data) (apply passoc! (.createEdge #^OGraphDatabase *db* v1 v2) (mapcat identity edge-data))))
  ([v1 edge-type props v2] (apply passoc! (link! v1 (name edge-type) v2) (mapcat identity props))))

(defn delete-edge! "" [edge] (.removeEdge #^OGraphDatabase *db* edge))

(defn get-links "Returns the set of edges between 2 vertices."
  ([v1 v2] (set (.getEdgesBetweenVertexes #^OGraphDatabase *db* v1 v2)))
  ([v1 v2 labels] (set (.getEdgesBetweenVertexes #^OGraphDatabase *db* v1 v2 (into-array String (map name labels)))))
  ([v1 v2 labels edge-types] (set (.getEdgesBetweenVertexes #^OGraphDatabase *db*
                                                            v1 v2
                                                            (into-array String (map name labels))
                                                            (into-array String (map name edge-types))))))

(defn linked? "Returns whether 2 vertexes are linked."
  [v1 v2] (not (empty? (get-links v1 v2))))

(defn unlink! "Removes all the edges between 2 vertices"
  [v1 v2] (dorun (map delete-edge! (get-links v1 v2))))

(defn get-vertex "Gets the :in or the :out vertex of an edge."
  [edge dir]
  (case dir
    :in (.getInVertex #^OGraphDatabase *db* edge)
    :out (.getOutVertex #^OGraphDatabase *db* edge)))

(defn get-edges
  "Gets the :in edges, the :out edges or :both edges from a vertex.
If provided properties to filter the edges, the properties can be a vector or a sequence (to filter edges that have the keys in there)
or a hash-map (to filter edges with properties set to specified values)."
  ([vertex dir] (case dir
                  :in (set (.getInEdges #^OGraphDatabase *db* vertex)),
                  :out (set (.getOutEdges #^OGraphDatabase *db* vertex)),
                  :both (set/union (get-edges vertex :in) (get-edges vertex :out))))
  ([vertex dir kclass-or-props]
   (if (= :both dir)
     (set/union (get-edges vertex :in kclass-or-props) (get-edges vertex :out kclass-or-props))
     (if (keyword? kclass-or-props)
       (case dir
         :in (set (.getInEdges #^OGraphDatabase *db* vertex (name kclass-or-props))),
         :out (set (.getOutEdges #^OGraphDatabase *db* vertex (name kclass-or-props))))
       (case dir
         :in (set (.getInEdgesHavingProperties #^OGraphDatabase *db* vertex (walk/stringify-keys kclass-or-props)))
         :out (set (.getOutEdgesHavingProperties #^OGraphDatabase *db* vertex (walk/stringify-keys kclass-or-props)))))
     ))
  ([vertex dir kclass props]
   (-> (case dir
         :in (.getInEdges #^OGraphDatabase *db* vertex (name kclass)),
         :out (.getOutEdges #^OGraphDatabase *db* vertex (name kclass)))
     (#(.filterEdgesByProperties #^OGraphDatabase *db* % props)))))

(defn get-ends
  "Gets the vertices at the end of the :in edges, the :out edges or :both edges from a vertex."
  ([vertex dir]
   (if (= :both dir)
     (concat (get-ends vertex :in) (get-ends vertex :out))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir))))
  ([vertex dir kclass-or-hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in kclass-or-hmap) (get-ends vertex :out kclass-or-hmap))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir kclass-or-hmap))))
  ([vertex dir kclass hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in kclass hmap) (get-ends vertex :out kclass hmap))
     (map #(get-vertex % (case dir :in :out, :out :in)) (get-edges vertex dir kclass hmap)))))
