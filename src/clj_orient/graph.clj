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
      :doc "This namespace wraps the GraphDB part of OrientDB."}
  clj-orient.graph
  (:refer-clojure :exclude [load])
  (:use clj-orient.core)
  (:import (com.orientechnologies.orient.core.db.graph OGraphDatabase OGraphDatabasePool)
           clj_orient.core.CljODoc)
  (:require [clojure.walk :as walk]))

(declare get-edges)

; Graph-DB fns
(defopener open-graph-db! OGraphDatabasePool
  "Opens and returns a new OGraphDatabase.")

(defn browse-vertices ""
  ([] (browse-vertices false))
  ([polymorphic?] (map #(CljODoc. %) (iterator-seq (.browseVertices ^OGraphDatabase *db* polymorphic?)))))
(defn browse-edges ""
  ([] (browse-edges false))
  ([polymorphic?] (map #(CljODoc. %) (iterator-seq (.browseEdges ^OGraphDatabase *db* polymorphic?)))))
(defn browse-elements ""
  ([kclass] (browse-elements kclass false))
  ([kclass polymorphic?] (map #(CljODoc. %) (iterator-seq (.browseElements ^OGraphDatabase *db* (kw->oclass-name kclass) polymorphic?)))))

(defn count-vertices "" [] (.countVertexes ^OGraphDatabase *db*))
(defn count-edges "Counts either all the edges in the DB or the edges of a vertex."
  ([] (.countEdges ^OGraphDatabase *db*))
  ([vertex] (count (get-edges vertex :both)))
  ([vertex dir] (count (get-edges vertex dir)))
  ([vertex dir kclass] (count (get-edges vertex  dir kclass)))
  ([vertex dir kclass props] (count (get-edges vertex  dir kclass props))))
(defn count-elements "" [] (+ (count-vertices) (count-edges)))

; Vertices
(defn vertex-base-class "" [] (.getVertexBaseClass ^OGraphDatabase *db*))

(defn create-vertex-type! ""
  ([kclass] (.createVertexType ^OGraphDatabase *db* (kw->oclass-name kclass)))
  ([kclass ksuperclass] (.createVertexType ^OGraphDatabase *db* (kw->oclass-name kclass) (kw->oclass-name ksuperclass))))

(defn vertex "Creates a new vertex. Works just like 'document'."
  ([] (CljODoc. (.createVertex ^OGraphDatabase *db*)))
  ([kclass-or-map]
   (cond
     (keyword? kclass-or-map) (CljODoc. (.createVertex ^OGraphDatabase *db* (kw->oclass-name kclass-or-map)))
     (map? kclass-or-map) (merge (vertex) kclass-or-map)))
  ([kclass hmap] (merge (vertex kclass) hmap)))

(defn delete-vertex! "" [vertex] (.removeVertex ^OGraphDatabase *db* (.odoc vertex)))

(defn get-root "Returns the root node of the given name (as a keyword)."
  [root-name] (CljODoc. (.getRoot ^OGraphDatabase *db* (name root-name))))

(defn add-root! "Sets up a root node with the given name (as a keyword)."
  [root-name vertex]
  (.setRoot ^OGraphDatabase *db* (name root-name) (.odoc vertex))
  vertex)

; Edges
(defn edge-base-class "" [] (.getEdgeBaseClass ^OGraphDatabase *db*))

(defn create-edge-type! ""
  ([kclass] (.createEdgeType ^OGraphDatabase *db* (kw->oclass-name kclass)))
  ([kclass ksuperclass] (.createEdgeType ^OGraphDatabase *db* (kw->oclass-name kclass) (kw->oclass-name ksuperclass))))

(defn link!
"Creates an edge between 2 vertices. An optional edge-type (as a :keyword) or hash-map can be passed to set the type and properties."
  ([v1 v2] (CljODoc. (.createEdge ^OGraphDatabase *db* (.odoc v1) (.odoc v2))))
  ([v1 edge-data v2]
   (if (map? edge-data)
     (merge (link! v1 v2) edge-data)
     (-> (.createEdge ^OGraphDatabase *db* (.odoc v1) (.odoc v2) (kw->oclass-name edge-data)) (.field "label" (kw->oclass-name edge-data)) CljODoc.)))
  ([v1 edge-type props v2] (merge (link! v1 edge-type v2) props)))

(defn delete-edge! "" [edge] (.removeEdge ^OGraphDatabase *db* (.odoc edge)))

(defn get-links "Returns the set of edges between 2 vertices."
  ([v1 v2] (map #(CljODoc. %) (.getEdgesBetweenVertexes ^OGraphDatabase *db* (.odoc v1) (.odoc v2))))
  ([v1 v2 labels] (map #(CljODoc. %) (.getEdgesBetweenVertexes ^OGraphDatabase *db* (.odoc v1) (.odoc v2) (into-array String (map kw->oclass-name labels)))))
  ([v1 v2 labels edge-types] (map #(CljODoc. %) 
                                  (.getEdgesBetweenVertexes ^OGraphDatabase *db*
                                    (.odoc v1) (.odoc v2)
                                    (into-array String (map kw->oclass-name labels))
                                    (into-array String (map kw->oclass-name edge-types))))))

(defn linked? "Returns whether 2 vertexes are linked."
  ([v1 v2] (not (.isEmpty (.getEdgesBetweenVertexes ^OGraphDatabase *db* (.odoc v1) (.odoc v2)))))
  ([v1 v2 labels] (not (.isEmpty (.getEdgesBetweenVertexes ^OGraphDatabase *db* (.odoc v1) (.odoc v2) (into-array String (map kw->oclass-name labels))))))
  ([v1 v2 labels edge-types] (not (.isEmpty (.getEdgesBetweenVertexes ^OGraphDatabase *db* (.odoc v1) (.odoc v2)
                                              (into-array String (map kw->oclass-name labels))
                                              (into-array String (map kw->oclass-name edge-types)))))))

(defn unlink! "Removes all the edges between 2 vertices"
  ([v1 v2] (dorun (map delete-edge! (get-links v1 v2))))
  ([v1 v2 labels] (dorun (map delete-edge! (get-links v1 v2 labels))))
  ([v1 v2 labels edge-types] (dorun (map delete-edge! (get-links v1 v2 labels edge-types)))))

(defn get-vertex "Gets the :in or the :out vertex of an edge."
  [edge dir]
  (case dir
    :in (wrap-odoc (.getInVertex ^OGraphDatabase *db* (.odoc edge)))
    :out (wrap-odoc (.getOutVertex ^OGraphDatabase *db* (.odoc edge)))))

(defn get-edges
  "Gets the :in edges, the :out edges or :both edges from a vertex.
If provided properties to filter the edges, the properties can be a vector or a sequence (to filter edges that have the keys in there)
or a hash-map (to filter edges with properties set to specified values)."
  ([vertex dir] (case dir
                  :in (map #(CljODoc. %) (.getInEdges ^OGraphDatabase *db* (.odoc vertex))),
                  :out (map #(CljODoc. %) (.getOutEdges ^OGraphDatabase *db* (.odoc vertex))),
                  :both (concat (get-edges vertex :in) (get-edges vertex :out))))
  ([vertex dir kclass-or-props]
   (if (= :both dir)
     (concat (get-edges vertex :in kclass-or-props) (get-edges vertex :out kclass-or-props))
     (let [arg (if (keyword? kclass-or-props) (kw->oclass-name kclass-or-props) (prop-in kclass-or-props))]
       (case dir
         :in (map #(CljODoc. %) (.getInEdges ^OGraphDatabase *db* (.odoc vertex) arg)),
         :out (map #(CljODoc. %) (.getOutEdges ^OGraphDatabase *db* (.odoc vertex) arg))))))
  ([vertex dir kclass props]
   (let [in (if (#{:in :both} dir) (.getInEdgesHavingProperties ^OGraphDatabase *db* (.odoc vertex) (prop-in (assoc props "@class" (kw->oclass-name kclass)))))
         out (if (#{:out :both} dir) (.getOutEdgesHavingProperties ^OGraphDatabase *db* (.odoc vertex) (prop-in (assoc props "@class" (kw->oclass-name kclass)))))]
     (map #(CljODoc. %) (concat in out)))))

(defn get-ends
  "Gets the vertices at the end of the :in edges, the :out edges or :both edges from a vertex."
  ([vertex dir]
   (if (= :both dir)
     (concat (get-ends vertex :in) (get-ends vertex :out))
     (map (comp #(CljODoc. %) (if (= dir :in) :out :in)) (get-edges vertex dir))))
  ([vertex dir kclass-or-hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in kclass-or-hmap) (get-ends vertex :out kclass-or-hmap))
     (map (comp #(CljODoc. %) (if (= dir :in) :out :in)) (get-edges vertex dir kclass-or-hmap))))
  ([vertex dir kclass hmap]
   (if (= :both dir)
     (concat (get-ends vertex :in kclass hmap) (get-ends vertex :out kclass hmap))
     (map (comp #(CljODoc. %) (if (= dir :in) :out :in)) (get-edges vertex dir kclass hmap)))))
