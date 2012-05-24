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
      :doc "Functions & macros for easier schema definition."}
     clj-orient.schema
  (:refer-clojure :exclude [load])
  (:use clj-orient.core))

(def schema-classes (atom []))

(defn- install-oclass! [[klass base & props]]
  (if (exists-class? klass)
    (do
      (if base
        (if-not (subclass? klass base)
          (derive! klass base)))
      (doseq [[p type conf] props]
        (if (exists-prop? klass p)
          (if type
            (update-prop! klass p conf)
            (drop-prop! klass p))
          (if type
            (create-prop! klass p type conf))))
      (save-schema!)
      true)
    (do (create-class! klass)
      (recur (list* klass base props)))))
(defn install-oclasses! "Installs the OClasses previously defined by defoclass."
  [] (dorun (map install-oclass! @schema-classes)) true)

(defn- separate [f coll] [(take-while f coll) (drop-while f coll)])

(defmacro defoclass "Defines a class (and it's properties) in the database schema."
  [sym & triplets]
  (let [[[base] triplets] (separate symbol? triplets)
        [[doc-string] triplets] (separate string? triplets)
        props (for [[sym type conf] triplets
                    :let [m (meta sym)]]
                (-> m
                  (clojure.set/rename-keys
                    {:mandatory :mandatory?
                     :nullable :nullable?})
                  (assoc :index (some identity (map (fn [k] (and (k m) k)) [:dictionary :fulltext :unique :not-unique :proxy])))
                  (merge conf)
                  (->> (vector (keyword sym) (if (vector? type) [(first type) (keyword (second type))] type)))))]
    `(swap! schema-classes conj ~(vec (list* (keyword sym) (keyword base) props)))))
