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

(ns ^{:author "Eduardo Julián <eduardoejp@gmail.com>",
      :doc "This namespace wraps the ObjectDB part of OrientDB."}
  clj-orient.object
  (:import (com.orientechnologies.orient.object.db OObjectDatabaseTx OObjectDatabasePool))
  (:require [clj-orient.core :as oc]))

(oc/defopener open-object-db! OObjectDatabasePool
  "Opens and returns a new OObjectDatabaseTx.")

(defn register-entity-class! "" [class]
  (-> ^OObjectDatabaseTx oc/*db* .getEntityManager (.registerEntityClass class)))

(defn register-entity-classes! "" [package]
  (-> ^OObjectDatabaseTx oc/*db* .getEntityManager (.registerEntityClasses package)))

(defn save-only-dirty! "" [?] (.setSaveOnlyDirty ^OObjectDatabaseTx oc/*db* ?))
(defn save-only-dirty? "" [] (.isSaveOnlyDirty ^OObjectDatabaseTx oc/*db*))
(defn set-dirty! "" [pojo] (.setDirty ^OObjectDatabaseTx oc/*db* pojo))

(defn new-obj
  "Creates a new object through the Object DB.
The class can be a Class object or a keyword."
  ([] (.newInstance ^OObjectDatabaseTx oc/*db*))
  ([klass]
   (.newInstance ^OObjectDatabaseTx oc/*db*
     (if (class? klass)
       klass
       (oc/kw->oclass-name klass))))
  ([klass & args]
   (.newInstance ^OObjectDatabaseTx oc/*db*
     (if (class? klass)
       klass
       (oc/kw->oclass-name klass))
     (to-array args))))
