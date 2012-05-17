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
  (:import (com.orientechnologies.orient.core.db.object ODatabaseObjectTx ODatabaseObjectPool))
  (:use (clj-orient core)))

(defopener open-object-db! ODatabaseObjectPool
  "Opens and returns a new ODatabaseObjectTx.")

(defn register-entity-class! "" [class]
  (-> ^ODatabaseObjectTx *db* .getEntityManager (.registerEntityClass class)))

(defn register-entity-classes! "" [package]
  (-> ^ODatabaseObjectTx *db* .getEntityManager (.registerEntityClasses package)))

(defn save-only-dirty! "" [?] (.setSaveOnlyDirty ^ODatabaseObjectTx *db* ?))
(defn save-only-dirty? "" [] (.isSaveOnlyDirty ^ODatabaseObjectTx *db*))
(defn set-dirty! "" [pojo] (.setDirty ^ODatabaseObjectTx *db* pojo))

(defn new-obj
  "Creates a new object through the Object DB.
The class can be a Class object or a keyword."
  [klass]
  (.newInstance ^ODatabaseObjectTx *db*
    (if (class? klass)
      klass
      (kw->oclass-name klass))))
