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
       :doc "This namespace wraps the ObjectDB part of OrientDB."}
  clj-orient.object
  (:import (com.orientechnologies.orient.core.db.object ODatabaseObjectTx))
  (:use (clj-orient core)))

(defopener open-object-db! ODatabaseObjectTx
  "Opens a new ODatabaseObjectTx and binds it to the *db* var. It then returns the ODatabaseObjectTx.")

(defn register-entity-class! "" [class]
  (-> *db* .getEntityManager (.registerEntityClass class)))

(defn register-entity-classes! "" [package]
  (-> *db* .getEntityManager (.registerEntityClasses package)))

(defn save-only-dirty! "" [?] (.setSaveOnlyDirty *db* ?))
(defn save-only-dirty? "" [] (.isSaveOnlyDirty *db*))
(defn set-dirty! "" [pojo] (.setDirty *db* pojo))
