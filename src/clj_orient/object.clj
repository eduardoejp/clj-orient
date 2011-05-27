
(ns clj-orient.object
  "This namespace wraps the ObjectDB part of OrientDB."
  (:import (com.orientechnologies.orient.core.db.object ODatabaseObjectTx))
  (:use (clj-orient core)))

(defn open-object-db! "Opens a new ODatabaseObjectTx and binds it to the *db* var. It then returns the ODatabaseObjectTx."
  [db-loc username password]
  (set-db! (.open (ODatabaseObjectTx. db-loc) username password))
  *db*)

(defn register-entity-class "" [class]
  (-> *db* .getEntityManager (.registerEntityClass class)))

(defn register-entity-classes "" [package]
  (-> *db* .getEntityManager (.registerEntityClasses package)))

(defn save-only-dirty "" [?] (.setSaveOnlyDirty *db* ?))
(defn save-only-dirty? "" [] (.isSaveOnlyDirty *db*))
(defn set-dirty "" [pojo] (.setDirty *db* pojo))
