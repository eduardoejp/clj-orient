
(ns clj-orient.object
  #^{:doc "This namespace implements the object DB part of OrientDB."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.db.object ODatabaseObjectTx ODatabaseObjectPool))
  (:use (clj-orient core)))

(defn open-object-db!
  [db-loc dbpath user pass]
  (let [db (ODatabaseObjectTx. (str (name db-loc) ":" dbpath))]
    (if (.exists db)
      (-> ODatabaseObjectPool .global (.adquire (str (name db-loc) ":" dbpath) user pass))
      (do (if (= :remote db-loc)
            (create-db! db-loc dbpath :local)
            (create-db! db-loc dbpath))
        (-> ODatabaseObjectPool .global (.adquire (str (name db-loc) ":" dbpath) user pass))))))

(defn register-entity-class [#^ODatabaseObjectTx db, #^Class class]
  (-> db .getEntityManager (.registerEntityClass class)))

(defn register-entity-classes [#^ODatabaseObjectTx db, #^String package]
  (-> db .getEntityManager (.registerEntityClasses package)))

(defn save-obj! [db obj] (.save db obj))
(defn delete-obj! [db obj] (.delete db obj))

(defn save-only-dirty [db ?] (.setSaveOnlyDirty db ?))
(defn save-only-dirty? [db] (.isSaveOnlyDirty db))
(defn set-dirty [db pojo] (.setDirty db pojo))
