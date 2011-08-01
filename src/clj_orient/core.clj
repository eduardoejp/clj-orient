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
       :doc "This namespace wraps the basic OrientDB API functionality and all the DocumentDB functionality."}
  clj-orient.core
  (:import (com.orientechnologies.orient.client.remote OServerAdmin)
    (com.orientechnologies.orient.core.db ODatabase ODatabaseComplex)
    (com.orientechnologies.orient.core.db.document ODatabaseDocumentTx ODatabaseDocumentPool)
    (com.orientechnologies.orient.core.db.record ODatabaseRecord)
    (com.orientechnologies.orient.core.record ORecord)
    (com.orientechnologies.orient.core.record.impl ODocument)
    (com.orientechnologies.orient.core.id ORecordId ORID)
    (com.orientechnologies.orient.core.metadata.schema OClass)
    (com.orientechnologies.orient.core.hook ORecordHook ORecordHookAbstract)
    ))

(defmacro defopener [sym class docstring]
  `(defn ~sym ~docstring
     [~'db-loc ~'user ~'pass]
     (let [~'db (.open (new ~class ~'db-loc) ~'user ~'pass)]
       (set-db! ~'db)
       ~'db)))

;DB Handling
(def #^{:doc "This dynamic var holds the current open DB."} *db* nil)

(defn create-db!
  "Creates a new database either locally or remotely. It does not, however, return the open instance or bind *db*."
  ([db-location]
   (-> (ODatabaseDocumentTx. db-location) .create .close))
  ([db-location user password]
   (-> (OServerAdmin. db-location) (.connect user password) (.createDatabase "remote") .close)))

(defn set-db! "Sets *db*'s root binding to the given DB."
  [db] (alter-var-root #'*db* (fn [_] db)))

(defopener open-document-db! ODatabaseDocumentTx
  "Opens a new ODatabaseDocumentTx and binds it to the *db* var. It then returns the ODatabaseDocumentTx.")

(defn close-db!
  "Closes the DB at *db* and sets *db* to nil."
  []
  (when *db*
    (.close *db*) (alter-var-root #'*db* (fn [_] nil))))

(defn delete-db! "Deletes the database bound to *db* and sets *db* to nil."
  ([] (.delete *db*) (alter-var-root #'*db* (fn [_] nil)))
  ([db] (.delete db)))

(defmacro with-db
  "Evaluates the given forms in an environment where *db* is bound to the given database."
  [db & forms]
  `(binding [*db* ~db] ~@forms))

(defn browse-class "" [kclass] (iterator-seq (.browseClass *db* (name kclass))))
(defn browse-cluster "" [kcluster] (iterator-seq (.browseCluster *db* (name kcluster))))

(defn count-class "" [kclass] (.countClass *db* (name kclass)))
(defn count-cluster "" [id] (.countClusterElements *db* (if (keyword? id) (name id) id)))

(defn get-cluster-names "" [] (map keyword (.getClusterNames *db*)))
(defn get-cluster-name "" [id] (keyword (.getClusterNameById *db* id)))
(defn get-cluster-id "" [kname] (.getClusterIdByName *db* (name kname)))
(defn get-cluster-type "" [clname] (keyword (.getClusterType *db* (name clname))))

(defn db-closed? "" [] (or (nil? *db*) (.isClosed *db*)))
(defn db-open? "" [] (not (db-closed? *db*)))
(defn db-exists? "" [] (.exists *db*))

(defn db-info "Returns information relevant to the DB as a hash-map."
  [] {:url (.getURL *db*), :id (.getId *db*), :name (.getName *db*), :user (.getUser *db*),
      :status (str (.getStatus *db*))})

(defmacro with-tx "Runs the following forms inside a transaction."
  [& forms]
  `(try (.begin *db*)
     (let [r# (do ~@forms)] (.commit *db*) r#)
     (catch Exception e# (.rollback *db*) (throw e#))))

;Document Handling
(defn document
"Returns a newly created document given the document's class (as a keyword). It can optionally take a clojure hash-map to set the document's properties."
  ([kclass] (ODocument. *db* (name kclass)))
  ([kclass properties]
   (let [d (document kclass)]
     (dorun (for [p properties] (.field d (name (first p)) (second p))))
     d)))

(defn document? "" [x] (instance? ODocument x))
(defn id? "" [x] (instance? ORID x))

(defn save! "Saves an ODocument or an object."
  ([document] (if (document? document)
                (.save document)
                (.save *db* document)))
  ([document kcluster] (.save document kcluster)))

(defn get-id "Returns the ORecordId for the given document or graph element."
  [record] (.getIdentity record))

(defn id->vec "Given an ORID, returns a vector [cluster-id, cluster-position]."
  [orid] [(.getClusterId orid) (.getClusterPosition orid)])

(defn vec->id "Given a vector [cluster-id, cluster-position], returns an ORecordId."
  [ridvec] (ORecordId. (first ridvec) (second ridvec)))

(defn props "Returns a list of the properties as keywords."
  [document] (map keyword (.fieldNames document)))

(defn pget "" [document key] (.field document (name key)))

(defn passoc! ""
  ([document key val]
   (.field document (name key) val)
   document)
  ([document key val & kvs]
   (apply passoc! (passoc! document key val) kvs)))

(defn doc->map
  "Given an ODocument, returns a hash-map of its keys and values."
  [document]
  (with-meta
    (apply hash-map (mapcat (fn [f] [(keyword f) (.field document f)])
                            (.fieldNames document)))
    {:id (id->vec (get-id document)), :type (keyword (.getClassName document))}))

(defn load-item "" [orid] (if (vector? orid) (.load *db* (vec->id orid)) (.load *db* orid)))

(defn delete!
  "Deletes an ODocument if it is passed or if its ID (as ORID or vector) is passed."
  [x]
  (cond
    (document? x) (.delete x)
    (id? x) (.delete (load-item *db* x))
    :else (.delete *db* x)))

;Document/GraphElement Classes
(defn get-class "Returns an OClass given the classname as a keyword."
  [kclass] (-> *db* .getMetadata .getSchema (.getClass (name kclass))))
(defn get-class-name "Get the classname from an OClass or an ODocument."
  [odoc] (keyword (if (instance? OClass odoc) (.getName odoc) (.getClassName odoc))))
(defn get-classes "" [] (-> *db* .getMetadata .getSchema .getClasses seq))

(defn derive! ""
  [subclass, superclass]
  (let [subclass (if (keyword? subclass) (get-class subclass) subclass)
        superclass (if (keyword? superclass) (get-class superclass) superclass)]
    (.setSuperClass subclass superclass)
    (-> *db* .getMetadata .getSchema .save)))

(defn create-class!
  "Creates a class in the given database and makes it inherit the given superclass. Superclass can be of type String, Class or OClass."
  ([kclass]
   (let [oclass (-> *db* .getMetadata .getSchema (.createClass (name kclass)))]
     (-> *db* .getMetadata .getSchema .save)
     oclass))
  ([kclass ksuperclass]
   (let [oclass (create-class! *db* kclass)]
     (derive! *db* kclass ksuperclass)
     oclass)))

(defn drop-class! ""
  [kclass]
  (-> *db* .getMetadata .getSchema (.dropClass (name kclass))))

(defn exists-class? ""
  [kclass]
  (-> *db* .getMetadata .getSchema (.existsClass (name kclass))))

(defn superclass? "" [superclass, subclass]
  (let [subclass (if (keyword? subclass) (get-class subclass) subclass)
        superclass (if (keyword? superclass) (get-class superclass) superclass)]
    (.isSubClassOf subclass superclass)))

(defn subclass? "" [subclass, superclass] (superclass? superclass subclass))

(defn truncate-class! "" [kclass] (.truncate (get-class kclass)))

(defn schema-info []
  (let [schema (-> *db* .getMetadata .getSchema)]
    {:id (.getIdentity schema), :version (.getVersion schema),
     :classes (map #(keyword (.getName %)) (.getClasses schema))}))

;Hooks
(def +triggers+
  {'before-create 'onRecordBeforeCreate, 'after-create 'onRecordAfterCreate,
   'before-read 'onRecordBeforeRead, 'after-read 'onRecordAfterRead,
   'before-update 'onRecordBeforeUpdate, 'after-update 'onRecordAfterUpdate,
   'before-delete 'onRecordBeforeDelete, 'after-delete 'onRecordAfterDelete})

(defn get-hooks "" [] (seq (.getHooks *db*)))
(defn add-hook! "" [hook] (.registerHook *db* hook))
(defn remove-hook! "" [hook] (.unregisterHook *db* hook))

(defmacro defhook
  "Creates a new hook from the following fn definitions (each one is optional):
  (before-create [~document] ~@forms)
  (before-read [~document] ~@forms)
  (before-update [~document] ~@forms)
  (before-delete [~document] ~@forms)
  (after-create [~document] ~@forms)
  (after-read [~document] ~@forms)
  (after-update [~document] ~@forms)
  (after-delete [~document] ~@forms)
Example:
(defhook log-hook
  (after-create [document] (println \"Created:\" (doc->map document))))

defhook only creates the hook. To add it to the current *db* use add-hook."
  [sym & triggers]
  (let [doc-string (when (string? (first triggers)) (first triggers))
        triggers (if (string? (first triggers)) (rest triggers) triggers)]
    `(def ~(with-meta sym {:doc doc-string})
       (proxy [com.orientechnologies.orient.core.hook.ORecordHookAbstract] []
         ~@(for [[meth args & forms] triggers] `(~(get +triggers+ meth) ~args ~@forms))))))
