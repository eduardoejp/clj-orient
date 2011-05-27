
(ns clj-orient.core
  "This namespace wraps the basic OrientDB API functionality and all the DocumentDB functionality."
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

(def #^{:doc "This dynamic var holds the current open DB."} *db* nil)

;DB Handling
(defn create-db!
  "Creates a new database either locally or remotely. It does not, however, return the open instance or bind *db*."
  ([db-location]
   (-> (ODatabaseDocumentTx. db-location) .create .close))
  ([db-location db-storage user password]
   (-> (OServerAdmin. db-location) (.connect user password) (.createDatabase (name db-storage)) .close)))

(defn set-db! "Sets *db*'s root binding to the given DB."
  [db] (alter-var-root *db* (fn [_] db)))

(defn open-document-db!
  "Opens a new ODatabaseDocumentTx and binds it to the *db* var. It then returns the ODatabaseDocumentTx."
  [db-loc user pass]
  (set-db! (.open (ODatabaseDocumentTx. db-loc) user pass))
  *db*)

(defn close-db! "Closes the DB at *db* and sets *db* to nil."
  [] (.close *db*) (alter-var-root *db* (fn [_] nil)))

(defmacro with-db
  "Evaluates the given forms in an environment where *db* is bound to the given database."
  [db & forms]
  `(binding [*db* ~db] ~@forms))

(defn browse-class "" [kclass] (.browseClass *db* (name kclass)))
(defn browse-cluster "" [kcluster] (.browseCluster *db* (name kcluster)))

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
  [] {:url (.getURL *db*), :id (.getId *db*), :name (.getName *db*), :user (.getUser *db*)})

(defmacro with-tx "Runs the following forms inside a transaction."
  [& forms]
  `(try (.begin *db*)
     (let [r# (do ~@forms)] (.commit *db*) r#)
     (catch Exception e# (.rollback *db*) (throw e#))))

;Document Handling
(defn make-document
"Returns a newly created document given the document's class (as a keyword). It can optionally take a clojure hash-map to set the document's properties."
  ([clss] (ODocument. *db* (name clss)))
  ([clss properties]
    (let [d (make-document *db* (name clss))]
      (doall (for [k (keys properties)] (.field d (name k) (get properties k))))
      (.save d)
      d)))

(defn document? "" [x] (instance? ODocument x))
(defn id? "" [x] (instance? ORID x))

(defn save! "Saves an ODocument or an object."
  ([document] (if (document? document)
                (.save document)
                (.save *db* document)))
  ([document clusname] (.save document clusname)))

(defn update!
  "Updates either an ODocument based on the given hash-map."
  [document hmap]
  (doall (for [k (keys hmap)] (.field document (name k) (get hmap k)))))

(defn get-id "Returns the ORecordId for the given document or graph element."
  [record] (.getIdentity record))

(defn id->vec "Given an ORID, returns a vector [cluster-id, cluster-position]."
  [orid] [(.getClusterId orid) (.getClusterPosition orid)])

(defn vec->id "Given a vector [cluster-id, cluster-position], returns an ORecordId."
  [ridvec] (ORecordId. (first ridvec) (second ridvec)))

(defn properties "Returns a list of the properties as keywords."
  [document] (map keyword (.fieldNames document)))

(defn pget "" [document kprop] (.field document (name kprop)))

(defn passoc! "" [document kprop val] (.field document (name kprop) val))

(defn doc->map
  "Given an ODocument, returns a hash-map of its keys and values."
  [document]
  (with-meta
    (apply hash-map (concat (for [f (.fieldNames document)]
                              [(keyword f) (.field document f)])))
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
(defn get-classes "" [] (-> *db* .getMetadata .getSchema .classes seq))

(defn derive! ""
  [subclass, superclass]
  (let [subclass (if (keyword? subclass) (get-class *db* subclass) subclass)
        superclass (if (keyword? superclass) (get-class *db* superclass) superclass)]
    (.setSuperClass subclass superclass)
    (-> *db* .getMetadata .getSchema .save)))

(defn create-class!
  "Creates a class in the given database and makes it inherit the given superclass. Superclass can be of type String, Class or OClass."
  ([class-name]
    (-> *db* .getMetadata .getSchema (.createClass (name class-name)))
    (-> *db* .getMetadata .getSchema .save))
  ([class-name superclass]
   (create-class! *db* class-name)
   (derive! *db* class-name superclass)))

(defn superclass? "" [superclass, subclass]
  (let [subclass (if (keyword? subclass) (get-class *db* subclass) subclass)
        superclass (if (keyword? superclass) (get-class *db* superclass) superclass)]
    (.isSubClassOf subclass superclass)))

(defn subclass? "" [subclass, superclass] (superclass? superclass subclass))

(defn truncate-class! "" [kclass] (.truncate (get-class *db* kclass)))

;Hooks
(defn get-hooks "" [] (seq (.getHooks *db*)))
(defn add-hook "" [hook] (.registerHook *db* hook))
(defn remove-hook "" [hook] (.unregisterHook *db* hook))
(defmacro make-hook
  "Creates a new hook from the following options:
  :before-create <forms>
  :before-read <forms>
  :before-update <forms>
  :before-delete <forms>
  :after-create <forms>
  :after-read <forms>
  :after-update <forms>
  :after-delete <forms>"
  [& options]
  `(proxy [com.orientechnologies.orient.core.hook.ORecordHookAbstract] []
     ~@(let [hooks (map #(list (get {:after-create 'onRecordAfterCreate,   :after-read 'onRecordAfterRead
                                     :after-update 'onRecordAfterUpdate,   :after-delete 'onRecordAfterDelete
                                     :before-create 'onRecordBeforeCreate, :before-read 'onRecordBeforeRead
                                     :before-update 'onRecordBeforeUpdate, :before-delete 'onRecordBeforeDelete}
                                 (first %))
                           (second %))
                     (partition 2 options))]
         (for [[meth forms] hooks] `(~meth [~'*record*] ~forms)))))
