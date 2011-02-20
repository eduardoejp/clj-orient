
(ns clj-orient.core
  #^{:doc "This namespace implements the basic OrientDB API functionality."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.db ODatabase ODatabaseComplex)
    (com.orientechnologies.orient.core.db.document ODatabaseDocumentTx)
    (com.orientechnologies.orient.core.record ORecord)
    (com.orientechnologies.orient.core.db.record ODatabaseRecord)
    (com.orientechnologies.orient.core.record.impl ODocument)
    (com.orientechnologies.orient.client.remote OServerAdmin)
    (com.orientechnologies.orient.core.id ORecordId ORID)
    (com.orientechnologies.orient.core.db.graph OGraphVertex)
    ))

;(set! *warn-on-reflection* true)

(defn create-db!
  ([db-loc dbpath]
   (.close (.create (ODatabaseDocumentTx. (str (name db-loc) ":" dbpath)))))
  ([db-loc dbpath storage]
   (-> (OServerAdmin. (str "remote:" dbpath)) .connect (.createDatabase (name storage)) .close)))

(defn open-document-db!
  [db-loc dbpath user pass]
  (-> (ODatabaseDocumentTx. (str (name db-loc) ":" dbpath)) (.open user pass)))

(defn document
"Returns a newly created document given the database and the document's class (as a String). It can optionally take a regular
hash-map to set the document's fields."
  ([#^ODatabaseRecord db clss]
    (let [d (ODocument. db (name clss))] d))
  ([#^ODatabaseRecord db clss m]
    (let [d #^ODocument (document db (name clss))]
      (doall (for [k (keys m)] (.field d (name k) (m k))))
      (.save d)
      d)))

(defn save! ([dc] (.save dc)) ([dc clusname] (.save dc clusname)))

(defn get-rid
  [record]
  (let [record (if (instance? OGraphVertex record) (.getDocument record) record)
        rid (.getIdentity record)]
    [(.getClusterId rid) (.getClusterPosition rid)]))

(defn to-map
  [d]
  (cond
    (instance? ODocument d) (with-meta
                              (apply hash-map (flatten (for [f (.fieldNames d)] [(keyword f) (.field d f)])))
                              {:rid (get-rid d) :class-name (keyword (.getClassName d))})
    (instance? OGraphVertex d) (recur (.getDocument d))))

(defn browse-class
  [#^ODatabaseDocumentTx db clss]
  (map identity (.browseClass db (name clss))))

(defn browse-cluster [#^ODatabaseDocumentTx db cluster] (.browseClass db (name cluster)))

(defn count-class [#^ODatabaseDocumentTx db cl] (.countClass db (name cl)))

(defn count-cluster [#^ODatabase db cl] (.countClusterElements db (name cl)))

(defn get-cluster-names [#^ODatabase db] (map keyword (.getClusterNames db)))

(defn get-cluster
  [#^ODatabase db id]
  (cond
    (integer? id) (keyword (.getClusterNameById db id))
    (keyword? id) (.getClusterIdByName db (name id))))

(defn get-cluster-type [#^ODatabase db clname] (keyword (.getClusterType db clname)))

(defn as-ORID [rid] (ORecordId. (first rid) (second rid)))

(defn load-item
  [db item]
  (if (vector? item)
    (.load db #^ORID (as-ORID item))
    (.load db item))
  ;(if (instance? ODatabaseDocumentTx db) (recur (.getUnderlying db) item))
  )

(defn delete!
  ([d] (.delete d))
  ([db rid] (.delete (load-item db rid))))

(defmacro with-tx
"Runs the following forms inside a transaction for the given DB. It will return 'true' if all went well and 'false' if the
transaction failed."
  [db & forms]
  `(try (.begin ~db) ~@forms (.commit ~db) true
     (catch Exception e# (.printStackTrace e#) (.rollback ~db) false)))

(defn get-class [#^ODatabase db clss] (-> db .getMetadata .getSchema (.getClass (name clss))))

(defn get-classes [#^ODatabaseComplex db] (map identity (-> db .getMetadata .getSchema .classes)))

(defn create-class!
"Creates a class in the given database and makes it inherit the given superclass. Superclass can be of type String, Class
or OClass."
  ([#^ODatabaseComplex db class-name]
    (-> db .getMetadata .getSchema (.createClass (name class-name)))
    (-> db .getMetadata .getSchema .save))
  ([#^ODatabaseComplex db class-name superclass]
   (if (keyword? superclass)
     (-> db .getMetadata .getSchema (.createClass (name class-name)) (.setSuperClass (get-class superclass)))
     (-> db .getMetadata .getSchema (.createClass (name class-name)) (.setSuperClass superclass)))
   (-> db .getMetadata .getSchema .save)))

(defn close-db! [#^ODatabase db] (.close db))

(defn closed? [#^ODatabase db] (.isClosed db))

(defn open? [db] (not (closed? db)))

(defn exists? [#^ODatabase db] (.exists db))

(defn db-info
  ([#^ODatabase db] {:url (.getURL db), :id (.getId db), :name (.getName db), :user (.getUser db)})
  ([#^ODatabase db, keyw] (cond (= keyw :url) (.getURL db)
                            (= keyw :id) (.getId db)
                            (= keyw :name) (.getName db)
                            (= keyw :user) (.getUser db))))

(defn update!
  [odoc hmap]
  (cond
    (instance? ODocument odoc) (doall (for [k (keys hmap)] (.field odoc (name k) (hmap k))))
    (instance? OGraphVertex odoc) (doall (for [k (keys hmap)] (.set odoc (name k) (hmap k)))))
  (save! odoc))

(defn derive!
  [#^ODatabase db, subclass, superclass]
  (let [subclass (if (keyword? subclass) (get-class db subclass) subclass)
        superclass (if (keyword? superclass) (get-class db superclass) superclass)]
    (.setSuperClass subclass superclass)
    (-> db .getMetadata .getSchema .save)))

(defn superclass? [#^ODatabase db, superclass, subclass]
  (let [subclass (if (keyword? subclass) (get-class db subclass) subclass)
        superclass (if (keyword? superclass) (get-class db superclass) superclass)]
    (.isSubClassOf subclass superclass)))

(defn subclass? [#^ODatabase db, subclass, superclass] (superclass? db superclass subclass))

(defn truncate-class! [#^ODatabase db, kclass] (.truncate (get-class db kclass)))
