
(ns clj-orient.core
  #^{:doc "This namespace implements the basic OrientDB API functionality."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.db ODatabase ODatabaseComplex)
    (com.orientechnologies.orient.core.db.document ODatabaseDocumentTx)
    (com.orientechnologies.orient.core.db.record ODatabaseRecord)
    (com.orientechnologies.orient.core.record.impl ODocument)
    (com.orientechnologies.orient.client.remote OServerAdmin)
    ))

;(set! *warn-on-reflection* true)

(defn create-document-db!
  "Given the DBs location as either :local, :remote or :memory and the path, creates the desired DB."
  ([db-loc dbpath]
   (.create (ODatabaseDocumentTx. (str (name db-loc) ":" dbpath))))
  ([db-loc dbpath storage]
   (-> (OServerAdmin. (str "remote:" dbpath)) .connect (.createDatabase (name storage)))))

(defn open-document-db!
"Given the DBs location as either :local, :remote or :memory and the path (with optional admin and password data), opens
the desired DB."
  [db-loc dbpath mode]
  (-> (ODatabaseDocumentTx. (str (name db-loc) ":" dbpath)) (.open (name mode) (name mode))))

(defn as-document
"Returns a newly created document given the database and the document's class (as a String). It can optionally take a regular
hash-map to set the document's fields."
  ([#^ODatabaseRecord db clss]
    (ODocument. db (name clss)))
  ([#^ODatabaseRecord db clss m]
    (let [d #^ODocument (as-document db (name clss))]
      (doall (for [k (keys m)]
               (.field d (name k) (m k))))
      d)))

(defn save
  "Given a database, the object's class and a hash-map, saves it as a document of that class."
  [dc] (.save dc))

(defn doc->map
"Given an ODocument, returns a hash-map with the document's data. The class name will be appended as metadata under the
:class-name key."
  [#^ODocument d]
  (with-meta
    (apply hash-map (flatten (for [f (.fieldNames d)] [(keyword f) (.field d f)])))
    {:class-name (keyword (.getClassName d))}))

(defn browse-class
  "Returns a lazy-seq of the documents of the specified class."
  [#^ODatabaseDocumentTx db clss]
  (for [d (.browseClass db (name clss))] d))

(defn browse-cluster
  "Returns a lazy-seq of the documents in the specified cluster."
  [#^ODatabaseDocumentTx db cluster]
  (for [d (.browseCluster db (name cluster))] d))

(defn count-class
  "Counts the documents of the specified class."
  [#^ODatabaseDocumentTx db cl]
  (.countClass db (name cl)))

(defn count-cluster
  "Counts the documents in the specified cluster."
  [#^ODatabase db cl]
  (.countClusterElements db (name cl)))

(defn get-cluster-names
  [#^ODatabase db]
  (seq (.getClusterNames db)))

(defn get-cluster
  [#^ODatabase db id]
  (cond
    (integer? id) (.getClusterNameById db id)
    (string? id) (.getClusterIdByName db id)))

(defn get-cluster-type
  [#^ODatabase db clname]
  (.getClusterType db clname))

(defn delete
  [d]
  (.delete d))

(defmacro with-tx
  [db & forms]
  `(try
     (.begin ~db)
     ~@forms
     (.commit ~db)
     (catch Exception e#
       (.rollback ~db))))

(defn create-class
"Creates a class in the given database and makes it inherit the given superclass. Superclass can be of type String, Class
or OClass."
  [#^ODatabaseComplex db class-name superclass]
  (-> db .getMetadata .getSchema (.createClass class-name) (.setSuperClass superclass))
  (-> db .getMetadata .getSchema .save))

(defn close-db [#^ODatabase db] (.close db))

(defn closed? [#^ODatabase db] (.isClosed db))

(defn exists? [#^ODatabase db] (.exists db))

(defn get-url [#^ODatabase db] (.getURL db))

(defn get-id [obj] (.getId obj))

(defn get-name [#^ODatabase db] (.getName db))
