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
    (com.orientechnologies.orient.core.db ODatabase ODatabaseComplex ODatabasePoolBase)
    (com.orientechnologies.orient.core.db.document ODatabaseDocumentTx ODatabaseDocumentPool)
    (com.orientechnologies.orient.core.db.record ODatabaseRecord)
    (com.orientechnologies.orient.core.record ORecord)
    (com.orientechnologies.orient.core.record.impl ODocument)
    (com.orientechnologies.orient.core.id ORecordId ORID)
    (com.orientechnologies.orient.core.metadata.schema OClass OProperty OProperty$INDEX_TYPE)
    (com.orientechnologies.orient.core.hook ORecordHook ORecordHookAbstract)
    )
  (:use clojure.contrib.def))

(declare get-schema save-schema! oclass)

; Utils
(defmacro defopener [sym class docstring]
  `(defn ~sym ~docstring [~'db-loc ~'user ~'pass]
     (-> (. ~class (global)) (.acquire ~'db-loc ~'user ~'pass))))

;;;;;;;;;;;;;;;;;;;
;;; DB Handling ;;;
;;;;;;;;;;;;;;;;;;;
(def #^{:doc "This dynamic var holds the current open DB."
        :tag com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx}
       *db* nil)

(defn create-db!
  "Creates a new database either locally or remotely. It does not, however, return the open instance or bind *db*."
  ([db-location]
   (-> (ODatabaseDocumentTx. db-location) .create .close))
  ([#^String db-location user password]
   (-> (OServerAdmin. db-location) (.connect user password) (.createDatabase "remote") .close)))

(defn set-db! "Sets *db*'s root binding to the given DB."
  [db] (alter-var-root #'*db* (fn [_] db)))

(defopener open-document-db! ODatabaseDocumentPool
  "Opens and returns a new ODatabaseDocumentTx.")

(defn close-db!
  "Closes the DB at *db* and sets *db* to nil."
  ([] (when *db* (.close *db*) (set-db! nil)))
  ([#^ODatabase db] (.close db)))

(defn delete-db! "Deletes the database bound to *db* and sets *db* to nil."
  ([] (.delete *db*) (set-db! nil))
  ([#^ODatabase db] (.delete db)))

(defmacro with-db
  "Evaluates the given forms in an environment where *db* is bound to the given database."
  [db & forms]
  `(binding [*db* ~db]
     (let [x# (do ~@forms)]
       (close-db! *db*)
       x#)))

(defn browse-class "Returns a seq of all the documents of the specified class."
  [kclass] (iterator-seq (.browseClass #^ODatabaseDocumentTx *db* (name kclass))))
(defn browse-cluster "Returns a seq of all the documents in the specified cluster."
  [kcluster] (iterator-seq (.browseCluster *db* (name kcluster))))

(defn count-class ""
  [kclass] (.countClass #^ODatabaseDocumentTx *db* (name kclass)))
(defn count-cluster ""
  [id] (.countClusterElements *db* (if (keyword? id) (name id) id)))

(defn get-cluster-names "" [] (map keyword (.getClusterNames *db*)))
(defn get-cluster-name "" [id] (keyword (.getClusterNameById *db* id)))
(defn get-cluster-id "" [kname] (.getClusterIdByName *db* (name kname)))
(defn get-cluster-type "" [clname] (keyword (.getClusterType *db* (name clname))))

(defn db-closed? ""
  ([] (db-closed? *db*))
  ([#^ODatabase db] (or (nil? db) (.isClosed db))))
(defn db-open? ""
  ([] (db-open? *db*))
  ([#^ODatabase db] (not (db-closed? db))))
(defn db-exists? ""
  ([] (db-exists? *db*))
  ([#^ODatabase db] (.exists #^ODatabase db)))

(defn db-info "Returns information relevant to the DB as a hash-map."
  ([] (db-info *db*))
  ([#^ODatabaseRecord db]
   {:name (.getName db),           :url (.getURL db),
    :status (str (.getStatus db)), :user (.getUser db)}))

(defmacro with-tx "Runs the following forms inside a transaction."
  [& forms]
  `(try (.begin *db*)
     (let [r# (do ~@forms)] (.commit *db*) r#)
     (catch Exception e# (.rollback *db*) (throw e#))))

;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Document Handling ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;
(defn document? "" [x] (instance? ODocument x))
(defn orid? "" [x] (instance? ORID x))
(defn oclass? "" [x] (instance? OClass x))

(defn document
  "Returns a newly created document given the document's class (as a keyword).
It can optionally take a Clojure hash-map to set the document's properties."
  ([kclass] (ODocument. *db* (name kclass)))
  ([kclass properties]
   (let [#^ODocument d (document kclass)]
     (dorun (for [p properties] (.field d (name (first p)) #^Object (second p))))
     d)))

(defn save! "Saves an ODocument, an OClass or an object."
  ([item] (cond
            (document? item) (.save #^ODocument item)
            (oclass? item) (do (save-schema!) item)
            :else (do (.save *db* item) item)))
  ([#^ODocument document kcluster] (.save document (name kcluster))))

(defn get-id "Returns the ORecordId for the given document or graph element."
  [#^ORecord record] (.getIdentity record))

(defn id->vec "Given an ORID, returns a vector [cluster-id, cluster-position]."
  [#^ORID orid] [(.getClusterId orid) (.getClusterPosition orid)])

(defn vec->id "Given a vector [cluster-id, cluster-position], returns an ORecordId."
  [ridvec] (ORecordId. (first ridvec) (second ridvec)))

(defn get-props
  "Returns a seq of the properties from either an ODocument (as keywords) or an OClass (as OProperty instances)."
  [item]
  (if (document? item)
    (map keyword (.fieldNames #^ODocument item))
    (.properties #^OClass item)))

(defn pget "Same a 'get', but for document properties."
  [#^ODocument document key]
  (let [v (.field document (name key))]
    (if (and (string? v) (.startsWitch #^String v ":"))
      (keyword (.substring #^String v 1))
      v)))

(defn passoc! "Same a 'assoc', but for document properties."
  ([#^ODocument document key val]
   (.field document (name key) (if (keyword? val) (str val) val))
   document)
  ([document key val & kvs]
   (apply passoc! (passoc! document key val) kvs)))

(defn pdissoc! "Same a 'dissoc', but for document properties."
  ([#^ODocument document key] (.removeField document key) document)
  ([document key & keys] (reduce pdissoc! document (conj key keys))))

(defn pcontains?
  "Same a 'contains?', but for document properties."
  [#^ODocument document key]
  (.containsField document key))

(defn merge! "Same a 'merge', but for document properties."
  [doc1 doc2]
  (apply passoc! doc1 (mapcat identity doc2)))

(defn doc->map
  "Given an ODocument, returns a hash-map of its keys and values."
  [#^ODocument document]
  (with-meta
    (apply hash-map (mapcat (fn [f] [(keyword f) (.field document f)])
                            (.fieldNames document)))
    {:id (id->vec (get-id document)), :type (keyword (.getClassName document))}))

(defn load-item "Returns an ODocument, given it's id (either as ORID or a vector)"
  [orid] (if (vector? orid) (.load *db* (vec->id orid)) (.load *db* orid)))

(defn delete!
  "Deletes an ODocument if it is passed or if its id (as ORID or vector) is passed.
Can also remove a class from the DB Schema."
  [x]
  (cond
    (document? x) (.delete #^ODocument x)
    (orid? x) (.delete #^ODocument (load-item x))
    (oclass? x) (do (.dropClass (get-schema) (.getName #^OClass x))
                  (save! x))
    (keyword? x) (delete! (oclass x))
    :else (.delete *db* x)))

;;;;;;;;;;;;;;;;;;;;;;;;
;;; Class Properties ;;;
;;;;;;;;;;;;;;;;;;;;;;;;
(defvar- kw->it
  {:dictionary OProperty$INDEX_TYPE/DICTIONARY,
   :fulltext   OProperty$INDEX_TYPE/FULLTEXT,
   :unique     OProperty$INDEX_TYPE/UNIQUE,
   :not-unique OProperty$INDEX_TYPE/NOTUNIQUE,
   :proxy      OProperty$INDEX_TYPE/PROXY})

(defn oprop->map "Returns a map with all the relevant information from an OProperty object."
  [#^OProperty oprop]
  {:id (.getId oprop),                    :name (keyword (.getName oprop)),
   :type (.getType oprop),                :regexp (.getRegexp oprop),
   :min (.getMin oprop),                  :max (.getMax oprop),
   :linked-class (.getLinkedClass oprop), :linked-type (.getLinkedType oprop),
   :mandatory? (.isMandatory oprop),      :nullable? (not (.isNotNull oprop)),
   :indexed? (.isIndexed oprop),          :index (.getIndex oprop)})

(defn update-oprop!
  "Updates an OProperty object.

Please note:
  min & max must be strings.
  type must be of the OType class.
  index must be one of the following keywords: :dictionary, :fulltext, :unique, :not-unique, :proxy"
  [#^OProperty oprop {:keys [name type regexp min max mandatory? nullable? index]}]
  (when name (.setName oprop (clojure.core/name name)))
  (when type (.setType oprop type))
  (when regexp (.setRegexp oprop regexp))
  (when min (.setMin oprop min))
  (when max (.setMax oprop max))
  (when mandatory? (.setMandatory oprop mandatory?))
  (when nullable? (.setNotNull oprop (not nullable?)))
  (cond
    (false? index) (.dropIndex oprop)
    (nil? index) nil
    :else (.createIndex oprop (kw->it index)))
  (save-schema!)
  oprop)

(defn create-prop!
  "When providing a type, it must be of the class OType.
When using linked types #{EMBEDDED, LINK}, provide a vector of [link-type type]

When providing a configuration hash-map, it must be in the format specified for update-oprop!."
  ([#^OClass oclass pname ptype] (create-prop! pname ptype {}))
  ([#^OClass oclass pname ptype conf]
   (-> (if (vector? ptype)
         (.createProperty oclass (name pname) (first ptype) (second ptype))
         (.createProperty oclass (name pname) ptype))
     (update-oprop! conf))
   oclass))

(defn drop-prop! "Removes a property from an OClass."
  [kclass kname]
  (let [#^OClass kclass (oclass kclass)]
    (.dropProperty oclass (name kname))
    (save! kclass)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Document/GraphElement Classes ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn get-schema "Returns *db*'s OSchema"
  [] (-> *db* .getMetadata .getSchema))

(defn- save-schema! ""
  [] (.save (get-schema)))

(defn #^OClass oclass
  "Returns an OClass given the classname as a keyword.
If given an OClass, returns it inmediately."
  [kclass]
  (if (oclass? kclass)
    kclass
    (.getClass (get-schema) (name kclass))))

(defn get-class-name "Returns the classname from an OClass or an ODocument."
  [odoc] (keyword (if (instance? OClass odoc) (.getName #^OClass odoc) (.getClassName #^ODocument odoc))))

(defn oclasses "Returns a seq of all the OClass objects in the schema."
  [] (-> (get-schema) .getClasses seq))

(defn derive! "Derives a class from another in the schema."
  [ksubclass, ksuperclass]
  (let [#^OClass subclass (oclass ksubclass)
        #^OClass superclass (oclass ksuperclass)]
    (.setSuperClass subclass superclass)
    (save! subclass)))

(defn base-classes! ""
  [kclass] (map #(keyword (.getName #^OClass %)) (.getBaseClasses (oclass kclass))))

(defn create-class!
  "Creates a class in the given database and makes it inherit the given superclass. Superclass can be of type String, Class or OClass."
  ([kclass] (-> (get-schema) (.createClass (name kclass)) save!))
  ([kclass ksuperclass-or-schema]
   (let [oclass (create-class! kclass)]
     (if (map? ksuperclass-or-schema)
       (reduce (fn [oc [k v]] (if (vector? v)
                                (apply create-prop! oc k v)
                                (apply create-prop! oc k [v])))
               oclass ksuperclass-or-schema)
       (derive! kclass ksuperclass-or-schema))))
  ([kclass ksuperclass schema]
   (let [oclass (create-class! kclass ksuperclass)
         oclass (reduce (fn [oc [k v]] (if (vector? v)
                                         (apply create-prop! oc k v)
                                         (apply create-prop! oc k [v])))
                        oclass schema)]
     (save! oclass))))

(defn exists-class? ""
  [kclass] (.existsClass (get-schema) (name kclass)))

(defn superclass? ""
  [ksuperclass, ksubclass]
  (let [#^OClass subclass (oclass ksubclass)
        #^OClass superclass (oclass ksuperclass)]
    (.isSubClassOf subclass superclass)))

(defn subclass? ""
  [ksubclass, ksuperclass] (superclass? ksuperclass ksubclass))

(defn truncate-class! ""
  [kclass] (.truncate (oclass kclass)))

(defn schema-info ""
  []
  (let [schema (get-schema)]
    {:id (.getIdentity schema), :version (.getVersion schema),
     :classes (map #(keyword (.getName #^OClass %)) (.getClasses schema))}))

;;;;;;;;;;;;;
;;; Hooks ;;;
;;;;;;;;;;;;;
(defvar- +triggers+
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
