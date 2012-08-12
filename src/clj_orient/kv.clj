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

(ns ^{:author "Eduardo Julian <eduardoejp@gmail.com>",
      :doc "An implementation of a key-value store on top of OrientDB's dictionary indices."}
     clj-orient.kv
  (:refer-clojure :exclude [assoc dissoc get keys count type conj sort inc dec
                            assoc-in update-in map sort])
  (:require [clj-orient.core :as oc])
  (:import (com.orientechnologies.orient.core.index OIndex OSimpleKeyIndexDefinition)
           (com.orientechnologies.orient.core.record.impl ODocument)
           (com.orientechnologies.orient.core.metadata.schema OClass$INDEX_TYPE OType)
           com.orientechnologies.orient.core.sql.OCommandSQL
           com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
           (com.orientechnologies.orient.core.db.record OTrackedList OTrackedMap OTrackedSet)
           clj_orient.core.CljODoc
           (java.util Date Calendar))
  (:use clojure.template))

(declare expire)

; <Globals>
(def ^:dynamic ^OIndex *bucket*)
(def ^:private *cleaners (atom {}))

; <Utils>
(defn- expiry->date [expiry]
  (if (instance? Date expiry)
    expiry
    (-> (Calendar/getInstance)
      (doto (.add Calendar/MILLISECOND expiry))
      .getTime)))

(defn- expired? [d] (and (.field d "e") (.after (Date.) (.field d "e"))))

(defn- v-in [x]
  (condp instance? x
    CljODoc        (.-odoc x)
    java.util.List (mapv v-in x)
    java.util.Set  (set (mapv v-in x))
    java.util.Map  (apply clojure.core/conj {} (mapv (fn [[k v]] [k (v-in v)]) x))
                   x))

(defn- v-out [x]
  (condp instance? x
    java.lang.String    x
    java.lang.Number    x
    java.lang.Character x
    java.lang.Boolean   x
    OTrackedList        (mapv v-out x)
    OTrackedMap         (->> x (into {}) (mapcat (fn [[k v]] [(v-out k) (v-out v)])) (apply hash-map))
    OTrackedSet         (->> x (clojure.core/map v-out) set)
    ODocument           (CljODoc. x)
                        x))

(defn- fetch-all-exists [ks]
  (->> (for [k ks]
         (if-let [rid (.get *bucket* k)]
           (.field (.load oc/*db* rid "*:-1") "v")))
    (filter #(not (nil? %)))))

; <API>
; String Interpolation
(defmacro <<
  "An small macro to do simple string interpolation, useful for generating keys.
Example:
(let [id 123]
  (<< \"user/~id/email\"))"
  [key]
  (let [kws (re-seq #"~[^0-9\s/~\(\)\[\]\{\}][^\s/~\(\)\[\]\{\}]*" key)
        syms (clojure.core/map #(-> % (.substring 1) symbol) kws)
        form (reduce (fn [[k & form] [kw s]]
                       (let [i (.indexOf k kw)]
                         (list* (.substring k (+ i (clojure.core/count kw)))
                                s
                                (.substring k 0 i)
                                form)))
                     (list key) (partition 2 (interleave kws syms)))
        form (filter #(and (not (nil? %)) (or (symbol? %) (not (empty? %)))) form)]
    `(str ~@(reverse form))))

; Buckets
(defn create-bucket! "" [bname]
  (-> oc/*db* .getMetadata .getIndexManager
    (.createIndex (name bname) (.toString OClass$INDEX_TYPE/DICTIONARY)
      (OSimpleKeyIndexDefinition. (into-array [OType/STRING]))
      nil nil))
  nil)

(defn delete-bucket! "" [bname] (-> oc/*db* .getMetadata .getIndexManager (.dropIndex (name bname))) nil)

(defmacro with-bucket "" [bucket & body]
  `(binding [*bucket* (-> oc/*db* .getMetadata .getIndexManager (.getIndex ~(if (keyword? bucket) (name bucket) `(name ~bucket))))]
     ~@body))

(defn clear "Removes all keys from the current bucket."
  [] (.clear *bucket*) nil)
(defn unload "Unloads the current bucket to free memory."
  [] (.unload *bucket*) nil)

; Basic Fns
(defn exists? "" [k] (.contains *bucket* k))

(defn assoc
  "Puts a key in the current bucket with an optional expiration time.
expiry: Can be a java.util.Date or a delay in milliseconds (counting from the current time).

*Note*: If a key already exists with an expiration date, it will be overwritten and the expiration date forgotten."
  ([k v]
   (if-let [rid (.get *bucket* k)]
     (doto (.load oc/*db* rid "*:-1") (.removeField "e") (.field "v" (v-in v)) .save)
     (.put *bucket* k (ODocument. {"v" (v-in v)})))
   nil)
  ([k v expiry]
   (if-let [rid (.get *bucket* k)]
     (doto (.load oc/*db* rid "*:-1") (.field "e" (expiry->date expiry)) (.field "v" (v-in v)) .save)
     (.put *bucket* k (ODocument. {"v" (v-in v), "e" (expiry->date expiry)})))
   nil))

(defn assoc-nx "Puts the key in the current bucket if it doesn't already exist."
  ([k v] (if (exists? k) (do (assoc k v) true) false))
  ([k v expiry] (if (exists? k) (do (assoc k v expiry) true) false)))

(defn dissoc "Removes a key from the current bucket."
  [k]
  (oc/with-tx
    (when-let [rid (.get *bucket* k)]
      (.delete oc/*db* (.load oc/*db* rid "*:-1"))
      (.remove *bucket* k))
    nil))

(defn update
  "Updates a key in the current bucket without changing it's expiration date."
  [k f & args]
  (if-let [rid (.get *bucket* k)]
    (let [d (.load oc/*db* rid "*:-1")]
      (if (expired? d)
        (dissoc k)
        (oc/with-tx
          (let [v (apply f (v-out (.field d "v")) args)]
            (-> d (.field "v" (v-in v)) .save)
            v))))))

(defn get
  "Retrieves a key from the current bucket."
  ([k not-found]
   (if-let [rid (.get *bucket* k)]
     (let [d (.load oc/*db* rid "*:-1")]
       (if (expired? d)
         (do (dissoc k) not-found)
         (v-out (.field d "v"))))
     not-found))
  ([k] (get k nil)))

(defn keys
  "Returns all keys in the current bucket or it can optionally filter them based on some RegEx pattern or if they contain a given substring."
  ([] (clojure.core/map #(.field % "key") (.keys *bucket*)))
  ([pattern]
   (let [q (OSQLSynchQuery. (str "SELECT key FROM index:" (.getName *bucket*) " WHERE key "
                                 (if (string? pattern)
                                   "CONTAINSTEXT ?"
                                   "MATCHES ?")))]
     (->> (.query oc/*db* q (to-array [(str pattern)]))
       (clojure.core/map #(.field % "key"))))))

(defn count "How many keys are in the current bucket?"
  [] (.getSize *bucket*))

(defn rename "" [from-key to-key]
  (if-let [rid (.get *bucket* from-key)]
    (let [d (.load oc/*db* rid "*:-1")]
      (oc/with-tx
        (.put *bucket* to-key (ODocument. {"v" (.field d "v"), "e" (.field d "e")}))
        (.delete oc/*db* d)
        (.remove *bucket* from-key))
      true)
    false))

(defn rename-nx "Renames the key only if the destination doesn't already exist."
  [from-key to-key]
  (if (exists? to-key)
    false
    (rename from-key to-key)))

(defn copy "" [from-key to-key]
  (if-let [rid (.get *bucket* from-key)]
    (let [d (.load oc/*db* rid "*:-1")]
      (.put *bucket* to-key (ODocument. {"v" (.field d "v"), "e" (.field d "e")}))
      true)
    false))

(defn copy-nx "Copies the key only if the destination doesn't already exist."
  [from-key to-key]
  (if (exists? to-key)
    false
    (copy from-key to-key)))

(defn move "Moves the desired list of keys from the current bucket to the specified bucket.
Removes the keys from the current bucket afterwards."
  [bucket keys]
  (let [vals (for [k keys] (.get *bucket* k))
        pairs (filter (fn [[k v]] v)
                      (clojure.core/map (fn [k v] [k v]) keys vals))]
    (if (empty? pairs)
      false
      (oc/with-tx
        (with-bucket bucket
          (doseq [[k v] pairs] (.put *bucket* k v)))
        (doseq [[k] pairs] (.remove *bucket* k))
        true))))

; Expiration
(defn expire "Sets up an expiration date on the given key.
expiry: Can be a java.util.Date or a delay in milliseconds (counting from the current time)."
  [k expiry]
  (boolean (if-let [rid (.get *bucket* k)]
             (.save (.field (.load oc/*db* rid "e:1") "e" (expiry->date expiry))))))
(defn persist "Removes the expiration date on the given key."
  [k]
  (boolean (if-let [rid (.get *bucket* k)]
             (.save (.removeField (.load oc/*db* rid "e:1") "e")))))
(defn expires? "" [k]
  (boolean (if-let [rid (.get *bucket* k)]
             (.field (.load oc/*db* rid "e:1") "e"))))
(defn persistent? "" [k]
  (boolean (if-let [rid (.get *bucket* k)]
             (not (.field (.load oc/*db* rid "e:1") "e")))))
(defn expiry "Returns the expiration date on the given key as a java.util.Date object."
  [k]
  (if-let [rid (.get *bucket* k)]
    (.field (.load oc/*db* rid "e:1") "e")))

(defn ttl "Returns the time-to-live (long) left on the given key as of the current time."
  [k]
  (if-let [rid (.get *bucket* k)]
    (if-let [e (.field (.load oc/*db* rid "e:1") "e")]
      (- (.getTime e) (.getTime (Date.))))))

; Expired Key Cleanup
(defn clean-expired-keys! "Removes all expired keys (as of the current time) in the current bucket."
  []
  (.execute (.command oc/*db* (OCommandSQL. (str "DELETE FROM index:" (.getName *bucket*) " WHERE e > " (Date.))))
    (into-array []))
  nil)

(defn periodic-expired-keys-cleanup! "Sets up a cleaning daemon for cleaning expired keys."
  [millis]
  (let [cleaner (proxy [java.util.TimerTask] [] (run [] (clean-expired-keys!)))
        timer (java.util.Timer. true)]
    (swap! *cleaners assoc (.getName *bucket*) timer)
    (.schedule timer cleaner 0 millis)
    nil))

(defn stop-expiry-cleanup! "Stops the cleaning daemon for the current bucket."
  []
  (.cancel (@*cleaners (.getName *bucket*)))
  (swap! *cleaners dissoc (.getName *bucket*))
  nil)

; Util Fns
(defn inc "" [k] (update k clojure.core/inc))
(defn inc-by "" [k n] (update k #(+ % n)))
(defn dec "" [k n] (update k clojure.core/dec))
(defn dec-by "" [k n] (update k #(- % n)))
(defn type "Returns the type of the value on that key as a Java class or interface."
  [k]
  (let [v (get k)]
    (condp instance? v
      String       (if (.startsWith v ":")
                     clojure.lang.Keyword
                     java.lang.String)
      OTrackedList java.util.List
      OTrackedMap  java.util.Map
      OTrackedSet  java.util.Map
      ODocument    CljODoc
      (type v)
    )))

; Data Structures
(defn conj "Conjoins the given elements on the data-structure at the key. Works for lists, maps and sets."
  [k & xs]
  (if-let [rid (.get *bucket* k)]
    (let [d (.load oc/*db* rid "*:-1")
          v (.field d "v")]
      (condp instance? v
        OTrackedList (doseq [x xs] (.add v x))
        OTrackedMap (doseq [[mk mv] xs] (.put v mk (v-in mv)))
        OTrackedSet (doseq [x xs] (.add v x)))
      (.save d)
      true)
    false))

(do-template [<normal> <store> <method>]
  (do
    (defn <normal> "For sets only. Returns the result."
      [ks]
      (let [[x & xs] (fetch-all-exists ks)]
        (set (reduce #(do (<method> %1 %2) %1) (.writeReplace x) xs))))
    (defn <store> "For sets only. Same as the normal version, but stores the result in the destination key."
      [destination ks]
      (assoc destination (<normal> ks))))
  difference   difference*   .removeAll
  union        union*        .addAll
  intersection intersection* .retainAll
  )

(do-template [<sym> <args> <doc> <body>]
  (defn <sym> <doc> <args>
    (if-let [rid (.get *bucket* k)]
      (let [d (.load oc/*db* rid "*:-1")
            n-k (.field d "v")
            n (reduce #(condp instance? %1
                         ODocument    (.field %1 (name %2))
                         OTrackedList (.get %1 %2)
                         OTrackedMap  (.get %1 (name %2)))
                      n-k (butlast ks))]
        (oc/with-tx
          <body>
          (.save d))
        true)
      false))
  assoc-in [k ks v] "Assocs inside structures. Works for embedded documents, lists and maps."
  (condp instance? n
    ODocument    (.field n (name (last ks)) v)
    OTrackedList (.set n (last ks))
    OTrackedMap  (.put n (name (last ks)) v))
  
  dissoc-in [k ks] "Dissocs inside structures. Works for embedded documents, lists, maps and sets."
  (condp instance? n
    ODocument    (.removeField n (name (last ks)))
    OTrackedList (.remove n (let [i (last ks)] (if (integer? i) (int i) i)))
    OTrackedMap  (.remove n (name (last ks)))
    OTrackedSet  (.remove n (name (last ks))))
  
  update-in [k ks f & args] "Updates inside structures. Works for embedded documents, lists and maps."
  (let [i (last ks)
        v (v-out (condp instance? n
                   ODocument    (.field n (name i))
                   OTrackedList (.get n i)
                   OTrackedMap  (.get n (name i))))
        v* (apply f v args)]
    (condp instance? n
      ODocument    (.field n (name i) v*)
      OTrackedList (.set n i v*)
      OTrackedMap  (.put n (name i) v v*)))
  )

(defn map "For lists only.\nMaps f on the values the lists."
  [f & ks]
  (let [ols (for [k ks] (.field (.load oc/*db* (.get *bucket* k) "*:-1") "v"))
        iters (clojure.core/map #(.iterator %) ols)
        vals (loop [vals []]
               (if (every? true? (clojure.core/map #(.hasNext %) iters))
                 (recur (clojure.core/conj vals (mapv #(.next %) iters)))
                 vals))]
    (mapv #(apply f %) vals)))

(defn map*
  "For lists only.\nSame as the normal version, but stores the result in the destination key."
  [destination f & ks]
  (oc/with-tx
    (assoc destination (apply map f ks))))

(defn sort "Sorts list k with function/comparator f."
  [f k]
  (if-let [rid (.get *bucket* k)]
    (let [d (.load oc/*db* rid "*:-1")
          l (.field d "v")]
      (java.util.Collections/sort l f)
      (.save d)
      true)
    false))
