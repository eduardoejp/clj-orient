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
      :doc "Utilities for scripting OrientDB using Clojure."}
     clj-orient.script.clojure
  (:use clj-orient.script
        [clj-orient.kv :only [<<]]))

(def *clj-script-fns (atom []))

(defmacro run-clj! "Runs forms as a server-side script and returns the resulting value."
  [& forms] `(run-script! "Clojure" ~(pr-str `(do ~@forms))))

(defmacro defdbfn "Defines a function that can be executed on the server and also from Clojure code."
  [sym & forms]
  (let [[[doc-str] impls] (split-with string? forms)
        impls (if (vector? (first impls))
                (list impls)
                impls)
        impls (map (fn [[args & body]]
                     (let [args* (map #(->> % (str "~") symbol)
                                      args)
                           rep-map (reduce conj {} (map (fn [o n] [o n]) args args*))
                           body* (apply pr-str (clojure.walk/prewalk-replace rep-map body))]
                       (list args `(run-script! "Clojure" (<< ~body*)))))
                   impls)]
    `(do
       (swap! *clj-script-fns conj ~(pr-str `(defn ~sym ~@forms)))
       ~(if doc-str
          `(defn ~sym ~doc-str ~@impls)
          `(defn ~sym ~@impls))
       )))

(defn install-db-fns!
  "Installs inside the server all the functions defined with defdbfn."
  []
  (doseq [fs @*clj-script-fns]
    (run-script! "Clojure" fs)))
