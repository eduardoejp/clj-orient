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

(ns ^{:author "Eduardo Julián <eduardoejp@gmail.com>",
      :doc "OrientDB scripting."}
  clj-orient.script
  (:import com.orientechnologies.orient.core.command.script.OCommandScript)
  (:require [clj-orient.core :as oc]))

(defn run-script!
  "Executes the given script on the current *db.*
If no type is provided, it defaults to \"Javascript\"."
  ([type script] (-> oc/*db* (.command (OCommandScript. type script)) (.execute (object-array []))))
  ([script] (run-script! "Javascript" script)))
