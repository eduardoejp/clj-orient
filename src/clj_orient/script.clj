
(ns clj-orient.script
  (:import com.orientechnologies.orient.core.command.script.OCommandScript)
  (:use (clj-orient core)))

(defn run-script
  "Executes the given script on the current *db.*
If no type is provided, it defaults to \"Javascript\"."
  ([type script] (-> *db* (.command (OCommandScript. type script)) (.execute (object-array []))))
  ([script] (run-script "Javascript" script)))
