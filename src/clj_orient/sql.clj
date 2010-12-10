
(ns clj-orient.sql
  #^{:doc "This namespace implements the SQL interface for OrientDB."
     :author "Eduardo Emilio Julián Pereyra"}
  (:import (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)))

(defn run-query!
  "Runs the given SQL query against the DB."
  [db qry]
  (-> (OSQLSynchQuery. qry) (.query db)))

(defn run-command!
  "Executes the given SQL command against the DB."
  [db comm]
  (-> (OCommandSQL. comm) (.command db) .execute))
