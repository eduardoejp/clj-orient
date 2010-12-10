
(ns clj-orient.sql
  (:import (com.orientechnologies.orient.core.sql.query OSQLSynchQuery)
    (com.orientechnologies.orient.core.sql OCommandSQL)))

(defn sql-query!
  "Runs the given SQL query against the DB."
  [db qry]
  (-> (OSQLSynchQuery. qry) (.query db)))

(defn sql-command!
  "Executes the given SQL command against the DB."
  [db comm]
  (-> (OCommandSQL. comm) (.command db) .execute))



