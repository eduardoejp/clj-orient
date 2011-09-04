(defproject clj-orient "0.2.2"
  :description "Wrapper for the OrientDB Native APIs. It supports version 1.0 of the APIs."
  :url "https://github.com/eduardoejp/clj-orient"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [com.orientechnologies/orient-commons "1.0rc3"]
                 [com.orientechnologies/orientdb-core "1.0rc3"]
                 [com.orientechnologies/orientdb-client "1.0rc3"]]
  :dev-dependencies [[org.clojars.rayne/autodoc "0.8.0-SNAPSHOT"]]
  :repositories {"orient" "http://www.orientechnologies.com/listing/m2"}
  :autodoc {:name "clj-orient"
            :description "Wrapper for the OrientDB Native APIs. It supports version 1.0 of the APIs."
            :copyright "Copyright 2011 Eduardo Julian"
            :web-src-dir "http://github.com/eduardoejp/clj-orient/blob/"
            :web-home "http://eduardoejp.github.com/clj-orient/"
            :output-path "autodoc"}
	)
