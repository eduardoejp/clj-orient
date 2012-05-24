(defproject clj-orient "0.4.0"
  :description "Wrapper for the OrientDB Native APIs. It supports version 1.0 of the APIs."
  :url "https://github.com/eduardoejp/clj-orient"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :plugins [[lein-autodoc "0.9.0"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.orientechnologies/orient-commons "1.0"]
                 [com.orientechnologies/orientdb-core "1.0"]
                 [com.orientechnologies/orientdb-client "1.0"]]
  :repositories {"sonatype" "https://oss.sonatype.org/content/repositories/releases/"}
  :autodoc {:name "clj-orient"
            :description "Wrapper for the OrientDB Native APIs. It supports version 1.0 of the APIs."
            :copyright "Copyright 2011~2012 Eduardo Julian"
            :web-src-dir "http://github.com/eduardoejp/clj-orient/blob/"
            :web-home "http://eduardoejp.github.com/clj-orient/"
            :output-path "autodoc"}
	)
