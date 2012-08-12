(defproject clj-orient "0.5.0"
  :description "Wrapper for the OrientDB Native APIs. It supports version 1.1 of the APIs."
  :url "https://github.com/eduardoejp/clj-orient"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :plugins [[lein-autodoc "0.9.0"]
            [lein-swank "1.4.4"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.orientechnologies/orient-commons "1.1.0"]
                 [com.orientechnologies/orientdb-client "1.1.0"]
                 [com.orientechnologies/orientdb-core "1.1.0"]
                 #_[com.orientechnologies/orientdb-object "1.1.0"]
                 ]
  :repositories {"sonatype" "https://oss.sonatype.org/content/groups/public/"}
  :autodoc {:name "clj-orient"
            :description "Wrapper for the OrientDB Native APIs. It supports version 1.1 of the APIs."
            :copyright "Copyright 2011~2012 Eduardo Julian"
            :web-src-dir "http://github.com/eduardoejp/clj-orient/blob/"
            :web-home "http://eduardoejp.github.com/clj-orient/"
            :output-path "autodoc"}
  )
