
clj-orient
==========

`clj-orient` is a binding for the OrientDB document-graph NoSQL database management system. It is based on the OrientDB Java API.

Usage
-----

Simply add this to your leiningen deps: `[clj-orient "0.3.0"]`

WARNING
-------

As some of you may notice, version 0.3.0 has an slightly different set of dependencies. The changes to the code were small, but 2 important changes were introduced to the dependencies.
The first one is that clj-orient now depends on Clojure 1.3.
The second one is that clj-orient now depends on OrientDB 1.0rc6.

I really hope this change does not affect anybody's code. If anybody has trouble with this version, you can always use version 0.2.2, which uses the old dependencies.

Documentation
-------------

The documentation can be found here: http://eduardoejp.github.com/clj-orient/

Structure
---------

Currently the library is divided into four namespaces:
`core`: The core functionality and the document-db functionality.
`graph`: The graph-db functionality.
`object`: The object-db functionality.
`query`: Functionality for querying your database (either with native or SQL queries).

Examples
--------

Working with the database:

	; Opening the database as a document DB and setting the *db* var for global use.
	; A database pool is used, to avoid the overhead of creating a DB object each time.
	(set-db! (open-document-db! "remote:localhost/my-db" "writer" "writer"))

	; Dynamically bind *db* to another DB.
	; The DB is closed after all the forms are evaluated.
	(with-db (open-document-db! "remote:localhost/another-db" "writer" "writer")
	  (form-1 ...)
	  (form-2 ...)
	  (form-3 ...)
	  ...
	  (form-n ...))

	; Close the DB
	(close-db!)

	; Using transactions (*db* must be bound to some database in the surrounding scope).
	(with-tx
	  (form-1 ...)
	  (form-2 ...)
	  (form-3 ...)
	  ...
	  (form-n ...))

Working with documents:

	(let [u (document :user {:first-name "Foo", :last-name "Bar", :age 10})
	      u (passoc! u :first-name "Mr. Foo", :age 20)]
	  (println (doc->map u))
	  (save! u))

Working with classes:

	(oclass :user)
	(derive! :user (get-vertex-base-class))
	(create-class! :knows (get-edge-base-class))

Working with the graph-db:

	(let [a (save! (vertex :user {:first-name "John", :last-name "Doe", :age 20, :country "USA"}))
		    b (save! (vertex :user {:first-name "Jane", :last-name "Doe", :age 25, :country "USA"}))]
		(save! (link! a :knows {:since "2011/8/1"} b)))

Querying a Database:

	(native-query :user {:country "USA", :age [:$>= 20], :first-name [:$like "J%"]})
	
	(sql-query "SELECT FROM user WHERE country = :country AND age >= :age AND first-name LIKE :fname LIMIT 10"
	           {:country "USA", :age 20, :fname "J%"})
	
	; Pagination can be activated by passing a third optional boolean value of true.
	; That will query the next X elements each time the seq runs out until there are no more results.
	(sql-query "SELECT FROM user LIMIT 10" nil true)

Hooks:

	(defhook my-hook
		(after-create [document]
		  (println "Created new document:" (doc->map document))))

	(add-hook! my-hook)

*Please note*: This is not a comprehensive guide. Please read the library documentation to know what functions and macros are available.

