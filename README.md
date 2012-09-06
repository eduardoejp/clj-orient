
clj-orient
==========

`clj-orient` is a binding for the OrientDB document-graph NoSQL database management system. It is based on the OrientDB Java API.

Usage
-----

Simply add this to your leiningen deps: `[clj-orient "0.5.0"]`

Documentation
-------------

The documentation can be found here: http://eduardoejp.github.com/clj-orient/
You can also check out the wiki at: https://github.com/eduardoejp/clj-orient/wiki

Structure
---------

Currently the library is divided into 6 namespaces:
`core`: The core functionality and the document-db functionality.
`graph`: The graph-db functionality.
`object`: The object-db functionality.
`query`: Functionality for querying your database (either with native or SQL queries).
`schema`: For easy definition of database schemas.
`script`: For doing server-side scripting.
`script.clojure`: For easy server-side scripting with Clojure.

Tests
-----
There is a test suite in the test directory that right now tests almost all of the library functionality. It can be run using `lein test`.

Examples
--------

Working with the database:

	(use 'clj-orient.core)
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

	(use 'clj-orient.core)
	(let [u (document :user {:first-name "Foo", :last-name "Bar", :age 10})
	      u (assoc u :first-name "Mr. Foo", :age 20)]
	  (save! u))

Working with classes:

	(use '(clj-orient core graph))

	(with-db (open-graph-db! "remote:localhost/my-graph-db" "writer" "writer")
	  (create-class! :user)
	  (derive! :user (vertex-base-class))
	  (create-class! :knows (edge-base-class)))

Working with the graph-db:

	(use 'clj-orient.graph)

	(set-db! (open-graph-db! "remote:localhost/my-graph-db" "writer" "writer"))
	(let [a (save! (vertex :user {:first-name "John", :last-name "Doe", :age 20, :country "USA"}))
	      b (save! (vertex :user {:first-name "Jane", :last-name "Doe", :age 25, :country "USA"}))]
	  (save! (link! a :knows {:since (java.util.Date.)} b)))

Querying a Database:

	(use 'clj-orient.query)
	(native-query :user {:country "USA", :age [:$>= 20], :first-name [:$like "J%"]})
	
	(sql-query "SELECT FROM user WHERE country = :country AND age >= :age AND first-name LIKE :fname LIMIT 10"
	           {:country "USA", :age 20, :fname "J%"})
	
	; Pagination can be activated by passing a third optional boolean value of true.
	; That will query the next X elements each time the seq runs out until there are no more results.
	(sql-query "SELECT FROM user LIMIT 10" nil true)
	
	(clj-query '{:from user :where [(= country ?country) (>= age ?age) (like? first-name ?fname)] :limit 10}
	           {:country "USA", :age 20, :fname "J%"})

Hooks:

	(use 'clj-orient.core)
	(defhook my-hook
          (after-create [document]
            (println "Created new document:" document)))

	(add-hook! my-hook)

*Please note*: This is **not** a comprehensive guide. Please read the library documentation to know what functions and macros are available.
