
clj-orient
==========

`clj-orient` is a binding for the OrientDB document-graph NoSQL database management system. It is based on the OrientDB Java API.

Usage
-----

Simply add this to your leiningen deps: `[clj-orient "0.1.0"]`

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

	; Opening the database as a document DB
	; Opens the database and binds the database object to the *db* var with which most functions in the library work.
	; There are variants of this function for the graph-db and object-db.
	(open-document-db! "remote:localhost/my-db" "writer" "writer")

	; Set *db* to another DB
	; The passed parameter must be a database object instance, not a path to the DB.
	(set-db! another-db)

	; Dynamically bind *db* to another DB. Also requires a DB object.
	(with-db another-db
		(form-1 ...)
		(form-2 ...)
		(form-3 ...)
		...
		(form-n ...))

	; Close the DB
	(close-db!)

	; Using transactions
	(with-tx
		(form-1 ...)
		(form-2 ...)
		(form-3 ...)
		...
		(form-n ...))

Working with documents:

	(let [u (document :user {:first-name "Foo", :last-name "Bar", :age 10})
		    u (passoc! u :first-name "Mr. Foo", :age 10)]
		(println (doc->map u))
		(save! u))

Working with classes:

	(get-class :user)
	(derive! :user (get-vertex-base-class))
	(create-class! :knows (get-edge-base-class))

Working with the graph-db:

	(let [a (save! (vertex :user {:first-name "John", :last-name "Doe", :age 20}))
		    b (save! (vertex :user {:first-name "Jane", :last-name "Doe", :age 25}))]
		(save! (link! a :knows {:since "2011/8/1"} b)))

Querying a Database:

	(native-query :user {:country "USA", :age [:$>= 20], :name [:$matches "John%"]})

	(sql-query "SELECT FROM user WHERE country = :country AND age >= :age AND name MATCHES :name"
		         {:country "USA", :age 20, :name "John%"}
		         100)

Hooks:

	(defhook my-hook
		(after-create [document]
		  (println "Created new document:" (doc->map document))))

	(add-hook! my-hook)

*Please note*: This is not a comprehensive guide. Please read the library documentation to know what functions or macros are available.

