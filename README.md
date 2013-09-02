ZClucy 
=====

[![Build Status](https://secure.travis-ci.org/yxzhang/clucy.png?branch=master)](http://travis-ci.org/yxzhang/clucy)

ZClucy is forked from clucy  a Clojure interface to [Lucene](http://lucene.apache.org/).
There are some enhanced futures in ZClucy :
(1)Support numeric values (such as int, long, float double)  store and query (simple query and range query)
(2)Support multivalued fileds.

Installation
------------

To install Clucy, add the following dependency to your `project.clj`
file:

    [zclucy "0.5.0"]

Usage
-----

To use Clucy, first require it:

    (ns example
      (:require [clucy.core :as clucy]))

Then create an index. You can use `(memory-index)`, which stores the search
index in RAM, or `(disk-index "/path/to/a-folder")`, which stores the index in
a folder on disk.

    (def index (clucy/memory-index))

Next, add Clojure maps to the index:

    (clucy/add index
       {:name "Bob", :job "Builder"}
       {:name "Donald", :job "Computer Scientist"})

You can remove maps just as easily:

    (clucy/delete index
       {:name "Bob", :job "Builder"})

Once maps have been added, the index can be searched:

    user=> (clucy/search index "bob" 10)
    ({:name "Bob", :job "Builder"})

    user=> (clucy/search index "scientist" 10)
    ({:name "Donald", :job "Computer Scientist"})

You can search and remove all in one step. To remove all of the
scientists...

    (clucy/search-and-delete index "job:scientist")

Numeric Types
--------------

You can add maps with numeric value to the index:

    (clucy/add index
       {:name "Bob", :age (int 20)}
       {:name "Donald", :age (int 35)})
       
Once maps have been added, the index can be searched:

	user=> (binding [clucy/*numeric-hints* {"age" "int"}]
	      (clucy/search index "age:20" 10))
	({:age 20, :name "Bob"})
	
Or do range query just as :

	user=> (binding [clucy/*numeric-hints* {"age" "int"}]
   	      (clucy/search index "age:[32 TO 35]" 10))
	({:age 35, :name "Donald"})

Numberic type can be one of  int, long, double, float.

Multivalued Fields
--------------

You can use clojure collection to manage multivalued fields, eg. 

    (clucy/add index
       {:name "Bob", :books ["Clojure Programming" "Clojure In Action"] }



Storing Fields
--------------

By default all fields in a map are stored and indexed. If you would
like more fine-grained control over which fields are stored and index,
add this to the meta-data for your map.

    (with-meta {:name "Stever", :job "Writer", :phone "555-212-0202"}
      {:phone {:stored false}})

When the map above is saved to the index, the phone field will be
available for searching but will not be part of map in the search
results. This example is pretty contrived, this makes more sense when
you are indexing something large (like the full text of a long
article) and you don't want to pay the price of storing the entire
text in the index.

Default Search Field
--------------------

A field called "\_content" that contains all of the map's values is
stored in the index for each map (excluding fields with {:stored false}
in the map's metadata). This provides a default field to run all
searches against. Anytime you call the search function without
providing a default search field "\_content" is used.

This behavior can be disabled by binding *content* to false, you must
then specify the default search field with every search invocation.
