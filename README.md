ZClucy 
=====

To install in your local repository:

```
lein localrepo install -p pom.xml /Users/hans/mcna/clucy/target/zclucy-mcna-0.9.2.jar zclucy-mcna 0.9.2
```

[![Build Status](https://secure.travis-ci.org/yxzhang/clucy.png?branch=master)](http://travis-ci.org/yxzhang/clucy)

ZClucy is forked from clucy  a Clojure interface to [Lucene](http://lucene.apache.org/).
There are some enhanced futures in ZClucy :

1. Supports numeric values (such as int, long, float double)  
1. Supports multivalued fileds.
1. Supports to sort results
1. Supports parallel indexing large number of data
1. Supports schema defined as index's  metadata
1. Supports upsert (Insert documents or update them if they exist)
1. Supports to only load certain stored fields and fast collect field values for statistics   (eg. sum, avg )


Installation
------------

To install Clucy, add the following dependency to your `project.clj`
file:

```clojure
    [zclucy "0.9.2"]
```

Usage
-----

To use Clucy, first require it:

```clojure
    (ns example
      (:require [clucy.core :as clucy]))
```

Then create an index. You can use `(memory-index)`, which stores the search
index in RAM, or `(disk-index "/path/to/a-folder")`, which stores the index in
a folder on disk.

```clojure
    (def index (clucy/memory-index))
```

Next, add Clojure maps to the index:

```clojure
    (clucy/add index
       {:name "Bob", :job "Builder"}
       {:name "Donald", :job "Computer Scientist"})
```

You can remove maps just as easily:

```clojure
    (clucy/delete index
       {:name "Bob", :job "Builder"})
```

Once maps have been added, the index can be searched:

```clojure
    user=> (clucy/search index "bob" 10)
    ({:name "Bob", :job "Builder"})
```

```clojure
    user=> (clucy/search index "scientist" 10)
    ({:name "Donald", :job "Computer Scientist"})
```

You can search and remove all in one step. To remove all of the
scientists...

```clojure
    (clucy/search-and-delete index "job:scientist")
```    

Manipulate Schema
--------------

By default, every field is a string stored, indexed, analyzed and stores norms. You can customise it just like :

```clojure
(def people-schema {:name {:type "string"} :age {:type "int" }})
```

```clojure
(binding [clucy/*schema-hints* people-schema]
;.... do some adding
;.....do some query
)
```

Or you can add a schema with index when create it :

```clojure
(def index (clucy/memory-index people-schema))
```

Then you need not  bind *schema-hints* anymore. Now here two statements get the same result :

```clojure
(clucy/add ....)
(clucy/add ....)
```

```clojure
(binding [clucy/*schema-hints* people-schema]
	(clucy/add ....)
	(clucy/add ....)
)
```

Then name is still a string stored, indexed, analyzed and stores norms, but age is a int without being analyzed and  norms.


Numeric Types
--------------

You can add maps with numeric value to the index:

```clojure
(def people-schema {:name {:type "string"} :age {:type "int"}})
```

```clojure
(binding [clucy/*schema-hints* people-schema]
    (clucy/add index
       {:name "Bob", :age (int 20)}
       {:name "Donald", :age (int 35)}))
```
       
Once maps have been added, the index can be searched:

```clojure
	user=> (binding [clucy/*schema-hints* people-schema]
	      (clucy/search index "age:20" 10))
	({:age 20, :name "Bob"})
```
	
Or do range query just as :

```clojure
	user=> (binding [clucy/*schema-hints* people-schema]
   	      (clucy/search index "age:[32 TO 35]" 10))
	({:age 35, :name "Donald"})
```

Numberic type can be one of  int, long, double, float.

Multivalued Fields
--------------

You can use clojure collection to manage multivalued fields, eg. 

```clojure
    (clucy/add index
       {:name "Bob", :books ["Clojure Programming" "Clojure In Action"] }
```

```clojure
    user=> (clucy/search index "books:action" 10)
       ({:name "Bob", :books ["Clojure Programming" "Clojure In Action"]})
```

Sort Results
--------------
First add some documents with a defined schema

```clojure
(def people-schema {:name {:type "string"} :age {:type "int" }})

(binding [clucy/*schema-hints* people-schema]
    (clucy/add index
       {:name "Bob", :age (int 20)}
       {:name "Donald", :age (int 35)}))
```

Then you can sort the result when search them :

```clojure
user=> ((binding [clucy/*schema-hints* people-schema]
          (clucy/search index "*:*" 10 :sort-by "age desc"))
({:age 35, :name "Donald"} {:age 20, :name "Bob"})
```

You can sort by several fields just like :

```clojure
((binding [clucy/*schema-hints* people-schema]
          (clucy/search index "*:*" 10 :sort-by "age desc, name asc"))
```

Or sort  by document number (index order) :

```clojure
((binding [clucy/*schema-hints* people-schema]
          (clucy/search index "*:*" 10 :sort-by "$doc asc"))
```
          
Or sort by  document score (relevance):

```clojure
((binding [clucy/*schema-hints* people-schema]
          (clucy/search index "*:*" 10 :sort-by "$score asc"))
```

Parallel indexing
--------------------

When you want to index a large number of data such as data from a large text file with one record per line,  you should use padd instead of add

```clojure
    (with-open [r (clojure.java.io/reader file)]
       (let [stime (System/currentTimeMillis)
               reporter (fn [n] 
                                 (when (= (mod n 100000) 0) ; print process per 100K
                                   (println n " cost:"(- (System/currentTimeMillis) stime)))) ]
                (clucy/padd index reporter 
                      (map 
                              #(let [row (clojure.string/split % #"\s+")] 
                                                {:id (row 0), :name (row 1) })
                                                 (line-seq r)))))
```

Upsert
--------------------
Before use upsert , you must define ID fields (just like primary key) in schema, for example  use "id" as unique value field :

```clojure
(def index (clucy/disk-index "mypath" {_id [:id]}, :id {:type "int"},  :name {:type "string"}))

;There's no record so just insert it.
(clucy/upsert  index {:id 1 :name "Tom"})

=>(clucy/search index "*:*" 10)
;({:id 1 :name "Tom"})

;now update Tom to Jack
(clucy/upsert index {:id 1 :name "Jack"})

=>(clucy/search index "*:*" 10)
;({:id 1 :name "Jack"})
```


Load certain stored fields and fast collect field values for statistics   (eg. sum, avg )
--------------------
```clojure
(let [index (memory-index people-schema)
                   avg (atom 0) sum (atom 0) max (atom 0) min (atom 0)]
             ;Do some insert here ........
             ;.......
             (search index "age:[34 TO 48]" 100 :fields [:age] ; only load age field
                     :doc-collector (fn [{age :age} i total]
                                                 (println "i:" i ",total: " total ",age:" age)
                                                 (swap! sum + age)
                                                 (when (or (= i 0) (> @min age)) (reset! min age))
                                                 (when (or (= i 0) (> age @max)) (reset! max age))
                                                 (when (= i (dec total)) 
                                                   (reset! avg  (/ @sum total)))))
```


Default Search Field
--------------------

A field called "\_content" that contains all of the map's values is
stored in the index for each map (excluding fields with {:stored false}
in the map's metadata). This provides a default field to run all
searches against. Anytime you call the search function without
providing a default search field "\_content" is used.

This behavior can be disabled by binding *content* to false, you must
then specify the default search field with every search invocation.
