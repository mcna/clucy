(ns clucy.core
  (:import (java.io StringReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.document Document FieldType  FieldType$NumericType Field IntField FloatField LongField DoubleField Field$Index Field$Store)
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery IndexSearcher Query ScoreDoc  Sort SortField SortField$Type
                                     Scorer TermQuery MatchAllDocsQuery NumericRangeQuery)
           (org.apache.lucene.search.highlight Highlighter QueryScorer
                                               SimpleHTMLFormatter)
           (org.apache.lucene.util Version AttributeSource)
           (org.apache.lucene.store NIOFSDirectory RAMDirectory Directory)))

(def ^{:dynamic true} *version* Version/LUCENE_44)

(def ^{:dynamic true} *analyzer* (StandardAnalyzer. *version*))

(def ^:dynamic  *doc-with-meta?* true)

;; flag to indicate a default "_content" field should be maintained
(def ^{:dynamic true} *content* true)

(def ^:dynamic  *schema-hints* {:*content* true,  :*doc-with-meta?* true})

(defn as-int ^Integer [x]
  (cond
    (instance? Integer  x) x
    (string? x) (Integer/parseInt x)
    :else (int x)))

(defn as-float ^Float [x]
  (cond
    (instance? Float  x) x
    (string? x) (Float/parseFloat x)
    :else (float x)))

(defn as-long ^long [x]
  (cond
    (integer? x) x
    (string? x) (Long/parseLong x)
    :else (long x)))

(defn as-double ^double [x]
  (cond
    (float? x) x
    (string? x) (Double/parseDouble x)
    :else (double x)))


;; To avoid a dependency on either contrib or 1.2+
(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
    (str x)))



(defn memory-index
  "Create a new index in RAM."
  ([]
    (RAMDirectory.))
  ([schema-hints]
    (proxy [RAMDirectory  clojure.lang.IMeta] [] (meta [] schema-hints))))

(defn disk-index
  "Create a new index in a directory on disk."
  ([^String dir-path]
    (NIOFSDirectory. (File. dir-path)))
  ([^String dir-path schema-hints]
    (proxy [NIOFSDirectory clojure.lang.IMeta] [(File. dir-path)] (meta [] schema-hints))))

(defn- index-writer
  "Create an IndexWriter."
  ^IndexWriter
  [index]
  (IndexWriter. index
                (IndexWriterConfig. *version* *analyzer*)))

(defn- index-reader
  "Create an IndexReader."
  ^IndexReader
  [index]
  (DirectoryReader/open ^Directory index))

(defn- add-field
  "Add a Field to a Document.
  Following options are allowed for meta-map:
  :stored - when false, then do not store the field value in the index.
  :indexed - when false, then do not index the field.
  :analyzed - when :indexed is enabled use this option to disable/eneble Analyzer for current field.
  :norms - when :indexed is enabled user this option to disable/enable the storing of norms."
  ([document key value]
     (add-field document key value {}))

  ([document key value meta-map]
    (let [n (name key)
          store? (not (false? (:stored meta-map)))
          index? (not (false? (:indexed meta-map)))
          vt (:type meta-map)
          field (case vt 
                  "int" (IntField. n (as-int value) (if store? IntField/TYPE_STORED IntField/TYPE_NOT_STORED))
                  "long" (LongField. n (as-long value)  (if store? LongField/TYPE_STORED LongField/TYPE_NOT_STORED))
                  "float" (FloatField. n (as-float value)  (if store? FloatField/TYPE_STORED FloatField/TYPE_NOT_STORED))
                  "double" (DoubleField. n (as-double value)  (if store? DoubleField/TYPE_STORED DoubleField/TYPE_NOT_STORED))
                  (let [analyzed? (not (false? (:analyzed meta-map)))
                          norms? (not (false? (:norms meta-map)))
                          field-type (doto (FieldType.) 
                       (.setStored store?) (.setIndexed index?) 
                       (.setTokenized analyzed?) (.setOmitNorms (not norms?)))] 
                    (Field. n (as-str value) field-type))
                  )
          ]
      (.add ^Document document field))))

(defn- map-stored
  "Returns a hash-map containing all of the values in the map that
  will be stored in the search index."
  [map-in]
  (merge {}
         (filter (complement nil?)
                 (map (fn [item]
                        (if (or (= nil (meta map-in))
                                (not= false
                                      (:stored ((first item) (meta map-in)))))
                          item)) map-in))))

(defn- concat-values
  "Concatenate all the maps values being stored into a single string."
  [map-in]
  (apply str (interpose " " (vals (map-stored map-in)))))

(defn- map->document
  "Create a Document from a map."
  [map]
  (let [document (Document.)]
    (doseq [[key value] map]
      (if (coll? value) (doseq [v value] (add-field document key v (key *schema-hints*)))
        (add-field document key value (key *schema-hints*))))
    (if *content*
      (add-field document :_content (concat-values map)))
    document))

(defn add
  "Add hash-maps to the search index."
  [index & maps]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
    (with-open [writer (index-writer index)]
      (doseq [m maps]
        (.addDocument writer
                    (map->document m)))))))

(defn padd
  "Parallel add hash-maps to the search index."
  [index process-reporter  large-maps]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
      (with-open [writer (index-writer index)]
        (let [c (atom 0)]
         (dorun (pmap (fn [maps]
                            (doseq [m maps]
													      (.addDocument writer
													                    (map->document m))
                            (process-reporter (swap! c inc))))
                      (partition-all 100000 large-maps)
                   ) ))))))

(defn delete
  "Deletes hash-maps from the search index."
  [index & maps]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
      (with-open [writer (index-writer index)]
		    (doseq [m maps]
		      (let [query (BooleanQuery.)]
		        (doseq [[key value] m]
		          (.add query
		                (BooleanClause.
		                  (let [vt (-> key keyword *schema-hints* :type)  field (.toLowerCase (as-str key))]
		                    (case vt
		                      "int"  (NumericRangeQuery/newIntRange field (as-int value) (as-int value) true true)
		                      "float"  (NumericRangeQuery/newFloatRange field (as-float value) (as-float value) true true)
		                      "long"  (NumericRangeQuery/newLongRange field (as-long value) (as-long value) true true)
		                      "double"  (NumericRangeQuery/newDoubleRange field (as-double value) (as-double value) true true)
		                      (TermQuery. (Term. field (.toLowerCase (as-str value)))) ))
		                 BooleanClause$Occur/MUST)))
		        (.deleteDocuments writer query)))))))

(defn- document->map
  "Turn a Document object into a map."
  ([^Document document score]
     (document->map document score (constantly nil)))
  ([^Document document score highlighter]
     (let [m (apply merge-with  (fn [a b] (if (coll? a) (conj a b) [a b]))
                    (for [^Field f (.getFields document)]
                        {(keyword (.name f)) (or (.numericValue f) (.stringValue f))}))]
       (if-not *doc-with-meta?* m
                (with-meta
					         (dissoc m :_content)
					         (-> (into {}
					                   (for [^Field f (.getFields document)
					                         :let [field-type (.fieldType f)]]
					                     [(keyword (.name f)) {:indexed (.indexed field-type)
					                                           :stored (.stored field-type)
					                                           :tokenized (.tokenized field-type)}]))
					             (assoc :_fragments (highlighter m) ; so that we can highlight :_content
					                    :_score score)
					             (dissoc :_content))))
)))

(defn- make-highlighter
  "Create a highlighter function which will take a map and return highlighted
fragments."
  [^Query query ^IndexSearcher searcher config]
  (if config
    (let [indexReader (.getIndexReader searcher)
          scorer (QueryScorer. (.rewrite query indexReader))
          config (merge {:field :_content
                         :max-fragments 5
                         :separator "..."
                         :pre "<b>"
                         :post "</b>"}
                        config)
          {:keys [field max-fragments separator fragments-key pre post]} config
          highlighter (Highlighter. (SimpleHTMLFormatter. pre post) scorer)]
      (fn [m]
        (let [str (field m)
              token-stream (.tokenStream ^Analyzer *analyzer*
                                         (name field)
                                         (StringReader. str))]
          (.getBestFragments ^Highlighter highlighter
                             ^TokenStream token-stream
                             ^String str
                             (int max-fragments)
                             ^String separator))))
    (constantly nil)))

(defn make-parser [default-field]
  (proxy [QueryParser] [*version* (as-str default-field) *analyzer*]
    (newFieldQuery [ analyzer,  field,  queryText,  quoted]
      (let [{:keys [type analyzed]} (-> field keyword *schema-hints*)]
              (case type
			          "long" (let [v (-> queryText Long/parseLong)] (NumericRangeQuery/newLongRange field  v v true true))
			          "int" (let [v (-> queryText Integer/parseInt)] (NumericRangeQuery/newIntRange field v v true true))
			          "double" (let [v (-> queryText Double/parseDouble)] (NumericRangeQuery/newDoubleRange field  v v true true))
			          "float" (let [v (-> queryText Float/parseFloat)] (NumericRangeQuery/newFloatRange field  v v true true))
                (if (false? analyzed) (proxy-super newTermQuery (Term.  field queryText))
                  (proxy-super newFieldQuery analyzer field queryText quoted)))))
    
    (newRangeQuery [field from to start-include? end-include?]
      (if-let [num-type (-> field keyword *schema-hints* :type)]
        (case num-type
          "long" (let [start (-> from Long/parseLong), end (-> to Long/parseLong)] (NumericRangeQuery/newLongRange field  start end true true))
          "int" (let [start (-> from Integer/parseInt), end (-> to Integer/parseInt)] (NumericRangeQuery/newIntRange field  start end true true))
          "double" (let [start (-> from Double/parseDouble) end (-> to Double/parseDouble)] (NumericRangeQuery/newDoubleRange field  start end true true))
          "float" (let [start (-> from  Float/parseFloat) end (-> to Double/parseDouble)] (NumericRangeQuery/newFloatRange field  start end true true)))
        (proxy-super newRangeQuery field from to start-include? end-include?)))))

(defn make-sort 
  "make sort from sort-str such as \"name asc, age desc\""
  [sort-str]
  (if (nil? sort-str) nil
    (let [sort (Sort.)]
      (doseq [[field flag] (partition-all 2 (clojure.string/split sort-str #",\s*|\s+"))]
      (.setSort sort (SortField. field
                        (case (-> field (keyword) *schema-hints* :type)
                              "long" SortField$Type/LONG
                              "int" SortField$Type/INT
                              "double" SortField$Type/DOUBLE
                              "float" SortField$Type/FLOAT
                              "string" SortField$Type/STRING
                              (case field "$doc" SortField$Type/DOC "$score"  SortField$Type/SCORE SortField$Type/STRING))
                        (= "desc" flag))))
      sort)))

(defn search
  "Search the supplied index with a query string."
  [index query max-results
   & {:keys [highlight default-field default-operator page results-per-page sort-by]
      :or {page 0 results-per-page max-results}}]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
            (if (every? false? [default-field *content*])
					    (throw (Exception. "No default search field specified"))
					    (with-open [reader (index-reader index)]
					      (let [default-field (or default-field :_content)
					            searcher (IndexSearcher. reader)
					            parser (doto (make-parser default-field)
					                     (.setDefaultOperator (case (or default-operator :or)
					                                            :and QueryParser/AND_OPERATOR
					                                            :or  QueryParser/OR_OPERATOR)))
					            sort (make-sort sort-by)
					            query (if (= "*:*" query) (MatchAllDocsQuery.) (.parse parser query))
					            hits (if (nil? sort) (.search searcher query (int max-results))  (.search searcher query (int max-results) sort))
					            highlighter (make-highlighter query searcher highlight)
					            start (* page results-per-page)
					            end (min (+ start results-per-page) (.totalHits hits))]
					        (doall
					         (with-meta (for [hit (map (partial aget (.scoreDocs hits))
					                                   (range start end))]
					                      (document->map (.doc ^IndexSearcher searcher
					                                           (.doc ^ScoreDoc hit))
					                                     (.score ^ScoreDoc hit)
					
					                                     highlighter))
					           {:_total-hits (.totalHits hits)
					            :_max-score (.getMaxScore hits)}))))))))



(defn search-and-delete
  "Search the supplied index with a query string and then delete all
of the results."
  ([index query]
     (if *content*
       (search-and-delete index query :_content)
       ;(throw (Exception. "No default search field specified"))
       (search-and-delete index query nil)
       ))
  ([index query default-field]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
           (with-open [writer (index-writer index)]
				       (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
				             query  (.parse parser query)]
				         (.deleteDocuments writer query)))))))

