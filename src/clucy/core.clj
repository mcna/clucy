(ns clucy.core
  (:import (java.io StringReader File)
           (java.security MessageDigest)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.document Document FieldType  BinaryDocValuesField FieldType$NumericType 
                                       Field IntField FloatField LongField DoubleField Field$Index Field$Store
                                       DocumentStoredFieldVisitor)
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.queryparser.classic QueryParserBase QueryParser)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur TopDocs
                                     BooleanQuery IndexSearcher Query ScoreDoc  Sort SortField SortField$Type
                                     Scorer TermQuery MatchAllDocsQuery NumericRangeQuery)
           (org.apache.lucene.search.highlight Highlighter QueryScorer
                                               SimpleHTMLFormatter)
           (org.apache.lucene.util Version AttributeSource BytesRef)
           (org.apache.lucene.store NIOFSDirectory RAMDirectory Directory)))

(def ^{:dynamic true} *version* Version/LUCENE_44)

(def ^{:dynamic true} *analyzer* (StandardAnalyzer. *version*))

(def ^:dynamic  *doc-with-meta?* true)

;; flag to indicate a default "_content" field should be maintained
(def ^{:dynamic true} *content* true)

(def ^:dynamic  *schema-hints* {:*content* true,  :*doc-with-meta?* true})

(def ^:dynamic *index-writer-cfg-fn* identity)

;; bindable vars for reusable index reader/searcher to utilize
;; lucene internal caching
(def ^:dynamic *index-reader* nil)

(def ^:dynamic *index-searcher* nil)

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
  (let [cfg (-> (IndexWriterConfig. *version* *analyzer*)
                *index-writer-cfg-fn*)]
    (IndexWriter. index
                  cfg)))

(defn index-reader
  "Create an IndexReader."
  ^org.apache.lucene.index.IndexReader
  [index]
  (DirectoryReader/open ^Directory index))

(def numeric-field-types {"int" {"truetrue" IntField/TYPE_STORED
                                 "truefalse" (doto (FieldType. IntField/TYPE_STORED) (.setIndexed false))
                                 "falsetrue" IntField/TYPE_NOT_STORED}
                          "long" {"truetrue" LongField/TYPE_STORED
                                  "truefalse" (doto (FieldType. LongField/TYPE_STORED) (.setIndexed false))
                                  "falsetrue" LongField/TYPE_NOT_STORED}
                          "float" {"truetrue" FloatField/TYPE_STORED
                                   "truefalse" (doto (FieldType. FloatField/TYPE_STORED) (.setIndexed false))
                                   "falsetrue" FloatField/TYPE_NOT_STORED}
                          "double" {"truetrue" DoubleField/TYPE_STORED
                                    "truefalse" (doto (FieldType. DoubleField/TYPE_STORED) (.setIndexed false))
                                    "falsetrue" DoubleField/TYPE_NOT_STORED}})

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
         ft (if-let [ftm (numeric-field-types vt)]
              (ftm (str store? index?)))
         field (case vt 
                 "int" (IntField. n (as-int value) ft)
                 "long" (LongField. n (as-long value) ft)
                 "float" (FloatField. n (as-float value) ft)
                 "double" (DoubleField. n (as-double value) ft)
                 (let [analyzed? (not (false? (:analyzed meta-map)))
                       norms? (not (false? (:norms meta-map)))
                       field-type (doto (FieldType.) 
                                    (.setStored store?) (.setIndexed index?) 
                                    (.setTokenized analyzed?) (.setOmitNorms (not norms?)))] 
                   (Field. n (as-str value) field-type)))]
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

(defn make-id-md5 
  "idv :  the vector of id fields, m : map"
  [idv m]
  (let [bytes (->> idv  (map m) (clojure.string/join "\n~\t!@") (#(.getBytes ^String % "utf-8")))
        md (doto ^ MessageDigest (MessageDigest/getInstance "MD5") (.reset) (.update  ^bytes bytes))]
    (.digest  ^MessageDigest  md)))

(defn ^String md5-to-hex [^bytes md5]
  (.toString (BigInteger. md5) 16))

(def  id-field-type (doto (FieldType.) (.setIndexed true) (.setStored false) (.setTokenized false) (.setOmitNorms true)))

(defn make-id-field 
  "if the _id  is composed from more than one field this function will return  a _id field with md5 values, or will return nil."
  [m]
  (if-let [idv (:_id *schema-hints*)]
    (if (> (count idv) 1)
      (Field. "_id"  ^String (md5-to-hex (make-id-md5 idv m)) ^FieldType  id-field-type))))

(defn make-id-term [m]
  (if-let [idv (:_id *schema-hints*)]
    (if (> (count idv) 1)
      (Term. "_id" (md5-to-hex (make-id-md5 idv m)))
      (Term. (name (first idv)) (-> idv first m str)))
    (throw (RuntimeException. "there's no _id defined in *schema-hints*"))))

(defn- map->document
  "Create a Document from a map."
  [map]
  (let [document (Document.)]
    (doseq [[key value] map]
      (if (coll? value) (doseq [v value] (add-field document key v (key *schema-hints*)))
          (add-field document key value (key *schema-hints*))))
    (if *content*
      (add-field document :_content (concat-values map)))
    (when-let [id-field (make-id-field map)]
      (.add ^Document document id-field))
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

(defn madd
  [index & map-chunks]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
      (with-open [writer (index-writer index)]
        (dorun 
         (pmap #(.addDocuments writer
                               (mapv map->document %))
               map-chunks))))))

(defn upsert 
  "Insert documents or update them if they exist"
  [index & maps ]
  (binding [*schema-hints* (or (meta index) *schema-hints*)] 
    (binding [*doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
              *content* (and *content* (-> :*content*  *schema-hints* false? not))]
      (with-open [writer (index-writer index)]
        (doseq [m maps]
          (.updateDocument writer (make-id-term m)
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
                       (partition-all 100000 large-maps))))))))

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
                         (TermQuery. (Term. field (.toLowerCase (as-str value))))))
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
     (if-not *doc-with-meta?*
       m
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
             (dissoc :_content)))))))

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

(defn ^QueryParser make-parser [default-field]
  (proxy [QueryParser] [*version* (as-str default-field) *analyzer*]
    (newFieldQuery [^Analyzer analyzer
                    ^String field
                    ^String queryText
                    quoted]
      (let [{:keys [type analyzed]} (-> field keyword *schema-hints*)]
        (case type
          "long" (let [v (-> queryText Long/parseLong)]
                   (NumericRangeQuery/newLongRange field v v true true))
          "int" (let [v (-> queryText Integer/parseInt)]
                  (NumericRangeQuery/newIntRange field v v true true))
          "double" (let [v (-> queryText Double/parseDouble)]
                     (NumericRangeQuery/newDoubleRange field  v v true true))
          "float" (let [v (-> queryText Float/parseFloat)]
                    (NumericRangeQuery/newFloatRange field  v v true true))
          (if (false? analyzed)
            (proxy-super newTermQuery (Term. field queryText))
            (let [^QueryParser this this]
              (proxy-super newFieldQuery analyzer field queryText quoted))))))
    
    (newRangeQuery [^String field ^String from ^String to   start-include?  end-include?]
      (if-let [num-type (-> field keyword *schema-hints* :type)]
        (case num-type
          "long" (let [start (-> from Long/parseLong)
                       end (-> to Long/parseLong)]
                   (NumericRangeQuery/newLongRange field  start end true true))
          "int" (let [start (-> from Integer/parseInt)
                      end (-> to Integer/parseInt)]
                  (NumericRangeQuery/newIntRange field  start end true true))
          "double" (let [start (-> from Double/parseDouble)
                         end (-> to Double/parseDouble)]
                     (NumericRangeQuery/newDoubleRange field  start end true true))
          "float" (let [start (-> from  Float/parseFloat)
                        end (-> to Float/parseFloat)]
                    (NumericRangeQuery/newFloatRange field  start end true true)))
        (let [^QueryParser this this]
          (proxy-super newRangeQuery field from to start-include? end-include?))))))

(defn ^Sort make-sort 
  "make sort from sort-str such as \"name asc, age desc\""
  [sort-str]
  (if (nil? sort-str) nil
      (let [sort (Sort.)]
        (doseq [[field flag] (partition-all 2 (clojure.string/split sort-str #",\s*|\s+"))]
          (.setSort sort (SortField. ^String field
                                     ^SortField$Type (case (-> field (keyword) *schema-hints* :type)
                                                       "long" SortField$Type/LONG
                                                       "int" SortField$Type/INT
                                                       "double" SortField$Type/DOUBLE
                                                       "float" SortField$Type/FLOAT
                                                       "string" SortField$Type/STRING
                                                       (case field
                                                         "$doc" SortField$Type/DOC
                                                         "$score" SortField$Type/SCORE
                                                         SortField$Type/STRING))
                                     (= "desc" flag))))
        sort)))

(defmacro with-searcher [index & forms]
  `(with-open [idx-rdr# (index-reader ~index)]
     (binding [*index-reader* idx-rdr#
               *index-searcher* (IndexSearcher. idx-rdr#)]
       (do ~@forms))))

(defn- search* [searcher query max-results highlight default-field default-operator page results-per-page sort-by fields doc-collector]
  (let [default-field (or default-field :_content)
        parser (doto (make-parser default-field)
                 (.setDefaultOperator (case (or default-operator :or)
                                        :and QueryParser/AND_OPERATOR
                                        :or  QueryParser/OR_OPERATOR)))
        sort (make-sort sort-by)
        ^Query query (if (= "*:*" query) (MatchAllDocsQuery.) (.parse parser query))
        ^TopDocs hits (if (nil? sort) (.search searcher query (int max-results))  (.search searcher query (int max-results) sort))
        highlighter (make-highlighter query searcher highlight)
        start (* page results-per-page)
        total (.totalHits hits)
        end (min (+ start results-per-page) total)
        field-set (and fields (set (map name fields)))]
    (if doc-collector 
      (doseq [i (range start end) :let [hit (aget (.scoreDocs hits) i)]]
        (doc-collector (document->map 
                        (.doc ^IndexSearcher searcher (.doc ^ScoreDoc hit)  ^java.util.Set field-set)
                        (.score ^ScoreDoc hit)
                        highlighter)
                       i total))
      (doall
       (with-meta (for [i (range start end) :let [hit (aget (.scoreDocs hits) i)]]
                    (document->map 
                     (.doc ^IndexSearcher searcher (.doc ^ScoreDoc hit)  ^java.util.Set  field-set)
                     (.score ^ScoreDoc hit)
                     highlighter))
         {:_total-hits (.totalHits hits)
          :_max-score (.getMaxScore hits)})))))

(defn search
  "Search the supplied index with a query string."
  [index query max-results
   & {:keys [highlight default-field default-operator page results-per-page sort-by fields doc-collector]
      :or {page 0 results-per-page max-results}}]
  (binding [*schema-hints* (or (meta index) *schema-hints*)
            *doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
            *content* (and *content* (-> :*content*  *schema-hints* false? not))]
    (if (every? false? [default-field *content*])
      (throw (Exception. "No default search field specified"))
      (if *index-searcher*
        (search* *index-searcher* query max-results highlight default-field default-operator page results-per-page sort-by fields doc-collector)
        (with-open [reader (index-reader index)]
          (search* (IndexSearcher. reader) query max-results highlight default-field default-operator page results-per-page sort-by fields doc-collector))))))

(defn search-and-delete-numeric [index field from to]
  (with-open [writer (index-writer index)]
    (let [query (NumericRangeQuery/newLongRange (as-str field) from to false false)]
      (.deleteDocuments writer query))))

(defn search-and-delete
  "Search the supplied index with a query string and then delete all
  of the results."
  ([index query]
   (search-and-delete index query (when *content* :_content)))
  ([index query default-field]
   (binding [*schema-hints* (or (meta index) *schema-hints*)
             *doc-with-meta?* (and *doc-with-meta?*  (-> :*doc-with-meta?*  *schema-hints* false? not))
             *content* (and *content* (-> :*content*  *schema-hints* false? not))]
     (with-open [writer (index-writer index)]
       (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
             query (.parse parser query)]
         (.deleteDocuments writer query))))))

