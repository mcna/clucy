(ns clucy.test.core
  (:use clucy.core
        clojure.test
        [clojure.set :only [intersection]]))

(def people-schema {:name {} :age {:type "int" } :title {:type "string" :analyzed false}})

(def people [{:name "Miles" :age 36}
             {:name "Emily" :age 0.3}
             {:name "Joanna" :age 34}
             {:name "Melinda" :age 34}
             {:name "Mary" :age 48}
             {:name "Mary Lou" :age 39}
             {:name "Baby" :age -1}
             {:name "Jack" :age 21 :title "A.B"}])

(deftest core

  (testing "memory-index fn"
    (let [index (memory-index)]
      (is (not (nil? index)))))

  (testing "disk-index fn"
    (let [index (disk-index "/tmp/test-index")]
      (is (not (nil? index)))))

  (testing "add fn"
    (let [index (memory-index)]
      (doseq [person people] (add index person))
      (is (== 1 (count (search index "name:miles" 10))))))

  (testing "delete fn"
     (binding [*schema-hints* people-schema]
       (let [index (memory-index)]
      (doseq [person people] (add index person))
      (delete index (first people))
      (is (== 0 (count (search index "name:miles" 10)))))))

  (testing "search fn"
   (binding [*schema-hints* people-schema]
    (let [index (memory-index)]
      (doseq [person people] (add index person))
      (is (== 1 (count (search index "name:miles" 10))))
      (is (=  {:indexed true, :stored true, :tokenized true} (->  (search index "name:miles" 10) first meta :name)))
      (is (nil? (binding [*doc-with-meta?* false] (->  (search index "name:miles" 10) first meta ))))
      (is (== 1 (count (search index "name:miles age:100" 10))))
      (is (== 0 (count (search index "name:miles AND age:100" 10))))
      (is (== 0 (count (search index "name:miles age:100" 10 :default-operator :and))))
      (is (= [{:name "Miles" :age 36 }] (search index "name:miles" 10)))
      (is (= [ {:name "Mary" :age 48} {:name "Mary Lou" :age 39}] 
               (search index "age:[39 TO 48]" 10)) )
      (is (= {:name "Mary" :age 48} (first (search index "*:*" 10 :sort-by "age desc"))))
      (is (= {:name "Baby" :age -1} (first (search index "age:\\-1" 10)) ))
      (is (= {:name "Jack" :age 21 :title "A.B"} (first (search index "title:A.B" 10)))))))

  (testing "search-and-delete fn"
    (binding [*schema-hints* people-schema]
     (let [index (memory-index)]
      (doseq [person people] (add index person))
      (search-and-delete index "name:mary")
      (is (== 0 (count (search index "name:mary" 10)))))))

  (testing "search fn with highlighting"
    (let [index (memory-index)
          config {:field :name}]
      (doseq [person people] (add index person))
      (is (= (map #(-> % meta :_fragments)
                  (search index "name:mary" 10 :highlight config))
             ["<b>Mary</b>" "<b>Mary</b> Lou"]))))

  (testing "search fn returns scores in metadata"
    (let [index (memory-index)
          _ (doseq [person people] (add index person))
          results (search index "name:mary" 10)]
      (is (true? (every? pos? (map (comp :_score meta) results))))
      (is (= 2 (:_total-hits (meta results))))
      (is (pos? (:_max-score (meta results))))
      (is (= (count people) (:_total-hits (meta (search index "*:*" 2)))))))

  (testing "define schema as index meta and test it"
           (let [index (memory-index {:*content* false, :*doc-with-meta?* false,  :age {:type "int"}})
                   _ (doseq [person people] (add index person))
                 r  (search index "age:48" 10) ]
             (is (= 1 (:_total-hits (meta r) )))
             (is (= 1 (count r)))
             (is (= {:name "Mary" :age 48} (first r)))
             (is (nil? (meta (first r))))
             ))
  
  (testing "parallel index"
           (let [index (memory-index), total (atom 0) rc (atom 0),  reportor (fn [c] (swap! rc inc) (swap! total + c))]
             (padd index reportor (for [i (range 1234567)] {:id i }))
             (is (= 1234567 @rc))
             (is (= (/ (* 1234567 1234568) 2) @total))
             (is (= 1234567 (-> (search index "*:*" 1) (meta) :_total-hits)))))
  
  (testing "pagination"
    (let [index (memory-index)]
      (doseq [person people] (add index person))
      (is (== 3 (count (search index "m*" 10 :page 0 :results-per-page 3))))
      (is (== 1 (count (search index "m*" 10 :page 1 :results-per-page 3))))
      (is (empty? (intersection
                    (set (search index "m*" 10 :page 0 :results-per-page 3))
                    (set (search index "m*" 10 :page 1 :results-per-page 3))))))))
