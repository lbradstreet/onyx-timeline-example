(ns onyx-timeline-example.functions-test
  (:require [clojure.test :refer :all]
            [onyx-timeline-example.onyx.component :as onyx]))

(deftest test-sentence-to-words
  (is (= ["Hi" "my" "name" "is" "Mike"]
         (onyx/into-words "Hi my name is Mike"))))

(deftest test-filtering-tweets
  (is (= {:tweet "Lucas"} (onyx/filter-by-regex #"(?i).*Luc.*" {:tweet "Lucas"})))
  (is (= [] (onyx/filter-by-regex #"(?i).*Luc.*" {:tweet "Mike"}))))

(deftest test-word-count
  (let [state (atom {})]
    (onyx/word-count state false {:word "Hi"})
    (is (= {"Hi" 1}  @state)))
  
  (let [state (atom {"Hi" 1})]
    (onyx/word-count state false {:word "Hi"})
    (is (= {"Hi" 2}  @state)))
  
  (let [state (atom {"Hi" 3})]
    (onyx/word-count state false {:word "Bye"})
    (is (= {"Hi" 3 "Bye" 1}  @state))))

(run-tests)