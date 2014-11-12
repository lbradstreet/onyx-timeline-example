(ns onyx-timeline-example.functions-test
  (:require [clojure.test :refer :all]
            [onyx-timeline-example.onyx.functions :as onyx]))

(deftest test-sentence-to-words
  (is (= ["Hi" "my" "name" "is" "Mike"]
         (onyx/into-words "Hi my name is Mike"))))

(deftest test-filtering-tweets
  (is (= {:tweet "Lucas"} (onyx/filter-by-regex #"(?i).*Luc.*" {:tweet "Lucas"})))
  (is (= [] (onyx/filter-by-regex #"(?i).*Luc.*" {:tweet "Mike"}))))

(deftest test-word-count
  (let [state (atom {:counts {} :tokens []})]
    (onyx/word-count state 500 false {:word "Hi"})
    (is (= {:counts {"Hi" 1} :tokens ["Hi"]}  @state)))
  
  (let [state (atom {:counts {"Hi" 1} :tokens ["Hi"]})]
    (onyx/word-count state 500 false {:word "Hi"})
    (is (= {:counts {"Hi" 2} :tokens ["Hi" "Hi"]}  @state)))

  (let [state (atom {:counts {"Hi" 1} :tokens ["Hi"]})]
    (onyx/word-count state 1 false {:word "Bye"})
    (is (= {:counts {"Bye" 1} :tokens ["Bye"]}  @state))))

(run-tests)
