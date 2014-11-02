(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]
            [lib-onyx.interval]))

(defn into-words [s]
  (clojure.string/split s #"\s"))

(defn extract-tweet [segment]
  {:tweet-id (str (:id segment))
   :twitter-user (:screen_name (:user segment))
   :tweet (:text segment)})

(defn filter-by-regex [regex {:keys [tweet] :as segment}]
  (if (re-find regex tweet) segment []))

(defn extract-links [{:keys [tweet]}]
  (->> (into-words tweet)
       (map (partial re-matches #"http(s)?:\/\/.*"))
       (map first)
       (filter identity)
       (map (fn [link] {:link link}))))

(defn extract-hashtags [{:keys [tweet]}]
  (->> (into-words tweet)
       (map (partial re-matches #"#.*"))
       (filter identity)
       (map (fn [hashtag] {:hashtag hashtag}))))

(defn split-into-words [min-chars {:keys [tweet]}]
  (->> (into-words tweet)
       (filter (fn [word] (> (count word) min-chars)))
       (map (fn [word] {:word word}))))

(defn word-count [local-state {:keys [word] :as segment}]
  (swap! local-state (fn [state] (assoc state word (inc (get state word 0)))))
  [])

(defn hashtag-count [local-state {:keys [hashtag] :as segment}]
  (swap! local-state (fn [state] (assoc state hashtag (inc (get state hashtag 0)))))
  [])

(defn top-words [m]
  (->> m
       (into [])
       (sort-by second)
       (reverse)
       (take 8)
       (into {})))

(defn log-and-purge-words [event]
  (let [result (swap! (:timeline/word-count-state event) top-words)]
    (>!! (:timeline/output-ch event) {:top-words result})))

(defn log-and-purge-hashtags [event]
  (let [result (swap! (:timeline/hashtag-count-state event) top-words)]
;;    (>!! (:timeline/output-ch event) {:top-hashtags result})
    ))

(def batch-size 50)

(def batch-timeout 300)

(def workflow
  [[:input :extract-tweet]
   [:extract-tweet :filter-by-regex]
   [:filter-by-regex :extract-links]
   [:filter-by-regex :extract-hashtags]
   [:filter-by-regex :split-into-words]
   [:split-into-words :word-count]
   [:extract-hashtags :hashtag-count]
   [:filter-by-regex :output]])

(def catalog
  [{:onyx/name :input
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :extract-tweet
    :onyx/fn :onyx-timeline-example.onyx.component/extract-tweet
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :filter-by-regex
    :onyx/fn :onyx-timeline-example.onyx.component/filter-by-regex
    :onyx/type :function
    :onyx/consumption :concurrent
    :timeline/regex #"(?i).*Halloween.*"
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :extract-links
    :onyx/fn :onyx-timeline-example.onyx.component/extract-links
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :extract-hashtags
    :onyx/fn :onyx-timeline-example.onyx.component/extract-hashtags
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :split-into-words
    :onyx/fn :onyx-timeline-example.onyx.component/split-into-words
    :onyx/type :function
    :onyx/consumption :concurrent
    :timeline.word-count/min-chars 3
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :word-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.component/word-count
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/consumption :concurrent
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.component/log-and-purge-words
    :lib-onyx.interval/ms 3000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :hashtag-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.component/hashtag-count
    :onyx/type :function
    ;; FIX: Grouping by key doesn't seem to work here. Onyx appears to throw the segments out?
;;    :onyx/group-by-key :hashtag
    :onyx/consumption :concurrent
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.component/log-and-purge-hashtags
    :lib-onyx.interval/ms 5000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Writes segments to a core.async channel"}])

(defrecord Channel [conf]
  component/Lifecycle
  (start [component]
    (println "Starting Channel")
    (let [capacity (:capacity (:core-async conf))]
      (assoc component :ch (chan (sliding-buffer capacity)))))
  (stop [component]
    (println "Stopping Channel")
    (when (:ch component)
      (close! (:ch component)))
    component))

(defn new-channel [conf] (map->Channel {:conf conf}))

(defrecord OnyxConnection [conf]
  component/Lifecycle
  (start [component]
    (println "Starting Onyx Coordinator")
    (let [conn (onyx.api/connect
                (:coordinator-type (:onyx conf))
                (:coord (:onyx conf)))]
      (assoc component :conn conn)))
  (stop [component]
    (println "Stopping Onyx Coordinator")
    (let [{:keys [conn]} component]
      (when conn (onyx.api/shutdown conn))
      component)))

(defn new-onyx-connection [conf] (map->OnyxConnection {:conf conf}))

(defrecord OnyxPeers [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Peers")
    (let [v-peers (onyx.api/start-peers (:conn onyx-connection)
                                        (:num-peers (:onyx conf))
                                        (:peer (:onyx conf)))]
      (assoc component :v-peers v-peers)))
  (stop [component]
    (println "Stopping Onyx Peers")
    (when-let [v-peers (:v-peers component)]
      (doseq [{:keys [shutdown-fn]} v-peers]
        (shutdown-fn)))
    component))

(defn new-onyx-peers [conf] (map->OnyxPeers {:conf conf}))

(defrecord OnyxJob [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Job")

    (defmethod l-ext/inject-lifecycle-resources :input
      [_ _] {:core-async/in-chan (:ch (:input-stream component))})

    (defmethod l-ext/inject-lifecycle-resources :output
      [_ _] {:core-async/out-chan (:ch (:output-stream component))})

    (defmethod l-ext/inject-lifecycle-resources :filter-by-regex
      [_ {:keys [onyx.core/task-map]}]
      {:onyx.core/params [(:timeline/regex task-map)]})

    (defmethod l-ext/inject-lifecycle-resources :split-into-words
      [_ {:keys [onyx.core/task-map]}]
      {:onyx.core/params [(:timeline.word-count/min-chars task-map)]})

    (defmethod l-ext/inject-lifecycle-resources :word-count
      [_ {:keys [onyx.core/queue] :as event}]
      (let [local-state (atom {})]
        {:onyx.core/params [local-state]
         :timeline/word-count-state local-state
         :timeline/output-ch (:ch (:output-stream component))}))

    (defmethod l-ext/inject-lifecycle-resources :hashtag-count
      [_ {:keys [onyx.core/queue] :as event}]
      (let [local-state (atom {})]
        {:onyx.core/params [local-state]
         :timeline/hashtag-count-state local-state
         :timeline/output-ch (:ch (:output-stream component))}))

    (let [job-id (onyx.api/submit-job
                  (:conn onyx-connection)
                  {:catalog catalog :workflow workflow})]
      (assoc component :job-id job-id)))
  (stop [component]
    (println "Stopping Onyx Job")
    (>!! (:ch (:input-stream component)) :done)
    component))

(defn new-onyx-job [conf] (map->OnyxJob {:conf conf}))
