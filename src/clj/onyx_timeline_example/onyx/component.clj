(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :as a :refer [pipe chan >!! <!! close!]]
            [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.extensions :as extensions]
            [onyx.api]
            [onyx.plugin.core-async]
            [lib-onyx.interval]))

(defn into-words [s]
  (clojure.string/split s #"\s"))

(defn extract-tweet [segment]
  {:tweet-id (str (:id segment))
   :twitter-user (:screen_name (:user segment))
   :tweet (:text segment)})

(defn filter-by-regex [regex {:keys [tweet] :as segment}]
  (if (re-find regex tweet) segment []))

(defn extract-hashtags [{:keys [tweet]}]
  (->> (into-words tweet)
       (map (partial re-matches #"#.*"))
       (filter identity)
       (map (fn [hashtag] {:hashtag hashtag}))))

(defn split-into-words [min-chars {:keys [tweet]}]
  (->> (into-words tweet)
       (filter (fn [word] (> (count word) min-chars)))
       (map (fn [word] {:word word}))))

(defn word-count [local-state exclude-hashtags? {:keys [word] :as segment}]
  (if (and exclude-hashtags? (.startsWith word "#"))
    []
    (do (swap! local-state (fn [state] (assoc state word (inc (get state word 0)))))
        [])))

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

(defn log-and-purge-words [{:keys [onyx.core/queue] :as event}]
  (let [result (swap! (:timeline/word-count-state event) top-words)
        compressed-state (fressian/write {:top-words result})]
    (let [session (extensions/create-tx-session queue)]
      (doseq [queue-name (:onyx.core/egress-queues event)]
        (let [producer (extensions/create-producer queue session queue-name)]
          (extensions/produce-message queue producer session compressed-state)
          (extensions/close-resource queue producer)))
      (extensions/commit-tx queue session)
      (extensions/close-resource queue session))))

(defn log-and-purge-hashtags [{:keys [onyx.core/queue] :as event}]
  (let [result (swap! (:timeline/hashtag-count-state event) top-words)
        compressed-state (fressian/write {:top-hashtags result})]
    (let [session (extensions/create-tx-session queue)]
      (doseq [queue-name (:onyx.core/egress-queues event)]
        (let [producer (extensions/create-producer queue session queue-name)]
          (extensions/produce-message queue producer session compressed-state)
          (extensions/close-resource queue producer)))
      (extensions/commit-tx queue session)
      (extensions/close-resource queue session))))

(def batch-size 50)

(def batch-timeout 300)

(def workflow
  [[:in :extract-tweet]
   [:extract-tweet :filter-by-regex]
   [:filter-by-regex :split-into-words]
   [:filter-by-regex :extract-hashtags]
   [:split-into-words :word-count]
   [:extract-hashtags :hashtag-count]
   [:filter-by-regex :out]
   [:word-count :out]
   [:hashtag-count :out]])

(def catalog
  [{:onyx/name :in
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
    :timeline/regex #".*" ;#"(?i).*(Halloween|Thanksgiving|Christmas).*"
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
    :timeline.words/min-chars 3
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :word-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.component/word-count
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/consumption :concurrent
    :timeline.words/exclude-hashtags? true
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.component/log-and-purge-words
    :lib-onyx.interval/ms 3000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :hashtag-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.component/hashtag-count
    :onyx/type :function
    :onyx/group-by-key :hashtag
    :onyx/consumption :concurrent
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.component/log-and-purge-hashtags
    :lib-onyx.interval/ms 5000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Writes segments to a core.async channel"}])

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

(defmethod l-ext/inject-lifecycle-resources :in
  [_ {:keys [onyx.core/peer-opts]}]
  ; Although we could just tap the input chan mult, we would have no way
  ; to send a :done sentinel to the job without stopping any other jobs 
  ; that depend on the timeline channel. Therefore we pipe the tapped
  ; timeline in, and only send the :done to the in chan.
  (let [timeline-tap (a/tap (:timeline/input-ch-mult peer-opts) (chan))
        in (chan)
        _ (pipe timeline-tap in)]
    {:core-async/in-chan in}))

(defmethod l-ext/inject-lifecycle-resources :out
  [_ {:keys [onyx.core/peer-opts]}]
  {:core-async/out-chan (:timeline/output-ch peer-opts)})

(defmethod l-ext/inject-lifecycle-resources :filter-by-regex
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.core/params [(:timeline/regex task-map)]})

(defmethod l-ext/inject-lifecycle-resources :split-into-words
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.core/params [(:timeline.words/min-chars task-map)]})

(defmethod l-ext/inject-lifecycle-resources :word-count
  [_ {:keys [onyx.core/queue onyx.core/task-map] :as event}]
  (let [local-state (atom {})]
    {:onyx.core/params [local-state (:timeline.words/exclude-hashtags? task-map)]
     :timeline/word-count-state local-state}))

(defmethod l-ext/inject-lifecycle-resources :hashtag-count
  [_ {:keys [onyx.core/queue] :as event}]
  (let [local-state (atom {})]
    {:onyx.core/params [local-state]
     :timeline/hashtag-count-state local-state}))

(defrecord OnyxJob [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Job")
    (let [job-id (onyx.api/submit-job
                  (:conn onyx-connection)
                  {:catalog catalog :workflow workflow})]
      (assoc component :job-id job-id)))
  (stop [component]
    (println "Stopping Onyx Job")
    ; Need to fix this to put :done on the in chan that is tapped to
    (>!! (:timeline/input-ch (:peer (:onyx conf))) :done)
    component))

(defn new-onyx-job [conf] (map->OnyxJob {:conf conf}))
