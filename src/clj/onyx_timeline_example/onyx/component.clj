(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :as a :refer [go-loop pipe chan >!! <!! close!]]
            [clojure.data.fressian :as fressian]
            [clojure.core.match :as match :refer (match)]
            [clojure.tools.logging :as log]
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


(defn update-trends [{:keys [tokens counts]} token period]
  (let [ts (conj tokens token)
        updated-count (inc (counts token 0))] 
    (if (> (count ts) period)
      (let [removed-token (first ts)
            removed-count (dec (counts removed-token))] 
        {:tokens (subvec ts 1) 
         :counts (assoc (if (zero? removed-count)
                          (dissoc counts removed-token)
                          (assoc counts removed-token removed-count))
                        token
                        updated-count)})
      {:tokens ts
       :counts (assoc counts token updated-count)})))

; move to conf
(def rolling-total-period 10000)

(defn word-count [local-state exclude-hashtags? {:keys [word] :as segment}]
  (if (and exclude-hashtags? (.startsWith word "#"))
    []
    (do (swap! local-state update-trends word rolling-total-period)
        [])))

(defn hashtag-count [local-state {:keys [hashtag] :as segment}]
  (swap! local-state update-trends hashtag rolling-total-period)
  [])

(defn top-words [m]
  (->> m
       (into [])
       (sort-by second)
       (reverse)
       (take 8)
       (into {})))

(defn log-and-purge-words [{:keys [onyx.core/queue] :as event}]
  (let [result (top-words (:counts @(:timeline/word-count-state event)))
        compressed-state (fressian/write {:top-words result})]
    (let [session (extensions/create-tx-session queue)]
      (doseq [queue-name (:onyx.core/egress-queues event)]
        (let [producer (extensions/create-producer queue session queue-name)]
          (extensions/produce-message queue producer session compressed-state)
          (extensions/close-resource queue producer)))
      (extensions/commit-tx queue session)
      (extensions/close-resource queue session))))

(defn log-and-purge-hashtags [{:keys [onyx.core/queue] :as event}]
  (let [result (top-words (:counts @(:timeline/hashtag-count-state event)))
        compressed-state (fressian/write {:top-hashtags result})
        session (extensions/create-tx-session queue)]
    (doseq [queue-name (:onyx.core/egress-queues event)]
      (let [producer (extensions/create-producer queue session queue-name)]
        (extensions/produce-message queue producer session compressed-state)
        (extensions/close-resource queue producer)))
      (extensions/commit-tx queue session)
      (extensions/close-resource queue session)))

(defn wrap-sente-user-info [user segment]
  (assoc segment :sente/uid user))

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

   {:onyx/name :in-take
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size 
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Reads counted number of segments from a core.async channel"}

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
    :timeline/regex #"(?i).*(Halloween|Thanksgiving|Christmas).*"
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

   {:onyx/name :wrap-sente-user-info
    :onyx/fn :onyx-timeline-example.onyx.component/wrap-sente-user-info
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :sente/client :any
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
    (let [conn (onyx.api/connect (:coordinator-type (:onyx conf))
                                 (:coord (:onyx conf)))]
      (assoc component :conn conn)))
  (stop [component]
    (println "Stopping Onyx Coordinator")
    (let [{:keys [conn]} component]
      (when conn (onyx.api/shutdown conn))
      component)))

(defn new-onyx-connection [conf] (map->OnyxConnection {:conf conf}))

(defrecord OnyxPeers [peer-conf n]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Peers")
    (let [v-peers (onyx.api/start-peers (:conn onyx-connection)
                                        n 
                                        peer-conf)]
      (assoc component :v-peers v-peers)))
  (stop [component]
    (println "Stopping Onyx Peers")
    (when-let [v-peers (:v-peers component)]
      (doseq [{:keys [shutdown-fn]} v-peers]
        (shutdown-fn)))
    component))

(defn new-onyx-peers [peer-conf n] (map->OnyxPeers {:peer-conf peer-conf :n n}))

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
  (let [local-state (atom {:tokens [] :counts {}})]
    {:onyx.core/params [local-state (:timeline.words/exclude-hashtags? task-map)]
     :timeline/word-count-state local-state}))

(defmethod l-ext/inject-lifecycle-resources :hashtag-count
  [_ {:keys [onyx.core/queue] :as event}]
  (let [local-state (atom {:tokens [] :counts {}})]
    {:onyx.core/params [local-state]
     :timeline/hashtag-count-state local-state}))

(defmethod l-ext/inject-lifecycle-resources :wrap-sente-user-info
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.core/params [(:sente/uid task-map)]})

(defmethod l-ext/inject-lifecycle-resources :in-take
  [_ {:keys [onyx.core/peer-opts onyx.core/task-map]}]
  ; Although we could just tap the input chan mult, we would have no way
  ; to send a :done sentinel to the job without stopping any other jobs 
  ; that depend on the timeline channel. Therefore we pipe the tapped
  ; timeline in, and only send the :done to the in chan.
  (let [in (-> peer-opts 
               :scheduler/jobs 
               deref
               (get-in [(:sente/uid task-map) :input-ch]))]
    {:core-async/in-chan in}))

(defrecord OnyxJob [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Job")
    (let [job-id (onyx.api/submit-job (:conn onyx-connection)
                                      {:catalog catalog :workflow workflow})]
      (assoc component :job-id job-id)))
  (stop [component]
    (println "Stopping Onyx Job")
    ; Need to fix this to put :done on the in chan that is tapped to
    (>!! (:timeline/input-ch (:peer (:onyx conf))) :done)
    component))

(defn new-onyx-job [conf] (map->OnyxJob {:conf conf}))

; May just be able to use a/take. Had some problems before.
(defn pipe-input-take [timeline-in cnt]
  ; Although we could just tap the input chan mult, we would have no way
  ; to send a :done sentinel to the job without stopping any other jobs 
  ; that depend on the timeline channel. Therefore we pipe the tapped
  ; timeline in, and only send the :done to the in chan.
  (let [in (chan)]
    (go-loop [n cnt]
             (if-not (zero? n)
               (do (>!! in (<!! timeline-in))
                   (recur (dec n)))
               (do (>!! in :done)
                   (close! timeline-in)
                   (close! in))))
    in))

(def client-workflow
  [[:in-take :extract-tweet]
   [:extract-tweet :filter-by-regex]
   [:filter-by-regex :wrap-sente-user-info]
   [:wrap-sente-user-info :out]])

(def num-new-peers 10)

(defn start-job! [onyx-connection peer-conf job-info catalog workflow]
  (let [jobs (:scheduler/jobs peer-conf)
        job-id (onyx.api/submit-job (:conn onyx-connection)
                                    {:catalog catalog :workflow workflow})
        ; the common timeline will be hogging all of the peers
        ; so we need to stand up new peers which will be allocated to the new job
        onyx-peers (-> (new-onyx-peers peer-conf num-new-peers)
                       (assoc :onyx-connection onyx-connection)
                       component/start)]
    (swap! jobs assoc (:uid job-info) job-info)
    (>!! (:timeline/output-ch peer-conf) {:onyx.job/started (:regex job-info)})
    (future (do @(onyx.api/await-job-completion (:conn onyx-connection) (str job-id))
                (println "Job done.")
                (swap! jobs dissoc (:uid job-info))
                (component/stop onyx-peers)))))

(defrecord OnyxScheduler [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Scheduler")
    (let [peer-conf (:peer (:onyx conf))
          cmd-ch (:scheduler/command-ch peer-conf)]
      (go-loop []
               (let [msg (<!! cmd-ch)]
                 (match msg
                        [:list-jobs]
                        (>!! (:timeline/output-ch peer-conf) {:onyx.job/list (map :regex (vals @(:scheduler/jobs peer-conf)))})
                        [:start-filter-job [regex uid]]
                        (let [timeline-tap (a/tap (:timeline/input-ch-mult peer-conf) (chan))
                              task-input-ch (pipe-input-take timeline-tap 1000)
                              ; Add a comment here about how an atom wouldn't be necessary 
                              ; if we use a queue where we pass in serializable parameters
                              ; via the task-opts.
                              cat (-> (zipmap (map :onyx/name catalog) catalog) 
                                      (assoc-in [:in-take :sente/uid] uid)    
                                      (assoc-in [:filter-by-regex :timeline/regex] regex)
                                      (assoc-in [:wrap-sente-user-info :sente/uid] uid)
                                      vals)]
                          (start-job! onyx-connection 
                                      peer-conf 
                                      {:input-ch task-input-ch :regex regex :uid uid}
                                      cat 
                                      client-workflow))) 
                 (recur)))
      (assoc component :command-ch cmd-ch)))
  (stop [component]
    (println "Stopping Onyx Scheduler")
    (doseq [ch (map :input-ch (vals @(:scheduler/jobs (:peer (:onyx conf)))))]
      (>!! ch :done))
    component))

(defn new-onyx-scheduler [conf] (map->OnyxScheduler {:conf conf}))
