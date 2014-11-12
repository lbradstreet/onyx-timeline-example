(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :as a :refer [go-loop pipe chan >!! <!! close!]]
            [clojure.data.fressian :as fressian]
            [clojure.core.match :as match :refer (match)]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.extensions :as extensions]
            [onyx.api]
            [onyx.plugin.core-async]
            [lib-onyx.interval]))

(def batch-size 50)

(def batch-timeout 300)

(def workflow
  [[:in :extract-tweet]
   [:extract-tweet :filter-by-regex]
   [:filter-by-regex :split-into-words]
   [:split-into-words :normalize-words]
   [:filter-by-regex :extract-hashtags]
   [:normalize-words :word-count]
   [:extract-hashtags :hashtag-count]
   [:filter-by-regex :out]
   [:word-count :out]
   [:hashtag-count :out]])


;;;;;;;;
;;; Catalog
;;;;;;;;

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
    :onyx/fn :onyx-timeline-example.onyx.functions/extract-tweet
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :filter-by-regex
    :onyx/fn :onyx-timeline-example.onyx.functions/filter-by-regex
    :onyx/type :function
    :onyx/consumption :concurrent
    :timeline/regex #"(?i).*(Halloween|Thanksgiving|Christmas).*"
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :extract-hashtags
    :onyx/fn :onyx-timeline-example.onyx.functions/extract-hashtags
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :split-into-words
    :onyx/fn :onyx-timeline-example.onyx.functions/split-into-words
    :onyx/type :function
    :onyx/consumption :concurrent
    :timeline.words/min-chars 3
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :normalize-words
    :onyx/fn :onyx-timeline-example.onyx.functions/normalize-words
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :word-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.functions/word-count
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/consumption :concurrent
    :timeline.words/exclude-hashtags? true
    :timeline.words/trend-period 10000
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.functions/log-and-purge-words
    :lib-onyx.interval/ms 3000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :hashtag-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.functions/hashtag-count
    :onyx/type :function
    :onyx/group-by-key :hashtag
    :onyx/consumption :concurrent
    :timeline.hashtags/trend-period 10000
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.functions/log-and-purge-hashtags
    :lib-onyx.interval/ms 5000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :wrap-sente-user-info
    :onyx/fn :onyx-timeline-example.onyx.functions/wrap-sente-user-info
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

;;;;;;;;
;;; Lifecycle Injections
;;;;;;;;

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
    {:onyx.core/params [local-state 
                        (:timeline.words/trend-period task-map)
                        (:timeline.words/exclude-hashtags? task-map)]
     :timeline/word-count-state local-state}))

(defmethod l-ext/inject-lifecycle-resources :hashtag-count
  [_ {:keys [onyx.core/queue onyx.core/task-map] :as event}]
  (let [local-state (atom {:tokens [] :counts {}})]
    {:onyx.core/params [local-state (:timeline.hashtags/trend-period task-map)]
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

;;;;;;;;
;;; Components
;;;;;;;;

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

(defn uid->public-uid [uid]
  (->> uid 
       str 
       (take 5) 
       (apply str)))

(defn jobs->list-message [jobs uid]
  {:sente/uid uid
   :onyx.job/list (mapv (juxt (comp uid->public-uid :uid)
                              (comp str :regex)) 
                        (vals jobs))})

(defn start-job! [onyx-connection peer-conf job-info catalog workflow]
  (let [jobs (:scheduler/jobs peer-conf)
        job-id (onyx.api/submit-job (:conn onyx-connection)
                                    {:catalog catalog :workflow workflow})
        ; the common timeline will be hogging all of the peers
        ; so we need to stand up new peers which will be allocated any job that
        ; is starved of peers, i.e. the new job
        onyx-peers (-> (new-onyx-peers peer-conf (:scheduler/num-peers-filter peer-conf))
                       (assoc :onyx-connection onyx-connection)
                       component/start)
        public-job-info (vector (uid->public-uid (:uid job-info)) 
                                (:regex job-info))]
    (swap! jobs assoc (:uid job-info) job-info)
    (>!! (:timeline/output-ch peer-conf) {:onyx.job/started public-job-info})
    (future (do @(onyx.api/await-job-completion (:conn onyx-connection) job-id)
                (>!! (:timeline/output-ch peer-conf) {:onyx.job/done public-job-info})
                (swap! jobs dissoc (:uid job-info))
                (component/stop onyx-peers)))))

(defn build-filter-catalog [regex uid]
  (-> (zipmap (map :onyx/name catalog) catalog) 
      (assoc-in [:in-take :sente/uid] uid)    
      (assoc-in [:filter-by-regex :timeline/regex] regex)
      (assoc-in [:wrap-sente-user-info :sente/uid] uid)
      vals))

; TODO: ensure you can only send a job when box is blue with checkmark.
; Add a jobs listing that shows the running jobs.
; Add a warning timeout thing when you run a job twice.
; Document.
; Ship it.

(defrecord OnyxScheduler [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Scheduler")
    (let [peer-conf (:peer (:onyx conf))
          cmd-ch (:scheduler/command-ch peer-conf)
          jobs (:scheduler/jobs peer-conf)]
      (go-loop []
               (let [msg (<!! cmd-ch)]
                 (match msg
                        [:scheduler/list-jobs [uid]]
                        (>!! (:timeline/output-ch peer-conf) (jobs->list-message @jobs uid))

                        [:scheduler/start-filter-job [regex uid]]
                        (cond (>= (count @jobs) (:scheduler/max-jobs peer-conf))
                              (>!! (:timeline/output-ch peer-conf) 
                                   {:onyx.job/start-failed "Failed to start job as too many jobs are running"
                                    :sente/uid uid})

                              (get @jobs uid)
                              (>!! (:timeline/output-ch peer-conf) 
                                   {:onyx.job/start-failed "Failed to start job as you are already running a job."
                                    :sente/uid uid})

                              :else
                              (let [timeline-tap (a/tap (:timeline/input-ch-mult peer-conf) (chan))
                                    task-input-ch (pipe-input-take timeline-tap (:user-filter/num-tweets peer-conf))]
                                (start-job! onyx-connection 
                                            peer-conf 
                                            {:input-ch task-input-ch :regex regex :uid uid}
                                            (build-filter-catalog regex uid) 
                                            client-workflow)))) 
                 (recur)))
      (assoc component :command-ch cmd-ch)))
  (stop [component]
    (println "Stopping Onyx Scheduler")
    (doseq [ch (map :input-ch (vals @(:scheduler/jobs (:peer (:onyx conf)))))]
      (>!! ch :done))
    component))

(defn new-onyx-scheduler [conf] (map->OnyxScheduler {:conf conf}))
