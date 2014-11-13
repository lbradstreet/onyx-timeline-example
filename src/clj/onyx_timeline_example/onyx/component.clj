(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :as a :refer [go-loop pipe chan >!! <!! close!]]
            [clojure.core.match :as match :refer (match)]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.extensions :as extensions]
            [onyx.api]
            [onyx.plugin.core-async]
            [onyx-timeline-example.onyx.functions]
            [onyx-timeline-example.onyx.workflow :as wf]
            [lib-onyx.interval]))

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
                                      {:catalog wf/catalog :workflow wf/workflow})]
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

(defn build-filter-catalog [base-catalog regex uid]
  (-> (zipmap (map :onyx/name base-catalog) base-catalog) 
      (assoc-in [:in-take :sente/uid] uid)    
      (assoc-in [:filter-by-regex :timeline/regex] regex)
      (assoc-in [:wrap-sente-user-info :sente/uid] uid)
      vals))

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
                                            (build-filter-catalog wf/catalog regex uid) 
                                            wf/client-workflow)))) 
                 (recur)))
      (assoc component :command-ch cmd-ch)))
  (stop [component]
    (println "Stopping Onyx Scheduler")
    (doseq [ch (map :input-ch (vals @(:scheduler/jobs (:peer (:onyx conf)))))]
      (>!! ch :done))
    component))

(defn new-onyx-scheduler [conf] (map->OnyxScheduler {:conf conf}))
