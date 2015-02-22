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

(defrecord OnyxPeers [config n]
  component/Lifecycle

  (start [component]
    (println "Starting Virtual Peers")
    (assoc component :peers (onyx.api/start-peers! n config)))

  (stop [component]
    (println "Stopping Virtual Peers")
    (doseq [peer (:peers component)]
      (onyx.api/shutdown-peer peer))
    (assoc component :peers)))

(defn new-onyx-peers [config n] (map->OnyxPeers {:config config :n n}))

(defrecord OnyxJob [peer-conf]
  component/Lifecycle
  (start [{:keys [onyx-env] :as component}]
    (println "Starting Onyx Job")
    (let [job-id (onyx.api/submit-job peer-conf
                                      {:catalog wf/catalog 
                                       :workflow wf/workflow
                                       :task-scheduler :onyx.task-scheduler/round-robin})]
      (assoc component :job-id job-id)))
  (stop [{:keys [onyx-env] :as component}]
    (println "Stopping Onyx Job")
    (>!! (:timeline/input-ch peer-conf) :done)
    component))

(defn new-onyx-job [peer-conf] (map->OnyxJob {:peer-conf peer-conf}))
