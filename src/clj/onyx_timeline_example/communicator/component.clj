(ns onyx-timeline-example.communicator.component
  (:gen-class)
  (:require
    [clojure.pprint :as pp]
    [clojure.tools.logging :as log]
    [onyx-timeline-example.communicator.websockets :as ws]
    [taoensso.sente :as sente]
    [taoensso.sente.packers.transit :as sente-transit]
    [com.stuartsierra.component :as component]
    [clojure.core.async :as async :refer [chan]]))

;; serialization format for client<->server comm
(def packer (sente-transit/get-flexi-packer :json))

(defrecord SenteCommunicator [web chsk-router]
  component/Lifecycle
  (start [component]
    (println "Starting Sente Communicator Component")
    (let [{:keys [ch-recv send-fn ajax-post-fn ajax-get-or-ws-handshake-fn connected-uids]}
          (sente/make-channel-socket! {:packer packer :user-id-fn ws/user-id-fn})
          event-handler (ws/make-handler)
          chsk-router (sente/start-chsk-router! ch-recv event-handler)]
      (ws/send-loop (:timeline/sente-ch web) (ws/send-stream connected-uids 
                                                             send-fn 
                                                             (:top-words web)
                                                             (:top-hashtags web)))
      (assoc component 
             :ajax-post-fn ajax-post-fn
             :ajax-get-or-ws-handshake-fn ajax-get-or-ws-handshake-fn
             :chsk-router chsk-router)))
  (stop [component]
    (println "Stopping Sente Communicator Component")
    (chsk-router) ;; stops router loop
    (assoc component :chsk-router nil)))

(defn new-sente-communicator [] (map->SenteCommunicator {}))
