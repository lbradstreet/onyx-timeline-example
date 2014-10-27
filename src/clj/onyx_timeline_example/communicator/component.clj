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

(def packer (sente-transit/get-flexi-packer :json)) ;; serialization format for client<->server comm

(defrecord Communicator [channels chsk-router]
  component/Lifecycle
  (start [component] (log/info "Starting Communicator Component")
    (println "Channels " channels)
         (let [{:keys [ch-recv send-fn ajax-post-fn ajax-get-or-ws-handshake-fn connected-uids]}
               (sente/make-channel-socket! {:packer packer :user-id-fn ws/user-id-fn})
               event-handler identity ; FIXME 
               chsk-router (sente/start-chsk-router! ch-recv event-handler)]
           (ws/send-loop (:timeline channels) (ws/send-stream connected-uids send-fn))
           (assoc component :ajax-post-fn ajax-post-fn
                            :ajax-get-or-ws-handshake-fn ajax-get-or-ws-handshake-fn
                            :chsk-router chsk-router)))
  (stop [component] (log/info "Stopping Communicator Component")
        (chsk-router) ;; stops router loop
        (assoc component :chsk-router nil)))

(defn new-communicator [] (map->Communicator {}))

(defrecord Communicator-Channels []
  component/Lifecycle
  (start [component] (log/info "Starting Communicator Channels Component")
    (assoc component
           :timeline (chan)))
  (stop [component] (log/info "Stop Communicator Channels Component")
        (assoc component :timeline nil)))

(defn new-communicator-channels [] (map->Communicator-Channels {}))

(defrecord Producer-Channels []
  component/Lifecycle
  (start [component] (log/info "Starting Producer Channels Component")
    (assoc component
           :output (chan)))
  (stop [component] (log/info "Stop Producer Channels Component")
    (assoc component :output nil)))

(defn new-producer-channels []
  (map->Producer-Channels {}))
