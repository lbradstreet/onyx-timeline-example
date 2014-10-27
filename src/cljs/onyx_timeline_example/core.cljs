(ns onyx-timeline-example.core
  (:require-macros [cljs.core.match.macros :refer (match)]
                   [cljs.core.async.macros :refer [go-loop go]])
  (:require [om.core :as om :include-macros true]
            [om-tools.dom :as dom :include-macros true]
            [om-tools.core :refer-macros [defcomponent]]
            [taoensso.sente  :as sente  :refer (cb-success?)]
            [taoensso.sente.packers.transit :as sente-transit]
            [cljs.core.async :as async :refer [<! >! chan put! alts! timeout]]
            
            ))

(def timeline-chan (chan))

(defonce app-state (atom {:text "Hello Chestnut!"}))

(def packer
  "Defines our packing (serialization) format for client<->server comms."
  (sente-transit/get-flexi-packer :json))

(let [{:keys [chsk ch-recv send-fn state]} (sente/make-channel-socket! "/chsk" {:packer packer :type :auto})]
  (def chsk       chsk)
  (def ch-chsk    ch-recv) ; ChannelSocket's receive channel
  (def chsk-send! send-fn) ; ChannelSocket's send API fn
  (def chsk-state app-state))  ; Watchable, read-only atom

(defn- event-handler [{:keys [event]}]
  (println "Event is " event)
  (match event
         [:chsk/state new-state] (print "Chsk state change:" new-state)
         [:chsk/recv payload] (let [[msg-type msg] payload]
                                (println "Payload " payload)
                                (match [msg-type msg]
                                       [:tweet/new tweet] (put! timeline-chan tweet)))
         :else (print "Unmatched event: %s" event)))

(defonce chsk-router (sente/start-chsk-router! ch-chsk event-handler))

(defn main []
  (om/root
    (fn [app owner]
      (reify
        om/IRender
        (render [_]
          (dom/h1 (:text app)))))
    app-state
    {:target (. js/document (getElementById "app"))}))
