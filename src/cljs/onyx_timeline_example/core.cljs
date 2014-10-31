(ns onyx-timeline-example.core
  (:require-macros [cljs.core.match.macros :refer (match)]
                   [cljs.core.async.macros :refer [go-loop go alt!]])
  (:require [om.core :as om :include-macros true]
            [om-bootstrap.panel :as p]
            [om-tools.dom :as d :include-macros true]
            [om-tools.core :refer-macros [defcomponent]]
            [taoensso.sente  :as sente  :refer (cb-success?)]
            [taoensso.sente.packers.transit :as sente-transit]
            [cljs.core.async :as async :refer [<! >! chan put! alts! timeout]]))

(def timeline-chan (chan 10000))

(defonce app-state (atom {:latest-id 0
                          :items []}))

(def packer
  "Defines our packing (serialization) format for client<->server comms."
  (sente-transit/get-flexi-packer :json))

(let [{:keys [chsk ch-recv send-fn state]} (sente/make-channel-socket! "/chsk" {:packer packer :type :auto})]
  (def chsk       chsk)
  (def ch-chsk    ch-recv) ; ChannelSocket's receive channel
  (def chsk-send! send-fn) ; ChannelSocket's send API fn
  (def chsk-state app-state))  ; Watchable, read-only atom

(defn- event-handler [{:keys [event]}]
  (match event
         [:chsk/state new-state] (print "Chsk state change:" new-state)
         [:chsk/recv payload] (let [[msg-type msg] payload]
                                (println "Payload " payload)
                                (match [msg-type msg]
                                       [:tweet/new tweet] (put! timeline-chan tweet)))
         :else (print "Unmatched event: %s" event)))

(defonce chsk-router (sente/start-chsk-router! ch-chsk event-handler))

(defn add-item [state item]
  (-> state
      (update-in [:items] (fn [items] 
                            (let [trunc-items (take 100 items)]
                              (cons {:id (:latest-id state)
                                     :text (:word item)} 
                                    trunc-items))))
      (update-in [:latest-id] inc)))

(defcomponent timeline [data owner]
  (will-mount [_]
              (go-loop [] 
                       ; Use alt for now, may have some other channels here in the future
                       (alt!
                         timeline-chan
                         ([msg]
                          (do (println (str "Received event on timeline channel: " msg))
                              (om/transact! data #(add-item % msg)))))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Results panel"
                   :list-group (d/ul {:class "list-group"}
                                     (for [item (:items data)]
                                       (d/li {:key (:id item)
                                              :class "list-group-item"
                                              :style {:color "blue"}}
                                             (:text item))))}
                  nil)))

(defn main []
  (om/root timeline 
           app-state 
           {:target (. js/document (getElementById "app"))}))
