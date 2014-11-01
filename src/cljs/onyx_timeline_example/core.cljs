(ns onyx-timeline-example.core
  (:require-macros [cljs.core.match.macros :refer (match)]
                   [cljs.core.async.macros :refer [go-loop go alt!]])
  (:require [om.core :as om :include-macros true]
            [om-bootstrap.panel :as p]
            [om-bootstrap.table :refer  [table]]
            [om-bootstrap.grid :as g]
            [om-tools.dom :as d :include-macros true]
            [om-tools.core :refer-macros [defcomponent]]
            [taoensso.sente  :as sente  :refer (cb-success?)]
            [taoensso.sente.packers.transit :as sente-transit]
            [cljs.core.async :as async :refer [<! >! chan put! alts! timeout]]))

; Put channels in root shared rather than refer to def
(def timeline-chan (chan))
(def agg-chan (chan))

(defonce app-state (atom {:top-word-counts {}
                          :timeline {:latest-id 0
                                     :items []}}))

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
                                (match [msg-type msg]
                                       [:tweet/new tweet] (put! timeline-chan tweet)
                                       [:agg/top-word-count counts] (put! agg-chan counts)))
         :else (print "Unmatched event: %s" event)))

(defonce chsk-router (sente/start-chsk-router! ch-chsk event-handler))

(defn add-tweet [timeline tweet]
  (-> timeline
      (update-in [:items] (fn [tweets] 
                            (let [trunc-tweets (take 100 tweets)]
                              (cons {:id (:latest-id timeline)
                                     :tweet-id (:tweet-id tweet)
                                     :twitter-user (:twitter-user tweet)
                                     :tweet (:text tweet)} 
                                    trunc-tweets))))
      (update-in [:latest-id] inc)))

(defcomponent top-word-counts [data owner]
  (init-state  [_]
              {:receive-chan (:agg-chan (om/get-shared owner :comms))})
  (will-mount [_]
              (go-loop [] 
                       ; Use alt for now, may have some other channels here in the future
                       (alt!
                         (om/get-state owner :receive-chan)
                         ([msg] (om/update! data msg)))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Word counts"
                   :list-group 
                   (table {:striped? true :bordered? true :condensed? true :hover? true}
                          (d/thead
                            (d/tr
                              (d/th "Count")
                              (d/th "Word")))
                          (d/tbody
                            (for [word-count (reverse (sort-by val data))]
                              (d/tr {:key (key word-count)}
                                    (d/td (val word-count))
                                    (d/td (key word-count))))))}
                  nil)))

(defcomponent timeline [data owner]
  (init-state  [_]
              {:receive-chan (:timeline (om/get-shared owner :comms))})
  (will-mount [_]
              (go-loop [] 
                       ; Use alt for now, may have some other channels here in the future
                       (alt!
                         (om/get-state owner :receive-chan)
                         ([msg]
                            (om/transact! data #(add-tweet % msg))
                            (.load (.-widgets js/twttr))))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Timeline"
                   :list-group (d/ul {:class "list-group"}
                                     (for [item (:items data)]
                                       (d/li {:key (:id item)
                                              :class "list-group-item"
                                              :style {}}
                                             (:tweet item)
                                             (d/blockquote
                                              {:class "twitter-tweet"}
                                              (d/a {:href (str "https://twitter.com/"
                                                               (:twitter-user item)
                                                               "/status/"
                                                               (:tweet-id item))})))))}
                  nil)))

(defcomponent app [data owner]
  (render-state [_ _]
                (d/div
                  {:class "grids-examples"}
                  (g/grid {}
                          (g/row {:class "show-grid"}
                                 (g/col {:xs 12 :md 8}
                                        (om/build timeline (:timeline data) {}))
                                 (g/col {:xs 6 :md 4}
                                        (om/build top-word-counts (:top-word-counts data) {})))))))

(defn main []
  (om/root app 
           app-state 
           {:target (. js/document (getElementById "app"))
            :shared {:comms {:timeline timeline-chan
                             :agg-chan agg-chan}}}))
