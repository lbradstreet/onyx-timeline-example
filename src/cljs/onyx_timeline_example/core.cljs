(ns onyx-timeline-example.core
  (:require-macros [cljs.core.async.macros :refer [go-loop go alt!]])
  (:require [om.core :as om :include-macros true]
            [om-bootstrap.panel :as p]
            [om-bootstrap.random :as r]
            [om-bootstrap.button :as b]
            [om-bootstrap.input :as i]
            [om-bootstrap.table :refer  [table]]
            [om-bootstrap.grid :as g]
            [om-tools.dom :as d :include-macros true]
            [om-tools.core :refer-macros [defcomponent]]
            [cljs.core.match :refer-macros [match]]
            [taoensso.sente  :as sente  :refer (cb-success?)]
            [taoensso.sente.packers.transit :as sente-transit]
            [cljs.core.async :as async :refer [<! >! chan put! timeout]]))

(enable-console-print!)

(def timeline-chan (chan))
(def words-agg-chan (chan))
(def hashtags-agg-chan (chan))

(defonce app-state (atom {:top-word-counts {}
                          :top-hashtag-counts {}
                          :timeline {:tweets []}}))

(def packer
  "Defines our packing (serialization) format for client<->server comms."
  (sente-transit/get-flexi-packer :json))

(let [{:keys [chsk ch-recv send-fn state]} (sente/make-channel-socket! "/chsk" {:packer packer :type :auto})]
  (def chsk       chsk)
  (def ch-chsk    ch-recv) ; ChannelSocket's receive channel
  (def chsk-send! send-fn) ; ChannelSocket's send API fn
  (def chsk-state state))  ; Watchable, read-only atom

(defn handle-payload [[msg-type contents]]
  (match [msg-type contents]
         [:tweet/new tweet] (put! timeline-chan tweet)
         [:agg/top-word-count counts] (put! words-agg-chan counts)
         [:agg/top-hashtag-count counts] (put! hashtags-agg-chan counts)
         :else (println "Couldn't match payload")))

(defn- event-handler [{:keys [event]}]
  (match event
         [:chsk/state new-state] 
         (match [new-state]
                ; Insert sente state handlers here
                ; e.g. when websocket is open
                ;[{:first-open? true}] 
                ;(chsk-send! [:onyx.job/list])
                :else 
                (println "Unmatched state change: " new-state))
         [:chsk/recv payload] (handle-payload payload)
         :else (print "Unmatched event: %s" event)))

(defonce chsk-router (sente/start-chsk-router! ch-chsk event-handler))

(def max-timeline-length 100)

(defn add-tweet [timeline tweet]
  (update-in timeline [:tweets] (fn [tweets] 
                                  (let [trunc-tweets (take max-timeline-length tweets)]
                                    (cons {:tweet-id (:tweet-id tweet)
                                           :twitter-user (:twitter-user tweet)} 
                                          trunc-tweets)))))

(defcomponent top-word-counts [data owner]
  (init-state  [_]
              {:receive-chan (:words-agg-chan (om/get-shared owner :comms))})
  (will-mount [_]
              (go-loop [] 
                       (let [msg (<! (om/get-state owner :receive-chan))]
                         (om/update! data msg))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Trending Words"
                   :list-group 
                   (table {:striped? true :bordered? true :condensed? true :hover? true}
                          (d/thead
                            (d/tr
                              (d/th "Count")
                              (d/th "Word")))
                          (d/tbody
                            (for [word-count (reverse (sort-by val data))]
                              (d/tr {:key (str (key word-count))}
                                    (d/td (val word-count))
                                    (d/td (key word-count))))))}
                  nil)))

(defcomponent top-hashtag-counts [data owner]
  (init-state  [_]
              {:receive-chan (:hashtags-agg-chan (om/get-shared owner :comms))})
  (will-mount [_]
              (go-loop [] 
                       (let [msg (<! (om/get-state owner :receive-chan))]
                         (om/update! data msg))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Trending Hashtags"
                   :list-group 
                   (table {:striped? true :bordered? true :condensed? true :hover? true}
                          (d/thead
                            (d/tr
                              (d/th "Count")
                              (d/th "Hashtag")))
                          (d/tbody
                            (for [word-count (reverse (sort-by val data))]
                              (d/tr {:key (str (key word-count))}
                                    (d/td (val word-count))
                                    (d/td (key word-count))))))}
                  nil)))

(defcomponent tweet-widget [tweet owner]
  (render [_]
          (d/div {:style {}}
                 (d/blockquote
                   {:ref "twitter-tweet-blockquote"
                    :class "twitter-tweet"}
                   (d/span {:class "small"} "Loading tweet...")
                   (d/a {:href (str "https://twitter.com/"
                                    (:twitter-user tweet)
                                    "/status/"
                                    (:tweet-id tweet))}))))
  (did-mount [_]
             (let [node (om/get-node owner)]
               (.load (.-widgets js/twttr) node))))

(defcomponent timeline [data owner opts]
  (init-state  [_]
              {:receive-chan (:timeline-ch opts)})
  (will-mount [_]
              (go-loop [] 
                       (let [msg (<! (om/get-state owner :receive-chan))]
                         (om/transact! data #(add-tweet % msg)))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Timeline"
                   :list-group (d/ul {:class "list-group"
                                      :style {:overflow-y "scroll"
                                              :height 800}}
                                     (for [tweet (:tweets data)]
                                       (d/li {:key (str (:tweet-id tweet))
                                              :class "list-group-item"}
                                             (om/build tweet-widget tweet {}))))}
                  nil)))

(defcomponent app [data owner]
  (render-state [_ _]
                 (g/grid {}
                        (g/row {} 
                               (g/col {:xs 6 :md 4}
                                      (om/build top-word-counts (:top-word-counts data) {}))
                               (g/col {:xs 6 :md 4} 
                                      (om/build top-hashtag-counts (:top-hashtag-counts data) {})))
                        (g/row {}
                               (om/build timeline 
                                         (:timeline data) 
                                         {:opts {:timeline-ch (:timeline (om/get-shared owner :comms))}})))))

(defn main []
  (om/root app 
           app-state 
           {:target (. js/document (getElementById "app"))
            :shared {:comms {:timeline timeline-chan
                             :words-agg-chan words-agg-chan
                             :hashtags-agg-chan hashtags-agg-chan}}}))
