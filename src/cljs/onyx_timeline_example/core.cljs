(ns onyx-timeline-example.core
  (:require-macros [cljs.core.match.macros :refer (match)]
                   [cljs.core.async.macros :refer [go-loop go alt!]])
  (:require [om.core :as om :include-macros true]
            [om-bootstrap.panel :as p]
            [om-bootstrap.random :as r]
            [om-bootstrap.button :as b]
            [om-bootstrap.input :as i]
            [om-bootstrap.table :refer  [table]]
            [om-bootstrap.grid :as g]
            [om-tools.dom :as d :include-macros true]
            [om-tools.core :refer-macros [defcomponent]]
            [taoensso.sente  :as sente  :refer (cb-success?)]
            [taoensso.sente.packers.transit :as sente-transit]
            [cljs.core.async :as async :refer [<! >! chan put! timeout]]))

(enable-console-print!)

(def timeline-chan (chan))
(def custom-filter-chan (chan))
(def words-agg-chan (chan))
(def hashtags-agg-chan (chan))
(def jobs-chan (chan))
(def alert-chan (chan))

(defonce app-state (atom {:jobs []
                          :top-word-counts {}
                          :top-hashtag-counts {}
                          :custom-filter-timeline {:tweets []}
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
         [(:or :onyx.job/started
               :onyx.job/list 
               :onyx.job/done) msg] (put! jobs-chan [msg-type contents])
         [:onyx.job/start-failed msg] (put! alert-chan msg)
         [:tweet/new tweet] (put! timeline-chan tweet)
         [:tweet/filtered-tweet tweet] (put! custom-filter-chan tweet)
         [:agg/top-word-count counts] (put! words-agg-chan counts)
         [:agg/top-hashtag-count counts] (put! hashtags-agg-chan counts)
         :else (println "Couldn't match payload")))

(defn- event-handler [{:keys [event]}]
  (match event
         [:chsk/state new-state] 
         (match [new-state]
                [{:first-open? true}] 
                (chsk-send! [:onyx.job/list])
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
                              (d/tr {:key (key word-count)}
                                    (d/td (val word-count))
                                    (d/td (key word-count))))))}
                  nil)))

(defcomponent jobs-list [data owner]
  (init-state  [_]
              {:receive-chan (:jobs-chan (om/get-shared owner :comms))})
  (will-mount [_]
              (go-loop [] 
                       (let [[msg-type contents] (<! (om/get-state owner :receive-chan))]
                         (case msg-type
                           :onyx.job/list (om/update! data contents)
                           :onyx.job/done (om/transact! data [] (fn [jobs] (vec (remove (partial = contents) jobs))))
                           :onyx.job/started (om/transact! data [] (fn [jobs] (conj jobs contents)))))
                       (recur)))
  (render-state [_ _]
                (p/panel
                  {:header "Running filter jobs"
                   :list-group 
                   (table {:striped? true :bordered? true :condensed? true :hover? true}
                          (d/thead
                            (d/tr
                              (d/th "UID")
                              (d/th "Regex")))
                          (d/tbody
                            (map (fn [[uid regex]]
                                   (d/tr {:key uid}
                                         (d/td uid)
                                         (d/td regex)))
                                 data)))}
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
                              (d/tr {:key (key word-count)}
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

(defcomponent alert-widget [alert-data owner]
  (init-state  [_]
              {:alert-msg nil
               :receive-chan (:alert-chan (om/get-shared owner :comms))})
  (will-mount [_]
              (go-loop [] 
                       (let [alert-msg (<! (om/get-state owner :receive-chan))] 
                         (om/set-state! owner :alert-msg alert-msg)
                         ; om-bootstrap dismiss-after seems to be broken
                         ; so we'll do it ourself
                         (.setTimeout js/window 
                                      #(om/set-state! owner :alert-msg nil)
                                      10000))
                       (recur)))
  (render-state [_ {:keys [alert-msg]}]
                (when alert-msg 
                  (r/alert {:bs-style "warning"}
                           (d/strong alert-msg) " Oh no!"))))

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
                                       (d/li {:key (:tweet-id tweet)
                                              :class "list-group-item"}
                                             (om/build tweet-widget tweet {}))))}
                  nil)))

(defcomponent new-job-toolbar [data owner]
  (render-state [_ {:keys [regex-str]}]
          (b/toolbar {} 
                     (let [regex-entered? (not-empty regex-str)]
                       (d/div {:class "form-horizontal"
                               :style {:padding-bottom 10}}
                              (i/input {:type "text" 
                                        :label "Custom Filter Regex"
                                        :placeholder "Enter regex (e.g. .*hi.*)"
                                        :ref "input"
                                        :feedback true
                                        :bs-style (if regex-entered? "success" "error")
                                        :on-change (fn [e] (om/set-state! owner :regex-str (.. e -target -value)))})
                              (b/button {:on-click (fn [e] 
                                                     (when regex-entered?
                                                       (set! (.-value (om/get-node owner "input")))
                                                       (om/set-state! owner :regex-str nil)
                                                       (chsk-send! [:onyx.job/start {:regex-str regex-str}] 
                                                                   8000 
                                                                   (fn [edn-reply]
                                                                     (if (sente/cb-success? edn-reply) 
                                                                       (println "Successful sente reply " edn-reply)
                                                                       (println "Error! " edn-reply))))))}
                                        "Send filter job")
                              (om/build alert-widget {} {}))))))

(defcomponent app [data owner]
  (render-state [_ _]
                 (g/grid {}
                        (g/row {}
                               (om/build new-job-toolbar {} {}))
                        (g/row {} 
                               (g/col {:xs 4 :md 4}
                                      (om/build top-word-counts (:top-word-counts data) {}))
                               (g/col {:xs 4 :md 4} 
                                      (om/build top-hashtag-counts (:top-hashtag-counts data) {}))
                               (g/col {:xs 4 :md 4} (om/build jobs-list (:jobs data) {})))
                        (g/row {}
                               (g/col {:xs 6 :md 8}
                                      (om/build timeline 
                                                (:custom-filter-timeline data) 
                                                {:opts {:timeline-ch (:custom-filter (om/get-shared owner :comms))}}))
                               (g/col {:xs 6 :md 8}
                                      (om/build timeline 
                                                (:timeline data) 
                                                {:opts {:timeline-ch (:timeline (om/get-shared owner :comms))}}))))))

(defn main []
  (om/root app 
           app-state 
           {:target (. js/document (getElementById "app"))
            :shared {:comms {:timeline timeline-chan
                             :custom-filter custom-filter-chan
                             :jobs-chan jobs-chan
                             :alert-chan alert-chan
                             :words-agg-chan words-agg-chan
                             :hashtags-agg-chan hashtags-agg-chan}}}))
