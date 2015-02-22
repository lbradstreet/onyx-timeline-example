(ns onyx-timeline-example.communicator.websockets
  (:gen-class)
  (:require
    [clojure.core.match :as match :refer (match)]
    [com.stuartsierra.component :as component]
    [clojure.pprint :as pp]
    [clojure.tools.logging :as log]
    [taoensso.sente :as sente]
    [clojure.core.async :as async :refer [chan <! <!! >! >!! put! timeout go-loop]]))

(defrecord WebState []
  component/Lifecycle
  (start [component]
    (println "Starting Communicator Channels Component")
    (assoc component 
           :timeline/sente-ch (chan)
           :top-words (atom {})
           :top-hashtags (atom {})))
  (stop [component]
    (println "Stopping Communicator Channels Component")
    (assoc component :timeline/sente-ch nil :top-words nil :top-hashtags nil)))

(defn new-web-state [] (map->WebState {}))

(defn user-id-fn [req]
  "generates unique ID for request"
  (let [uid (get-in req [:cookies "ring-session" :value])]
    (log/info "Connected:" (:remote-addr req) uid)
    uid))

(defn ev->session [ev-msg]
  (get-in ev-msg [:ring-req :cookies "ring-session" :value]))

(defn make-handler []
  (fn [{:keys [event ?reply-fn] :as ev-msg}]
    (match event
           [:chsk/uidport-close] (println "User " (ev->session ev-msg) " closed session.")
           :else (println "Received event " event))))

(defn send-loop [channel f]
  "run loop, call f with message on channel"
  (go-loop [] 
           (let [msg (<!! channel)] 
             (f msg)) 
           (recur)))

; If multiple grouping tasks are run, each task will send a segment
; with the top n words, with no intersections between the maps.
; We will thus merge into a top-words/hashtags atom, and then only send
; the top n words from the merged map to the client
(defn top->displayed-trend 
  [merged n]
  (into {} (take n (reverse (sort-by val merged)))))

(def num-shown 8)

(defn segment->msg [segment top-words top-hashtags]
  (match [segment]
         [:done] nil
         [{:top-hashtags t}] [:agg/top-hashtag-count (top->displayed-trend 
                                                       (swap! top-hashtags merge t) num-shown)]
         [{:top-words t}] [:agg/top-word-count (top->displayed-trend 
                                                 (swap! top-words merge t) num-shown)]
         [{:tweet-id id :twitter-user user}] [:tweet/new {:tweet-id id :twitter-user user}] 
         :else (println "Couldn't match segment")))

(defn send-stream 
  "deliver percolation matches to interested clients"
  [uids chsk-send! top-words top-hashtags]
  (fn [segment]
    (let [user (:sente/uid segment)] 
      (doseq [uid (cond (nil? user) (:any @uids) 
                        (= :any user) (:any @uids)      
                        :else (list user))]
        (when-let [msg (segment->msg segment top-words top-hashtags)]
          (chsk-send! uid msg))))))
