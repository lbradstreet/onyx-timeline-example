(ns onyx-timeline-example.communicator.websockets
  (:gen-class)
  (:require
    [clojure.core.match :as match :refer (match)]
    [clojure.pprint :as pp]
    [clojure.tools.logging :as log]
    [taoensso.sente :as sente]
    [clojure.core.async :as async :refer [<! <!! >! >!! put! timeout go-loop]]))

(defn user-id-fn [req]
  "generates unique ID for request"
  (let [uid (get-in req [:cookies "ring-session" :value])]
    (log/info "Connected:" (:remote-addr req) uid)
    uid))

(defn start-job-handler [reply-fn command-ch params regexp uid]
  (>!! command-ch [:start-filter-job [regexp uid]])
  (reply-fn :success))

(defn ev->cookie [ev-msg]
  (get-in ev-msg [:ring-req :cookies "ring-session" :value]))

(defn make-handler [command-ch]
  (fn [{:keys [event ?reply-fn] :as ev-msg}]
    (match event
           [:job/start params] (start-job-handler ?reply-fn 
                                                  command-ch 
                                                  params 
                                                  (re-pattern (:regex-str params))
                                                  (ev->cookie ev-msg))
           [:chsk/uidport-close] (println "Client " (ev->cookie ev-msg) 
                                          " closed page. Can flush job.")
           :else (println "Got event " event))))

(defn send-loop [channel f]
  "run loop, call f with message on channel"
  (go-loop [] 
           (let [msg (<!! channel)] 
             (f msg)) 
           (recur)))

(defn segment->msg [segment]
  (cond (= segment :done) [:onyx/job :done] 
        (and (:tweet segment) (:sente/user segment)) [:tweet/new-user-filter segment] 
        (contains? segment :tweet) [:tweet/new segment] 
        (contains? segment :top-words) [:agg/top-word-count (:top-words segment)] 
        (contains? segment :top-hashtags) [:agg/top-hashtag-count (:top-hashtags segment)]))

(defn send-stream [uids chsk-send!]
  "deliver percolation matches to interested clients"
  (fn [segment]
    ;(println "Seg " segment)
    (let [user (:sente/user segment)] 
      (doseq [uid (if (or (nil? user) (= :any user)) 
                  (:any @uids)     
                  (list user))]
      (when-let [msg (segment->msg segment)]
        ;(println "sending " uid " " msg)
        (chsk-send! uid msg))))))
