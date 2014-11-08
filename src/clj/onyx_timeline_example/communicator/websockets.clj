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
           [:chsk/uidport-close] (println "User " (ev->cookie ev-msg) " closed page. Can flush job?")
           :else (println "Got event " event))))

(defn send-loop [channel f]
  "run loop, call f with message on channel"
  (go-loop [] 
           (let [msg (<!! channel)] 
             (f msg)) 
           (recur)))

(def top-words (atom {}))
(def top-hashtags (atom {}))

(comment
  (let [x {:a 1 :b 1}]
    (match [x]
           [{:a _ :b 2}] :a0
           [{:a 1 :b 1}] :a1
           [{:c 3 :d _ :e 4}] :a2
           :else nil)))

; TODO use match
(defn segment->msg [segment]
  (cond (= segment :done) [:onyx.job/done] 
        (contains? segment :onyx.job/started) [:onyx.job/started (str (second (:onyx.job/started segment)))]
        (contains? segment :onyx.job/list) [:onyx.job/list (:onyx.job/list segment)]
        (and (:tweet segment) (:sente/uid segment)) [:tweet/new-user-filter segment] 
        (contains? segment :tweet) [:tweet/new segment] 
        ; TODO: only select top words before sending to client
        (contains? segment :top-words) [:agg/top-word-count (swap! top-words merge (:top-words segment) )] 
        (contains? segment :top-hashtags) [:agg/top-hashtag-count (swap! top-hashtags merge (:top-hashtags segment))]))

(defn send-stream [uids chsk-send!]
  "deliver percolation matches to interested clients"
  (fn [segment]
    (let [user (:sente/uid segment)] 
      (doseq [uid (cond (nil? user) (:any @uids) 
                        (= :any user) (:any @uids)      
                        :else (list user))]
      (when-let [msg (segment->msg segment)]
        (println "sending " uid " " msg)
        (chsk-send! uid msg))))))
