(ns onyx-timeline-example.communicator.websockets
  (:gen-class)
  (:require
    [clojure.core.match :as match :refer (match)]
    [clojure.pprint :as pp]
    [clojure.tools.logging :as log]
    [taoensso.sente :as sente]
    [clojure.core.async :as async :refer [<! <!! >! put! timeout go-loop]]))

(defn user-id-fn [req]
  "generates unique ID for request"
  (let [uid (str (java.util.UUID/randomUUID))]
    (log/info "Connected:" (:remote-addr req) uid)
    uid))

; (defn send-loop [channel f]
;   "run loop, call f with message on channel"
;   (go-loop [] (let [msg (<! channel)] 
;                 (f msg)) 
;            (recur)))

(defn send-loop [channel f]
  "run loop, call f with message on channel"
  (go-loop [] (let [msg (<!! channel)] 
                (f msg)) 
           (recur)))

(defn segment->msg [segment]
  (cond (contains? segment :tweet)
        [:tweet/new (:tweet segment)]       
        (contains? segment :top-words)
        [:agg/top-word-count (:top-words segment)]))

(defn send-stream [uids chsk-send!]
  "deliver percolation matches to interested clients"
  (fn [segment]
    (doseq [uid (:any @uids)]
      (when-let [msg (segment->msg segment)]
        (chsk-send! uid msg)))))
