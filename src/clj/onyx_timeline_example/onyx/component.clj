(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]))

;;;;; Implementation functions ;;;;;
(defn split-by-spaces-impl [s]
  (clojure.string/split s #"\s+"))

(defn loud-impl [s]
  (str s "!"))

;;;;; Destructuring functions ;;;;;
(defn split-by-spaces [segment]
  (map (fn [word] {:word word}) (split-by-spaces-impl (:sentence segment))))

(defn loud [segment]
  {:word (loud-impl (:word segment))})

(def workflow
  [[:input :split-by-spaces]
   [:split-by-spaces :loud]
   [:loud :output]])

(def batch-size 1)

(def catalog
  [{:onyx/name :input
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :split-by-spaces
    :onyx/fn :onyx-timeline-example.onyx.component/split-by-spaces
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :loud
    :onyx/fn :onyx-timeline-example.onyx.component/loud
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(defrecord Channel [capacity]
  component/Lifecycle
  (start [component]
    (assoc component :ch (chan (sliding-buffer capacity))))
  (stop [component]
    (close! (:ch component))
    component))

(defn new-channel [capacity] (map->Channel {:capacity capacity}))

(defrecord Onyx [conf input-chans output-chan conn v-peers]
  component/Lifecycle
  (start [component] (log/info "Starting Onyx Component")
    (let [coord-conf (:coord conf)
          peer-conf (:peer conf)
          num-peers (:num-peers conf)]

      (let [conn (onyx.api/connect :memory coord-conf)
            v-peers (onyx.api/start-peers conn num-peers peer-conf)]
        ; FIXME: Move the submit job out of the component
        (onyx.api/submit-job conn {:catalog catalog :workflow workflow})
        (assoc component 
               :conn conn
               :v-peers v-peers))))
  (stop [component] 
    (log/info "Stop Onyx Component")
    (doseq [v-peer v-peers]
      ((:shutdown-fn v-peer)))
    (onyx.api/shutdown conn)))

(defn new-onyx-server [conf] (map->Onyx {:conf conf}))
