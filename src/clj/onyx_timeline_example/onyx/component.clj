(ns onyx-timeline-example.onyx.component
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]))

(defn extract-tweet [segment]
  {:tweet (:text segment)})

(defn split-by-spaces [segment]
  (map (fn [word] {:word word})
       (clojure.string/split (:tweet segment) #"\s+")))

(defn loud [segment]
  {:word (str (:word segment) "!")})

(def batch-size 25)

(def batch-timeout 3000)

(def workflow
  [[:input :extract-tweet]
   [:extract-tweet :split-by-spaces]
   [:split-by-spaces :loud]
   [:loud :output]])

(def catalog
  [{:onyx/name :input
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :extract-tweet
    :onyx/fn :onyx-timeline-example.onyx.component/extract-tweet
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :split-by-spaces
    :onyx/fn :onyx-timeline-example.onyx.component/split-by-spaces
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :loud
    :onyx/fn :onyx-timeline-example.onyx.component/loud
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :sequential
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Writes segments to a core.async channel"}])

(defrecord Channel [conf]
  component/Lifecycle
  (start [component]
    (println "Starting Channel")
    (let [capacity (:capacity (:core-async conf))]
      (assoc component :ch (chan (sliding-buffer capacity)))))
  (stop [component]
    (println "Stopping Channel")
    (when (:ch component)
      (close! (:ch component)))
    component))

(defn new-channel [conf] (map->Channel {:conf conf}))

(defrecord OnyxConnection [conf]
  component/Lifecycle
  (start [component]
    (println "Starting Onyx Coordinator")
    (let [conn (onyx.api/connect
                (:coordinator-type (:onyx conf))
                (:coord (:onyx conf)))]
      (assoc component :conn conn)))
  (stop [component]
    (println "Stopping Onyx Coordinator")
    (let [{:keys [conn]} component]
      (when conn (onyx.api/shutdown conn))
      component)))

(defn new-onyx-connection [conf] (map->OnyxConnection {:conf conf}))

(defrecord OnyxPeers [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Peers")
    (let [v-peers (onyx.api/start-peers (:conn onyx-connection)
                                        (:num-peers (:onyx conf))
                                        (:peer (:onyx conf)))]
      (assoc component :v-peers v-peers)))
  (stop [component]
    (println "Stopping Onyx Peers")
    (when-let [v-peers (:v-peers component)]
      (doseq [{:keys [shutdown-fn]} v-peers]
        (shutdown-fn)))
    component))

(defn new-onyx-peers [conf] (map->OnyxPeers {:conf conf}))

(defrecord OnyxJob [conf]
  component/Lifecycle
  (start [{:keys [onyx-connection] :as component}]
    (println "Starting Onyx Job")

    (defmethod l-ext/inject-lifecycle-resources :input
      [_ _] {:core-async/in-chan (:ch (:input-stream component))})

    (defmethod l-ext/inject-lifecycle-resources :output
      [_ _] {:core-async/out-chan (:ch (:output-stream component))})
    
    (let [job-id (onyx.api/submit-job
                  (:conn onyx-connection)
                  {:catalog catalog :workflow workflow})]
      (assoc component :job-id job-id)))
  (stop [component]
    (println "Stopping Onyx Job")
    (>!! (:ch (:input-stream component)) :done)
    component))

(defn new-onyx-job [conf] (map->OnyxJob {:conf conf}))
