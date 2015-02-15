(ns onyx-timeline-example.onyx.workflow
  (:require [clojure.core.async :as a :refer [go-loop pipe chan >!! <!! close!]]
            [clojure.data.fressian :as fressian]
            [clojure.tools.logging :as log]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.extensions :as extensions]
            [onyx.api]
            [onyx-timeline-example.onyx.functions]
            [lib-onyx.interval]))

(def batch-size 50)

(def batch-timeout 300)



(def workflow
  [[:in :extract-tweet]
   [:extract-tweet :filter-by-regex]
   [:filter-by-regex :split-into-words]
   [:split-into-words :normalize-words]
   [:filter-by-regex :extract-hashtags]
   [:normalize-words :word-count]
   [:extract-hashtags :normalize-hashtags]
   [:normalize-hashtags :hashtag-count]
   [:filter-by-regex :out]
   [:word-count :out]
   [:hashtag-count :out]])

; (def workflow-alt
;   {:in #{:extract-tweet}
;    :extract-tweet #{:filter-by-regex}
;    :filter-by-regex #{:split-into-words :extract-hashtags :out}
;    :split-into-words #{:normalize-words}
;    :normalize-words #{:word-count}
;    :normalize-hashtags #{:hashtag-count}
;    :extract-hashtags #{:normalize-hashtags}
;    :word-count #{:out}
;    :hashtag-count #{:out}})

(def client-workflow
  [[:in-take :extract-tweet]
   [:extract-tweet :filter-by-regex]
   [:filter-by-regex :wrap-sente-user-info]
   [:wrap-sente-user-info :out]])

;;;;;;;;
;;; Catalog
;;;;;;;;

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size 
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Reads segments from a core.async channel"}
   
   {:onyx/name :in-take
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/max-peers 1
    :onyx/consumption :concurrent
    :onyx/medium :core.async
    :onyx/batch-size batch-size 
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Reads counted number of segments from a core.async channel"}

   {:onyx/name :extract-tweet
    :onyx/fn :onyx-timeline-example.onyx.functions/extract-tweet
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :filter-by-regex
    :onyx/fn :onyx-timeline-example.onyx.functions/filter-by-regex
    :onyx/type :function
    :onyx/consumption :concurrent
    :timeline/regex #"(?i).*(Halloween|Thanksgiving|Christmas|CNY|Valentines).*"
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :extract-hashtags
    :onyx/fn :onyx-timeline-example.onyx.functions/extract-hashtags
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :split-into-words
    :onyx/fn :onyx-timeline-example.onyx.functions/split-into-words
    :onyx/type :function
    :onyx/consumption :concurrent
    :timeline.words/min-chars 3
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :normalize-words
    :onyx/fn :onyx-timeline-example.onyx.functions/normalize-words
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :normalize-hashtags
    :onyx/fn :onyx-timeline-example.onyx.functions/normalize-hashtags
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :word-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.functions/word-count
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/consumption :concurrent
    :timeline.words/exclude-hashtags? true
    :timeline.words/trend-period 10000
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.functions/log-and-purge-words
    :lib-onyx.interval/ms 3000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :hashtag-count
    :onyx/ident :lib-onyx.interval/recurring-action
    :onyx/fn :onyx-timeline-example.onyx.functions/hashtag-count
    :onyx/type :function
    :onyx/group-by-key :hashtag
    :onyx/consumption :concurrent
    :timeline.hashtags/trend-period 10000
    :lib-onyx.interval/fn :onyx-timeline-example.onyx.functions/log-and-purge-hashtags
    :lib-onyx.interval/ms 5000
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :wrap-sente-user-info
    :onyx/fn :onyx-timeline-example.onyx.functions/wrap-sente-user-info
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :sente/client :any
    :onyx/batch-timeout batch-timeout}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/consumption :concurrent
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Writes segments to a core.async channel"}])

;;;;;;;;
;;; Lifecycle Injections
;;;;;;;;

(defmethod l-ext/inject-lifecycle-resources :in
  [_ {:keys [onyx.core/peer-opts]}]
  ; Although we could just tap the input chan mult, we would have no way
  ; to send a :done sentinel to the job without stopping any other jobs 
  ; that depend on the timeline channel. Therefore we pipe the tapped
  ; timeline in, and only send the :done to the in chan.
  (let [timeline-tap (a/tap (:timeline/input-ch-mult peer-opts) (chan))]
    {:core-async/in-chan (pipe timeline-tap (chan))}))

(defmethod l-ext/inject-lifecycle-resources :out
  [_ {:keys [onyx.core/peer-opts]}]
  {:core-async/out-chan (:timeline/output-ch peer-opts)})

(defmethod l-ext/inject-lifecycle-resources :filter-by-regex
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.core/params [(:timeline/regex task-map)]})

(defmethod l-ext/inject-lifecycle-resources :split-into-words
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.core/params [(:timeline.words/min-chars task-map)]})

(defmethod l-ext/inject-lifecycle-resources :word-count
  [_ {:keys [onyx.core/queue onyx.core/task-map] :as event}]
  (let [local-state (atom {:tokens [] :counts {}})]
    {:onyx.core/params [local-state 
                        (:timeline.words/trend-period task-map)
                        (:timeline.words/exclude-hashtags? task-map)]
     :timeline/word-count-state local-state}))

(defmethod l-ext/inject-lifecycle-resources :hashtag-count
  [_ {:keys [onyx.core/queue onyx.core/task-map] :as event}]
  (let [local-state (atom {:tokens [] :counts {}})]
    {:onyx.core/params [local-state (:timeline.hashtags/trend-period task-map)]
     :timeline/hashtag-count-state local-state}))

(defmethod l-ext/inject-lifecycle-resources :wrap-sente-user-info
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.core/params [(:sente/uid task-map)]})

(defmethod l-ext/inject-lifecycle-resources :in-take
  [_ {:keys [onyx.core/peer-opts onyx.core/task-map]}]
  ; to send a :done sentinel to the job without stopping any other jobs 
  ; that depend on the timeline channel. Therefore we pipe the tapped
  ; timeline in, and only send the :done to the in chan.
  (let [in (-> peer-opts 
               :scheduler/jobs 
               deref
               (get-in [(:sente/uid task-map) :input-ch]))]
    {:core-async/in-chan in}))
