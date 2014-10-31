(ns onyx-timeline-example.server
  (:gen-class)
  (:require [onyx-timeline-example.communicator.component :as comm]
            [onyx-timeline-example.communicator.twitter :as twitter]
            [onyx-timeline-example.http.component :as http]
            [onyx-timeline-example.onyx.component :as onyx]
            [onyx-timeline-example.switchboard :as sw]
            [clojure.tools.namespace.repl :refer  (refresh)]
            [environ.core :refer [env]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))

(def onyx-id 
  (java.util.UUID/randomUUID))

(def conf {:tw-check-interval-sec    10
           :tw-restart-wait          60
           :port                     8888
           :core-async {:capacity 1000}
           :onyx {:coord {:hornetq/mode :vm ;; Run HornetQ inside the VM for convenience
                          :hornetq/server? true
                          :hornetq.server/type :vm
                          :zookeeper/address "127.0.0.1:2185"
                          :zookeeper/server? true ;; Run ZK inside the VM for convenience
                          :zookeeper.server/port 2185
                          :onyx/id onyx-id
                          :onyx.coordinator/revoke-delay 5000}
                  :peer {:hornetq/mode :vm
                         :zookeeper/address "127.0.0.1:2185"
                         :onyx/id onyx-id}
                  :num-peers 8}})

(defn get-system [conf]
  "Create system by wiring individual components so that component/start
  will bring up the individual components in the correct order."
  ;; Fix some of these keyword names.
  (component/system-map
   :input-stream (onyx/new-channel (:capacity (:core-async conf)))
   :output-stream (onyx/new-channel (:capacity (:core-async conf)))
;;   :comm-channels (comm/new-communicator-channels)
;;   :onyx          (component/using (onyx/new-onyx-server (:onyx conf)) [:input-stream :output-stream])
;;   :comm          (component/using (comm/new-communicator)     {:channels :comm-channels})
;;   :http          (component/using (http/new-http-server conf) [:comm])
   :twitter       (component/using (twitter/new-tweet-stream) [:input-stream])
;;   :switchboard   (component/using (sw/new-switchboard)        [:comm-channels :onyx])
   ))

(def system nil)

(defn init []
  (alter-var-root #'system (constantly (get-system conf))))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system (fn [s] (when s (component/stop s)))))

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))

;(go)
;(stop)
;(reset)

(defn -main [& args]
  ;(log/info "Application started, PID" (pid/current))
  (alter-var-root #'system component/start))
