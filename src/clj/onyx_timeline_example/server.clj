(ns onyx-timeline-example.server
  (:require [clojure.core.async :refer [mult chan sliding-buffer]]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [onyx-timeline-example.communicator.component :as comm]
            [onyx-timeline-example.communicator.twitter :as twitter]
            [onyx-timeline-example.http.component :as http]
            [onyx-timeline-example.onyx.component :as onyx]
            [onyx-timeline-example.switchboard :as sw]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component])
  (:gen-class))

(def onyx-id (java.util.UUID/randomUUID))

(def capacity 1000)

; There should be a conf component!
; can probably get rid of communicator channels then.
; or maybe put the sente channel in here too.
(def input-ch (chan (sliding-buffer capacity)))
(def output-ch (chan (sliding-buffer capacity)))
(def onyx-command-ch (chan))

(def conf {:port 8888
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
                         :onyx/id onyx-id
                         :scheduler/jobs (atom {})
                         :scheduler/command-ch onyx-command-ch
                         :timeline/input-ch input-ch
                         :timeline/input-ch-mult (mult input-ch)
                         :timeline/output-ch output-ch}
                  :num-peers 20
                  :coordinator-type :memory}})

(defn get-system [conf]
  "Create system by wiring individual components so that component/start
  will bring up the individual components in the correct order."
  ;; Fix some of these keyword names.
  (component/system-map
   :twitter (twitter/new-tweet-stream conf)
   :onyx-connection (component/using (onyx/new-onyx-connection conf) [:twitter])
   :onyx-peers (component/using (onyx/new-onyx-peers conf) [:onyx-connection])
   :onyx-scheduler (component/using (onyx/new-onyx-scheduler conf) [:onyx-connection])
   :onyx-job (component/using (onyx/new-onyx-job conf) [:onyx-connection])
   :comm-channels (comm/new-sente-communicator-channels)
   :comm (component/using (comm/new-sente-communicator) {:channels :comm-channels :onyx-scheduler :onyx-scheduler})
   :http (component/using (http/new-http-server conf) [:comm])
   :switchboard (component/using (sw/new-switchboard conf) [:comm-channels])))

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
  (alter-var-root #'system component/start))
