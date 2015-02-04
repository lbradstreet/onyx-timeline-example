(ns onyx-timeline-example.server
  (:require [clojure.core.async :refer [mult chan sliding-buffer]]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [taoensso.timbre.appenders.rotor :as rotor]
            [onyx-timeline-example.communicator.websockets :as web]
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

(def input-ch (chan (sliding-buffer capacity)))
(def output-ch (chan (sliding-buffer capacity)))
(def onyx-command-ch (chan))

(def log-config {:appenders {:standard-out {:enabled? false}
                             :spit {:enabled? false}
                             :rotor {:min-level :warn
                                     :enabled? true
                                     :async? false
                                     :max-message-per-msecs nil
                                     :fn rotor/appender-fn}}
                 :shared-appender-config {:rotor {:path "timeline.log"
                                                  :max-size (* 512 10240) :backlog 5}}})

(def conf {:port 8888
           :onyx {:coord {:hornetq/mode :vm ;; Run HornetQ inside the VM for convenience
                          :hornetq/server? true
                          :hornetq.server/type :vm
                          :zookeeper/address "127.0.0.1:2185"
                          :zookeeper/server? true ;; Run ZK inside the VM for convenience
                          :zookeeper.server/port 2185
                          :onyx.log/config log-config
                          :onyx/id onyx-id
                          :onyx.coordinator/revoke-delay 5000}
                  :peer {:hornetq/mode :vm
                         :zookeeper/address "127.0.0.1:2185"
                         :onyx/id onyx-id
                         :onyx.log/config log-config
                         :scheduler/max-jobs 5
                         :scheduler/num-peers-filter 10
                         :scheduler/jobs (atom {})
                         :scheduler/command-ch onyx-command-ch
                         :user-filter/num-tweets 1000
                         :timeline/input-ch input-ch
                         :timeline/input-ch-mult (mult input-ch)
                         :timeline/output-ch output-ch}
                  :num-peers 20
                  :coordinator-type :memory}})

(defn get-system [conf]
  "Create system by wiring individual components so that component/start
  will bring up the individual components in the correct order."
  (component/system-map
   :twitter (twitter/new-tweet-stream conf)
   :onyx-connection (component/using (onyx/new-onyx-connection conf) [:twitter])
   :onyx-peers (component/using (onyx/new-onyx-peers (:peer (:onyx conf))
                                                     (:num-peers (:onyx conf))) [:onyx-connection])
   :onyx-scheduler (component/using (onyx/new-onyx-scheduler conf) [:onyx-connection])
   :onyx-job (component/using (onyx/new-onyx-job conf) [:onyx-connection])
   :web (web/new-web-state)
   :comm (component/using (comm/new-sente-communicator) [:web :onyx-scheduler])
   :http (component/using (http/new-http-server conf) [:comm])
   :switchboard (component/using (sw/new-switchboard conf) [:web])))

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

(defn -main [& args]
  (go))
