(ns onyx-timeline-example.server
  (:gen-class)
  (:require [onyx-timeline-example.communicator.component :as comm]
            [onyx-timeline-example.http.component :as http]
            [onyx-timeline-example.switchboard :as sw]
            [clojure.tools.namespace.repl :refer  (refresh)]
            [environ.core :refer [env]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))

(def conf {:tw-check-interval-sec    10
           :tw-restart-wait          60
           :port                     8888})

(defn get-system [conf]
  "Create system by wiring individual components so that component/start
  will bring up the individual components in the correct order."
  (component/system-map
    :comm-channels          (comm/new-communicator-channels)
    :producer-channels (comm/new-producer-channels)
    :comm          (component/using (comm/new-communicator)     {:channels   :comm-channels})
    :http          (component/using (http/new-http-server conf) {:comm       :comm})
    :switchboard   (component/using (sw/new-switchboard)        {:comm-chans :comm-channels
                                                                 :producer-chans :producer-channels})))
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
