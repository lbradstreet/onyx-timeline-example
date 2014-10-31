(ns onyx-timeline-example.switchboard
  (:gen-class)
  (:require
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [clojure.core.async :as async :refer [chan mult tap pipe]]))

;;;; This component is the central switchboard for information flow in this application.
;;;; The individual channel components come together like wiring harnesses in a car. One for the engine,
;;;; one for the AC, one for the soundsystem and so on.

(defrecord Switchboard []
  component/Lifecycle
  (start [component]
    (println "Starting Switchboard Component")
    (pipe (:ch (:output-stream component))
          (:timeline (:comm-channels component)))
    component)
  (stop [component]
    (println "Stopping Switchboard Component")
    component))

(defn new-switchboard [] (map->Switchboard {}))
