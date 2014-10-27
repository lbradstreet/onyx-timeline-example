(ns onyx-timeline-example.switchboard
  (:gen-class)
  (:require
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [clojure.core.async :as async :refer [chan mult tap pipe]]))

;;;; This component is the central switchboard for information flow in this application.
;;;; The individual channel components come together like wiring harnesses in a car. One for the engine,
;;;; one for the AC, one for the soundsystem and so on.

(defrecord Switchboard [comm-chans producer-chans]
  component/Lifecycle
  (start [component] (log/info "Starting Switchboard Component")
    (pipe (:output producer-chans) (:timeline comm-chans))
    component)
  (stop [component] (log/info "Stop Switchboard Component")))

(defn new-switchboard [] (map->Switchboard {}))
