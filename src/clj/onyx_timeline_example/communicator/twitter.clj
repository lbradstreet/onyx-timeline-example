(ns onyx-timeline-example.communicator.twitter
  (:require [clojure.core.async :refer [>!!]]
            [com.stuartsierra.component :as component]
            [cheshire.core :refer [parse-string]]
            [environ.core :refer [env]])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [com.twitter.hbc ClientBuilder]
           [com.twitter.hbc.core Constants]
           [com.twitter.hbc.core HttpHosts]
           [com.twitter.hbc.core.endpoint StatusesSampleEndpoint]
           [com.twitter.hbc.httpclient.auth OAuth1]
           [com.twitter.hbc.core.processor StringDelimitedProcessor]))

(defn consume-firehose! [client message-queue ch]
  (.connect client)
  (try
    (while true
      (let [tweet (parse-string (.take message-queue) true)]
        (when (:id_str tweet)
          (println (:text tweet))
          (>!! ch tweet))))
    (finally
     (.stop client 500))))

(defrecord TweetStream []
  component/Lifecycle
  (start [component]
    (println "Starting Tweet Stream")
    (let [ch (:ch (:input-stream component))
          consumer-key (env :twitter-consumer-key)
          consumer-secret (env :twitter-consumer-secret)
          token (env :twitter-token)
          secret (env :twitter-secret)
          hosts (HttpHosts. Constants/STREAM_HOST)
          endpoint (StatusesSampleEndpoint.)
          auth (OAuth1. consumer-key consumer-secret token secret)
          message-queue (LinkedBlockingQueue. 100000)
          builder (doto (ClientBuilder.)
                    (.name "client-1")
                    (.hosts hosts)
                    (.authentication auth)
                    (.endpoint endpoint)
                    (.processor (StringDelimitedProcessor. message-queue)))
          client (.build builder)]
      (assoc component :firehose-fut (future (consume-firehose! client message-queue ch)))))
  (stop [component]
    (println "Stopping Tweet Stream")
    (future-cancel (:firehose-fut component))
    component))

(defn new-tweet-stream [] (map->TweetStream {}))
