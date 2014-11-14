(ns onyx-timeline-example.onyx.functions
  (:require [clojure.data.fressian :as fressian]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [onyx.extensions :as extensions]
            [lib-onyx.interval]))

(defn into-words [s]
  (s/split s #"\s"))

(defn extract-tweet [segment]
  {:tweet-id (str (:id segment))
   :twitter-user (:screen_name (:user segment))
   :tweet (:text segment)})

(defn filter-by-regex [regex {:keys [tweet] :as segment}]
  (if (re-find regex tweet) segment []))

(defn extract-hashtags [{:keys [tweet]}]
  (->> (into-words tweet)
       (keep (partial re-matches #"#.*"))
       (map (fn [hashtag] {:hashtag hashtag}))))

(defn split-into-words [min-chars {:keys [tweet]}]
  (->> (into-words tweet)
       (filter (fn [word] (> (count word) min-chars)))
       (map (fn [word] {:word word}))))

(defn normalize-token [token]
  (-> token
      s/lower-case
      (s/replace #"[,.:']+$" "")))

(defn normalize-words [{:keys [word]}]
  {:word (normalize-token word)})

(defn normalize-hashtags [{:keys [hashtag]}]
  {:hashtag (normalize-token hashtag)})

(defn update-trends 
  "Maintains a rolling count of tokens (words, hashtags, etc),
  over the last period num token occurrences"
  [{:keys [tokens counts]} token period]
  (let [ts (conj tokens token)
        updated-count (inc (counts token 0))] 
    (if (> (count ts) period)
      (let [removed-token (first ts)
            removed-count (dec (counts removed-token))] 
        {:tokens (subvec ts 1) 
         :counts (assoc (if (zero? removed-count)
                          (dissoc counts removed-token)
                          (assoc counts removed-token removed-count))
                        token
                        updated-count)})
      {:tokens ts
       :counts (assoc counts token updated-count)})))

(defn word-count [local-state trend-period exclude-hashtags? {:keys [word] :as segment}]
  (if (and exclude-hashtags? (.startsWith word "#"))
    []
    (do (swap! local-state update-trends word trend-period)
        [])))

(defn hashtag-count [local-state trend-period {:keys [hashtag] :as segment}]
  (swap! local-state update-trends hashtag trend-period)
  [])

(defn top-words [m]
  (->> m
       (into [])
       (sort-by second)
       (reverse)
       (take 8)
       (into {})))

(defn log-and-purge-words [{:keys [onyx.core/queue] :as event}]
  (let [result (top-words (:counts @(:timeline/word-count-state event)))
        compressed-state (fressian/write {:top-words result})]
    (let [session (extensions/create-tx-session queue)]
      (doseq [queue-name (:onyx.core/egress-queues event)]
        (let [producer (extensions/create-producer queue session queue-name)]
          (extensions/produce-message queue producer session compressed-state)
          (extensions/close-resource queue producer)))
      (extensions/commit-tx queue session)
      (extensions/close-resource queue session))))

(defn log-and-purge-hashtags [{:keys [onyx.core/queue] :as event}]
  (let [result (top-words (:counts @(:timeline/hashtag-count-state event)))
        compressed-state (fressian/write {:top-hashtags result})
        session (extensions/create-tx-session queue)]
    (doseq [queue-name (:onyx.core/egress-queues event)]
      (let [producer (extensions/create-producer queue session queue-name)]
        (extensions/produce-message queue producer session compressed-state)
        (extensions/close-resource queue producer)))
      (extensions/commit-tx queue session)
      (extensions/close-resource queue session)))

(defn wrap-sente-user-info [user segment]
  (assoc segment :sente/uid user))
