(ns redque.stream-queue
  (:require [clojure.data.json :as json]
            [redque.redis :as redis])
  (:import [redis.clients.jedis StreamEntryID]
           [redis.clients.jedis.params XAddParams XReadGroupParams]))

(defrecord StreamQueue [redis-pool])

(defn make-stream-queue [url]
  (StreamQueue. (redis/create-redis-pool url)))

(defn produce [stream-queue stream payload & {:keys [maxlen] :or {maxlen 100000}}]
  (let [params (doto (XAddParams.)
                 (.id "*")
                 (.maxLen maxlen)
                 (.approximateTrimming))]
    (redis/with (:redis-pool stream-queue) [jedis]
      (.xadd jedis stream params {"payload" (json/write-str payload)}))))

(defn ensure-group [stream-queue stream group & {:keys [rewind] :or {rewind false}}]
  (try
    (let [id (if rewind (StreamEntryID. 0) (StreamEntryID/LAST_ENTRY))]
      (redis/with (:redis-pool stream-queue) [jedis]
        (.xgroupCreate jedis stream group id true)))
    (catch Exception _
      (comment "BUSYGROUP Consumer Group name already exists"))))

(defn- real-consume [stream-queue stream group consumer id]
  (let [params  (doto (XReadGroupParams.)
                  (.count 1))
        kv-list (redis/with (:redis-pool stream-queue) [jedis]
                  (.xreadGroup jedis group consumer params {stream id}))]
    (when (> (count kv-list) 0)
      (let [stream-entry-list (.getValue (.get kv-list 0))]
        (when (> (count stream-entry-list) 0)
          (let [stream-entry (.get stream-entry-list 0)
                fields       (.getFields stream-entry)]
            [(.getID stream-entry)
             (json/read-str (.get fields "payload") :key-fn keyword)]))))))

(defn consume [stream-queue stream group consumer]
  (real-consume stream-queue stream group consumer (StreamEntryID/UNRECEIVED_ENTRY)))

(defn consume-pending [stream-queue stream group consumer]
  (real-consume stream-queue stream group consumer (StreamEntryID. 0)))

(defn ack [stream-queue stream group id]
  (redis/with (:redis-pool stream-queue) [jedis]
    (.xack jedis stream group (into-array StreamEntryID [id]))))
