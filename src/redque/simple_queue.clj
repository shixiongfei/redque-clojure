;;;
;;; simple_queue.clj
;;;
;;; Copyright (c) 2023-2024 Xiongfei Shi
;;;
;;; Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
;;; License: Apache-2.0
;;;
;;; https://github.com/shixiongfei/redque-clojure
;;;

(ns redque.simple-queue
  (:require [clojure.data.json :as json]
            [redque.redis :as redis]))

(defrecord SimpleQueue [redis-pool])

(defn make-simple-queue [url]
  (SimpleQueue. (redis/create-redis-pool url)))

(defn produce [simple-queue name payload]
  (redis/with (:redis-pool simple-queue) [jedis]
    (.lpush jedis name (into-array String [(json/write-str payload)]))))

(defn consume [simple-queue name & {:keys [timeout]}]
  (if (nil? timeout)
    (when-some [payload (redis/with (:redis-pool simple-queue) [jedis]
                          (.rpop jedis name))]
      (json/read-str payload :key-fn keyword))
    (when-some [payload (redis/with (:redis-pool simple-queue) [jedis]
                          (.brpop jedis (double timeout) name))]
      (json/read-str (.getValue payload) :key-fn keyword))))
