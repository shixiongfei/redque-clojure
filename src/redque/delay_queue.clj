(ns redque.delay-queue
  (:require [clojure.data.json :as json]
            [nano-id.core :refer [nano-id]]
            [redque.redis :as redis]))

(defrecord DelayQueue [redis-pool])

(defn make-delay-queue [url]
  (DelayQueue. (redis/create-redis-pool url)))

(def ^:private producer-script-sha "REDQUE:SCRIPTS:DELAY:PRODUCER:SHA")
(def ^:private consumer-script-sha "REDQUE:SCRIPTS:DELAY:CONSUMER:SHA")

(defn- reload-script [jedis key script]
  (let [script-sha (.scriptLoad jedis script)]
    (.set jedis key script-sha)
    script-sha))

(defn- reload-producer-script [jedis]
  (let [script "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
                redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
                return 1"]
    (reload-script jedis producer-script-sha script)))

(defn- reload-consumer-script [jedis]
  (let [script "local status, type = next(redis.call('TYPE', KEYS[1]))
                if status ~= nil and status == 'ok' then
                  if type == 'zset' then
                      local list = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])
                      if list ~= nil and #list > 0 then
                          redis.call('ZREM', KEYS[1], unpack(list))
                          local result = redis.call('HMGET', KEYS[2], unpack(list))
                          redis.call('HDEL', KEYS[2], unpack(list))
                          return result
                      end
                  end
                end
                return nil"]
    (reload-script jedis consumer-script-sha script)))

(defn produce [delay-queue name payload delay]
  (redis/with (:redis-pool delay-queue) [jedis]
    (let [script-sha (atom (.get jedis producer-script-sha))]
      (when (nil? @script-sha)
        (reset! script-sha (reload-producer-script jedis)))
      (let [keys      [name (str name ":hash")]
            arguments [(str delay) (nano-id) (json/write-str payload)]]
        (try
          (.evalsha jedis @script-sha keys arguments)
          (catch Exception _
            (reset! script-sha (reload-producer-script jedis))
            (.evalsha jedis @script-sha keys arguments)))))))

(defn consume [delay-queue name max-delay]
  (redis/with (:redis-pool delay-queue) [jedis]
    (let [script-sha (atom (.get jedis consumer-script-sha))]
      (when (nil? @script-sha)
        (reset! script-sha (reload-consumer-script jedis)))
      (let [keys      [name (str name ":hash")]
            arguments ["0" (str max-delay) "0" "1"]]
        (when-some [payload (try
                              (.evalsha jedis @script-sha keys arguments)
                              (catch Exception _
                                (reset! script-sha (reload-consumer-script jedis))
                                (.evalsha jedis @script-sha keys arguments)))]
          (when (> (count payload) 0)
            (json/read-str (.get payload 0) :key-fn keyword)))))))
