;;;
;;; redis.clj
;;;
;;; Copyright (c) 2023-2024 Xiongfei Shi
;;;
;;; Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
;;; License: Apache-2.0
;;;
;;; https://github.com/shixiongfei/redque-clojure
;;;

(ns redque.redis
  (:require [clojure.string :as string])
  (:import [redis.clients.jedis JedisPool JedisPoolConfig]))

(defn- parse-redis-url [url]
  (let [uri         (java.net.URI. url)
        port        (.getPort uri)
        user-info   (or (.getUserInfo uri) "")
        user-passwd (string/split user-info #":")
        path        (.getPath uri)]
    {:scheme   (.getScheme uri)
     :host     (.getHost uri)
     :port     (if (> port 0) port 6379)
     :user     (get user-passwd 0 "")
     :password (get user-passwd 1 "")
     :database (if (= path "") 0
                   (Integer. (subs path 1)))}))

(defn create-redis-pool [url & {:keys [max-total max-idle min-idle]
                                :or   {max-total 3 max-idle  2 min-idle  1}}]
  (let [redis-conf (parse-redis-url url)
        ssl        (= (:scheme redis-conf) "rediss")
        pool-conf  (doto (JedisPoolConfig.)
                     (.setMaxTotal max-total)
                     (.setMaxIdle  max-idle)
                     (.setMinIdle  min-idle))]
    (if (not= (:user redis-conf) "")
      (JedisPool. pool-conf
                  (:host redis-conf)
                  (:port redis-conf)
                  3000
                  (:user redis-conf)
                  (:password redis-conf)
                  (:database redis-conf)
                  ssl)
      (JedisPool. pool-conf
                  (:host redis-conf)
                  (:port redis-conf)
                  3000
                  (:password redis-conf)
                  (:database redis-conf)
                  ssl))))

(defn connect [jedis-pool] (.getResource jedis-pool))

(defn disconnect [jedis] (.close jedis))

(defmacro with [jedis-pool [jedis] & body]
  `(let [~jedis (connect ~jedis-pool)]
     (try
       ~@body
       (finally (disconnect ~jedis)))))
