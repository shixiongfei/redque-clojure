(defproject redque "0.1.0"
  :description "A reliable message queue base on Redis"
  :url "https://github.com/shixiongfei/redque-clojure"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.txt"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [redis.clients/jedis "5.0.1"]
                 [org.clojure/data.json "2.4.0"]
                 [nano-id "1.0.0"]])
