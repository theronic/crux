(defproject juxt/crux-3df "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux "19.04-1.0.2-alpha-SNAPSHOT"]
                 [org.rocksdb/rocksdbjni "5.17.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.kafka/kafka-clients "2.1.0"]

                 [clj-3df "0.1.0"]]
  :global-vars {*warn-on-reflection* true}
  :main example-standalone-webservice.main)
