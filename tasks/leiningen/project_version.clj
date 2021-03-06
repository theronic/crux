(ns leiningen.project-version
  (:require [clojure.java.shell :as sh]
            [clojure.string]))

(defn project-version [project & args]
  (println (:version project)))

(defn- version-from-git []
  (let [{:keys [exit out]} (sh/sh "git" "describe" "--tags" "--dirty" "--long")]
    (assert (= 0 exit))
    ;;"19.04-1.0.2-alpha-22-g9452e91b-dirty"
    (let [[_ tag ahead sha dirty] (re-find #"(.*)\-(\d+)\-([0-9a-z]*)(\-dirty)?$" (clojure.string/trim out))
          ahead? (not= ahead "0")
          dirty? (not (empty? dirty))]
      (assert (re-find #"^\d+\.\d+\-\d+\.\d+\.\d+(\-(alpha|beta))?$" tag) "Tag format unexpected.")
      (if (and (not ahead?) (not dirty?))
        tag
        (str tag "-SNAPSHOT")))))

(defn middleware [project]
  (assoc project :version (version-from-git)))
