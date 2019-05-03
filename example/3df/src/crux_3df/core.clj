(ns crux-3df.core
  (:require
   [clj-3df.core :as df :use [exec!]]
   [clj-3df.attribute :as attribute]
   [crux.api :as api]
   [crux.db :as crux-db]
   [crux.codec :as crux-codec]
   [crux.memory :as crux-memory]
   [crux.decorators.core :as crux-decorators]))

(def index-dir "data/db-dir")
(def log-dir "data/eventlog")

(def crux-options
  {:kv-backend "crux.kv.rocksdb.RocksKv"
   :bootstrap-servers "kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092"
   :event-log-dir log-dir
   :db-dir index-dir})

(extend-protocol crux-codec/IdToBuffer
  Long
  (id->buffer [^Long this to] (java.nio.ByteBuffer/allocate Long/BYTES)
    (crux-codec/id-function
      to (.array (doto (java.nio.ByteBuffer/allocate Long/BYTES)
                   (.putLong this))))))

(defn validate-schema!
  [crux schema tx-ops]
  (doseq [[tx-op a b] tx-ops]
    (case tx-op
      :crux.tx/put (do (assert (instance? Long a))
                       (assert (map? b))
                       (doseq [[k v] (dissoc b :crux.db/id)
                               :let [{:keys [db/valueType input_semantics]} (get schema k)]]
                         (assert (contains? schema k))
                         (case input_semantics
                           "CardinalityMany"
                           (case valueType
                             (:Eid :Number) (do (assert (coll? v))
                                                (doseq [i v]
                                                  (assert (number? i))))
                             :String (do (assert (coll? v))
                                         (doseq [i v]
                                           (assert (string? i)))))

                           "CardinalityOne"
                           (case valueType
                             (:Eid :Number) (assert (number? v))
                             :String (assert (string? v)))))))))

(defn crux-3df-decorator
  [conn schema]
  (crux-decorators/system-decorator
    {#'api/submit-tx (fn [crux tx-ops]
                       (validate-schema! crux schema tx-ops)
                       (api/submit-tx crux tx-ops))}))

(defn index-to-3df
  [conn db crux tx-ops tx-time tx-id]
  (let [crux-db (api/db crux)]
    (with-open [snapshot (api/new-snapshot crux-db)]
      (let [new-transaction
            (reduce
              (fn [acc [op-key a b]]
                (case op-key
                  :crux.tx/put (do
                                 ;; TODO load whatever the previus document is
                                 (let [new-doc (api/document crux b)
                                       _ (println "NEW-DOC: " new-doc)
                                       eid (:crux.db/id new-doc)
                                       old-doc (some->> (api/history-descending crux-db snapshot (:crux.db/id new-doc))
                                                        ;; history-descending inconsistently includes the current document
                                                        ;; sometimes (on first transaction attleast
                                                        (filter
                                                          (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                                                        first :crux.db/doc)]
                                   (into
                                     acc
                                     (apply
                                       concat
                                       (for [k (set (concat
                                                      (keys new-doc)
                                                      (keys old-doc)))
                                             :when (not= k :crux.db/id)]
                                         (let [old-val (get old-doc k)
                                               new-val (get new-doc k)
                                               old-set (when old-val (if (coll? old-val) (set old-val) #{old-val}))
                                               new-set (when new-val (if (coll? new-val) (set new-val) #{new-val}))]
                                           (println "KEY: " k old-set new-set)
                                           (concat
                                             (for [new new-set
                                                   :when new
                                                   :when (or (nil? old-set) (not (old-set new)))]
                                               [:db/add eid k new])
                                             (for [old old-set
                                                   :when old
                                                   :when (or (nil? new-set) (not (new-set old)))]
                                               [:db/retract eid k old]))))))))))
              []
              tx-ops)]
        (println "3DF: " new-transaction)
        @(exec! conn (df/transact db new-transaction))))))

(defn crux-3df-system
  [conn db schema bootstrap-fn options with-system-fn]
  (let [crux-ref (atom nil)]
    (with-open [^java.io.Closeable crux
                (bootstrap-fn
                  (assoc-in options
                            [:crux/hooks :crux.tx/post-index-hook]
                            (fn [tx-ops tx-time tx-id]
                              (try
                                (println "HOOK CALLED?")
                                (index-to-3df conn db @crux-ref tx-ops tx-time tx-id)
                                (catch Throwable t
                                  (println "ERROR " t))))))]
      (reset! crux-ref ((crux-3df-decorator conn schema) crux))
      (with-system-fn @crux-ref))))

(def schema
  {:user/name (merge
                (attribute/of-type :String)
                (attribute/input-semantics :db.semantics.cardinality/one)
                (attribute/tx-time))

   :user/email (merge
                 (attribute/of-type :String)
                 (attribute/input-semantics :db.semantics.cardinality/one)
                 (attribute/tx-time))

   :user/knows (merge
                 (attribute/of-type :Eid)
                 (attribute/input-semantics :db.semantics.cardinality/many)
                 (attribute/tx-time))

   :user/likes (merge
                 (attribute/of-type :String)
                 (attribute/input-semantics :db.semantics.cardinality/many)
                 (attribute/tx-time))})

(def conn (df/create-debug-conn! "ws://127.0.0.1:6262"))
(def db (df/create-db schema))

(def system
  (future
    (crux-3df-system
      conn
      db
      schema
      api/start-standalone-system
      crux-options
      (fn [c]
        (def crux c)
        (Thread/sleep Long/MAX_VALUE)))))

(comment

  @system

  (future-cancel system)

  (exec! conn (df/create-db-inputs db))

  (api/submit-tx
    crux
    [[:crux.tx/put
      1
      {:crux.db/id 1
       :user/name "Patrik"
       :user/likes ["apples" "bananas"]
       :user/email "p@p.com"}]])

  (api/submit-tx
    crux
    [[:crux.tx/put
      1
      {:crux.db/id 1
       :user/likes ["something new" "change this"]
       :user/name "Patrik"
       :user/knows [3]}]])

  (api/submit-tx
    crux
    [[:crux.tx/put
      2
      {:crux.db/id 2
       :user/name "lars"
       :user/knows [3]}]
     [:crux.tx/put
      3
      {:crux.db/id 3
       :user/name "henrik"
       :user/knows [4]}]])


  (exec! conn
         (df/query
           db "patrik-email"
           '[:find ?email
             :where
             [?patrik :user/name "Patrik"]
             [?patrik :user/email ?email]]))

  (exec! conn
         (df/query
           db "patrik-likes"
           '[:find ?likes
             :where
             [?patrik :user/name "Patrik"]
             [?patrik :user/likes ?likes]]))

  (exec! conn
         (df/query
           db "patrik-knows-1"
           '[:find ?knows
             :where
             [?patrik :user/name "Patrik"]
             [?patrik :user/knows ?knows]]))

  (exec! conn
         (df/query
           db "patrik-knows"
           '[:find ?user-name
             :where
             [?patrik :user/name "Patrik"]
             (trans-knows ?patrik ?knows)
             [?knows :user/name ?user-name]]
           '[[(trans-knows ?user ?knows)
              [?user :user/knows ?knows]]
             [(trans-knows ?user ?knows)
              [?user :user/knows ?knows-between]
              (trans-knows ?knows-between ?knows)]]))

  (df/listen!
    conn
    :key
    (fn [& data] (println "DATA: " data)))

  (df/listen-query!
    conn
    "patrik-knows"
    (fn [& message]
      (println "QUERY BACK: " message)))


  )
