(ns message-data.util
  (:require
   [datahike.api         :as d]
   [datahike.db.utils    :refer [db?]]
   [datahike.pull-api    :as dp]
   [message-data.schema  :refer [db-schema]]
   [mount.core           :refer [defstate]]
   [taoensso.timbre      :as log]))

;;; ToDo: The problem with output to log/debug might have to do with *err* not defined in cljs.
(defn custom-output-fn
  " - I don't want :hostname_ and :timestamp_ in the log output preface text..
    - I don't want any preface text in rad-mapper.parse output."
  ([data] (custom-output-fn nil data))
  ([opts data]
   (if (=  (:?ns-str data) "rad-mapper.parse")
     (apply str (:vargs data)) ; So it can do simple indented call tracing.
     (taoensso.timbre/default-output-fn opts (dissoc data :hostname_ :timestamp_)))))

(defn config-log
  "Configure Timbre: set reporting levels and specify a custom :output-fn."
  [min-level]
  (if (#{:trace :debug :info :warn :error :fatal :report} min-level)
    (log/set-config!
     (-> log/*config*
         (assoc :output-fn #'custom-output-fn)
         (assoc :min-level [[#{"message-data.*" "user"} min-level]
                            [#{"datahike.*"} :error]
                            [#{"*"} :error]])))
     (log/error "Invalid timbre reporting level:" min-level)))

(defn default-min-log-level
  "Get the value of 'RM-DEFAULT' in (:min-level log/*config*), it designates
   the logging level for namespaces of rad-mapper, including rad-mapper,
   exerciser-app, and user."
  []
  (->> log/*config* :min-level (some #(when (contains? (first %) "RM-DEFAULT") (second %)))))

;;;====================================== DB stuff ======================================
;;; Schema stuff
;;; This seems to cause problems in recursive resolution. (See resolve-db-id)"
(defn db-ref?
  "It looks to me that a datahike ref is a map with exactly one key: :db/id."
  [obj]
  (and (map? obj) (= [:db/id] (keys obj))))

;;; {:db/id 3779}
(defn resolve-db-id
  "Return the form resolved, removing properties in filter-set,
   a set of db attribute keys, for example, #{:db/id}."
  ([form conn-atm] (resolve-db-id form conn-atm #{}))
  ([form conn-atm filter-set]
   (letfn [(resolve-aux [obj]
             (cond
               (db-ref? obj) (let [res (dp/pull @conn-atm '[*] (:db/id obj))]
                               (if (= res obj) nil (resolve-aux res)))
               (map? obj) (reduce-kv (fn [m k v] (if (filter-set k) m (assoc m k (resolve-aux v))))
                                     {}
                                     obj)
               (vector? obj)      (mapv resolve-aux obj)
               (set? obj)    (set (mapv resolve-aux obj))
               (coll? obj)        (map  resolve-aux obj)
               :else  obj))]
     (resolve-aux form))))

(declare connect-atm)
;;; (list-schemas :sdo :oagi)
(defn list-schemas
  "Return a list of schema, by default they are sorted.
   Attrs should be a list of attributes to return"
  [& {:keys [sdo sort? _attrs] :or {sort? true sdo '_}}] ; ToDo: attrs
  (let [result (d/q '[:find ?n ?sdo
                      :keys schema_name schema_sdo
                      :in $ ?sdo
                      :where
                      [?s :schema_name ?n]
                      [?s :schema_sdo ?sdo]]
                    @(connect-atm :schema) sdo)]
    (cond->> result
      true (map :schema_name)
      sort? sort
      true vec)))

;;; (get-schema ""urn:oagi-10.:elena.2023-07-02.ProcessInvoice-BC_1_v2")
(defn get-schema
  "Return the map stored in the database for the given schema-urn. Useful in development.
    :filter-set - DB attribute to leave out (e.g. #{:db/id} or #{:db/doc-string}) "
  [schema-name & {:keys [resolve? filter-set] :or {resolve? true filter-set #{:db/id}}}]
  (when-let [ent  (d/q '[:find ?ent .
                         :in $ ?schema-name
                         :where [?ent :schema_name ?schema-name]]
                       @(connect-atm :schema) schema-name)]
    (cond-> (dp/pull @(connect-atm :schema) '[*] ent)
      resolve? (resolve-db-id (connect-atm :schema) filter-set))))

(defn db-for!
  "Create a database for the argument data, a vector, and return a connection to it."
  [data & {:keys [db-name schema] :or {db-name "temp-db" schema db-schema}}]
  (let [db-cfg {:store {:backend :mem :id db-name} :keep-history? false :schema-flexibility :write}]
    (when (d/database-exists? db-cfg) (d/delete-database db-cfg))
    (d/create-database db-cfg)
    (let [db-atm (d/connect db-cfg)]
      (d/transact db-atm schema)
      (when (not-empty data) (d/transact db-atm data))
      db-atm)))

(defonce databases-atm (atom {}))

(defn register-db
  "Add a DB configuration."
  [k config]
  (assert (#{:schema} k))
  (swap! databases-atm #(assoc % k config)))

(defn connect-atm
  "Return a connection atom for the DB."
  [k]
  (when-let [db-cfg (get @databases-atm k)]
    (if (d/database-exists? db-cfg)
      (d/connect db-cfg)
      (log/warn "There is no DB to connect to."))))

(defstate logging
  :start
  (do (config-log :info)))
