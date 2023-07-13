(ns message-data.core
  (:require
   [clojure.java.io :as io]
   [datahike.api         :as d]
   [datahike.pull-api    :as dp]
   [message-data.util    :as util :refer [connect-atm db-for! get-schema list-schemas register-db resolve-db-id]] ; For mount
   [mount.core           :as mount :refer [defstate]]
   [taoensso.timbre      :as log]))

(def db-cfg-atm "Configuration map used for connecting to the db. It is set in core."  (atom nil))
(def base-dir "The base directory of the databases. Can't be set at compile time in Docker." nil)
(def db-dir "The directory containing schema DBs. Can't be set at compile time in Docker." nil)
(declare schema-cfg)

;;; Hmmm, 24262 of these!
(defn get-elem-names
  "Get the element_names"
  []
  (->> (d/q '[:find ?name :keys elem/name
              :where
              [_ :element_name ?name]]
               @(connect-atm :schema))
      distinct
      (sort-by :elem/name)))

(defn get-schema-names
  "Get the element_names"
  []
  (->> (d/q '[:find [?name ...]
              :where
              [_ :schema_name ?name]]
               @(connect-atm))
       sort))

(defn tryme []
  (let [bie (get-schema "urn:oagi-10.:elena.2023-07-02.ProcessInvoice-BC_1_v2")]
    (db-for! (vector bie))))


(defn parent-child [db]
  (d/q '[:find ?parent ?child
         :keys parent child
         :where
         [?x     :element_name        ?parent]
         [?x     :element_complexType ?cplx1]
         [?cplx1 :model_sequence      ?def]
         [?def   :model_elementDef    ?cplx2]
         [?cplx2 :element_name        ?child]]
         db))

;;; Schema DB connection ============================================
(defn init-db
  "Set directory vars from environment variables.
   Reset and return the atom used to connect to the db."
  []
  (alter-var-root
   (var base-dir)
   (fn [_]
     (or (-> (System/getenv) (get "RM_MESSAGING"))
         (throw (ex-info (str "Set the environment variable RM_MESSAGING to the directory containing RADmapper databases."
                              "\nCreate a directory 'schema' under it.") {})))))
  (alter-var-root
   (var db-dir)
   (fn [_]
     (if (-> base-dir (str "/databases/schema") io/file .isDirectory)
       (str base-dir "/databases/schema")
       (throw (ex-info "Directory not found:" {:dir (str base-dir "/databases/schema")})))))
  (reset! db-cfg-atm {:store {:backend :file :path db-dir}
                      :keep-history? false ; Not clear that I'd have to respecify this, right?
                      :schema-flexibility :write})
  (register-db :schema @db-cfg-atm)
  @db-cfg-atm)

(defstate schema-cfg
  :start (init-db))
