(ns message-data.core
  (:require
   [clojure.java.io      :as io]
   [clojure.pprint       :refer [cl-format pprint]]
   [datahike.api         :as d]
   [datahike.pull-api    :as dp]
   [message-data.util    :as util :refer [connect-atm db-for! get-schema list-schemas register-db resolve-db-id]] ; For mount
   [mount.core           :as mount :refer [defstate]]
   [taoensso.timbre      :as log]))

(def diag (atom nil))
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

(def sought-sdo? nil)
(def sought-type? nil)
(defn get-schema-names ; There are 226 schema meeting the default requirements.
  "Get the names of schema that meet requirements:
    - sdos  : a set, defaults to #{:oagi :oasis :qif}.
    - types : a set, defaults to #{::generic_messageSchema :cct_bie :generic_xsdFile :cct_messageSchema}"
  [& {:keys [sdos types] :or {sdos #{:oagi :oasis :qif}
                              types #{:generic_messageSchema :cct_bie :generic_xsdFile :cct_messageSchema :oagis_bod}}}]
  (alter-var-root #'sought-sdo?  (fn [_] (fn [x] (sdos x))))
  (alter-var-root #'sought-type? (fn [_] (fn [x] (types x))))
  (d/q '[:find [?name ...]
         :where
         [?e :schema_name ?name]
         [?e :schema_sdo  ?sdo]
         [?e :schema_type ?type]
         [(message-data.core/sought-sdo? ?sdo)]
         [(message-data.core/sought-type? ?type)]]
       @(connect-atm :schema)))

(defn get-attr
  "Get all values of the argument DB attribute."
  [attr]
  (d/q '[:find [?val ...]
         :in $ ?attr
         :where [?s ?attr ?val]]
       @(connect-atm :schema)
       attr))

(defn random-names
  "Return a vector of n random schema names."
  [n]
  (let [schemas (get-schema-names)
        len (count schemas)
        result (atom [])]
    (dotimes [_i n] (swap! result conj (nth schemas (rand-int len))))
    @result))

;;; -------------------- Shape ------------------------------
#_(defn parent-child [db]
  (d/q '[:find ?parent ?child
         :keys parent child
         :where
         [?x     :element_ref         ?parent]
         [?x     :element_complexType ?cplx1]
         [?cplx1 :model_sequence      ?def]
         [?def   :model_elementDef    ?cplx2]
         [?cplx2 :element_ref         ?child]]
       @db))

(defn parent-child [db]
  (d/q '[:find ?parent ?child
         :keys parent child
         :where
         [?x     :element_name        ?parent]
         [?x     :element_complexType ?cplx1]
         [?cplx1 :model_sequence      ?def]
         [?def   :model_elementDef    ?cplx2]
         [?cplx2 :element_name        ?child]]
       @db))

(defn roots [db]
  (d/q '[:find ?name
         :where
         [?c :schema_content   ?e]
         [?e :model_elementDef ?d]
         [?d :element_name     ?name]]
       @db))

(defn shape
  "Return the shape map for the schema information provided."
  [root pc-data]
    (letfn [(children [p] (->> pc-data (filter #(= (:parent %) p)) (map :child)))
            (shp [parent]
              (reduce (fn [pmap c]
                        (update pmap parent #(assoc % c (or (not-empty (-> (shp c) (get c))) "<data>"))))
                      {}
                      (children parent)))]
      (shp root)))

(defn get-shape
  "Return the shape give a schema_name."
  [sname]
  (let [obj (get-schema sname)
        db (db-for! (vector obj))
        root (-> db roots first first)
        pc (parent-child db)]
    (reset! diag {:root root :pc pc :db db})
    (shape root pc)))

(defn random-shapes
  "Return a vector of n random schema shapes."
  [n]
  (reduce (fn [m name] (assoc m name (get-shape name)))
          {}
          (random-names n)))

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
