(ns flambo-plygrnd.sqlexecutor
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  (:require [flambo.tuple :as ft])
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [monger.operators :refrer :all]))

(defn make-spark-context
  [ master app-name ]  
    (-> (conf/spark-conf)
        (conf/master master)
        (conf/app-name app-name)
        (f/spark-context)))

(defn make-db-connection
  [ db-name ]
    (-> (mg/connect)
        (mg/get-db db-name)))

(def db-name "bit-core")
(def coll "bitts") 

(def db (make-db-connection db-name ))

(defn load-ts
  [ sym tick ] 
    (mc/find-maps db coll {:code sym :unit 10 }))

;(load-ts "BTC" 10)

