(ns flambo-plygrnd.core
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.sql :as sql]
            [flambo.sql-functions :as sqlf]

            [flambo-plygrnd.sqlexecutor :as se]
            [flambo-plygrnd.utils :as u]

            [clj-time.core :as t]
            [clojure.string :as s]))

(defn ts-to-pair-rdd 
  [ sc ts ]
    (->> ts
         (map (fn [x] (ft/tuple (u/timefstr (:candleDateTime x)) (ft/tuple (:code x) (:tradePrice x)))))
         (f/parallelize-pairs sc)
         (f/sort-by-key)))

(def sc (se/make-spark-context "local" "app"))

(def a (->>
            (se/load-ts "BTC" 10)
            (map (fn [x] (ft/tuple (u/timefstr (:candleDateTime x)) (:tradePrice x)) ))))

(def rdd (f/parallelize-pairs sc a))
(f/sort-by-key rdd)
(f/reduce 



(defn mdd-sub-func
  [ x ] 
  




