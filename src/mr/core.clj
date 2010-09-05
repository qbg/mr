(ns mr.core
  (:require [mr.wq :as wq]))

(defn- mr-single
  "mapreduce vec on this thread"
  [cont f g init vec]
  (let [n (count vec)]
    (if (zero? n)
      (cont init)
      (loop [pos 0, acc init]
	(if (< pos n)
	  (recur (inc pos) (g acc (f (nth vec pos))))
	  (cont acc))))))

(defn- mr-worker
  "Helper fuction for mapreduce.  wq is the work queue, t is the threshold, cont
is the continuation, f is the map fn, g is the reduce fn, init is the identity
of g, and vec is a vector"
  [wq t cont f g init vec]
  (let [n (count vec)]
    (if (<= n t)
      (mr-single cont f g init vec)
      (let [half (quot n 2)
	    left (subvec vec 0 half)
	    right (subvec vec half (count vec))]
	(wq/invoke-later-task
	 wq
	 #(cont (g %1 %2))
	 #(mr-worker wq t % f g init left)
	 #(mr-worker wq t % f g init right))))))
			   
(defn mapreduce
  "Mapreduce coll (vector preferred), where f is the mapper, g is the reducer,
and init is the identity of g.  mapreduce works in parallel, with n worker
threads, each processing chunks up to c elements of coll long."
  [n c f g init coll]
  (if (not (vector? coll))
    (recur n c f g init (vec coll))
    (let [wq (wq/make-wq n)
	  val (promise)
	  final (fn [v]
		  (deliver val v)
		  (wq/shutdown-wq wq))]
      (wq/add-task wq #(mr-worker wq c final f g init coll))
      @val)))

(defn mr
  "Factory function for mapreduce. Recognized options are :workers to set the
number of workers (defaults to number of available processors), and :chunks to
set the chunk size (defaults to 1)"
  [& options]
  (let [{:keys [workers chunks]
	 :or {workers (.. Runtime getRuntime availableProcessors), chunks 1}
	 :as opts}
	options
	remainder (apply dissoc opts [:workers :chunks])]
    (if (not= remainder {})
      (throw (IllegalArgumentException. (format "Unknown options %s" remainder))))
    #(mapreduce workers chunks %1 %2 %3 %4)))
