(ns mr.core
  (:require [mr.wq :as wq]))

(defn- mr-worker
  "Helper fuction for mapreduce.  wq is the work queue, cont is the
continuation, f is the map fn, g is the reduce fn, init is the identity of g,
and vec is a vector"
  [wq cont f g init vec]
  (let [n (count vec)]
    (cond
     (zero? n) (cont init)
     (= n 1) (cont (f (nth vec 0)))
     :else (let [half (quot n 2)
		 left (subvec vec 0 half)
		 right (subvec vec half (count vec))]
	     (wq/invoke-later-task
	      wq
	      #(cont (g %1 %2))
	      #(mr-worker wq % f g init left)
	      #(mr-worker wq % f g init right))))))
			   
(defn mapreduce
  "Mapreduce coll (vector preferred) using n workers with the mapping fn being
f, the reducing fn being g, and the identity of g being init.  Only useful when
the cost of mapping and reducing outweighs the coordination costs of executing
in parallel."
  [n f g init coll]
  (if (not (vector? coll))
    (recur n f g init (vec coll))
    (let [wq (wq/make-wq n)
	  val (promise)
	  final (fn [v]
		  (deliver val v)
		  (wq/shutdown-wq wq))]
      (wq/add-task wq #(mr-worker wq final f g init coll))
      @val)))

