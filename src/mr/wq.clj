;;;; The work queue abstraction used by mapreduce
(ns mr.wq
  (:import [java.util.concurrent
	    ExecutorService Executors]))

(set! *warn-on-reflection* true)

(defn add-task
  "Enqueue f, a no-arg fn, in wq"
  [^ExecutorService wq f]
  (.execute wq f))

(defn shutdown-wq
  "Shutdown the wq workers"
  [^ExecutorService wq]
  (.shutdown wq))

(defn make-wq
  "Create a new running wq with n workers"
  [n]
  (Executors/newFixedThreadPool n))

(defn invoke-later-task
  "Schedule tasks (a fn whose arg is the continuation to call) to execute and
then call f with their results"
  [wq f & fns]
  (let [n (count fns)
	args (atom (vec (repeat n nil)))
	left (atom n)
	set-arg (fn [n v]
		  (swap! args assoc n v)
		  (if (zero? (swap! left dec))
		    (add-task wq #(apply f @args))))]
    (doseq [[job n] (map list fns (range n))]
      (add-task wq (fn [] (job #(set-arg n %)))))))

(defmacro later-call
  "Asynchronously schedule a parallel call"
  [wq f & args]
  (letfn [(do-value [arg]
	    `(fn [c#]
	       (c# ~arg)))]
    `(invoke-later-task ~wq ~f ~@(map do-value args))))

(defmacro later-let
  "Asynchronously evaluate the body after getting the bindings in parallel.
Does not return the value of the body"
  [wq bindings & body]
  (let [locals (take-nth 2 bindings)
	vals (take-nth 2 (rest bindings))]
    `(later-call
      wq
      (fn [~@locals] ~@body)
      ~@vals)))
