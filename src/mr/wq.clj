;;;; The work queue abstraction used by mapreduce
(ns mr.wq
  (:import [java.util.concurrent
	    LinkedBlockingQueue TimeUnit]))

(set! *warn-on-reflection* true)

(defn make-chan
  "Create a chan"
  []
  (LinkedBlockingQueue.))

(defn put-chan
  "Send item on chan"
  [^LinkedBlockingQueue chan item]
  (.put chan item))

(defn get-chan
  "Recieve an item on chan, waiting as needed"
  [^LinkedBlockingQueue chan]
  (.take chan))

(defn get-chan-timeout
  "Recieve an item on chan, waiting for up to n timeunits.  If the wait times
out, returns nil"
  [^LinkedBlockingQueue chan n timeunit]
  (.poll chan n timeunit))

(defn listen-chan
  "Call f with every item that arrives on chan, quitting when :shutdown is
received"
  [chan f]
  (let [item (get-chan chan)]
    (when (not= :shutdown item)
      (f item)
      (recur chan f))))

(defmacro listen
  "Macro version of listen-chan"
  [[chan name] & body]
  `(listen-chan ~chan (fn [~name] ~@body)))

(defrecord WorkQueue [work workers chans running nworkers])

(defn add-task
  "Enqueue f, a no-arg fn, in wq"
  [wq f]
  (put-chan (:work wq) f))

(defn- worker-avail
  "Signal that the worker associated with chan is available in wq"
  [wq chan]
  (put-chan (:workers wq) chan))

(defn- worker
  "Start a worker listing on chan working on wq"
  [wq chan]
  (listen [chan f]
    (f)
    (worker-avail wq chan))
  (swap! (:nworkers wq) dec))

(defn shutdown-wq
  "Shutdown the wq workers"
  [wq]
  (reset! (:running wq) false)
  (doseq [c (:chans wq)]
    (put-chan c :shutdown)))

(defn- find-worker
  "Return a free worker in wq, or nil if there are no workers"
  [wq]
  (if (> @(:nworkers wq) 0)
    (if-let [w (get-chan-timeout (:workers wq) 1 TimeUnit/SECONDS)]
      w
      (recur wq))))

(defn- wq-controller
  "Push jobs to workers as long as the wq is running"
  [wq]
  (while @(:running wq)
    (if-let [job (get-chan-timeout (:work wq) 1 TimeUnit/SECONDS)]
      (if-let [worker (find-worker wq)]
	(put-chan worker job)))))

(defn make-wq
  "Create a new running wq with n workers"
  [n]
  (let [chans (vec (repeatedly n make-chan))
	wq (WorkQueue. (make-chan)
		       (make-chan)
		       chans
		       (atom true)
		       (atom n))]
    (dotimes [n (count chans)]
      (let [c (nth chans n)]
	(.start (Thread. #(worker wq c) (str "WQ Worker " n)))
	(put-chan (:workers wq) c)))
    (.start (Thread. #(wq-controller wq) "WQ Controller"))
    wq))

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
