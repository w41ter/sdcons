; Copyright 2021 The sdcons Authors.
; 
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
; 
;     http://www.apache.org/licenses/LICENSE-2.0
; 
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(ns sdcons_jepsen.core
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]
             [nemesis :as nemesis]
             [independent :as independent]
             [checker :as checker]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model])
  (:import  [sdcons KvClient
               KvClient$TimeoutException
               KvClient$KeyNotFoundException
               KvClient$CasException
               KvClient$FailedException]
            [java.util HashMap]
            [java.lang Integer]
            [java.lang String]))

(def process-image "kv-server")
(def election_timeout_seconds 6)

(defn get-output-name [wd] (str wd "/OUTPUT"))
(defn get-pid-name [wd] (str wd "/PID"))
(defn get-binary-image [wd] (str wd "/" process-image))
(defn get-config-file [wd] (str wd "/config.toml"))

(defn setup_env
  "setup sdcons kv server process image"
  [opts]
  (info "setup local env: " opts))

(defn teardown_env
  "teardown sdcons kv server env"
  []
  (info "teardown local env"))

(defn prepare_server_env
  "prepare sdcons kv server env"
  [node]
  (info node "setup env"))

(defn endpoint
  [node port]
  (str (name node) ":" port))

(defn node-id
  [test node]
  (get (:routers test) (keyword node)))

(defn get-working-dir
  [test node]
  (str (:working-dir test) "/server/" (node-id test node)))

(defn get-root-dir
  [test node]
  (str (:working-dir test)))

(defn server-address
  [node]
  (endpoint node 8000))

;; (defn sdcons_address
;;   [node]
;;   (endpoint node 8001))

;; (defn snapshot_address
;;   [node]
;;   (endpoint node 8002))

;; (defn clusters
;;   [test]
;;   (->> (:nodes test)
;;        (map (fn [node]
;;               (str (node-id test node) "," (raft_address node) "," (snapshot_address node))))
;;        (str/join ",")))

(defn servers
  [test]
  (into {} (->> (:nodes test)
                (map (fn [node]
                       [(keyword (node-id test node)) (server-address node)]))
                vec)))

(defn cast-to-java-map
  [map]
  (let [result (new HashMap)]
    (do
      (doseq [[k v] map]
        (let [id (Integer/parseInt (name k))]
          (.put result id v)))
      result)))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn kv-get
  [client key]
  (try
    (parse-long (.Get client (String/valueOf key)))
    (catch KvClient$KeyNotFoundException e nil)))

(defn kv-write
  [client key value]
  (.Put client (String/valueOf key) (String/valueOf value)))

(defn kv-cas
  [client key value expect]
  (try
    (do (.PutWithExpect client
                        (String/valueOf key)
                        (String/valueOf value)
                        (String/valueOf expect)) true)
    (catch KvClient$CasException e false)))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (do
      (info "start connect to node" (str (node-id test node)))
      (let [routers (cast-to-java-map (servers test))
            c       (KvClient. routers)]
        (info "connect success, routers " routers ", conn " c)
        (assoc this :conn c))))

  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)
          conn (:conn this)]
      (try
        (case (:f op)
          :read (let [value (kv-get conn k)]
                  (assoc op :type :ok, :value (independent/tuple k value)))
          :write (do (kv-write conn k v)
                     (assoc op :type :ok))
          :cas (let [[expect value] v]
                 (assoc op :type (if (kv-cas conn k value expect)
                                   :ok
                                   :fail))))
        (catch KvClient$TimeoutException e
          (assoc op :type (if (= :read (:f op)) :fail :info)
                 :error :timeout))
        (catch KvClient$FailedException e
          (assoc op :type (if (= :read (:f op)) :fail :info)
                 :error (str e))))))

  (teardown! [this test])

  (close! [_ test]))

(defn start-kvserver-on-node [node test]
  (let [working-dir (get-working-dir test node)]
    (cu/start-daemon!
     {:logfile (get-output-name working-dir)
      :pidfile (get-pid-name working-dir)
      :chdir   working-dir
      :make-pidfile :true}
     (get-binary-image (get-root-dir test node))
      :--config (get-config-file working-dir))))

(defn start-kvserver [node test]
  (info "start node " node)
  (c/on node
        (start-kvserver-on-node node test)))

(defn stop-kvserver [node test]
  (info "stop node " node)
  (c/on node
        (cu/stop-daemon! (get-pid-name (get-working-dir test node)))
        (c/exec :sleep election_timeout_seconds)))

(defn setup_db
  "sdcons kv server"
  [working-dir]
  (reify db/DB
    (setup! [_ test node]
      (c/su (c/exec :rm :-rf (str (get-working-dir test node) "/wal"))
            (c/exec :rm :-rf (str (get-working-dir test node) "/data"))
            (start-kvserver-on-node node test)
            (info node "sleep " (* 2 election_timeout_seconds) " secs for election")
            (c/exec :sleep (* 2 election_timeout_seconds)))
      (info node "setup success"))

    (teardown! [_ test node]
      (info node "tearing down sdcons")
      (cu/stop-daemon! (get-binary-image (get-root-dir test node)) (get-pid-name (get-working-dir test node))))))
      ; (c/su (c/exec :rm :-rf root_dir)))

    ;; db/LogFiles
    ;; (log-files [_ test node]
    ;;   [output_name log_name])))

(defn setup_checker []
  (checker/compose
   {:perf  (checker/perf)
    :indep (independent/checker
            (checker/compose
             {:linear (checker/linearizable
                       {:model (model/cas-register)
                        :algorithm :linear})
              :timeline (timeline/html)}))}))

(defn nemesis_keyword [nemesis method]
  (keyword (str (name nemesis) "-" (name method))))

(defn nemesis_seq
  [methods]
  (->> methods
       (map (fn [method]
              [(gen/sleep election_timeout_seconds)
               {:type :info, :f (nemesis_keyword method :start)}
               (gen/sleep (* election_timeout_seconds 5))
               {:type :info, :f (nemesis_keyword method :stop)}]))
       flatten
       vec))

(defn setup_generator [opts]
  (->> (independent/concurrent-generator
        (:num-keys opts)
        (range)
        (fn [k]
          (->>
           (gen/mix [r w cas])
           (gen/stagger (:rate opts))
           (gen/limit (:ops-per-key opts)))))
       (gen/nemesis
        (gen/seq (cycle (nemesis_seq [:partition
                                      :partition-node
                                      :crash
                                      :bridge
                                      :clock-scrambler]))))
       (gen/time-limit (:time-limit opts))))

(defn quorum-small-nonempty-subset
  [xs]
  (-> xs
      count
      (/ 2)
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))))

(defn crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (nemesis/node-start-stopper
   quorum-small-nonempty-subset
   (fn start [test node] (stop-kvserver node test) [:killed node])
   (fn stop  [test node] (start-kvserver node test) [:restarted node])))

(defn bridge-nemesis
  []
  (nemesis/partitioner (comp nemesis/bridge shuffle)))

(defn nemesis_config [name]
  {(nemesis_keyword name :start) :start
   (nemesis_keyword name :stop) :stop})

(defn setup_nemesis []
  (nemesis/compose
   {(nemesis_config :partition) (nemesis/partition-random-halves)
    (nemesis_config :partition-node) (nemesis/partition-random-node)
    (nemesis_config :clock-scrambler) (nemesis/clock-scrambler election_timeout_seconds)
    (nemesis_config :crash) (crash-nemesis)
    (nemesis_config :bridge) (bridge-nemesis)}))

(defn setup_test [opts]
  (setup_env opts)
  (merge tests/noop-test
         opts
         {:name "sdcons"
          :os debian/os
          :db (setup_db (:working-dir opts))
          :client (Client. nil)
          :nemesis (setup_nemesis)
          :checker (setup_checker)
          :generator (setup_generator opts)}))

(defn sdcons-test
  [opts]
  (try
    (setup_test opts)
    (finally (teardown_env))))

(defn parse-node
  [node]
  (let [res (str/split node #" ")]
    (when-not (= (count res) 2)
      (throw (IllegalArgumentException.
              (str "node format should like '1 endpoint' but got " node))))
    [(str (get res 0))
     (str (get res 1))]))

(defn last-element
  [v]
  (get v 1))

(defn make-pair
  [v]
  [(keyword (get v 1)) (get v 0)])

(defn parse-nodes
  [parsed]
  (let [nodes (:nodes (:options parsed))
        parsed-nodes (->> nodes
                          (map parse-node)
                          vec)
        ; convert to [e1 e2 e3]
        new-nodes    (->> parsed-nodes
                          (map last-element)
                          vec)
        ; convert to {:e1 i1, :e2 i2}
        routers      (into {} (->> parsed-nodes
                                   (map make-pair)
                                   vec))]
    (assoc parsed :options (-> (:options parsed)
                               (assoc :routers routers)
                               (assoc :nodes new-nodes)))))

(def cli-opts
  "Additional command line options.
  options:
   --time-limit 60          # test execute time
   --concurrency 10         # test concurreny, must large or equals to 10
   "
  [[nil "--working-dir DIR" "The working dir of this server"]
   [nil "--num-keys NUM" "Num of keys to test"
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  (/ 1 10)
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (info :opts args)
  (println (. System getProperty "java.library.path"))
  (cli/run! (merge (cli/single-test-cmd {:test-fn sdcons-test
                                         :opt-fn  parse-nodes
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
