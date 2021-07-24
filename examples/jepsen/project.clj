(defproject sdcons_jepsen "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :main sdcons_jepsen.core
  :native-path "libs"
  :jvm-opts [~(str "-Djava.library.path=libs/:" (System/getenv "LD_LIBRARY_PATH"))]
  :java-source-paths ["src/"]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.1.18"]
                 ]
  :repl-options {:init-ns sdcons_jepsen.core})

