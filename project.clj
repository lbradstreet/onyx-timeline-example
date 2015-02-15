(defproject onyx-timeline-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :jvm-opts ["-Xmx4g" "-server"] 

  :source-paths ["src/clj" "src/cljs"]
  :test-paths ["test/clj"]

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [prismatic/schema "0.3.7"]
                 [org.clojure/clojurescript "0.0-2850"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/tools.logging  "0.3.1"]
                 [ring/ring-defaults  "0.1.4"]
                 [com.cognitect/transit-clj "0.8.259"]
                 [com.cognitect/transit-cljs "0.8.205"]
                 [com.stuartsierra/component "0.2.2"]
                 [com.twitter/hbc-core "2.2.0"
                  :exclusions [commons-codec
                               com.google.guava/guava
                               org.apache.httpcomponents/httpclient]]
                 [potemkin "0.3.11"]
                 [com.mdrogalis/onyx "0.5.1"]
                 [com.mdrogalis/onyx-core-async "0.5.0"]
                 [com.mdrogalis/lib-onyx "0.5.0"]
                 [com.taoensso/sente "1.3.0"]
                 [cheshire "5.4.0"]
                 [ring "1.3.2"]
                 [compojure "1.3.1"]
                 [enlive "1.1.5"]
                 [org.omcljs/om "0.8.8"]
                 [figwheel "0.2.3-SNAPSHOT"]
                 [environ "1.0.0"]
                 [com.cemerick/piggieback "0.1.5"]
                 [weasel "0.5.0"]
                 [leiningen "2.5.1"]
                 [http-kit "2.1.19"]
                 [racehub/om-bootstrap "0.4.0" :exclusions [om]]
                 [prismatic/om-tools "0.3.10" :exclusions [om]]]

  :plugins [[lein-cljsbuild "1.0.4"]
            [lein-environ "1.0.0"]]

  :main onyx-timeline-example.server

  :min-lein-version "2.5.0"

  :uberjar-name "onyx-timeline-example.jar"

  :cljsbuild {:builds {:app {:source-paths ["src/cljs"]
                             :compiler {:output-to     "resources/public/js/app.js"
                                        :output-dir    "resources/public/js/out"
                                        :source-map    "resources/public/js/out.js.map"
                                        :preamble      ["react/react.min.js"
                                                        ;"public/twitter/widgets.js"
                                                        ]
                                        :externs       ["react/externs/react.js"
                                                        "resources/public/twitter/widgets.js"]
                                        :optimizations :none
                                        :pretty-print  true}}}}

  :profiles {:dev {:repl-options {:init-ns onyx-timeline-example.server
                                  :nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}
                   :hooks [leiningen.cljsbuild]
                   :plugins [[lein-figwheel "0.2.3-SNAPSHOT"]]
                   :figwheel {:http-server-root "public"
                              :port 3449
                              :css-dirs ["resources/public/css"]}
                   :env {:is-dev true}
                   :cljsbuild {:builds {:app {:source-paths ["env/dev/cljs"]}}}}

             :uberjar {:hooks [leiningen.cljsbuild]
                       :env {:production true}
                       :omit-source true
                       :aot :all
                       :cljsbuild {:builds {:app
                                            {:source-paths ["env/prod/cljs"]
                                             :compiler
                                             {:optimizations :advanced
                                              :pretty-print false}}}}}})
