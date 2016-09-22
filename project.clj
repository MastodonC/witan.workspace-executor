(defproject witan.workspace-executor "0.2.6"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [com.rpl/specter "0.11.2"]
                 [com.taoensso/timbre "4.3.1"]
                 [aero "0.2.0"]
                 [rhizome "0.2.5"]
                 [prismatic/schema "1.1.2"]
                 [witan.workspace-api "0.1.20"]]
  :profiles {:uberjar {:aot :all}}
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]])
