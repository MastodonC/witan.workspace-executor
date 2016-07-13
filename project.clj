(defproject witan.workspace-executor "0.1.4-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.onyxplatform/onyx "0.9.4"]
                 [com.rpl/specter "0.11.2"]
                 [com.taoensso/timbre "4.3.1"]
                 [com.taoensso/carmine "2.12.2" :exclusions [com.taoensso/timbre com.taoensso/encore]]
                 [aero "0.2.0"]
                 [manifold "0.1.4"]
                 [rhizome "0.2.5"]
                 [prismatic/schema "1.1.0"]
                 [witan.workspace-api "0.1.8"]]
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]])
