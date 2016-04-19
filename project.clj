(defproject witan.workspace-executor "0.1.3-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [manifold "0.1.4"]
                 [rhizome "0.2.5"]
                 [com.taoensso/timbre "4.3.1"]
                 [prismatic/schema "1.1.0"]]
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]])
