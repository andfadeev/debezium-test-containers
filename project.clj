(defproject debezium-test-containers "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [io.debezium/debezium-testing-testcontainers "2.4.1.Final"]

                 [com.github.seancorfield/next.jdbc "1.3.894"]
                 [org.postgresql/postgresql "42.2.10"]
                 [com.github.seancorfield/honeysql "2.5.1091"]

                 [cheshire "5.12.0"]
                 [fundingcircle/jackdaw "0.9.11"]

                 [org.slf4j/slf4j-api "2.0.9"]
                 [org.slf4j/slf4j-simple "2.0.9"]
                 [org.apache.kafka/kafka-streams-test-utils "3.6.0"]

                 [org.testcontainers/testcontainers "1.19.3"]
                 [org.testcontainers/kafka "1.19.3"]
                 [org.testcontainers/postgresql "1.19.3"]]
  :main ^:skip-aot debezium-test-containers.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
