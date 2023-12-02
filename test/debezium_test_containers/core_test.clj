(ns debezium-test-containers.core-test
  (:require [clojure.test :refer :all]
            [debezium-test-containers.core :refer :all]
            [next.jdbc :as jdbc]
            [honey.sql :as sql]
            [cheshire.core :as json]
            [jackdaw.test.commands :as cmd]
            [jackdaw.serdes :as js]
            [jackdaw.test :as jdt]
            [jackdaw.test :refer [test-machine]])
  (:import (io.debezium.testing.testcontainers Connector$State ConnectorConfiguration DebeziumContainer)
           (org.postgresql.util PGobject)
           (org.testcontainers.containers KafkaContainer Network PostgreSQLContainer)
           (org.testcontainers.lifecycle Startable Startables)
           (org.testcontainers.utility DockerImageName)))

;; JDBC connector vs Debezium: must see: https://www.confluent.io/en-gb/events/kafka-summit-london-2022/jdbc-source-connector-what-could-go-wrong/

; https://debezium.io/documentation/reference/stable/integrations/testcontainers.html
; https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
; https://debezium.io/documentation/reference/stable/connectors/postgresql.html
; https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/
; For Java Quarkus users - https://debezium.io/documentation/reference/stable/integrations/outbox.html

; Outbox diagram: https://debezium.io/documentation/reference/stable/_images/outbox_pattern.png

; this repo: https://github.com/andfadeev/debezium-test-containers


(def DEBEZIUM_OUTBOX_CONNECTOR
  "debezium-outbox-connector")

(defn ->jsonb
  [x]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/encode x))))

(def network (Network/newNetwork))

(def kafka-test-container (-> (KafkaContainer.)
                               (.withNetwork network)))

(def postgres-test-container
  (-> (DockerImageName/parse "quay.io/debezium/postgres:15")
      (.asCompatibleSubstituteFor "postgres")
      (PostgreSQLContainer.)
      (.withNetwork network)
      (.withNetworkAliases (into-array String ["postgres"]))))

(def debezium-test-container
  (-> (DebeziumContainer. "quay.io/debezium/connect:2.4.1.Final")
      (.withNetwork network)
      (.withKafka kafka-test-container)))

(defn start-containers
  []
  (println "Starting all test containers")
  (.get (Startables/deepStart [kafka-test-container
                               postgres-test-container
                               debezium-test-container]))
  (println "All test containers are started"))

(def with-test-containers
  (fn [f]
    (try
      (start-containers)
      (f)
      (finally
        (println "Stopping all test containers")
        (doseq [^Startable c [kafka-test-container
                              postgres-test-container
                              debezium-test-container]]
          (.stop c))
        (println "All test containers are stopped")))))


(use-fixtures :each with-test-containers)

(defn get-datasource
  []
  (println {:jdbcUrl (.getJdbcUrl postgres-test-container)
            :username (.getUsername postgres-test-container)
            :password (.getPassword postgres-test-container)})
  (jdbc/get-datasource
    {:jdbcUrl (.getJdbcUrl postgres-test-container)
     :user (.getUsername postgres-test-container)
     :password (.getPassword postgres-test-container)}))

(defn create-outbox-table
  [ds]
  (jdbc/execute!
    ds
    ["
CREATE TABLE outbox  (
  id UUID PRIMARY KEY,
  aggregatetype VARCHAR(255) NOT NULL,
  aggregateid VARCHAR(255) NOT NULL,
  type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);"]))

(defn outbox-insert!
  [ds event]
  (let [event (update event :payload ->jsonb)]
    (->> {:insert-into [:outbox]
          :values [event]}
         (sql/format)
         (jdbc/execute! ds))))

(defn register-debezium-outbox-connector!
  []
  (let [connector-config
        (-> (ConnectorConfiguration/forJdbcContainer postgres-test-container)
            (.with "topic.prefix" "app")
            (.with "time.precision.mode" "connect")

            (.with "transforms" "outbox")
            (.with "transforms.outbox.type" "io.debezium.transforms.outbox.EventRouter")
            (.with "transforms.outbox.table.expand.json.payload" "true")

            (.with "transforms.outbox.table.fields.additional.placement" "type:header,created_at:header")

            ;; todo: find a good way to sync test connector configuration with the real one (usually defined in Terraform etc)

            )]
    (.registerConnector debezium-test-container
                        DEBEZIUM_OUTBOX_CONNECTOR
                        connector-config)))

(defn topic-config
  [topic-name]
  {:topic-name topic-name
   :key-serde (js/string-serde)
   :value-serde (js/string-serde)
   :partition-count 1
   :replication-factor 1})

(defn parse-headers
  [headers]
  (update-vals headers
               (fn [h]
                 (String. h))))

(deftest debezium-outbox-end-to-end-test
  (let [ds (get-datasource)
        payload {:item "Laptop"
                 :price "1350"}]
    (create-outbox-table ds)
    (register-debezium-outbox-connector!)

    (.ensureConnectorRegistered debezium-test-container DEBEZIUM_OUTBOX_CONNECTOR)
    (.ensureConnectorState debezium-test-container
                           DEBEZIUM_OUTBOX_CONNECTOR
                           Connector$State/RUNNING)
    (.ensureConnectorTaskState debezium-test-container
                               DEBEZIUM_OUTBOX_CONNECTOR
                               0
                               Connector$State/RUNNING)

    ;; above calls should already ensure that connector is running
    (is (= [DEBEZIUM_OUTBOX_CONNECTOR]
           (.getRegisteredConnectors debezium-test-container)))
    (is (= Connector$State/RUNNING
           (.getConnectorState debezium-test-container
                               DEBEZIUM_OUTBOX_CONNECTOR)))
    (is (= Connector$State/RUNNING
           (.getConnectorTaskState debezium-test-container
                                   DEBEZIUM_OUTBOX_CONNECTOR
                                   0)))

    (outbox-insert! ds {:id (random-uuid)
                        :aggregatetype "Order"
                        :aggregateid (str (random-uuid))
                        :type "OrderCreated"
                        :payload {:item "Laptop"
                                  :price "1350"}})

    (is (some? (jdbc/execute-one! ds (-> {:select :*
                                          :from :outbox}
                                         (sql/format)))))

    (with-open [machine (test-machine
                          (jdt/kafka-transport
                            {"bootstrap.servers" (.getBootstrapServers kafka-test-container)
                             "group.id" (str "test-machine-" (random-uuid))
                             "auto.offset.reset" "earliest"}
                            {:output-topic (topic-config "outbox.event.Order")}
                            ))]

      (let [watch-cmd (cmd/watch
                        (fn [journal]
                          (let [output (->> (get-in journal [:topics :output-topic])
                                            (remove nil?))]
                            (when (= 1 (count output))
                              output)))
                        {:timeout 10000})
            {:keys [results _journal]} (jdt/run-test machine [watch-cmd])
            [watch-result] results

            kafka-record (-> watch-result :result :info first)]

        ;(is (= [] kafka-record))
        ;(is (= [] (parse-headers (:headers kafka-record))))
        (is (= payload (json/decode (:value kafka-record) true)))))))
