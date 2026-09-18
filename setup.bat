@echo off
REM =============================================================================
REM FUEL PIPELINE — POST-COMPOSE SETUP
REM =============================================================================
REM Run this ONCE, after `docker compose up -d` has finished and all 8
REM containers show "Up" in `docker ps`. This does everything that can't be
REM expressed as container config alone: fixes HDFS write permissions,
REM installs the two Kafka Connect plugins, creates the MySQL table and the
REM Kafka topic, and submits both sink connectors.
REM
REM Re-running this script is safe (idempotent) except for the two
REM confluent-hub install steps, which are slow (~1-2 min each) even when
REM the plugin is already installed.
REM =============================================================================

echo.
echo [1/8] Waiting 30s for containers to finish starting...
timeout /t 30 /nobreak

echo.
echo [2/8] Fixing HDFS write permissions (this is the #1 cause of silent HDFS
echo       write failures if skipped)...
docker exec namenode hdfs dfs -mkdir -p /flights
docker exec namenode hdfs dfs -mkdir -p /logs
docker exec namenode hdfs dfs -chown appuser:supergroup /flights
docker exec namenode hdfs dfs -chown appuser:supergroup /logs
docker exec namenode hdfs dfs -chmod 755 /flights
docker exec namenode hdfs dfs -chmod 755 /logs
docker exec namenode hdfs dfs -ls /

echo.
echo [3/8] Creating the MySQL flights table...
docker exec mysql mysql -uflightuser -pflightpass flights_db -e "CREATE TABLE IF NOT EXISTS flights (flight_id VARCHAR(64) PRIMARY KEY, fleet VARCHAR(32), great_circle_distance_nm DOUBLE, gross_weight_at_liftoff_kg DOUBLE, max_tailwind_during_takeoff_kt DOUBLE, generated_at TIMESTAMP);"
docker exec mysql mysql -uflightuser -pflightpass flights_db -e "SHOW TABLES;"

echo.
echo [4/8] Installing the HDFS sink connector plugin (slow, ~1-2 min)...
docker exec kafka-connect confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:latest

echo.
echo [5/8] Installing the JDBC sink connector plugin (slow, ~1-2 min)...
docker exec kafka-connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest

echo.
echo [6/8] Copying the MySQL driver into the JDBC connector's lib folder
echo       (Confluent doesn't bundle this driver for licensing reasons)...
docker cp mysql-connector-j.jar kafka-connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j.jar

echo.
echo [7/8] Restarting Kafka Connect to load the new plugins, then waiting
echo       90s for it to fully come back up...
docker restart kafka-connect
timeout /t 90 /nobreak

echo.
echo Checking Connect is responding...
curl http://localhost:8083/connectors

echo.
echo [8/8] Creating the Kafka topic and submitting both sink connectors...
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic flights --partitions 3 --replication-factor 2 --bootstrap-server localhost:19092

curl -X POST -H "Content-Type: application/json" --data @hdfs-sink-config.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @mysql-sink-config.json http://localhost:8083/connectors

echo.
echo Done. Verify both connectors are RUNNING:
curl http://localhost:8083/connectors/hdfs-flights-sink/status
curl http://localhost:8083/connectors/mysql-flights-sink/status

echo.
echo =============================================================================
echo Setup complete. Next: start the Django API (see README.md, Step 3) and
echo run the KDE generator loop to begin producing data.
echo =============================================================================
pause
