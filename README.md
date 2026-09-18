# A Big Data Pipeline for Predicting Aircraft Fuel Consumption

Big Data Essentials — Group Final Exam Project

## What this project does

A complete, working big data pipeline that predicts aircraft fuel burn from
distance, takeoff weight, and takeoff tailwind. Because the real flight data
behind this project is confidential, it is never shared directly: instead, a
kernel density estimate (KDE) is fitted to the real data's shape and used to
generate realistic *synthetic* flights continuously. Those synthetic flights
flow through a full streaming architecture — Kafka, dual storage (HDFS +
MySQL), a Spark MLlib model trained on the real historical data, and a live
dashboard — without the running system ever needing the original data again.

Full methodology, results, and honest limitations are in `report/`.

## Architecture

```
KDE generator --> Django REST API --> Kafka (3 brokers, keyed by fleet)
                                          |
                                    Kafka Connect
                                    /            \
                              HDFS               MySQL
                        (historical store)   (operational store)
                                    \            /
                              Spark batch scoring job
                              (reads HDFS, applies the
                               trained model, writes
                               predictions to MySQL)
                                          |
                                   Django dashboard
```

A separate custom Kafka consumer (`flight_notifier.py`) reads the same
stream independently, flagging unusually long or heavy flights in real time
— this is the "custom business logic / notifications" branch, run
separately from Kafka Connect's storage job.

## Repository structure

```
docker-compose.yml         Brings up all 8 infrastructure containers
setup.bat                  Post-compose setup (HDFS permissions, Connect
                            plugins, topic creation, connector submission)
hdfs-sink-config.json      Kafka Connect -> HDFS sink configuration
mysql-sink-config.json     Kafka Connect -> MySQL sink configuration
mysql-connector-j.jar      MySQL JDBC driver (not bundled with Connect)

batch_scoring.py           Spark job: HDFS -> trained model -> MySQL
pyspark_insights_docker.py Spark job: reads HDFS, produces business insights

KDE_Generator.ipynb        Fits the per-fleet KDE, produces kde_models_3d.pkl
spark_fuel_model.ipynb     Trains the Spark MLlib model, produces the
                            coefficients used in batch_scoring.py
Data_quality_check.ipynb   Data cleaning: duplicates, sensor artifact,
                            missing-value handling
data_distribution.ipynb    Distribution plots used in the report (Figure 1)
flight_notifier.ipynb      The custom Kafka consumer / notable-flight logic

kde_models_3d.pkl          Fitted KDE (the actual artifact the live
                            generator loads — see note below)
spark_fuel_models.pkl      Trained model coefficients (used by
                            batch_scoring.py)

data_api/                  Django project: REST API, Kafka producer,
                            dashboard (flights/templates/flights/dashboard.html)

data_set.xlsx              Structure-only sample of the raw merged data
model_df_clean.xlsx        Structure-only sample of the cleaned dataset
                            (see note on confidentiality below)

images/                    Supporting figures
fuel_vs_distance.png       Report Figure 2 (real fuel burn vs. distance)

report/                    The 8-page report (PDF/DOCX + LaTeX source)
slides/                    Presentation deck
startup_checklist.txt      Quick command reference for day-to-day use
                            once the system is already set up once
```

## A note on the data

`data_set.xlsx` and `model_df_clean.xlsx` are **structure-only samples** —
they show the real dataset's column layout, not its full confidential
content. This is deliberate and consistent with the project's own design:
the whole point of the KDE generator is that the live pipeline never needs
the real data again once it has been fitted once.

One consequence worth knowing: the notebooks (`KDE_Generator.ipynb`,
`spark_fuel_model.ipynb`, etc.) show *how* `kde_models_3d.pkl` and
`spark_fuel_models.pkl` were originally built, but re-running them against
the small sample files here will not reproduce those exact artifacts. The
`.pkl` files themselves are the real, already-fitted results the live
pipeline actually uses — the notebooks are documentation of the process,
not meant to be re-executed from this folder.

## How to run it

**1. Start the infrastructure**

```
docker compose up -d
setup.bat
```

`setup.bat` fixes HDFS permissions, installs the two Kafka Connect plugins,
creates the MySQL table, creates the Kafka topic, and submits both sink
connectors. Takes a few minutes; only needs running once per fresh setup.

**2. Start the Django API**

```
cd data_api
python -m venv myenv
myenv\Scripts\activate
pip install django djangorestframework kafka-python mysql-connector-python numpy pandas openpyxl
python manage.py migrate
python manage.py runserver
```

**3. Generate data**

Open `KDE_Generator.ipynb`, load `kde_models_3d.pkl`, and run the
continuous generation loop. Flights will start flowing through the whole
pipeline within seconds.

**4. View the dashboard**

```
http://localhost:8000/api/dashboard/
```

**5. (Optional) Run the custom notification consumer**

```
python flight_notifier.py
```

Run it as two separate processes with the same group ID to see Kafka
reassign partitions live between them.

**6. (Optional) Re-run the batch scoring / insights jobs**

```
docker run --rm --network kafka-net -v "%cd%:/scripts" apache/spark-py:latest /opt/spark/bin/spark-submit --jars /scripts/mysql-connector-j.jar /scripts/batch_scoring.py

docker run --rm --network kafka-net -v "%cd%:/scripts" apache/spark-py:latest /opt/spark/bin/spark-submit /scripts/pyspark_insights_docker.py
```

## Key results

- Distance elasticity: a 10% longer flight burns roughly 7% more fuel,
  consistent across two independently fitted models.
- RMSE 519 kg / MAE 350 kg (narrow-body group), RMSE 1,014 kg / MAE 791 kg
  (wide-body group) on held-out test data.
- 3,587 historical flights scored by the batch pipeline in one run.

Full results, figures, and discussion are in `report/`.

## Known limitations

Covered honestly in the report's Limitations section: a thin sample for
one aircraft group, a weak tailwind signal (only takeoff/descent-phase
wind was available), a single-node HDFS setup with no replication, and
batch rather than continuous streaming scoring (explicitly permitted by
the assignment brief).
