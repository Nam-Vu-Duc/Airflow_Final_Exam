
# Airflow Final Project Step By Step

## I. Prerequisites

Required software installed on your machine:

- IDE like Visual Studio Code or Pycharm
- Python 3.10 or higher (this Project is using Python 3.12.3)
- Docker Desktop

## II. Project Setup

1. Download this project folder.
3. Open project in VSCode/Pycharm.
4. Open Terminal, make sure the path is correctly pointing to the Project Path.
5. Run the following command to initialize a new image for airflow with added packages requirements:

```bash
docker build -t namvd13-airflow-image .
```

6. Run the following command to run docker containers:

```bash
docker compose up
```

7. Opening your browser at the URL [http://localhost:8080](http://localhost:8080).

If you see the Airflow UI, your project is set up and ready to run!
(If not, don't be worry, please wait for a few minutes for docker to init the airflow)

---

## III. Running DAG
1. Go to the **DAGs** page.

Then, find the DAG named **TuneStream_ETL** 
![Screenshot 2025-07-25 162835.png](images/Screenshot%202025-07-25%20162835.png)

Find the **Trigger** button anh click it.
![Screenshot 2025-07-25 162936.png](images/Screenshot%202025-07-25%20162936.png)

Wait until the DAG finishes running.
---

## IV. Results

![Screenshot 2025-07-25 135951.png](images/Screenshot%202025-07-25%20135951.png)

#### 1. Stage_songs tasks
Log from Airflow
![Screenshot 2025-07-25 155641.png](images/Screenshot%202025-07-25%20155641.png)

Query data from table

![Screenshot 2025-07-25 160218.png](images/Screenshot%202025-07-25%20160218.png)

#### 2. Stage_events task
Log from Airflow
![Screenshot 2025-07-25 155608.png](images/Screenshot%202025-07-25%20155608.png)

Query data from table
![Screenshot 2025-07-25 160148.png](images/Screenshot%202025-07-25%20160148.png)

#### 3. Load_songplays_fact_table task
Log from Airflow
![Screenshot 2025-07-25 155706.png](images/Screenshot%202025-07-25%20155706.png)

Query data from table
![Screenshot 2025-07-25 160105.png](images/Screenshot%202025-07-25%20160105.png)

#### 4. Load_user_dim_table task
Log from Airflow
![Screenshot 2025-07-25 155800.png](images/Screenshot%202025-07-25%20155800.png)

Query data from table
![Screenshot 2025-07-25 160303.png](images/Screenshot%202025-07-25%20160303.png)

#### 5. Load_artist_dim_table task
Log from Airflow
![Screenshot 2025-07-25 155812.png](images/Screenshot%202025-07-25%20155812.png)

Query data from table
![Screenshot 2025-07-25 160011.png](images/Screenshot%202025-07-25%20160011.png)

#### 6. Load_song_dim_table task
Log from Airflow
![Screenshot 2025-07-25 155820.png](images/Screenshot%202025-07-25%20155820.png)

Query data from table
![Screenshot 2025-07-25 160126.png](images/Screenshot%202025-07-25%20160126.png)

#### 7. Load_time_dim_table task
Log from Airflow
![Screenshot 2025-07-25 155730.png](images/Screenshot%202025-07-25%20155730.png)

Query data from table
![Screenshot 2025-07-25 160242.png](images/Screenshot%202025-07-25%20160242.png)

#### 8. Run_data_quality_checks task
Log from Airflow
![Screenshot 2025-07-25 155906.png](images/Screenshot%202025-07-25%20155906.png)
