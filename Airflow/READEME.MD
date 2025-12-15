# start airflow 1st time
docker-compose up -d --no-deps airflow-init

#start airflow server

docker-compose up -d 

docker-compose up -d --force-recreate

#build airflow

docker-compose up -d --build

#Tutorials


https://github.com/astronomer/airflow-databricks-use-case/blob/main/dags/tutorial_dag.py

https://docs.databricks.com/aws/en/jobs/how-to/use-airflow-with-jobs

https://www.astronomer.io/docs/learn/connections/databricks

https://www.youtube.com/watch?v=zOji_adf6Iw