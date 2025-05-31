## Airflow-spark Integration

This includes spark integration with airflow using ssh connection to submit the spark-jobs.
Run the 'docker-compose.yml' to run the services.  


## Initializing postgres db for airflow services  

```
docker compose up airflow-init  
```

Now You can start all its services,  

```
docker compose up  
```




## setting authentincation for post request to airflow api



Update auth_backends in airflow.cfg:
Locate your airflow.cfg file. This file typically resides within your AIRFLOW_HOME directory. 
Open the file in a text editor.
Find the [api] section and the auth_backends setting. 
Add airflow.api.auth.backend.session to the list of authentication backends. For example: 
Code


[api]


auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session 


## Setup ssh connection for airflow to submit spark-jobs using ssh connection



I used SSHOperator cause spark-submit operator for some reason isn't working as it should be.

The custom spark-master image 'custom-spark-master:3.5.5' image used here has already been configured with ssh package with followinf credentials.  



All thats left to do is setup following configs in airflow ui.  

```
connection type : ssh  
ssh_conn_id : spark_master_ssh  
Host : spark-master  
login: root  
password : sparkpass  
port : 22   

```