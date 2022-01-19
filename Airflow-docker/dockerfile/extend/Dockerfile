#FROM airflow-base
#COPY dags /root/airflow/dags
#RUN mkdir -p /home/etc
#COPY . /home/etc
#WORKDIR /home/etc
#RUN chmod u+x setup.sh
#RUN ./setup.sh

FROM airflow-1 
COPY dags /root/airflow/dags
COPY . /home/etc
WORKDIR /home/etc
RUN chmod u+x entrypoint.sh 
ENTRYPOINT ["./entrypoint.sh"]