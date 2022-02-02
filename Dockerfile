FROM ubuntu:latest
RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip3 install pandas
RUN pip3 install pymongo
RUN pip3 install APScheduler
WORKDIR /home
COPY ./etl_asr.py ./etl_asr.py
COPY ./etl_brs.py ./etl_brs.py
COPY ./etl_frs.py ./etl_frs.py
COPY ./etl_trs.py ./etl_trs.py
COPY ./utils.py ./utils.py
COPY ./run.py ./run.py
COPY ./dbconfig.json ./dbconfig.json
RUN chmod 777 ./run.py
CMD ["./run.py"]