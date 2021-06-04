# SABD : Analisi del dataset delle vaccinazioni anti Covid-19

## Dataset
I file csv usati sono stati scaricati da [repository-covid-19](https://github.com/italia/covid19-opendata-vaccini/tree/master/dati)
il giorno 31/05/2021 e sono memorizzati nella cartella **/data**.

## Prerequisiti

1. Installare Spark, al [link](https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/) è disponibile un breve tutorial.
2. Installare maven: `apt install maven`
3. Installare java

## Modalità d'uso 


1. Far partire lo spark master eseguendo il comando `start-master.sh`
2. compilare il maven project con i seguenti comandi: 
    - `mvn compile; mvn package; mvn install`
3. fare eseguire il docker-compose: `~  cd dist/hdfs/; docker-compose up`
      per fare partire i container per l'HDFS
4. una volta ottenuto il jar **target/SABD-Project_1-1.0-SNAPSHOT.jar** a seguito del punto 3. è possibile eseguire gli script forniti nella cartella SABD-Project_1 dopo avergli concesso i permessi di esecuzione.
    - `~ ./query1.sh`
    - `~ ./query2.sh`
    - `~ ./main.sh`per eseguire sia la query1 che la query 2

Per usare il programma in modalità debug basta usare il flag "-D"
- `~ ./query1.sh -D`
- `~ ./query2.sh -D`
- `~ ./main.sh -D` 

I risultati verranno esportati sia su hdfs che su una cartella locale denominata Results. 
Per verificarne il contenuto dell'omologa cartella su HDFS: `hdfs dfs -ls /Results/query1` e`hdfs dfs -ls /Results/query2` 
e farne il cat `hdfs dfs -cat /Results/query1/csv-name`, `hdfs dfs -cat /Results/query1/csv-name`

I tempi di esecuzione vengono stampati a schermo per ciascuna delle queries

    