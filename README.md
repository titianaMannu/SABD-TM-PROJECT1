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
  > il JAR deve essere generato poiché la piattaforma non ne permette l'upload a causa delle sue dimensioni corrispondenti a
  &asymp; 108MB   
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

## DAG query1 
Il grafico viene preso dal report del job disponibile in: **SABD-TIZIANA-MANNUCCI-PROJECT - Details for Job query1.pdf**

![DAG-query1](./Images/DAG%20query1.png)    

## DAG query2
Il grafico viene preso dal report del job disponibile in: **SABD-TIZIANA-MANNUCCI-PROJECT - Details for Job query2.pdf**

![DAG-query1](./Images/DAG%20query2.png)

## Architettura per il test

I test sono stati effettuati su una macchina con le seguenti specifiche:

- CPU: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz sbloccato (up to 4900MHz)
- Motherboard: MPG Z390 GAMING PRO CARBON AC 
- RAM: 16GB DIMM DDR4 Synchronous 3200 MHz (0.3 ns) CMK16GX4M2B3200C16 a 64bit
- Scheda grafica: MSI RTX 2070 super ventus

per ulteriori dettagli vedere il  [report](lshw.html)