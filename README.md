# Traitement et visualisation de données avec spark streaming
## Requis
pour tester ce environnement vous avez besoin d'installer docker 
et spark. Vous devez compiler le projet pour génerer le consomateur et le producteur

## Etape 1: Verfier que les services sont demarrer 

```bash
 docker compose ps
```
## Etape 2: Demarrer les services

```bash
docker compose up -d
```

## Etape 3: Créer le topic kafka

```bash

docker compose exec cp-kafka kafka-topics --create \
--topic nom_du_topic --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092

```
## Exemple de de creation de topic

```bash
docker compose exec cp-kafka kafka-topics --create \
--topic stations --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
```
## Verification de la création du topic
```bash
docker compose exec cp-kafka kafka-topics \
--bootstrap-server  localhost:9092 --list

```

## Lancer le consomateur de donnée

```bash
java -jar Consumer.jar nom_du_topic nom_base_de_donnée nom_utilisateur_bd mot_de_pass_bd

```
## Exemple de lancement
```bash
java -jar Consumer.jar stations sparkdb nene nene
```
## Lancer le producteur de donnée (lancer dans un nouveau terminal)
```bash
"Usage: Main <inputFile> <linesPerSegment>  <intervalSeconds>  <topicName>"
java -jar Producer.jar prix-carburants-quotidien.json 2000 30 stations

```