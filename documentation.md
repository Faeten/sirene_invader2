L'exercice sur les requêtes demandé lundi est présent dans le projet git, il s'agit du fichier **exo1.txt**
# Documentation du Processus d'Indexation

## Introduction

Cette documentation décrit le processus d'indexation des données du fichier CSV **Sirene : Fichier StockEtablissement**.
Ce fichier CSV volumineux sera décomposer fichiers plus petit, puis indexer dans une base de données MongoDB.

## Pré-requis

Avant de commencer, il faut avoir :

- Node.js installé sur votre système.
- MongoDB opérationnel (local ou hébergé, par exemple MongoDB Atlas).
- Les dépendances nécessaires pour le script d'indexation (`csv-split-stream`, `mongoose`, `pm2`). en faisant un `npm i`

## Étapes de l'indexation
- Décomposition du CSV volumineux en plusieurs CSV léger.
- Connexion à la base de données à l'aide de **Mongoose**
- Lecture des fichiers : 
  - Conversion des données recherchées sur chaque lignes en modèle créé à l'aide de mongoose.schema
  - Sauvegarde des objets dans la base de données