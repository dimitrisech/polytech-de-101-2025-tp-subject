# Commentaires sur le projet 
## Temps de travail
Le projet a été réalisé dans les 6h prévues + 2 heures à la maison (sachant que je fais le projet seul).

## Modification sur les colonnes
Pour les tables, j'ai modifié le nom pour CONSOLIDATE_STATION, il y avait la colonne "CAPACITTY" que j'ai remplacé par "CAPACITY".

Egalement pour les dates, il y avait CREATED_DATE de la table CONSOLIDATE_CITY et la colonne de même nom pour la table CONSOLIDATE_STATION_STATEMENT avec LAST_STATEMENT_DATE qui étaient des colonnes déclarées en var et non en date. J'ai donc modifié le type pour ces 3 colonnes. 

J'ai laissé la colonne stationid qui peut paraitre en trop mais peut être utile par la suite si des villes ont les mêmes id de stations, je pourrais après concaténer l'id avec la ville. 

## Résultats
Pour ce qui est du barème les quatres me semblent être respectés : 


- Les ingestions fonctionnent correctement et produisent des fichiers json localement.

- La consolidation est correctement réalisé avec les nouvelles ingérées.

L'agrégation des données est correctement réalisée et les requêtes SQL fonctionnent.

Le projet intègre pas seulement les données de la ville de Paris mais aussi les données d'une autre ville (ici Nantes). 

Si je devais mettre un regret ou une limite sur le projet je dirais que l'ingestion n'est pas assez généraliste pour traiter l'ensemble des villes en France.


## Commandes pour lancer le projet 

cd polytech-de-101-2025-tp-subject

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

python src/main.py

## lancer duckdb

duckdb data/duckdb/mobility_analysis.duckdb

## lister les tables
.tables

## les requêtes pour tester

-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');



-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;

## quitter duckdb
.q


