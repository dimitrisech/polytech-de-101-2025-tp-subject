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

Si je devais mettre un regret ou une limite sur le projet je dirais que l'ingestion n'est pas assez généraliste pour traiter l'ensemble des villes. 

