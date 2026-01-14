# 🌍✈️ Climat & Tourisme — Trouver le meilleur moment pour voyager

Bienvenue dans le projet **Climat & Tourisme** !  
Ce projet a pour objectif de **recommander les meilleures périodes pour voyager** en s’appuyant sur des **données météorologiques fiables et objectives**.

Grâce à un **pipeline ETL automatisé avec Apache Airflow**, nous collectons, nettoyons et analysons des données météo issues de **OpenWeather** et **Meteostat**, afin de produire des indicateurs simples et exploitables pour le tourisme.

---

## 🌤️ Pourquoi ce projet ?
Quand on prépare un voyage, une question revient toujours :

> *Quel est le meilleur moment pour visiter une ville ?*

Plutôt que de se baser sur des impressions ou des moyennes approximatives, ce projet adopte une approche **data-driven**, fondée sur des critères météorologiques mesurables.

---

## 📊 Indicateurs utilisés
Les périodes recommandées reposent sur les critères suivants :

- 🌡️ **Température idéale** : entre **22°C et 28°C**
- 🌧️ **Faibles précipitations**
- 🌬️ **Vent modéré**
- ⭐ **Score météo journalier et mensuel** (**sur 20 points**)

---

## 🔄 Pipeline automatisé avec Apache Airflow
Le projet est orchestré à l’aide de **trois DAGs Apache Airflow** :

### 🧩 `etl_climat_dag`
- Récupération des données météo actuelles et des prévisions à 5 jours via l’API **OpenWeather**
- Nettoyage, transformation et export des données au format **CSV**

### 🕰️ `etl_climat_historique_dag`
- Collecte des données météorologiques historiques via **Meteostat**
- Nettoyage et stockage des données passées

### 🧠 `etl_climat_master_dag`
- Déclenche automatiquement les deux DAGs précédents
- Combine les données historiques et récentes
- Génère un **modèle en étoile (Star Schema)** prêt pour l’analyse

---

## 🛠️ Modules ETL
Le pipeline ETL repose sur des modules Python clairs et maintenables :

- **`extract.py`**  
  Extraction des données depuis OpenWeather et Meteostat

- **`transform.py`**  
  Nettoyage des données, calcul du score météo et création du modèle en étoile

- **`combine.py`**  
  Fusion des jeux de données historiques et récentes pour chaque ville

---

## ⭐ Calcul du score météo
Chaque journée se voit attribuer un **score entre 0 et 20**, calculé selon les critères suivants :

| Critère | Points max |
|-------|------------|
| 🌡️ Température idéale | 10 |
| 🌧️ Faible pluie | 5 |
| 🌬️ Vent modéré | 5 |
| **Total** | **20** |

Un score élevé indique des conditions particulièrement favorables au tourisme.

---

## 🧩 Modèle de données (Star Schema)

### 📌 Table de faits
- **`Weather.csv`**
  - Température
  - Précipitations
  - Vent
  - Score météo
  - `date_id`
  - `city_id`

### 📌 Tables de dimensions
- **`City.csv`** : informations sur les villes  
- **`Date.csv`** : calendrier détaillé (jour, mois, année, semaine, jour de la semaine)

Chaque mesure météo est reliée à **une ville** et **une date**, facilitant les analyses temporelles et comparatives.

---

## 📈 Analyses possibles
Une fois les données chargées et structurées, vous pouvez :

- 🧳 Identifier les **meilleurs mois pour visiter chaque ville**
- 🌍 Comparer les **conditions météo moyennes entre plusieurs villes**
- 📉 Visualiser l’**évolution du score météo dans le temps**
- 📊 Alimenter des dashboards ou outils de data visualisation

---

## 🧰 Technologies utilisées
- **Python**
- **Apache Airflow**
- **OpenWeather API**
- **Meteostat**
- **Modélisation de données (Star Schema)**
- **CSV**

---

✨ *Projet orienté Data Engineering & Analyse, appliqué au tourisme.*
