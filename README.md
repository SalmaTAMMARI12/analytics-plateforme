# NeoBank – Plateforme Lakehouse bancaire sur Databricks

Pipeline de données de bout en bout (SQL Server et fichiers CSV → Bronze → Silver → Gold → dashboard et Genie), piloté par des **métadonnées** : un seul code d'ingestion pour toutes les tables, configuré par des tables de configuration.
 
---

## 1. Contexte et problème

NeoBank (banque fictive) produit des données dans plusieurs systèmes :

- un **système bancaire central** (SQL Server) : agences, clients, comptes, transactions ;
- des **passerelles de paiement** (fichiers CSV) : journaux de chaque paiement ;
- un **bureau de crédit externe** (fichiers CSV) : score de crédit et niveau de risque.

| Problème | Conséquence |
|---|---|
| Données dispersées dans des silos | Pas de vue unique du client |
| Pipelines manuels, un script par table | Lent, coûteux à maintenir, difficile à faire évoluer |
| Pas d'analyse en libre-service | Les équipes métier dépendent de l'équipe data |

## 2. Solution

1. **Architecture Lakehouse en médaillon** (Bronze / Silver / Gold) sur Delta Lake.
2. **Framework piloté par métadonnées** : le mode de chargement, la clé et le watermark de chaque table sont déclarés dans des tables de configuration, pas dans le code.
3. **Orchestration, audit et notification** : Databricks Jobs, table d'audit, e-mail récapitulatif.
4. **Consommation en libre-service** : dashboard à 4 pages et agent Genie.

## 3. Architecture
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/b8cfce18-e3b4-493c-ab90-29a1c31e4580" />

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/aea9236b-6805-4deb-bbfb-49586419a6d2" />


### Orchestration (job maître)
Les deux ingestions s'exécutent en parallèle. La couche Gold démarre quand elles sont terminées, puis le dashboard est rafraîchi et l'e-mail est envoyé.
<img width="950" height="510" alt="image" src="https://github.com/user-attachments/assets/0aa32664-61e4-456c-a4f4-382590b84ae2" />
<img width="746" height="406" alt="image" src="https://github.com/user-attachments/assets/d0a83488-a454-4768-ac0a-81db0962be16" />
<img width="806" height="366" alt="image" src="https://github.com/user-attachments/assets/44337741-f48e-4ba4-bf59-9ad500a6ceee" />
<img width="1012" height="562" alt="image" src="https://github.com/user-attachments/assets/9e935aab-a70b-454b-804d-f2e7282bed6e" />


### Traitement d'une table (job interne)
<img width="1536" height="768" alt="Diagram d’orchestration des traitements de données" src="https://github.com/user-attachments/assets/6e5a9b0c-b1a9-4348-95a4-974ffd23ff47" />


## 4. Tables de métadonnées (`banking.metadata`)

| Table | Question à laquelle elle répond | Contenu |
|---|---|---|
| `tables` | Quoi traiter ? | Source, schémas, active ou non, ordre de chargement |
| `table_parameters` | Comment ? | `load_type`, `primary_key`, `watermark_column` (format nom / valeur) |
| `table_watermarks` | Jusqu'où a-t-on chargé ? | Dernière valeur chargée par table |
| `pipeline_runs` | Que s'est-il passé ? | Audit : début, fin, statut, volumétrie, erreur |

### Configuration des tables sources

| Table | Source | Mode | Clé | Watermark |
|---|---|---|---|---|
| customers | SQL Server | MERGE | customer_id | updated_at |
| accounts | SQL Server | MERGE | account_id | updated_at |
| transactions | SQL Server | APPEND | txn_id | txn_timestamp |
| branches | SQL Server | FULL | branch_code | – |
| credit_bureau_reports | Fichiers CSV | MERGE | customer_id | bureau_pull_date |
| payment_gateway_logs | Fichiers CSV | APPEND | txn_id | processed_timestamp |

**Principe :** les trois modes de chargement sont écrits une seule fois dans `Bronze_to_silver`. Les métadonnées indiquent seulement lequel appliquer. Ajouter une table source revient à insérer quelques lignes de configuration.

## 5. Couche Gold

| Table | Contenu |
|---|---|
| `customer_360` | Vue unique du client : comptes, transactions, score de crédit, segment |
| `branch_performance` | Clients, comptes, dépôts et transactions par agence |
| `daily_bank_kpi` | Indicateurs quotidiens de toute la banque |
| `risk_customer_summary` | Clients, score moyen et impayés par grade de risque |
| `transaction_channel_summary` | Réussite, échec et temps de traitement par passerelle et appareil |

## 6. Restitution

- **Dashboard** : Executive dashboard, Customer Insights, Branch Performance, Gateway & Devices (environ 5 000 clients et 20 000 transactions).
- **Agent Genie** : instructions métier, description des tables, règles de lecture des données (valeurs de snapshot, identification des clients par `customer_id`…) et jeu de questions de référence (benchmarks).
<img width="953" height="552" alt="image" src="https://github.com/user-attachments/assets/e5c5af99-aa5a-49ee-aa51-e820b8796d5b" />
<img width="1145" height="545" alt="image" src="https://github.com/user-attachments/assets/232fc253-d7b4-41c9-833d-39539bd60120" />
<img width="1150" height="550" alt="image" src="https://github.com/user-attachments/assets/8e9ce4b8-f6fc-4ac5-9a89-8cdfb8db33f1" />
<img width="1322" height="501" alt="image" src="https://github.com/user-attachments/assets/2786ef84-8bb4-47df-97b6-86fe3ca8893c" />
<img width="1136" height="580" alt="image" src="https://github.com/user-attachments/assets/4f439710-db5d-4b8c-8323-6ea3c1ee91b9" />
<img width="1372" height="620" alt="image" src="https://github.com/user-attachments/assets/82d29d1f-dc25-4fce-ab7f-3e1891aab450" />
<img width="1043" height="650" alt="image" src="https://github.com/user-attachments/assets/67f420c0-84c5-4e7d-b18b-ca09147fec5e" />


## 8. Lancer le projet

1. Créer le catalogue `banking` et lancer `setup_Metadata` **une seule fois** (le notebook supprime puis recrée les tables de métadonnées : ne pas le relancer sur un environnement déjà utilisé).
2. Créer le secret scope `banking-scope` avec la connexion SQL Server et les identifiants d'e-mail, **sans les écrire dans le code**.
3. Déposer les fichiers CSV dans le volume `banking.source.volume`.
4. Créer les jobs `NeoBank_source_to_silver`, `NeoBank_silver_to_gold` et `NeoBank_Master_Job`.
5. Lancer le job maître, puis consulter `pipeline_runs`, l'e-mail et le dashboard.

## 10. Technologies

Databricks · PySpark · SQL · Delta Lake · Unity Catalog · Auto Loader · JDBC · Databricks Jobs · Genie · Dashboards
