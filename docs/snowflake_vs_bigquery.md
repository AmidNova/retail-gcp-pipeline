# Snowflake vs BigQuery — ce qui change pour ce projet

> **Statut** : ce projet tourne **uniquement sur BigQuery**. Aucune ligne n'a été exécutée sur Snowflake.
> Ce document compare les deux plateformes, inventorie ce qui est déjà portable dans le code et décrit comment je
> porterais le projet. Les tarifs évoluent : les chiffres de coût donnent un ordre de grandeur, à vérifier sur les
> grilles officielles avant toute décision.

## En 5 phrases (version orale)

1. Les deux séparent stockage et calcul, mais **Snowflake te fait choisir la puissance de calcul** (un *virtual warehouse*
   de taille XS à 6XL, allumé à la demande), alors que **BigQuery est serverless** : tu envoies une requête, Google alloue
   des *slots*.
2. Côté coût, Snowflake facture **le temps d'allumage du warehouse** (crédits/heure, à la seconde) ; BigQuery facture
   **les octets scannés** (on-demand) ou **les slots réservés** (capacity).
3. Les deux ont du **time travel** et une période de **fail-safe**, avec des durées et des options différentes.
4. La gouvernance est **RBAC à base de rôles SQL** chez Snowflake, **IAM** (projet / dataset / table) chez BigQuery.
5. Pour dbt, la logique métier reste la même ; ce qui change, c'est le **profil**, quelques **configs spécifiques**
   (partitionnement, tables transientes, labels…) et les **fonctions SQL non standard**, qu'on isole avec les macros
   cross-database et `adapter.dispatch`.

---

## 1. Modèle d'exécution : warehouses vs slots

| | Snowflake | BigQuery |
|---|---|---|
| Unité de calcul | *Virtual warehouse* (cluster de calcul nommé) | *Slot* (unité de CPU/mémoire allouée par Google) |
| Dimensionnement | Choisi par toi : XS, S, M, L… chaque taille **double** les ressources et le coût horaire | Automatique en on-demand ; en capacity, tu réserves un nombre de slots (avec autoscaling) |
| Démarrage / arrêt | `AUTO_RESUME` / `AUTO_SUSPEND` (ex. arrêt après 60 s d'inactivité) | Rien à gérer |
| Concurrence | *Multi-cluster warehouse* (édition Enterprise) : des clusters s'ajoutent quand les requêtes font la queue | Le pool de slots est partagé ; en on-demand, la concurrence est gérée par Google |
| Isolation des charges | **Un warehouse par usage** (ex. `TRANSFORM_WH` pour dbt, `BI_WH` pour Metabase) : la BI ne ralentit pas les transformations | Réservations et *assignments* de slots par projet, en mode capacity |
| Cache | Cache de résultats (24 h) + cache disque local du warehouse | Cache de résultats (24 h, requête identique) |

**Ce qu'il faut retenir** : sur Snowflake, la question « quelle taille de warehouse et quand l'éteindre ? » est une
décision d'ingénierie et de coût. Sur BigQuery, la question équivalente est « combien d'octets ma requête scanne-t-elle ? »,
d'où l'importance du partitionnement et du clustering.

## 2. Stockage et coûts

| | Snowflake | BigQuery |
|---|---|---|
| Format | Micro-partitions colonnaires compressées, gérées automatiquement | Format colonnaire (Capacitor), géré automatiquement |
| Organisation physique | Clustering automatique ; *clustering key* optionnelle sur les grosses tables | Partitionnement (date, ingestion, entier) + clustering (jusqu'à 4 colonnes) |
| Facturation du calcul | Crédits consommés pendant que le warehouse tourne : XS = 1 crédit/h, S = 2, M = 4… facturé à la seconde (minimum 60 s à chaque démarrage). Prix du crédit selon édition et région | **On-demand** : par TiB scanné (le premier TiB du mois est gratuit). **Capacity** (éditions) : par slot-heure |
| Facturation du stockage | Par To compressé et par mois (time travel et fail-safe compris) | Actif vs *long-term* (table non modifiée depuis 90 jours : environ moitié prix), en octets logiques ou physiques |
| Garde-fou de coût | *Resource monitors* (quota de crédits, suspension du warehouse) | `maximum_bytes_billed` par requête, quotas par utilisateur ou projet, budgets |

**Dans ce repo** : le garde-fou BigQuery est `maximum_bytes_billed: 2000000000` dans les targets `dev` et `ci`
(`include/dbt/profiles.yml`). L'équivalent Snowflake serait un warehouse XS dédié à la CI, avec `AUTO_SUSPEND` court
et un *resource monitor*.

## 3. Time travel, fail-safe, clones

| | Snowflake | BigQuery |
|---|---|---|
| Time travel | 0 à 1 jour en édition Standard, jusqu'à 90 jours en Enterprise (`DATA_RETENTION_TIME_IN_DAYS`) | Fenêtre configurable de 2 à 7 jours (7 par défaut) |
| Syntaxe | `SELECT … AT(OFFSET => -3600)`, `UNDROP TABLE` | `SELECT … FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(…)` |
| Fail-safe | 7 jours après le time travel, pour les tables permanentes ; récupération **via le support Snowflake** uniquement | 7 jours après la fenêtre de time travel ; récupération via le support |
| Tables sans fail-safe | Tables **transient** et **temporary** : moins chères, pas de fail-safe | — |
| Copies | **Zero-copy clone** (`CREATE … CLONE`) : instantané, seules les modifications coûtent | *Table clone* et *table snapshot* |

**Point dbt important** : par défaut, `dbt-snowflake` crée des tables **transient**, sans fail-safe, ce qui réduit le
coût de stockage des modèles reconstructibles. C'est un choix à assumer : une table transient supprimée par erreur ne se
récupère que pendant sa courte fenêtre de time travel.

**Usage typique en plateforme** : un zero-copy clone de la base de prod donne un environnement de dev ou de CI sur de
vraies données, sans copie physique.

## 4. Organisation et gouvernance

| | Snowflake | BigQuery |
|---|---|---|
| Hiérarchie | `account` → `database` → `schema` → `table` | `organisation` → `projet` → `dataset` → `table` |
| Accès | **RBAC** : privilèges accordés à des **rôles**, rôles accordés aux utilisateurs ; hiérarchie de rôles (`ACCOUNTADMIN`, `SYSADMIN`, `SECURITYADMIN`…) ; `FUTURE GRANTS` pour les objets à venir | **IAM** : rôles prédéfinis (`bigquery.dataViewer`, `dataEditor`, `jobUser`…) au niveau projet, dataset ou table |
| Sécurité fine | Masking policies, row access policies | Policy tags (sécurité par colonne), row-level security |
| Identifiants | Non quotés = **mis en majuscules** (`invoice_id` → `INVOICE_ID`) | Noms de colonnes insensibles à la casse ; noms de datasets et de tables sensibles à la casse |
| Auth des comptes de service | Key-pair (clé RSA) recommandée ; Snowflake retire progressivement l'authentification par mot de passe seul | Comptes de service ; sans clé via Workload Identity Federation (ce que fait la CI de ce repo) |

**Dans ce repo** : le compte de service CI `dbt-ci` n'a que `bigquery.user` + `bigquery.dataViewer`
(`scripts/setup_ci_wif.sh`). L'équivalent Snowflake serait un rôle `CI_ROLE` avec `USAGE` sur un warehouse CI,
`CREATE SCHEMA` sur une base CI et `SELECT` sur les schémas de prod (pour `--defer`).

## 5. Ce que ça change pour dbt

| Sujet | BigQuery (actuel) | Snowflake |
|---|---|---|
| Adapter | `dbt-bigquery` | `dbt-snowflake` |
| Profil | `project`, `dataset`, `location`, `method` (oauth / service-account) | `account`, `user`, `role`, `warehouse`, `database`, `schema`, auth key-pair |
| « Schéma » dbt | Un **dataset** BigQuery | Un **schéma** Snowflake (dans une base) |
| Configs spécifiques | `partition_by`, `cluster_by`, `labels`, `maximum_bytes_billed` | `cluster_by`, `transient`, `snowflake_warehouse` (par modèle), `query_tag`, `copy_grants` |
| Fonctions | `SAFE_CAST`, `DATE_DIFF`, `PARSE_DATETIME`, `FORMAT_DATE` | `TRY_CAST`, `DATEDIFF`, `TO_TIMESTAMP`, `TO_CHAR` |
| Types | `STRING`, `INT64`, `NUMERIC`, `FLOAT64`, `TIMESTAMP` (UTC) / `DATETIME` | `VARCHAR`, `NUMBER(38,0)`, `NUMBER(p,s)`, `FLOAT`, `TIMESTAMP_NTZ` / `_LTZ` / `_TZ` |

**Le mécanisme de portabilité** : les macros cross-database de dbt (`dbt.type_string()`, `dbt.safe_cast()`,
`dbt.dateadd()`, `dbt.datediff()`, `dbt.date_trunc()`…) appellent `adapter.dispatch`, qui choisit l'implémentation selon
l'adapter actif. On écrit `{{ dbt.datediff('a', 'b', 'day') }}` une seule fois ; dbt génère `datetime_diff(...)` sur
BigQuery et `datediff(...)` sur Snowflake. Pour une fonction non couverte, on écrit sa propre macro avec
`adapter.dispatch` : une implémentation `bigquery__ma_macro`, une `snowflake__ma_macro`, et une `default__ma_macro`.

## 6. Inventaire de ce repo

### Déjà écrit de façon portable

| Fichier | Ce qui est portable |
|---|---|
| `models/staging/stg_retail__invoice_lines.sql` | Types via `dbt.type_string()`, `dbt.type_bigint()`, `dbt.type_numeric()`, `dbt.type_timestamp()` ; parsing de date via `dbt.safe_cast()` (`TRY_CAST` sur Snowflake, qui fonctionne ici car la colonne source est une chaîne) |
| `models/intermediate/int_invoice_lines__customer_sales.sql` | Clé de substitution via `dbt_utils.generate_surrogate_key` |
| `models/marts/dim_dates.sql` | Calendrier via `dbt_utils.date_spine` et `dbt.date_trunc` ; `EXTRACT(… FROM …)` existe dans les deux |
| `models/ml/ml_customer_rfm.sql` | `dbt.dateadd`, `dbt.datediff` |
| `models/reports/rpt_customer_revenue_concentration.sql` | `dbt.type_float()` pour la division ; fonctions de fenêtre standard |
| Tests singuliers (`tests/`) | SQL standard + `dbt.datediff` |

### Spécifique à BigQuery, à isoler au portage

| Fichier | Point | Piste |
|---|---|---|
| `models/*/_*__models.yml` | `data_type` des contrats en types BigQuery (`int64`, `float64`, `string`, `numeric`) | Tester des noms de types acceptés par les deux plateformes, sinon un fichier de propriétés par adapter |
| `seeds/_seeds.yml` | `column_types` en `int64` / `string` | Même approche |
| `dbt_project.yml` | `+labels` (config BigQuery) | Ignoré par Snowflake ; équivalent possible : `query_tag` par couche |
| `profiles.yml` | Targets `type: bigquery`, `maximum_bytes_billed` | Ajouter un target `snowflake` (key-pair, warehouse XS, `AUTO_SUSPEND` court) |
| `dags/online_retail.py` | Chargement GCS → BigQuery ; **BigQuery ML** (ARIMA_PLUS, KMEANS) | Chargement : stage externe + `COPY INTO`. ML : **reste spécifique à BigQuery**, documenté comme tel ; Snowflake propose ses propres fonctions ML (ex. `SNOWFLAKE.ML.FORECAST`), mais ce serait une réimplémentation, pas un portage |
| `include/soda/configuration.yml` | Data sources `type: bigquery` | Soda a une data source Snowflake : connexion à ajouter |
| `.sqlfluff` | `dialect = bigquery` | Lint par adapter (le dialecte `snowflake` existe) |
| Colonnes source CamelCase (`Invoice`, `StockCode`…) | Snowflake les mettra en majuscules si elles ne sont pas quotées | Charger le raw avec des noms normalisés, ou quoter dans la source dbt (`quoting`) |

## 7. Plan de portage (phase 3)

1. Compte d'essai Snowflake (30 jours) : rôle `TRANSFORMER`, warehouse `TRANSFORM_WH` en XS avec `AUTO_SUSPEND = 60`,
   base `RETAIL`, *resource monitor* sur les crédits.
2. Charger le raw : fichier CSV dans un *internal stage*, puis `COPY INTO RAW.ONLINE_RETAIL` avec un schéma explicite
   (même principe que le chargement BigQuery : pas d'inférence de types).
3. Ajouter un target `snowflake` dans `profiles.yml` (auth key-pair, aucun secret dans le repo).
4. `dbt build --target snowflake --exclude path:models/ml` et corriger ce qui casse (types des contrats, casse des
   identifiants). Chaque correction de dialecte devient une macro `adapter.dispatch`, pas un `if` dans un modèle.
5. Vérifier l'équivalence avec la même méthode que le refactor de la phase 1 : mêmes lignes, mêmes factures, même CA
   dans `fct_invoice_lines` sur les deux plateformes.
6. En CI : `dbt parse --target snowflake` sans connexion (pour ne pas dépendre de la fin de l'essai gratuit), et
   l'exécution réelle sur Snowflake archivée dans `docs/`.

## 8. Questions probables en entretien

**« Tu as déjà utilisé Snowflake ? »**
Pas en production. J'ai construit ce pipeline sur BigQuery en écrivant les modèles avec les macros cross-database de dbt,
et j'ai identifié précisément ce qui ne serait pas portable : les types des contrats, le chargement et BigQuery ML.

**« Comment tu dimensionnes un warehouse ? »**
On commence petit (XS), on mesure : si une requête « spille » sur disque (*bytes spilled* dans le query profile), on
monte d'une taille, ce qui double les ressources et le coût horaire mais réduit souvent la durée. Si le problème vient de
requêtes qui font la queue (concurrence), on ajoute des clusters (*multi-cluster warehouse*) plutôt que d'augmenter la
taille. Et on sépare les warehouses par usage (transformations, BI, CI).

**« Comment tu maîtrises les coûts ? »**
Sur Snowflake : `AUTO_SUSPEND` court, warehouses dédiés par usage, *resource monitors*, tables transient pour les modèles
reconstructibles. Sur BigQuery : partitionnement et clustering pour scanner moins, `maximum_bytes_billed` comme
garde-fou (c'est ce que fait ce repo en dev et en CI), budgets et alertes.

**« Time travel ou backups ? »**
Le time travel couvre les erreurs récentes (un `DELETE` raté, un modèle cassé) sur quelques jours. Ce n'est pas une
stratégie de sauvegarde à long terme : le fail-safe n'est accessible que via le support. Pour un retour arrière long
terme : clones ou snapshots datés.

**« Comment dbt gère deux warehouses ? »**
Avec un target par plateforme dans `profiles.yml`, les macros cross-database pour les fonctions courantes, et
`adapter.dispatch` pour mes propres macros. Les configs spécifiques (`partition_by` côté BigQuery, `transient` côté
Snowflake) sont ignorées par l'autre adapter ou placées dans des blocs conditionnés par `target.type`.
