# ADR 004 — Santé du pipeline dans une table BigQuery

- Statut : accepté
- Date : 2026-10-17

## Contexte

Un run qui échoue se voit dans l'UI Airflow. Mais rien ne répondait à :
« le dernier run a-t-il réussi, combien de lignes a-t-il chargées, combien
d'alertes qualité a-t-il levées, et est-ce que ça change d'une semaine à
l'autre ? ». Les logs Airflow et la base de métadonnées sont locaux et ne se
requêtent pas depuis l'outil de BI.

## Options

1. **Base de métadonnées Airflow** (Postgres local) lue par Metabase. Rien à
   écrire, mais pas de résultats Soda ni de volumes, et le dashboard dépend
   d'un schéma interne d'Airflow.
2. **StatsD / Prometheus + Grafana.** Le standard pour des métriques
   d'infrastructure, mais deux services de plus pour un run par semaine.
3. **Une table `retail_monitoring.pipeline_runs`** écrite par la dernière
   tâche du DAG.

## Décision

Option 3 ([`pipeline_health.py`](../../include/monitoring/pipeline_health.py)).

- La tâche `record_pipeline_health` tourne avec `trigger_rule=all_done` : elle
  s'exécute aussi quand une tâche amont a échoué.
- Une ligne par run : état, tâches échouées, tests dbt passés et échoués,
  scans Soda passés et échoués, alertes Soda, `raw_rows`, `fct_rows`.
- Insertion paramétrée ; la table est créée par la première tâche du DAG.
- Si une tâche a échoué, la tâche de santé écrit la ligne puis échoue
  elle-même, pour que le run reste marqué en échec.
- En plus : un `on_failure_callback` écrit un log JSON structuré (et poste
  sur Slack si `SLACK_WEBHOOK_URL` est défini), et le DAG `retail_freshness`
  vérifie chaque jour la fraîcheur de la table brute.

## Conséquences

- Le dashboard Metabase affiche l'historique des runs à côté des chiffres
  métier : on voit sur le même écran si un chiffre vient d'un run sain.
- Le contrôle Soda de volume compare au dernier run réussi lu dans cette
  table (ADR 003).
- Limite : `raw_rows` et `fct_rows` viennent des métadonnées des tables en
  fin de run. Après un échec précoce, ils montrent les volumes du run
  précédent.
- Cette solution ne remplace pas une supervision d'infrastructure ; pour un
  run hebdomadaire, elle suffit.
