# ADR 003 — Tests dbt pour les invariants, Soda pour les seuils statistiques

- Statut : accepté
- Date : 2026-10-17

## Contexte

Le projet de départ n'avait aucun test dbt. Ses checks Soda étaient
structurels (schéma, clés non nulles, totaux négatifs) : le genre de règle
qu'un test dbt exprime à côté du modèle. Aucun contrôle ne portait sur les
volumes ou les distributions, donc une dérive des données passait.

## Options

1. **Tout dans dbt** (tests génériques + `dbt_expectations`). Un seul outil,
   mais pas d'état entre deux runs : pas de comparaison au run précédent.
2. **Tout dans Soda.** Perd les tests unitaires dbt, les contrats et le lien
   test ↔ modèle dans le lineage.
3. **Partage par nature du contrôle.**

## Décision

Option 3.

- **dbt** vérifie ce qui doit être vrai à 100 % : clés uniques et non nulles,
  relations fait → dimensions, plages de valeurs, contrats, réconciliation
  des rapports avec la table de faits, série journalière sans trou. Un échec
  arrête le run.
- **Soda** surveille ce qui est statistique, à chaque frontière de couche
  (raw, marts, reports), avec deux niveaux :
  - `warn` : écart à la valeur mesurée, à regarder ;
  - `fail` : donnée inutilisable, le run s'arrête.

Les seuils partent des valeurs mesurées sur le jeu complet :

| Contrôle | Mesuré | warn | fail |
|---|---|---|---|
| Volume brut vs dernier run réussi | 0 % | ≠ 0 % | < −5 % |
| Lignes sans client | 22,8 % | > 25 % | > 35 % |
| Lignes d'annulation | 1,8 % | > 3 % | > 10 % |
| Part des lignes brutes dans la table de faits | 75,2 % | hors 72–79 % | hors 60–90 % |
| Clients sans code ISO | 0,63 % | > 2 % | > 10 % |
| Mois manquants dans le CA mensuel | 0 | — | > 0 |
| Clients qui font 80 % du CA | 23,1 % | hors 20–26 % | — |

## Conséquences

- Chaque seuil est commenté dans le YAML Soda avec sa valeur de référence et
  ce qu'un dépassement signifie.
- Le nombre d'alertes `warn` de chaque run est stocké dans
  `retail_monitoring.pipeline_runs.soda_checks_warned` (voir ADR 004).
- Le contrôle de volume lit la table de santé : au premier run, sans
  historique, l'écart vaut 0 et le contrôle passe.
- Le contrôle de volume aurait signalé l'ancien CSV tronqué (−1,76 %) en
  `warn`.
