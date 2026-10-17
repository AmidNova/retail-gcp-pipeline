# ADR 002 — CI sans clé (Workload Identity Federation) et slim CI

- Statut : accepté
- Date : 2026-10-16

## Contexte

L'ancienne CI ciblait la branche `main` alors que le dépôt utilise `master` :
elle n'avait jamais tourné. Elle attendait une clé JSON de compte de service
en secret GitHub, et ne lançait aucun test. Le dépôt est public.

Deux questions : comment la CI s'authentifie sur BigQuery, et que construit-elle.

## Options — authentification

1. **Clé JSON en secret GitHub.** Simple, mais une clé longue durée qui ne
   tourne pas, à révoquer à la main en cas de fuite.
2. **Workload Identity Federation (WIF).** GitHub émet un jeton OIDC, échangé
   contre un jeton court d'un compte de service dédié. Aucune clé n'existe.

## Options — périmètre du build

1. **`dbt build` complet à chaque PR.** Simple, mais tout est reconstruit et
   payé, même pour un changement de doc.
2. **Slim CI** : `dbt build --select state:modified+ --defer --state <manifest prod>`.
   Seuls les modèles modifiés et leurs descendants sont construits ; les
   parents non modifiés sont lus en production.

## Décision

WIF + slim CI ([`ci.yml`](../../.github/workflows/ci.yml),
[`setup_ci_wif.sh`](../../scripts/setup_ci_wif.sh)).

- Le provider WIF n'accepte que les jetons de ce dépôt
  (`attribute-condition` sur `assertion.repository`).
- Le compte `dbt-ci` a seulement `bigquery.user` (lancer des jobs, créer ses
  datasets) et `bigquery.dataViewer` (lire la prod pour `--defer`). Il ne
  peut pas écrire en production.
- Chaque push sur `master` publie `manifest.json` en artefact ; la PR
  télécharge le dernier. Sans manifest, la CI repasse en build complet.
- Les datasets `retail_ci_pr<N>_*` sont supprimés en fin de job, même en cas
  d'échec (`if: always()`).
- `maximum_bytes_billed` plafonne chaque requête de la cible `ci`.

## Conséquences

- Aucun secret dans le dépôt ni dans GitHub : uniquement des variables
  (identifiant du projet, du provider, du compte de service).
- Une PR qui ne touche qu'un rapport ne reconstruit que ce rapport.
- La qualité du `--defer` dépend de la fraîcheur de la prod : un modèle
  modifié est testé contre les tables du dernier run de production.
- sqlfluff avec le templater dbt doit compiler le projet, donc il tourne dans
  le job qui a des identifiants, pas dans le job statique.
