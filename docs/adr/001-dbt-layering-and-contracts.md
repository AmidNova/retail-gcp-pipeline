# ADR 001 — Couches dbt et contrats sur les tables exposées

- Statut : accepté
- Date : 2026-10-16

## Contexte

Le projet de départ avait un seul dossier `transform/` (dimensions + faits) et
un dossier `report/`. Chaque modèle lisait directement la source brute, la
typait et la filtrait lui-même ; `fct_invoices` filtrait `Quantity > 0` puis
perdait encore des lignes par des `INNER JOIN` sur les dimensions. Résultat :
la règle « qu'est-ce qu'une vente ? » était dispersée et en partie implicite,
et une modification de colonne en amont cassait le dashboard sans prévenir.

## Options

1. **Garder deux dossiers** et ajouter des tests. Peu de travail, mais le
   périmètre reste éparpillé et rien ne protège le schéma des tables lues
   par Metabase.
2. **Couches staging → intermediate → marts → reports (+ ml)**, conventions
   dbt Labs, contrats sur les tables lues par d'autres outils.

## Décision

Option 2.

- **staging** (vues) : une source, typage et renommage, drapeaux
  (`is_cancellation`, `is_guest_checkout`). Aucune ligne filtrée, pour que les
  contrôles de volume voient tout.
- **intermediate** (vue) : le périmètre des ventes défini **une seule fois**
  (quantité > 0, prix > 0, client identifié, code article qui est un produit).
- **marts** (tables, `contract: enforced`) : schéma en étoile
  `fct_invoice_lines` + `dim_customers` / `dim_products` / `dim_dates`.
- **reports** (tables, contrat) : agrégats lus par Metabase.
- **ml** (tables) : séries et features d'entrée de BQML.

Chaque couche a son dataset (`retail_<couche>`), avec un préfixe par cible
(`retail_dev_*`, `retail_ci_pr<N>_*`), via le `generate_schema_name` par
défaut de dbt.

## Conséquences

- Un changement de type ou de nom de colonne dans un mart ou un report fait
  échouer `dbt build` au lieu de casser le dashboard en silence.
- La refonte a été vérifiée par équivalence : sur l'ancien CSV, l'ancien
  `fct_invoices` et le nouveau `fct_invoice_lines` donnaient les mêmes
  780 926 lignes, 36 413 factures et 17 126 075,78 £.
- Le test singulier `assert_reports_reconcile_with_fact` vérifie que chaque
  report retombe sur le CA de la table de faits.
- Coût : plus de fichiers, et le grain de la table de faits passe de la
  facture à la ligne de facture (renommage en `fct_invoice_lines`).
