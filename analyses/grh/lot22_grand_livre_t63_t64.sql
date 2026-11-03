-- =============================================================================
-- LOT 22 — Le grand livre pour les ratios de T63 et T64 : partie A, la structure
-- Rédigé le 02/11/2026 — À EXÉCUTER (lecture du dictionnaire Oracle seulement).
-- Exécution : en mode script (F5) dans SQL Developer. Ce lot ne lit que la
-- description des tables (noms, colonnes, volumes, commentaires) : aucune
-- écriture comptable, aucun libellé, aucun montant, aucune donnée bancaire.
-- Pourquoi ce lot : T63 (frais de personnel / charges de fonctionnement) et
-- T64 (dépenses de formation / masse salariale, et / budget de formation) ne
-- se calculent pas depuis la GRH : la paie n'est pas dans la copie (lot 8,
-- Q55). La comptabilité, elle, y est : le schéma STG_GL compte 1 518 974
-- écritures, 8 028 comptes de comptabilité générale et 9 272 lignes de budget
-- (lot 9, Q66), mais on n'a jamais lu sa structure.
-- Les comptes à prendre sont ceux de la CNSS elle-même, dans sa grille
-- « Evaluation des Ratios CIPRES 2025.xlsx » (cnss_files), feuilles
-- « 07_Ratios budgétaires » et « 09_Ratios ASS », toutes dans la gestion
-- administrative, notée « G ou 7 » :
--   - masse salariale (MSAL) : « total salaires payés », comptes 651 et 652 ;
--   - dépenses de formation (FOR) : comptes 63812, 6386 et 6577 ;
--   - charges administratives ou de fonctionnement (CF) : comptes 61 à 68 ;
--   - produits techniques : compte 70 (autre ratio, même grille).
-- La grille ne donne pas les comptes des frais de personnel (FP) de T63 :
-- proposition BI, les comptes 65 (dont 651 et 652), à confirmer par la
-- direction financière. Les autres gestions de la grille : I ou 6, S ou 5
-- (action sanitaire et sociale), T ou 8, E ou 9.
-- Ce lot (partie A) trouve où sont le compte, la gestion, le montant et la
-- date dans STG_GL. La partie B, écrite d'après ses résultats, fera les totaux
-- de 2024 par compte pour la gestion G ou 7 et cherchera le budget de
-- formation dans STG_BUDGET.
-- Numérotation : suite du lot 21, Q121 à Q123.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q121 — Les tables du schéma STG_GL
-- Objectif : toutes les tables de STG_GL, avec leur nombre de lignes, la date
--            des statistiques et leur commentaire.
-- Tableaux : T63, T64.
-- Lecture  : repérer la table des écritures (ou des balances) et celle du
--            plan de comptes ; le lot 9 (Q66) a vu STG_ECRITURE, STG_BALANCES,
--            STG_PORTEFEUILLE, STG_BUDGET, des engagements et des dépenses.
-- Résultat : à remplir.
-- -----------------------------------------------------------------------------
SELECT t.table_name, t.num_rows, t.last_analyzed,
       c.comments
FROM ALL_TABLES t
LEFT JOIN ALL_TAB_COMMENTS c ON c.owner = t.owner AND c.table_name = t.table_name
WHERE t.owner = 'STG_GL'
ORDER BY t.num_rows DESC NULLS LAST, t.table_name;


-- -----------------------------------------------------------------------------
-- Q122 — Les colonnes des tables non vides de STG_GL
-- Objectif : pour chaque table qui a des lignes, ses colonnes avec leur type,
--            le nombre de valeurs distinctes et de valeurs vides (statistiques
--            d'Oracle, sans lire les données), et leur commentaire.
-- Tableaux : T63, T64.
-- Lecture  : chercher le numéro de compte (une colonne texte d'environ 5 à 12
--            caractères, quelques milliers de valeurs), la gestion (quelques
--            valeurs : G, I, S, T, E ou 5 à 9), le montant (débit et crédit,
--            ou un montant signé), la date ou l'exercice.
-- Résultat : à remplir.
-- -----------------------------------------------------------------------------
SELECT c.table_name, c.column_id, c.column_name, c.data_type, c.data_length,
       s.num_distinct, s.num_nulls,
       cc.comments
FROM ALL_TAB_COLUMNS c
JOIN ALL_TABLES t ON t.owner = c.owner AND t.table_name = c.table_name
LEFT JOIN ALL_TAB_COL_STATISTICS s ON s.owner = c.owner
                                  AND s.table_name = c.table_name
                                  AND s.column_name = c.column_name
LEFT JOIN ALL_COL_COMMENTS cc ON cc.owner = c.owner
                             AND cc.table_name = c.table_name
                             AND cc.column_name = c.column_name
WHERE c.owner = 'STG_GL'
  AND NVL(t.num_rows, 0) > 0
ORDER BY c.table_name, c.column_id;


-- -----------------------------------------------------------------------------
-- Q123 — La comptabilité et le budget ailleurs que dans STG_GL
-- Objectif : les tables et les vues des autres schémas dont le nom parle de
--            compte, d'écriture, de balance, de grand livre, de budget ou de
--            gestion, avec leur nombre de lignes : une autre copie de la
--            comptabilité (domaine des finances) ou un plan de comptes avec
--            ses libellés.
-- Tableaux : T63, T64.
-- Lecture  : garder ce qui a des lignes ; les tables DTM sont notées pour
--            mémoire, leur contenu n'est pas fiable (données de test en 2025,
--            lot 9, Q66).
-- Résultat : à remplir.
-- -----------------------------------------------------------------------------
SELECT o.owner, o.object_name, o.object_type, t.num_rows
FROM ALL_OBJECTS o
LEFT JOIN ALL_TABLES t ON t.owner = o.owner AND t.table_name = o.object_name
WHERE o.object_type IN ('TABLE', 'VIEW')
  AND o.owner NOT IN ('SYS', 'SYSTEM', 'STG_GL', 'XDB', 'MDSYS', 'CTXSYS', 'ORDSYS',
                      'OLAPSYS', 'WMSYS', 'APEX_030200', 'EXFSYS', 'DBSNMP')
  AND (o.object_name LIKE '%COMPT%' OR o.object_name LIKE '%ECRIT%'
       OR o.object_name LIKE '%BALANCE%' OR o.object_name LIKE '%GRAND%LIVRE%'
       OR o.object_name LIKE '%BUDGET%' OR o.object_name LIKE '%GESTION%'
       OR o.object_name LIKE 'GL\_%' ESCAPE '\')
ORDER BY o.owner, o.object_name;
