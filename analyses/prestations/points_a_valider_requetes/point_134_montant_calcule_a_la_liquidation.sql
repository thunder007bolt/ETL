-- =============================================================================
-- Point 134. Le montant calculé à la liquidation
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « on n'a pas trouvé ce montant. La table des étapes
--   de calcul (ETAPE), au calcul initial (TEA_CODE = CIP), en porte peut-être
--   un. »
-- Requêtes :
--   1. Les types d'étape des prestations, dont le calcul initial (CIP).
--   2. Les colonnes de la table ETAPE, pour y chercher un montant.
-- Reprise de : lot1_inventaire_prestations.sql, P12 (exécutée le
--   26/10/2026) ; la requête 2 est nouvelle.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; la requête 2 ne lit que le catalogue.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc montants (la
--   somme des paiements de la prestation). Fiche des règles : règle 70.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Types d'étape (ETAPE, TEA_CODE) : libellé, nombre, et pour le
-- calcul initial, le nombre de 2025 et la date la plus récente.
-- -----------------------------------------------------------------------------
SELECT e.TEA_CODE, t.TEA_DESCRIPTION,
       COUNT(*)                     AS nb_etapes,
       COUNT(DISTINCT e.PE_ID)      AS nb_prestations,
       MIN(e.EA_DATE_EFFET)         AS date_min,
       MAX(e.EA_DATE_EFFET)         AS date_max,
       SUM(CASE WHEN e.EA_DATE_EFFET >= DATE '2025-01-01'
                 AND e.EA_DATE_EFFET <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)  AS nb_2025
FROM USER_DWH.ETAPE e
LEFT JOIN USER_DWH.TYPE_ETAPE t ON t.TEA_CODE = e.TEA_CODE
GROUP BY e.TEA_CODE, t.TEA_DESCRIPTION
ORDER BY nb_etapes DESC;

-- Résultat obtenu (P12, exécutée le 26/10/2026) : 13 types d'étape ; CIP
-- calcul initial 947 441, une par prestation (30 236 en 2025) ; RP révision
-- 588 919 ; MAJ majoration 167 810 ; des retenues, RRR rabais pour rachat de
-- rente, des correctifs. Date maximale du calcul initial en 2100.


-- -----------------------------------------------------------------------------
-- Requête 2. Les colonnes de la table ETAPE (catalogue ALL_TAB_COLUMNS).
-- -----------------------------------------------------------------------------
SELECT COLUMN_ID,
       COLUMN_NAME,
       DATA_TYPE
FROM ALL_TAB_COLUMNS
WHERE OWNER      = 'USER_DWH'
  AND TABLE_NAME = 'ETAPE'
ORDER BY COLUMN_ID;

-- Résultat : pas encore exécutée.
-- Lecture attendue : une colonne de montant sur l'étape CIP donnerait le
--   montant liquidé de T54 et T55 sans attendre les derniers paiements.
