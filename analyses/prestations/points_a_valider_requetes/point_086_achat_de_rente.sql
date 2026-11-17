-- =============================================================================
-- Point 86. L'achat de rente (T29)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T28 à
--   T30.
-- Ce que dit le point : « AJUSTEMENT, TAJ_CODE = RR, payé par un débours
--   d'ajustement (DEBOURS, DEB_TYPE = AJ), pas par un paiement ordinaire
--   (PM). » ; « en 2024, 11 rachats pour 21,5 M dans la base ; l'annuaire
--   CNSS 2024 en publie 25,4 M (tableaux 90, 96 et 97). »
-- Requête : les débours d'ajustement payés pour un mois de 2024, par branche
--   et par type d'ajustement.
-- Reprise de : lot9_indus.sql, P104 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : aucune requête proposée, T28 à T30 ne sont pas
--   produits. Fiche des règles : règle 37.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Débours d'ajustement (DEB_TYPE = AJ) au statut D ou P dont le mois payé
-- tombe en 2024, par branche (CODE_BRANCHE) et par type d'ajustement
-- (AJUSTEMENT, TAJ_CODE).
-- -----------------------------------------------------------------------------
SELECT NVL(deb.CODE_BRANCHE, '?')                                 AS branche,
       NVL(aj.TAJ_CODE, '(sans ajustement)')                      AS type_ajustement,
       COUNT(*)                                                   AS debours,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant,
       MIN(deb.DEB_MONTANT)                                       AS plus_petit,
       MAX(deb.DEB_MONTANT)                                       AS plus_grand
FROM USER_DWH.DEBOURS deb
LEFT JOIN USER_DWH.AJUSTEMENT aj ON aj.AJ_ID = deb.AJ_ID
WHERE deb.DEB_TYPE = 'AJ'
  AND deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
  AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
GROUP BY NVL(deb.CODE_BRANCHE, '?'), NVL(aj.TAJ_CODE, '(sans ajustement)')
ORDER BY branche, montant DESC;

-- Résultat obtenu (P104, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P104_debours_aj_2024.tsv), la ligne des rachats :
--   Branche   Type   Débours   Montant
--   A         RR     11        21 519 361
-- Face à l'annuaire CNSS 2024 : 25 366 909 de rachats de rentes en 2024
-- (tableau 90, p. 71 ; tableaux 96 et 97, p. 74).
-- Lecture : 11 rachats pour 21,5 M dans la base, 25,4 M publiés ; l'écart
--   n'est pas encore expliqué.
