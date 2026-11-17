-- =============================================================================
-- Point 61. Les débours de trop-perçu
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « un débours de type AJ (DEB_TYPE) est relié par AJ_ID
--   à un ajustement ; quand c'est un trop-perçu (TAJ_CODE = TP), son montant
--   (DEB_MONTANT) est tantôt négatif, tantôt positif. » ; « en 2024, ce
--   montant va de −6,1 M à +6,6 M. »
-- Requête : les débours d'ajustement payés pour un mois de 2024, par branche
--   et par type d'ajustement : nombre, montant, plus petit et plus grand.
-- Reprise de : lot9_indus.sql, P104 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (le
--   trop-perçu le mois où il est établi, pas ses débours). Fiche des règles :
--   règle 45.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Débours d'ajustement (DEB_TYPE = AJ) au statut D ou P dont le mois payé
-- tombe en 2024, par branche (CODE_BRANCHE) et par type d'ajustement
-- (TAJ_CODE) : nombre, montant, plus petit et plus grand débours.
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
-- resultats/lot9_P104_debours_aj_2024.tsv), les débours liés à un
-- trop-perçu :
--   Branche   Type      Débours   Montant      Plus petit   Plus grand
--   V         TP        6 839     78 175 530   −6 063 000   6 639 133
--   V         TP-CONV     522      6 122 999     −100 694   4 296 990
--   A         TP          112      2 775 394     −113 042   2 306 520
--   F         TP          118      1 552 490     −145 000     150 000
--   M         TP           20      2 112 687      −29 464   1 091 716
--   Ils ont les deux signes : de −6,1 M à +6,6 M en pensions.
-- Lecture : un débours de trop-perçu est tantôt une retenue, tantôt un
--   versement : son sens est à faire dire par la DSI.
