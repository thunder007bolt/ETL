-- =============================================================================
-- Point 33. Les paiements par ajustement
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « un débours d'ajustement (DEBOURS, DEB_TYPE = AJ)
--   relié à l'ajustement (AJUSTEMENT, TAJ_CODE = MP ou RC pour les rappels, AV
--   pour les avances), non un débours PM. » ; « en 2024, 408,3 M de rappels et
--   373,2 M d'avances versés ainsi ; les rappels font environ 0,7 % des
--   prestations payées. »
-- Requêtes :
--   1. Les débours d'ajustement payés pour un mois de 2024, par branche et
--      par type d'ajustement.
--   2. Les prestations payées de 2024 (débours PM), pour comparer.
-- Reprise de : lot9_indus.sql, P104 et P105 (exécutées le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc payees (débours PM
--   seulement) ; t50_t57_requete_proposee.sql, bloc paie (T56). Fiche des
--   règles : règles 7 et 46.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Débours d'ajustement (DEB_TYPE = AJ) au statut D ou P dont le
-- mois payé tombe en 2024, par branche et par type d'ajustement (TAJ_CODE).
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
-- resultats/lot9_P104_debours_aj_2024.tsv), débours / montant :
--   V   AV 169 081 / 303 622 709 ; RC 1 157 / 132 994 743 ; MP 175 /
--       132 576 956 ; TP 6 839 / 78 175 530 ; TP-CONV 522 / 6 122 999 ;
--       REM-TP 12 / −8 495 627 ; REV-AV 170 / −57 834 912
--   F   MP 17 043 / 131 408 360 ; AV 86 494 / 68 834 531 ; TP 118 /
--       1 552 490 ; REM-TP 6 / −387 000 ; REV-AV 12 / −408 000
--   M   MP 58 / 8 121 771 ; TP 20 / 2 112 687 ; REM-TP 6 / −1 468 043
--   A   RR 11 / 21 519 361 ; MP 21 / 3 210 681 ; TP 112 / 2 775 394 ;
--       AV 5 953 / 749 365 ; REV-AV 2 / −149 001 ; REM-TP 1 / −2 306 520
-- Lecture (calculé ici) :
--   · rappels (MP et RC) : 132 994 743 + 132 576 956 + 131 408 360
--     + 8 121 771 + 3 210 681 = 408 312 511, soit 408,3 M ;
--   · avances (AV) : 303 622 709 + 68 834 531 + 749 365 = 373 206 605, soit
--     373,2 M.


-- -----------------------------------------------------------------------------
-- Requête 2. T31 de 2024 : les prestations payées (débours PM au statut D ou
-- P), par mois et par branche.
-- -----------------------------------------------------------------------------
WITH mois AS (
    SELECT LEVEL AS mois FROM DUAL CONNECT BY LEVEL <= 12
),
branches AS (
    SELECT 'PVID' AS branche FROM DUAL
    UNION ALL SELECT 'AT/MP' FROM DUAL
    UNION ALL SELECT 'PF'    FROM DUAL
),
payees_mois AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM deb.DEB_DATE_EFFET)                 AS mois,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
    GROUP BY CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM deb.DEB_DATE_EFFET)
),
payees_saisie AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM deb.DEB_DATE_INSERT)                AS mois,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_INSERT >= DATE '2024-01-01'
      AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
    GROUP BY CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM deb.DEB_DATE_INSERT)
),
ajustements AS (
    SELECT CASE d.TDOS_CODE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM aj.AJ_DATE_ETABLISSEMENT)           AS mois,
           SUM(CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV')
                    THEN aj.AJ_MONTANT ELSE 0 END)                AS trop_percus,
           SUM(CASE WHEN aj.TAJ_CODE IN ('MP', 'RC')
                    THEN aj.AJ_MONTANT ELSE 0 END)                AS moins_percus,
           SUM(CASE WHEN aj.TAJ_CODE = 'REM-TP'
                    THEN aj.AJ_MONTANT ELSE 0 END)                AS remb_trop_percus
    FROM USER_DWH.AJUSTEMENT aj
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = aj.DOS_CODE
    WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV', 'MP', 'RC', 'REM-TP')
      AND d.TDOS_CODE IN ('V', 'A', 'F', 'M')
      AND aj.AJ_DATE_ETABLISSEMENT >= DATE '2024-01-01'
      AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2025-01-01'
    GROUP BY CASE d.TDOS_CODE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM aj.AJ_DATE_ETABLISSEMENT)
)
SELECT m.mois,
       b.branche,
       ROUND(NVL(pm.montant, 0))                                  AS payees_mois_paye,
       ROUND(NVL(ps.montant, 0))                                  AS payees_saisie,
       ROUND(NVL(a.trop_percus, 0))                               AS trop_percus,
       ROUND(NVL(a.moins_percus, 0))                              AS moins_percus,
       ROUND(NVL(a.remb_trop_percus, 0))                          AS remb_trop_percus,
       ROUND(100 * NVL(a.trop_percus, 0) / NULLIF(pm.montant, 0), 3) AS taux_indus_pct
FROM mois m
CROSS JOIN branches b
LEFT JOIN payees_mois   pm ON pm.branche = b.branche AND pm.mois = m.mois
LEFT JOIN payees_saisie ps ON ps.branche = b.branche AND ps.mois = m.mois
LEFT JOIN ajustements   a  ON a.branche  = b.branche AND a.mois  = m.mois
ORDER BY b.branche, m.mois;

-- Résultat obtenu (P105, exécutée le 01/11/2026) : 58 732 916 866 payés en
-- 2024 au mois payé, toutes branches.
-- Lecture (calculé ici) : les rappels versés par ajustement font
--   408 312 511 ÷ 58 732 916 866 = 0,7 % des prestations payées. Ils ne sont
--   ni dans les payées de T31, ni dans les prestations servies de T56.
