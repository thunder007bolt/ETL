-- =============================================================================
-- Point 35. Le montant d'un tableau
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « les paiements : DEBOURS, DEB_TYPE = PM, DEB_STATUT D
--   ou P, montant DEB_MONTANT. » ; « pour T32 en 2024, 37,67 Md payés aux
--   pensionnés comptés, 38,63 Md en tout. L'annuaire CNSS 2024 publie
--   38,24 Md, entre les deux. »
-- Requête : par tableau et par sexe, le montant payé aux bénéficiaires comptés
--   et tout le payé.
-- Reprise de : lot5_beneficiaires_31_12.sql, P75 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, blocs paie et
--   beneficiaire (le montant payé aux personnes comptées). Fiche des règles :
--   règle 51.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par année, tableau et sexe : personnes comptées au 31/12, montant payé dans
-- l'année aux comptés, tout le payé (débours PM au statut D ou P pour un mois
-- de l'année), personnes payées mais pas comptées.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2024 AS annee FROM DUAL UNION ALL SELECT 2025 FROM DUAL
),
perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
pe AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
),
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.sexe,
           pe.IND_ID_PREST,
           pa.montant,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (pe.tableau NOT IN ('T35', 'T39') OR a.annee - pe.annee_naissance < 25)
                 AND (pe.tableau <> 'T41' OR a.annee - pe.annee_naissance <= 21)
                THEN 1 ELSE 0 END                                 AS compte
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       tableau,
       sexe,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS personnes_comptees,
       ROUND(SUM(CASE WHEN compte = 1 THEN montant END))          AS montant_des_comptes,
       ROUND(SUM(montant))                                        AS montant_tout_le_paye,
       COUNT(DISTINCT CASE WHEN compte = 0 AND montant > 0
                           THEN IND_ID_PREST END)                 AS payes_non_comptes
FROM base
GROUP BY annee, tableau, sexe
ORDER BY annee, tableau, sexe;

-- Résultat obtenu (P75, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P75_montants_2024_2025.tsv), en 2024 :
--   Tableau      Montant des comptés   Tout le payé       Publié
--   T32          37 665 983 112        38 625 184 454     38 235 880 033 (vieillesse seule)
--   T33             112 058 723           116 130 355        113 603 730
--   T34 à T36     7 161 046 495         7 222 987 423      7 047 164 871
--   T41           7 688 913 000         7 954 458 000      8 236 741 500
--   Publiés : annuaire CNSS 2024, tableau 34 (p. 28) pour T32 à T36,
--   tableau 40 (p. 32) pour T41.
-- Lecture : pour T32, 37,67 Md payés aux comptés et 38,63 Md en tout ; le
--   publié, 38,24 Md, tombe entre les deux. Le montant des comptés est le seul
--   qui se range par âge et par sexe.
