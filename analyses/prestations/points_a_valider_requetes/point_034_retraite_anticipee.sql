-- =============================================================================
-- Point 34. La retraite anticipée (T32)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « PRESTATION_ESP, code RPRA ; la vieillesse ordinaire
--   a le code RPV. » ; « fin 2024, 87 pensions de retraite anticipée en cours
--   dans la base (le nombre de personnes reste à mesurer). L'annuaire CNSS
--   2024 fait pareil : il publie à part ses 78 retraités en retraite
--   anticipée. »
-- Requête : les pensions actives et en vie au 31/12, de 2020 à 2024, par type.
-- Reprise de : comparaison_annuaire_cnss_2024/requetes_comparaison.sql, A01
--   (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc perimetre. Fiche
--   des règles : règle 53.
-- À noter : le bloc perimetre range encore RPRA avec RPV dans T32, donc dans
--   les tranches d'âge. La ligne à part que décrivent ce point et la règle 53
--   n'est pas encore dans la requête (« choix encore à valider », dit son
--   en-tête).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Pensions actives et en vie au 31/12, de 2020 à 2024, par type de pension
-- (retraite vieillesse RPV, anticipée RPRA, veuves, veufs, orphelins,
-- invalidité, ascendants), selon plusieurs définitions.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 5
),
pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
paie AS (
    SELECT d.PE_ID,
           EXTRACT(YEAR FROM d.DEB_DATE_EFFET)                    AS annee,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P') THEN 1 ELSE 0 END) AS paye,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                     AND EXTRACT(MONTH FROM d.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS paye_t4,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                     AND EXTRACT(MONTH FROM d.DEB_DATE_EFFET) = 12
                    THEN 1 ELSE 0 END)                            AS paye_dec,
           SUM(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                    THEN d.DEB_MONTANT ELSE 0 END)                AS montant
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P', 'V')
      AND d.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID, EXTRACT(YEAR FROM d.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.TPE_CODE,
           pe.IND_ID_PREST,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                THEN 1 ELSE 0 END                                 AS actif,
           CASE WHEN pe.date_deces IS NULL
                  OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                THEN 1 ELSE 0 END                                 AS vivant,
           a.annee - pe.annee_naissance                           AS age,
           pa.PE_ID                                               AS pe_paye,
           NVL(pa.paye, 0)                                        AS paye,
           NVL(pa.paye_t4, 0)                                     AS paye_t4,
           NVL(pa.paye_dec, 0)                                    AS paye_dec,
           pa.montant
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       CASE TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(actif)                                                 AS actives_31_12,
       SUM(actif * vivant)                                        AS actives_en_vie,
       SUM(CASE WHEN actif = 1 AND vivant = 1 AND age < 25
                THEN 1 ELSE 0 END)                                AS actives_en_vie_moins_25,
       SUM(paye)                                                  AS payees_annee,
       SUM(CASE WHEN pe_paye IS NOT NULL THEN 1 ELSE 0 END)       AS payees_ou_validees,
       SUM(paye_t4)                                               AS payees_t4,
       SUM(paye_dec)                                              AS payees_decembre,
       SUM(CASE WHEN actif = 1 AND paye = 1 THEN 1 ELSE 0 END)    AS actives_et_payees_annee,
       COUNT(DISTINCT CASE WHEN paye = 1 THEN IND_ID_PREST END)   AS personnes_payees_annee,
       ROUND(SUM(montant))                                        AS montant_paye_annee
FROM base
GROUP BY annee,
         CASE TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY annee, type_pension;

-- Résultat obtenu (A01, exécutée le 30/10/2026 ; détail dans
-- comparaison_annuaire_cnss_2024/resultats/A01_tableau18.tsv), pensions
-- actives et en vie au 31/12, de 2020 à 2024 :
--   retraite vieillesse (RPV)   25 246 / 26 558 / 28 037 / 29 230 / 31 180
--   anticipée (RPRA)                70 /     70 /     78 /     78 /     87
-- Face au tableau 18 de l'annuaire CNSS 2024 (p. 16) : 78 retraités en
-- retraite anticipée en 2024, publiés à part des 30 730 de vieillesse.
-- Lecture : 87 pensions de retraite anticipée en cours fin 2024 (des pensions,
--   pas des personnes) pour 78 publiés.
