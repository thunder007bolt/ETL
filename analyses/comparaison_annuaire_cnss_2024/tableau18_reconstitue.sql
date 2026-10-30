-- =============================================================================
-- Annuaire CNSS, tableau 18 — Évolution des bénéficiaires de pension selon le
-- type de pension, sur 5 ans
-- Écrite le 30/10/2026 pour la comparaison avec l'annuaire statistique de
-- la CNSS (n° 32, année 2024, p. 16) : reproduit son tableau 18 avec la
-- définition retrouvée par requetes_comparaison.sql (A01). Pas une requête
-- de production : elle ne fait pas partie du suivi des prestations.
-- Sources : DWH.FAIT_PRESTATION_ESP, FAIT_INDIVIDU (copies des tables de
--           USER_DWH).
-- Définition (A01, A02) : un bénéficiaire de pension au 31/12 de l'année est
--   une pension en cours à cette date (effet avant, ni radiée, ni suspendue
--   sauf réactivation) dont l'ayant droit est en vie. C'est la définition de
--   la CNSS : la retraite vieillesse tombe à 0,3 à 1,5 % près sur 2020 à 2024,
--   et la répartition par direction du tableau 22 aussi. Ce n'est pas « payée
--   dans l'année », qui donne 2 000 pensionnés de vieillesse de moins.
--   Orphelins : moins de 25 ans, comme dans le tableau 20 (année moins année
--   de naissance) ; limite à confirmer avec la direction des prestations.
-- Écarts connus : la base est une copie au 02/02/2026. Les pensions créées
-- après la publication avec un effet rétroactif, et les radiations saisies
-- après coup, font bouger les chiffres de quelques centaines (A06, A07).
-- :1 = CLICHE (MMYYYY) ; :2 = dernière année du tableau (2024 pour
--      l'annuaire 2024). Colonnes : les 5 années de :2-4 à :2.
-- Type de pension (P34) : RPV retraite vieillesse, RPSV veuve, RPSF veuf,
-- RPSE orphelin, RPI invalidité, RPRA retraite anticipée, RPA ascendant.
-- =============================================================================

WITH

annees AS (
    SELECT TO_NUMBER(:2) - 5 + LEVEL AS annee
    FROM   DUAL
    CONNECT BY LEVEL <= 5
),

pension AS (
    SELECT CASE pe.TPE_CODE
               WHEN 'RPV'  THEN 1
               WHEN 'RPSV' THEN 2
               WHEN 'RPSF' THEN 3
               WHEN 'RPSE' THEN 4
               WHEN 'RPI'  THEN 5
               WHEN 'RPRA' THEN 6
               ELSE             7
           END                                                AS rang,
           TRUNC(pe.PE_DATE_EFFET)                            AS date_effet,
           TRUNC(pe.PE_DATE_RADIATION)                        AS date_radiation,
           TRUNC(pe.PE_DATE_SUSPENSION)                       AS date_suspension,
           TRUNC(pe.PE_DATE_REACTIVATION)                     AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)            AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                            AS date_deces
    FROM      DWH.FAIT_PRESTATION_ESP pe
    LEFT JOIN DWH.FAIT_INDIVIDU       i ON i.IND_ID  = pe.IND_ID_PREST
                                       AND i.CLICHE  = :1
    WHERE pe.CLICHE = :1
      AND pe.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),

-- Une pension compte au 31/12 de l'année si elle est en cours et que
-- l'ayant droit est en vie ; un orphelin, s'il a moins de 25 ans.
beneficiaire AS (
    SELECT a.annee, p.rang
    FROM   annees  a
    JOIN   pension p
           ON  p.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
          AND (p.date_radiation IS NULL
               OR p.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
          AND (   p.date_suspension IS NULL
               OR p.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
               OR (    p.date_reactivation >= p.date_suspension
                   AND p.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
          AND (p.date_deces IS NULL
               OR p.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
          AND (p.rang <> 4 OR a.annee - p.annee_naissance < 25)
),

lignes AS (
    SELECT LEVEL AS rang FROM DUAL CONNECT BY LEVEL <= 7
),

comptage AS (
    SELECT l.rang,
           SUM(CASE WHEN b.annee = TO_NUMBER(:2) - 4 THEN 1 ELSE 0 END) AS n_4,
           SUM(CASE WHEN b.annee = TO_NUMBER(:2) - 3 THEN 1 ELSE 0 END) AS n_3,
           SUM(CASE WHEN b.annee = TO_NUMBER(:2) - 2 THEN 1 ELSE 0 END) AS n_2,
           SUM(CASE WHEN b.annee = TO_NUMBER(:2) - 1 THEN 1 ELSE 0 END) AS n_1,
           SUM(CASE WHEN b.annee = TO_NUMBER(:2)     THEN 1 ELSE 0 END) AS n_0
    FROM      lignes       l
    LEFT JOIN beneficiaire b ON b.rang = l.rang
    GROUP BY l.rang
)

SELECT CASE rang
           WHEN 1 THEN 'Pension de retraite vieillesse'
           WHEN 2 THEN 'Pension de veuves'
           WHEN 3 THEN 'Pension de veufs'
           WHEN 4 THEN 'Pension d''orphelins'
           WHEN 5 THEN 'Pension d''invalidité'
           WHEN 6 THEN 'Pension anticipée'
           ELSE        'Pension d''ascendants'
       END                                                    AS TYPE_DE_PENSION,
       n_4                                                    AS ANNEE_N_MOINS_4,
       n_3                                                    AS ANNEE_N_MOINS_3,
       n_2                                                    AS ANNEE_N_MOINS_2,
       n_1                                                    AS ANNEE_N_MOINS_1,
       n_0                                                    AS ANNEE_N,
       rang                                                   AS RANG
FROM   comptage
UNION ALL
SELECT 'Total', SUM(n_4), SUM(n_3), SUM(n_2), SUM(n_1), SUM(n_0), 8
FROM   comptage
ORDER BY RANG
