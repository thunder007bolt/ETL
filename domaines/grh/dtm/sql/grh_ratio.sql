-- DTM_GRH_RATIO — Ratios d'encadrement par exercice (effectif actif au 31/12)
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
--
-- Règles :
--   - exercices clos depuis 1960, arrêtés au 31/12 ;
--   - effectif : agents présents et actifs, selon les règles de présence et
--     d'activité de DTM_GRH_EFFECTIF ; la date de naissance n'est pas exigée ;
--   - encadrants : qualifications HC, 04, 05, 06 ; non-encadrants : 01, 02, 03 ;
--   - colonnes financières vides en attendant une source comptable.
WITH
-- ── Exercices clos ─────────────────────────────────────────────────────────
ANNEES AS (
    SELECT DISTINCT
        EXTRACT(YEAR FROM DATE_DEBUT)                                    AS ANNEE,
        EXTRACT(YEAR FROM DATE_DEBUT) * 10000 + 1231                    AS ID_TEMPS,
        TO_DATE(EXTRACT(YEAR FROM DATE_DEBUT) || '-12-31', 'YYYY-MM-DD') AS DT_REF
    FROM DWH.FAIT_GRH_SITUATION
    WHERE CLICHE     = :1
      AND DATE_DEBUT IS NOT NULL
      AND EXTRACT(YEAR FROM DATE_DEBUT) BETWEEN 1960 AND EXTRACT(YEAR FROM SYSDATE) - 1
),
-- ── Première sortie datée de chaque agent ─────────────────────────────────
SORTIES AS (
    SELECT PERS_ID, MIN(DT_SORTIE) AS DT_SORTIE
    FROM (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               d.DATE_VIGUEUR             AS DT_SORTIE
        FROM DWH.FAIT_GRH_DECISION d
        LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
               ON  dp.DECI_NO = d.DECI_NO
               AND dp.CLICHE  = :1
        WHERE d.CLICHE       = :1
          AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
          AND d.DATE_VIGUEUR IS NOT NULL
          AND NOT EXISTS (
                SELECT 1
                FROM DWH.FAIT_GRH_DECISION a
                WHERE a.CLICHE    = :1
                  AND a.CDEC_CODE = '40'
                  AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO)
              )
        UNION ALL
        SELECT p.PERS_ID, p.DATE_DECES
        FROM DWH.FAIT_GRH_PERSONNE p
        WHERE p.CLICHE      = :1
          AND p.DATE_DECES IS NOT NULL
    )
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
-- ── Agents présents au 31/12 ──────────────────────────────────────────────
PRESENTS AS (
    SELECT a.ID_TEMPS, a.DT_REF, p.PERS_ID
    FROM ANNEES a
    INNER JOIN DWH.FAIT_GRH_PERSONNE p
        ON  p.CLICHE     = :1
        AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= a.DT_REF
    LEFT JOIN SORTIES so
        ON  so.PERS_ID   = p.PERS_ID
    WHERE p.ETAT_EMPLOYE = 'ACTIF'
       OR so.DT_SORTIE   > a.DT_REF
),
-- ── Dernière situation commencée avant le 31/12 ───────────────────────────
DERNIERE_SITU AS (
    SELECT ID_TEMPS, PERS_ID, QUAL_CODE, STATUT_EMPLOYE
    FROM (
        SELECT
            pr.ID_TEMPS,
            s.PERS_ID,
            s.QUAL_CODE,
            s.STATUT_EMPLOYE,
            ROW_NUMBER() OVER (
                PARTITION BY pr.ID_TEMPS, s.PERS_ID
                ORDER     BY s.DATE_DEBUT DESC, s.SITU_ID DESC
            )                                                     AS RN
        FROM PRESENTS pr
        INNER JOIN DWH.FAIT_GRH_SITUATION s
            ON  s.PERS_ID    = pr.PERS_ID
            AND s.CLICHE     = :1
            AND s.DATE_DEBUT <= pr.DT_REF
    )
    WHERE RN = 1
),
-- ── Effectif actif au 31/12 par exercice ──────────────────────────────────
EFFECTIF AS (
    SELECT
        pr.ID_TEMPS,
        COUNT(DISTINCT pr.PERS_ID)                            AS NB_TOTAL,
        -- Encadrants : Cadres Sup (HC/05/06) + Cadres Moyens (04)
        COUNT(DISTINCT
            CASE WHEN ds.QUAL_CODE IN ('HC','05','06','04')
                 THEN pr.PERS_ID END)                         AS NB_ENC,
        -- Non encadrants : Agents d'exécution (01/02) + Agents de Maîtrise (03)
        COUNT(DISTINCT
            CASE WHEN ds.QUAL_CODE IN ('01','02','03')
                 THEN pr.PERS_ID END)                         AS NB_NENC,
        -- Inconnus : QUAL_CODE NULL ou hors HC/01-06
        COUNT(DISTINCT
            CASE WHEN ds.QUAL_CODE NOT IN ('HC','05','06','04','01','02','03')
                   OR ds.QUAL_CODE IS NULL
                 THEN pr.PERS_ID END)                         AS NB_INC
    FROM PRESENTS pr
    LEFT JOIN DERNIERE_SITU ds
        ON  ds.ID_TEMPS = pr.ID_TEMPS
        AND ds.PERS_ID  = pr.PERS_ID
    -- Actifs : hors détachement, mise à disposition, disponibilité, suspension,
    -- service militaire
    WHERE NVL(ds.STATUT_EMPLOYE, 'A') NOT IN ('B','D','E','F','H')
    GROUP BY pr.ID_TEMPS
)

SELECT
    e.ID_TEMPS,
    e.NB_TOTAL                                      AS NB_AGENTS_TOTAL,
    e.NB_ENC                                        AS NB_ENCADRANTS,
    e.NB_NENC                                       AS NB_NON_ENCADRANTS,
    e.NB_INC                                        AS NB_QUAL_INCONNU,
    ROUND(e.NB_ENC / NULLIF(e.NB_TOTAL, 0), 4)     AS TAUX_ENCADREMENT,
    NULL AS MASSE_SALARIALE,
    NULL AS FRAIS_PERSONNEL,
    NULL AS CHARGES_FONCTIONNEMENT,
    NULL AS RATIO_FP_CHARGES,
    NULL AS COUT_FORMATION,
    NULL AS BUDGET_FORMATION,
    NULL AS RATIO_FORM_MSA,
    NULL AS RATIO_FORM_BUDGET,
    SYSDATE AS DATE_CHARGEMENT,
    :1      AS CLICHE
FROM EFFECTIF e
