-- DTM_GRH_RATIO — Ratios d'encadrement par exercice (effectif actif au 31/12)
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
WITH
-- ── Années valides ─────────────────────────────────────────────────────────
ANNEES AS (
    SELECT DISTINCT
        EXTRACT(YEAR FROM DATE_DEBUT)                                    AS ANNEE,
        EXTRACT(YEAR FROM DATE_DEBUT) * 10000 + 1231                    AS ID_TEMPS,
        TO_DATE(EXTRACT(YEAR FROM DATE_DEBUT) || '-12-31', 'YYYY-MM-DD') AS DT_REF
    FROM DWH.FAIT_GRH_SITUATION
    WHERE CLICHE     = :1
      AND DATE_DEBUT IS NOT NULL
      AND EXTRACT(YEAR FROM DATE_DEBUT) BETWEEN 1960 AND 2099
),
-- ── Effectif actif au 31/12 par exercice ──────────────────────────────────
EFFECTIF AS (
    SELECT
        a.ID_TEMPS,
        COUNT(DISTINCT s.PERS_ID)                             AS NB_TOTAL,
        -- Encadrants : Cadres Sup (HC/05/06) + Cadres Moyens (04)
        COUNT(DISTINCT
            CASE WHEN s.QUAL_CODE IN ('HC','05','06','04')
                 THEN s.PERS_ID END)                          AS NB_ENC,
        -- Non encadrants : Agents d'exécution (01/02) + Agents de Maîtrise (03)
        COUNT(DISTINCT
            CASE WHEN s.QUAL_CODE IN ('01','02','03')
                 THEN s.PERS_ID END)                          AS NB_NENC,
        -- Inconnus : QUAL_CODE NULL ou hors HC/01-06
        COUNT(DISTINCT
            CASE WHEN s.QUAL_CODE NOT IN ('HC','05','06','04','01','02','03')
                   OR s.QUAL_CODE IS NULL
                 THEN s.PERS_ID END)                          AS NB_INC
    FROM ANNEES a
    INNER JOIN DWH.FAIT_GRH_SITUATION s
        ON  s.DATE_DEBUT <= a.DT_REF
        AND (s.DATE_FIN IS NULL OR s.DATE_FIN > a.DT_REF)
        AND s.CLICHE     = :1
    WHERE s.STATUT_EMPLOYE = 'A'
      AND UPPER(TRIM(s.NATURE_SITUATION))
              IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')
      AND s.DATE_DEBUT >= DATE '1960-01-01'
      AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1960 AND 2099
    GROUP BY a.ID_TEMPS
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
