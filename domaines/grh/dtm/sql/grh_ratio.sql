-- DTM_GRH_RATIO — Ratios d'encadrement et financiers GRH
-- :1 = CLICHE (MMYYYY) — filtre le snapshot source et étiquette les lignes insérées
WITH
-- ── Toutes les années présentes dans le snapshot ───────────────────────────
ANNEES AS (
    SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS ANNEE
    FROM DWH.FAIT_GRH_SITUATION s
    WHERE s.CLICHE        = :1
      AND s.DATE_DEBUT   IS NOT NULL
),
-- ── Date de référence 31/12 par exercice ──────────────────────────────────
REF AS (
    SELECT
        ANNEE,
        TO_NUMBER(TO_CHAR(ANNEE) || '1231')           AS ID_TEMPS,
        TO_DATE(TO_CHAR(ANNEE)   || '1231', 'YYYYMMDD') AS DT_REF
    FROM ANNEES
),
-- ── Effectif actif au 31/12 par exercice ──────────────────────────────────
EFFECTIF AS (
    SELECT
        r.ID_TEMPS,
        COUNT(DISTINCT s.PERS_ID) AS NB_TOTAL,

        COUNT(DISTINCT
            CASE WHEN s.QUAL_CODE IN ('S','M')
                 THEN s.PERS_ID END) AS NB_ENC,

        COUNT(DISTINCT
            CASE WHEN s.QUAL_CODE IN ('I','A')
                 THEN s.PERS_ID END) AS NB_NENC,

        COUNT(DISTINCT
            CASE WHEN s.QUAL_CODE NOT IN ('S','M','I','A')
                  OR s.QUAL_CODE IS NULL
                 THEN s.PERS_ID END) AS NB_INC

    FROM REF r
    INNER JOIN DWH.FAIT_GRH_SITUATION s
        ON  s.DATE_DEBUT <= r.DT_REF
        AND (s.DATE_FIN IS NULL OR s.DATE_FIN > r.DT_REF)
        AND s.CLICHE       = :1
        AND s.STATUT_EMPLOYE = 'A'
        AND UPPER(TRIM(s.NATURE_SITUATION))
              IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')
        AND s.DATE_DEBUT  >= DATE '2000-01-01'
        AND NVL(s.VALIDE, 'N') = 'O'
    GROUP BY r.ID_TEMPS
)

SELECT
    e.ID_TEMPS,
    e.NB_TOTAL  AS NB_AGENTS_TOTAL,
    e.NB_ENC    AS NB_ENCADRANTS,
    e.NB_NENC   AS NB_NON_ENCADRANTS,
    e.NB_INC    AS NB_QUAL_INCONNU,
    ROUND(e.NB_ENC / NULLIF(e.NB_TOTAL, 0), 4) AS TAUX_ENCADREMENT,
    NULL AS MASSE_SALARIALE,
    NULL AS FRAIS_PERSONNEL,
    NULL AS CHARGES_FONCTIONNEMENT,
    NULL AS RATIO_FP_CHARGES,
    NULL AS COUT_FORMATION,
    NULL AS BUDGET_FORMATION,
    NULL AS RATIO_FORM_MSA,
    NULL AS RATIO_FORM_BUDGET,
    :1   AS CLICHE
FROM EFFECTIF e
