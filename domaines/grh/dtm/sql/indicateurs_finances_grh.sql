-- DTM_INDICATEURS_FINANCES_GRH — Taux d'encadrement par exercice (autres ratios en attente)
-- :1 = CLICHE (MMYYYY) — filtre DTM_GRH_EFFECTIF sur le cliché chargé et étiquette les lignes insérées
--
-- Règles :
--   - lit DTM_GRH_EFFECTIF du même cliché, qui doit être chargé avant cette table
--     (ordre de dtm_config.py) ;
--   - taux d'encadrement : cadres supérieurs (HC, 05, 06) / ensemble des agents présents ;
--   - taux financiers vides en attendant une source comptable.
WITH
ANNEES AS (
    SELECT DISTINCT TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) AS annee
    FROM   DTM.DTM_GRH_EFFECTIF
    WHERE  CLICHE = :1
      AND  TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) IN (
               SELECT AN_ID FROM DTM.DIM_ANNEE
           )
),
EFFECTIF AS (
    SELECT
        TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))                             AS annee,
        SUM(CASE WHEN QUAL_CODE IN ('HC','05','06') THEN NB_AGENTS ELSE 0 END) AS nb_cadres,
        SUM(NB_AGENTS)                                                         AS nb_total
    FROM   DTM.DTM_GRH_EFFECTIF
    WHERE  CLICHE = :1
    GROUP  BY TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))
)
SELECT
    a.annee                                                       AS AN_ID,
    ROUND(NVL(e.nb_cadres, 0) / NULLIF(e.nb_total, 0), 4)        AS TX_ENCADREMENT,
    NULL                                                          AS TX_FRAIS_PERSONNEL,
    NULL                                                          AS TX_CHARGES_ADMIN,
    NULL                                                          AS TX_FORMATION,
    NULL                                                          AS TX_EXECUTION_BUDGETAIRE,
    NVL(e.nb_cadres, 0)                                           AS NB_CADRES,
    NVL(e.nb_total,  0)                                           AS NB_EFFECTIF_TOTAL,
    'TX_FRAIS_PERSONNEL, TX_CHARGES_ADMIN, TX_FORMATION, TX_EXECUTION_BUDGETAIRE : '
    || 'en attente sources (masse sal. personnel CNSS + DWH.FAIT_BUDGET)'             AS OBSERVATION,
    :1                                                            AS CLICHE,
    SYSDATE                                                       AS DATE_CHARGEMENT
FROM       ANNEES  a
LEFT JOIN  EFFECTIF e ON e.annee = a.annee
