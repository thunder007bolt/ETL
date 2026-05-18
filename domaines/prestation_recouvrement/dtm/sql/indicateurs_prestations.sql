-- DTM_INDICATEURS_PRESTATIONS — Indicateurs CIPRES prestations, toutes années × branches
-- :1 = CLICHE (MMYYYY) — étiquette uniquement les lignes insérées (pas de filtre source)
WITH
ANNEES AS (
    SELECT DISTINCT TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) AS annee
    FROM   DTM.DTM_DOSSIER
    WHERE  TDOS_CODE IN ('F','A','V','M')
    INTERSECT
    SELECT DISTINCT TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))
    FROM   DTM.DTM_PRESTATION_INDUE
    WHERE  TDOS_CODE IN ('F','A','V','M')
),
BRANCHES AS (
    SELECT 'F' AS tdos FROM DUAL UNION ALL
    SELECT 'A'         FROM DUAL UNION ALL
    SELECT 'V'         FROM DUAL UNION ALL
    SELECT 'M'         FROM DUAL
),
GRILLE AS (
    SELECT a.annee, b.tdos FROM ANNEES a CROSS JOIN BRANCHES b
),
DOSSIERS AS (
    SELECT
        TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) AS annee,
        TDOS_CODE,
        SUM(NB_INF_15J + NB_15_45J)               AS nb_inf45j,
        SUM(NB_LIQUIDES)                           AS nb_liq,
        SUM(NB_INSTANCE)                           AS nb_inst,
        SUM(NB_DOSSIERS)                           AS nb_dos
    FROM   DTM.DTM_DOSSIER
    WHERE  TDOS_CODE IN ('F','A','V','M')
    GROUP  BY TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)), TDOS_CODE
),
CTRL AS (
    SELECT
        EXTRACT(YEAR FROM a.AJ_DATE_ETABLISSEMENT) AS annee,
        d2.TDOS_CODE,
        COUNT(DISTINCT a.DOS_CODE)                 AS nb_ctrl
    FROM   DWH.FAIT_AJUSTEMENT a
    JOIN   DWH.FAIT_DOSSIER    d2 ON d2.DOS_CODE = a.DOS_CODE
    WHERE  d2.TDOS_CODE IN ('F','A','V','M')
    GROUP  BY EXTRACT(YEAR FROM a.AJ_DATE_ETABLISSEMENT), d2.TDOS_CODE
),
PORTF AS (
    -- Portefeuille total toutes années (dénominateur TX_CONTROLE_POSTERIORI)
    SELECT
        d3.TDOS_CODE,
        COUNT(DISTINCT a.DOS_CODE) AS nb_portefeuille
    FROM   DWH.FAIT_AJUSTEMENT a
    JOIN   DWH.FAIT_DOSSIER    d3 ON d3.DOS_CODE = a.DOS_CODE
    WHERE  d3.TDOS_CODE IN ('F','A','V','M')
    GROUP  BY d3.TDOS_CODE
),
INDUS AS (
    SELECT
        TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))            AS annee,
        TDOS_CODE,
        SUM(MONTANT_TROP_PERCU + MONTANT_MOINS_PERCU)         AS mnt_indu,
        SUM(MONTANT_INDU)                                     AS mnt_ctrl
    FROM   DTM.DTM_PRESTATION_INDUE
    WHERE  TDOS_CODE IN ('F','A','V','M')
    GROUP  BY TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)), TDOS_CODE
),
PRESTA AS (
    SELECT
        TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) AS annee,
        TDOS_CODE,
        SUM(MONTANT_TOTAL)                         AS mnt_paye
    FROM   DTM.DTM_PRESTATION
    WHERE  TDOS_CODE IN ('F','A','V','M')
    GROUP  BY TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)), TDOS_CODE
)
SELECT
    g.annee                                                             AS AN_ID,
    g.tdos                                                              AS TDOS_CODE,
    ROUND(NVL(d.nb_inf45j, 0) / NULLIF(d.nb_liq,          0), 4)      AS TX_DELAI_LIQUIDATION_45J,
    ROUND(NVL(d.nb_inst,   0) / NULLIF(d.nb_dos,          0), 4)      AS TX_RESTE_A_TRAITER,
    ROUND(NVL(c.nb_ctrl,   0) / NULLIF(p.nb_portefeuille, 0), 4)      AS TX_CONTROLE_POSTERIORI,
    ROUND(NVL(i.mnt_indu,  0) / NULLIF(pr.mnt_paye,       0), 4)      AS TX_PRESTATIONS_INDUES,
    NULL                                                                AS TX_REDISTRIBUTION,
    NVL(d.nb_inf45j, 0)                                                 AS NB_DOSSIERS_INF_45J,
    NVL(d.nb_liq,    0)                                                 AS NB_DOSSIERS_LIQUIDES,
    NVL(d.nb_inst,   0)                                                 AS NB_INSTANCES_FIN,
    NVL(d.nb_dos,    0)                                                 AS NB_DOSSIERS_A_TRAITER,
    NVL(c.nb_ctrl,   0)                                                 AS NB_DOSSIERS_CONTROLES,
    NVL(i.mnt_indu,  0)                                                 AS MNT_INDU,
    NVL(i.mnt_ctrl,  0)                                                 AS MNT_TOTAL_CONTROLE,
    NVL(pr.mnt_paye, 0)                                                 AS MNT_PRESTATIONS,
    NULL                                                                AS MNT_PRODUITS_TECHNIQUES,
    :1                                                                  AS CLICHE,
    SYSDATE                                                             AS DATE_CHARGEMENT
FROM       GRILLE   g
LEFT JOIN  DOSSIERS d   ON d.annee     = g.annee AND d.TDOS_CODE = g.tdos
LEFT JOIN  CTRL     c   ON c.annee     = g.annee AND c.TDOS_CODE = g.tdos
LEFT JOIN  PORTF    p   ON p.TDOS_CODE = g.tdos
LEFT JOIN  INDUS    i   ON i.annee     = g.annee AND i.TDOS_CODE = g.tdos
LEFT JOIN  PRESTA   pr  ON pr.annee    = g.annee AND pr.TDOS_CODE = g.tdos
