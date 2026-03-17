-- DTM_COTISATIONS — Branche PF (Prestations Familiales)
-- CTE tx_agg : pré-agrégation FAIT_TRANSACTION_COTISATION par PECO_ID
-- Branche    : TXCO_SOUS_TYPE = 'PF'
-- Temps      : join DTM.DIM_TEMPS via CLICHE (MMYYYY) → ANNEE + MOIS + JOUR=1
-- Exclus     : CLICHE et DATE_CHARGEMENT (injectés par le pipeline)
WITH tx_agg AS (
    SELECT
        PECO_ID,
        SUM(CASE WHEN TXCO_TYPE IN ('DD','DR','DT') AND TXCO_SOUS_TYPE = 'PF'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_APPELE,
        SUM(CASE WHEN TXCO_TYPE IN ('RP','RA')      AND TXCO_SOUS_TYPE = 'PF'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_ENCAISSE,
        SUM(CASE WHEN TXCO_TYPE IN ('RR','RU','RM') AND TXCO_SOUS_TYPE = 'PF'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_RENVERSEMENT,
        SUM(CASE WHEN TXCO_TYPE = 'DM'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_MAJORATION,
        SUM(CASE WHEN TXCO_TYPE = 'DP'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_PENALITE,
        SUM(CASE WHEN TXCO_TYPE = 'RM' AND TXCO_SOUS_TYPE != 'PF'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_REMISE,
        SUM(CASE WHEN TXCO_TYPE = 'RJ'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_AVOIR
    FROM DWH.FAIT_TRANSACTION_COTISATION
    GROUP BY PECO_ID
)
SELECT
    0                                                            AS LP_NO,
    NVL(e.SP_NO, 0)                                             AS SP_NO,
    NVL(e.DR_NO, 0)                                             AS DR_NO,
    'PF'                                                         AS CODE_NATURE,
    NVL(e.EMP_REGIME,      'X')                                 AS EMP_REGIME,
    NVL(e.SA_NO,            0)                                  AS SA_NO,
    tef.TEF_CODE                                     AS TEF_CODE,
    NVL(dp.ID_PERIODICITE, 0)                                   AS ID_PERIODICITE,
    t.ID_TEMPS,
    COUNT(DISTINCT pc.EMP_ID)                                   AS NB_EMPLOYEURS,
    SUM(NVL(pc.PECO_NB_TRAV, 0))                               AS NB_TRAVAILLEURS,
    SUM(NVL(tx.MONTANT_APPELE,       0))                        AS MONTANT_APPELE,
    SUM(NVL(tx.MONTANT_ENCAISSE,     0))                        AS MONTANT_ENCAISSE,
    SUM(NVL(tx.MONTANT_RENVERSEMENT, 0))                        AS MONTANT_RENVERSEMENT,
    SUM(NVL(tx.MONTANT_APPELE, 0)
        - NVL(tx.MONTANT_ENCAISSE, 0)
        - NVL(tx.MONTANT_RENVERSEMENT, 0))                      AS MONTANT_IMPAYE,
    SUM(NVL(tx.MONTANT_MAJORATION,   0))                        AS MONTANT_MAJORATION,
    SUM(NVL(tx.MONTANT_PENALITE,     0))                        AS MONTANT_PENALITE,
    SUM(NVL(tx.MONTANT_REMISE,       0))                        AS MONTANT_REMISE,
    SUM(NVL(tx.MONTANT_AVOIR,        0))                        AS MONTANT_AVOIR,
    ROUND(SUM(NVL(tx.MONTANT_ENCAISSE, 0))
          / NULLIF(SUM(NVL(tx.MONTANT_APPELE, 0)), 0) * 100
    , 2)                                                         AS TAUX_RECOUVREMENT,
    pc.CLICHE                                                    AS CLICHE
FROM DWH.FAIT_PERIODE_COTISATION        pc
LEFT JOIN  tx_agg                       tx  ON  tx.PECO_ID          = pc.PECO_ID
JOIN       DWH.FAIT_EMPLOYEUR           e   ON  e.EMP_ID             = pc.EMP_ID
                                            AND e.CLICHE             = pc.CLICHE
LEFT JOIN  DTM.DIM_TEMPS                t   ON  t.ID_TEMPS          = TO_NUMBER(TO_CHAR(
        TRUNC(TO_DATE(TO_CHAR(pc.PER_ID), 'YYYYMM'), 'MM'),
        'YYYYMMDD'))
LEFT JOIN  DTM.DIM_PERIODICITE_VERSEMENT dp ON  dp.CODE_PERIODICITE  = e.EMP_PERIODICITE
LEFT JOIN  DTM.DIM_TRANCHE_EFFECTIF      tef ON  e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
WHERE pc.CLICHE = :1
GROUP BY
    NVL(e.SP_NO, 0),
    NVL(e.DR_NO, 0),
    NVL(e.EMP_REGIME,      'X'),
    NVL(e.SA_NO,            0),
    tef.TEF_CODE,
    NVL(dp.ID_PERIODICITE, 0),
    t.ID_TEMPS,
    pc.CLICHE
