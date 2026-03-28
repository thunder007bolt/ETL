-- DTM_COTISATIONS — Branche AT (Accidents du Travail / Risques Professionnels)
-- CTE tx_agg : pré-agrégation FAIT_TRANSACTION_COTISATION par PECO_ID
-- Branche    : TXCO_SOUS_TYPE = 'AT'
-- Temps      : join DTM.DIM_TEMPS via CLICHE (MMYYYY) → ANNEE + MOIS + JOUR=1
-- Note       : MONTANT_MAJORATION/PENALITE/REMISE/AVOIR = 0 (transversaux PF uniquement)
-- Exclus     : CLICHE et DATE_CHARGEMENT (injectés par le pipeline)
WITH tx_agg AS (
    SELECT
        PECO_ID,
        SUM(CASE WHEN TXCO_TYPE IN ('DD','DR','DT') AND TXCO_SOUS_TYPE = 'AT'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_APPELE,
        SUM(CASE WHEN TXCO_TYPE IN ('RP','RA')      AND TXCO_SOUS_TYPE = 'AT'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_ENCAISSE,
        SUM(CASE WHEN TXCO_TYPE IN ('RR','RU','RM') AND TXCO_SOUS_TYPE = 'AT'
                 THEN TXCO_MONTANT ELSE 0 END)                  AS MONTANT_RENVERSEMENT
    FROM DWH.FAIT_TRANSACTION_COTISATION
    WHERE CLICHE = :1
    GROUP BY PECO_ID
)
SELECT
    t.ID_TEMPS,
    0                                                            AS LP_NO,
    e.SP_NO,
    e.DR_NO,
    'AT'                                                         AS CODE_BRANCHE_COTISATION,
    e.EMP_REGIME,
    e.SA_NO,
    dp.ID_PERIODICITE,
    tef.TEF_CODE,
    COUNT(DISTINCT pc.EMP_ID)                                   AS NB_EMPLOYEURS,
    SUM(pc.PECO_NB_TRAV)                                        AS NB_TRAVAILLEURS,
    SUM(NVL(tx.MONTANT_APPELE,       0))                        AS MONTANT_APPELE,
    SUM(NVL(tx.MONTANT_ENCAISSE,     0))                        AS MONTANT_ENCAISSE,
    SUM(NVL(tx.MONTANT_RENVERSEMENT, 0))                        AS MONTANT_RENVERSEMENT,
    SUM(NVL(tx.MONTANT_APPELE, 0)
        - NVL(tx.MONTANT_ENCAISSE, 0)
        - NVL(tx.MONTANT_RENVERSEMENT, 0))                      AS MONTANT_IMPAYE,
    0                                                            AS MONTANT_MAJORATION,
    0                                                            AS MONTANT_PENALITE,
    0                                                            AS MONTANT_REMISE,
    0                                                            AS MONTANT_AVOIR,
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
    t.ID_TEMPS,
    e.SP_NO,
    e.DR_NO,
    e.EMP_REGIME,
    e.SA_NO,
    dp.ID_PERIODICITE,
    tef.TEF_CODE,
    pc.CLICHE
