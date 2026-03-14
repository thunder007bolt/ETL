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
    pc.PER_ID,
    0                                                            AS LP_NO,
    NVL(e.SP_NO, 0)                                             AS SP_NO,
    NVL(e.DR_NO, 0)                                             AS DR_NO,
    'PF'                                                         AS CODE_NATURE,
    NVL(e.EMP_REGIME,      'X')                                 AS EMP_REGIME,
    NVL(e.SA_NO,            0)                                  AS SA_NO,
    NVL(e.EMP_PERIODICITE, 'A')                                 AS EMP_PERIODICITE,
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         ELSE '100+' END                                         AS TRANCHE_EFFECTIF,
    t.ID_TEMPS,
    NVL(dp.ID_PERIODICITE, 0)                                   AS ID_PERIODICITE,
    sp.SP_DESC                                                   AS LIBELLE_SP,
    r.DR_DESC                                                    AS LIBELLE_DR,
    t.ANNEE,
    t.MOIS,
    t.LIBELLE_MOIS,
    t.TRIMESTRE,
    'Prestations Familiales'                                     AS LIBELLE_NATURE,
    CASE NVL(e.EMP_REGIME, 'X')
        WHEN 'G' THEN 'Regime General'
        WHEN 'V' THEN 'Assure Volontaire'
        WHEN 'M' THEN 'Gens de Maison'
        ELSE 'Non determine' END                                  AS LIBELLE_REGIME,
    sa.SA_DESC                                                   AS SA_LIBELLE,
    CASE NVL(e.EMP_PERIODICITE, 'A')
        WHEN 'M' THEN 'Mensuel'
        WHEN 'T' THEN 'Trimestriel'
        ELSE 'Autre' END                                          AS LIBELLE_PERIODICITE,
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
LEFT JOIN  DTM.DIM_SECTEUR_ACTIVITE     sa  ON  sa.SA_NO             = e.SA_NO
LEFT JOIN  DTM.DIM_DIRECTION_REGIONALE  r   ON  r.DR_NO              = e.DR_NO
LEFT JOIN  DTM.DIM_SERVICE_PROVINCIAL   sp  ON  sp.SP_NO             = e.SP_NO
LEFT JOIN  DTM.DIM_PERIODICITE_VERSEMENT dp ON  dp.CODE_PERIODICITE  = e.EMP_PERIODICITE
WHERE pc.CLICHE = :1
GROUP BY
    pc.PER_ID,
    NVL(e.SP_NO, 0),
    NVL(e.DR_NO, 0),
    NVL(e.EMP_REGIME,      'X'),
    NVL(e.SA_NO,            0), sa.SA_DESC,
    NVL(e.EMP_PERIODICITE, 'A'),
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         ELSE '100+' END,
    t.ID_TEMPS, t.ANNEE, t.MOIS, t.LIBELLE_MOIS, t.TRIMESTRE,
    NVL(dp.ID_PERIODICITE, 0),
    sp.SP_DESC,
    r.DR_DESC,
    pc.CLICHE
