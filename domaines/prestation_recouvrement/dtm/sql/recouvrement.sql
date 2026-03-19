-- DTM_RECOUVREMENT V5
-- Source     : DWH.FAIT_TRANSACTION_REGLEMENT + DWH.FAIT_EMPLOYEUR
-- Géographie : DR => SP (SP_NO + DR_NO) => LP (LP_NO + SP_NO + DR_NO)
-- Temps      : DIM_TEMPS via TRUNC(TXRE_DATE_EFFECTUE, 'MM') → ID_TEMPS
-- Exclus     : CLICHE et DATE_CHARGEMENT (injectés par le pipeline)
SELECT
    -- ── GRAIN ──────────────────────────────────────────────────────
    t.ID_TEMPS,
    tx.LP_NO,
    tx.SP_NO,
    tx.DR_NO,
    tx.TXRE_TYPE,
    tx.TXRE_MODE_PAIEMENT,
    tx.TXRE_NATURE,
    e.EMP_REGIME,
    tef.TEF_CODE,
    -- ── MESURES ────────────────────────────────────────────────────
    COUNT(tx.TXRE_ID)                                               AS NB_TRANSACTIONS,
    COUNT(DISTINCT tx.EMP_ID)                                       AS NB_EMPLOYEURS,
    SUM(tx.TXRE_MONTANT)                                            AS MONTANT_TOTAL,
    SUM(tx.TXRE_MNT_FRAIS)                                         AS MONTANT_FRAIS,
    SUM(CASE WHEN tx.TXRE_ID_RENVERSE IS NULL AND tx.TXRE_TYPE = 'P'
             THEN tx.TXRE_MONTANT ELSE 0 END)                       AS MONTANT_PAIEMENT,
    SUM(CASE WHEN tx.TXRE_ID_RENVERSE IS NOT NULL
             THEN tx.TXRE_MONTANT ELSE 0 END)                       AS MONTANT_RENVERSEMENT,
    SUM(CASE WHEN tx.TXRE_TYPE = 'M'
             THEN tx.TXRE_MONTANT ELSE 0 END)                       AS MONTANT_MAJORATION,
    SUM(CASE WHEN tx.TXRE_TYPE = 'F'
             THEN tx.TXRE_MONTANT ELSE 0 END)                       AS MONTANT_FORCE,
    COUNT(CASE WHEN tx.TXRE_ECHEANCIER     IS NOT NULL THEN 1 END)  AS NB_AVEC_ECHEANCIER,
    COUNT(CASE WHEN tx.TXRE_REGULARISATION = 'O'       THEN 1 END)  AS NB_REGULARISATIONS,
    ROUND(
        SUM(CASE WHEN tx.TXRE_ID_RENVERSE IS NOT NULL
                 THEN tx.TXRE_MONTANT ELSE 0 END)
        / NULLIF(
            SUM(CASE WHEN tx.TXRE_ID_RENVERSE IS NULL AND tx.TXRE_TYPE = 'P'
                     THEN tx.TXRE_MONTANT ELSE 0 END)
          , 0) * 100, 2)                                            AS TAUX_RENVERSEMENT,
    tx.CLICHE                                                        AS CLICHE
FROM DWH.FAIT_TRANSACTION_REGLEMENT          tx
LEFT JOIN DWH.FAIT_EMPLOYEUR                 e   ON  e.EMP_ID  = tx.EMP_ID
                                            AND e.CLICHE   = tx.CLICHE
LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF           tef ON  e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
LEFT JOIN DTM.DIM_TEMPS                      t   ON  t.ID_TEMPS =
    TO_NUMBER(TO_CHAR(TRUNC(tx.TXRE_DATE_EFFECTUE, 'MM'), 'YYYYMMDD'))
WHERE tx.CLICHE = :1
GROUP BY
    t.ID_TEMPS,
    tx.LP_NO,
    tx.SP_NO,
    tx.DR_NO,
    tx.TXRE_TYPE,
    tx.TXRE_MODE_PAIEMENT,
    tx.TXRE_NATURE,
    e.EMP_REGIME,
    tef.TEF_CODE,
    tx.CLICHE
