-- DTM_RECOUVREMENT V4
-- Source     : DWH.FAIT_TRANSACTION_REGLEMENT + DWH.FAIT_EMPLOYEUR
-- Géographie : DR => SP (SP_NO + DR_NO) => LP (LP_NO + SP_NO + DR_NO)
-- Temps      : DIM_TEMPS via TRUNC(TXRE_DATE_EFFECTUE, 'MM') → ID_TEMPS
-- Famille    : REN prioritaire (TXRE_ID_RENVERSE IS NOT NULL), puis TXRE_TYPE
-- Exclus     : CLICHE et DATE_CHARGEMENT (injectés par le pipeline)
SELECT
    -- ── GRAIN ──────────────────────────────────────────────────────
    TO_NUMBER(TO_CHAR(tx.TXRE_DATE_EFFECTUE, 'YYYYMM'))             AS PER_ID,
    NVL(tx.LP_NO, 0)                                                AS LP_NO,
    NVL(tx.SP_NO, 0)                                                AS SP_NO,
    NVL(tx.DR_NO, 0)                                                AS DR_NO,
    CASE
        WHEN tx.TXRE_ID_RENVERSE IS NOT NULL THEN 'REN'
        WHEN tx.TXRE_TYPE = 'M'             THEN 'MAJ'
        WHEN tx.TXRE_TYPE = 'F'             THEN 'FOR'
        WHEN tx.TXRE_TYPE = 'P'             THEN 'PAY'
        ELSE 'INC'
    END                                                             AS FAMILLE,
    NVL(tx.TXRE_MODE_PAIEMENT, 'NA')                               AS TXRE_MODE_PAIEMENT,
    NVL(tx.TXRE_NATURE,        'NC')                               AS TXRE_NATURE,
    NVL(e.EMP_REGIME,          'X')                                AS EMP_REGIME,
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         WHEN e.EMP_NO_TR_DECLAR >= 100              THEN '100+'
         ELSE 'NC' END                                              AS TRANCHE_EFFECTIF,
    -- ── FK TEMPS ───────────────────────────────────────────────────
    t.ID_TEMPS,
    -- ── LIBELLÉS TEMPS ─────────────────────────────────────────────
    t.ANNEE,
    t.MOIS,
    t.TRIMESTRE,
    -- ── MESURES ────────────────────────────────────────────────────
    COUNT(tx.TXRE_ID)                                               AS NB_TRANSACTIONS,
    COUNT(DISTINCT tx.EMP_ID)                                       AS NB_EMPLOYEURS,
    SUM(tx.TXRE_MONTANT)                                            AS MONTANT_TOTAL,
    SUM(NVL(tx.TXRE_MNT_FRAIS, 0))                                 AS MONTANT_FRAIS,
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
LEFT JOIN DTM.DIM_TEMPS                      t   ON  t.ID_TEMPS =
    TO_NUMBER(TO_CHAR(TRUNC(tx.TXRE_DATE_EFFECTUE, 'MM'), 'YYYYMMDD'))
-- (JOINs géographie dimension supprimés — codes suffisent pour le grain)
WHERE tx.CLICHE = :1
GROUP BY
    TO_NUMBER(TO_CHAR(tx.TXRE_DATE_EFFECTUE, 'YYYYMM')),
    NVL(tx.LP_NO, 0),
    NVL(tx.SP_NO, 0),
    NVL(tx.DR_NO, 0),
    CASE
        WHEN tx.TXRE_ID_RENVERSE IS NOT NULL THEN 'REN'
        WHEN tx.TXRE_TYPE = 'M'             THEN 'MAJ'
        WHEN tx.TXRE_TYPE = 'F'             THEN 'FOR'
        WHEN tx.TXRE_TYPE = 'P'             THEN 'PAY'
        ELSE 'INC'
    END,
    NVL(tx.TXRE_MODE_PAIEMENT, 'NA'),
    NVL(tx.TXRE_NATURE,        'NC'),
    NVL(e.EMP_REGIME,          'X'),
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         WHEN e.EMP_NO_TR_DECLAR >= 100              THEN '100+'
         ELSE 'NC' END,
    t.ID_TEMPS,
    t.ANNEE, t.MOIS, t.TRIMESTRE,
    tx.CLICHE
