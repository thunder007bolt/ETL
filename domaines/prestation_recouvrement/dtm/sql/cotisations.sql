-- DTM_COTISATIONS — Appels de cotisations consolidés
-- Source     : DWH.FAIT_APPEL + DWH.FAIT_TRANSACTION_DECLARATION + DWH.FAIT_EMPLOYEUR
-- Grain      : mois de préparation × DR × SP × régime × secteur × périodicité
--              × état employeur × tranche effectif × forme juridique
-- :1         : CLICHE (MMYYYY), fourni par le pipeline DTM
-- DATE_CHARGEMENT est exclu du SELECT : DEFAULT SYSDATE sur la table cible.

SELECT
    -- ── Grain ──────────────────────────────────────────────────────
    TO_NUMBER(TO_CHAR(
        TRUNC(a.AP_DATE_PREPARATION, 'MM'),
        'YYYYMMDD'))                                               AS ID_TEMPS,
    NVL(a.DR_NO, NVL(e.DR_NO, 99))                                  AS DR_NO,
    NVL(a.SP_NO, NVL(e.SP_NO, 9999))                                AS SP_NO,
    e.EMP_REGIME,
    NVL(e.SA_NO, 99)                                                AS SA_NO,
    dp.ID_PERIODICITE,
    e.EMP_ETAT,
    tef.TEF_CODE,
    e.EMP_FORME_JURIDIQUE,

    -- Ligne consolidée toutes branches : les montants PF/AT/AV restent séparés
    -- dans les colonnes dédiées ci-dessous.
    CAST(NULL AS VARCHAR2(20))                                      AS CODE_BRANCHE_COTISATION,

    -- ── Appels T16/T17 ─────────────────────────────────────────────
    -- Total des appels préparés sur le mois.
    COUNT(a.AP_ID)                                                  AS NB_APPELS_TOTAL,

    -- Appels rattachés à une transaction de déclaration enregistrée au plus tard
    -- à la fin de la période appelée.
    COUNT(CASE
        WHEN td.TXDE_DATE <= LAST_DAY(TO_DATE(TO_CHAR(a.PER_ID_AU), 'YYYYMM'))
        THEN 1 END)                                                 AS NB_APPELS_ECHEANCE,

    -- Appels rattachés à une transaction de déclaration après la fin de la
    -- période appelée.
    COUNT(CASE
        WHEN td.TXDE_DATE > LAST_DAY(TO_DATE(TO_CHAR(a.PER_ID_AU), 'YYYYMM'))
        THEN 1 END)                                                 AS NB_APPELS_APRES,

    -- Appels sans transaction de déclaration rattachée.
    COUNT(CASE WHEN a.TXDE_ID IS NULL THEN 1 END)                   AS NB_APPELS_SANS_SUITE,

    -- ── Cotisations appelées T18/T19 ───────────────────────────────
    -- Total appelé toutes branches, calculé depuis les masses d'appel.
    SUM(NVL(a.AP_MASSE_PF, 0)
      + NVL(a.AP_MASSE_AT, 0)
      + NVL(a.AP_MASSE_AV, 0))                                      AS MONTANT_APPELE,

    -- Détail par branche conservé sur la ligne consolidée.
    SUM(NVL(a.AP_MASSE_PF, 0))                                      AS MONTANT_APPELE_PF,
    SUM(NVL(a.AP_MASSE_AT, 0))                                      AS MONTANT_APPELE_AT,
    SUM(NVL(a.AP_MASSE_AV, 0))                                      AS MONTANT_APPELE_AV,

    -- Retards et pénalités issus de FAIT_APPEL.
    SUM(NVL(a.AP_MNT_RT, 0))                                        AS MONTANT_APPELE_RT,

    -- FAIT_APPEL ne porte pas le montant NP : colonne maintenue pour la cible.
    CAST(NULL AS NUMBER)                                            AS MONTANT_APPELE_NP,

    -- ── Effectifs ─────────────────────────────────────────────────
    COUNT(DISTINCT a.EMP_ID)                                        AS NB_EMPLOYEURS,

    -- ── Audit ─────────────────────────────────────────────────────
    a.CLICHE                                                        AS CLICHE
FROM DWH.FAIT_APPEL                        a
LEFT JOIN DWH.FAIT_TRANSACTION_DECLARATION td
       ON td.TXDE_ID = a.TXDE_ID
      AND td.CLICHE  = a.CLICHE
LEFT JOIN DWH.FAIT_EMPLOYEUR               e
       ON e.EMP_ID   = a.EMP_ID
      AND e.CLICHE   = a.CLICHE
LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT    dp
       ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF         tef
       ON NVL(e.EMP_NO_TR_DECLAR, 0) BETWEEN tef.INF AND tef.SUP
WHERE a.CLICHE              = :1
  AND a.AP_DATE_PREPARATION IS NOT NULL
GROUP BY
    TO_NUMBER(TO_CHAR(
        TRUNC(a.AP_DATE_PREPARATION, 'MM'),
        'YYYYMMDD')),
    NVL(a.DR_NO, NVL(e.DR_NO, 99)),
    NVL(a.SP_NO, NVL(e.SP_NO, 9999)),
    e.EMP_REGIME,
    NVL(e.SA_NO, 99),
    dp.ID_PERIODICITE,
    e.EMP_ETAT,
    tef.TEF_CODE,
    e.EMP_FORME_JURIDIQUE,
    a.CLICHE
