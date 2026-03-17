-- DTM_MISE_EN_DEMEURE V4
-- Source     : DWH.FAIT_MISE_EN_DEMEURE + DWH.FAIT_EMPLOYEUR
-- Géographie : DR uniquement (SP/LP absents de FAIT_MISE_EN_DEMEURE)
-- Temps      : DIM_TEMPS via TRUNC(MED_DATE, 'MM') → ID_TEMPS
-- Grain      : ANNEE x MOIS x DR_NO x EMP_REGIME x SA_NO x MED_STATUT x TRANCHE_EFFECTIF
-- Exclus     : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
SELECT
    -- ── GRAIN ──────────────────────────────────────────────────────
    t.ID_TEMPS,
    NVL(m.DR_NO,       0)                                           AS DR_NO,
    NVL(e.EMP_REGIME, 'X')                                          AS EMP_REGIME,
    NVL(e.SA_NO,       0)                                           AS SA_NO,
    NVL(m.MED_STATUT, 'X')                                          AS MED_STATUT,
    tef.TEF_CODE                                                    AS TEF_CODE,
    -- ── MESURES VOLUMÉTRIE ─────────────────────────────────────────
    COUNT(m.MED_ID)                                                 AS NB_MED,
    COUNT(DISTINCT m.EMP_ID)                                        AS NB_EMPLOYEURS,
    COUNT(CASE WHEN m.CTL_ID     IS NOT NULL THEN 1 END)            AS NB_MED_AVEC_CONTROLE,
    COUNT(CASE WHEN m.MED_ID_SUP IS NOT NULL THEN 1 END)            AS NB_MED_ESCALADE,
    -- ── MESURES FINANCIÈRES ────────────────────────────────────────
    SUM(NVL(m.MED_MNT_CP,       0))                                AS MONTANT_PRINCIPAL,
    SUM(NVL(m.MED_MNT_RT,       0))                                AS MONTANT_MAJORATION,
    SUM(NVL(m.MED_MNT_PENALITE, 0))                                AS MONTANT_PENALITE,
    SUM(NVL(m.MED_MNT_ARRIERE,  0))                                AS MONTANT_ARRIERE,
    SUM(NVL(m.MED_RT_ARRIERE,   0))                                AS MONTANT_MAJORATION_ARRIERE,
    SUM(NVL(m.MED_NP_ARRIERE,   0))                                AS MONTANT_NP_ARRIERE,
    SUM(NVL(m.MED_MNT_CP,       0))
    + SUM(NVL(m.MED_MNT_RT,       0))
    + SUM(NVL(m.MED_MNT_PENALITE, 0))
    + SUM(NVL(m.MED_MNT_ARRIERE,  0))                              AS MONTANT_TOTAL_RECLAME,
    -- ── DÉLAIS (jours) ─────────────────────────────────────────────
    ROUND(AVG(CASE WHEN m.MED_DATE_ACCUSE  IS NOT NULL
                   THEN m.MED_DATE_ACCUSE  - m.MED_DATE END), 1)   AS DELAI_MOYEN_REPONSE,
    ROUND(AVG(CASE WHEN m.MED_DATE_VIGUEUR IS NOT NULL
                   THEN m.MED_DATE_VIGUEUR - m.MED_DATE END), 1)   AS DELAI_MOYEN_VIGUEUR,
    ROUND(AVG(m.MED_DELAIS_RGL), 1)                                AS DELAI_MOY_RGL,
    -- ── PÉRIODES COUVERTES ─────────────────────────────────────────
    MIN(m.MED_PER_DU)                                              AS PER_DU_MIN,
    MAX(m.MED_PER_AU)                                              AS PER_AU_MAX,
    -- ── INDICATEURS ────────────────────────────────────────────────
    ROUND(COUNT(CASE WHEN m.CTL_ID IS NOT NULL THEN 1 END)
          / NULLIF(COUNT(m.MED_ID), 0) * 100, 2)                   AS TAUX_AVEC_CONTROLE,
    -- ── CLICHE ─────────────────────────────────────────────────────
    m.CLICHE                                                        AS CLICHE
FROM DWH.FAIT_MISE_EN_DEMEURE          m
LEFT JOIN DWH.FAIT_EMPLOYEUR           e   ON  e.EMP_ID  = m.EMP_ID
                                           AND e.CLICHE   = m.CLICHE
LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF     tef ON  e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
LEFT JOIN DTM.DIM_TEMPS                t   ON  t.ID_TEMPS =
    TO_NUMBER(TO_CHAR(TRUNC(m.MED_DATE, 'MM'), 'YYYYMMDD'))
WHERE m.CLICHE = :1
  AND m.MED_DATE IS NOT NULL
GROUP BY
    t.ID_TEMPS,
    NVL(m.DR_NO,       0),
    NVL(e.EMP_REGIME, 'X'),
    NVL(e.SA_NO,       0),
    NVL(m.MED_STATUT, 'X'),
    tef.TEF_CODE,
    m.CLICHE
