-- DTM_MED_EMPLOYEUR
-- Grain   : EMP_ID x ANNEE_MED x DR_NO x EMP_REGIME x SA_NO x TRANCHE_EFFECTIF
-- Sources : DWH.FAIT_MISE_EN_DEMEURE + DWH.FAIT_PERIODE_COTISATION
--           + DWH.FAIT_TRANSACTION_COTISATION + DWH.FAIT_EMPLOYEUR
-- CTE 1   : med_agg  — MED agrégées par EMP_ID × ANNEE_MED
-- CTE 2   : cot_agg  — impayés cotisation sur périodes couvertes par MED
-- Note    : TXCO_SOUS_TYPE PF='PF' | AT='RP' | AV='PE' (valeurs réelles source)
-- Exclus  : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH
-- ── CTE 1 : agrégats MED par employeur × année ─────────────────
med_agg AS (
    SELECT
        m.EMP_ID,
        EXTRACT(YEAR FROM m.MED_DATE)                               AS ANNEE_MED,
        NVL(m.DR_NO, 0)                                             AS DR_NO,
        COUNT(CASE WHEN NVL(m.MED_STATUT, 'X') <> 'A' THEN 1 END)  AS NB_MED_EMISES,
        COUNT(CASE WHEN m.MED_STATUT = 'N' THEN 1 END)              AS NB_MED_EN_COURS,
        COUNT(CASE WHEN m.MED_STATUT = 'V' THEN 1 END)              AS NB_MED_REGLEES,
        COUNT(CASE WHEN m.MED_STATUT = 'A' THEN 1 END)              AS NB_MED_ANNULEES,
        COUNT(CASE WHEN m.CTL_ID IS NOT NULL THEN 1 END)             AS NB_MED_AVEC_CONTROLE,
        SUM(NVL(m.MED_MNT_CP,       0))                             AS MONTANT_PRINCIPAL,
        SUM(NVL(m.MED_MNT_RT,       0))                             AS MONTANT_MAJORATION,
        SUM(NVL(m.MED_MNT_PENALITE, 0))                             AS MONTANT_PENALITE,
        SUM(NVL(m.MED_MNT_ARRIERE,  0))                             AS MONTANT_ARRIERE,
        SUM(NVL(m.MED_RT_ARRIERE,   0))                             AS MONTANT_MAJORATION_ARRIERE,
        SUM(NVL(m.MED_NP_ARRIERE,   0))                             AS MONTANT_NP_ARRIERE,
        SUM(NVL(m.MED_MNT_CP, 0) + NVL(m.MED_MNT_RT, 0)
          + NVL(m.MED_MNT_PENALITE, 0) + NVL(m.MED_MNT_ARRIERE, 0)) AS MONTANT_TOTAL_RECLAME,
        MIN(m.MED_DATE)                                             AS DATE_PREMIERE_MED,
        MAX(m.MED_DATE)                                             AS DATE_DERNIERE_MED,
        ROUND(AVG(CASE WHEN m.MED_DATE_ACCUSE  IS NOT NULL
                       THEN m.MED_DATE_ACCUSE  - m.MED_DATE END), 1) AS DELAI_MOYEN_REPONSE,
        ROUND(AVG(CASE WHEN m.MED_DATE_VIGUEUR IS NOT NULL
                       THEN m.MED_DATE_VIGUEUR - m.MED_DATE END), 1) AS DELAI_MOYEN_VIGUEUR,
        MIN(m.MED_PER_DU)                                           AS PER_DU_MIN,
        MAX(m.MED_PER_AU)                                           AS PER_AU_MAX,
        MIN(m.CLICHE)                                               AS CLICHE
    FROM DWH.FAIT_MISE_EN_DEMEURE m
    WHERE m.CLICHE = :1
      AND m.MED_DATE IS NOT NULL
    GROUP BY m.EMP_ID, EXTRACT(YEAR FROM m.MED_DATE), NVL(m.DR_NO, 0)
),
-- ── CTE 2 : impayés cotisation sur périodes couvertes par MED ──
cot_agg AS (
    SELECT
        pc.EMP_ID,
        -- Appelé par branche (TXCO_SOUS_TYPE réels : PF='PF', AT='RP', AV='PE')
        SUM(CASE WHEN tx.TXCO_TYPE IN ('DD','DR','DT') AND tx.TXCO_SOUS_TYPE = 'PF'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS APPELE_PF,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('DD','DR','DT') AND tx.TXCO_SOUS_TYPE = 'RP'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS APPELE_AT,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('DD','DR','DT') AND tx.TXCO_SOUS_TYPE = 'PE'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS APPELE_AV,
        -- Encaissé par branche
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RP','RA') AND tx.TXCO_SOUS_TYPE = 'PF'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS ENCAISSE_PF,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RP','RA') AND tx.TXCO_SOUS_TYPE = 'RP'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS ENCAISSE_AT,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RP','RA') AND tx.TXCO_SOUS_TYPE = 'PE'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS ENCAISSE_AV,
        -- Renversement par branche
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RR','RU','RM') AND tx.TXCO_SOUS_TYPE = 'PF'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS RENVERSEMENT_PF,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RR','RU','RM') AND tx.TXCO_SOUS_TYPE = 'RP'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS RENVERSEMENT_AT,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RR','RU','RM') AND tx.TXCO_SOUS_TYPE = 'PE'
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS RENVERSEMENT_AV,
        -- Totaux transversaux
        SUM(CASE WHEN tx.TXCO_TYPE IN ('DD','DR','DT')
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS APPELE_TOTAL,
        SUM(CASE WHEN tx.TXCO_TYPE IN ('RP','RA')
                 THEN tx.TXCO_MONTANT ELSE 0 END)                   AS ENCAISSE_TOTAL,
        -- Nombre de périodes avec impayé
        COUNT(DISTINCT CASE
            WHEN tx.TXCO_TYPE IN ('DD','DR','DT') THEN pc.PECO_ID
        END)                                                        AS NB_PERIODES_IMPAYEES
    FROM DWH.FAIT_PERIODE_COTISATION       pc
    INNER JOIN (
        SELECT EMP_ID, PER_DU_MIN, PER_AU_MAX
        FROM med_agg
    ) mf ON  mf.EMP_ID = pc.EMP_ID
         AND pc.PER_ID BETWEEN NVL(mf.PER_DU_MIN, 0) AND NVL(mf.PER_AU_MAX, 999999)
    LEFT JOIN DWH.FAIT_TRANSACTION_COTISATION tx ON tx.PECO_ID = pc.PECO_ID
    GROUP BY pc.EMP_ID
)
-- ── Requête principale ─────────────────────────────────────────
SELECT
    ma.EMP_ID,
    ma.ANNEE_MED,
    ma.DR_NO,
    NVL(e.EMP_REGIME, 'X')                                          AS EMP_REGIME,
    NVL(e.SA_NO,       0)                                           AS SA_NO,
    tef.TEF_CODE                                         AS TEF_CODE,
    -- ── LIBELLÉS ─────────────────────────────────────────────────
    dr.DR_DESC                                                      AS LIBELLE_DR,
    CASE NVL(e.EMP_REGIME, 'X')
        WHEN 'G' THEN 'Regime General'
        WHEN 'V' THEN 'Assure Volontaire'
        WHEN 'M' THEN 'Gens de Maison'
        ELSE 'Non determine'
    END                                                             AS LIBELLE_REGIME,
    sa.SA_DESC,
    -- ── VOLUMÉTRIE MED ───────────────────────────────────────────
    ma.NB_MED_EMISES,
    ma.NB_MED_EN_COURS,
    ma.NB_MED_REGLEES,
    ma.NB_MED_ANNULEES,
    -- ── MONTANTS MED ─────────────────────────────────────────────
    ma.MONTANT_PRINCIPAL,
    ma.MONTANT_MAJORATION,
    ma.MONTANT_PENALITE,
    ma.MONTANT_ARRIERE,
    ma.MONTANT_MAJORATION_ARRIERE,
    ma.MONTANT_NP_ARRIERE,
    ma.MONTANT_TOTAL_RECLAME,
    -- ── IMPAYÉS COTISATIONS (APPELE - ENCAISSE + RENVERSEMENT) ───
    NVL(ca.APPELE_PF, 0) - NVL(ca.ENCAISSE_PF, 0) + NVL(ca.RENVERSEMENT_PF, 0) AS IMPAYE_COUVERT_PF,
    NVL(ca.APPELE_AT, 0) - NVL(ca.ENCAISSE_AT, 0) + NVL(ca.RENVERSEMENT_AT, 0) AS IMPAYE_COUVERT_AT,
    NVL(ca.APPELE_AV, 0) - NVL(ca.ENCAISSE_AV, 0) + NVL(ca.RENVERSEMENT_AV, 0) AS IMPAYE_COUVERT_AV,
    (NVL(ca.APPELE_PF, 0) + NVL(ca.APPELE_AT, 0) + NVL(ca.APPELE_AV, 0))
  - (NVL(ca.ENCAISSE_PF, 0) + NVL(ca.ENCAISSE_AT, 0) + NVL(ca.ENCAISSE_AV, 0))
  + (NVL(ca.RENVERSEMENT_PF, 0) + NVL(ca.RENVERSEMENT_AT, 0) + NVL(ca.RENVERSEMENT_AV, 0))
                                                                    AS TOTAL_IMPAYE_COUVERT,
    NVL(ca.APPELE_TOTAL,   0)                                       AS APPELE_TOTAL,
    NVL(ca.ENCAISSE_TOTAL, 0)                                       AS ENCAISSE_TOTAL,
    ROUND(NVL(ca.ENCAISSE_TOTAL, 0)
          / NULLIF(NVL(ca.APPELE_TOTAL, 0), 0) * 100, 2)           AS TAUX_RECOUVREMENT,
    -- ── TEMPORALITÉ MED ──────────────────────────────────────────
    ma.DATE_PREMIERE_MED,
    ma.DATE_DERNIERE_MED,
    ma.DELAI_MOYEN_REPONSE,
    ma.DELAI_MOYEN_VIGUEUR,
    -- ── PÉRIODES COUVERTES ────────────────────────────────────────
    ma.PER_DU_MIN,
    ma.PER_AU_MAX,
    -- ── INDICATEURS ──────────────────────────────────────────────
    ROUND(ma.NB_MED_AVEC_CONTROLE
          / NULLIF(ma.NB_MED_EMISES, 0) * 100, 2)                  AS TAUX_AVEC_CONTROLE,
    ma.NB_MED_AVEC_CONTROLE,
    NVL(ca.NB_PERIODES_IMPAYEES, 0)                                 AS NB_PERIODES_IMPAYEES,
    -- ── CLICHE ───────────────────────────────────────────────────
    ma.CLICHE                                                       AS CLICHE
FROM med_agg                              ma
LEFT JOIN DWH.FAIT_EMPLOYEUR              e   ON  e.EMP_ID  = ma.EMP_ID
LEFT JOIN DTM.DIM_SECTEUR_ACTIVITE        sa  ON  sa.SA_NO  = e.SA_NO
LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF        tef ON  e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
LEFT JOIN DTM.DIM_DIRECTION_REGIONALE     dr  ON  dr.DR_NO  = ma.DR_NO
LEFT JOIN cot_agg                         ca  ON  ca.EMP_ID = ma.EMP_ID
