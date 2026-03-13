-- DTM_CONTROLE V4
-- Source     : DWH.FAIT_CONTROLE + DWH.FAIT_EMPLOYEUR
-- Géographie : DR uniquement (SP et LP absents de FAIT_CONTROLE)
-- Temps      : DIM_TEMPS via TRUNC(CTL_DATE, 'MM') → ID_TEMPS
-- Exclus     : CLICHE et DATE_CHARGEMENT (injectés par le pipeline)
SELECT
    -- ── GRAIN ──────────────────────────────────────────────────────
    TO_NUMBER(TO_CHAR(c.CTL_DATE, 'YYYYMM'))                        AS PER_ID,
    NVL(c.DR_NO, 0)                                                 AS DR_NO,
    NVL(c.CTL_TYPE,   'NA')                                         AS CTL_TYPE,
    NVL(c.CTL_NATURE, 'X')                                          AS CTL_NATURE,
    NVL(c.CTL_STATUT, 'X')                                          AS CTL_STATUT,
    NVL(e.EMP_REGIME,  'X')                                         AS EMP_REGIME,
    NVL(e.SA_NO,        0)                                          AS SA_NO,
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
    -- ── LIBELLÉS GÉOGRAPHIE (DR uniquement) ────────────────────────
    dr.DR_DESC                                                      AS LIBELLE_DR,
    -- ── LIBELLÉS TEMPS ─────────────────────────────────────────────
    t.ANNEE,
    t.MOIS,
    t.LIBELLE_MOIS,
    t.TRIMESTRE,
    -- ── LIBELLÉS CODIFICATIONS ─────────────────────────────────────
    MIN(CASE NVL(c.CTL_TYPE, 'NA')
        WHEN 'CT' THEN 'Controle de terrain'
        WHEN 'EQ' THEN 'Enquete'
        ELSE 'Inconnu'
    END)                                                            AS LIBELLE_CTL_TYPE,
    MIN(CASE NVL(c.CTL_NATURE, 'X')
        WHEN 'I' THEN 'Inopinee'
        WHEN 'P' THEN 'Programmee'
        ELSE 'Inconnu'
    END)                                                            AS LIBELLE_CTL_NATURE,
    MIN(CASE NVL(c.CTL_STATUT, 'X')
        WHEN 'V' THEN 'Valide'
        WHEN 'A' THEN 'Annule'
        WHEN 'E' THEN 'En cours'
        ELSE 'Inconnu'
    END)                                                            AS LIBELLE_CTL_STATUT,
    MIN(CASE NVL(e.EMP_REGIME, 'X')
        WHEN 'G' THEN 'Regime General'
        WHEN 'V' THEN 'Assure Volontaire'
        WHEN 'M' THEN 'Gens de Maison'
        ELSE 'Non determine'
    END)                                                            AS LIBELLE_REGIME,
    MIN(sa.SA_DESC)                                                 AS SA_DESC,
    -- ── MESURES VOLUMÉTRIE ─────────────────────────────────────────
    COUNT(c.CTL_ID)                                                 AS NB_CONTROLES,
    COUNT(DISTINCT c.EMP_ID)                                        AS NB_EMPLOYEURS,
    COUNT(CASE WHEN c.MISE_DEMEURE = 'O' THEN 1 END)               AS NB_AVEC_MED,
    SUM(NVL(c.CTL_NB_SALAIRE, 0))                                  AS NB_SALARIES,
    -- ── MESURES FINANCIÈRES ────────────────────────────────────────
    SUM(NVL(c.CTL_MONTANT_DU,    0))                               AS MONTANT_DU,
    SUM(NVL(c.CTL_MNT_RT,        0))                               AS MONTANT_RT,
    SUM(NVL(c.CTL_MNT_NP,        0))                               AS MONTANT_NP,
    SUM(NVL(c.CTL_MNT_ARRIERE,   0))                               AS MONTANT_ARRIERE,
    SUM(NVL(c.CTL_MNT_PAYE,      0))                               AS MONTANT_PAYE,
    SUM(NVL(c.CTL_ARRIERE_AVOIR, 0))                               AS MONTANT_ARRIERE_AVOIR,
    -- ── INDICATEURS ────────────────────────────────────────────────
    ROUND(COUNT(CASE WHEN c.MISE_DEMEURE = 'O' THEN 1 END)
          / NULLIF(COUNT(c.CTL_ID), 0) * 100, 2)                   AS TAUX_MED,
    ROUND(SUM(NVL(c.CTL_MNT_PAYE, 0))
          / NULLIF(SUM(NVL(c.CTL_MONTANT_DU, 0)), 0) * 100, 2)    AS TAUX_RECOUVREMENT_CTL,
    c.CLICHE                                                         AS CLICHE
FROM DWH.FAIT_CONTROLE                   c
LEFT JOIN DWH.FAIT_EMPLOYEUR             e   ON  e.EMP_ID  = c.EMP_ID
LEFT JOIN DTM.DIM_SECTEUR_ACTIVITE       sa  ON  sa.SA_NO  = e.SA_NO
LEFT JOIN DTM.DIM_TEMPS                  t   ON  t.ID_TEMPS =
    TO_NUMBER(TO_CHAR(TRUNC(c.CTL_DATE, 'MM'), 'YYYYMMDD'))
LEFT JOIN DTM.DIM_DIRECTION_REGIONALE    dr  ON  dr.DR_NO  = c.DR_NO
WHERE c.CLICHE = :1
GROUP BY
    TO_NUMBER(TO_CHAR(c.CTL_DATE, 'YYYYMM')),
    NVL(c.DR_NO, 0),
    NVL(c.CTL_TYPE,   'NA'),
    NVL(c.CTL_NATURE, 'X'),
    NVL(c.CTL_STATUT, 'X'),
    NVL(e.EMP_REGIME,  'X'),
    NVL(e.SA_NO,        0),
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         WHEN e.EMP_NO_TR_DECLAR >= 100              THEN '100+'
         ELSE 'NC' END,
    t.ID_TEMPS,
    t.ANNEE, t.MOIS, t.LIBELLE_MOIS, t.TRIMESTRE,
    dr.DR_DESC,
    c.CLICHE
