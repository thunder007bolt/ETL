-- DTM_SALAIRE V5
-- Sources    : DWH.FAIT_SALAIRE + DWH.FAIT_DECLARATION_NOMINATIVE
--              + DWH.FAIT_TRAVAILLEUR + DWH.FAIT_EMPLOYEUR
-- Géographie : DR <= sp.DR_NO => SP (SP_NO de dn.SP_NO)
-- Temps      : DIM_TEMPS via PER_ID YYYYMM → ID_TEMPS = PER_ID * 100 + 1
-- Filtre     : SAL_STATUT IS NULL OR SAL_STATUT NOT IN ('A','R')
-- Exclus     : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
SELECT
    -- ── GRAIN ──────────────────────────────────────────────────────
    dn.PER_ID,
    NVL(dn.SP_NO,      0)                                           AS SP_NO,
    NVL(sp.DR_NO,      0)                                           AS DR_NO,
    NVL(e.EMP_REGIME, 'X')                                          AS EMP_REGIME,
    NVL(e.SA_NO,       0)                                           AS SA_NO,
    NVL(tr.TR_SEXE,    0)                                           AS TR_SEXE,
    tag.TAG_CODE                                                    AS TAG_CODE,
    tef.TEF_CODE                                         AS TEF_CODE,
    -- ── AXES TEMPORELS ─────────────────────────────────────────────
    t.ANNEE,
    t.MOIS,
    t.TRIMESTRE,
    t.ID_TEMPS,
    -- ── LIBELLÉS CODIFICATIONS ─────────────────────────────────────
    MIN(CASE NVL(tr.TR_SEXE, 0)
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE 'Inconnu'
    END)                                                            AS LIBELLE_SEXE,
    -- ── MESURES VOLUMÉTRIE ─────────────────────────────────────────
    COUNT(s.SAL_ID)                                                 AS NB_DECLARATIONS,
    COUNT(DISTINCT s.TR_ID)                                         AS NB_TRAVAILLEURS,
    COUNT(DISTINCT dn.EMP_ID)                                       AS NB_EMPLOYEURS,
    COUNT(CASE WHEN s.SAL_COTISANT_PF = 'O' THEN 1 END)            AS NB_COTISANTS_PF,
    COUNT(CASE WHEN s.SAL_COTISANT_AT = 'O' THEN 1 END)            AS NB_COTISANTS_AT,
    COUNT(CASE WHEN s.SAL_COTISANT_AV = 'O' THEN 1 END)            AS NB_COTISANTS_AV,
    -- ── MESURES SALARIALES ─────────────────────────────────────────
    SUM(NVL(s.SAL_BASE_COTISATION, 0))                             AS MASSE_BASE_COTISATION,
    SUM(NVL(s.SAL_MONTANT_BRUT,    0))                             AS MASSE_BRUT,
    SUM(NVL(s.SAL_MONTANT_SALAIRE, 0))                             AS MASSE_NET,
    SUM(NVL(s.SAL_NB,              0))                             AS NB_JOURS_TOTAL,
    ROUND(AVG(s.SAL_BASE_COTISATION), 2)                           AS SALAIRE_MOYEN,
    MIN(s.SAL_BASE_COTISATION)                                     AS SALAIRE_MIN,
    MAX(s.SAL_BASE_COTISATION)                                     AS SALAIRE_MAX,
    -- ── CLICHE ─────────────────────────────────────────────────────
    s.CLICHE                                                        AS CLICHE
FROM DWH.FAIT_SALAIRE                          s
INNER JOIN DWH.FAIT_DECLARATION_NOMINATIVE     dn  ON  dn.DN_ID   = s.DN_ID
                                                   AND dn.CLICHE   = s.CLICHE
LEFT JOIN  DWH.FAIT_TRAVAILLEUR                tr  ON  tr.TR_ID    = s.TR_ID
                                                   AND tr.CLICHE   = s.CLICHE
LEFT JOIN  DWH.FAIT_EMPLOYEUR                  e   ON  e.EMP_ID    = dn.EMP_ID
                                                   AND e.CLICHE    = s.CLICHE
LEFT JOIN  DTM.DIM_TEMPS                       t   ON  t.ID_TEMPS  =
    TO_NUMBER(TO_CHAR(dn.PER_ID) || '01')
LEFT JOIN  DTM.DIM_SERVICE_PROVINCIAL          sp  ON  sp.SP_NO    = dn.SP_NO
LEFT JOIN  DTM.DIM_TRANCHE_EFFECTIF            tef ON  e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
LEFT JOIN  DTM.DIM_TRANCHE_AGE                 tag ON  FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) BETWEEN tag.INF AND tag.SUP
WHERE s.CLICHE = :1
  AND (s.SAL_STATUT IS NULL OR s.SAL_STATUT NOT IN ('A', 'R'))
  AND dn.PER_ID IS NOT NULL
GROUP BY
    dn.PER_ID,
    NVL(dn.SP_NO,      0),
    NVL(sp.DR_NO,      0),
    NVL(e.EMP_REGIME, 'X'),
    NVL(e.SA_NO,       0),
    NVL(tr.TR_SEXE,    0),
    tag.TAG_CODE,
    tef.TEF_CODE,
    t.ANNEE, t.MOIS, t.TRIMESTRE, t.ID_TEMPS,
    s.CLICHE
