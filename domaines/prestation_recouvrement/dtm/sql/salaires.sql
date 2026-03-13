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
    CASE
        WHEN tr.TR_DATE_NAISSANCE IS NULL                                        THEN 'NC'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 25     THEN '<25'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 35     THEN '25-34'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 45     THEN '35-44'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 55     THEN '45-54'
        ELSE '55+'
    END                                                             AS TRANCHE_AGE,
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         WHEN e.EMP_NO_TR_DECLAR >= 100              THEN '100+'
         ELSE 'NC' END                                              AS TRANCHE_EFFECTIF,
    -- ── AXES TEMPORELS ─────────────────────────────────────────────
    t.ANNEE,
    t.MOIS,
    t.LIBELLE_MOIS,
    t.TRIMESTRE,
    t.ID_TEMPS,
    -- ── LIBELLÉS GÉOGRAPHIE ────────────────────────────────────────
    dr.DR_DESC                                                      AS LIBELLE_DR,
    sp.SP_DESC                                                      AS LIBELLE_SP,
    -- ── LIBELLÉS CODIFICATIONS ─────────────────────────────────────
    MIN(CASE NVL(tr.TR_SEXE, 0)
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE 'Inconnu'
    END)                                                            AS LIBELLE_SEXE,
    MIN(CASE NVL(e.EMP_REGIME, 'X')
        WHEN 'G' THEN 'Regime General'
        WHEN 'V' THEN 'Assure Volontaire'
        WHEN 'M' THEN 'Gens de Maison'
        ELSE 'Non determine'
    END)                                                            AS LIBELLE_REGIME,
    MIN(sa.SA_DESC)                                                 AS SA_DESC,
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
LEFT JOIN  DTM.DIM_SECTEUR_ACTIVITE            sa  ON  sa.SA_NO    = e.SA_NO
LEFT JOIN  DTM.DIM_TEMPS                       t   ON  t.ID_TEMPS  =
    TO_NUMBER(TO_CHAR(dn.PER_ID) || '01')
LEFT JOIN  DTM.DIM_SERVICE_PROVINCIAL          sp  ON  sp.SP_NO    = dn.SP_NO
LEFT JOIN  DTM.DIM_DIRECTION_REGIONALE         dr  ON  dr.DR_NO    = sp.DR_NO
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
    CASE
        WHEN tr.TR_DATE_NAISSANCE IS NULL                                        THEN 'NC'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 25     THEN '<25'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 35     THEN '25-34'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 45     THEN '35-44'
        WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) < 55     THEN '45-54'
        ELSE '55+'
    END,
    CASE WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0       THEN 'NC'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 20 AND 49  THEN '20-49'
         WHEN e.EMP_NO_TR_DECLAR BETWEEN 50 AND 99  THEN '50-99'
         WHEN e.EMP_NO_TR_DECLAR >= 100              THEN '100+'
         ELSE 'NC' END,
    t.ANNEE, t.MOIS, t.LIBELLE_MOIS, t.TRIMESTRE, t.ID_TEMPS,
    dr.DR_DESC,
    sp.SP_DESC,
    s.CLICHE
