-- DTM_DECLARATION V1
-- Sources : DWH.FAIT_DECLARATION_NOMINATIVE (principal — grain DN_ID)
--           DWH.FAIT_SALAIRE               (NB_TR_DECLARES, MASSE_SALARIALE)
--           DWH.FAIT_EMPLOYEUR             (régime, secteur, périodicité, effectif)
--           DTM.DIM_SERVICE_PROVINCIAL     (DR_NO de secours via SP_NO employeur)
--           DTM.DIM_FORME_JURIDIQUE        (FJ_CODE)
--           DTM.DIM_PERIODICITE_VERSEMENT  (ID_PERIODICITE)
--           DTM.DIM_TRANCHE_EFFECTIF       (TEF_CODE)
-- Grain   : ID_TEMPS × DR_NO × EMP_REGIME × SA_NO × FJ_CODE
--           × ID_PERIODICITE × EMP_ETAT × TEF_CODE
-- DR_NO   : cascade dn.DR_NO → e.DR_NO → sp.DR_NO (DIM_SERVICE_PROVINCIAL)
-- Filtre  : SAL_STATUT IS NULL OR NOT IN ('A','R')
-- Métriques : NB_DECLARATIONS, NB_DECLARATIONS_VERIF, NB_RECTIFICATIONS,
--             NB_EMPLOYEURS_DISTINCTS, NB_TR_DECLARES, MASSE_SALARIALE,
--             TAUX_VERIFICATION
-- :1 = CLICHE (YYYYMM) — snapshot DWH uniforme

SELECT
    TO_NUMBER(TO_CHAR(
        TRUNC(dn.DN_DATE, 'MM'),
        'YYYYMMDD'))                                              AS ID_TEMPS,

    -- DR_NO en cascade : déclaration → employeur → service provincial
    CASE WHEN dn.DR_NO IS NOT NULL THEN dn.DR_NO
         WHEN e.DR_NO  IS NOT NULL THEN e.DR_NO
         WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO
    END                                                           AS DR_NO,

    e.EMP_REGIME,
    e.SA_NO,
    fj.FJ_CODE,
    dp.ID_PERIODICITE,
    e.EMP_ETAT,
    tef.TEF_CODE,

    COUNT(dn.DN_ID)                                               AS NB_DECLARATIONS,
    COUNT(CASE WHEN dn.DN_VERIFIE = 'O' THEN 1 END)              AS NB_DECLARATIONS_VERIF,
    COUNT(CASE WHEN dn.DN_ID_SUP IS NOT NULL THEN 1 END)          AS NB_RECTIFICATIONS,
    COUNT(DISTINCT dn.EMP_ID)                                     AS NB_EMPLOYEURS_DISTINCTS,
    COUNT(DISTINCT s.TR_ID)                                       AS NB_TR_DECLARES,
    SUM(s.SAL_BASE_COTISATION)                                    AS MASSE_SALARIALE,

    ROUND(
        COUNT(CASE WHEN dn.DN_VERIFIE = 'O' THEN 1 END)
        / NULLIF(COUNT(dn.DN_ID), 0) * 100, 2)                   AS TAUX_VERIFICATION,

    :1                                                            AS CLICHE

FROM DWH.FAIT_DECLARATION_NOMINATIVE    dn

LEFT JOIN DWH.FAIT_SALAIRE              s
       ON s.DN_ID    = dn.DN_ID
      AND s.CLICHE   = :1
      AND (s.SAL_STATUT IS NULL OR s.SAL_STATUT NOT IN ('A', 'R'))

LEFT JOIN DWH.FAIT_EMPLOYEUR            e
       ON e.EMP_ID   = dn.EMP_ID
      AND e.CLICHE   = :1

LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL    sp
       ON sp.SP_NO   = e.SP_NO

LEFT JOIN DTM.DIM_FORME_JURIDIQUE       fj
       ON fj.FJ_CODE = e.EMP_FORME_JURIDIQUE

LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT dp
       ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE

LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF      tef
       ON e.EMP_NO_TR_DECLAR  BETWEEN tef.INF AND tef.SUP

WHERE dn.CLICHE = :1

GROUP BY
    TO_NUMBER(TO_CHAR(TRUNC(dn.DN_DATE, 'MM'), 'YYYYMMDD')),
    CASE WHEN dn.DR_NO IS NOT NULL THEN dn.DR_NO
         WHEN e.DR_NO  IS NOT NULL THEN e.DR_NO
         WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO
    END,
    e.EMP_REGIME,
    e.SA_NO,
    fj.FJ_CODE,
    dp.ID_PERIODICITE,
    e.EMP_ETAT,
    tef.TEF_CODE,
    :1
