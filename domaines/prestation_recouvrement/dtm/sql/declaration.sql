-- DTM_DECLARATION V2 — Étape 1/3 : INSERT de base
-- Sources : DWH.FAIT_DECLARATION_NOMINATIVE  (principal — grain DN_ID)
--           DWH.FAIT_SALAIRE                 (NB_TR_DECLARES, MASSE_SALARIALE)
--           DWH.FAIT_EMPLOYEUR               (régime, secteur, périodicité, effectif)
--           DTM.DIM_SERVICE_PROVINCIAL        (DR_NO de secours)
--           DTM.DIM_FORME_JURIDIQUE           (FJ_CODE)
--           DTM.DIM_PERIODICITE_VERSEMENT     (ID_PERIODICITE)
--           DTM.DIM_TRANCHE_EFFECTIF          (TEF_CODE)
-- Grain   : ID_TEMPS × DR_NO × EMP_REGIME × SA_NO × FJ_CODE × ID_PERIODICITE × TEF_CODE
-- DR_NO   : cascade dn.DR_NO → e.DR_NO → sp.DR_NO
-- NB_EMP_EMIS  = COUNT(DISTINCT EMP_ID) depuis DNM — fallback (écrasé par step 2 pour employeurs T)
-- NB_TR_EMIS   = NULL — mis à jour par declaration_step2.sql
-- NB_TR_RECUS  = NULL — mis à jour par declaration_step3.sql
-- NB_TR_SAISIS = NULL — mis à jour par declaration_step3.sql
-- :1 = CLICHE (MMYYYY)

WITH
sal AS (
    SELECT s.DN_ID,
           COUNT(DISTINCT s.TR_ID)     AS NB_TR,
           SUM(s.SAL_BASE_COTISATION)  AS MASSE_SAL
    FROM DWH.FAIT_SALAIRE s
    WHERE s.CLICHE = :1
      AND (s.SAL_STATUT IS NULL OR s.SAL_STATUT NOT IN ('A','R'))
    GROUP BY s.DN_ID
)
SELECT
    TO_NUMBER(TO_CHAR(TRUNC(dn.DN_DATE,'MM'),'YYYYMMDD'))        AS ID_TEMPS,
    NVL(
        CASE WHEN dn.DR_NO IS NOT NULL THEN dn.DR_NO
             WHEN e.DR_NO  IS NOT NULL THEN e.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO
        END,
        99
    )                                                             AS DR_NO,
    e.EMP_REGIME,
    NVL(e.SA_NO, 99)                                              AS SA_NO,
    NVL(fj.FJ_CODE, 'X')                                         AS FJ_CODE,
    dp.ID_PERIODICITE,
    tef.TEF_CODE,

    COUNT(dn.DN_ID)                                               AS NB_DECLARATIONS,
    COUNT(CASE WHEN dn.DN_VERIFIE = 'O'           THEN 1 END)    AS NB_DECLARATIONS_VERIF,
    COUNT(CASE WHEN dn.DN_ID_SUP IS NOT NULL       THEN 1 END)    AS NB_RECTIFICATIONS,

    -- Fallback : écrasé par declaration_step2.sql pour les employeurs trimestriels
    COUNT(DISTINCT dn.EMP_ID)                                     AS NB_EMP_EMIS,
    COUNT(DISTINCT CASE WHEN dn.DN_VERIFIE IN ('N','O')
                        THEN dn.EMP_ID END)                       AS NB_EMP_RECUS,
    COUNT(DISTINCT CASE WHEN dn.DN_VERIFIE = 'O'
                        THEN dn.EMP_ID END)                       AS NB_EMP_SAISIS,

    -- Remplis par les étapes UPDATE (step 2 et 3)
    NULL                                                          AS NB_TR_RECUS,
    NULL                                                          AS NB_TR_SAISIS,

    COUNT(CASE WHEN dn.DN_TYPE = 'S' THEN 1 END)                 AS NB_DECL_S,
    COUNT(CASE WHEN dn.DN_TYPE = 'C' THEN 1 END)                 AS NB_DECL_C,
    COUNT(CASE WHEN dn.DN_TYPE = 'M' THEN 1 END)                 AS NB_DECL_M,
    COUNT(CASE WHEN dn.DN_TYPE = 'R' THEN 1 END)                 AS NB_DECL_R,
    COUNT(CASE WHEN dn.DN_TYPE NOT IN ('S','C','M','R')
                OR dn.DN_TYPE IS NULL THEN 1 END)                 AS NB_DECL_AUTRES,

    SUM(sal.NB_TR)                                                AS NB_TR_DECLARES,
    NULL                                                          AS NB_TR_EMIS,
    SUM(sal.MASSE_SAL)                                            AS MASSE_SALARIALE,

    ROUND(
        COUNT(DISTINCT CASE WHEN dn.DN_VERIFIE = 'O' THEN dn.EMP_ID END)
        / NULLIF(COUNT(DISTINCT CASE WHEN dn.DN_VERIFIE IN ('N','O')
                       THEN dn.EMP_ID END), 0) * 100, 2)         AS TAUX_VERIFICATION,

    SYSDATE                                                       AS DATE_CHARGEMENT,
    :1                                                            AS CLICHE

FROM DWH.FAIT_DECLARATION_NOMINATIVE     dn
LEFT JOIN sal                            ON sal.DN_ID            = dn.DN_ID
LEFT JOIN DWH.FAIT_EMPLOYEUR             e   ON e.EMP_ID         = dn.EMP_ID
LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL     sp  ON sp.SP_NO         = e.SP_NO
LEFT JOIN DTM.DIM_FORME_JURIDIQUE        fj  ON fj.FJ_CODE       = e.EMP_FORME_JURIDIQUE
LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT  dp  ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF       tef ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
WHERE dn.CLICHE   = :1
  AND dn.DN_DATE >= DATE '1960-01-01'
  AND dn.DN_DATE <= DATE '2099-12-31'
GROUP BY
    TO_NUMBER(TO_CHAR(TRUNC(dn.DN_DATE,'MM'),'YYYYMMDD')),
    NVL(CASE WHEN dn.DR_NO IS NOT NULL THEN dn.DR_NO
             WHEN e.DR_NO  IS NOT NULL THEN e.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO END, 99),
    e.EMP_REGIME, NVL(e.SA_NO, 99), NVL(fj.FJ_CODE,'X'),
    dp.ID_PERIODICITE, tef.TEF_CODE,
    :1
