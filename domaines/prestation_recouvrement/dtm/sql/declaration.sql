-- DTM_DECLARATION V2
-- Sources : DWH.FAIT_DECLARATION_NOMINATIVE  (principal — grain DN_ID)
--           DWH.FAIT_SALAIRE                 (NB_TR_DECLARES, MASSE_SALARIALE)
--           DWH.FAIT_EMPLOYEUR               (régime, secteur, périodicité, effectif)
--           DWH.FAIT_TRANSACTION_DECLARATION  (NB_EMP_EMIS, NB_TR_EMIS, NB_TR_RECUS, NB_TR_SAISIS)
--           DTM.DIM_SERVICE_PROVINCIAL        (DR_NO de secours via SP_NO employeur)
--           DTM.DIM_FORME_JURIDIQUE           (FJ_CODE)
--           DTM.DIM_PERIODICITE_VERSEMENT     (ID_PERIODICITE)
--           DTM.DIM_TRANCHE_EFFECTIF          (TEF_CODE)
-- Grain   : ID_TEMPS × DR_NO × EMP_REGIME × SA_NO × FJ_CODE × ID_PERIODICITE × TEF_CODE
-- DR_NO   : cascade dn.DR_NO → e.DR_NO → sp.DR_NO (DIM_SERVICE_PROVINCIAL)
-- Filtre  : SAL_STATUT IS NULL OR NOT IN ('A','R')
-- Métriques : NB_DECLARATIONS, NB_DECLARATIONS_VERIF, NB_RECTIFICATIONS,
--             NB_EMP_EMIS, NB_EMP_RECUS, NB_TR_RECUS,
--             NB_EMP_SAISIS, NB_TR_SAISIS,
--             NB_DECL_S, NB_DECL_C, NB_DECL_M, NB_DECL_R, NB_DECL_AUTRES,
--             NB_TR_DECLARES, NB_TR_EMIS, MASSE_SALARIALE, TAUX_VERIFICATION
-- NB_EMP_EMIS / NB_TR_EMIS : FAIT_TRANSACTION_DECLARATION (TXDE_TYPE='D', N-1 et N-2)
--   Fallback NB_EMP_EMIS : COUNT(DISTINCT EMP_ID) depuis DNM si TXDE absent
-- NB_TR_RECUS / NB_TR_SAISIS : FAIT_TRANSACTION_DECLARATION (TXDE_TYPE='D', année N-1)
-- :1 = CLICHE (MMYYYY) — snapshot DWH uniforme

WITH

-- ── CTE 1 : salaires par DN_ID ────────────────────────────────────────────
sal AS (
    SELECT s.DN_ID,
           COUNT(DISTINCT s.TR_ID)     AS NB_TR,
           SUM(s.SAL_BASE_COTISATION)  AS MASSE_SAL
    FROM DWH.FAIT_SALAIRE s
    WHERE s.CLICHE = :1
      AND (s.SAL_STATUT IS NULL OR s.SAL_STATUT NOT IN ('A','R'))
    GROUP BY s.DN_ID
),

-- ── CTE 2 : déclarations nominatives agrégées par grain ──────────────────
dcl AS (
    SELECT
        WHEN TO_NUMBER(TO_CHAR(TRUNC(dn.DN_DATE, 'MM'), 'YYYYMMDD')) BETWEEN 19500101 AND 20351231
        THEN TO_NUMBER(TO_CHAR(TRUNC(dn.DN_DATE, 'MM'), 'YYYYMMDD'))
        ELSE 20000101
    END                                                           AS ID_TEMPS,

    -- DR_NO en cascade : déclaration → employeur → service provincial
        CASE WHEN dn.DR_NO IS NOT NULL THEN dn.DR_NO
             WHEN e.DR_NO  IS NOT NULL THEN e.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO
        END                                                       AS DR_NO,
        e.EMP_REGIME,
        e.SA_NO,
        NVL(fj.FJ_CODE, 'X')                                     AS FJ_CODE,
        dp.ID_PERIODICITE,
        tef.TEF_CODE,
        COUNT(dn.DN_ID)                                           AS NB_DECL,
        COUNT(CASE WHEN dn.DN_VERIFIE = 'O' THEN 1 END)          AS NB_VERIF,
        COUNT(CASE WHEN dn.DN_ID_SUP IS NOT NULL THEN 1 END)      AS NB_RECTIF,
        COUNT(DISTINCT dn.EMP_ID)                                 AS NB_EMP_EMIS,
        COUNT(DISTINCT CASE WHEN dn.DN_VERIFIE IN ('N','O')
                            THEN dn.EMP_ID END)                   AS NB_EMP_RECUS,
        COUNT(DISTINCT CASE WHEN dn.DN_VERIFIE = 'O'
                            THEN dn.EMP_ID END)                   AS NB_EMP_SAISIS,
        COUNT(CASE WHEN dn.DN_TYPE = 'S' THEN 1 END)             AS NB_DECL_S,
        COUNT(CASE WHEN dn.DN_TYPE = 'C' THEN 1 END)             AS NB_DECL_C,
        COUNT(CASE WHEN dn.DN_TYPE = 'M' THEN 1 END)             AS NB_DECL_M,
        COUNT(CASE WHEN dn.DN_TYPE = 'R' THEN 1 END)             AS NB_DECL_R,
        COUNT(CASE WHEN dn.DN_TYPE NOT IN ('S','C','M','R')
                    OR dn.DN_TYPE IS NULL THEN 1 END)             AS NB_DECL_AUT,
        SUM(sal.NB_TR)                                            AS NB_TR_DECL,
        SUM(sal.MASSE_SAL)                                        AS MASSE_SAL
    FROM DWH.FAIT_DECLARATION_NOMINATIVE    dn
    LEFT JOIN sal                            ON sal.DN_ID           = dn.DN_ID
    LEFT JOIN DWH.FAIT_EMPLOYEUR             e  ON e.EMP_ID         = dn.EMP_ID
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL     sp ON sp.SP_NO         = e.SP_NO
    LEFT JOIN DTM.DIM_FORME_JURIDIQUE        fj ON fj.FJ_CODE       = e.EMP_FORME_JURIDIQUE
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT  dp ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF       tef ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
    WHERE dn.CLICHE   = :1
      AND dn.DN_DATE >= DATE '1960-01-01'
      AND dn.DN_DATE <= DATE '2099-12-31'
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(dn.DN_DATE,'MM'),'YYYYMMDD')),
        CASE WHEN dn.DR_NO IS NOT NULL THEN dn.DR_NO
             WHEN e.DR_NO  IS NOT NULL THEN e.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO END,
        e.EMP_REGIME, e.SA_NO, NVL(fj.FJ_CODE,'X'),
        dp.ID_PERIODICITE, tef.TEF_CODE
),

-- ── CTE 3 : NB_EMP_EMIS + NB_TR_EMIS (trimestres N-1 et N-2) ────────────
-- ID_TEMPS = premier mois du trimestre suivant (décalage déclaratif trimestriel)
emis AS (
    SELECT
        CASE
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 1
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM')))||'0401')
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 2
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM')))||'0701')
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 3
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM')))||'1001')
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 4
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))+1)||'0101')
        END                                                       AS ID_TEMPS,
        CASE WHEN td.DR_NO IS NOT NULL THEN td.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO
        END                                                       AS DR_NO,
        e.EMP_REGIME,
        e.SA_NO,
        NVL(fj.FJ_CODE, 'X')                                     AS FJ_CODE,
        dp.ID_PERIODICITE,
        tef.TEF_CODE,
        COUNT(DISTINCT td.EMP_ID)                                 AS NB_EMP_PREV,
        SUM(td.TXDE_NB_TOTAL)                                     AS NB_TR_PREV
    FROM DWH.FAIT_TRANSACTION_DECLARATION                          td
    LEFT JOIN DWH.FAIT_EMPLOYEUR            e   ON e.EMP_ID        = td.EMP_ID
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL    sp  ON sp.SP_NO        = e.SP_NO
    LEFT JOIN DTM.DIM_FORME_JURIDIQUE       fj  ON fj.FJ_CODE      = e.EMP_FORME_JURIDIQUE
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT dp  ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF      tef ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
    WHERE td.CLICHE          = :1
      AND td.TXDE_TYPE       = 'D'
      AND td.EMP_PERIODICITE = 'T'
      AND EXTRACT(YEAR FROM TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))
          IN (EXTRACT(YEAR FROM SYSDATE)-1, EXTRACT(YEAR FROM SYSDATE)-2)
    GROUP BY
        CASE
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 1
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM')))||'0401')
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 2
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM')))||'0701')
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 3
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM')))||'1001')
            WHEN CEIL(EXTRACT(MONTH FROM
                 TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))/3) = 4
                 THEN TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                      TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))+1)||'0101')
        END,
        CASE WHEN td.DR_NO IS NOT NULL THEN td.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO END,
        e.EMP_REGIME, e.SA_NO, NVL(fj.FJ_CODE,'X'),
        dp.ID_PERIODICITE, tef.TEF_CODE
),

-- ── CTE 4 : NB_TR_RECUS + NB_TR_SAISIS (année N-1) ──────────────────────
-- Travailleurs attendus vs reçus — basé sur PER_ID_AU de l'année précédente
tr_recus AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(
            TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'),'MM'),'YYYYMMDD'))  AS ID_TEMPS,
        CASE WHEN td.DR_NO IS NOT NULL THEN td.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO
        END                                                               AS DR_NO,
        e.EMP_REGIME,
        e.SA_NO,
        NVL(fj.FJ_CODE, 'X')                                             AS FJ_CODE,
        dp.ID_PERIODICITE,
        tef.TEF_CODE,
        SUM(td.TXDE_NB_TOTAL)                                            AS NB_TR
    FROM DWH.FAIT_TRANSACTION_DECLARATION                                 td
    LEFT JOIN DWH.FAIT_EMPLOYEUR            e   ON e.EMP_ID              = td.EMP_ID
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL    sp  ON sp.SP_NO              = e.SP_NO
    LEFT JOIN DTM.DIM_FORME_JURIDIQUE       fj  ON fj.FJ_CODE            = e.EMP_FORME_JURIDIQUE
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT dp  ON dp.CODE_PERIODICITE   = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF      tef ON e.EMP_NO_TR_DECLAR    BETWEEN tef.INF AND tef.SUP
    WHERE td.CLICHE          = :1
      AND td.TXDE_TYPE       = 'D'
      AND td.EMP_PERIODICITE = 'T'
      AND EXTRACT(YEAR FROM TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'))
          = EXTRACT(YEAR FROM SYSDATE) - 1
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(
            TO_DATE(TO_CHAR(td.PER_ID_AU),'YYYYMM'),'MM'),'YYYYMMDD')),
        CASE WHEN td.DR_NO IS NOT NULL THEN td.DR_NO
             WHEN sp.DR_NO IS NOT NULL THEN sp.DR_NO END,
        e.EMP_REGIME, e.SA_NO, NVL(fj.FJ_CODE,'X'),
        dp.ID_PERIODICITE, tef.TEF_CODE
)

-- ── SELECT FINAL ─────────────────────────────────────────────────────────
SELECT
    d.ID_TEMPS,
    d.DR_NO,
    d.EMP_REGIME,
    d.SA_NO,
    d.FJ_CODE,
    d.ID_PERIODICITE,
    d.TEF_CODE,

    d.NB_DECL                                                    AS NB_DECLARATIONS,
    d.NB_VERIF                                                   AS NB_DECLARATIONS_VERIF,
    d.NB_RECTIF                                                  AS NB_RECTIFICATIONS,

    -- NB_EMP_EMIS : depuis TXDE si disponible, sinon COUNT(DISTINCT EMP_ID) DNM
    NVL(e.NB_EMP_PREV, d.NB_EMP_EMIS)                           AS NB_EMP_EMIS,

    d.NB_EMP_RECUS,
    NVL(r.NB_TR, 0)                                             AS NB_TR_RECUS,
    d.NB_EMP_SAISIS,
    NVL(r.NB_TR, 0)                                             AS NB_TR_SAISIS,

    d.NB_DECL_S,
    d.NB_DECL_C,
    d.NB_DECL_M,
    d.NB_DECL_R,
    d.NB_DECL_AUT                                               AS NB_DECL_AUTRES,

    d.NB_TR_DECL                                                AS NB_TR_DECLARES,
    NVL(e.NB_TR_PREV, 0)                                        AS NB_TR_EMIS,
    d.MASSE_SAL                                                  AS MASSE_SALARIALE,

    ROUND(d.NB_EMP_SAISIS
          / NULLIF(d.NB_EMP_RECUS, 0) * 100, 2)                AS TAUX_VERIFICATION,

    SYSDATE                                                      AS DATE_CHARGEMENT,
    :1                                                           AS CLICHE

FROM dcl                                                         d

LEFT JOIN emis                                                   e
       ON  e.ID_TEMPS        = d.ID_TEMPS
      AND (e.DR_NO           = d.DR_NO           OR (e.DR_NO IS NULL           AND d.DR_NO IS NULL))
      AND (e.EMP_REGIME      = d.EMP_REGIME      OR (e.EMP_REGIME IS NULL      AND d.EMP_REGIME IS NULL))
      AND (e.SA_NO           = d.SA_NO           OR (e.SA_NO IS NULL           AND d.SA_NO IS NULL))
      AND  e.FJ_CODE         = d.FJ_CODE
      AND (e.ID_PERIODICITE  = d.ID_PERIODICITE  OR (e.ID_PERIODICITE IS NULL  AND d.ID_PERIODICITE IS NULL))
      AND (e.TEF_CODE        = d.TEF_CODE        OR (e.TEF_CODE IS NULL        AND d.TEF_CODE IS NULL))

LEFT JOIN tr_recus                                               r
       ON  r.ID_TEMPS        = d.ID_TEMPS
      AND (r.DR_NO           = d.DR_NO           OR (r.DR_NO IS NULL           AND d.DR_NO IS NULL))
      AND (r.EMP_REGIME      = d.EMP_REGIME      OR (r.EMP_REGIME IS NULL      AND d.EMP_REGIME IS NULL))
      AND (r.SA_NO           = d.SA_NO           OR (r.SA_NO IS NULL           AND d.SA_NO IS NULL))
      AND  r.FJ_CODE         = d.FJ_CODE
      AND (r.ID_PERIODICITE  = d.ID_PERIODICITE  OR (r.ID_PERIODICITE IS NULL  AND d.ID_PERIODICITE IS NULL))
      AND (r.TEF_CODE        = d.TEF_CODE        OR (r.TEF_CODE IS NULL        AND d.TEF_CODE IS NULL))
