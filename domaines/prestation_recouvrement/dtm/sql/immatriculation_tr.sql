-- DTM_IMM_TRAVAILLEUR V4
-- Source    : DWH.FAIT_TRAVAILLEUR (snapshot CLICHE)
-- Grain     : ID_TEMPS x DR_NO x SP_NO x TR_SEXE x TAG_CODE x TR_ETAT
-- Exclus    : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH flux_tr AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(tr.TR_DATE_IMM, 'MM'), 'YYYYMMDD')) AS ID_TEMPS,
        NVL(tr.DR_NO, NVL(sp.DR_NO, 0))          AS DR_NO,
        NVL(tr.SP_NO,               0)            AS SP_NO,
        NVL(tr.TR_SEXE,             0)            AS TR_SEXE,
        CASE NVL(tr.TR_SEXE, 0)
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        NULL
        END                                       AS LIBELLE_SEXE,
        tag.TAG_CODE                              AS TAG_CODE,
        NVL(tr.TR_ETAT,            'X')           AS TR_ETAT,
        COUNT(DISTINCT tr.TR_ID)                  AS NB_NOUVELLES_IMM
    FROM DWH.FAIT_TRAVAILLEUR              tr
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL   sp
           ON sp.SP_NO = NVL(tr.SP_NO, 0)
    LEFT JOIN DTM.DIM_TRANCHE_AGE          tag
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) BETWEEN tag.INF AND tag.SUP
    WHERE tr.TR_DATE_IMM IS NOT NULL
      AND tr.CLICHE = :1
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(tr.TR_DATE_IMM, 'MM'), 'YYYYMMDD')),
        NVL(tr.DR_NO, NVL(sp.DR_NO, 0)),
        NVL(tr.SP_NO,               0),
        NVL(tr.TR_SEXE,             0),
        CASE NVL(tr.TR_SEXE, 0)
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        NULL
        END,
        tag.TAG_CODE,
        NVL(tr.TR_ETAT,            'X')
)
SELECT
    t.ID_TEMPS,
    ft.DR_NO,
    ft.SP_NO,
    ft.TR_SEXE,
    ft.LIBELLE_SEXE,
    ft.TAG_CODE,
    ft.TR_ETAT,
    ft.NB_NOUVELLES_IMM,
    :1                                            AS CLICHE
FROM flux_tr                                   ft
LEFT JOIN DTM.DIM_TEMPS                        t
       ON t.ID_TEMPS = ft.ID_TEMPS
