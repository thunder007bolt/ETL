-- DTM_IMM_TRAVAILLEUR V4
-- Source    : DWH.FAIT_TRAVAILLEUR (snapshot CLICHE)
-- Grain     : ID_TEMPS x DR_NO x SP_NO x TR_SEXE x TAG_CODE x TR_ETAT
-- Exclus    : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH flux_tr AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(tr.TR_DATE_IMM, 'MM'), 'YYYYMMDD')) AS ID_TEMPS,
        NVL(tr.DR_NO, sp.DR_NO)                   AS DR_NO,
        tr.SP_NO,
        tr.TR_SEXE,
        CASE tr.TR_SEXE
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        NULL
        END                                       AS LIBELLE_SEXE,
        tag.TAG_CODE,
        tr.TR_ETAT,
        COUNT(DISTINCT tr.TR_ID)                  AS NB_NOUVELLES_IMM
    FROM DWH.FAIT_TRAVAILLEUR              tr
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL   sp
           ON sp.SP_NO = tr.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE          tag
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE) / 12) BETWEEN tag.INF AND tag.SUP
    WHERE tr.TR_DATE_IMM IS NOT NULL
      AND tr.CLICHE = :1
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(tr.TR_DATE_IMM, 'MM'), 'YYYYMMDD')),
        NVL(tr.DR_NO, sp.DR_NO),
        tr.SP_NO,
        tr.TR_SEXE,
        CASE tr.TR_SEXE
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        NULL
        END,
        tag.TAG_CODE,
        tr.TR_ETAT
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
