-- DTM_CONTROLE_PRESTATION V1
-- Sources : DWH.FAIT_DOSSIERS_CONTROLES (principal — grain DC_ID)
--           DWH.FAIT_DOSSIER             (géographie)
--           DWH.FAIT_PRESTATION_ESP      (flag payé — statut 'F')
-- Grain   : ID_TEMPS × TDOS_CODE × CODE_PRESTATION × DR_NO × SP_NO × LP_NO
-- FLAG_PAYE : dossier ayant au moins une prestation ESP liquidée (PE_STATUT='F')
-- Métriques : NB_CONTROLES, NB_DOSSIERS_CONTROLES, NB_DOSSIERS_PAYES
-- :1 = CLICHE (YYYYMM) — snapshot DWH uniforme

WITH

-- ── CTE base contrôles enrichis ───────────────────────────────────────────
base AS (
    SELECT
        TO_NUMBER(TO_CHAR(
            TRUNC(dc.DC_DATE_CONTROLE, 'MM'),
            'YYYYMMDD'))                                     AS ID_TEMPS,

        dc.DC_ID,
        dc.DOS_CODE,
        dc.TDOS_CODE,
        dc.TPE_CODE                                          AS CODE_PRESTATION,

        d.DR_NO,
        d.SP_NO,
        d.LP_NO,

        -- Dossier payé : au moins une prestation ESP liquidée
        CASE WHEN EXISTS (
            SELECT 1
            FROM   DWH.FAIT_PRESTATION_ESP pe
            WHERE  pe.DOS_CODE  = dc.DOS_CODE
              AND  pe.TPE_CODE  = dc.TPE_CODE
              AND  pe.PE_STATUT = 'F'
              AND  pe.CLICHE    = :1
        ) THEN 1 ELSE 0 END                                  AS FLAG_PAYE

    FROM   DWH.FAIT_DOSSIERS_CONTROLES  dc

    JOIN   DWH.FAIT_DOSSIER             d
        ON d.DOS_CODE            = dc.DOS_CODE
       AND d.CLICHE              = :1

    WHERE  dc.CLICHE             = :1
      AND  dc.DC_DATE_CONTROLE   IS NOT NULL
)

SELECT
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.CODE_PRESTATION,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,

    COUNT(b.DC_ID)              AS NB_CONTROLES,
    COUNT(DISTINCT b.DOS_CODE)  AS NB_DOSSIERS_CONTROLES,
    SUM(b.FLAG_PAYE)            AS NB_DOSSIERS_PAYES,

    :1                          AS CLICHE

FROM base b

GROUP BY
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.CODE_PRESTATION,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    :1
