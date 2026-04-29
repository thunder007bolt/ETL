-- DTM_PRESTATION_INDUE V4
-- Sources : DWH.FAIT_AJUSTEMENT   (principal — grain AJ_ID)
--           DWH.FAIT_DOSSIER      (branche, géographie)
--           DWH.FAIT_INDIVIDU     (sexe, date naissance)
-- Grain   : ID_TEMPS × TDOS_CODE × TAJ_CODE × TYPE_INDU × AJ_STATUT
--           × DR_NO × SP_NO × LP_NO × SEXE × TAG_CODE
-- Branches : V (PVID), A (AT/MP), F (Prestations Familiales)
-- TYPE_INDU : TROP_PERCU (TP / TP-CONV / REM-TP) | MOINS_PERCU (MP / RC)
-- AJ_STATUT : EN (A) | RC (C) | RJ (R) | IN (autres)
-- Métriques : NB_AJUSTEMENTS, NB_DOSSIERS, NB_BENEFICIAIRES,
--             NB_TROP_PERCU, NB_MOINS_PERCU,
--             MONTANT_INDU, MONTANT_TROP_PERCU, MONTANT_MOINS_PERCU,
--             MONTANT_RECOUVRE, RESTE_A_RECOUVRIR,
--             NB_RECOUVRES, NB_IRRECUPERABLES
-- :1 = CLICHE (YYYYMM) — snapshot DWH uniforme

WITH

-- ── CTE base ajustements enrichis ────────────────────────────────────────
base AS (
    SELECT
        TO_NUMBER(TO_CHAR(
            TRUNC(aj.AJ_DATE_ETABLISSEMENT, 'MM'),
            'YYYYMMDD'))                                     AS ID_TEMPS,

        aj.AJ_ID,
        aj.DOS_CODE,
        aj.IND_ID,
        aj.TAJ_CODE,
        aj.AJ_MONTANT,

        -- Classification indu
        CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP')
             THEN 'TROP_PERCU'
             ELSE 'MOINS_PERCU'
        END                                                  AS TYPE_INDU,

        -- Statut recouvrement
        CASE aj.AJ_STATUT
            WHEN 'A' THEN 'EN'
            WHEN 'C' THEN 'RC'
            WHEN 'R' THEN 'RJ'
            ELSE          'IN'
        END                                                  AS AJ_STATUT,

        NVL(d.TDOS_CODE, 1000)                                AS TDOS_CODE,
        d.DR_NO,
        d.SP_NO,
        d.LP_NO,

        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,

        -- Tranche d'âge au 31/12 de l'année d'établissement
        tag.TAG_CODE

    FROM      DWH.FAIT_AJUSTEMENT               aj

    LEFT JOIN DWH.FAIT_DOSSIER                  d
           ON d.DOS_CODE          = aj.DOS_CODE
          AND d.CLICHE            = :1

    LEFT JOIN DWH.FAIT_INDIVIDU                 ind
           ON ind.IND_ID          = aj.IND_ID
          AND ind.CLICHE          = :1

    LEFT JOIN DTM.DIM_TRANCHE_AGE               tag
           ON TRUNC(
                  MONTHS_BETWEEN(
                      ADD_MONTHS(TRUNC(aj.AJ_DATE_ETABLISSEMENT, 'YYYY'), 12) - 1,
                      ind.IND_DATE_NAISSANCE
                  ) / 12
              ) BETWEEN tag.INF AND tag.SUP

    WHERE aj.CLICHE                   = :1
      AND aj.TAJ_CODE                 IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC')
      AND aj.AJ_DATE_ETABLISSEMENT    IS NOT NULL
      AND d.TDOS_CODE                 IN ('V', 'A', 'F')
)

SELECT
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.TAJ_CODE,
    b.TYPE_INDU,
    b.AJ_STATUT,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE                                               AS SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END                                                      AS LIBELLE_SEXE,
    b.TAG_CODE,

    COUNT(b.AJ_ID)                                           AS NB_AJUSTEMENTS,
    COUNT(DISTINCT b.DOS_CODE)                               AS NB_DOSSIERS,
    COUNT(DISTINCT b.IND_ID)                                 AS NB_BENEFICIAIRES,

    COUNT(CASE WHEN b.TYPE_INDU = 'TROP_PERCU'
               THEN b.AJ_ID END)                             AS NB_TROP_PERCU,
    COUNT(CASE WHEN b.TYPE_INDU = 'MOINS_PERCU'
               THEN b.AJ_ID END)                             AS NB_MOINS_PERCU,

    SUM(b.AJ_MONTANT)                                        AS MONTANT_INDU,
    SUM(CASE WHEN b.TYPE_INDU = 'TROP_PERCU'
             THEN b.AJ_MONTANT ELSE 0 END)                   AS MONTANT_TROP_PERCU,
    SUM(CASE WHEN b.TYPE_INDU = 'MOINS_PERCU'
             THEN b.AJ_MONTANT ELSE 0 END)                   AS MONTANT_MOINS_PERCU,

    SUM(CASE WHEN b.AJ_STATUT = 'RC'
             THEN b.AJ_MONTANT ELSE 0 END)                   AS MONTANT_RECOUVRE,
    SUM(b.AJ_MONTANT) -
    SUM(CASE WHEN b.AJ_STATUT = 'RC'
             THEN b.AJ_MONTANT ELSE 0 END)                   AS RESTE_A_RECOUVRIR,

    COUNT(CASE WHEN b.AJ_STATUT = 'RC'
               THEN b.AJ_ID END)                             AS NB_RECOUVRES,
    COUNT(CASE WHEN b.AJ_STATUT = 'RJ'
               THEN b.AJ_ID END)                             AS NB_IRRECUPERABLES,

    :1                                                       AS CLICHE

FROM base b

GROUP BY
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.TAJ_CODE,
    b.TYPE_INDU,
    b.AJ_STATUT,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END,
    b.TAG_CODE,
    :1
