-- DTM_IMMATRICULATION_EMPLOYEUR V7
-- Source    : DWH.FAIT_EMPLOYEUR (principale — snapshot CLICHE)
--             DWH.FAIT_DOSSIER_IMMATRICULATION (radiations DI_TYPE=R)
-- Grain     : ID_TEMPS x DR_NO x SP_NO x EMP_REGIME x EMP_ETAT
--             x SA_NO x EMP_FORME_JURIDIQUE x ID_PERIODICITE x TEF_CODE
-- Exclus    : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH

-- ── flux immatriculations (date officielle EMP_DATE_IMM) ───────
flux_imm AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_IMM,'MM'),'YYYYMMDD')) AS ID_TEMPS,
        NVL(e.DR_NO, sp.DR_NO)                    AS DR_NO,
        e.SP_NO,
        e.EMP_REGIME,
        e.SA_NO,
        e.EMP_FORME_JURIDIQUE,
        dp.ID_PERIODICITE,
        e.EMP_ETAT,
        tef.TEF_CODE,
        COUNT(DISTINCT e.EMP_ID)                  AS NB_EMP
    FROM DWH.FAIT_EMPLOYEUR                    e
    LEFT JOIN DWH.FAIT_DOSSIER_IMMATRICULATION di
           ON di.EMP_ID        = e.EMP_ID
          AND di.DI_TYPE       = 'N'
          AND di.TR_NOM_PRENOM IS NULL
          AND di.CLICHE        = :1
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
           ON sp.SP_NO         = e.SP_NO
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT    dp
           ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF         tef
           ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
    WHERE e.EMP_DATE_IMM IS NOT NULL
      AND e.CLICHE = :1
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_IMM,'MM'),'YYYYMMDD')),
        NVL(e.DR_NO, sp.DR_NO),
        e.SP_NO,
        e.EMP_REGIME,
        e.SA_NO,
        e.EMP_FORME_JURIDIQUE,
        dp.ID_PERIODICITE,
        e.EMP_ETAT,
        tef.TEF_CODE
),

-- ── flux radiations (DI_TYPE=R — grain sans EMP_ETAT) ──────────
flux_rad AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(di.DI_DATE_RECEPTION,'MM'),'YYYYMMDD')) AS ID_TEMPS,
        NVL(di.DR_NO, sp.DR_NO)                   AS DR_NO,
        e.SP_NO,
        e.EMP_REGIME,
        e.SA_NO,
        e.EMP_FORME_JURIDIQUE,
        dp.ID_PERIODICITE,
        tef.TEF_CODE,
        COUNT(*)                                  AS NB_RAD
    FROM DWH.FAIT_DOSSIER_IMMATRICULATION      di
    LEFT JOIN DWH.FAIT_EMPLOYEUR               e
           ON e.EMP_ID   = di.EMP_ID
          AND e.CLICHE   = :1
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
           ON sp.SP_NO   = e.SP_NO
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT    dp
           ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF         tef
           ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
    WHERE di.CLICHE            = :1
      AND di.DI_TYPE           = 'R'
      AND di.DI_DATE_RECEPTION IS NOT NULL
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(di.DI_DATE_RECEPTION,'MM'),'YYYYMMDD')),
        NVL(di.DR_NO, sp.DR_NO),
        e.SP_NO,
        e.EMP_REGIME,
        e.SA_NO,
        e.EMP_FORME_JURIDIQUE,
        dp.ID_PERIODICITE,
        tef.TEF_CODE
)

SELECT
    t.ID_TEMPS,
    fi.DR_NO,
    fi.SP_NO,
    fi.EMP_REGIME,
    fi.SA_NO,
    fi.EMP_FORME_JURIDIQUE,
    fi.ID_PERIODICITE,
    fi.EMP_ETAT,
    fi.TEF_CODE,
    fi.NB_EMP                               AS NB_NOUVELLE_IMM_EMP,
    fr.NB_RAD                               AS NB_RADIATIONS,
    :1                                      AS CLICHE
FROM flux_imm                              fi
LEFT JOIN flux_rad                         fr
       ON fr.ID_TEMPS            = fi.ID_TEMPS
      AND fr.DR_NO               = fi.DR_NO
      AND fr.SP_NO               = fi.SP_NO
      AND fr.EMP_REGIME          = fi.EMP_REGIME
      AND fr.SA_NO               = fi.SA_NO
      AND fr.EMP_FORME_JURIDIQUE = fi.EMP_FORME_JURIDIQUE
      AND fr.ID_PERIODICITE      = fi.ID_PERIODICITE
      AND fr.TEF_CODE            = fi.TEF_CODE
LEFT JOIN DTM.DIM_TEMPS                    t
       ON t.ID_TEMPS = fi.ID_TEMPS
