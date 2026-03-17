-- DTM_IMMATRICULATION_EMPLOYEUR V6
-- Source    : DWH.FAIT_EMPLOYEUR (principale — snapshot CLICHE)
--             DWH.FAIT_DOSSIER_IMMATRICULATION (radiations DI_TYPE=R)
-- Grain     : ANNEE x MOIS x DR_NO x SP_NO x EMP_REGIME x EMP_ETAT
--             x SA_NO x EMP_FORME_JURIDIQUE x ID_PERIODICITE x TEF_CODE
-- Exclus    : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH

-- ── flux immatriculations (date officielle EMP_DATE_IMM) ───────
flux_imm AS (
    SELECT
        EXTRACT(YEAR  FROM e.EMP_DATE_IMM)        AS ANNEE,
        EXTRACT(MONTH FROM e.EMP_DATE_IMM)        AS MOIS,
        NVL(e.DR_NO, NVL(sp.DR_NO, 0))           AS DR_NO,
        NVL(e.SP_NO,                0)            AS SP_NO,
        NVL(e.EMP_REGIME,          'X')           AS EMP_REGIME,
        NVL(e.SA_NO,                0)            AS SA_NO,
        NVL(e.EMP_FORME_JURIDIQUE, 'NC')          AS EMP_FORME_JURIDIQUE,
        NVL(dp.ID_PERIODICITE,      0)            AS ID_PERIODICITE,
        NVL(e.EMP_ETAT,            'X')           AS EMP_ETAT,
        NVL(tef.TEF_CODE,          'NC')          AS TEF_CODE,
        COUNT(DISTINCT e.EMP_ID)                  AS NB_EMP
    FROM DWH.FAIT_EMPLOYEUR                    e
    LEFT JOIN DWH.FAIT_DOSSIER_IMMATRICULATION di
           ON di.EMP_ID        = e.EMP_ID
          AND di.DI_TYPE       = 'N'
          AND di.TR_NOM_PRENOM IS NULL
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
           ON sp.SP_NO         = NVL(e.SP_NO, 0)
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT    dp
           ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF         tef
           ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
    WHERE e.EMP_DATE_IMM IS NOT NULL
      AND e.CLICHE = :1
    GROUP BY
        EXTRACT(YEAR  FROM e.EMP_DATE_IMM),
        EXTRACT(MONTH FROM e.EMP_DATE_IMM),
        NVL(e.DR_NO, NVL(sp.DR_NO, 0)),
        NVL(e.SP_NO,                0),
        NVL(e.EMP_REGIME,          'X'),
        NVL(e.SA_NO,                0),
        NVL(e.EMP_FORME_JURIDIQUE, 'NC'),
        NVL(dp.ID_PERIODICITE,      0),
        NVL(e.EMP_ETAT,            'X'),
        NVL(tef.TEF_CODE,          'NC')
),

-- ── flux radiations (DI_TYPE=R — grain sans EMP_ETAT) ──────────
flux_rad AS (
    SELECT
        EXTRACT(YEAR  FROM di.DI_DATE_RECEPTION)  AS ANNEE,
        EXTRACT(MONTH FROM di.DI_DATE_RECEPTION)  AS MOIS,
        NVL(di.DR_NO, NVL(sp.DR_NO, 0))          AS DR_NO,
        NVL(e.SP_NO,                0)            AS SP_NO,
        NVL(e.EMP_REGIME,          'X')           AS EMP_REGIME,
        NVL(e.SA_NO,                0)            AS SA_NO,
        NVL(e.EMP_FORME_JURIDIQUE, 'NC')          AS EMP_FORME_JURIDIQUE,
        NVL(dp.ID_PERIODICITE,      0)            AS ID_PERIODICITE,
        NVL(tef.TEF_CODE,          'NC')          AS TEF_CODE,
        COUNT(*)                                  AS NB_RAD
    FROM DWH.FAIT_DOSSIER_IMMATRICULATION      di
    LEFT JOIN DWH.FAIT_EMPLOYEUR               e
           ON e.EMP_ID   = di.EMP_ID
          AND e.CLICHE   = :1
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
           ON sp.SP_NO   = NVL(e.SP_NO, 0)
    LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT    dp
           ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
    LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF         tef
           ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP
    WHERE di.DI_TYPE           = 'R'
      AND di.DI_DATE_RECEPTION IS NOT NULL
    GROUP BY
        EXTRACT(YEAR  FROM di.DI_DATE_RECEPTION),
        EXTRACT(MONTH FROM di.DI_DATE_RECEPTION),
        NVL(di.DR_NO, NVL(sp.DR_NO, 0)),
        NVL(e.SP_NO,                0),
        NVL(e.EMP_REGIME,          'X'),
        NVL(e.SA_NO,                0),
        NVL(e.EMP_FORME_JURIDIQUE, 'NC'),
        NVL(dp.ID_PERIODICITE,      0),
        NVL(tef.TEF_CODE,          'NC')
)

SELECT
    fi.ANNEE,
    fi.MOIS,
    fi.DR_NO,
    fi.SP_NO,
    fi.EMP_REGIME,
    fi.SA_NO,
    fi.EMP_FORME_JURIDIQUE,
    fi.ID_PERIODICITE,
    fi.EMP_ETAT,
    fi.TEF_CODE,
    t.ID_TEMPS,
    fi.NB_EMP                               AS NB_NOUVELLE_IMM_EMP,
    NVL(fr.NB_RAD, 0)                       AS NB_RADIATIONS,
    :1                                      AS CLICHE
FROM flux_imm                              fi
LEFT JOIN flux_rad                         fr
       ON fr.ANNEE               = fi.ANNEE
      AND fr.MOIS                = fi.MOIS
      AND fr.DR_NO               = fi.DR_NO
      AND fr.SP_NO               = fi.SP_NO
      AND fr.EMP_REGIME          = fi.EMP_REGIME
      AND fr.SA_NO               = fi.SA_NO
      AND fr.EMP_FORME_JURIDIQUE = fi.EMP_FORME_JURIDIQUE
      AND fr.ID_PERIODICITE      = fi.ID_PERIODICITE
      AND fr.TEF_CODE            = fi.TEF_CODE
LEFT JOIN DTM.DIM_TEMPS                    t
       ON t.ID_TEMPS = TO_NUMBER(TO_CHAR(
              TRUNC(ADD_MONTHS(
                  TO_DATE(fi.ANNEE || '0101', 'YYYYMMDD'),
                  fi.MOIS - 1
              ), 'MM'),
          'YYYYMMDD'))
