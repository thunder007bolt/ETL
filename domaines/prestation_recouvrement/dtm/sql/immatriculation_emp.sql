-- DTM_IMMATRICULATION_EMPLOYEUR V5
-- Source    : DWH.FAIT_EMPLOYEUR (principale — snapshot CLICHE)
--             DWH.FAIT_DOSSIER_IMMATRICULATION (radiations DI_TYPE=R)
-- Grain     : ANNEE x MOIS x DR_NO x SP_NO x EMP_REGIME x EMP_ETAT
--             x SA_NO x EMP_FORME_JURIDIQUE x EMP_PERIODICITE x TRANCHE_EFFECTIF
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
        NVL(e.EMP_PERIODICITE,     'X')           AS EMP_PERIODICITE,
        NVL(e.EMP_ETAT,            'X')           AS EMP_ETAT,
        CASE
            WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0        THEN 'NC'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
            WHEN e.EMP_NO_TR_DECLAR >= 20               THEN '20+'
            ELSE 'NC'
        END                                       AS TRANCHE_EFFECTIF,
        COUNT(DISTINCT e.EMP_ID)                  AS NB_EMP
    FROM DWH.FAIT_EMPLOYEUR                    e
    LEFT JOIN DWH.FAIT_DOSSIER_IMMATRICULATION di
           ON di.EMP_ID        = e.EMP_ID
          AND di.DI_TYPE       = 'N'
          AND di.TR_NOM_PRENOM IS NULL
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
           ON sp.SP_NO         = NVL(e.SP_NO, 0)
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
        NVL(e.EMP_PERIODICITE,     'X'),
        NVL(e.EMP_ETAT,            'X'),
        CASE
            WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0        THEN 'NC'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
            WHEN e.EMP_NO_TR_DECLAR >= 20               THEN '20+'
            ELSE 'NC'
        END
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
        NVL(e.EMP_PERIODICITE,     'X')           AS EMP_PERIODICITE,
        CASE
            WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0        THEN 'NC'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
            WHEN e.EMP_NO_TR_DECLAR >= 20               THEN '20+'
            ELSE 'NC'
        END                                       AS TRANCHE_EFFECTIF,
        COUNT(*)                                  AS NB_RAD
    FROM DWH.FAIT_DOSSIER_IMMATRICULATION      di
    LEFT JOIN DWH.FAIT_EMPLOYEUR               e
           ON e.EMP_ID   = di.EMP_ID
          AND e.CLICHE   = :1
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
           ON sp.SP_NO   = NVL(e.SP_NO, 0)
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
        NVL(e.EMP_PERIODICITE,     'X'),
        CASE
            WHEN NVL(e.EMP_NO_TR_DECLAR, 0) = 0        THEN 'NC'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 1  AND 4   THEN '1-4'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 5  AND 9   THEN '5-9'
            WHEN e.EMP_NO_TR_DECLAR BETWEEN 10 AND 19  THEN '10-19'
            WHEN e.EMP_NO_TR_DECLAR >= 20               THEN '20+'
            ELSE 'NC'
        END
)

SELECT
    fi.ANNEE,
    fi.MOIS,
    fi.DR_NO,
    fi.SP_NO,
    fi.EMP_REGIME,
    fi.SA_NO,
    fi.EMP_FORME_JURIDIQUE,
    fi.EMP_PERIODICITE,
    fi.EMP_ETAT,
    fi.TRANCHE_EFFECTIF,
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
      AND fr.EMP_PERIODICITE     = fi.EMP_PERIODICITE
      AND fr.TRANCHE_EFFECTIF    = fi.TRANCHE_EFFECTIF
LEFT JOIN DTM.DIM_TEMPS                    t
       ON t.ID_TEMPS = TO_NUMBER(TO_CHAR(
              TRUNC(ADD_MONTHS(
                  TO_DATE(fi.ANNEE || '0101', 'YYYYMMDD'),
                  fi.MOIS - 1
              ), 'MM'),
          'YYYYMMDD'))
