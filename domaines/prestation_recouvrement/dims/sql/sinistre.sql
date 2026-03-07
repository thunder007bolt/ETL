
SELECT
    t.DOS_CODE,

    -- Qualification du sinistre
    t.SEM_CODE,
    t.SEM_LIBELLE,
    t.SQU_CODE,
    t.SQU_LIBELLE,
    t.SSL_CODE,
    t.SSL_LIBELLE,
    t.SLA_CODE,
    t.SLA_LIBELLE,
    t.SNL_CODE,
    t.SNL_LIBELLE,
    t.TAT_CODE,
    t.TAT_DESCRIPTION,

    -- Géographie de l'accident
    t.DOS_VILLE_ACCIDENT,
    t.SA_NO,
    t.SA_DESC,

    -- Dates et contrôle
    t.DOS_DATE_ACCIDENT,
    t.DOS_DATE_NOTIFICATION,
    t.JOUR_SEMAINE,
    t.DOS_VALIDE

FROM TSINISTRE t
