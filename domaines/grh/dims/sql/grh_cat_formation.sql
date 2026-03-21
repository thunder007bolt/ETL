-- DIM_GRH_CAT_FORMATION — Source : GRH_CAT_FORMATION (full reload)
-- CAT_FORM_FINALITE : Adaptation | Perfectionnement | Reconversion | Autre (CIPRES)
SELECT
    CAT_FORM_CODE,
    CAT_FORM_LIBELLE,
    CAT_FORM_FINALITE,
    RANG
FROM GRH_CAT_FORMATION
