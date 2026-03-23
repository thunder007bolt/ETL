-- DTM_GRH_EFFECTIF — Effectif au 31/12 par exercice, qualification, tranche d'âge/ancienneté, sexe, domaine, UA, lieu, fonction
-- :1 = CLICHE (MMYYYY) — filtre les snapshots DWH source et étiquette les lignes insérées
WITH
ANNEES AS (
    SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS ANNEE
    FROM DWH.FAIT_GRH_SITUATION s
    WHERE s.CLICHE        = :1
      AND s.DATE_DEBUT   IS NOT NULL
),
REF AS (
    SELECT
        ANNEE,
        TO_NUMBER(TO_CHAR(ANNEE) || '1231')            AS ID_TEMPS,
        TO_DATE(TO_CHAR(ANNEE)   || '1231','YYYYMMDD') AS DT_REF
    FROM ANNEES
),
BASE AS (
    SELECT
        r.ID_TEMPS,
        s.PERS_ID,
        s.QUAL_CODE,
        s.CODE_DOMAINE_ACT                                        AS CODE_DOMAINE,
        u.UA_NATURE,
        s.LIEU_ID,
        s.FNCT_CODE,
        p.SEXE,
        FLOOR((r.DT_REF - p.DATE_NAISS) / 365.25)                AS AGE_REF,
        GREATEST(
            FLOOR((r.DT_REF
                - COALESCE(p.DATE_EMBAUCHE,
                           p.DATE_PRISE_SERVICE)) / 365.25), 0)  AS ANC_REF,
        s.STATUT_EMPLOYE
    FROM REF r
    INNER JOIN DWH.FAIT_GRH_SITUATION s
        ON  s.DATE_DEBUT <= r.DT_REF
        AND (s.DATE_FIN IS NULL OR s.DATE_FIN > r.DT_REF)
        AND s.CLICHE     = :1
    INNER JOIN DWH.FAIT_GRH_PERSONNE p
        ON  p.PERS_ID    = s.PERS_ID
        AND p.CLICHE     = :1
    LEFT JOIN DTM.DIM_GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE    = s.UA_CODE
    WHERE UPPER(TRIM(s.NATURE_SITUATION))
              IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')
      AND s.DATE_DEBUT >= DATE '2000-01-01'
)

SELECT
    b.ID_TEMPS,
    b.QUAL_CODE,
    ta.TRANCHE_AGE_CODE,
    tan.TRANCHE_ANC_CODE,
    b.SEXE,
    CASE b.SEXE
        WHEN '1' THEN 'Masculin'
        WHEN '2' THEN 'Féminin'
        ELSE NULL
    END                                                   AS LIBELLE_SEXE,
    b.CODE_DOMAINE,
    b.UA_NATURE,
    b.LIEU_ID,
    b.FNCT_CODE,
    COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
    COUNT(DISTINCT CASE WHEN b.STATUT_EMPLOYE = 'A'
                        THEN b.PERS_ID END)               AS NB_AGENTS_ACTIFS,
    COUNT(DISTINCT CASE WHEN b.STATUT_EMPLOYE != 'A'
                        THEN b.PERS_ID END)               AS NB_AGENTS_INACTIFS,
    ROUND(AVG(b.AGE_REF), 1)                              AS AGE_MOYEN,
    ROUND(AVG(b.ANC_REF), 1)                              AS ANCIENNETE_MOYENNE,
    :1                                                    AS CLICHE
FROM BASE b
INNER JOIN DTM.DIM_GRH_TRANCHE_AGE ta
    ON  b.AGE_REF >= ta.AGE_MIN
    AND (b.AGE_REF < ta.AGE_MAX OR ta.AGE_MAX IS NULL)
INNER JOIN DTM.DIM_GRH_TRANCHE_ANCIENNETE tan
    ON  b.ANC_REF >= tan.ANC_MIN
    AND (b.ANC_REF < tan.ANC_MAX OR tan.ANC_MAX IS NULL)
GROUP BY
    b.ID_TEMPS,
    b.QUAL_CODE,
    ta.TRANCHE_AGE_CODE,
    tan.TRANCHE_ANC_CODE,
    b.SEXE,
    b.CODE_DOMAINE,
    b.UA_NATURE,
    b.LIEU_ID,
    b.FNCT_CODE
