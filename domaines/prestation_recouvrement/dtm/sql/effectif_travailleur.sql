-- DTM_EFFECTIF_TRAVAILLEUR — Effectif des travailleurs actifs (snapshot CLICHE)
-- Source : DWH.FAIT_TRAVAILLEUR
-- Grain  : DR_NO x TR_SEXE x TR_ETAT x TR_ASSURE_VOL x AGE
-- Age    : différence d'années (année du cliché - année de naissance), sans arrondi mensuel
-- Filtre : travailleurs actifs uniquement (TR_ACTIVE = 'O')
-- Exclus : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
WITH base AS (
    SELECT
        tr.DR_NO,
        tr.TR_SEXE,
        CASE tr.TR_SEXE
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        NULL
        END                                                                    AS LIBELLE_SEXE,
        tr.TR_ETAT,
        tr.TR_ASSURE_VOL,
        TO_NUMBER(SUBSTR(:1, 3, 4)) - EXTRACT(YEAR FROM tr.TR_DATE_NAISSANCE)   AS AGE,
        tr.TR_ID
    FROM DWH.FAIT_TRAVAILLEUR  tr
    WHERE tr.CLICHE              = :1
      AND tr.TR_ACTIVE           = 'O'
      AND tr.TR_DATE_NAISSANCE  IS NOT NULL
      AND EXTRACT(YEAR FROM tr.TR_DATE_NAISSANCE)
            BETWEEN 1900 AND TO_NUMBER(SUBSTR(:1, 3, 4))
)
SELECT
    DR_NO,
    TR_SEXE,
    LIBELLE_SEXE,
    TR_ETAT,
    TR_ASSURE_VOL,
    AGE,
    COUNT(DISTINCT TR_ID)   AS NB_TRAVAILLEURS,
    :1                      AS CLICHE
FROM base
GROUP BY
    DR_NO,
    TR_SEXE,
    LIBELLE_SEXE,
    TR_ETAT,
    TR_ASSURE_VOL,
    AGE
