-- DTM_AGE_TRAVAILLEUR — Répartition des travailleurs actifs par âge (snapshot CLICHE)
-- Source : DWH.FAIT_TRAVAILLEUR + dernier emploi actif + DWH.FAIT_EMPLOYEUR
-- Grain  : DR_NO x TR_SEXE x TR_ETAT x EMP_REGIME x FJ_CODE x AGE
-- Age    : différence d'années (année du cliché - année de naissance), sans arrondi mensuel
-- Filtre : travailleurs actifs uniquement (TR_ACTIVE = 'O')
-- Exclus : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
WITH contrat_actif AS (
    SELECT *
    FROM (
        SELECT em.*,
               ROW_NUMBER() OVER (PARTITION BY em.TR_ID ORDER BY em.EM_DATE_DEBUT DESC) rn
        FROM DWH.FAIT_EMPLOI em
        WHERE em.EM_DATE_FIN IS NULL
          AND em.CLICHE = :1
    )
    WHERE rn = 1
),
base AS (
    SELECT
        tr.DR_NO,
        tr.TR_SEXE,
        CASE tr.TR_SEXE
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        NULL
        END                                                                    AS LIBELLE_SEXE,
        tr.TR_ETAT,
        emp.EMP_REGIME,
        emp.EMP_FORME_JURIDIQUE AS FJ_CODE,
        TO_NUMBER(SUBSTR(:1, 3, 4)) - EXTRACT(YEAR FROM tr.TR_DATE_NAISSANCE)   AS AGE,
        tr.TR_ID
    FROM DWH.FAIT_TRAVAILLEUR  tr
    LEFT JOIN contrat_actif     ca  ON ca.TR_ID   = tr.TR_ID
    LEFT JOIN DWH.FAIT_EMPLOYEUR emp ON emp.EMP_ID = ca.EMP_ID AND emp.CLICHE = :1
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
    EMP_REGIME,
    FJ_CODE,
    AGE,
    COUNT(DISTINCT TR_ID)   AS NB_TRAVAILLEURS,
    :1                      AS CLICHE
FROM base
GROUP BY
    DR_NO,
    TR_SEXE,
    LIBELLE_SEXE,
    TR_ETAT,
    EMP_REGIME,
    FJ_CODE,
    AGE
