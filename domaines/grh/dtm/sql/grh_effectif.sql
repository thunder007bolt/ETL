-- DTM_GRH_EFFECTIF — Effectif au 31/12 par exercice, qualification, tranche d'âge/ancienneté, sexe, domaine, UA, lieu, fonction
-- :1 = CLICHE (MMYYYY) — filtre les snapshots DWH source et étiquette les lignes insérées
--
-- Règles :
--   - exercices clos uniquement, arrêtés au 31/12 ;
--   - agent présent au 31/12 : entré au plus tard à cette date (embauche, à défaut
--     prise de service) et soit encore ACTIF, soit sorti après le 31/12 ;
--   - date de sortie : première décision 02, 34, 35, 36, 52 ou 54 non annulée par
--     une décision 40, à défaut date de décès ; un agent qui n'est pas ACTIF et
--     n'a pas de date de sortie n'est compté dans aucun exercice ;
--   - agent sans date de naissance exploitable non compté (âge non calculable) ;
--   - l'agent est décrit par sa dernière situation commencée au plus tard le 31/12 ;
--   - inactifs : détachement, mise à disposition, disponibilité, suspension, service
--     militaire (statuts B, D, E, F, H) ; actifs : tous les autres ;
--   - âge et ancienneté en années pleines au 31/12 ; tranches à bornes incluses.
WITH
-- ── Exercices clos ─────────────────────────────────────────────────────────
ANNEES AS (
    SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS ANNEE
    FROM DWH.FAIT_GRH_SITUATION s
    WHERE s.CLICHE        = :1
      AND s.DATE_DEBUT   IS NOT NULL
      AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) - 1
),
REF AS (
    SELECT
        ANNEE,
        TO_NUMBER(TO_CHAR(ANNEE) || '1231')            AS ID_TEMPS,
        TO_DATE(TO_CHAR(ANNEE)   || '1231','YYYYMMDD') AS DT_REF
    FROM ANNEES
),
-- ── Première sortie datée de chaque agent ─────────────────────────────────
SORTIES AS (
    SELECT PERS_ID, MIN(DT_SORTIE) AS DT_SORTIE
    FROM (
        -- Décisions de départ, de fin de contrat ou de liquidation, hors annulées
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               d.DATE_VIGUEUR             AS DT_SORTIE
        FROM DWH.FAIT_GRH_DECISION d
        LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
               ON  dp.DECI_NO = d.DECI_NO
               AND dp.CLICHE  = :1
        WHERE d.CLICHE       = :1
          AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
          AND d.DATE_VIGUEUR IS NOT NULL
          AND NOT EXISTS (
                SELECT 1
                FROM DWH.FAIT_GRH_DECISION a
                WHERE a.CLICHE    = :1
                  AND a.CDEC_CODE = '40'
                  AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO)
              )
        UNION ALL
        -- Décès
        SELECT p.PERS_ID, p.DATE_DECES
        FROM DWH.FAIT_GRH_PERSONNE p
        WHERE p.CLICHE      = :1
          AND p.DATE_DECES IS NOT NULL
    )
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
-- ── Agents présents au 31/12 ──────────────────────────────────────────────
PRESENTS AS (
    SELECT
        r.ID_TEMPS,
        r.DT_REF,
        p.PERS_ID,
        p.SEXE,
        p.DATE_NAISS,
        COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)          AS DATE_ENTREE
    FROM REF r
    INNER JOIN DWH.FAIT_GRH_PERSONNE p
        ON  p.CLICHE     = :1
        AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= r.DT_REF
        AND p.DATE_NAISS IS NOT NULL
        AND EXTRACT(YEAR FROM p.DATE_NAISS) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE)
    LEFT JOIN SORTIES so
        ON  so.PERS_ID   = p.PERS_ID
    WHERE p.ETAT_EMPLOYE = 'ACTIF'
       OR so.DT_SORTIE   > r.DT_REF
),
-- ── Dernière situation commencée avant le 31/12 ───────────────────────────
DERNIERE_SITU AS (
    SELECT ID_TEMPS, PERS_ID, QUAL_CODE, CODE_DOMAINE_ACT, UA_CODE, LIEU_ID, FNCT_CODE, STATUT_EMPLOYE
    FROM (
        SELECT
            pr.ID_TEMPS,
            s.PERS_ID,
            s.QUAL_CODE,
            s.CODE_DOMAINE_ACT,
            s.UA_CODE,
            s.LIEU_ID,
            s.FNCT_CODE,
            s.STATUT_EMPLOYE,
            ROW_NUMBER() OVER (
                PARTITION BY pr.ID_TEMPS, s.PERS_ID
                ORDER     BY s.DATE_DEBUT DESC, s.SITU_ID DESC
            )                                                     AS RN
        FROM PRESENTS pr
        INNER JOIN DWH.FAIT_GRH_SITUATION s
            ON  s.PERS_ID    = pr.PERS_ID
            AND s.CLICHE     = :1
            AND s.DATE_DEBUT <= pr.DT_REF
    )
    WHERE RN = 1
),
BASE AS (
    SELECT
        pr.ID_TEMPS,
        pr.PERS_ID,
        ds.QUAL_CODE,
        ds.CODE_DOMAINE_ACT                                       AS CODE_DOMAINE,
        u.UA_NATURE,
        ds.LIEU_ID,
        ds.FNCT_CODE,
        pr.SEXE,
        FLOOR((pr.DT_REF - pr.DATE_NAISS) / 365.25)               AS AGE_REF,
        GREATEST(FLOOR((pr.DT_REF - pr.DATE_ENTREE) / 365.25), 0) AS ANC_REF,
        ds.STATUT_EMPLOYE
    FROM PRESENTS pr
    LEFT JOIN DERNIERE_SITU ds
        ON  ds.ID_TEMPS  = pr.ID_TEMPS
        AND ds.PERS_ID   = pr.PERS_ID
    LEFT JOIN DTM.DIM_GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE    = ds.UA_CODE
)

SELECT
    b.ID_TEMPS,
    b.QUAL_CODE,
    ta.TRANCHE_AGE_CODE,
    tan.TRANCHE_ANC_CODE,
    b.SEXE,
    CASE b.SEXE
        WHEN 'M' THEN 'Masculin'
        WHEN 'F' THEN 'Féminin'
        ELSE 'NON RENSEIGNE'
    END                                                   AS LIBELLE_SEXE,
    b.CODE_DOMAINE,
    b.UA_NATURE,
    b.LIEU_ID,
    b.FNCT_CODE,
    COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
    COUNT(DISTINCT CASE WHEN NVL(b.STATUT_EMPLOYE, 'A') NOT IN ('B','D','E','F','H')
                        THEN b.PERS_ID END)               AS NB_AGENTS_ACTIFS,
    COUNT(DISTINCT CASE WHEN b.STATUT_EMPLOYE IN ('B','D','E','F','H')
                        THEN b.PERS_ID END)               AS NB_AGENTS_INACTIFS,
    ROUND(AVG(b.AGE_REF), 1)                              AS AGE_MOYEN,
    ROUND(AVG(b.ANC_REF), 1)                              AS ANCIENNETE_MOYENNE,
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE
FROM BASE b
INNER JOIN DTM.DIM_GRH_TRANCHE_AGE ta
    ON  b.AGE_REF >= ta.AGE_MIN
    AND (b.AGE_REF <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
INNER JOIN DTM.DIM_GRH_TRANCHE_ANCIENNETE tan
    ON  b.ANC_REF >= tan.ANC_MIN
    AND (b.ANC_REF <= tan.ANC_MAX OR tan.ANC_MAX IS NULL)
GROUP BY
    b.ID_TEMPS,
    b.QUAL_CODE,
    ta.TRANCHE_AGE_CODE,
    tan.TRANCHE_ANC_CODE,
    b.SEXE,
    CASE b.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
    b.CODE_DOMAINE,
    b.UA_NATURE,
    b.LIEU_ID,
    b.FNCT_CODE
