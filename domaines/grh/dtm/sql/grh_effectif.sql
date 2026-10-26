-- DTM_GRH_EFFECTIF — Effectif au 31/12 par exercice, qualification, tranche d'âge/ancienneté, sexe, domaine, UA, lieu, fonction
-- Tableaux : CIPRES T58 (catégorie, âge, sexe), T59 (domaine, sexe), T61 (ancienneté, sexe)
-- Sources  : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION, FAIT_GRH_DECISION_PERSONNE
--            DTM.DIM_GRH_UNITE_ADMINISTRATIVE, DIM_GRH_TRANCHE_AGE, DIM_GRH_TRANCHE_ANCIENNETE
-- Grain    : ID_TEMPS × QUAL_CODE × TRANCHE_AGE_CODE × TRANCHE_ANC_CODE × SEXE × CODE_DOMAINE
--            × UA_NATURE × LIEU_ID × FNCT_CODE × QUALITE_EMPLOYE × NATURE_EMPLOI
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
--   - âge et ancienneté en années pleines au 31/12 ; tranches à bornes incluses ;
--   - type d'agent : qualité de la fiche agent (A agent CNSS, B fonction publique,
--     C affecté) et nature d'emploi de la situation au 31/12 (permanent,
--     temporaire), laissés en axes pour que la restitution filtre selon la DRH.
--
-- Étapes :
--   1. ANNEES         exercices clos à publier
--   2. REF            identifiant AAAA1231 et date du 31/12 de chaque exercice
--   3. SORTIES        première date de sortie de chaque agent
--   4. PRESENTS       agents présents au 31/12 (une ligne par agent et par exercice)
--   5. DERNIERE_SITU  situation de chaque agent présent au 31/12
--   6. BASE           agent décrit au 31/12, avec son âge et son ancienneté
--   7. SELECT final   tranches d'âge et d'ancienneté, puis comptages par axe
WITH
-- ── 1. Exercices clos à publier ──────────────────────────────────────────────
-- Une ligne par année où commence au moins une situation du cliché, jusqu'à
-- l'année dernière : l'exercice en cours n'est publié qu'une fois clos.
ANNEES AS (
    SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS ANNEE
    FROM DWH.FAIT_GRH_SITUATION s
    WHERE s.CLICHE        = :1
      AND s.DATE_DEBUT   IS NOT NULL
      AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) - 1
),
-- ── 2. Identifiant et 31/12 de chaque exercice ───────────────────────────────
-- ID_TEMPS au format AAAA1231 ; DT_REF, le 31/12, sert de date d'arrêté.
REF AS (
    SELECT
        ANNEE,
        TO_NUMBER(TO_CHAR(ANNEE) || '1231')            AS ID_TEMPS,
        TO_DATE(TO_CHAR(ANNEE)   || '1231','YYYYMMDD') AS DT_REF
    FROM ANNEES
),
-- ── 3. Première sortie datée de chaque agent ─────────────────────────────────
-- Une ligne par agent qui a une date de sortie : la plus ancienne entre ses
-- décisions de départ et sa date de décès.
SORTIES AS (
    SELECT PERS_ID, MIN(DT_SORTIE) AS DT_SORTIE
    FROM (
        -- Décisions de départ, de fin de contrat ou de liquidation, hors annulées :
        -- 02 retraite, 34 liquidation des droits, 35 indemnité de fin de contrat,
        -- 36 démission, 52 rupture de contrat, 54 licenciement. L'agent est porté
        -- par la décision, ou par sa liste d'agents quand elle est collective.
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               d.DATE_VIGUEUR             AS DT_SORTIE
        FROM DWH.FAIT_GRH_DECISION d
        LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
               ON  dp.DECI_NO = d.DECI_NO
               AND dp.CLICHE  = :1
        WHERE d.CLICHE       = :1
          AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
          AND d.DATE_VIGUEUR IS NOT NULL
          -- Écarte les décisions annulées par une décision 40 qui les cite
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
-- ── 4. Agents présents au 31/12 de chaque exercice ───────────────────────────
-- Une ligne par agent et par exercice : entré au plus tard le 31/12, et encore
-- ACTIF ou sorti après le 31/12.
PRESENTS AS (
    SELECT
        r.ID_TEMPS,
        r.DT_REF,
        p.PERS_ID,
        p.SEXE,
        p.DATE_NAISS,
        COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)          AS DATE_ENTREE,
        -- Qualité : A agent CNSS, B agent de la fonction publique, C agent affecté
        TO_CHAR(p.QUALITE)                                       AS QUALITE_EMPLOYE
    FROM REF r
    INNER JOIN DWH.FAIT_GRH_PERSONNE p
        ON  p.CLICHE     = :1
        -- Entré au plus tard le 31/12 : embauche, à défaut prise de service
        AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= r.DT_REF
        -- Date de naissance exploitable, sans quoi l'âge ne se calcule pas
        AND p.DATE_NAISS IS NOT NULL
        AND EXTRACT(YEAR FROM p.DATE_NAISS) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE)
    LEFT JOIN SORTIES so
        ON  so.PERS_ID   = p.PERS_ID
    -- Encore en fonction à la date du cliché, ou sorti après le 31/12
    WHERE p.ETAT_EMPLOYE = 'ACTIF'
       OR so.DT_SORTIE   > r.DT_REF
),
-- ── 5. Situation de chaque agent présent au 31/12 ────────────────────────────
-- Dernière situation commencée au plus tard le 31/12, quelle qu'en soit la
-- nature : elle donne la qualification, le domaine, l'unité, le lieu, la
-- fonction, le statut et la nature d'emploi. Une ligne par agent et par exercice.
DERNIERE_SITU AS (
    SELECT ID_TEMPS, PERS_ID, QUAL_CODE, CODE_DOMAINE_ACT, UA_CODE, LIEU_ID, FNCT_CODE, STATUT_EMPLOYE,
           NATURE_EMPLOIE
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
            s.NATURE_EMPLOIE,
            -- RN = 1 : la situation la plus récente ; à date égale, la dernière saisie
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
-- ── 6. Agent décrit au 31/12, avec son âge et son ancienneté ─────────────────
-- Une ligne par agent et par exercice. Un agent sans situation reste compté,
-- avec des axes vides (jointure externe).
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
        -- Âge et ancienneté en années pleines au 31/12
        FLOOR((pr.DT_REF - pr.DATE_NAISS) / 365.25)               AS AGE_REF,
        GREATEST(FLOOR((pr.DT_REF - pr.DATE_ENTREE) / 365.25), 0) AS ANC_REF,
        ds.STATUT_EMPLOYE,
        -- Type d'agent : qualité de la fiche, nature d'emploi de la situation
        pr.QUALITE_EMPLOYE,
        ds.NATURE_EMPLOIE                                         AS NATURE_EMPLOI
    FROM PRESENTS pr
    LEFT JOIN DERNIERE_SITU ds
        ON  ds.ID_TEMPS  = pr.ID_TEMPS
        AND ds.PERS_ID   = pr.PERS_ID
    LEFT JOIN DTM.DIM_GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE    = ds.UA_CODE
)

-- ── 7. Tranches d'âge et d'ancienneté, puis comptages par axe ────────────────
-- Chaque agent tombe dans une seule tranche d'âge et une seule tranche
-- d'ancienneté (bornes incluses) ; il est compté une fois par exercice, sur la
-- ligne de sa combinaison d'axes.
SELECT
    -- ── AXES ─────────────────────────────────────────────────────────
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
    b.QUALITE_EMPLOYE,
    b.NATURE_EMPLOI,
    -- ── MESURES ──────────────────────────────────────────────────────
    COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
    -- Actifs : tous les statuts sauf B, D, E, F, H ; un agent sans situation est actif
    COUNT(DISTINCT CASE WHEN NVL(b.STATUT_EMPLOYE, 'A') NOT IN ('B','D','E','F','H')
                        THEN b.PERS_ID END)               AS NB_AGENTS_ACTIFS,
    -- Inactifs : détachement, mise à disposition, disponibilité, suspension, drapeaux
    COUNT(DISTINCT CASE WHEN b.STATUT_EMPLOYE IN ('B','D','E','F','H')
                        THEN b.PERS_ID END)               AS NB_AGENTS_INACTIFS,
    ROUND(AVG(b.AGE_REF), 1)                              AS AGE_MOYEN,
    ROUND(AVG(b.ANC_REF), 1)                              AS ANCIENNETE_MOYENNE,
    -- ── CHARGEMENT ───────────────────────────────────────────────────
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE
FROM BASE b
-- Tranche d'âge : AGE_MIN <= âge <= AGE_MAX, la dernière tranche n'a pas de borne haute
INNER JOIN DTM.DIM_GRH_TRANCHE_AGE ta
    ON  b.AGE_REF >= ta.AGE_MIN
    AND (b.AGE_REF <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
-- Tranche d'ancienneté : même principe
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
    b.FNCT_CODE,
    b.QUALITE_EMPLOYE,
    b.NATURE_EMPLOI
