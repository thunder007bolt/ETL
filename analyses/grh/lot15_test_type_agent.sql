-- =============================================================================
-- LOT 15 — Test de l'axe « type d'agent » des requêtes DTM GRH
-- Rédigé le 25/10/2026 — À EXÉCUTER sur la base du DWH (schémas DWH et DTM).
-- Exécution : en mode script (F5) ; aucune saisie, le cliché est écrit en clair
-- ('102026'). Q91 à Q93 n'écrivent rien dans le DTM ; Q94 se lance après le
-- chargement (modifications 1 à 4 de domaines/grh/dtm/SUIVI_MODIFICATIONS_DWH.md).
-- Pourquoi ce lot : grh_effectif.sql et grh_formation.sql ont deux axes de plus,
-- QUALITE_EMPLOYE et NATURE_EMPLOI. Les totaux doivent rester ceux du lot 12, et
-- le détail doit retrouver les types d'agents du lot 14.
-- Généré depuis domaines/grh/dtm/sql le 25/10/2026.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q91 — Effectif par exercice : totaux inchangés (grh_effectif.sql)
-- Attendu : comme au lot 12 (Q80) : 2019 846 … 2024 753, 2025 885 agents.
-- Résultat : (à compléter)
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS,
       SUM(NB_AGENTS) AS nb_agents,
       SUM(NB_AGENTS_ACTIFS) AS nb_actifs
FROM (
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
        WHERE s.CLICHE        = '102026'
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
                   AND dp.CLICHE  = '102026'
            WHERE d.CLICHE       = '102026'
              AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
              AND d.DATE_VIGUEUR IS NOT NULL
              -- Écarte les décisions annulées par une décision 40 qui les cite
              AND NOT EXISTS (
                    SELECT 1
                    FROM DWH.FAIT_GRH_DECISION a
                    WHERE a.CLICHE    = '102026'
                      AND a.CDEC_CODE = '40'
                      AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO)
                  )
            UNION ALL
            -- Décès
            SELECT p.PERS_ID, p.DATE_DECES
            FROM DWH.FAIT_GRH_PERSONNE p
            WHERE p.CLICHE      = '102026'
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
            ON  p.CLICHE     = '102026'
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
                AND s.CLICHE     = '102026'
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
        '102026'                                                    AS CLICHE
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
) q
WHERE ID_TEMPS >= 20190101
GROUP BY ID_TEMPS
ORDER BY ID_TEMPS;


-- -----------------------------------------------------------------------------
-- Q92 — Effectif au 31/12/2025 par type d'agent
-- Attendu : 885 au total ; presque tous A et PERMANENT ; environ 6 A et
--            TEMPORAIRE et 3 B (lot 14, Q90, sur les agents en activité).
-- Résultat : (à compléter)
-- -----------------------------------------------------------------------------
SELECT QUALITE_EMPLOYE, NATURE_EMPLOI,
       SUM(NB_AGENTS) AS nb_agents,
       SUM(NB_AGENTS_ACTIFS) AS nb_actifs
FROM (
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
        WHERE s.CLICHE        = '102026'
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
                   AND dp.CLICHE  = '102026'
            WHERE d.CLICHE       = '102026'
              AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
              AND d.DATE_VIGUEUR IS NOT NULL
              -- Écarte les décisions annulées par une décision 40 qui les cite
              AND NOT EXISTS (
                    SELECT 1
                    FROM DWH.FAIT_GRH_DECISION a
                    WHERE a.CLICHE    = '102026'
                      AND a.CDEC_CODE = '40'
                      AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO)
                  )
            UNION ALL
            -- Décès
            SELECT p.PERS_ID, p.DATE_DECES
            FROM DWH.FAIT_GRH_PERSONNE p
            WHERE p.CLICHE      = '102026'
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
            ON  p.CLICHE     = '102026'
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
                AND s.CLICHE     = '102026'
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
        '102026'                                                    AS CLICHE
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
) q
WHERE ID_TEMPS = 20251231
GROUP BY QUALITE_EMPLOYE, NATURE_EMPLOI
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q93 — Formation 2025 par type d'agent (grh_formation.sql)
-- Attendu : 691 inscriptions et 608 agents au total, comme au lot 12 (Q83).
-- Résultat : (à compléter)
-- -----------------------------------------------------------------------------
SELECT QUALITE_EMPLOYE, NATURE_EMPLOI,
       SUM(NB_STAGIAIRES) AS somme_stagiaires,
       SUM(NB_INSCRIPTIONS) AS inscriptions
FROM (
    -- DTM_GRH_FORMATION — Indicateurs de formation par exercice, catégorie, qualification et sexe
    -- Tableaux : CIPRES T62 (agents formés) et partie formation de T64 (dépenses)
    -- Sources  : DTM.DIM_GRH_FORMATION (formations), USER_GRH.GRH_INSCRIPTION (inscriptions,
    --            lues dans la base de l'application), DWH.FAIT_GRH_PERSONNE (sexe)
    -- Grain    : ID_TEMPS × CAT_FORM_CODE × QUAL_CODE × SEXE × QUALITE_EMPLOYE × NATURE_EMPLOI
    -- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
    --
    -- Règles :
    --   - formations des exercices clos, quel que soit leur statut, rattachées à l'année
    --     de leur fin (à défaut, de leur début) ;
    --   - qualification : celle saisie sur l'inscription ; sexe : celui de la fiche agent ;
    --   - coût d'une inscription : somme des 7 frais, un frais vide comptant pour zéro ;
    --   - budget vide : le champ BUDGET d'une formation contient une année budgétaire,
    --     pas un montant ;
    --   - durée : somme des durées saisies sur les inscriptions, sans conversion d'unité ;
    --   - type d'agent : qualité de la fiche agent (A agent CNSS, B fonction publique,
    --     C affecté) et nature d'emploi de sa situation au 31/12 de l'exercice de la
    --     formation, laissés en axes pour que la restitution filtre selon la DRH.
    --
    -- Étapes :
    --   1. FORM_BASE      formations des exercices clos, rattachées à leur année
    --   2. INS_BASE       inscriptions à ces formations, avec leur coût
    --   3. SITU_FORM      nature d'emploi de chaque agent formé au 31/12 de l'exercice
    --   4. INS_COMPLET    inscriptions complétées par l'exercice, la catégorie, le sexe
    --                     et le type d'agent
    --   5. SELECT final   comptages et sommes par exercice, catégorie, qualification,
    --                     sexe et type d'agent
    WITH
    -- ── 1. Formations des exercices clos ─────────────────────────────────────────
    -- Une ligne par formation, rattachée à l'année de sa fin (à défaut de son
    -- début), quel que soit son statut. L'année en cours est exclue.
    FORM_BASE AS (
        SELECT
            f.FORM_NO,
            TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                COALESCE(f.DATE_FIN, f.DATE_DEBUT))) || '1231') AS ID_TEMPS,
            f.CATEGORIE                                         AS CAT_FORM_CODE
        FROM DTM.DIM_GRH_FORMATION f
        WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) IS NOT NULL
          AND EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) < EXTRACT(YEAR FROM SYSDATE)
    ),
    -- ── 2. Inscriptions avec leur coût ───────────────────────────────────────────
    -- Une ligne par inscription à une formation retenue à l'étape 1.
    INS_BASE AS (
        SELECT
            i.FORM_NO,
            i.PERS_ID,
            i.INS_ID,
            -- Qualification de l'agent au moment de l'inscription
            i.QUAL_CODE_INS                                    AS QUAL_CODE,
            -- Durée telle que saisie, sans conversion d'unité
            i.DUREE                                           AS DUREE,
            -- Coût : somme des 7 frais, un frais vide comptant pour zéro
            NVL(i.FRAIS_INSCRIPTION, 0)
          + NVL(i.FRAIS_SCOLARITE, 0)
          + NVL(i.FRAIS_TRANSPORT, 0)
          + NVL(i.FRAIS_SEJOUR, 0)
          + NVL(i.FRAIS_DOSSIER, 0)
          + NVL(i.FRAIS_ASSURANCE, 0)
          + NVL(i.FRAIS_ACCESSOIRE, 0)                       AS COUT_REEL
        FROM USER_GRH.GRH_INSCRIPTION i
        WHERE i.FORM_NO IN (SELECT FORM_NO FROM FORM_BASE)
    ),
    -- ── 3. Nature d'emploi de l'agent formé au 31/12 de l'exercice ───────────────
    -- Dernière situation commencée au plus tard le 31/12 de l'exercice de la
    -- formation. Une ligne par agent et par exercice.
    SITU_FORM AS (
        SELECT ID_TEMPS, PERS_ID, NATURE_EMPLOIE
        FROM (
            SELECT
                a.ID_TEMPS,
                s.PERS_ID,
                s.NATURE_EMPLOIE,
                -- RN = 1 : la situation la plus récente ; à date égale, la dernière saisie
                ROW_NUMBER() OVER (
                    PARTITION BY a.ID_TEMPS, s.PERS_ID
                    ORDER     BY s.DATE_DEBUT DESC, s.SITU_ID DESC
                )                                                     AS RN
            FROM (SELECT DISTINCT fb.ID_TEMPS, ib.PERS_ID
                  FROM FORM_BASE fb
                  INNER JOIN INS_BASE ib ON ib.FORM_NO = fb.FORM_NO) a
            INNER JOIN DWH.FAIT_GRH_SITUATION s
                ON  s.PERS_ID    = a.PERS_ID
                AND s.CLICHE     = '102026'
                AND s.DATE_DEBUT <= TO_DATE(TO_CHAR(a.ID_TEMPS), 'YYYYMMDD')
        )
        WHERE RN = 1
    ),
    -- ── 4. Inscriptions avec l'exercice, la catégorie, le sexe et le type d'agent ──
    -- Le sexe vient de la fiche de l'agent dans le cliché ; ses différentes
    -- écritures (M, MASCULIN, H, 1, F, FEMININ, 2...) sont ramenées à M ou F.
    INS_COMPLET AS (
        SELECT
            fb.ID_TEMPS,
            fb.CAT_FORM_CODE,
            ib.QUAL_CODE,
            CASE
                WHEN UPPER(TRIM(TO_CHAR(p.SEXE))) IN ('M', 'MASCULIN', 'H', 'HOMME', '1')
                THEN 'M'
                WHEN UPPER(TRIM(TO_CHAR(p.SEXE))) IN ('F', 'FEMININ', 'FÉMININ', 'FEMME', '2')
                THEN 'F'
                ELSE NULL
            END                                                   AS SEXE,
            ib.PERS_ID,
            ib.INS_ID,
            ib.DUREE,
            ib.COUT_REEL,
            -- Type d'agent : qualité de la fiche, nature d'emploi de la situation
            TO_CHAR(p.QUALITE)                                    AS QUALITE_EMPLOYE,
            sf.NATURE_EMPLOIE                                     AS NATURE_EMPLOI
        FROM FORM_BASE fb
        INNER JOIN INS_BASE ib     ON ib.FORM_NO  = fb.FORM_NO
        -- Jointure externe : une inscription dont l'agent manque au cliché reste
        -- comptée, avec un sexe vide
        LEFT  JOIN DWH.FAIT_GRH_PERSONNE p
                                   ON p.PERS_ID   = ib.PERS_ID
                                  AND p.CLICHE    = '102026'
        LEFT  JOIN SITU_FORM sf
                                   ON sf.ID_TEMPS  = fb.ID_TEMPS
                                  AND sf.PERS_ID   = ib.PERS_ID
    )

    -- ── 5. Comptages et sommes par exercice, catégorie, qualification, sexe et type ──
    -- NB_STAGIAIRES compte chaque agent une fois par ligne : un agent inscrit dans
    -- plusieurs catégories ou qualifications figure sur plusieurs lignes, le total
    -- annuel ne s'obtient donc pas en les additionnant.
    SELECT
        -- ── AXES ─────────────────────────────────────────────────────────
        ic.ID_TEMPS,
        ic.CAT_FORM_CODE,
        ic.QUAL_CODE,
        ic.SEXE,
        CASE ic.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        ic.QUALITE_EMPLOYE,
        ic.NATURE_EMPLOI,
        -- ── MESURES ──────────────────────────────────────────────────────
        COUNT(DISTINCT ic.PERS_ID)                            AS NB_STAGIAIRES,
        COUNT(ic.INS_ID)                                      AS NB_INSCRIPTIONS,
        SUM(ic.DUREE)                                         AS NB_JOURS_FORMATION,
        SUM(ic.COUT_REEL)                                     AS COUT_TOTAL,
        -- Vide : le champ BUDGET de la formation est une année budgétaire, pas un montant
        NULL                                                  AS BUDGET_FORMATION,
        -- ── CHARGEMENT ───────────────────────────────────────────────────
        SYSDATE                                               AS DATE_CHARGEMENT,
        '102026'                                                    AS CLICHE
    FROM INS_COMPLET ic
    GROUP BY
        ic.ID_TEMPS,
        ic.CAT_FORM_CODE,
        ic.QUAL_CODE,
        ic.SEXE,
        ic.QUALITE_EMPLOYE,
        ic.NATURE_EMPLOI
) q
WHERE ID_TEMPS = 20251231
GROUP BY QUALITE_EMPLOYE, NATURE_EMPLOI
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q94 — Après chargement : colonnes remplies dans le DTM
-- Attendu : mêmes chiffres que Q92, lus dans la table chargée.
-- Résultat : (à compléter)
-- -----------------------------------------------------------------------------
SELECT QUALITE_EMPLOYE, NATURE_EMPLOI,
       SUM(NB_AGENTS) AS nb_agents
FROM DTM.DTM_GRH_EFFECTIF
WHERE CLICHE = '102026'
  AND ID_TEMPS = 20251231
GROUP BY QUALITE_EMPLOYE, NATURE_EMPLOI
ORDER BY 1, 2;
