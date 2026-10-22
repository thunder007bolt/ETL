-- =============================================================================
-- LOT 12 — Nouveau test des requêtes DTM GRH, avec le cliché écrit en clair
-- Rédigé le 22/10/2026 — À EXÉCUTER sur la base du DWH (schémas DWH et DTM).
-- Exécution : en mode script (F5) ; aucune saisie, sauf pour Q84. Les requêtes
-- n'écrivent rien dans le DTM et ne renvoient que des comptages.
-- Pourquoi ce lot : au lot 11, le DWH contient bien le cliché 102026, avec
-- 3 180 agents dont 879 actifs (Q75 à Q77). L'effectif et le ratio n'auraient
-- donc pas dû sortir vides au lot 10. Hypothèse : SQL Developer ne transmet la
-- valeur saisie qu'à la première occurrence de :1, alors que ces requêtes
-- l'utilisent plusieurs fois ; les autres occurrences valent NULL et aucun
-- agent n'est retenu. Le pipeline Python n'est pas concerné : il a toujours
-- chargé des données avec plusieurs :1. Ici, le cliché est écrit en clair
-- ('102026') ; Q84 vérifie l'hypothèse.
-- Généré depuis domaines/grh/dtm/sql le 22/10/2026 ; exécuté, il n'est plus
-- régénéré.
-- Exécuté le 22/10/2026 : effectif, ratio et formation conformes aux attendus ;
-- Q84 écarte l'hypothèse sur SQL Developer : les résultats vides du lot 10
-- venaient de la valeur saisie, pas des requêtes.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q80 — Effectif par exercice depuis 2019 (grh_effectif.sql)
-- Attendu : 2025 autour de 880 agents (879 actifs à la date de la copie,
--            plus les sortis datés de début 2026, moins les entrés de 2026) ;
--            aucune ligne 2026 ; peu d'agents inactifs.
-- Résultat (22/10/2026) : exercice · agents · actifs · inactifs
--            2019 · 846 · 842 · 4 ; 2020 · 789 · 785 · 4 ; 2021 · 752 · 748 · 4 ;
--            2022 · 721 · 717 · 4 ; 2023 · 690 · 686 · 4 ; 2024 · 753 · 749 · 4 ;
--            2025 · 885 · 882 · 3. Aucune ligne 2026.
-- Constat  : conforme : 885 agents au 31/12/2025, dans la fourchette attendue.
--            La baisse de 2019 à 2023 est en partie artificielle : un agent
--            parti sans date de sortie n'est compté dans aucun exercice (817
--            agents dans ce cas).
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS,
       SUM(NB_AGENTS) AS nb_agents,
       SUM(NB_AGENTS_ACTIFS) AS nb_actifs,
       SUM(NB_AGENTS_INACTIFS) AS nb_inactifs
FROM (
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
        WHERE s.CLICHE        = '102026'
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
                   AND dp.CLICHE  = '102026'
            WHERE d.CLICHE       = '102026'
              AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
              AND d.DATE_VIGUEUR IS NOT NULL
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
            ON  p.CLICHE     = '102026'
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
                AND s.CLICHE     = '102026'
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
        '102026'                                                    AS CLICHE
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
) q
WHERE ID_TEMPS >= 20190101
GROUP BY ID_TEMPS
ORDER BY ID_TEMPS;


-- -----------------------------------------------------------------------------
-- Q81 — Effectif au 31/12/2025 par tranche d'âge, puis d'ancienneté
-- Attendu : « 55 ans et plus » autour de 83 ; « moins de 1 an »
--            d'ancienneté autour de 153.
-- Résultat (22/10/2026) : âge : <25 23 · 25-29 59 · 30-34 119 · 35-39 156 ·
--            40-44 138 · 45-49 151 · 50-54 151 · 55+ 88 (total 885).
--            Ancienneté : <1 154 · 1-4 104 · 5-9 88 · 10-14 255 · 15-19 80 ·
--            20-24 138 · 25-29 34 · 30+ 32 (total 885).
-- Constat  : conforme : 88 agents de 55 ans et plus (environ 83 attendus) et
--            154 de moins d'un an (153 attendus). Les deux répartitions
--            retrouvent les 885 agents : aucune tranche n'en perd.
-- -----------------------------------------------------------------------------
SELECT TRANCHE_AGE_CODE, SUM(NB_AGENTS) AS nb_agents
FROM (
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
        WHERE s.CLICHE        = '102026'
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
                   AND dp.CLICHE  = '102026'
            WHERE d.CLICHE       = '102026'
              AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
              AND d.DATE_VIGUEUR IS NOT NULL
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
            ON  p.CLICHE     = '102026'
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
                AND s.CLICHE     = '102026'
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
        '102026'                                                    AS CLICHE
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
) q
WHERE ID_TEMPS = 20251231
GROUP BY TRANCHE_AGE_CODE
ORDER BY TRANCHE_AGE_CODE;

SELECT TRANCHE_ANC_CODE, SUM(NB_AGENTS) AS nb_agents
FROM (
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
        WHERE s.CLICHE        = '102026'
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
                   AND dp.CLICHE  = '102026'
            WHERE d.CLICHE       = '102026'
              AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
              AND d.DATE_VIGUEUR IS NOT NULL
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
            ON  p.CLICHE     = '102026'
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
                AND s.CLICHE     = '102026'
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
        '102026'                                                    AS CLICHE
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
) q
WHERE ID_TEMPS = 20251231
GROUP BY TRANCHE_ANC_CODE
ORDER BY TRANCHE_ANC_CODE;


-- -----------------------------------------------------------------------------
-- Q82 — Ratio d'encadrement depuis 2019 (grh_ratio.sql)
-- Attendu : 2025 autour de 870 actifs et 366 encadrants, soit environ 42 % ;
--            NB_AGENTS_TOTAL égal à nb_actifs de Q80 (les 3 180 agents ont
--            une date de naissance, lot 11, Q76) ; aucune ligne 2026.
-- Résultat (22/10/2026) : exercice · actifs · encadrants · non-encadrants ·
--            inconnus · taux
--            2019 · 842 · 321 · 520 · 1 · 38,1 % ; 2020 · 785 · 309 · 475 · 1 · 39,4 % ;
--            2021 · 748 · 295 · 452 · 1 · 39,4 % ; 2022 · 717 · 285 · 431 · 1 · 39,8 % ;
--            2023 · 686 · 269 · 416 · 1 · 39,2 % ; 2024 · 749 · 323 · 425 · 1 · 43,1 % ;
--            2025 · 882 · 368 · 513 · 1 · 41,7 %.
-- Constat  : conforme : chaque année, le total égale les actifs de Q80 ; 2025
--            donne 368 encadrants et 41,7 % (environ 366 et 42 % attendus).
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS, NB_AGENTS_TOTAL, NB_ENCADRANTS, NB_NON_ENCADRANTS,
       NB_QUAL_INCONNU, TAUX_ENCADREMENT
FROM (
    -- DTM_GRH_RATIO — Ratios d'encadrement par exercice (effectif actif au 31/12)
    -- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
    --
    -- Règles :
    --   - exercices clos depuis 1960, arrêtés au 31/12 ;
    --   - effectif : agents présents et actifs, selon les règles de présence et
    --     d'activité de DTM_GRH_EFFECTIF ; la date de naissance n'est pas exigée ;
    --   - encadrants : qualifications HC, 04, 05, 06 ; non-encadrants : 01, 02, 03 ;
    --   - colonnes financières vides en attendant une source comptable.
    WITH
    -- ── Exercices clos ─────────────────────────────────────────────────────────
    ANNEES AS (
        SELECT DISTINCT
            EXTRACT(YEAR FROM DATE_DEBUT)                                    AS ANNEE,
            EXTRACT(YEAR FROM DATE_DEBUT) * 10000 + 1231                    AS ID_TEMPS,
            TO_DATE(EXTRACT(YEAR FROM DATE_DEBUT) || '-12-31', 'YYYY-MM-DD') AS DT_REF
        FROM DWH.FAIT_GRH_SITUATION
        WHERE CLICHE     = '102026'
          AND DATE_DEBUT IS NOT NULL
          AND EXTRACT(YEAR FROM DATE_DEBUT) BETWEEN 1960 AND EXTRACT(YEAR FROM SYSDATE) - 1
    ),
    -- ── Première sortie datée de chaque agent ─────────────────────────────────
    SORTIES AS (
        SELECT PERS_ID, MIN(DT_SORTIE) AS DT_SORTIE
        FROM (
            SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
                   d.DATE_VIGUEUR             AS DT_SORTIE
            FROM DWH.FAIT_GRH_DECISION d
            LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
                   ON  dp.DECI_NO = d.DECI_NO
                   AND dp.CLICHE  = '102026'
            WHERE d.CLICHE       = '102026'
              AND d.CDEC_CODE   IN ('02','34','35','36','52','54')
              AND d.DATE_VIGUEUR IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM DWH.FAIT_GRH_DECISION a
                    WHERE a.CLICHE    = '102026'
                      AND a.CDEC_CODE = '40'
                      AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO)
                  )
            UNION ALL
            SELECT p.PERS_ID, p.DATE_DECES
            FROM DWH.FAIT_GRH_PERSONNE p
            WHERE p.CLICHE      = '102026'
              AND p.DATE_DECES IS NOT NULL
        )
        WHERE PERS_ID IS NOT NULL
        GROUP BY PERS_ID
    ),
    -- ── Agents présents au 31/12 ──────────────────────────────────────────────
    PRESENTS AS (
        SELECT a.ID_TEMPS, a.DT_REF, p.PERS_ID
        FROM ANNEES a
        INNER JOIN DWH.FAIT_GRH_PERSONNE p
            ON  p.CLICHE     = '102026'
            AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= a.DT_REF
        LEFT JOIN SORTIES so
            ON  so.PERS_ID   = p.PERS_ID
        WHERE p.ETAT_EMPLOYE = 'ACTIF'
           OR so.DT_SORTIE   > a.DT_REF
    ),
    -- ── Dernière situation commencée avant le 31/12 ───────────────────────────
    DERNIERE_SITU AS (
        SELECT ID_TEMPS, PERS_ID, QUAL_CODE, STATUT_EMPLOYE
        FROM (
            SELECT
                pr.ID_TEMPS,
                s.PERS_ID,
                s.QUAL_CODE,
                s.STATUT_EMPLOYE,
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
    -- ── Effectif actif au 31/12 par exercice ──────────────────────────────────
    EFFECTIF AS (
        SELECT
            pr.ID_TEMPS,
            COUNT(DISTINCT pr.PERS_ID)                            AS NB_TOTAL,
            -- Encadrants : Cadres Sup (HC/05/06) + Cadres Moyens (04)
            COUNT(DISTINCT
                CASE WHEN ds.QUAL_CODE IN ('HC','05','06','04')
                     THEN pr.PERS_ID END)                         AS NB_ENC,
            -- Non encadrants : Agents d'exécution (01/02) + Agents de Maîtrise (03)
            COUNT(DISTINCT
                CASE WHEN ds.QUAL_CODE IN ('01','02','03')
                     THEN pr.PERS_ID END)                         AS NB_NENC,
            -- Inconnus : QUAL_CODE NULL ou hors HC/01-06
            COUNT(DISTINCT
                CASE WHEN ds.QUAL_CODE NOT IN ('HC','05','06','04','01','02','03')
                       OR ds.QUAL_CODE IS NULL
                     THEN pr.PERS_ID END)                         AS NB_INC
        FROM PRESENTS pr
        LEFT JOIN DERNIERE_SITU ds
            ON  ds.ID_TEMPS = pr.ID_TEMPS
            AND ds.PERS_ID  = pr.PERS_ID
        -- Actifs : hors détachement, mise à disposition, disponibilité, suspension,
        -- service militaire
        WHERE NVL(ds.STATUT_EMPLOYE, 'A') NOT IN ('B','D','E','F','H')
        GROUP BY pr.ID_TEMPS
    )

    SELECT
        e.ID_TEMPS,
        e.NB_TOTAL                                      AS NB_AGENTS_TOTAL,
        e.NB_ENC                                        AS NB_ENCADRANTS,
        e.NB_NENC                                       AS NB_NON_ENCADRANTS,
        e.NB_INC                                        AS NB_QUAL_INCONNU,
        ROUND(e.NB_ENC / NULLIF(e.NB_TOTAL, 0), 4)     AS TAUX_ENCADREMENT,
        NULL AS MASSE_SALARIALE,
        NULL AS FRAIS_PERSONNEL,
        NULL AS CHARGES_FONCTIONNEMENT,
        NULL AS RATIO_FP_CHARGES,
        NULL AS COUT_FORMATION,
        NULL AS BUDGET_FORMATION,
        NULL AS RATIO_FORM_MSA,
        NULL AS RATIO_FORM_BUDGET,
        SYSDATE AS DATE_CHARGEMENT,
        '102026'      AS CLICHE
    FROM EFFECTIF e
) q
WHERE ID_TEMPS >= 20190101
ORDER BY ID_TEMPS;


-- -----------------------------------------------------------------------------
-- Q83 — Formation 2025 par sexe (grh_formation.sql)
-- Attendu : 331 hommes et 277 femmes, 691 inscriptions au total ; pas de
--            sexe vide si la jointure aux agents du cliché fonctionne.
-- Résultat (22/10/2026) : sexe · agents · inscriptions · coût
--            F · 277 · 313 · 98 906 878 ; M · 331 · 378 · 116 500 992.
-- Constat  : conforme : 608 agents (331 hommes, 277 femmes), 691 inscriptions
--            et un coût de 215 407 870, comme en Q73 ; aucun sexe vide.
-- -----------------------------------------------------------------------------
SELECT SEXE,
       SUM(NB_STAGIAIRES) AS somme_stagiaires,
       SUM(NB_INSCRIPTIONS) AS inscriptions,
       SUM(COUT_TOTAL) AS cout_total
FROM (
    -- DTM_GRH_FORMATION — Indicateurs de formation par exercice, catégorie, qualification et sexe
    -- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
    --
    -- Règles :
    --   - formations des exercices clos, quel que soit leur statut, rattachées à l'année
    --     de leur fin (à défaut, de leur début) ;
    --   - qualification : celle saisie sur l'inscription ; sexe : celui de la fiche agent ;
    --   - coût d'une inscription : somme des 7 frais, un frais vide comptant pour zéro ;
    --   - budget vide : le champ BUDGET d'une formation contient une année budgétaire,
    --     pas un montant ;
    --   - durée : somme des durées saisies sur les inscriptions, sans conversion d'unité.
    WITH
    -- ── Formations des exercices clos ─────────────────────────────────────────
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
    -- ── Inscriptions avec détail des frais ────────────────────────────────────
    INS_BASE AS (
        SELECT
            i.FORM_NO,
            i.PERS_ID,
            i.INS_ID,
            i.QUAL_CODE_INS                                    AS QUAL_CODE,
            i.DUREE                                           AS DUREE,
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
    -- ── Jointure avec FAIT_GRH_PERSONNE pour le sexe ─────────────────────────
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
            ib.COUT_REEL
        FROM FORM_BASE fb
        INNER JOIN INS_BASE ib     ON ib.FORM_NO  = fb.FORM_NO
        LEFT  JOIN DWH.FAIT_GRH_PERSONNE p
                                   ON p.PERS_ID   = ib.PERS_ID
                                  AND p.CLICHE    = '102026'
    )

    SELECT
        ic.ID_TEMPS,
        ic.CAT_FORM_CODE,
        ic.QUAL_CODE,
        ic.SEXE,
        CASE ic.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        COUNT(DISTINCT ic.PERS_ID)                            AS NB_STAGIAIRES,
        COUNT(ic.INS_ID)                                      AS NB_INSCRIPTIONS,
        SUM(ic.DUREE)                                         AS NB_JOURS_FORMATION,
        SUM(ic.COUT_REEL)                                     AS COUT_TOTAL,
        NULL                                                  AS BUDGET_FORMATION,
        SYSDATE                                               AS DATE_CHARGEMENT,
        '102026'                                                    AS CLICHE
    FROM INS_COMPLET ic
    GROUP BY
        ic.ID_TEMPS,
        ic.CAT_FORM_CODE,
        ic.QUAL_CODE,
        ic.SEXE
) q
WHERE ID_TEMPS = 20251231
GROUP BY SEXE
ORDER BY SEXE;


-- -----------------------------------------------------------------------------
-- Q84 — SQL Developer et les variables répétées
-- Objectif : vérifier que SQL Developer transmet la valeur saisie à chaque
--            occurrence d'une même variable. Saisir 102026, sans guillemets,
--            à chaque demande, et noter combien de valeurs il demande.
-- Attendu : 3 180 pour les deux requêtes si la valeur est reprise partout ;
--            0 pour la première si seule la première occurrence de :1 la
--            reçoit, ce qui expliquerait le lot 10.
-- Résultat (22/10/2026) : 3 180 pour les deux requêtes.
-- Constat  : SQL Developer transmet bien la valeur à chaque occurrence de :1,
--            l'hypothèse est écartée. Les résultats vides du lot 10 venaient
--            donc de la valeur saisie pour Q70 à Q72 (par exemple avec les
--            guillemets de l'exemple de l'en-tête), et non des requêtes.
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS agents
FROM DWH.FAIT_GRH_PERSONNE
WHERE CLICHE = :1
  AND CLICHE = :1;

SELECT COUNT(*) AS agents
FROM DWH.FAIT_GRH_PERSONNE
WHERE CLICHE = :cliche
  AND CLICHE = :cliche;
