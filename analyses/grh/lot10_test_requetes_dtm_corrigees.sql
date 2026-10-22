-- =============================================================================
-- LOT 10 — Test des requêtes DTM corrigées, avant tout chargement
-- Rédigé le 22/10/2026 — À EXÉCUTER sur la base du DWH (schémas DWH et DTM).
-- Chaque requête enveloppe le fichier corrigé de domaines/grh/dtm/sql dans un
-- simple comptage : rien n'est écrit dans le DTM. Généré à partir des fichiers
-- du 22/10/2026 ; exécuté, il n'est plus régénéré : la suite est au lot 12.
-- :1 = cliché à tester, au format MMAAAA, saisi sans guillemets (ex. 102026).
-- Ce doit être le cliché présent dans les faits du DWH (lot 11, Q75) : ces
-- tables ne gardent que leur dernier chargement. SQL Developer ne le demande
-- qu'une fois par requête.
-- Exécution : en mode script (F5). Alias limités à 30 caractères.
-- Les résultats attendus viennent des lots 6 à 9.
-- Exécuté le 22/10/2026 : Q73 conforme ; Q70 à Q72 vides, sans doute à cause du
-- cliché saisi (diagnostic au lot 11) ; Q74 montre encore l'ancien chargement.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q70 — Effectif corrigé par exercice depuis 2019 (grh_effectif.sql)
-- Attendu : 2025 entre 879 et 890 agents (les 879 actifs, plus les agents sortis
--           début 2026 avec une date) ; aucune ligne 2026 ; des chiffres qui
--           montent en 2024 et 2025 avec les recrutements.
--           Avant correction, le DTM publiait 2 118 agents pour 2025.
-- Résultat : aucune ligne (22/10/2026).
-- Constat  : le chargement des faits GRH archive le DWH dans l'ODS puis vide
--            les tables (TRUNCATE) : le DWH ne garde qu'un cliché. Si le cliché
--            saisi n'est pas celui-là, aucun agent n'est trouvé. À vérifier
--            avec le lot 11 (Q75, Q76), puis relancer avec le bon cliché.
--            Suite : le DWH contient bien 102026 (lot 11) et la requête renvoie
--            885 agents avec le cliché écrit en clair (lot 12) : c'est la valeur
--            saisie pour Q70 à Q72 qui était en cause.
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
) q
WHERE ID_TEMPS >= 20190101
GROUP BY ID_TEMPS
ORDER BY ID_TEMPS;


-- -----------------------------------------------------------------------------
-- Q71 — Effectif corrigé au 31/12/2025 par tranche d'âge, puis d'ancienneté
-- Attendu : « 55 ans et plus » autour de 83 (1 263 avant correction) ; la tranche
--           « moins de 1 an » d'ancienneté autour de 153 (0 avant correction).
-- Résultat : aucune ligne pour les deux requêtes (22/10/2026), même cause
--            probable que Q70.
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
) q
WHERE ID_TEMPS = 20251231
GROUP BY TRANCHE_ANC_CODE
ORDER BY TRANCHE_ANC_CODE;


-- -----------------------------------------------------------------------------
-- Q72 — Ratio d'encadrement corrigé depuis 2019 (grh_ratio.sql)
-- Attendu : 2025 autour de 870 agents actifs et 366 encadrants, soit environ
--           42 % (avant correction : 1 636 agents et 31,1 %) ; même nombre
--           d'actifs que NB_AGENTS_ACTIFS de Q70 ; aucune ligne 2026.
-- Résultat : aucune ligne (22/10/2026), même cause probable que Q70.
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS, NB_AGENTS_TOTAL, NB_ENCADRANTS, NB_NON_ENCADRANTS, NB_QUAL_INCONNU, TAUX_ENCADREMENT
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
        WHERE CLICHE     = :1
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
        SELECT a.ID_TEMPS, a.DT_REF, p.PERS_ID
        FROM ANNEES a
        INNER JOIN DWH.FAIT_GRH_PERSONNE p
            ON  p.CLICHE     = :1
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
                AND s.CLICHE     = :1
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
        :1      AS CLICHE
    FROM EFFECTIF e
) q
WHERE ID_TEMPS >= 20190101
ORDER BY ID_TEMPS;


-- -----------------------------------------------------------------------------
-- Q73 — Formation corrigée par exercice depuis 2019 (grh_formation.sql)
-- Attendu : des lignes chaque année (avant correction : aucune) ; inscriptions
--           2025 = 691, 2024 = 525, 2019 = 602 (lot 9, Q64) ; agents formés en
--           2025 autour de 608 (la somme par catégorie peut dépasser un peu) ;
--           budget renseigné à partir de 2020 seulement.
-- Résultat (22/10/2026) : année · somme des agents · inscriptions · jours ·
--            coût · budget
--            2019 · 515 · 602 · 11 159 · 0 · vide
--            2020 · 350 · 435 · 2 629 · 115 404 649 · 56 557
--            2021 · 494 · 556 · 3 735 · 217 096 293 · 74 767
--            2022 · 524 · 886 · 5 030 · 237 581 601 · 78 854
--            2023 · 548 · 735 · 3 695 · 92 857 950 · 72 819,99
--            2024 · 413 · 525 · 6 358 · 164 549 732 · 89 046,01
--            2025 · 608 · 691 · 6 267 · 215 407 870 · 66 821
-- Constat  : - inscriptions conformes au lot 9 (602, 525, 691) ;
--            - agents conformes au profilage en 2020-2023 et en 2025 ; en 2019
--              (+9) et en 2024 (+3), la somme compte deux fois l'agent présent
--              sur deux lignes (règle 4.2 du document d'exploitation) ;
--            - aucun frais saisi en 2019 : coût 0 ;
--            - budget annuel de 57 000 à 89 000, soit 1 000 à 3 000 fois moins
--              que le coût : la plupart des budgets sont sans doute écartés par
--              le contrôle numérique, ou saisis en milliers (lot 11, Q78, Q79) ;
--            - les décimales ,99 et ,01 viennent de l'arrondi de la part de
--              budget de chaque ligne.
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS,
       SUM(NB_STAGIAIRES) AS somme_stagiaires,
       SUM(NB_INSCRIPTIONS) AS inscriptions,
       SUM(NB_JOURS_FORMATION) AS jours,
       SUM(COUT_TOTAL) AS cout_total,
       SUM(BUDGET_FORMATION) AS budget
FROM (
    -- DTM_GRH_FORMATION — Indicateurs de formation par exercice, catégorie, qualification et sexe
    -- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
    --
    -- Règles :
    --   - formations des exercices clos, quel que soit leur statut, rattachées à l'année
    --     de leur fin (à défaut, de leur début) ;
    --   - qualification : celle saisie sur l'inscription ; sexe : celui de la fiche agent ;
    --   - coût d'une inscription : somme des 7 frais, un frais vide comptant pour zéro ;
    --   - budget de la formation réparti à parts égales entre ses inscriptions ;
    --   - durée : somme des durées saisies sur les inscriptions, sans conversion d'unité.
    WITH
    -- ── Formations des exercices clos ─────────────────────────────────────────
    FORM_BASE AS (
        SELECT
            f.FORM_NO,
            TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
                COALESCE(f.DATE_FIN, f.DATE_DEBUT))) || '1231') AS ID_TEMPS,
            f.CATEGORIE                                         AS CAT_FORM_CODE,
            CASE
                WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+(\.[0-9]+)?$')
                THEN TO_NUMBER(f.BUDGET)
                ELSE NULL
            END                                               AS BUDGET
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
          + NVL(i.FRAIS_ACCESSOIRE, 0)                       AS COUT_REEL,
            COUNT(*) OVER (PARTITION BY i.FORM_NO)            AS NB_INS_FORMATION
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
            ib.COUT_REEL,
            fb.BUDGET / ib.NB_INS_FORMATION                       AS BUDGET_PART
        FROM FORM_BASE fb
        INNER JOIN INS_BASE ib     ON ib.FORM_NO  = fb.FORM_NO
        LEFT  JOIN DWH.FAIT_GRH_PERSONNE p
                                   ON p.PERS_ID   = ib.PERS_ID
                                  AND p.CLICHE    = :1
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
        ROUND(SUM(ic.BUDGET_PART), 2)                         AS BUDGET_FORMATION,
        SYSDATE                                               AS DATE_CHARGEMENT,
        :1                                                    AS CLICHE
    FROM INS_COMPLET ic
    GROUP BY
        ic.ID_TEMPS,
        ic.CAT_FORM_CODE,
        ic.QUAL_CODE,
        ic.SEXE
) q
WHERE ID_TEMPS >= 20190101
GROUP BY ID_TEMPS
ORDER BY ID_TEMPS;


-- -----------------------------------------------------------------------------
-- Q74 — Indicateurs (indicateurs_finances_grh.sql) : à contrôler après le premier
-- chargement, car la requête lit la table DTM_GRH_EFFECTIF chargée pour le
-- cliché. Attendu : une seule ligne par année, sans 2026, et un taux calculé sur
-- l'effectif corrigé.
-- Résultat (22/10/2026) : année · taux · cadres · effectif
--            2019 13,43 % · 241 · 1 794 ; 2020 13,74 % · 262 · 1 907 ;
--            2021 13,07 % · 236 · 1 806 ; 2022 12,95 % · 265 · 2 047 ;
--            2023 13,86 % · 263 · 1 898 ; 2024 13,22 % · 261 · 1 975 ;
--            2025 13,55 % · 287 · 2 118 ; 2026 13,36 % · 287 · 2 149.
-- Constat  : ce sont les chiffres de l'ancien chargement (2 118 agents en 2025,
--            exercice 2026 publié) : la table n'a pas encore été rechargée avec
--            les nouvelles requêtes. À refaire après le chargement.
-- -----------------------------------------------------------------------------
SELECT AN_ID, TX_ENCADREMENT, NB_CADRES, NB_EFFECTIF_TOTAL
FROM DTM.DTM_INDICATEURS_FINANCES_GRH
WHERE CLICHE = :1 AND AN_ID >= 2019
ORDER BY AN_ID;
