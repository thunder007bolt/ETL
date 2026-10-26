-- =============================================================================
-- LOT 14 — Types d'agents de la CNSS, requête de présentation
-- Rédigé le 25/10/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) ou F9 ; un seul SELECT, sans saisie, qui ne
-- renvoie que des comptages (aucune donnée nominative).
-- Pourquoi ce lot : présenter simplement à la CNSS les types d'agents connus de
-- l'application GRH. Le type croise la qualité de l'agent (qui l'emploie) et
-- la nature d'emploi de sa dernière situation (permanent ou temporaire). Même
-- logique que lot 13, Q86, regroupée en une ligne par type.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q90 — Nombre d'agents par type, en activité et au total
-- Objectif : une ligne par type d'agent, avec les agents en activité et tous
--            ceux que l'application a connus.
-- Tableaux : T58 à T61, T65 (périmètre de l'effectif), T60.
-- Attendu  : d'après lot 13, Q86 :
--            1. Agent CNSS permanent        869 en activité, 2 166 au total
--            2. Agent CNSS temporaire         6 en activité,   837 au total
--            3. Agent de la fonction publique 3 en activité,    40 au total
--            4. Agent affecté                 0 en activité,    21 au total
--            5. Type non renseigné            1 en activité,   116 au total
--            Total                          879 en activité, 3 180 au total
-- Résultat : premier essai (25/10/2026) en échec, ORA-00979, sur la ligne Total
--            calculée par ROLLUP et GROUPING. Réécrite avec UNION ALL : à
--            relancer.
-- -----------------------------------------------------------------------------
WITH derniere_situation AS (
    -- Dernière situation de chaque agent : elle donne sa nature d'emploi
    SELECT s.PERS_ID,
           s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
agents AS (
    -- Type de chaque agent : qualité (A, B, C) croisée avec la nature d'emploi
    SELECT p.PERS_ID,
           p.ETAT_EMPLOYE,
           CASE
               WHEN TO_CHAR(p.QUALITE) = 'A' AND ds.NATURE_EMPLOIE = 'PERMANENT'
                    THEN '1. Agent CNSS permanent'
               WHEN TO_CHAR(p.QUALITE) = 'A' AND ds.NATURE_EMPLOIE = 'TEMPORAIRE'
                    THEN '2. Agent CNSS temporaire'
               WHEN TO_CHAR(p.QUALITE) = 'B'
                    THEN '3. Agent de la fonction publique'
               WHEN TO_CHAR(p.QUALITE) = 'C'
                    THEN '4. Agent affecté'
               ELSE '5. Type non renseigné'
           END AS type_agent
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN derniere_situation ds
           ON ds.PERS_ID = p.PERS_ID
          AND ds.rn = 1
)
-- Une ligne par type
SELECT type_agent,
       SUM(CASE WHEN ETAT_EMPLOYE = 'ACTIF' THEN 1 ELSE 0 END) AS en_activite,
       COUNT(*)                                                AS total_connus
FROM agents
GROUP BY type_agent
UNION ALL
-- Puis une ligne Total
SELECT 'Total',
       SUM(CASE WHEN ETAT_EMPLOYE = 'ACTIF' THEN 1 ELSE 0 END),
       COUNT(*)
FROM agents
ORDER BY 1;
