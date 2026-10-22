-- =============================================================================
-- LOT 11 — Diagnostic du lot 10 : effectif et ratio vides, budget de formation
-- Rédigé le 22/10/2026 — À EXÉCUTER sur la base du DWH (schémas DWH et DTM).
-- Exécution : en mode script (F5) ; aucune saisie de cliché, les requêtes
-- prennent celui qui est présent dans les faits du DWH. Tout est en SELECT et
-- ne renvoie que des comptages ou des montants agrégés (aucune donnée
-- nominative). Alias limités à 30 caractères, sans accents.
-- Pourquoi ce lot : au lot 10, Q70 à Q72 (effectif et ratio) n'ont renvoyé
-- aucune ligne, alors que Q73 (formation) fonctionne. Or le chargement des
-- faits GRH archive le contenu du DWH dans l'ODS puis vide les tables
-- (TRUNCATE, pipeline_facts.py) : le DWH ne garde que le cliché du dernier
-- chargement. Si le cliché saisi n'est pas celui-là, l'effectif et le ratio
-- ne trouvent aucun agent ; la formation n'a besoin des faits que pour le sexe.
-- Ensuite : relancer Q70 à Q72 du lot 10 en saisissant le cliché affiché par
-- Q75, sans guillemets.
-- Exécuté le 22/10/2026. Conclusions du lot :
--   - le DWH ne contient que le cliché 102026, avec toutes les données utiles :
--     le cliché n'explique pas les résultats vides du lot 10 (Q75 à Q77) ;
--     hypothèse suivante, testée au lot 12 : SQL Developer ne transmet la
--     valeur qu'à la première occurrence de :1 (écartée au lot 12, Q84 : la
--     valeur saisie au lot 10 était en cause) ;
--   - le champ BUDGET des formations contient une année budgétaire, pas un
--     montant : le budget de formation ne peut pas venir de la GRH (Q78, Q79).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q75 — Clichés présents dans les faits GRH du DWH
-- Objectif : savoir quel cliché saisir pour tester l'effectif et le ratio.
-- Attendu  : un seul cliché par table, le même partout (mois du dernier
--            chargement des faits). Une table absente du résultat est vide :
--            il faudra relancer le chargement des faits avant le DTM.
-- Résultat : un seul cliché, 102026, partout : FAIT_GRH_DECISION 29 115 lignes,
--            FAIT_GRH_DECISION_PERSONNE 38 013, FAIT_GRH_PERSONNE 3 180,
--            FAIT_GRH_SITUATION 10 906.
-- Constat  : le DWH ne garde bien que le dernier chargement des faits. Le
--            cliché 102026 était le bon : la cause des résultats vides du lot
--            10 est ailleurs.
-- -----------------------------------------------------------------------------
SELECT 'FAIT_GRH_PERSONNE' AS table_dwh, CLICHE, COUNT(*) AS lignes
FROM DWH.FAIT_GRH_PERSONNE GROUP BY CLICHE
UNION ALL
SELECT 'FAIT_GRH_SITUATION', CLICHE, COUNT(*)
FROM DWH.FAIT_GRH_SITUATION GROUP BY CLICHE
UNION ALL
SELECT 'FAIT_GRH_DECISION', CLICHE, COUNT(*)
FROM DWH.FAIT_GRH_DECISION GROUP BY CLICHE
UNION ALL
SELECT 'FAIT_GRH_DECISION_PERSONNE', CLICHE, COUNT(*)
FROM DWH.FAIT_GRH_DECISION_PERSONNE GROUP BY CLICHE
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q76 — Étapes de la requête d'effectif sur le dernier cliché du DWH
-- Objectif : si Q75 montre que le cliché saisi au lot 10 était le bon, voir à
--            quelle étape les agents disparaissent.
-- Attendu  : environ 3 180 agents, dont 879 à l'état ACTIF, presque tous avec
--            une date d'entrée et une date de naissance ; plusieurs centaines
--            de décisions de sortie (source figée au 18/02/2026).
-- Résultat : 102026 · 3 180 agents · 879 à l'état ACTIF · 3 180 avec une date
--            d'entrée · 3 180 avec une date de naissance · 10 906 situations ·
--            1 499 décisions de sortie.
-- Constat  : toutes les données utiles sont là, la requête d'effectif aurait
--            dû trouver des agents. Hypothèse : SQL Developer ne transmet la
--            valeur saisie qu'à la première occurrence de :1, les suivantes
--            valent NULL. La formation (seule son occurrence de jointure en
--            dépend, et c'est une jointure externe) et Q74 (une seule
--            occurrence) n'y sont pas sensibles. Test au lot 12 : hypothèse
--            écartée (Q84), la requête renvoie 885 agents en 2025 (Q80).
-- -----------------------------------------------------------------------------
WITH C AS (
    SELECT CLICHE FROM (
        SELECT CLICHE, TO_DATE(LPAD(TO_CHAR(CLICHE), 6, '0'), 'MMYYYY') AS mois
        FROM DWH.FAIT_GRH_PERSONNE
        GROUP BY CLICHE
        ORDER BY mois DESC
    ) WHERE ROWNUM = 1
)
SELECT c.CLICHE,
       (SELECT COUNT(*) FROM DWH.FAIT_GRH_PERSONNE p
         WHERE p.CLICHE = c.CLICHE) AS agents,
       (SELECT COUNT(*) FROM DWH.FAIT_GRH_PERSONNE p
         WHERE p.CLICHE = c.CLICHE AND p.ETAT_EMPLOYE = 'ACTIF') AS agents_actifs,
       (SELECT COUNT(*) FROM DWH.FAIT_GRH_PERSONNE p
         WHERE p.CLICHE = c.CLICHE
           AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) IS NOT NULL) AS avec_date_entree,
       (SELECT COUNT(*) FROM DWH.FAIT_GRH_PERSONNE p
         WHERE p.CLICHE = c.CLICHE AND p.DATE_NAISS IS NOT NULL) AS avec_date_naissance,
       (SELECT COUNT(*) FROM DWH.FAIT_GRH_SITUATION s
         WHERE s.CLICHE = c.CLICHE) AS situations,
       (SELECT COUNT(*) FROM DWH.FAIT_GRH_DECISION d
         WHERE d.CLICHE = c.CLICHE
           AND d.CDEC_CODE IN ('02','34','35','36','52','54')
           AND d.DATE_VIGUEUR IS NOT NULL) AS decisions_sortie
FROM C c;


-- -----------------------------------------------------------------------------
-- Q77 — États des agents dans les faits du DWH
-- Objectif : vérifier que l'état « ACTIF » est écrit comme dans la source
--            (la requête d'effectif compare ETAT_EMPLOYE à 'ACTIF').
-- Attendu  : ACTIF 879 (longueur 5), INACTIF 2 234, vide 67.
-- Résultat : 102026 · ACTIF (5) 879 · INACTIF (7) 2 234 · vide 67.
-- Constat  : identique à la source, sans espace parasite.
-- -----------------------------------------------------------------------------
SELECT p.CLICHE,
       NVL(p.ETAT_EMPLOYE, '(vide)') AS etat,
       LENGTH(p.ETAT_EMPLOYE) AS longueur,
       COUNT(*) AS agents
FROM DWH.FAIT_GRH_PERSONNE p
GROUP BY p.CLICHE, NVL(p.ETAT_EMPLOYE, '(vide)'), LENGTH(p.ETAT_EMPLOYE)
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q78 — Budget des formations par année (T64)
-- Objectif : comprendre pourquoi le budget du lot 10 (Q73) est 1 000 à 3 000
--            fois plus faible que le coût : budgets écartés par le contrôle
--            numérique de la requête, ou saisis en milliers ?
-- Attendu  : si budget_numerique est bien inférieur à budget_saisi, des
--            budgets sont écrits avec des espaces, des points ou une unité ;
--            si budget_max reste de l'ordre de quelques milliers, ils sont
--            saisis en milliers de francs.
-- Résultat : année · formations · budget saisi · numérique · min · max · total
--            2019 · 31 · 0 · 0 · vide · vide · vide
--            2020 · 28 · 28 · 28 · 2019 · 2020 · 56 557
--            2021 · 37 · 37 · 37 · 2020 · 2021 · 74 767
--            2022 · 39 · 39 · 39 · 2021 · 2022 · 78 854
--            2023 · 36 · 36 · 36 · 2022 · 2023 · 72 820
--            2024 · 45 · 44 · 44 · 2023 · 2024 · 89 046
--            2025 · 35 · 35 · 35 · 2024 · 2025 · 70 869
-- Constat  : le champ BUDGET contient une année, celle de la formation ou la
--            précédente : c'est l'exercice budgétaire, pas un montant. Le
--            « budget » du lot 10 n'est qu'une somme d'années (56 557 = 28
--            formations × 2 020 environ). BUDGET_FORMATION doit rester vide ;
--            le montant du budget de formation viendra de la comptabilité.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       COUNT(*) AS formations,
       SUM(CASE WHEN f.BUDGET IS NOT NULL THEN 1 ELSE 0 END) AS budget_saisi,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+(\.[0-9]+)?$')
                THEN 1 ELSE 0 END) AS budget_numerique,
       MIN(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+(\.[0-9]+)?$')
                THEN TO_NUMBER(f.BUDGET) END) AS budget_min,
       MAX(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+(\.[0-9]+)?$')
                THEN TO_NUMBER(f.BUDGET) END) AS budget_max,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+(\.[0-9]+)?$')
                THEN TO_NUMBER(f.BUDGET) END) AS budget_total
FROM DTM.DIM_GRH_FORMATION f
WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
  AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q79 — Formes d'écriture des budgets saisis (T64)
-- Objectif : voir comment le budget est écrit, sans afficher les montants :
--            chaque chiffre devient 9 et chaque lettre a (ex. « 9 999 999 »,
--            « 9999999 aaaa »).
-- Attendu  : les 20 formes les plus fréquentes, avec les années concernées.
-- Résultat : une seule forme, « 9999 », pour 220 formations, de 2004 à 2025.
-- Constat  : toujours 4 chiffres : confirme que BUDGET est une année.
-- -----------------------------------------------------------------------------
SELECT * FROM (
    SELECT REGEXP_REPLACE(REGEXP_REPLACE(f.BUDGET, '[0-9]', '9'), '[A-Za-z]', 'a') AS forme,
           COUNT(*) AS formations,
           MIN(EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))) AS premiere_annee,
           MAX(EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))) AS derniere_annee
    FROM DTM.DIM_GRH_FORMATION f
    WHERE f.BUDGET IS NOT NULL
    GROUP BY REGEXP_REPLACE(REGEXP_REPLACE(f.BUDGET, '[0-9]', '9'), '[A-Za-z]', 'a')
    ORDER BY COUNT(*) DESC
) WHERE ROWNUM <= 20;
