-- =============================================================================
-- LOT 20 — Les mouvements de T60 que les sorties ne donnent pas
-- Rédigé le 01/11/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement,
-- aucune donnée nominative.
-- Pourquoi ce lot : les lots 17 à 19 retrouvent les recrutements, les
-- retraites, les démissions et les licenciements du tableau 121 de l'annuaire
-- de la CNSS (T60). Restent les décès (plus de date de décès depuis 2010), les
-- disponibilités, les remises en activité, les détachements, et la sortie des
-- 84 engagés de 2019. L'application a pour cela des décisions (07 mise en
-- disponibilité, 18 remise en activité, 14 mise à disposition, 37 engagement
-- de détaché : lot 3, Q15) et des statuts de situation (R retraite, X radié,
-- Y décédé : lot 2, Q08) que les lots précédents n'ont pas comptés.
-- Chiffres publiés (annuaire de la CNSS 2024, tableau 121), de 2020 à 2024 :
--   décès 4, 2, 3, 5, 3 ; disponibilités 5, 3, 2, 4, 7 ; remises en activité
--   3, 2, 3, 5, 6 ; détachements (entrées) 0, 0, 0, 2, 1 ; fins de
--   détachement 0, 0, 0, 2, 0 ; autres sorties 0, 1, 1, 2, 84.
-- Numérotation : suite du lot 19, Q117.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q117 — Décisions de position et statuts de fin de carrière, de 2019 à 2024
-- Objectif : chez les permanents (agents CNSS de nature PERMANENT au 31/12 de
--            l'année), compter par année les agents qui ont une décision 07,
--            18, 14 ou 37 non annulée (date de vigueur), et ceux dont une
--            situation passe au statut R (retraite), X (radié) ou Y (décédé)
--            (date de début) ; et, à part, combien de ces agents sont des 84
--            engagés de 2019.
-- Tableaux : T60.
-- Lecture  : comparer aux chiffres publiés de l'en-tête. Le statut Y daterait
--            les décès, le statut X la sortie des 84 (2024 dans l'annuaire),
--            les décisions 07 et 18 les disponibilités et les remises en
--            activité.
-- Résultat (01/11/2026) : mouvement · 2019 · 2020 · 2021 · 2022 · 2023 · 2024 ·
--            dont les 84 engagés de 2019
--            Décision 07 (mise en disponibilité)  7 · 2 · 4 · 2 · 5 · 5 ·  1
--            Décision 18 (remise en activité)     6 · 4 · 3 · 6 · 5 · 4 ·  0
--            Décision 14 (mise à disposition)     2 · 2 · 1 · 2 · 1 · 2 ·  1
--            Statut Y (décédé)                    0 · 1 · 0 · 0 · 0 · 0 ·  0
--            Statut X (radié)                    10 · 0 · 0 · 1 · 0 · 3 · 10
--            Statut R (retraite)                  3 · 2 · 0 · 0 · 0 · 0 ·  0
--            Aucune décision 37 (engagement de détaché) chez les permanents.
-- Constat  : - disponibilités : la décision 07 donne 2, 4, 2, 5, 5 de 2020 à
--              2024 pour 5, 3, 2, 4, 7 publiées (18 pour 21) ; remises en
--              activité : la décision 18 donne 4, 3, 6, 5, 4 pour 3, 2, 3, 5, 6
--              (22 pour 19). Ce sont bien les mouvements de l'annuaire, à 1 à 3
--              près par an ;
--            - les statuts de fin de carrière ne sont presque pas saisis : un
--              seul décédé (Y) en six ans, des retraites (R) jusqu'en 2020
--              seulement. Les décès restent à lire dans la liquidation des
--              droits (34), faute de date de décès ;
--            - 10 des 84 engagés de 2019 sont radiés (X) dès 2019 ; les 74
--              autres n'ont aucune trace de départ. L'annuaire les sort tous en
--              2024 : la base ne permet pas de le refaire ;
--            - les détachements publiés (2 en 2023, 1 en 2024) ne sont pas des
--              décisions 37 de permanents : question à la DRH.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
engages_2019 AS (
    SELECT DISTINCT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE = '51'
      AND d.DATE_VIGUEUR >= DATE '2019-01-01' AND d.DATE_VIGUEUR < DATE '2020-01-01'
),
-- les permanents de chaque année : agent CNSS, dernière situation commencée au
-- 31/12 de nature PERMANENT
nature AS (
    SELECT a.annee, s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY a.annee, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM annees a
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
),
permanents AS (
    SELECT n.annee, n.PERS_ID
    FROM nature n
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = n.PERS_ID
    WHERE n.rn = 1
      AND n.NATURE_EMPLOIE = 'PERMANENT'
      AND TO_CHAR(p.QUALITE) = 'A'
),
-- les décisions de position, une ligne par agent et par année
decisions AS (
    SELECT DISTINCT
           EXTRACT(YEAR FROM d.DATE_VIGUEUR)                      AS annee,
           NVL(d.PERS_ID, dp.PERS_ID)                             AS PERS_ID,
           CASE d.CDEC_CODE
                WHEN '07' THEN '1. Décision 07 : mise en disponibilité'
                WHEN '18' THEN '2. Décision 18 : remise en activité'
                WHEN '14' THEN '3. Décision 14 : mise à disposition'
                ELSE           '4. Décision 37 : engagement de détaché' END AS mouvement
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('07', '18', '14', '37')
      AND d.DATE_VIGUEUR >= DATE '2019-01-01'
      AND d.DATE_VIGUEUR <  DATE '2025-01-01'
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
-- le statut de chaque situation et celui de la situation d'avant
statuts AS (
    SELECT s.PERS_ID, s.DATE_DEBUT,
           NVL(TO_CHAR(s.STATUT_EMPLOYE), '-')                    AS statut,
           LAG(NVL(TO_CHAR(s.STATUT_EMPLOYE), '-'))
               OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT, s.SITU_ID) AS statut_avant
    FROM USER_GRH.GRH_SITUATION s
),
fins AS (
    SELECT DISTINCT
           EXTRACT(YEAR FROM DATE_DEBUT)                          AS annee,
           PERS_ID,
           CASE statut WHEN 'Y' THEN '5. Statut Y : décédé'
                       WHEN 'X' THEN '6. Statut X : radié'
                       ELSE          '7. Statut R : retraite' END AS mouvement
    FROM statuts
    WHERE statut IN ('R', 'X', 'Y')
      AND NVL(statut_avant, '-') <> statut
      AND DATE_DEBUT >= DATE '2019-01-01'
      AND DATE_DEBUT <  DATE '2025-01-01'
),
lignes AS (
    SELECT m.mouvement, m.annee,
           CASE WHEN e.PERS_ID IS NOT NULL THEN 1 ELSE 0 END      AS engage_2019
    FROM (SELECT annee, PERS_ID, mouvement FROM decisions
          UNION ALL
          SELECT annee, PERS_ID, mouvement FROM fins) m
    JOIN permanents pe ON pe.PERS_ID = m.PERS_ID AND pe.annee = m.annee
    LEFT JOIN engages_2019 e ON e.PERS_ID = m.PERS_ID
)
SELECT mouvement,
       SUM(CASE WHEN annee = 2019 THEN 1 ELSE 0 END)              AS a2019,
       SUM(CASE WHEN annee = 2020 THEN 1 ELSE 0 END)              AS a2020,
       SUM(CASE WHEN annee = 2021 THEN 1 ELSE 0 END)              AS a2021,
       SUM(CASE WHEN annee = 2022 THEN 1 ELSE 0 END)              AS a2022,
       SUM(CASE WHEN annee = 2023 THEN 1 ELSE 0 END)              AS a2023,
       SUM(CASE WHEN annee = 2024 THEN 1 ELSE 0 END)              AS a2024,
       SUM(engage_2019)                                           AS dont_engages_2019
FROM lignes
GROUP BY mouvement
ORDER BY mouvement;
