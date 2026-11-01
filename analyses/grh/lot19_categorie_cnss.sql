-- =============================================================================
-- LOT 19 — La catégorie de la CNSS, et les derniers écarts avec l'annuaire
-- Rédigé le 01/11/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement,
-- aucune donnée nominative.
-- Pourquoi ce lot : le lot 18 (Q103) a trouvé le champ de catégorie de la
-- CNSS : GRH_SITUATION.CATEGORIE, la catégorie de la grille de salaire. Avec
-- 01 et 02 = employés, 03 = agents de maîtrise, 04 et 05 = cadres moyens, 06 =
-- cadres supérieurs, les permanents au 31/12/2024 donnent 173, 207, 304 et 58
-- pour 177, 208, 311 et 62 publiés. Ce lot :
--   1. lit les libellés des catégories (Q110) ;
--   2. refait T58 (Q111), T65 (Q112) et T62 (Q113) avec cette catégorie ;
--   3. cherche les 46 contractuels publiés (Q114) et les 16 permanents qui
--      manquent au 31/12/2024 (Q115).
-- Règles reprises des lots 17 et 18 : présent au 31/12 = entré au plus tard le
-- 31/12 et encore ACTIF ou sorti après le 31/12 ; sortie = première décision
-- 02, 34, 35, 36, 52 ou 54 non annulée par une 40, à défaut le décès ; l'agent
-- est décrit par sa dernière situation commencée au 31/12 ; permanent =
-- qualité A et nature PERMANENT.
-- Catégorie CIPRES (annuaire de la CNSS 2024) : 01, 02 employés ; 03 agents de
-- maîtrise ; 04, 05 cadres moyens ; 06 cadres supérieurs. L'atelier de Kaya
-- (2017) rangeait 05 en cadres supérieurs : les requêtes montrent 05 à part.
-- Chiffres publiés (annuaire de la CNSS 2024) : voir l'en-tête du lot 17.
-- Numérotation : suite du lot 18, Q110 à Q116.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q110 — Les libellés des catégories, qualifications et échelles
-- Objectif : lire le sens des codes de catégorie (01 à 06 et les autres), de
--            qualification et d'échelle, dans les listes de codes de
--            l'application et dans la vue des catégories.
-- Tableaux : T58, T62, T65.
-- Lecture  : vérifier que 04 et 05 sont des catégories de cadres moyens, et 06
--            de cadres supérieurs.
-- Résultat : premier essai (01/11/2026) en échec : ORA-01722 (nombre non
--            valide). L'erreur vient sans doute de la vue V_GRH_CATEGORIE, qui
--            convertirait un code en nombre ; la vue est retirée, les listes
--            de codes de l'application suffisent. Relancée le 01/11/2026 :
--            GRH_QUALIFICATION : HC Cadre supérieur (S) ; 01, 02 Agent
--            d'exécution (A) ; 03 Agent de maîtrise (I) ; 04 Cadre moyen (M) ;
--            05, 06 Cadre supérieur (S).
--            Liste CATEGORIE : 01 à 06 « 1ère » à « 6ième catégorie » ; HC hors
--            catégorie ; I à V et 001 à 005 « Catégorie I » à « Catégorie V ».
--            Liste CATEGORIE_SUPERIEUR : CS chef de service, DR directeur
--            régional, DS directeur de service, SG secrétaire général.
--            Liste ECHELLE : A, B, C ; DA directeur général adjoint, DG
--            directeur général, SG secrétaire général.
--            Liste QUALIFICATION : A agents d'exécution, I agents de maîtrise,
--            M cadres moyens, S cadres supérieurs.
-- Constat  : - les catégories 01 à 06 sont les six catégories de la grille de
--              la CNSS, sans libellé d'employé ou de cadre : le regroupement
--              01-02 / 03 / 04-05 / 06 vient de l'annuaire de 2024 (Q111, Q112),
--              pas de l'application ;
--            - le regroupement de la qualification (05 et 06 en cadres
--              supérieurs) est celui de Kaya (2017), pas celui de l'annuaire
--              de 2024 ;
--            - les codes I à V et 001 à 005 (catégories de la fonction
--              publique ?) ne concernent aucun permanent au 31/12/2024 (lot
--              18, Q103).
-- -----------------------------------------------------------------------------
SELECT 'Liste ' || RV_DOMAIN       AS source,
       TO_CHAR(RV_LOW_VALUE)       AS code,
       TO_CHAR(RV_MEANING)         AS libelle,
       TO_CHAR(NULL)               AS niveau
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN IN ('CATEGORIE', 'CATEGORIE_SUPERIEUR', 'QUALIFICATION',
                    'ECHELLE', 'RECLASSEMENT')
UNION ALL
SELECT 'GRH_QUALIFICATION', TO_CHAR(QUAL_CODE), TO_CHAR(NOM), TO_CHAR(NIVEAU)
FROM USER_GRH.GRH_QUALIFICATION
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q111 — Les permanents au 31/12/2024 par catégorie, sexe et âge (T58)
-- Objectif : une ligne par code de catégorie de la situation au 31/12/2024 :
--            hommes, femmes, total, et les tranches d'âge de l'Annuaire (âge en
--            années pleines au 31/12).
-- Tableaux : T58 (tableau 120 de l'annuaire de la CNSS).
-- Lecture  : en additionnant 01 + 02, 03, 04 + 05 et 06, retrouver par sexe
--            133 H / 44 F (employés), 94 / 114 (maîtrise), 143 / 168 (cadres
--            moyens), 42 / 20 (cadres supérieurs), à quelques agents près.
-- Résultat (01/11/2026) : catégorie · hommes · femmes · total · moins de 20 ·
--            20-24 · 25-29 · 30-34 · 35-39 · 40-44 · 45-49 · 50-54 · 55-59 · 60+
--            01 ·  17 ·   3 ·  20 · 0 · 0 · 0 ·  0 ·  2 ·  3 ·  4 ·  9 ·  2 · 0
--            02 · 112 ·  41 · 153 · 0 · 2 · 10 · 19 · 21 · 27 · 34 · 33 ·  7 · 0
--            03 ·  92 · 115 · 207 · 0 · 4 ·  9 · 18 · 43 · 34 · 41 · 45 · 13 · 0
--            04 ·  97 · 123 · 220 · 0 · 4 ·  4 · 22 · 31 · 36 · 49 · 39 · 35 · 0
--            05 ·  42 ·  42 ·  84 · 0 · 2 ·  4 ·  6 · 10 · 23 · 15 · 18 ·  6 · 0
--            06 ·  40 ·  18 ·  58 · 0 · 0 ·  1 ·  5 ·  8 · 16 · 12 ·  7 ·  9 · 0
--            Regroupé (hommes / femmes, publié entre parenthèses) : employés
--            129 / 44 (133 / 44) ; agents de maîtrise 92 / 115 (94 / 114) ;
--            cadres moyens 139 / 165 (143 / 168) ; cadres supérieurs 40 / 18
--            (42 / 20).
-- Constat  : - chaque case catégorie et sexe est à 4 agents près de l'annuaire,
--              les femmes employées exactement : la catégorie de la situation
--              est bien celle de la CNSS ;
--            - avec le regroupement de Kaya (05 en cadres supérieurs), on
--              aurait 220 cadres moyens et 142 cadres supérieurs pour 311 et
--              62 : l'annuaire de 2024 range bien 05 en cadres moyens.
-- -----------------------------------------------------------------------------
WITH sorties AS (
    SELECT PERS_ID, MIN(dt) AS dt_sortie
    FROM (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
        WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
          AND d.DATE_VIGUEUR IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                          WHERE a.CDEC_CODE = '40'
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
        UNION ALL
        SELECT PERS_ID, DATE_DECES FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
    )
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
situ AS (
    SELECT s.PERS_ID, s.CATEGORIE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
permanents AS (
    SELECT NVL(TO_CHAR(si.CATEGORIE), '-')                        AS categorie,
           p.SEXE,
           FLOOR((DATE '2024-12-31' - p.DATE_NAISS) / 365.25)     AS age
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    WHERE COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31')
      AND TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
)
SELECT categorie,
       SUM(CASE WHEN SEXE = 'M' THEN 1 ELSE 0 END)                AS hommes,
       SUM(CASE WHEN SEXE = 'F' THEN 1 ELSE 0 END)                AS femmes,
       COUNT(*)                                                   AS total,
       SUM(CASE WHEN age < 20 THEN 1 ELSE 0 END)                  AS moins_20,
       SUM(CASE WHEN age BETWEEN 20 AND 24 THEN 1 ELSE 0 END)     AS a20_24,
       SUM(CASE WHEN age BETWEEN 25 AND 29 THEN 1 ELSE 0 END)     AS a25_29,
       SUM(CASE WHEN age BETWEEN 30 AND 34 THEN 1 ELSE 0 END)     AS a30_34,
       SUM(CASE WHEN age BETWEEN 35 AND 39 THEN 1 ELSE 0 END)     AS a35_39,
       SUM(CASE WHEN age BETWEEN 40 AND 44 THEN 1 ELSE 0 END)     AS a40_44,
       SUM(CASE WHEN age BETWEEN 45 AND 49 THEN 1 ELSE 0 END)     AS a45_49,
       SUM(CASE WHEN age BETWEEN 50 AND 54 THEN 1 ELSE 0 END)     AS a50_54,
       SUM(CASE WHEN age BETWEEN 55 AND 59 THEN 1 ELSE 0 END)     AS a55_59,
       SUM(CASE WHEN age >= 60 THEN 1 ELSE 0 END)                 AS plus_60
FROM permanents
GROUP BY categorie
ORDER BY categorie;


-- -----------------------------------------------------------------------------
-- Q112 — Le taux d'encadrement par la catégorie, de 2020 à 2024 (T65)
-- Objectif : pour chaque année, permanents présents au 31/12 par catégorie de
--            leur situation à cette date : non-cadres (01 à 03), catégories 04,
--            05 et 06, sans catégorie, total, taux d'encadrement (04, 05, 06) ;
--            et, à part, les 84 engagés de 2019 sans sortie datée (lot 18,
--            Q105) que l'annuaire compte présents de 2019 à 2023.
-- Tableaux : T65 (tableau 124 de l'annuaire de la CNSS).
-- Lecture  : publié, cadres / total : 2020 358 / 869 ; 2021 345 / 834 ; 2022
--            333 / 806 ; 2023 316 / 767 ; 2024 373 / 758. Pour 2020 à 2023,
--            ajouter les colonnes des 84 aux colonnes de la base.
-- Résultat (01/11/2026) : année · non-cadres · 04 · 05 · 06 · autre · total ·
--            taux · les 84 : non-cadres · cadres
--            2020 · 448 · 200 · 83 · 46 · 0 · 777 · 42,3 % · 59 · 25
--            2021 · 425 · 191 · 80 · 45 · 0 · 741 · 42,6 % · 59 · 25
--            2022 · 404 · 182 · 77 · 43 · 1 · 707 · 42,7 % · 59 · 25
--            2023 · 388 · 173 · 70 · 41 · 0 · 672 · 42,3 % · 59 · 25
--            2024 · 380 · 220 · 84 · 58 · 0 · 742 · 48,8 % ·  — ·  —
--            Avec les 84 (cadres / total, taux ; publié entre parenthèses) :
--            2020 354 / 861, 41,1 % (358 / 869, 41,2 %) ; 2021 341 / 825,
--            41,3 % (345 / 834, 41,4 %) ; 2022 327 / 791, 41,3 % (333 / 806,
--            41,3 %) ; 2023 309 / 756, 40,9 % (316 / 767, 41,2 %) ; 2024
--            362 / 742, 48,8 % (373 / 758, 49,2 %).
-- Constat  : - le taux d'encadrement de l'annuaire est retrouvé à 0,3 point
--              près chaque année, en comptant les 84 engagés de 2019 comme
--              l'annuaire (25 cadres, 59 non-cadres) ;
--            - le saut de 2024 (de 41 à 49 %) vient de la sortie des 84
--              (surtout des non-cadres) et des recrutements de 2024 (47
--              agents de catégorie 04 de plus) ;
--            - la règle de T65 est donc : cadres = catégories 04, 05 et 06,
--              sur les permanents présents au 31/12.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 5
),
sorties AS (
    SELECT PERS_ID, MIN(dt) AS dt_sortie
    FROM (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
        WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
          AND d.DATE_VIGUEUR IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                          WHERE a.CDEC_CODE = '40'
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
        UNION ALL
        SELECT PERS_ID, DATE_DECES FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
    )
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
-- les 84 engagés de 2019 (décision 51), fiche inactive, sans sortie datée
engages_2019 AS (
    SELECT DISTINCT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = NVL(d.PERS_ID, dp.PERS_ID)
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    WHERE d.CDEC_CODE = '51'
      AND d.DATE_VIGUEUR >= DATE '2019-01-01' AND d.DATE_VIGUEUR < DATE '2020-01-01'
      AND NVL(p.ETAT_EMPLOYE, '-') <> 'ACTIF'
      AND so.dt_sortie IS NULL
),
-- tous les agents CNSS, avec leur situation au 31/12 de chaque année
situ AS (
    SELECT a.annee, p.PERS_ID, p.ETAT_EMPLOYE,
           COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
           so.dt_sortie, s.CATEGORIE, s.NATURE_EMPLOIE,
           CASE WHEN e.PERS_ID IS NOT NULL THEN 1 ELSE 0 END      AS engage_2019,
           ROW_NUMBER() OVER (PARTITION BY a.annee, p.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM annees a
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = s.PERS_ID
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    LEFT JOIN engages_2019 e ON e.PERS_ID = p.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
),
base AS (
    SELECT annee, engage_2019,
           CASE WHEN date_entree <= TO_DATE(annee || '1231', 'YYYYMMDD')
                 AND (ETAT_EMPLOYE = 'ACTIF'
                      OR dt_sortie > TO_DATE(annee || '1231', 'YYYYMMDD'))
                THEN 1 ELSE 0 END                                 AS present,
           NVL(TO_CHAR(CATEGORIE), '-')                           AS categorie
    FROM situ
    WHERE rn = 1
      AND NATURE_EMPLOIE = 'PERMANENT'
)
SELECT annee,
       SUM(CASE WHEN present = 1 AND categorie IN ('01', '02', '03') THEN 1 ELSE 0 END) AS non_cadres,
       SUM(CASE WHEN present = 1 AND categorie = '04' THEN 1 ELSE 0 END) AS cat_04,
       SUM(CASE WHEN present = 1 AND categorie = '05' THEN 1 ELSE 0 END) AS cat_05,
       SUM(CASE WHEN present = 1 AND categorie = '06' THEN 1 ELSE 0 END) AS cat_06,
       SUM(CASE WHEN present = 1 AND categorie NOT IN ('01', '02', '03', '04', '05', '06')
                THEN 1 ELSE 0 END)                                AS autre_ou_vide,
       SUM(present)                                               AS permanents,
       ROUND(100 * SUM(CASE WHEN present = 1 AND categorie IN ('04', '05', '06')
                            THEN 1 ELSE 0 END)
             / NULLIF(SUM(present), 0), 1)                        AS taux_encadrement_pct,
       SUM(CASE WHEN engage_2019 = 1 AND categorie IN ('01', '02', '03')
                THEN 1 ELSE 0 END)                                AS engages_2019_non_cadres,
       SUM(CASE WHEN engage_2019 = 1 AND categorie IN ('04', '05', '06')
                THEN 1 ELSE 0 END)                                AS engages_2019_cadres
FROM base
GROUP BY annee
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q113 — Les formés de 2024 par la catégorie (T62)
-- Objectif : pour les formations qui touchent 2024, par code de catégorie de
--            la situation au 31/12/2024 : inscriptions et agents, tous et
--            permanents seuls ; inscriptions par finalité de la formation
--            (perfectionnement, diplômante, autre) ; classements changés.
-- Tableaux : T62 (tableau 123 de l'annuaire de la CNSS).
-- Lecture  : publié : employés 132, agents de maîtrise 142, cadres moyens 118,
--            cadres supérieurs 88 ; 480, dont 6 avec promotion. Chercher la
--            colonne (agents ou inscriptions) qui retombe sur cette
--            répartition en additionnant 01 + 02, 03, 04 + 05 et 06.
-- Résultat (01/11/2026) : catégorie · inscriptions · agents · inscriptions de
--            permanents · agents permanents · perfectionnement · diplômante ·
--            autre · classement changé
--            HC ·   1 ·   1 ·   0 ·   0 ·   1 · 0 · 0 · 0
--            01 ·   6 ·   6 ·   6 ·   6 ·   6 · 0 · 0 · 0
--            02 ·  90 ·  75 ·  90 ·  75 ·  90 · 0 · 0 · 0
--            03 · 103 ·  96 · 103 ·  96 · 103 · 0 · 0 · 0
--            04 · 140 · 110 · 140 · 110 · 140 · 0 · 0 · 0
--            05 ·  98 ·  71 ·  98 ·  71 ·  96 · 2 · 0 · 2
--            06 ·  87 ·  51 ·  87 ·  51 ·  86 · 0 · 1 · 0
--            Regroupé (inscriptions / agents des permanents ; publié) :
--            employés 96 / 81 (132) ; agents de maîtrise 103 / 96 (142) ;
--            cadres moyens 238 / 181 (118) ; cadres supérieurs 87 / 51 (88).
-- Constat  : - T62 ne retombe pas sur l'annuaire, ni en agents (409), ni en
--              inscriptions (524), ni par catégorie : la base a deux fois
--              plus de cadres moyens formés que l'annuaire, et moins
--              d'employés et d'agents de maîtrise. L'annuaire est sans doute
--              tiré d'un autre registre (le plan de formation ?). Question à
--              la DRH, non bloquante ;
--            - proposition BI : compter les agents distincts, par la
--              catégorie de la situation et la date de début de la formation
--              (Kaya 2017) ;
--            - les 2 classements changés sont deux formations diplômantes de
--              catégorie 05 ; l'annuaire publie 6 formations avec
--              promotion.
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.CATEGORIE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
inscrits AS (
    SELECT i.PERS_ID,
           NVL(TO_CHAR(si.CATEGORIE), '-')                        AS categorie,
           NVL(TO_CHAR(f.CATEGORIE), '-')                         AS finalite,
           CASE WHEN TO_CHAR(p.QUALITE) = 'A' AND si.NATURE_EMPLOIE = 'PERMANENT'
                THEN 1 ELSE 0 END                                 AS permanent,
           CASE WHEN i.CLA_APRES IS NOT NULL AND i.CLA_AVANT IS NOT NULL
                 AND TO_CHAR(i.CLA_APRES) <> TO_CHAR(i.CLA_AVANT)
                THEN 1 ELSE 0 END                                 AS classement_change
    FROM USER_GRH.GRH_INSCRIPTION i
    JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
    LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = i.PERS_ID
    LEFT JOIN situ si ON si.PERS_ID = i.PERS_ID AND si.rn = 1
    WHERE f.DATE_DEBUT < DATE '2025-01-01'
      AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2024-01-01'
)
SELECT categorie,
       COUNT(*)                                                   AS inscriptions,
       COUNT(DISTINCT PERS_ID)                                    AS agents,
       SUM(permanent)                                             AS inscriptions_permanents,
       COUNT(DISTINCT CASE WHEN permanent = 1 THEN PERS_ID END)   AS agents_permanents,
       SUM(CASE WHEN finalite = 'PERF'  THEN 1 ELSE 0 END)        AS insc_perfectionnement,
       SUM(CASE WHEN finalite = 'DIPLO' THEN 1 ELSE 0 END)        AS insc_diplomante,
       SUM(CASE WHEN finalite NOT IN ('PERF', 'DIPLO') THEN 1 ELSE 0 END) AS insc_autre,
       SUM(classement_change)                                     AS classement_change
FROM inscrits
GROUP BY categorie
ORDER BY categorie;


-- -----------------------------------------------------------------------------
-- Q114 — Les contractuels au 31/12/2024 (46 publiés)
-- Objectif : agents CNSS entrés au plus tard le 31/12/2024, de nature
--            TEMPORAIRE au 31/12/2024, sans sortie datée au plus tard le
--            31/12/2024, par code de catégorie : ceux que la règle compte
--            (ACTIF ou sortis après le 31/12) ; ceux qui ont un engagement
--            temporaire (décision 50) non annulé en 2024, ou au second
--            semestre 2023 ; ceux dont la dernière situation a commencé en
--            2024.
-- Tableaux : T58 (partie temporaires, Kaya 2017), tableau 119 de l'annuaire.
-- Lecture  : publié : 46 contractuels, dont employés 18, agents de maîtrise
--            13, cadres moyens 10, cadres supérieurs 5. La base n'en compte que
--            7 avec la règle de présence : les contrats en cours au 31/12/2024
--            (engagement de moins d'un an) retombent-ils sur 46 ?
-- Résultat (01/11/2026) : catégorie · sans sortie avant 2025 · comptés par la
--            règle · engagés (50) en 2024 · au 2e semestre 2023 · dernière
--            situation commencée en 2024
--            01 · 112 · 0 ·  1 · 0 ·  1
--            02 · 118 · 1 · 17 · 2 · 16
--            03 · 159 · 0 ·  8 · 0 ·  6
--            04 ·  45 · 0 · 12 · 0 ·  8
--            05 ·  26 · 2 ·  6 · 0 ·  4
--            Total · 460 · 3 · 44 · 2 · 35
-- Constat  : - 44 temporaires engagés en 2024 (et 2 au second semestre 2023)
--              sans sortie avant 2025 : 46, comme l'annuaire. Les
--              contractuels publiés sont donc les temporaires sous contrat au
--              31/12, lus par leur dernier engagement (décision 50) et non par
--              l'état de la fiche ;
--            - par catégorie, les employés retombent (18 pour 18), pas les
--              autres (8 agents de maîtrise pour 13, 18 cadres moyens pour 10,
--              aucun cadre supérieur pour 5) ;
--            - 3 seulement comptés par la règle de présence ici, contre 7 au
--              lot 17 (Q97) : 4 temporaires actifs ont déjà une fin de contrat
--              (35) en 2024, pour un contrat précédent. Pour les temporaires,
--              une fin de contrat n'est pas une sortie si un nouveau contrat
--              suit.
-- -----------------------------------------------------------------------------
WITH sorties AS (
    SELECT PERS_ID, MIN(dt) AS dt_sortie
    FROM (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
        WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
          AND d.DATE_VIGUEUR IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                          WHERE a.CDEC_CODE = '40'
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
        UNION ALL
        SELECT PERS_ID, DATE_DECES FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
    )
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
-- le dernier engagement temporaire (50) non annulé de chaque agent, au plus
-- tard le 31/12/2024
engagements AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, MAX(d.DATE_VIGUEUR) AS dt_engagement
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE = '50'
      AND d.DATE_VIGUEUR <= DATE '2024-12-31'
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
),
situ AS (
    SELECT s.PERS_ID, s.CATEGORIE, s.NATURE_EMPLOIE, s.DATE_DEBUT,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
temporaires AS (
    SELECT NVL(TO_CHAR(si.CATEGORIE), '-')                        AS categorie,
           CASE WHEN p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31'
                THEN 1 ELSE 0 END                                 AS compte_par_la_regle,
           CASE WHEN en.dt_engagement >= DATE '2024-01-01' THEN 1 ELSE 0 END AS engage_2024,
           CASE WHEN en.dt_engagement >= DATE '2023-07-01'
                 AND en.dt_engagement <  DATE '2024-01-01' THEN 1 ELSE 0 END AS engage_2s_2023,
           CASE WHEN si.DATE_DEBUT >= DATE '2024-01-01' THEN 1 ELSE 0 END AS situation_2024
    FROM USER_GRH.GRH_PERSONNE p
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    LEFT JOIN engagements en ON en.PERS_ID = p.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'TEMPORAIRE'
      AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (so.dt_sortie IS NULL OR so.dt_sortie > DATE '2024-12-31')
)
SELECT categorie,
       COUNT(*)                                                   AS sans_sortie_avant_2025,
       SUM(compte_par_la_regle)                                   AS comptes_par_la_regle,
       SUM(engage_2024)                                           AS engages_50_en_2024,
       SUM(engage_2s_2023)                                        AS engages_50_2e_sem_2023,
       SUM(situation_2024)                                        AS situation_debut_2024
FROM temporaires
GROUP BY categorie
ORDER BY categorie;


-- -----------------------------------------------------------------------------
-- Q115 — Les 16 permanents qui manquent au 31/12/2024
-- Objectif : permanents (nature PERMANENT au 31/12/2024) entrés au plus tard
--            le 31/12/2024 que la règle ne compte pas présents, hors les 84
--            engagés de 2019 : sortis en 2024 (par mois et motif, le 31/12 à
--            part), ou sans sortie datée (par année de début de leur dernière
--            situation) ; par tranche d'âge au 31/12/2024.
-- Tableaux : T58 à T61, T65.
-- Lecture  : les 16 manquants (lot 17, Q98) ont tous 35 ans ou plus, dont 7
--            de 55 à 59 ans ; l'annuaire n'a personne de 60 ans et plus. Un
--            groupe de 16 environ, sans agent de 60 ans et plus, désignerait
--            les agents que la CNSS compte encore : sortis le 31/12/2024,
--            sortis en décembre, ou partis sans date après 2022.
-- Résultat (01/11/2026) : groupe · moins de 35 · 35-54 · 55-59 · 60+ · total
--            Sorties de 2024 (aucune le 31/12) :
--              retraite (02) : mars 0 · 0 · 0 · 2 ; avril 0 · 0 · 1 · 0 ;
--              mai 0 · 0 · 1 · 0 ; juin 0 · 0 · 2 · 0 ; juillet 0 · 0 · 0 · 1 ;
--              août 0 · 0 · 1 · 0 ; septembre 0 · 0 · 1 · 2 ; octobre
--              0 · 0 · 0 · 2 ; novembre 0 · 0 · 1 · 2 ; décembre 0 · 0 · 8 · 1
--              (25 retraites, dont 9 en décembre) ;
--              liquidation des droits (34) : mars 0 · 1 · 1 · 0 ; juin
--              0 · 1 · 0 · 0 ; septembre 0 · 1 · 0 · 0 ; décembre 0 · 1 · 0 · 0 ;
--              démission (36) : mars 0 · 1 · 0 · 0 ; octobre 1 · 0 · 0 · 0 ;
--              licenciement (54) : octobre 1 · 0 · 0 · 0.
--            Sans sortie datée : dernière situation avant 2020, INACTIF
--              0 · 26 · 9 · 102 (137) ; avant 2020, état vide 0 · 0 · 0 · 5 ;
--              2024, INACTIF 1 · 0 · 0 · 0.
-- Constat  : - 9 retraites en décembre 2024, dont 8 agents de 55 à 59 ans :
--              si l'annuaire les compte encore présents au 31/12 (retraite
--              effective en janvier), la tranche 55-59 passe de 72 à 80 (79
--              publiés) et l'effectif de 742 à 750 ou 751 ;
--            - le reste de l'écart (7 à 9 agents de 35 à 54 ans) ne se voit
--              dans aucun groupe : les départs sans date ont presque tous une
--              dernière situation antérieure à 2020 ;
--            - écart restant de 1 % : accepté. Question à la DRH, non
--              bloquante : un agent retraité en décembre est-il compté présent
--              au 31/12 ? Q116 teste cette idée sur les retraites publiées de
--              2020 à 2024.
-- -----------------------------------------------------------------------------
WITH decisions AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt,
           d.CDEC_CODE                AS motif
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    UNION ALL
    SELECT PERS_ID, DATE_DECES, 'DECES' FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
),
sorties AS (
    SELECT PERS_ID, dt AS dt_sortie, motif
    FROM (SELECT PERS_ID, dt, motif,
                 ROW_NUMBER() OVER (PARTITION BY PERS_ID ORDER BY dt, motif) AS rn
          FROM decisions WHERE PERS_ID IS NOT NULL)
    WHERE rn = 1
),
engages_2019 AS (
    SELECT DISTINCT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE = '51'
      AND d.DATE_VIGUEUR >= DATE '2019-01-01' AND d.DATE_VIGUEUR < DATE '2020-01-01'
),
situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE, s.DATE_DEBUT,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
non_comptes AS (
    SELECT CASE WHEN so.dt_sortie IS NULL
                THEN '2. Sans sortie datée, dernière situation '
                     || CASE WHEN si.DATE_DEBUT < DATE '2020-01-01' THEN 'avant 2020'
                             ELSE TO_CHAR(EXTRACT(YEAR FROM si.DATE_DEBUT)) END
                     || ', état ' || NVL(p.ETAT_EMPLOYE, 'vide')
                ELSE '1. Sortie ' || TO_CHAR(so.dt_sortie, 'YYYY-MM')
                     || CASE WHEN so.dt_sortie = DATE '2024-12-31' THEN ' (le 31)' END
                     || ', motif ' || so.motif END                AS groupe,
           FLOOR((DATE '2024-12-31' - p.DATE_NAISS) / 365.25)     AS age
    FROM USER_GRH.GRH_PERSONNE p
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
      AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND NVL(p.ETAT_EMPLOYE, '-') <> 'ACTIF'
      AND (so.dt_sortie IS NULL
           OR (so.dt_sortie >= DATE '2024-01-01' AND so.dt_sortie <= DATE '2024-12-31'))
      AND NOT EXISTS (SELECT 1 FROM engages_2019 e WHERE e.PERS_ID = p.PERS_ID)
)
SELECT groupe,
       SUM(CASE WHEN age < 35 THEN 1 ELSE 0 END)                  AS moins_35,
       SUM(CASE WHEN age BETWEEN 35 AND 54 THEN 1 ELSE 0 END)     AS a35_54,
       SUM(CASE WHEN age BETWEEN 55 AND 59 THEN 1 ELSE 0 END)     AS a55_59,
       SUM(CASE WHEN age >= 60 THEN 1 ELSE 0 END)                 AS plus_60,
       COUNT(*)                                                   AS agents
FROM non_comptes
GROUP BY groupe
ORDER BY groupe;


-- -----------------------------------------------------------------------------
-- Q116 — Les retraites et les liquidations des permanents, mois par mois
-- Objectif : pour chaque année de 2019 à 2024, les premières sorties des
--            permanents par retraite (02) et par liquidation des droits (34),
--            mois par mois.
-- Tableaux : T60 (tableau 121 de l'annuaire de la CNSS).
-- Lecture  : publié, retraites : 2020 43, 2021 31, 2022 26, 2023 32, 2024 23 ;
--            décès : 4, 2, 3, 5, 3. Si l'annuaire compte une retraite de
--            décembre l'année suivante, ses retraites de l'année N valent
--            celles de la base de janvier à novembre N, plus décembre N−1
--            (Q115 : 9 en décembre 2024 dans la base).
-- Résultat (01/11/2026) : motif · année · janvier à décembre · total
--            02 · 2019 · 5 8 4 1 0 3 1 0 2 2 11 2 · 39
--            02 · 2020 · 2 2 4 6 1 1 4 2 1 2  5 9 · 39
--            02 · 2021 · 3 3 5 1 1 1 2 2 1 1  2 9 · 31
--            02 · 2022 · 2 0 3 1 5 2 2 1 1 3  1 8 · 29
--            02 · 2023 · 3 0 1 1 2 3 1 5 4 1  2 7 · 30
--            02 · 2024 · 0 0 2 1 1 2 1 1 3 2  3 9 · 25
--            34 · 2019 : mars 1, septembre 1 (2) ; 2020 : janvier, mars, juin
--              (3) ; 2021 : février, mars, novembre 2 (4) ; 2022 : avril 2,
--              août, décembre (4) ; 2023 : juin 2, octobre (3) ; 2024 : mars 2,
--              juin, septembre, décembre (5).
-- Constat  : - l'idée « une retraite de décembre compte l'année suivante » ne
--              tient pas : elle donne 32, 31, 30, 31, 23 retraites pour 43,
--              31, 26, 32, 23 publiées (16 d'écart en tout), contre 39, 31,
--              29, 30, 25 avec la date de la base (11 d'écart) ;
--            - règle gardée : la retraite à la date de la décision 02 ; un
--              retraité est présent au 31/12 s'il part après le 31/12 ;
--            - beaucoup de retraites en décembre (7 à 9 par an) : des départs
--              à la fin de l'année de l'âge limite.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
decisions AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt,
           d.CDEC_CODE                AS motif
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    UNION ALL
    SELECT PERS_ID, DATE_DECES, 'DECES' FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
),
-- la première sortie de chaque agent, avec son motif
sorties AS (
    SELECT PERS_ID, dt AS dt_sortie, motif
    FROM (SELECT PERS_ID, dt, motif,
                 ROW_NUMBER() OVER (PARTITION BY PERS_ID ORDER BY dt, motif) AS rn
          FROM decisions WHERE PERS_ID IS NOT NULL)
    WHERE rn = 1
),
nature AS (
    SELECT a.annee, s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY a.annee, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM annees a
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
),
-- les sorties par retraite et liquidation des permanents, l'année de la sortie
sorties_permanents AS (
    SELECT n.annee, so.motif, EXTRACT(MONTH FROM so.dt_sortie) AS mois
    FROM nature n
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = n.PERS_ID
    JOIN sorties so ON so.PERS_ID = n.PERS_ID
    WHERE n.rn = 1
      AND n.NATURE_EMPLOIE = 'PERMANENT'
      AND TO_CHAR(p.QUALITE) = 'A'
      AND so.motif IN ('02', '34')
      AND EXTRACT(YEAR FROM so.dt_sortie) = n.annee
)
SELECT motif, annee,
       SUM(CASE WHEN mois = 1  THEN 1 ELSE 0 END)                 AS janv,
       SUM(CASE WHEN mois = 2  THEN 1 ELSE 0 END)                 AS fevr,
       SUM(CASE WHEN mois = 3  THEN 1 ELSE 0 END)                 AS mars,
       SUM(CASE WHEN mois = 4  THEN 1 ELSE 0 END)                 AS avr,
       SUM(CASE WHEN mois = 5  THEN 1 ELSE 0 END)                 AS mai,
       SUM(CASE WHEN mois = 6  THEN 1 ELSE 0 END)                 AS juin,
       SUM(CASE WHEN mois = 7  THEN 1 ELSE 0 END)                 AS juil,
       SUM(CASE WHEN mois = 8  THEN 1 ELSE 0 END)                 AS aout,
       SUM(CASE WHEN mois = 9  THEN 1 ELSE 0 END)                 AS sept,
       SUM(CASE WHEN mois = 10 THEN 1 ELSE 0 END)                 AS oct,
       SUM(CASE WHEN mois = 11 THEN 1 ELSE 0 END)                 AS nov,
       SUM(CASE WHEN mois = 12 THEN 1 ELSE 0 END)                 AS decembre,
       COUNT(*)                                                   AS total
FROM sorties_permanents
GROUP BY motif, annee
ORDER BY motif, annee;
