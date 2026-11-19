-- =============================================================================
-- LOT 27 — Les suites des contrôles du 17/11/2026, avant la réunion
-- Rédigé le 17/11/2026 — EXÉCUTÉ le 17/11/2026 sur USER_GRH, la base source :
-- aucune table de l'entrepôt (DWH, DTM).
-- Exécution : en mode script (F5) dans SQL Developer. Rien à saisir. Comptages
-- seulement : aucune donnée nominative.
-- Pourquoi ce lot : les contrôles des requêtes proposées et le lot 26,
-- lancés le 17/11, ont fait apparaître six points à creuser avant de voir
-- la CNSS :
--   - 299 temporaires au 31/12/2024 avec la règle actuelle, pour 46 publiés
--     (Q153) ;
--   - 88 des 122 embauchés de 2019 ont pris leur service une autre année :
--     sont-ce les 84 engagés de 2019, et sont-ils revenus en 2024 sous une
--     autre fiche (Q154, Q155) ? L'annuaire CNSS les fait entrer en 2019
--     (84 « autres entrées ») et sortir en 2024 (84 « autres sorties »),
--     l'année de 104 recrutements ;
--   - T60 donne 0 à 1 disponibilité par an, l'annuaire 2 à 7 (Q156) ;
--   - 4 agents sans aucune situation sont comptés chaque année, sans nature
--     (Q157) ;
--   - 5 permanents de 60 ans et plus sont présents au 31/12/2024, aucun dans
--     l'annuaire (Q158).
-- Décisions prises en compte, comme dans la requête de l'effectif : avec une
-- date de vigueur, non annulées par une décision 40 ; l'agent est porté par
-- la décision, ou par sa liste quand elle est collective.
-- Numérotation : suite du lot 26, Q153 à Q158.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q153 — Les temporaires au 31/12, selon la façon de finir un contrat
-- Objectif : compter, à chaque 31/12 de 2020 à 2025, les temporaires de la
--            CNSS encore sous contrat (dernier engagement 50 sans départ
--            depuis), puis ceux dont la fiche n'est pas inactive, et ceux
--            engagés depuis moins de 12, 18 et 24 mois.
-- Tableaux : T58 (tableau des temporaires), T60, T65 ; points 20 et 15 de la
--            v2.
-- Lecture  : la colonne regle_actuelle doit approcher les 299 du contrôle
--            de l'effectif au 31/12/2024. L'annuaire CNSS publie 46
--            contractuels fin 2024. La colonne qui approche 46 dit quelle
--            règle proposer à la DRH tant que les fins de contrat ne sont
--            pas saisies. L'état de la fiche est celui d'aujourd'hui : pour
--            les années passées, la colonne fiche_non_inactive est une borne
--            basse.
-- Résultat (17/11/2026) : année · règle actuelle · fiche non inactive ·
--            engagé depuis 12 mois · 18 mois · 24 mois
--            2020 · 237 · 43 · 20 · 27 · 30
--            2021 · 244 · 44 · 20 · 24 · 29
--            2022 · 260 · 45 · 25 · 34 · 37
--            2023 · 268 · 46 · 27 · 29 · 34
--            2024 · 299 · 47 · 56 · 60 · 61
--            2025 · 338 · 47 · 75 · 87 · 95
-- Constat  : - la règle actuelle retombe exactement sur le contrôle de
--              l'effectif (237 à 338) : ce comptage est le bon ;
--            - en ne gardant que les fiches non inactives : 47 temporaires
--              fin 2024, pour 46 contractuels publiés. C'est la règle à
--              proposer en attendant les fins de contrat (réglage
--              fin_contrat_inconnue = FICHE, déjà prévu dans la requête) ;
--            - une durée de 12, 18 ou 24 mois donne 56 à 61 fin 2024 : moins
--              bon, et ce serait une durée fixée par la BI ;
--            - limite : l'état de la fiche est celui d'aujourd'hui, pas celui
--              de la date d'arrêté.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT TO_DATE(TO_CHAR(2019 + LEVEL) || '1231', 'YYYYMMDD') AS dt
    FROM DUAL
    CONNECT BY LEVEL <= 6
),
decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('50', '02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
-- le dernier engagement temporaire de chaque agent à chaque 31/12
engages AS (
    SELECT a.dt AS date_arrete, e.PERS_ID, MAX(e.dt) AS debut
    FROM annees a
    JOIN decs e ON e.code = '50' AND e.dt <= a.dt
    WHERE e.PERS_ID IS NOT NULL
    GROUP BY a.dt, e.PERS_ID
),
-- encore sous contrat : aucun départ entre cet engagement et le 31/12
sous_contrat AS (
    SELECT g.date_arrete, g.PERS_ID, g.debut
    FROM engages g
    WHERE NOT EXISTS (SELECT 1 FROM decs x
                      WHERE x.PERS_ID = g.PERS_ID
                        AND x.code <> '50'
                        AND x.dt >= g.debut
                        AND x.dt <= g.date_arrete)
),
-- la nature de la dernière situation à chaque 31/12
situ AS (
    SELECT a.dt AS date_arrete, s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY a.dt, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM annees a
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= a.dt
)
SELECT TO_CHAR(c.date_arrete, 'YYYY')                                         AS annee,
       COUNT(*)                                                               AS regle_actuelle,
       SUM(CASE WHEN NVL(p.ETAT_EMPLOYE, '-') <> 'INACTIF' THEN 1 ELSE 0 END)  AS fiche_non_inactive,
       SUM(CASE WHEN c.debut > ADD_MONTHS(c.date_arrete, -12) THEN 1 ELSE 0 END) AS engage_depuis_12_mois,
       SUM(CASE WHEN c.debut > ADD_MONTHS(c.date_arrete, -18) THEN 1 ELSE 0 END) AS engage_depuis_18_mois,
       SUM(CASE WHEN c.debut > ADD_MONTHS(c.date_arrete, -24) THEN 1 ELSE 0 END) AS engage_depuis_24_mois
FROM sous_contrat c
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = c.PERS_ID
JOIN situ si ON si.PERS_ID = c.PERS_ID
            AND si.date_arrete = c.date_arrete
            AND si.rn = 1
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND si.NATURE_EMPLOIE = 'TEMPORAIRE'
GROUP BY TO_CHAR(c.date_arrete, 'YYYY')
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- Q154 — Les embauchés de 2019 : année de prise de service, nature et état
-- Objectif : ranger les agents de la CNSS embauchés en 2019 par année de
--            prise de service, nature de la dernière situation et état de la
--            fiche.
-- Tableaux : T58, T60, T65 (points 2 et 50 de la v2).
-- Lecture  : le lot 26 (Q144) trouve 88 embauchés de 2019 avec une prise de
--            service une autre année. Si ce sont des permanents à la fiche
--            inactive, ce sont les 84 engagés de 2019 ; leur année de prise
--            de service dit quand ils sont vraiment arrivés.
-- Résultat (17/11/2026) : année de prise de service · nature · état · agents
--            2016 · TEMPORAIRE · INACTIF ·  1
--            2018 · PERMANENT  · INACTIF · 84
--            2019 · TEMPORAIRE · INACTIF · 34
--            2020 · TEMPORAIRE · ACTIF   ·  1
--            2023 · TEMPORAIRE · INACTIF ·  1
--            2025 · TEMPORAIRE · INACTIF ·  1
-- Constat  : - les 84 engagés de 2019 sont bien un seul groupe : 84
--              permanents, fiches inactives, en service depuis 2018 et
--              embauchés (engagés) en 2019 ;
--            - l'annuaire CNSS les entre en 2019 comme « autres entrées »,
--              pas comme recrutements : c'est cohérent avec des agents déjà
--              en service en 2018 ;
--            - aucun n'a de fiche active.
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
)
SELECT NVL(TO_CHAR(p.DATE_PRISE_SERVICE, 'YYYY'), '(vide)')   AS annee_prise_service,
       NVL(si.NATURE_EMPLOIE, '(sans situation)')             AS nature,
       NVL(p.ETAT_EMPLOYE, '(sans etat)')                     AS etat,
       COUNT(*)                                               AS agents
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND p.DATE_EMBAUCHE >= DATE '2019-01-01'
  AND p.DATE_EMBAUCHE <  DATE '2020-01-01'
GROUP BY NVL(TO_CHAR(p.DATE_PRISE_SERVICE, 'YYYY'), '(vide)'),
         NVL(si.NATURE_EMPLOIE, '(sans situation)'),
         NVL(p.ETAT_EMPLOYE, '(sans etat)')
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q155 — Les engagés de 2019 sans départ ont-ils une autre fiche plus récente ?
-- Objectif : compter les agents de la CNSS embauchés en 2019, à la fiche
--            inactive, sans décès ni décision de départ ; puis ceux qui ont
--            une autre fiche, embauchée de 2020 à 2025, avec la même date de
--            naissance et le même sexe.
-- Tableaux : T58, T60, T65 (point 2 de la v2).
-- Lecture  : si presque tous ont une autre fiche embauchée en 2024, les 84
--            engagés de 2019 sont parmi les recrutés de 2024 : la CNSS a
--            refait leur fiche, et l'annuaire dit la même chose (84 entrés en
--            2019, 84 sortis en 2024, 104 recrutés en 2024). La question à
--            la DRH change alors du tout au tout. Une même date de naissance
--            peut être un hasard : quelques cas ne prouvent rien.
-- Résultat (17/11/2026) : engagés de 2019 sans départ · autre fiche en 2024 ·
--            en 2025 · de 2020 à 2023 · sans autre fiche
--            96 · 3 · 3 · 1 · 90
-- Constat  : - l'hypothèse tombe : les engagés de 2019 n'ont pas été
--              réenregistrés en 2024 (3 dates de naissance communes sur 96,
--              le niveau du hasard) ;
--            - ils sont donc partis sans aucun départ saisi : la question du
--              point 2 reste entière (que sont-ils devenus, et quand).
-- -----------------------------------------------------------------------------
WITH partis AS (
    SELECT DISTINCT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
),
anciens AS (
    SELECT p.PERS_ID, TRUNC(p.DATE_NAISS) AS naissance, p.SEXE
    FROM USER_GRH.GRH_PERSONNE p
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND p.DATE_EMBAUCHE >= DATE '2019-01-01'
      AND p.DATE_EMBAUCHE <  DATE '2020-01-01'
      AND p.ETAT_EMPLOYE = 'INACTIF'
      AND p.DATE_DECES IS NULL
      AND p.DATE_NAISS IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM partis x WHERE x.PERS_ID = p.PERS_ID)
),
recents AS (
    SELECT p.PERS_ID, TRUNC(p.DATE_NAISS) AS naissance, p.SEXE,
           EXTRACT(YEAR FROM p.DATE_EMBAUCHE) AS annee
    FROM USER_GRH.GRH_PERSONNE p
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND p.DATE_EMBAUCHE >= DATE '2020-01-01'
),
doubles AS (
    SELECT a.PERS_ID,
           MAX(CASE WHEN r.annee = 2024 THEN 1 ELSE 0 END)              AS en_2024,
           MAX(CASE WHEN r.annee = 2025 THEN 1 ELSE 0 END)              AS en_2025,
           MAX(CASE WHEN r.annee BETWEEN 2020 AND 2023 THEN 1 ELSE 0 END) AS de_2020_a_2023
    FROM anciens a
    JOIN recents r ON r.naissance = a.naissance
                  AND r.SEXE = a.SEXE
                  AND r.PERS_ID <> a.PERS_ID
    GROUP BY a.PERS_ID
)
SELECT COUNT(*)                                AS engages_2019_sans_depart,
       SUM(NVL(d.en_2024, 0))                  AS autre_fiche_en_2024,
       SUM(NVL(d.en_2025, 0))                  AS autre_fiche_en_2025,
       SUM(NVL(d.de_2020_a_2023, 0))           AS autre_fiche_2020_a_2023,
       SUM(CASE WHEN d.PERS_ID IS NULL THEN 1 ELSE 0 END) AS sans_autre_fiche
FROM anciens a
LEFT JOIN doubles d ON d.PERS_ID = a.PERS_ID;


-- -----------------------------------------------------------------------------
-- Q156 — Les mises en disponibilité et les remises en activité, par année
-- Objectif : par année depuis 2019, compter les décisions 07 et 18 des agents
--            de la CNSS ; parmi les 07, celles qui suivent déjà une 07
--            (prolongation) et celles suivies d'une 18 la même année ; parmi
--            les 18, celles qui ne suivent pas une 07.
-- Tableaux : T60 (sorties DISP et entrées REM ; points 49 et 51 de la v2).
-- Lecture  : T60 lit les mouvements entre deux 31/12 : 1, 1, 0, 1, 0
--            disponibilités et 2, 1, 2, 3, 0 remises de 2020 à 2024.
--            L'annuaire CNSS publie 5, 3, 2, 4, 7 disponibilités et 3, 2, 3,
--            5, 6 remises. Une prolongation, ou une disponibilité finie dans
--            l'année, est une décision mais pas un mouvement entre deux
--            31/12 : ces colonnes disent si l'écart vient de là.
-- Résultat (17/11/2026) : année · mises en disponibilité · dont
--            prolongations · dont finies dans l'année · remises en activité ·
--            dont sans disponibilité avant
--            2019 · 7 · 0 · 2 · 6 · 2
--            2020 · 3 · 1 · 0 · 4 · 1
--            2021 · 4 · 0 · 0 · 3 · 2
--            2022 · 2 · 0 · 0 · 6 · 3
--            2023 · 5 · 1 · 0 · 5 · 1
--            2024 · 6 · 2 · 1 · 4 · 3
--            2025 · 3 · 1 · 0 · 6 · 3
-- Constat  : - les décisions sont là, au niveau de l'annuaire : 3, 4, 2, 5 et
--              6 mises en disponibilité de 2020 à 2024 (publié 5, 3, 2, 4,
--              7) ; 4, 3, 6, 5 et 4 remises (publié 3, 2, 3, 5, 6) ;
--            - les prolongations et les disponibilités finies dans l'année
--              sont rares : elles n'expliquent pas que T60 ne montre que 0 à
--              1 disponibilité par an ;
--            - une remise en activité sur trois ne suit pas une
--              disponibilité (retour d'une mise à disposition, lot 26, Q145) ;
--            - piste : l'agent parti en disponibilité et jamais revenu a une
--              fiche inactive sans départ daté ; la règle ne le compte alors
--              à aucune date, même avant sa disponibilité (lot 28, Q159).
-- -----------------------------------------------------------------------------
WITH decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('07', '18')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
suite AS (
    SELECT PERS_ID, code, dt,
           LAG(code)  OVER (PARTITION BY PERS_ID ORDER BY dt, code) AS code_avant,
           LEAD(code) OVER (PARTITION BY PERS_ID ORDER BY dt, code) AS code_apres,
           LEAD(dt)   OVER (PARTITION BY PERS_ID ORDER BY dt, code) AS dt_apres
    FROM decs
    WHERE PERS_ID IS NOT NULL
)
SELECT EXTRACT(YEAR FROM su.dt)                                               AS annee,
       SUM(CASE WHEN su.code = '07' THEN 1 ELSE 0 END)                        AS mises_en_disponibilite,
       SUM(CASE WHEN su.code = '07' AND su.code_avant = '07' THEN 1 ELSE 0 END) AS dont_prolongations,
       SUM(CASE WHEN su.code = '07' AND su.code_apres = '18'
                 AND EXTRACT(YEAR FROM su.dt_apres) = EXTRACT(YEAR FROM su.dt)
                THEN 1 ELSE 0 END)                                            AS dont_finies_dans_l_annee,
       SUM(CASE WHEN su.code = '18' THEN 1 ELSE 0 END)                        AS remises_en_activite,
       SUM(CASE WHEN su.code = '18' AND NVL(su.code_avant, '-') <> '07'
                THEN 1 ELSE 0 END)                                            AS dont_sans_dispo_avant
FROM suite su
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = su.PERS_ID
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND su.dt >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM su.dt)
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- Q157 — Les agents sans aucune situation
-- Objectif : compter les fiches sans aucune ligne dans GRH_SITUATION, par
--            qualité, état de la fiche et période d'entrée.
-- Tableaux : T58, T59, T61, T65 (règle G1 : un agent sans situation suit la
--            règle des permanents).
-- Lecture  : le contrôle de l'effectif compte 4 agents de la CNSS sans
--            nature ni catégorie à chaque 31/12 de 2019 à 2025 : ils ne sont
--            ni dans le tableau des permanents ni dans celui des
--            temporaires. Les lignes de qualité A dont l'état n'est pas
--            INACTIF sont ces agents.
-- Résultat (17/11/2026) : qualité · état · entrée · fiches
--            A · INACTIF     · avant 2015  · 111
--            A · (sans état) · avant 2015  ·   1
--            A · (sans état) · 2015 à 2019 ·   2
--            B · INACTIF     · avant 2015  ·   7
-- Constat  : - 3 fiches de la CNSS sans état et sans situation, anciennes,
--              sont comptées chaque année par la règle « une fiche sans état
--              est encore là » : ce ne sont pas des embauches récentes ;
--            - le quatrième agent sans nature du contrôle a une situation,
--              mais sans nature d'emploi.
-- -----------------------------------------------------------------------------
SELECT NVL(TO_CHAR(p.QUALITE), '(vide)')                                  AS qualite,
       NVL(p.ETAT_EMPLOYE, '(sans etat)')                                 AS etat,
       CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) IS NULL
                 THEN 'd. sans date'
            WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2015-01-01'
                 THEN 'a. avant 2015'
            WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2020-01-01'
                 THEN 'b. 2015 a 2019'
            ELSE 'c. 2020 et apres' END                                   AS entree,
       COUNT(*)                                                           AS fiches
FROM USER_GRH.GRH_PERSONNE p
WHERE NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_SITUATION s WHERE s.PERS_ID = p.PERS_ID)
GROUP BY NVL(TO_CHAR(p.QUALITE), '(vide)'),
         NVL(p.ETAT_EMPLOYE, '(sans etat)'),
         CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) IS NULL
                   THEN 'd. sans date'
              WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2015-01-01'
                   THEN 'a. avant 2015'
              WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2020-01-01'
                   THEN 'b. 2015 a 2019'
              ELSE 'c. 2020 et apres' END
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q158 — Les permanents de 60 ans et plus présents au 31/12/2024
-- Objectif : compter les permanents de la CNSS présents au 31/12/2024 (entrés
--            au plus tard ce jour-là, fiche non inactive ou premier départ
--            après) et âgés de 60 ans ou plus, par âge, état de la fiche et
--            retraite saisie.
-- Tableaux : T58, T61 (points 22 et 25 de la v2).
-- Lecture  : le contrôle de l'effectif trouve 5 permanents de 60 ans et plus
--            et 6 de 40 ans d'ancienneté et plus ; l'annuaire CNSS 2024
--            (tableaux 120 et 122) en publie 0 et 1. Sans retraite saisie, ou
--            avec une retraite datée après le 31/12/2024, ces agents sont
--            sans doute déjà partis.
-- Résultat (17/11/2026) : âge · état · retraite · agents
--            69 · (sans état) · aucune retraite saisie · 1
--            73 · (sans état) · aucune retraite saisie · 1
--            76 · (sans état) · aucune retraite saisie · 1
--            78 · (sans état) · aucune retraite saisie · 1
--            80 · (sans état) · aucune retraite saisie · 1
-- Constat  : - les 5 permanents de 60 ans et plus sont 5 fiches sans état,
--              de 69 à 80 ans, sans retraite saisie : des fiches anciennes
--              jamais fermées, pas des agents en poste ;
--            - c'est la règle « une fiche sans état est encore là » (05/11)
--              qui les compte. Sans elles : 736 permanents fin 2024, aucun
--              de 60 ans et plus (annuaire 0), et 1 de 40 ans d'ancienneté
--              et plus (annuaire 1) ;
--            - à corriger dans la requête de l'effectif : une fiche sans état
--              ne compte que si l'entrée est récente (point 45).
-- -----------------------------------------------------------------------------
WITH decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
departs AS (
    SELECT PERS_ID, MIN(dt) AS depart,
           MIN(CASE WHEN code = '02' THEN dt END) AS retraite
    FROM decs
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
)
SELECT FLOOR(MONTHS_BETWEEN(DATE '2024-12-31', p.DATE_NAISS) / 12)         AS age,
       NVL(p.ETAT_EMPLOYE, '(sans etat)')                                  AS etat,
       CASE WHEN dp.retraite IS NULL THEN 'aucune retraite saisie'
            ELSE 'retraite datee de ' || TO_CHAR(dp.retraite, 'YYYY') END  AS retraite,
       COUNT(*)                                                            AS agents
FROM USER_GRH.GRH_PERSONNE p
JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
LEFT JOIN departs dp ON dp.PERS_ID = p.PERS_ID
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND si.NATURE_EMPLOIE = 'PERMANENT'
  AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
  AND (NVL(p.ETAT_EMPLOYE, '-') <> 'INACTIF'
       OR COALESCE(dp.depart, TRUNC(p.DATE_DECES)) > DATE '2024-12-31')
  AND FLOOR(MONTHS_BETWEEN(DATE '2024-12-31', p.DATE_NAISS) / 12) >= 60
GROUP BY FLOOR(MONTHS_BETWEEN(DATE '2024-12-31', p.DATE_NAISS) / 12),
         NVL(p.ETAT_EMPLOYE, '(sans etat)'),
         CASE WHEN dp.retraite IS NULL THEN 'aucune retraite saisie'
              ELSE 'retraite datee de ' || TO_CHAR(dp.retraite, 'YYYY') END
ORDER BY 1, 2, 3;
