-- =============================================================================
-- LOT 17 — Le personnel au 31/12/2024 face à l'annuaire de la CNSS
-- Rédigé le 01/11/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement,
-- aucune donnée nominative (sexe, dates de naissance, d'embauche et de décès ne
-- servent qu'à compter).
-- Pourquoi ce lot : l'annuaire de la CNSS 2024 (partie IX, p. 89 à 92) publie
-- le personnel permanent au 31/12/2024 sous la forme des tableaux CIPRES T58,
-- T60, T61, T62 et T65. Reconstituer ces tableaux depuis la base dit si les
-- règles des requêtes DTM corrigées (grh_effectif, grh_ratio, grh_formation)
-- sont celles de la CNSS, et lesquelles revoir. C'est la même démarche que pour
-- les pensions (analyses/prestations, lot 5).
-- Règles reprises des requêtes DTM corrigées :
--   · présent au 31/12 : entré au plus tard le 31/12 (embauche, à défaut prise
--     de service), et encore ACTIF ou sorti après le 31/12 ;
--   · date de sortie : première décision 02 (retraite), 34 (liquidation des
--     droits), 35 (indemnité de fin de contrat), 36 (démission), 52 (rupture
--     de contrat) ou 54 (licenciement) non annulée par une décision 40, à
--     défaut la date de décès ;
--   · l'agent est décrit par sa dernière situation commencée au 31/12 ;
--   · catégorie : le niveau de la qualification (GRH_QUALIFICATION.NIVEAU) :
--     A employés, I agents de maîtrise, M cadres moyens, S cadres supérieurs
--     (lot 6 Q36, lot 8 Q46) ;
--   · type d'agent : qualité de la fiche (A agent CNSS, B fonction publique, C
--     affecté) et nature d'emploi de la situation (PERMANENT, TEMPORAIRE) ;
--   · inactif : statut B, D, E, F ou H (détachement, mise à disposition,
--     disponibilité, suspension, service militaire).
-- Chiffres publiés pour comparer (annuaire de la CNSS 2024, au 31/12/2024) :
--   tableau 119 : permanents 758 (employés 177, agents de maîtrise 208, cadres
--   moyens 311, cadres supérieurs 62) ; contractuels 46 (18, 13, 10, 5) ; 804.
--   tableau 120 : permanents par sexe : employés 133 H, 44 F ; agents de
--   maîtrise 94 H, 114 F ; cadres moyens 143 H, 168 F ; cadres supérieurs 42 H,
--   20 F. Par âge : moins de 20 ans 0 ; 20-24 12 ; 25-29 28 ; 30-34 70 ;
--   35-39 117 ; 40-44 141 ; 45-49 157 ; 50-54 154 ; 55-59 79 ; 60 et plus 0.
--   tableau 121 (permanents) : début / recrutements / sorties (retraite, décès,
--   licenciement, démission, disponibilité, fin de détachement, autres) / fin :
--   2020 918 / 0 / 43, 4, 0, 0, 5, 0, 0 / 869 (+3 remises en activité) ;
--   2021 869 / 0 / 31, 2, 0, 0, 3, 0, 1 / 834 (+2) ;
--   2022 834 / 0 / 26, 3, 1, 0, 2, 0, 1 / 806 (+3 remises, +2 autres) ;
--   2023 806 / 0 / 32, 5, 0, 1, 4, 2, 2 / 767 (+5 remises, +2 détachements) ;
--   2024 767 / 104 / 23, 3, 1, 2, 7, 0, 84 / 758 (+6 remises, +1 détachement).
--   tableau 122 : ancienneté, hommes / femmes : moins d'un an 77 / 26 ; 1-4
--   1 / 0 ; 5-9 54 / 68 ; 10-14 121 / 111 ; 15-19 50 / 52 ; 20-24 57 / 63 ;
--   25-29 22 / 16 ; 30-34 22 / 7 ; 35-39 7 / 3 ; 40 et plus 1 / 0 ; total
--   412 / 346.
--   tableau 123 : permanents formés en 2024 : employés 132, agents de maîtrise
--   142, cadres moyens 118, cadres supérieurs 88 ; 480, dont 6 avec promotion.
--   tableau 124 : cadres / non-cadres / total : 2020 358 / 511 / 869 ; 2021
--   345 / 489 / 834 ; 2022 333 / 473 / 806 ; 2023 316 / 451 / 767 ; 2024
--   373 / 385 / 758 (49 %).
-- Numérotation : suite du lot 16, Q97 à Q102.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q97 — Le personnel au 31/12/2024 par type d'agent et catégorie (tableau 119)
-- Objectif : agents présents au 31/12/2024, par type d'agent et niveau de
--            qualification, et parmi eux les actifs (hors détachement,
--            disponibilité…).
-- Tableaux : T58 (périmètre de l'effectif), T65.
-- Lecture  : la ligne « Agent CNSS permanent » doit retomber sur les 758
--            permanents (177, 208, 311, 62) ; les temporaires sur les 46
--            contractuels. Si la CNSS compte les seuls actifs, c'est la colonne
--            « dont actifs » qui retombe.
-- Résultat (01/11/2026) : type · employés · maîtrise · cadres moyens · cadres
--            supérieurs · sans niveau · présents · dont actifs
--            1. Agent CNSS permanent         204 · 216 · 209 · 113 · 0 · 742 · 740
--            2. Agent CNSS temporaire          2 ·   3 ·   0 ·   2 · 0 ·   7 ·   7
--            3. Agent de la fonction publique  1 ·   0 ·   0 ·   2 · 0 ·   3 ·   1
--            5. Type non renseigné             0 ·   0 ·   0 ·   0 · 1 ·   1 ·   1
-- Constat  : - 742 permanents pour 758 publiés (16 de moins, 2 %) : le
--              périmètre est le bon (agent CNSS de nature PERMANENT). Les
--              seuls actifs (740) ne s'en approchent pas mieux ;
--            - la répartition par catégorie ne retombe pas : employés 204
--              (177 publiés), agents de maîtrise 216 (208), cadres moyens 209
--              (311), cadres supérieurs 113 (62). La CNSS ne classe donc pas
--              ses agents par le niveau de la qualification : le lot 18 (Q103,
--              Q104) cherche le champ qu'elle utilise ;
--            - 7 temporaires pour 46 contractuels publiés : les contractuels de
--              la CNSS ne sont pas (ou pas seulement) les temporaires actifs
--              (lot 18, Q106).
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
presents AS (
    SELECT p.PERS_ID, TO_CHAR(p.QUALITE) AS qualite
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    WHERE COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31')
),
situ AS (
    SELECT s.PERS_ID, s.QUAL_CODE, s.NATURE_EMPLOIE, s.STATUT_EMPLOYE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
base AS (
    SELECT pr.PERS_ID,
           CASE WHEN pr.qualite = 'A' AND si.NATURE_EMPLOIE = 'PERMANENT'  THEN '1. Agent CNSS permanent'
                WHEN pr.qualite = 'A' AND si.NATURE_EMPLOIE = 'TEMPORAIRE' THEN '2. Agent CNSS temporaire'
                WHEN pr.qualite = 'B' THEN '3. Agent de la fonction publique'
                WHEN pr.qualite = 'C' THEN '4. Agent affecté'
                ELSE '5. Type non renseigné' END                  AS type_agent,
           NVL(q.NIVEAU, '-')                                     AS niveau,
           CASE WHEN si.STATUT_EMPLOYE IN ('B', 'D', 'E', 'F', 'H')
                THEN 0 ELSE 1 END                                 AS actif
    FROM presents pr
    LEFT JOIN situ si ON si.PERS_ID = pr.PERS_ID AND si.rn = 1
    LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = si.QUAL_CODE
)
SELECT type_agent,
       SUM(CASE WHEN niveau = 'A' THEN 1 ELSE 0 END)              AS employes,
       SUM(CASE WHEN niveau = 'I' THEN 1 ELSE 0 END)              AS agents_maitrise,
       SUM(CASE WHEN niveau = 'M' THEN 1 ELSE 0 END)              AS cadres_moyens,
       SUM(CASE WHEN niveau = 'S' THEN 1 ELSE 0 END)              AS cadres_superieurs,
       SUM(CASE WHEN niveau NOT IN ('A', 'I', 'M', 'S') THEN 1 ELSE 0 END) AS sans_niveau,
       COUNT(*)                                                   AS presents,
       SUM(actif)                                                 AS dont_actifs
FROM base
GROUP BY type_agent
ORDER BY type_agent;


-- -----------------------------------------------------------------------------
-- Q98 — Les permanents au 31/12/2024 par catégorie, âge et sexe (tableau 120)
-- Objectif : agents CNSS permanents présents au 31/12/2024, par tranche d'âge
--            de l'Annuaire (âge en années pleines au 31/12) et par catégorie et
--            sexe.
-- Tableaux : T58.
-- Lecture  : comparer case par case au tableau 120 (en-tête du lot). Un
--            décalage d'une tranche voudrait dire que la CNSS calcule l'âge
--            autrement (année moins année de naissance, comme le glossaire de
--            l'Annuaire).
-- Résultat (01/11/2026) : tranche · employés H / F · maîtrise H / F · cadres
--            moyens H / F · cadres supérieurs H / F · sans niveau · total
--            20-24 ·  2 /  0 ·  2 /  2 ·  2 /  2 ·  2 /  0 · 0 ·  12
--            25-29 · 10 /  0 ·  7 /  2 ·  1 /  3 ·  5 /  0 · 0 ·  28
--            30-34 · 17 /  3 ·  7 / 16 ·  7 / 11 ·  7 /  2 · 0 ·  70
--            35-39 · 22 /  6 · 23 / 24 · 16 /  9 · 11 /  4 · 0 · 115
--            40-44 · 20 / 19 ·  8 / 26 · 19 / 16 · 14 / 17 · 0 · 139
--            45-49 · 33 / 13 · 21 / 23 · 19 / 27 · 13 /  6 · 0 · 155
--            50-54 · 33 / 16 · 13 / 29 · 18 / 24 · 10 /  8 · 0 · 151
--            55-59 ·  8 /  2 ·  4 /  9 · 17 / 18 ·  9 /  5 · 0 ·  72
--            Aucun agent de moins de 20 ans ni de 60 ans et plus, aucun âge
--            inconnu. Totaux : employés 145 H / 59 F, maîtrise 85 / 131, cadres
--            moyens 99 / 110, cadres supérieurs 71 / 42 ; 400 hommes, 342
--            femmes.
-- Constat  : - par tranche d'âge, les chiffres publiés sont retrouvés jusqu'à
--              34 ans (12, 28, 70), puis à 2 près jusqu'à 54 ans (115 pour
--              117, 139 pour 141, 155 pour 157, 151 pour 154) ; 72 pour 79 à
--              55-59 ans. L'âge en années pleines au 31/12 est donc bien la
--              règle de la CNSS (aucun glissement d'une tranche à l'autre) ;
--            - les 16 permanents manquants ont tous 35 ans ou plus, dont 7 de
--              55 à 59 ans : des départs proches de la retraite, que la base
--              date en 2024 ou ne date pas (lot 18, Q106) ;
--            - par sexe : 400 hommes (412 publiés) et 342 femmes (346) ;
--            - par catégorie, l'écart de Q97 se retrouve dans chaque tranche :
--              employés 145 H / 59 F (133 / 44 publiés), maîtrise 85 / 131
--              (94 / 114), cadres moyens 99 / 110 (143 / 168), cadres
--              supérieurs 71 / 42 (42 / 20).
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
    SELECT s.PERS_ID, s.QUAL_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
permanents AS (
    SELECT p.SEXE,
           NVL(q.NIVEAU, '-')                                     AS niveau,
           FLOOR((DATE '2024-12-31' - p.DATE_NAISS) / 365.25)     AS age
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = si.QUAL_CODE
    WHERE COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31')
      AND TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
),
tranches AS (
    SELECT SEXE, niveau,
           CASE WHEN age IS NULL THEN 99
                WHEN age < 20    THEN 0
                WHEN age >= 60   THEN 60
                ELSE FLOOR(age / 5) * 5 END                       AS rang
    FROM permanents
)
SELECT CASE rang WHEN 0 THEN 'Moins de 20 ans' WHEN 60 THEN '60 ans et plus'
                 WHEN 99 THEN 'Âge inconnu'
                 ELSE rang || ' - ' || (rang + 4) || ' ans' END   AS tranche_age,
       SUM(CASE WHEN niveau = 'A' AND SEXE = 'M' THEN 1 ELSE 0 END) AS employes_h,
       SUM(CASE WHEN niveau = 'A' AND SEXE = 'F' THEN 1 ELSE 0 END) AS employes_f,
       SUM(CASE WHEN niveau = 'I' AND SEXE = 'M' THEN 1 ELSE 0 END) AS maitrise_h,
       SUM(CASE WHEN niveau = 'I' AND SEXE = 'F' THEN 1 ELSE 0 END) AS maitrise_f,
       SUM(CASE WHEN niveau = 'M' AND SEXE = 'M' THEN 1 ELSE 0 END) AS cadres_moyens_h,
       SUM(CASE WHEN niveau = 'M' AND SEXE = 'F' THEN 1 ELSE 0 END) AS cadres_moyens_f,
       SUM(CASE WHEN niveau = 'S' AND SEXE = 'M' THEN 1 ELSE 0 END) AS cadres_sup_h,
       SUM(CASE WHEN niveau = 'S' AND SEXE = 'F' THEN 1 ELSE 0 END) AS cadres_sup_f,
       SUM(CASE WHEN niveau NOT IN ('A', 'I', 'M', 'S') THEN 1 ELSE 0 END) AS sans_niveau,
       COUNT(*)                                                   AS total
FROM tranches
GROUP BY rang
ORDER BY rang;


-- -----------------------------------------------------------------------------
-- Q99 — Les mouvements des permanents, de 2020 à 2024 (tableau 121)
-- Objectif : pour chaque année de 2019 à 2024, permanents présents au début
--            (31/12 de l'année d'avant) ; recrutés dans l'année (date
--            d'embauche, à défaut de prise de service) ; sortis dans l'année,
--            par motif de la première sortie (retraite 02, décès, démission
--            36, licenciement 54, autres : 34, 35, 52) ; présents à la fin ;
--            et l'écart début + recrutements − sorties − fin, qui mesure les
--            changements de type (temporaire devenu permanent…) et les
--            départs sans date.
-- Tableaux : T60.
-- Lecture  : comparer au tableau 121 (en-tête du lot) : 2024, 767 au début,
--            104 recrutements, 23 retraites, 3 décès, 758 à la fin. Les 84
--            « autres » entrées de 2019 et les 84 « autres » sorties de 2024
--            de l'annuaire sont à expliquer (Q102 et lot 18, Q105).
-- Résultat : premier essai (01/11/2026) en échec : ORA-00904 "A"."ANNEE",
--            ligne 278. Cette version d'Oracle refuse une colonne de la
--            requête principale dans le ON d'une jointure placée dans une
--            sous-requête. Réécrite avec des comptages par année, joints
--            ensuite aux années.
--            Relancée le 01/11/2026 : année · début · recrutements · retraites
--            (02) · décès (date de décès) · démissions (36) · licenciements
--            (54) · autres sorties (34, 35, 52) · fin · écart
--            (publié : début · recrutements · retraites · décès · démissions ·
--            licenciements · disponibilités · autres · fin)
--            2019 · 862 · 84 · 39 · 0 · 0 · 3 · 2 · 818 · 84
--            2020 · 818 ·  1 · 39 · 0 · 0 · 0 · 3 · 777 ·  0
--                 (918 · 0 · 43 · 4 · 0 · 0 · 5 · 0 · 869, +3 remises en activité)
--            2021 · 777 ·  0 · 31 · 0 · 0 · 1 · 4 · 741 ·  0
--                 (869 · 0 · 31 · 2 · 0 · 0 · 3 · 1 · 834, +2 remises)
--            2022 · 741 ·  1 · 29 · 0 · 0 · 2 · 4 · 707 ·  0
--                 (834 · 0 · 26 · 3 · 0 · 1 · 2 · 1 · 806, +3 remises, +2 autres)
--            2023 · 707 ·  0 · 30 · 0 · 1 · 0 · 4 · 672 ·  0
--                 (806 · 0 · 32 · 5 · 1 · 0 · 4 · 2 + 2 fins de détachement · 767,
--                 +5 remises, +2 détachements)
--            2024 · 672 · 104 · 25 · 0 · 2 · 1 · 5 · 742 ·  1
--                 (767 · 104 · 23 · 3 · 2 · 1 · 7 · 84 · 758, +6 remises,
--                 +1 détachement)
-- Constat  : - les 84 engagés de 2019 sont retrouvés : la base les compte en
--              recrutements en 2019, mais jamais présents en fin d'année (écart
--              de 84), car leur fiche est inactive sans sortie datée.
--              L'annuaire les compte en « autres entrées » en 2019, présents
--              de 2019 à 2023, puis en « autres sorties » en 2024. Sans eux,
--              l'écart avec l'annuaire tombe à 8 à 16 agents par an (918 − 84 −
--              818 = 16 en 2019 ; 869 − 84 − 777 = 8 en 2020 ; 9, 15, 11, puis
--              16 en 2024) ;
--            - les recrutements de 2024 sont exactement ceux de l'annuaire
--              (104), et aucun de 2020 à 2023 (1 en 2020 et en 2022 dans la
--              base) : la règle (embauche dans l'année, permanent au 31/12)
--              est la bonne ;
--            - retraites (décision 02) à 4 près chaque année (39 pour 43, 31
--              pour 31, 29 pour 26, 30 pour 32, 25 pour 23) ; démissions (36)
--              exactes (1 en 2023, 2 en 2024) ; licenciements (54) à 1 près ;
--            - aucun décès daté depuis 2010 dans la base (lot 7, Q42) : les
--              décès se lisent par la liquidation des droits (34), environ 8
--              mois plus tard, comptée ici en « autres sorties » (3, 4, 4, 4, 5
--              pour 4, 2, 3, 5, 3 décès publiés). Le lot 18 (Q109) sépare les
--              codes 34, 35 et 52 ;
--            - l'annuaire compte aussi les disponibilités en sorties (5, 3, 2,
--              4, 7) et les remises en activité en entrées (3, 2, 3, 5, 6) : la
--              base les garde présents. Le lot 18 (Q109) les compte ;
--            - de 2020 à 2023, l'écart est nul : chaque mouvement de la base
--              est daté. En 2024, un agent manque en fin d'année sans sortie
--              datée (départ sans date ou passage hors des permanents).
-- -----------------------------------------------------------------------------
WITH annees AS (
    -- 2018 sert de point de départ : l'effectif au début de 2019
    SELECT 2017 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 7
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
-- les agents CNSS (qualité A), avec leur entrée et leur première sortie
agents AS (
    SELECT p.PERS_ID, p.ETAT_EMPLOYE,
           COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) AS date_entree,
           so.dt_sortie, so.motif
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
),
-- la nature d'emploi de chaque agent au 31/12 d'une année : celle de sa
-- dernière situation commencée à cette date
nature AS (
    SELECT a.annee, s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY a.annee, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM annees a
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
),
-- une ligne par année et par agent CNSS de nature PERMANENT au 31/12
permanent AS (
    SELECT n.annee, TO_DATE(n.annee || '1231', 'YYYYMMDD') AS fin_annee,
           ag.ETAT_EMPLOYE, ag.date_entree, ag.dt_sortie, ag.motif
    FROM nature n
    JOIN agents ag ON ag.PERS_ID = n.PERS_ID
    WHERE n.rn = 1
      AND n.NATURE_EMPLOIE = 'PERMANENT'
),
-- les permanents présents au 31/12 de chaque année
presents AS (
    SELECT annee, COUNT(*) AS nb
    FROM permanent
    WHERE date_entree <= fin_annee
      AND (ETAT_EMPLOYE = 'ACTIF' OR dt_sortie > fin_annee)
    GROUP BY annee
),
-- les entrées et les sorties de chaque année
mouvements AS (
    SELECT annee,
           SUM(CASE WHEN EXTRACT(YEAR FROM date_entree) = annee THEN 1 ELSE 0 END) AS recrutements,
           SUM(CASE WHEN EXTRACT(YEAR FROM dt_sortie) = annee AND motif = '02'
                    THEN 1 ELSE 0 END)                            AS retraites,
           SUM(CASE WHEN EXTRACT(YEAR FROM dt_sortie) = annee AND motif = 'DECES'
                    THEN 1 ELSE 0 END)                            AS deces,
           SUM(CASE WHEN EXTRACT(YEAR FROM dt_sortie) = annee AND motif = '36'
                    THEN 1 ELSE 0 END)                            AS demissions,
           SUM(CASE WHEN EXTRACT(YEAR FROM dt_sortie) = annee AND motif = '54'
                    THEN 1 ELSE 0 END)                            AS licenciements,
           SUM(CASE WHEN EXTRACT(YEAR FROM dt_sortie) = annee
                     AND motif IN ('34', '35', '52')
                    THEN 1 ELSE 0 END)                            AS autres_sorties
    FROM permanent
    GROUP BY annee
)
SELECT a.annee,
       NVL(d.nb, 0)                                               AS debut,
       NVL(m.recrutements, 0)                                     AS recrutements,
       NVL(m.retraites, 0)                                        AS retraites,
       NVL(m.deces, 0)                                            AS deces,
       NVL(m.demissions, 0)                                       AS demissions,
       NVL(m.licenciements, 0)                                    AS licenciements,
       NVL(m.autres_sorties, 0)                                   AS autres_sorties,
       NVL(f.nb, 0)                                               AS fin,
       NVL(d.nb, 0) + NVL(m.recrutements, 0)
         - NVL(m.retraites, 0) - NVL(m.deces, 0) - NVL(m.demissions, 0)
         - NVL(m.licenciements, 0) - NVL(m.autres_sorties, 0)
         - NVL(f.nb, 0)                                           AS ecart
FROM annees a
LEFT JOIN presents   d ON d.annee = a.annee - 1
LEFT JOIN mouvements m ON m.annee = a.annee
LEFT JOIN presents   f ON f.annee = a.annee
WHERE a.annee >= 2019
ORDER BY a.annee;


-- -----------------------------------------------------------------------------
-- Q100 — Les permanents au 31/12/2024 par ancienneté et sexe (tableau 122)
-- Objectif : agents CNSS permanents présents au 31/12/2024, par tranche
--            d'ancienneté de l'Annuaire (années pleines depuis l'embauche, à
--            défaut la prise de service) et par sexe.
-- Tableaux : T61.
-- Lecture  : comparer au tableau 122 (en-tête du lot) ; « moins d'un an »
--            (103) doit retomber sur les 104 recrutés de 2024.
-- Résultat (01/11/2026) : ancienneté · hommes · femmes · total (publié)
--            moins de 1 an  74 ·  27 · 101 (77 / 26 · 103)
--            1-4 ans         0 ·   1 ·   1 ( 1 /  0 ·   1)
--            5-9 ans        53 ·  67 · 120 (54 / 68 · 122)
--            10-14 ans     118 · 106 · 224 (121 / 111 · 232)
--            15-19 ans      50 ·  56 · 106 (50 / 52 · 102)
--            20-24 ans      55 ·  61 · 116 (57 / 63 · 120)
--            25-29 ans      21 ·  14 ·  35 (22 / 16 ·  38)
--            30-34 ans      20 ·   7 ·  27 (22 /  7 ·  29)
--            35-39 ans       8 ·   3 ·  11 ( 7 /  3 ·  10)
--            40 ans et plus  1 ·   0 ·   1 ( 1 /  0 ·   1)
--            Total 400 · 342 · 742 (412 / 346 · 758).
-- Constat  : - chaque tranche est à 8 près du chiffre publié, la plupart à 2
--              près : l'ancienneté en années pleines depuis l'embauche (à
--              défaut la prise de service) est la règle de la CNSS ;
--            - moins d'un an : 101 pour 103 publiés et 104 recrutés en 2024 ;
--            - un seul agent de 1 à 4 ans d'ancienneté, comme dans l'annuaire :
--              aucun recrutement permanent de 2020 à 2023 ;
--            - le plus gros écart (10-14 ans, 8 de moins) porte sur des agents
--              embauchés de 2010 à 2014.
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
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
permanents AS (
    SELECT p.SEXE,
           FLOOR((DATE '2024-12-31' - COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE))
                 / 365.25)                                        AS anciennete
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    WHERE COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31')
      AND TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
),
tranches AS (
    SELECT SEXE,
           CASE WHEN anciennete < 1  THEN 0
                WHEN anciennete < 5  THEN 1
                WHEN anciennete >= 40 THEN 40
                ELSE FLOOR(anciennete / 5) * 5 END                AS rang
    FROM permanents
)
SELECT CASE rang WHEN 0 THEN 'Moins de 1 an' WHEN 1 THEN '1 - 4 ans'
                 WHEN 40 THEN '40 ans et plus'
                 ELSE rang || ' - ' || (rang + 4) || ' ans' END   AS anciennete,
       SUM(CASE WHEN SEXE = 'M' THEN 1 ELSE 0 END)                AS hommes,
       SUM(CASE WHEN SEXE = 'F' THEN 1 ELSE 0 END)                AS femmes,
       COUNT(*)                                                   AS total
FROM tranches
GROUP BY rang
ORDER BY rang;


-- -----------------------------------------------------------------------------
-- Q101 — Les agents formés en 2024 (tableau 123)
-- Objectif : agents distincts inscrits à une formation, par niveau (situation
--            au 31/12/2024), selon trois dates : formation finie en 2024 (règle
--            de grh_formation.sql), commencée en 2024, ou en cours à un moment
--            de 2024 ; les mêmes parmi les permanents ; inscrits dont le
--            classement change après la formation (promotion ?).
-- Tableaux : T62.
-- Lecture  : comparer au tableau 123 : 480 permanents formés en 2024 (132, 142,
--            118, 88), dont 6 avec promotion. Le lot 9 (Q67) comptait 410
--            agents formés dans l'année 2024, tous types confondus.
-- Résultat (01/11/2026) : niveau · finie en 2024 · commencée en 2024 · en
--            2024 · permanents en 2024 · classement changé
--            -  ·   0 ·   0 ·   0 ·   0 · 0
--            A  ·  99 ·  99 ·  99 ·  99 · 0
--            I  ·  99 ·  99 ·  99 ·  99 · 0
--            M  · 112 · 110 · 112 · 112 · 2
--            S  · 100 · 100 · 100 ·  99 · 0
--            Total : 410 agents formés en 2024, dont 409 permanents.
-- Constat  : - les trois dates donnent presque le même compte : les formations
--              de 2024 commencent et finissent dans l'année (lot 9, Q67). Les
--              deux « 99 » des employés et des agents de maîtrise sont une
--              coïncidence, pas un défaut ;
--            - 410 agents pour 480 publiés. Le lot 9 (Q64) compte 525
--              inscriptions en 2024 : le chiffre de la CNSS est entre les
--              agents et les inscriptions. Le lot 18 (Q108) compte les
--              inscriptions des seuls permanents ;
--            - par catégorie (99, 99, 112, 100 pour 132, 142, 118, 88), le
--              même écart de classement que pour l'effectif (Q97) : trop de
--              cadres supérieurs, pas assez d'employés et d'agents de maîtrise ;
--            - 2 classements changés après une formation pour 6 formations
--              « avec promotion » publiées.
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.QUAL_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
inscrits AS (
    SELECT i.PERS_ID,
           NVL(q.NIVEAU, '-')                                     AS niveau,
           CASE WHEN TO_CHAR(p.QUALITE) = 'A' AND si.NATURE_EMPLOIE = 'PERMANENT'
                THEN 1 ELSE 0 END                                 AS permanent,
           CASE WHEN COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2024-01-01'
                 AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) <  DATE '2025-01-01'
                THEN 1 ELSE 0 END                                 AS finie_2024,
           CASE WHEN f.DATE_DEBUT >= DATE '2024-01-01'
                 AND f.DATE_DEBUT <  DATE '2025-01-01'
                THEN 1 ELSE 0 END                                 AS commencee_2024,
           CASE WHEN f.DATE_DEBUT < DATE '2025-01-01'
                 AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2024-01-01'
                THEN 1 ELSE 0 END                                 AS touche_2024,
           CASE WHEN i.CLA_APRES IS NOT NULL AND i.CLA_AVANT IS NOT NULL
                 AND TO_CHAR(i.CLA_APRES) <> TO_CHAR(i.CLA_AVANT)
                THEN 1 ELSE 0 END                                 AS classement_change
    FROM USER_GRH.GRH_INSCRIPTION i
    JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
    LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = i.PERS_ID
    LEFT JOIN situ si ON si.PERS_ID = i.PERS_ID AND si.rn = 1
    LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = si.QUAL_CODE
)
SELECT niveau,
       COUNT(DISTINCT CASE WHEN finie_2024 = 1 THEN PERS_ID END)  AS formes_finie_2024,
       COUNT(DISTINCT CASE WHEN commencee_2024 = 1 THEN PERS_ID END) AS formes_commencee_2024,
       COUNT(DISTINCT CASE WHEN touche_2024 = 1 THEN PERS_ID END) AS formes_en_2024,
       COUNT(DISTINCT CASE WHEN touche_2024 = 1 AND permanent = 1
                           THEN PERS_ID END)                      AS permanents_en_2024,
       COUNT(DISTINCT CASE WHEN touche_2024 = 1 AND classement_change = 1
                           THEN PERS_ID END)                      AS classement_change
FROM inscrits
GROUP BY niveau
ORDER BY niveau;


-- -----------------------------------------------------------------------------
-- Q102 — Le taux d'encadrement, de 2020 à 2024 (tableau 124)
-- Objectif : pour chaque année, permanents présents au 31/12 : cadres (niveaux
--            M et S), non-cadres (A et I), sans niveau, total, et part des
--            cadres ; et le même total pour tous les agents présents.
-- Tableaux : T65.
-- Lecture  : comparer au tableau 124 : 2020 358 / 869, 2021 345 / 834, 2022
--            333 / 806, 2023 316 / 767, 2024 373 / 758 (49 %). Le saut de 2024
--            (de 41 à 49 %) vient-il des 104 recrutés, ou d'un reclassement ?
-- Résultat (01/11/2026) : année · cadres · non-cadres · sans niveau ·
--            permanents · taux · tous les agents (publié : cadres / total, taux)
--            2020 · 307 · 470 · 0 · 777 · 39,5 % · 788 (358 / 869, 41,2 %)
--            2021 · 294 · 447 · 0 · 741 · 39,7 % · 751 (345 / 834, 41,4 %)
--            2022 · 281 · 426 · 0 · 707 · 39,7 % · 720 (333 / 806, 41,3 %)
--            2023 · 263 · 409 · 0 · 672 · 39,1 % · 689 (316 / 767, 41,2 %)
--            2024 · 322 · 420 · 0 · 742 · 43,4 % · 752 (373 / 758, 49,2 %)
-- Constat  : - 2024 : 742 permanents comme en Q97 ; 43,4 % pour 49 % publiés,
--              à cause du classement des catégories (Q97) ;
--            - de 2020 à 2023, il manque chaque année de 92 à 99 permanents
--              (777 pour 869… 672 pour 767), contre 16 en 2024. La CNSS compte
--              donc de 2019 à 2023 environ 84 agents qu'elle sort en 2024 :
--              ce sont les 84 « autres entrées » de 2019 et les 84 « autres
--              sorties » de 2024 du tableau 121. Le lot 13 (Q88) a trouvé 84
--              agents engagés comme permanents (décision 51) en 2019 et
--              radiés sans date : ils ne sont jamais comptés présents. Le lot
--              18 (Q105) vérifie que ce sont les mêmes ;
--            - l'écart sur les cadres est stable (51 à 53 de moins chaque
--              année) : c'est une différence de classement, pas un trou de
--              données. Le saut de 2024 (+59 cadres dans la base, +57 publiés)
--              vient des recrutements ;
--            - 10 à 17 agents de plus par an quand on compte tous les types
--              (temporaires, fonction publique).
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
presents AS (
    SELECT a.annee, p.PERS_ID, TO_CHAR(p.QUALITE) AS qualite
    FROM annees a
    JOIN USER_GRH.GRH_PERSONNE p
         ON COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    WHERE p.ETAT_EMPLOYE = 'ACTIF'
       OR so.dt_sortie > TO_DATE(a.annee || '1231', 'YYYYMMDD')
),
situ AS (
    SELECT pr.annee, pr.PERS_ID, pr.qualite, s.QUAL_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY pr.annee, pr.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM presents pr
    JOIN USER_GRH.GRH_SITUATION s ON s.PERS_ID = pr.PERS_ID
                                 AND s.DATE_DEBUT <= TO_DATE(pr.annee || '1231', 'YYYYMMDD')
),
base AS (
    SELECT si.annee, si.PERS_ID,
           CASE WHEN si.qualite = 'A' AND si.NATURE_EMPLOIE = 'PERMANENT'
                THEN 1 ELSE 0 END                                 AS permanent,
           NVL(q.NIVEAU, '-')                                     AS niveau
    FROM situ si
    LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = si.QUAL_CODE
    WHERE si.rn = 1
)
SELECT annee,
       SUM(CASE WHEN permanent = 1 AND niveau IN ('M', 'S') THEN 1 ELSE 0 END) AS cadres,
       SUM(CASE WHEN permanent = 1 AND niveau IN ('A', 'I') THEN 1 ELSE 0 END) AS non_cadres,
       SUM(CASE WHEN permanent = 1 AND niveau NOT IN ('A', 'I', 'M', 'S')
                THEN 1 ELSE 0 END)                                AS sans_niveau,
       SUM(permanent)                                             AS permanents,
       ROUND(100 * SUM(CASE WHEN permanent = 1 AND niveau IN ('M', 'S') THEN 1 ELSE 0 END)
             / NULLIF(SUM(permanent), 0), 1)                      AS taux_encadrement_pct,
       COUNT(*)                                                   AS tous_les_agents
FROM base
GROUP BY annee
ORDER BY annee;
