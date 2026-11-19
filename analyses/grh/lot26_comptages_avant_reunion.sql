-- =============================================================================
-- LOT 26 — Les comptages qui manquent aux points à valider, avant la réunion
-- Rédigé le 17/11/2026 — EXÉCUTÉ le 17/11/2026 sur USER_GRH, la base source :
-- aucune table de l'entrepôt (DWH, DTM), qui n'est pas encore chargé.
-- Exécution : en mode script (F5) dans SQL Developer. Rien à saisir. Comptages
-- seulement : aucune donnée nominative.
-- Pourquoi ce lot : la fiche des points à valider de la GRH (points_a_valider_
-- v2.md, 63 points) part à la CNSS. Sept points y sont posés d'après les
-- règles, sans chiffre de la base : 12, 24, 28, 47, 50, 51 et 60. Un comptage
-- peut fermer la question sans la poser (aucun cas), ou la rendre plus nette
-- (combien d'agents). Trois autres comptages vérifient des points fragiles :
-- l'écart des formés avec l'annuaire (point 4), les temporaires sans
-- engagement (point 61) et les codes de mouvement (point 49).
-- Mouvements pris en compte, comme dans la requête de l'effectif : décisions
-- avec une date de vigueur, non annulées par une décision 40 (qui cite la
-- décision annulée dans NO_DECI_MODIF) ; l'agent est porté par la décision, ou
-- par sa liste quand elle est collective (GRH_DECISION_PERSONNE).
-- Agents de la CNSS : GRH_PERSONNE.QUALITE = A.
-- Numérotation : suite du lot 25, Q143 à Q152.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q143 — Les agents hors des catégories 01 à 06 (HC, vide, autres codes)
-- Objectif : compter les agents de la CNSS dont la dernière situation au
--            31/12/2024 n'a pas une catégorie de 01 à 06, par catégorie,
--            nature et état de la fiche.
-- Tableaux : T58, T62, T65 (points 47 et 41 de la v2).
-- Lecture  : une ligne HC / PERMANENT / ACTIF dit combien d'agents présents
--            sont rangés en cadres supérieurs par la règle « HC ». Aucune
--            ligne HC : le point 47 tombe.
-- Résultat (17/11/2026) : catégorie · nature · état · agents
--            HC · PERMANENT · INACTIF · 2
--            Aucune autre ligne.
-- Constat  : - seuls 2 agents de la CNSS sont hors des catégories 01 à 06 :
--              deux permanents HC, dont la fiche est aujourd'hui inactive ;
--            - aucun temporaire, aucune catégorie vide (les 4 agents sans
--              catégorie du contrôle de l'effectif n'ont aucune situation) ;
--            - le point 47 ne pèse presque rien : 2 agents au plus, pour les
--              années passées.
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE, TO_CHAR(s.CATEGORIE) AS categorie,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
)
SELECT NVL(si.categorie, '(vide)')          AS categorie,
       NVL(si.NATURE_EMPLOIE, '(vide)')     AS nature,
       NVL(p.ETAT_EMPLOYE, '(sans etat)')   AS etat,
       COUNT(*)                             AS agents
FROM USER_GRH.GRH_PERSONNE p
JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND (si.categorie IS NULL
       OR si.categorie NOT IN ('01', '02', '03', '04', '05', '06'))
GROUP BY NVL(si.categorie, '(vide)'), NVL(si.NATURE_EMPLOIE, '(vide)'),
         NVL(p.ETAT_EMPLOYE, '(sans etat)')
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q144 — La date d'embauche face à la date de prise de service
-- Objectif : par année d'entrée depuis 2015, compter les agents de la CNSS
--            selon que les deux dates sont le même jour, la même année, ou
--            sur deux années différentes ; et ceux à qui une date manque.
-- Tableaux : T58, T60 (date d'entrée), T61 (ancienneté) ; point 50 de la v2.
-- Lecture  : la colonne annee_differente dit combien de recrutements
--            changeraient d'année dans T60 si l'entrée devenait la prise de
--            service. Zéro partout : le point 50 n'a pas d'effet.
-- Résultat (17/11/2026) : année · agents · sans embauche · sans prise de
--            service · même jour · même année autre jour · année différente
--            2015 ·  62 · 0 · 4 ·   3 ·  54 ·  1
--            2016 · 103 · 0 · 2 ·  12 ·  86 ·  3
--            2017 ·  31 · 0 · 1 ·   1 ·  25 ·  4
--            2018 ·  16 · 0 · 1 ·   2 ·  12 ·  1
--            2019 · 122 · 0 · 0 ·  14 ·  20 · 88
--            2020 ·   6 · 0 · 0 ·   1 ·   3 ·  2
--            2021 ·   7 · 0 · 1 ·   4 ·   2 ·  0
--            2022 ·  12 · 0 · 1 ·   3 ·   7 ·  1
--            2023 ·  10 · 0 · 0 ·   2 ·   6 ·  2
--            2024 · 139 · 0 · 1 ·   6 · 132 ·  0
--            2025 · 201 · 0 · 7 · 112 ·  82 ·  0
--            2026 ·   4 · 0 · 4 ·   0 ·   0 ·  0
-- Constat  : - de 2020 à 2025, 5 agents seulement ont une prise de service
--              une autre année que leur embauche (aucun en 2024 et 2025) :
--              le point 50 ne change presque rien à T60 ;
--            - 2019 est à part : 88 des 122 embauchés ont pris leur service
--              une autre année. Ce sont sans doute les 84 engagés de 2019
--              (point 2) : à creuser au lot 27 (Q154, Q155).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)) AS annee_entree,
       COUNT(*)                                                           AS agents,
       SUM(CASE WHEN p.DATE_EMBAUCHE IS NULL THEN 1 ELSE 0 END)           AS sans_embauche,
       SUM(CASE WHEN p.DATE_PRISE_SERVICE IS NULL THEN 1 ELSE 0 END)      AS sans_prise_service,
       SUM(CASE WHEN TRUNC(p.DATE_EMBAUCHE) = TRUNC(p.DATE_PRISE_SERVICE)
                THEN 1 ELSE 0 END)                                        AS meme_jour,
       SUM(CASE WHEN TRUNC(p.DATE_EMBAUCHE) <> TRUNC(p.DATE_PRISE_SERVICE)
                 AND EXTRACT(YEAR FROM p.DATE_EMBAUCHE) = EXTRACT(YEAR FROM p.DATE_PRISE_SERVICE)
                THEN 1 ELSE 0 END)                                        AS meme_annee_autre_jour,
       SUM(CASE WHEN EXTRACT(YEAR FROM p.DATE_EMBAUCHE) <> EXTRACT(YEAR FROM p.DATE_PRISE_SERVICE)
                THEN 1 ELSE 0 END)                                        AS annee_differente
FROM USER_GRH.GRH_PERSONNE p
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) >= DATE '2015-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE))
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- Q145 — Les mises à disposition (décision 14) et ce qui les suit
-- Objectif : pour la dernière mise à disposition de chaque agent de la CNSS,
--            par année, dire si une remise en activité (18) ou un départ la
--            suit, et combien d'agents sont encore mis à disposition.
-- Tableaux : T58, T60 (point 51 de la v2 ; sortie MISE_DIS de T60).
-- Lecture  : la colonne encore_mis_a_disposition donne les agents que la
--            règle garde aujourd'hui dans l'effectif. La colonne
--            avec_remise_apres dit si la décision 18 sert aussi de retour
--            d'une mise à disposition.
-- Résultat (17/11/2026) : 33 agents ont eu une mise à disposition (dernière
--            de 1971 à 2024). Après elle : 17 ont une remise en activité
--            (18), 7 sont partis, 5 n'ont ni retour ni départ et une fiche
--            non inactive (dernières mises à disposition de 1971, 2006,
--            2007, 2017 et 2023), 4 ont une fiche inactive sans suite datée
--            (2016, 2019, 2022, 2024).
-- Constat  : - la décision 18 sert aussi de retour d'une mise à disposition :
--              17 cas sur 33 ;
--            - 5 agents seraient encore mis à disposition aujourd'hui, et
--              comptés dans l'effectif par la règle ;
--            - le sujet est petit (une mise à disposition par an environ).
-- -----------------------------------------------------------------------------
WITH decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('14', '18', '02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
mad AS (
    SELECT PERS_ID, MAX(dt) AS dt
    FROM decs
    WHERE code = '14' AND PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
suite AS (
    SELECT m.PERS_ID, m.dt,
           MIN(CASE WHEN x.code = '18' THEN x.dt END)  AS retour,
           MIN(CASE WHEN x.code <> '18' THEN x.dt END) AS depart
    FROM mad m
    LEFT JOIN decs x ON x.PERS_ID = m.PERS_ID
                    AND x.dt > m.dt
                    AND x.code <> '14'
    GROUP BY m.PERS_ID, m.dt
)
SELECT EXTRACT(YEAR FROM su.dt)                                        AS annee_mise_a_dispo,
       COUNT(*)                                                        AS agents,
       SUM(CASE WHEN su.retour IS NOT NULL THEN 1 ELSE 0 END)          AS avec_remise_apres,
       SUM(CASE WHEN su.retour IS NULL AND su.depart IS NOT NULL
                THEN 1 ELSE 0 END)                                     AS partis_ensuite,
       SUM(CASE WHEN su.retour IS NULL AND su.depart IS NULL
                 AND NVL(p.ETAT_EMPLOYE, '-') <> 'INACTIF'
                THEN 1 ELSE 0 END)                                     AS encore_mis_a_disposition,
       SUM(CASE WHEN su.retour IS NULL AND su.depart IS NULL
                 AND NVL(p.ETAT_EMPLOYE, '-') = 'INACTIF'
                THEN 1 ELSE 0 END)                                     AS fiche_inactive_sans_suite
FROM suite su
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = su.PERS_ID
WHERE TO_CHAR(p.QUALITE) = 'A'
GROUP BY EXTRACT(YEAR FROM su.dt)
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- Q146 — Les agents entrés et partis la même année
-- Objectif : compter, par année d'entrée depuis 2019 et par nature, les
--            agents de la CNSS dont le premier départ tombe la même année que
--            l'entrée.
-- Tableaux : T60 (point 24 de la v2, règle G16).
-- Lecture  : ces agents ne sont sur aucune des deux photos de l'année : T60
--            ne les compte ni en entrée ni en sortie. Zéro chez les
--            permanents : le point 24 n'a pas d'effet sur le tableau des
--            permanents.
-- Résultat (17/11/2026) : année · nature · agents entrés et partis
--            2019 · TEMPORAIRE · 15
--            2021 · TEMPORAIRE ·  1
--            2024 · PERMANENT  ·  2
-- Constat  : - 2 permanents sont entrés et partis en 2024 : T60 donne 101
--              recrutements, l'annuaire CNSS 104 ; avec ces deux agents, 103 ;
--            - aucun autre cas chez les permanents depuis 2019 ; le point 24
--              ne joue que sur ces 2 agents.
-- -----------------------------------------------------------------------------
WITH decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
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
    SELECT PERS_ID, dt FROM decs WHERE PERS_ID IS NOT NULL
    UNION ALL
    SELECT PERS_ID, TRUNC(DATE_DECES) FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
),
entrees AS (
    SELECT p.PERS_ID, TRUNC(COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)) AS entree
    FROM USER_GRH.GRH_PERSONNE p
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) >= DATE '2019-01-01'
),
-- le premier départ après l'entrée
premier AS (
    SELECT e.PERS_ID, e.entree, MIN(dp.dt) AS depart
    FROM entrees e
    JOIN departs dp ON dp.PERS_ID = e.PERS_ID AND dp.dt >= e.entree
    GROUP BY e.PERS_ID, e.entree
),
situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
)
SELECT EXTRACT(YEAR FROM pm.entree)                 AS annee,
       NVL(si.NATURE_EMPLOIE, '(sans situation)')   AS nature,
       COUNT(*)                                     AS entres_et_partis
FROM premier pm
LEFT JOIN situ si ON si.PERS_ID = pm.PERS_ID AND si.rn = 1
WHERE EXTRACT(YEAR FROM pm.depart) = EXTRACT(YEAR FROM pm.entree)
GROUP BY EXTRACT(YEAR FROM pm.entree), NVL(si.NATURE_EMPLOIE, '(sans situation)')
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q147 — Les décès : en activité, ou après la retraite
-- Objectif : par année de décès depuis 2019, compter les agents de la CNSS
--            morts sans retraite avant (décès en activité), morts après leur
--            retraite, et les retraites datées après le décès.
-- Tableaux : T60 (colonne des décès ; points 12 et 3 de la v2).
-- Lecture  : la date du décès est la date de décès de la fiche, sinon la
--            liquidation des droits (34). T60 ne compte que la colonne
--            deces_en_activite. L'annuaire CNSS 2024 (tableau 121) publie 4,
--            2, 3, 5 et 3 décès de 2020 à 2024.
-- Résultat (17/11/2026) : année · décès · datés par la fiche · en activité ·
--            après la retraite · retraite datée après
--            2019 · 2 · 0 · 2 · 0 · 0
--            2020 · 3 · 0 · 3 · 0 · 0
--            2021 · 4 · 0 · 4 · 0 · 0
--            2022 · 4 · 0 · 4 · 0 · 0
--            2023 · 3 · 0 · 3 · 0 · 0
--            2024 · 5 · 0 · 5 · 0 · 0
--            Aucune ligne pour 2025.
-- Constat  : - aucun décès après la retraite n'est enregistré depuis 2019 :
--              le point 12 n'a aucun effet aujourd'hui ;
--            - aucun décès n'est daté par la fiche : tous le sont par la
--              liquidation des droits (34), environ 8 mois plus tard ;
--            - aucun décès n'est encore visible pour 2025 ;
--            - l'annuaire CNSS 2024 (tableau 121) publie 4, 2, 3, 5 et 3 décès
--              de 2020 à 2024, la base 3, 4, 4, 3 et 5 : le même total à deux
--              près (17 pour 19), décalé d'un an environ (point 3).
-- -----------------------------------------------------------------------------
WITH decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
retraites AS (
    SELECT PERS_ID, MIN(dt) AS dt
    FROM decs
    WHERE code = '02' AND PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
-- la date de décès de la fiche d'abord ; sinon la liquidation des droits
deces AS (
    SELECT p.PERS_ID,
           COALESCE(TRUNC(p.DATE_DECES), l.dt) AS dt,
           CASE WHEN p.DATE_DECES IS NOT NULL THEN 'fiche' ELSE 'liquidation' END AS origine
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN (SELECT PERS_ID, MIN(dt) AS dt
               FROM decs
               WHERE code = '34' AND PERS_ID IS NOT NULL
               GROUP BY PERS_ID) l ON l.PERS_ID = p.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND (p.DATE_DECES IS NOT NULL OR l.dt IS NOT NULL)
)
SELECT EXTRACT(YEAR FROM dc.dt)                                           AS annee_deces,
       COUNT(*)                                                           AS deces,
       SUM(CASE WHEN dc.origine = 'fiche' THEN 1 ELSE 0 END)               AS dates_par_la_fiche,
       SUM(CASE WHEN r.PERS_ID IS NULL THEN 1 ELSE 0 END)                 AS deces_en_activite,
       SUM(CASE WHEN r.dt <= dc.dt THEN 1 ELSE 0 END)                     AS deces_apres_retraite,
       SUM(CASE WHEN r.dt > dc.dt THEN 1 ELSE 0 END)                      AS retraite_datee_apres
FROM deces dc
LEFT JOIN retraites r ON r.PERS_ID = dc.PERS_ID
WHERE dc.dt >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM dc.dt)
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- Q148 — Les formations à cheval sur deux années, et celles de plus d'un an
-- Objectif : par année de début depuis 2019, compter les formations finies
--            une autre année que celle de leur début, celles de plus de 365
--            jours, et les agents inscrits à ces formations.
-- Tableaux : T62 (points 28 et 60 de la v2).
-- Lecture  : la règle compte une formation l'année où elle commence. La
--            colonne inscrits_a_cheval donne les agents qu'une autre règle
--            (année de fin, ou « en formation » chaque année) déplacerait ou
--            compterait deux fois. Zéro : les points 28 et 60 tombent.
-- Résultat (17/11/2026) : année · formations · sans date de fin · finies une
--            autre année · plus d'un an · inscrits à cheval · inscrits plus
--            d'un an
--            2019 · 28 · 0 · 0 · 0 · 0 · 0
--            2020 · 28 · 0 · 0 · 0 · 0 · 0
--            2021 · 37 · 3 · 0 · 0 · 0 · 0
--            2022 · 39 · 0 · 0 · 0 · 0 · 0
--            2023 · 37 · 1 · 1 · 0 · 2 · 0
--            2024 · 44 · 0 · 0 · 0 · 0 · 0
--            2025 · 35 · 0 · 0 · 0 · 0 · 0
-- Constat  : - en sept ans, une seule formation est à cheval sur deux années
--              (commencée en 2023, finie en 2024), avec 2 agents ;
--            - aucune formation ne dure plus d'un an : le point 60 tombe ;
--            - le point 28 ne porte que sur ces 2 agents (ce sont les 2
--              « avec promotion » de 2024, voir Q150 et le contrôle de T62).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM f.DATE_DEBUT)                                    AS annee_debut,
       COUNT(*)                                                           AS formations,
       SUM(CASE WHEN f.DATE_FIN IS NULL THEN 1 ELSE 0 END)                AS sans_date_fin,
       SUM(CASE WHEN EXTRACT(YEAR FROM f.DATE_FIN) > EXTRACT(YEAR FROM f.DATE_DEBUT)
                THEN 1 ELSE 0 END)                                        AS finies_une_autre_annee,
       SUM(CASE WHEN f.DATE_FIN - f.DATE_DEBUT > 365 THEN 1 ELSE 0 END)   AS plus_d_un_an,
       SUM(CASE WHEN EXTRACT(YEAR FROM f.DATE_FIN) > EXTRACT(YEAR FROM f.DATE_DEBUT)
                THEN NVL(i.nb, 0) ELSE 0 END)                             AS inscrits_a_cheval,
       SUM(CASE WHEN f.DATE_FIN - f.DATE_DEBUT > 365
                THEN NVL(i.nb, 0) ELSE 0 END)                             AS inscrits_plus_d_un_an
FROM USER_GRH.GRH_FORMATION f
LEFT JOIN (SELECT FORM_NO, COUNT(DISTINCT PERS_ID) AS nb
           FROM USER_GRH.GRH_INSCRIPTION
           GROUP BY FORM_NO) i ON i.FORM_NO = f.FORM_NO
WHERE f.DATE_DEBUT >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM f.DATE_DEBUT)
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- Q149 — Les formés de 2024 selon qui ils sont (qualité et nature)
-- Objectif : compter les personnes inscrites à une formation commencée en
--            2024, par qualité de la fiche et nature de la dernière situation
--            au 31/12/2024 ; avec les inscriptions sans agent et les agents
--            inconnus de la fiche.
-- Tableaux : T62 (points 4, 27 et 53 de la v2).
-- Lecture  : la base donne 409 permanents de la CNSS formés, l'annuaire 480.
--            Si le total de toutes les lignes approche 480, l'annuaire
--            compte aussi les temporaires, la fonction publique (B), les
--            affectés (C) ou des personnes externes.
-- Résultat (17/11/2026) : qualité · nature · personnes · inscriptions
--            A · PERMANENT · 407 · 522
--            B · PERMANENT ·   1 ·   1
--            Aucune autre ligne.
-- Constat  : - 408 personnes formées en 2024, toutes permanentes : 407 agents
--              de la CNSS et 1 agent de la fonction publique ;
--            - aucun temporaire formé en 2024 (le point 27 parlait à tort
--              d'un contractuel : c'est un agent de la fonction publique) ;
--            - aucune personne externe n'est dans l'application ;
--            - les 480 de l'annuaire ne viennent donc pas d'autres types
--              d'agents (point 4).
-- -----------------------------------------------------------------------------
WITH inscrits AS (
    SELECT i.PERS_ID, COUNT(*) AS inscriptions
    FROM USER_GRH.GRH_INSCRIPTION i
    JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
    WHERE f.DATE_DEBUT >= DATE '2024-01-01'
      AND f.DATE_DEBUT <  DATE '2025-01-01'
    GROUP BY i.PERS_ID
),
situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
)
SELECT CASE WHEN n.PERS_ID IS NULL THEN '(inscription sans agent)'
            WHEN p.PERS_ID IS NULL THEN '(agent inconnu de la fiche)'
            ELSE NVL(TO_CHAR(p.QUALITE), '(vide)') END     AS qualite,
       NVL(si.NATURE_EMPLOIE, '(sans situation)')          AS nature,
       COUNT(*)                                            AS personnes,
       SUM(n.inscriptions)                                 AS inscriptions
FROM inscrits n
LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = n.PERS_ID
LEFT JOIN situ si ON si.PERS_ID = n.PERS_ID AND si.rn = 1
GROUP BY CASE WHEN n.PERS_ID IS NULL THEN '(inscription sans agent)'
              WHEN p.PERS_ID IS NULL THEN '(agent inconnu de la fiche)'
              ELSE NVL(TO_CHAR(p.QUALITE), '(vide)') END,
         NVL(si.NATURE_EMPLOIE, '(sans situation)')
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q150 — Les permanents formés en 2024 selon la date retenue
-- Objectif : compter les permanents de la CNSS formés en 2024 de trois
--            façons : formation commencée en 2024 (la règle), finie en 2024,
--            ou en cours à un moment de 2024.
-- Tableaux : T62 (points 4, 28 et 60 de la v2).
-- Lecture  : la règle donne 409 (colonne debut_en_2024). Si en_cours_en_2024
--            ou fin_en_2024 approche 480, l'annuaire ne compte pas à la date
--            de début : les points 28 et 60 expliquent alors l'écart.
-- Résultat (17/11/2026) : début en 2024 · fin en 2024 · en cours en 2024 ·
--            inscriptions (début) · inscriptions (en cours)
--            407 · 409 · 409 · 522 · 524
-- Constat  : - quelle que soit la date retenue, la base donne 407 à 409
--              permanents formés : la date n'explique pas les 480 de
--              l'annuaire (point 4) ;
--            - les 2 agents d'écart sont ceux de la formation commencée en
--              2023 et finie en 2024 (Q148).
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
permanents AS (
    SELECT p.PERS_ID
    FROM USER_GRH.GRH_PERSONNE p
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
)
SELECT COUNT(DISTINCT CASE WHEN f.DATE_DEBUT >= DATE '2024-01-01'
                            AND f.DATE_DEBUT <  DATE '2025-01-01'
                           THEN i.PERS_ID END)                         AS debut_en_2024,
       COUNT(DISTINCT CASE WHEN f.DATE_FIN >= DATE '2024-01-01'
                            AND f.DATE_FIN <  DATE '2025-01-01'
                           THEN i.PERS_ID END)                         AS fin_en_2024,
       COUNT(DISTINCT CASE WHEN f.DATE_DEBUT < DATE '2025-01-01'
                            AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2024-01-01'
                           THEN i.PERS_ID END)                         AS en_cours_en_2024,
       SUM(CASE WHEN f.DATE_DEBUT >= DATE '2024-01-01'
                 AND f.DATE_DEBUT <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                     AS inscriptions_debut_2024,
       SUM(CASE WHEN f.DATE_DEBUT < DATE '2025-01-01'
                 AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2024-01-01'
                THEN 1 ELSE 0 END)                                     AS inscriptions_en_cours_2024
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
JOIN permanents pe ON pe.PERS_ID = i.PERS_ID;


-- -----------------------------------------------------------------------------
-- Q151 — Les temporaires sans aucune décision d'engagement (50)
-- Objectif : compter les agents de la CNSS dont la dernière situation est
--            temporaire et qui n'ont aucune décision 50, par état de la fiche
--            et par période d'entrée.
-- Tableaux : T58 (tableau des temporaires), T60 ; point 61 de la v2.
-- Lecture  : sans décision 50, la règle ne les compte jamais. Seules les
--            lignes dont l'état n'est pas INACTIF posent question : ce sont
--            peut-être des contractuels en poste, absents de T58.
-- Résultat (17/11/2026) : état · entrée · temporaires
--            INACTIF     · avant 2015  · 227
--            (sans état) · avant 2015  ·   2
--            (sans état) · 2015 à 2019 ·   1
-- Constat  : - 230 temporaires n'ont aucune décision 50, dont 227 à la fiche
--              inactive, entrés avant 2015 : sans effet sur les années
--              suivies ;
--            - restent 3 fiches sans état, anciennes : une fiche sans état
--              n'est donc pas toujours une embauche récente (point 45).
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
),
engages AS (
    SELECT DISTINCT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE = '50'
      AND d.DATE_VIGUEUR IS NOT NULL
)
SELECT NVL(p.ETAT_EMPLOYE, '(sans etat)')                                 AS etat,
       CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) IS NULL
                 THEN 'd. sans date'
            WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2015-01-01'
                 THEN 'a. avant 2015'
            WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2020-01-01'
                 THEN 'b. 2015 a 2019'
            ELSE 'c. 2020 et apres' END                                   AS entree,
       COUNT(*)                                                           AS temporaires
FROM USER_GRH.GRH_PERSONNE p
JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND si.NATURE_EMPLOIE = 'TEMPORAIRE'
  AND NOT EXISTS (SELECT 1 FROM engages e WHERE e.PERS_ID = p.PERS_ID)
GROUP BY NVL(p.ETAT_EMPLOYE, '(sans etat)'),
         CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) IS NULL
                   THEN 'd. sans date'
              WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2015-01-01'
                   THEN 'a. avant 2015'
              WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) < DATE '2020-01-01'
                   THEN 'b. 2015 a 2019'
              ELSE 'c. 2020 et apres' END
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q152 — Les codes de mouvement de l'application
-- Objectif : lister les types de mouvement de USER_GRH.GRH_TYPE_MOUVEMENT, la
--            table de la source que l'entrepôt copie dans
--            DTM.DIM_GRH_TYPE_MOUVEMENT.
-- Tableaux : T60 (point 49 de la v2 ; tâche 2 de regles_t60.md).
-- Lecture  : la requête de T60 range chaque mouvement par ces codes. Il faut
--            y trouver les 18 codes de la CNSS (DEBUT, RECRU, REM, DETAC,
--            AUTRE_ENT, RET, DEC, LIC, DEM, DISP, MISE_DIS, FIN_DETACH,
--            FIN_CTRAT, AUTRE_SORT, AFF, MUT, PROMO, RETRO) et NOM. Un code
--            absent laisse sa nature vide dans T60 : la colonne « autres »
--            du contrôle l'oublierait. Un code écrit deux fois compterait
--            deux fois ses mouvements.
-- Résultat (17/11/2026) : 18 codes, aucun en double.
--            ALL  : DEBUT (INITIAL) ; RECRU, AUTRE_ENT (ENTREES) ; RET, DEC,
--                   AUTRE_SORT (SORTIES).
--            CNSS : REM, DETAC (ENTREES) ; AFF, MUT, PROMO, RETRO (INTERNE) ;
--                   LIC, DEM, DISP, MISE_DIS, FIN_DETACH, FIN_CTRAT (SORTIES).
-- Constat  : - les 18 codes de la CNSS sont là ; la liste du point 49 est
--              bien celle de l'application ;
--            - le code NOM (nomination) n'y est pas, alors que la table des
--              mouvements l'utilise (2 à 52 mouvements par an) : sa nature
--              reste vide dans T60 (point 42).
-- -----------------------------------------------------------------------------
SELECT DOMAINE_TYPE_MVT, CODE_NAT_MVT, CODE_TYPE_MVT, LIB_TYPE_MVT, RANG_TYPE_MVT
FROM USER_GRH.GRH_TYPE_MOUVEMENT
ORDER BY 1, 2, 5, 3;
