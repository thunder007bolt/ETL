-- =============================================================================
-- LOT 6 — Effectif au 31/12, sorties et zones d'ombre
-- Rédigé et exécuté le 21/10/2026 sur USER_GRH (Q39 sur le schéma DTM).
-- Exécution : en mode script (F5) dans SQL Developer ; tout est en SELECT et ne
-- renvoie que des comptages (aucune donnée nominative).
-- Tableaux CIPRES visés : T58, T59, T61, T65 (effectif au 31/12) et T60
-- (effectif au 1er janvier, sorties).
-- Hypothèse testée : la règle de l'effectif du DTM (situation en vigueur =
-- date de fin vide ou postérieure) compte les agents partis, car leur dernière
-- situation n'a presque jamais de date de fin (lot 3, Q14).
-- Conclusion du lot : hypothèse confirmée. La règle compte 2 845 agents au
-- 31/12/2025 pour 879 agents réellement actifs.
-- Q32 et Q38 ont échoué au premier passage (ORA-00972 : un alias dépassait
-- 30 caractères, limite d'Oracle avant 12.2). Alias raccourcis : Q32 relancée,
-- Q38 encore à relancer.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q23 — Effectif au 31/12/2025 selon la règle du DTM, par état de l'agent
-- Objectif : mesurer combien d'agents INACTIF la règle actuelle compte encore,
--            et si un agent a plusieurs situations « en vigueur ».
-- Tableaux : T58, T59, T61, T65 ; ligne « effectif » de T60.
-- À comparer : 879 agents ACTIF dans GRH_PERSONNE.
-- Résultat : ACTIF 873 agents comptés (872 au statut A, 873 situations) ;
--            INACTIF 1 915 (716 au statut A, 1 916 situations) ;
--            état vide 57 (50 au statut A, 57 situations).
--            Total : 2 845 agents comptés, dont 1 638 au statut A.
-- Constat  : la règle compte 1 915 agents partis. L'effectif est plus que
--            triplé et le nombre d'actifs presque doublé. Pas de doublon :
--            une situation en vigueur par agent.
-- -----------------------------------------------------------------------------
SELECT p.ETAT_EMPLOYE,
       COUNT(DISTINCT s.PERS_ID) AS agents_comptes,
       COUNT(DISTINCT CASE WHEN s.STATUT_EMPLOYE = 'A' THEN s.PERS_ID END) AS comptes_actifs,
       COUNT(*) AS situations_en_vigueur
FROM USER_GRH.GRH_SITUATION s
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = s.PERS_ID
WHERE s.DATE_DEBUT <= DATE '2025-12-31'
  AND (s.DATE_FIN IS NULL OR s.DATE_FIN > DATE '2025-12-31')
  AND UPPER(TRIM(s.NATURE_SITUATION)) IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')
  AND p.DATE_NAISS IS NOT NULL
GROUP BY p.ETAT_EMPLOYE;


-- -----------------------------------------------------------------------------
-- Q24 — Effectif au 31/12 de 2019 à 2025 : règle du DTM contre agents présents
-- Objectif : voir si l'écart grandit chaque année (signe que les partants
--            s'accumulent dans l'effectif).
--            « présents » = embauchés avant le 31/12, sans décès ni décision de
--            sortie (02, 36, 52, 54) avant cette date. C'est un maximum : les
--            temporaires partis sans décision y restent.
-- Tableaux : T58, T59, T61, T65 ; effectif au 1er janvier et au 31/12 de T60.
-- Résultat : année · règle DTM · dont statut A · présents sans sortie connue
--            2019 · 2 472 · 1 353 · 2 068
--            2020 · 2 478 · 1 356 · 2 036
--            2021 · 2 488 · 1 361 · 2 011
--            2022 · 2 497 · 1 360 · 1 991
--            2023 · 2 508 · 1 363 · 1 971
--            2024 · 2 633 · 1 459 · 2 083
--            2025 · 2 845 · 1 638 · 2 261
-- Constat  : l'effectif selon la règle du DTM augmente chaque année, car les
--            partants s'y accumulent. Même en retirant les sorties connues, il
--            reste 2 261 agents en 2025 pour 879 actifs : environ 1 380 départs
--            n'ont ni décision de sortie ni date de décès.
-- -----------------------------------------------------------------------------
WITH ans AS (
  SELECT 2018 + LEVEL AS annee,
         TO_DATE(TO_CHAR(2018 + LEVEL) || '1231', 'YYYYMMDD') AS dt
  FROM dual CONNECT BY LEVEL <= 7
),
sorties AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, MIN(d.DATE_VIGUEUR) AS dt_sortie
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE IN ('02','36','52','54')
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
)
SELECT a.annee,
       (SELECT COUNT(DISTINCT s.PERS_ID)
          FROM USER_GRH.GRH_SITUATION s
         WHERE s.DATE_DEBUT <= a.dt
           AND (s.DATE_FIN IS NULL OR s.DATE_FIN > a.dt)
           AND UPPER(TRIM(s.NATURE_SITUATION)) IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')) AS effectif_regle_dtm,
       (SELECT COUNT(DISTINCT s.PERS_ID)
          FROM USER_GRH.GRH_SITUATION s
         WHERE s.DATE_DEBUT <= a.dt
           AND (s.DATE_FIN IS NULL OR s.DATE_FIN > a.dt)
           AND s.STATUT_EMPLOYE = 'A'
           AND UPPER(TRIM(s.NATURE_SITUATION)) IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')) AS actifs_regle_dtm,
       (SELECT COUNT(*)
          FROM USER_GRH.GRH_PERSONNE p
         WHERE p.DATE_EMBAUCHE <= a.dt
           AND (p.DATE_DECES IS NULL OR p.DATE_DECES > a.dt)
           AND NOT EXISTS (SELECT 1 FROM sorties x
                            WHERE x.PERS_ID = p.PERS_ID AND x.dt_sortie <= a.dt)) AS presents_sans_sortie_connue
FROM ans a
ORDER BY a.annee;


-- -----------------------------------------------------------------------------
-- Q25 — Nombre de situations « en vigueur » par agent au 31/12/2025
-- Objectif : savoir si un même agent est compté dans plusieurs cases de
--            l'effectif (qualification, domaine ou lieu différents).
-- Tableaux : T58, T59, T61.
-- Résultat : 2 844 agents ont une seule situation en vigueur, 1 en a deux.
-- Constat  : pas de double comptage. Le problème vient uniquement des agents
--            partis dont la dernière situation reste ouverte.
-- -----------------------------------------------------------------------------
SELECT nb_situations, COUNT(*) AS agents
FROM (SELECT s.PERS_ID, COUNT(*) AS nb_situations
        FROM USER_GRH.GRH_SITUATION s
       WHERE s.DATE_DEBUT <= DATE '2025-12-31'
         AND (s.DATE_FIN IS NULL OR s.DATE_FIN > DATE '2025-12-31')
         AND UPPER(TRIM(s.NATURE_SITUATION)) IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')
       GROUP BY s.PERS_ID)
GROUP BY nb_situations
ORDER BY nb_situations;


-- -----------------------------------------------------------------------------
-- Q26 — Champs d'état des situations (ETAT, VALIDE, VERIFIE)
-- Objectif : trouver un champ qui désigne la situation en cours, ce qui
--            donnerait une règle d'effectif fiable sans date de fin.
-- Tableaux : T58, T59, T61, T65.
-- Résultat : ETAT = INACTIF : 7 928 situations (1 621 agents), toutes avec une
--            date de fin, années 212 à 3015.
--            ETAT = ACTIF : 2 978 situations (2 952 agents), dont 2 952 sans
--            date de fin, années 1957 à 3015.
--            VALIDE et VERIFIE valent toujours N.
-- Constat  : ETAT = ACTIF désigne la situation en cours de l'agent, pas sa
--            présence dans le personnel : 2 952 agents ont une situation « en
--            cours » alors que 879 sont actifs. 26 agents ont deux situations en
--            cours. VALIDE et VERIFIE sont inutilisables.
-- -----------------------------------------------------------------------------
SELECT s.ETAT, s.VALIDE, s.VERIFIE,
       COUNT(*) AS nb,
       COUNT(DISTINCT s.PERS_ID) AS agents,
       SUM(CASE WHEN s.DATE_FIN IS NULL THEN 1 ELSE 0 END) AS sans_date_fin,
       MIN(EXTRACT(YEAR FROM s.DATE_DEBUT)) AS an_min,
       MAX(EXTRACT(YEAR FROM s.DATE_DEBUT)) AS an_max
FROM USER_GRH.GRH_SITUATION s
GROUP BY s.ETAT, s.VALIDE, s.VERIFIE
ORDER BY nb DESC;


-- -----------------------------------------------------------------------------
-- Q27 — Chaînage des situations par OLD_SITU_ID
-- Objectif : vérifier si chaque nouvelle situation pointe vers l'ancienne.
--            Si oui, la situation en cours = celle qu'aucune autre ne remplace,
--            et il en faut une seule par agent.
-- Tableaux : T58, T59, T61, T65.
-- Résultat : non remplacées 8 211 (dont 2 952 sans date de fin) ; remplacées
--            2 695 (toutes avec date de fin). Situations non remplacées par
--            agent : 1 (1 585 agents), 2 (363), 3 (322), 4 (255), 5 (173),
--            jusqu'à 28.
-- Constat  : OLD_SITU_ID n'est rempli qu'en partie et ne permet pas de trouver
--            la situation en cours. La date de fin, elle, est bien posée quand
--            une situation est remplacée.
-- -----------------------------------------------------------------------------
SELECT etat_chaine, COUNT(*) AS nb, SUM(CASE WHEN DATE_FIN IS NULL THEN 1 ELSE 0 END) AS sans_date_fin
FROM (SELECT s.DATE_FIN,
             CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_SITUATION n WHERE n.OLD_SITU_ID = s.SITU_ID)
                  THEN 'remplacée par une autre' ELSE 'non remplacée' END AS etat_chaine
        FROM USER_GRH.GRH_SITUATION s)
GROUP BY etat_chaine;

SELECT nb_non_remplacees, COUNT(*) AS agents
FROM (SELECT s.PERS_ID, COUNT(*) AS nb_non_remplacees
        FROM USER_GRH.GRH_SITUATION s
       WHERE NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_SITUATION n WHERE n.OLD_SITU_ID = s.SITU_ID)
       GROUP BY s.PERS_ID)
GROUP BY nb_non_remplacees
ORDER BY nb_non_remplacees;


-- -----------------------------------------------------------------------------
-- Q28 — Qui sont les agents de chaque statut ? (statut × état × décisions)
-- Objectif : expliquer en une requête plusieurs zones d'ombre :
--            - les 409 décédés (Y) : ont-ils une date de décès, des frais
--              funéraires (75), une liquidation des droits (34) ?
--            - les 387 radiés (X) : licenciement, démission, rupture,
--              temporaire en fin de contrat ?
--            - les 691 inactifs restés au statut A : retraités, temporaires ?
--            - les 879 actifs : combien de temporaires (50) et de permanents (51) ?
-- Tableaux : T60 (lignes décès et autres sorties) ; T58 à T65 (effectif).
-- Résultat : (colonnes : agents · date de décès · 02 · 34 · 35 · 36 · 50 · 51 ·
--            52 · 54 · 75 · aucune de ces décisions)
--            A / ACTIF     875 ·  0 ·   4 ·   0 ·   7 · 0 ·   6 · 626 · 0 ·  0 · 0 · 239
--            A / INACTIF   691 · 26 · 165 ·  80 · 227 · 8 · 316 ·  27 · 1 · 12 · 0 ·  85
--            R / INACTIF   584 ·  0 · 568 ·   5 ·   2 · 0 ·   2 ·  46 · 0 ·  1 · 1 ·   9
--            Y / INACTIF   409 · 32 ·  63 · 114 ·  24 · 1 ·   6 ·  21 · 0 ·  0 · 2 · 199
--            X / INACTIF   387 ·  0 ·   8 ·   2 ·  87 · 10 · 233 ·  94 · 0 · 12 · 0 ·  37
--            - / INACTIF   130 ·  0 ·   6 ·   2 ·   9 · 1 ·   2 ·   0 · 0 ·  1 · 0 · 109
--            A / (vide)     56 ·  0 ·   0 ·   0 ·   3 · 0 ·  43 ·   8 · 0 ·  0 · 0 ·   4
--            autres        48 lignes réparties sur E, F, B, D, X, Y, C, H.
--            Total : 3 180 agents.
-- Constat  : - actifs : 626 engagés comme permanents (51), 6 temporaires (50),
--              239 sans décision d'engagement (agents anciens) ;
--            - 691 inactifs restés au statut A : 316 temporaires, 227 avec une
--              indemnité de fin de contrat (35), 165 retraités (02), 26 décès
--              datés, 12 licenciés, 8 démissionnaires, 85 sans aucune de ces
--              décisions ;
--            - 584 retraités (R) : 568 ont une décision 02 ;
--            - 409 décédés (Y) : 32 dates de décès, 114 liquidations des
--              droits (34), 63 décisions de retraite (décès après la retraite ?),
--              199 sans aucune décision ;
--            - 387 radiés (X) : 233 temporaires, 87 indemnités de fin de
--              contrat, 94 engagés comme permanents, 12 licenciements,
--              10 démissions.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.STATUT_EMPLOYE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
),
deci AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
         MAX(CASE WHEN d.CDEC_CODE = '02' THEN 1 ELSE 0 END) AS f02,
         MAX(CASE WHEN d.CDEC_CODE = '34' THEN 1 ELSE 0 END) AS f34,
         MAX(CASE WHEN d.CDEC_CODE = '35' THEN 1 ELSE 0 END) AS f35,
         MAX(CASE WHEN d.CDEC_CODE = '36' THEN 1 ELSE 0 END) AS f36,
         MAX(CASE WHEN d.CDEC_CODE = '50' THEN 1 ELSE 0 END) AS f50,
         MAX(CASE WHEN d.CDEC_CODE = '51' THEN 1 ELSE 0 END) AS f51,
         MAX(CASE WHEN d.CDEC_CODE = '52' THEN 1 ELSE 0 END) AS f52,
         MAX(CASE WHEN d.CDEC_CODE = '54' THEN 1 ELSE 0 END) AS f54,
         MAX(CASE WHEN d.CDEC_CODE = '75' THEN 1 ELSE 0 END) AS f75
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE IN ('02','34','35','36','50','51','52','54','75')
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
)
SELECT NVL(r.STATUT_EMPLOYE, '-') AS statut_derniere_situation,
       NVL(p.ETAT_EMPLOYE, '-') AS etat_agent,
       COUNT(*) AS agents,
       SUM(CASE WHEN p.DATE_DECES IS NOT NULL THEN 1 ELSE 0 END) AS date_deces,
       SUM(NVL(x.f02, 0)) AS retraite_02,
       SUM(NVL(x.f34, 0)) AS liquidation_34,
       SUM(NVL(x.f35, 0)) AS indemnite_fin_contrat_35,
       SUM(NVL(x.f36, 0)) AS demission_36,
       SUM(NVL(x.f50, 0)) AS temporaire_50,
       SUM(NVL(x.f51, 0)) AS engagement_51,
       SUM(NVL(x.f52, 0)) AS rupture_52,
       SUM(NVL(x.f54, 0)) AS licenciement_54,
       SUM(NVL(x.f75, 0)) AS frais_funeraires_75,
       SUM(CASE WHEN x.PERS_ID IS NULL THEN 1 ELSE 0 END) AS aucune_de_ces_decisions
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
LEFT JOIN deci x ON x.PERS_ID = p.PERS_ID
GROUP BY NVL(r.STATUT_EMPLOYE, '-'), NVL(p.ETAT_EMPLOYE, '-')
ORDER BY agents DESC;


-- -----------------------------------------------------------------------------
-- Q29 — Agents sans aucune situation datée avant aujourd'hui
-- Objectif : savoir qui sont les ~122 agents absents de l'effectif faute de
--            situation (temporaires ? embauches récentes ? dates aberrantes ?).
-- Tableaux : T58, T59, T61, T65.
-- Résultat : INACTIF « autre » 116 (embauchés de 1985 à 2003) ; INACTIF
--            temporaires 2 (1997–2005) ; état vide 2 (2016–2017) et 1
--            temporaire (2011) ; ACTIF 1 (embauché en 2016).
-- Constat  : surtout d'anciens agents partis, sans effet sur l'effectif actuel.
--            Un seul agent actif n'a aucune situation et manque à l'effectif.
-- -----------------------------------------------------------------------------
SELECT etat, type_engagement, COUNT(*) AS agents,
       MIN(an_embauche) AS embauche_min, MAX(an_embauche) AS embauche_max
FROM (SELECT NVL(p.ETAT_EMPLOYE, '-') AS etat,
             EXTRACT(YEAR FROM p.DATE_EMBAUCHE) AS an_embauche,
             CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION d
                                WHERE d.CDEC_CODE = '50' AND d.PERS_ID = p.PERS_ID)
                  THEN 'temporaire (50)' ELSE 'autre' END AS type_engagement
        FROM USER_GRH.GRH_PERSONNE p
       WHERE NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_SITUATION s
                          WHERE s.PERS_ID = p.PERS_ID AND s.DATE_DEBUT <= SYSDATE))
GROUP BY etat, type_engagement
ORDER BY agents DESC;


-- -----------------------------------------------------------------------------
-- Q30 — Que sont devenus les agents engagés comme permanents (51) en 2019 ?
-- Objectif : expliquer l'anomalie « 84 engagés en 2019, 1 seul encore actif ».
-- Tableau  : T60 (recrutements 2019, sorties des années suivantes).
-- Résultat : 83 INACTIF au statut X (radiés), embauchés entre le 13/08/2019 et
--            le 13/09/2019, sans décision de sortie ni engagement temporaire ;
--            1 INACTIF au statut F (suspendu), embauché le 13/08/2019.
-- Constat  : les 84 engagés de 2019 ont tous été radiés, sans décision ni date.
--            Cela ressemble à une opération ponctuelle (contrat à durée
--            déterminée) saisie comme un engagement permanent. Question DRH.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.STATUT_EMPLOYE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
),
perm2019 AS (
  SELECT DISTINCT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE = '51'
    AND d.DATE_VIGUEUR >= DATE '2019-01-01' AND d.DATE_VIGUEUR < DATE '2020-01-01'
)
SELECT NVL(p.ETAT_EMPLOYE, '-') AS etat_agent,
       NVL(r.STATUT_EMPLOYE, '-') AS statut_derniere_situation,
       COUNT(*) AS agents,
       MIN(p.DATE_EMBAUCHE) AS embauche_min,
       MAX(p.DATE_EMBAUCHE) AS embauche_max,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION d2
                             WHERE d2.PERS_ID = p.PERS_ID AND d2.CDEC_CODE IN ('02','36','52','54'))
                THEN 1 ELSE 0 END) AS avec_decision_de_sortie,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION d3
                             WHERE d3.PERS_ID = p.PERS_ID AND d3.CDEC_CODE = '50')
                THEN 1 ELSE 0 END) AS avec_temporaire_50
FROM perm2019 x
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = x.PERS_ID
LEFT JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
GROUP BY NVL(p.ETAT_EMPLOYE, '-'), NVL(r.STATUT_EMPLOYE, '-')
ORDER BY agents DESC;


-- -----------------------------------------------------------------------------
-- Q31 — Année de mise à jour de la situation des partants (R, X, Y)
-- Objectif : tester si la date de mise à jour de la dernière situation peut
--            servir de date de sortie quand aucune décision n'existe (décès,
--            radiations). À comparer aux retraites par an de Q18.
-- Tableau  : T60 (décès et autres sorties).
-- Résultat : R (retraite) : 546 en 2020, 6 en 2024, 32 en 2025.
--            X (radié) : 297 en 2020, 1 en 2024, 42 en 2025, 28 en 2026,
--            23 sans date.
--            Y (décédé) : 404 en 2020, 4 en 2025, 1 en 2026, 1 sans date.
-- Constat  : une mise à jour de masse en 2020 a touché presque tous les
--            statuts de sortie (reprise de données ?). La date de mise à jour
--            ne peut pas servir de date de sortie.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.STATUT_EMPLOYE, s.DATE_UPDATE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT r.STATUT_EMPLOYE, EXTRACT(YEAR FROM r.DATE_UPDATE) AS annee_maj, COUNT(*) AS agents
FROM der r
WHERE r.rn = 1 AND r.STATUT_EMPLOYE IN ('R','X','Y')
GROUP BY r.STATUT_EMPLOYE, EXTRACT(YEAR FROM r.DATE_UPDATE)
ORDER BY r.STATUT_EMPLOYE, annee_maj;


-- -----------------------------------------------------------------------------
-- Q32 — Retraites : âge à la date de vigueur et délai depuis la création
-- Objectif : vérifier que la date de vigueur est bien la date de départ
--            (âges groupés autour de l'âge légal) et non la date de l'acte.
-- Tableau  : T60 (ligne retraites).
-- Résultat : premier passage en échec (ORA-00972, alias de 33 caractères),
--            relancée avec l'alias delai_moy_mois. 596 décisions depuis 2010 :
--            âge 55 (69 décisions, délai moyen 0,8 mois) · 56 (88 ; 2,5) ·
--            57 (116 ; 1,1) · 58 (117 ; 2,4) · 59 (95 ; 1,3) · 60 (105 ; 2,5) ;
--            cas isolés : 35 et 45 ans (1 chacun), 61 ans (2 ; 22 mois),
--            64 et 65 ans (1 chacun).
-- Constat  : 590 retraites sur 596 tombent entre 55 et 60 ans, et la décision
--            est prise 1 à 2,5 mois avant sa date de vigueur. La date de vigueur
--            de la décision 02 est donc bien la date de départ : la ligne
--            « Retraites » de T60 est fiable. Les 6 cas hors de 55–60 ans sont
--            à vérifier (retraite anticipée, date de naissance erronée,
--            régularisation tardive à 61 ans).
-- -----------------------------------------------------------------------------
SELECT FLOOR(MONTHS_BETWEEN(d.DATE_VIGUEUR, p.DATE_NAISS) / 12) AS age_a_la_vigueur,
       COUNT(*) AS decisions,
       ROUND(AVG(MONTHS_BETWEEN(d.DATE_VIGUEUR, d.DATE_CREATION)), 1) AS delai_moy_mois
FROM USER_GRH.GRH_DECISION d
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
WHERE d.CDEC_CODE = '02'
  AND d.DATE_VIGUEUR >= DATE '2010-01-01'
GROUP BY FLOOR(MONTHS_BETWEEN(d.DATE_VIGUEUR, p.DATE_NAISS) / 12)
ORDER BY age_a_la_vigueur;


-- -----------------------------------------------------------------------------
-- Q33 — Modifications (41) des retraites : changent-elles la date ?
-- Objectif : comprendre les 218 retraites modifiées (report, correction ?).
-- Tableau  : T60 (ligne retraites, année de rattachement).
-- Résultat : écart entre la vigueur de la modification et celle de la retraite
--            de −446 à +303 mois. Écarts les plus fréquents : 44 mois (13 cas),
--            67 (10), 1 (9), 39 (8), 73 (7), −376 (6), 34, 0 et 3 (6 chacun).
--            Toujours le même agent.
-- Constat  : la date de la modification n'a aucun lien régulier avec celle de
--            la retraite (jusqu'à 37 ans d'écart). On ne sait pas ce que la
--            décision 41 modifie. Question DRH.
-- -----------------------------------------------------------------------------
SELECT ROUND(MONTHS_BETWEEN(a.DATE_VIGUEUR, o.DATE_VIGUEUR)) AS ecart_mois,
       COUNT(*) AS nb,
       SUM(CASE WHEN a.PERS_ID = o.PERS_ID THEN 1 ELSE 0 END) AS meme_agent
FROM USER_GRH.GRH_DECISION a
JOIN USER_GRH.GRH_DECISION o ON TO_CHAR(o.DECI_NO) = TO_CHAR(a.NO_DECI_MODIF)
WHERE a.CDEC_CODE = '41' AND o.CDEC_CODE = '02'
GROUP BY ROUND(MONTHS_BETWEEN(a.DATE_VIGUEUR, o.DATE_VIGUEUR))
ORDER BY nb DESC;


-- -----------------------------------------------------------------------------
-- Q34 — Mouvements E et N : ont-ils été ressaisis en O ?
-- Objectif : si un E est presque toujours doublé par un O proche, E est un
--            brouillon remplacé ; sinon ce sont des mouvements jamais validés.
-- Tableau  : T60 (détail CNSS des mouvements).
-- Résultat : E : AFF 994 (130 ressaisis en O), NOM 334 (8), MUT 271 (4),
--            REM 12 (0), DET 5 (0), DIS 3 (1), sans code 2 (0) ; saisis de
--            2004 à 2026.
--            N : AFF 211 (18), NOM 113 (3), MUT 80 (0), sans code 29 (0),
--            REM 11 (0), DIS 10 (1), DET 2 (0).
-- Constat  : 9 % seulement des mouvements E ont été ressaisis en O. Ce ne sont
--            pas des brouillons remplacés, mais des mouvements jamais validés.
-- -----------------------------------------------------------------------------
SELECT e.VALIDE, UPPER(TRIM(e.NATURE_MVT)) AS code, COUNT(*) AS nb,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_MOUVEMENT o
                             WHERE o.VALIDE = 'O' AND o.PERS_ID = e.PERS_ID
                               AND UPPER(TRIM(o.NATURE_MVT)) = UPPER(TRIM(e.NATURE_MVT))
                               AND ABS(o.DATE_DEBUT - e.DATE_DEBUT) <= 90)
                THEN 1 ELSE 0 END) AS avec_un_o_proche,
       MIN(EXTRACT(YEAR FROM e.DATE_SAISIE)) AS saisie_an_min,
       MAX(EXTRACT(YEAR FROM e.DATE_SAISIE)) AS saisie_an_max
FROM USER_GRH.GRH_MOUVEMENT e
WHERE e.VALIDE IN ('E','N')
GROUP BY e.VALIDE, UPPER(TRIM(e.NATURE_MVT))
ORDER BY e.VALIDE, nb DESC;


-- -----------------------------------------------------------------------------
-- Q35 — Fraîcheur de la base
-- Objectif : savoir si la base consultée est une copie figée (aucun mouvement
--            validé après le 19/01/2026) ou si la saisie s'est arrêtée.
-- Résultat : dernière insertion / dernière mise à jour :
--            GRH_MOUVEMENT 02/02/2026 / 02/02/2026 ; GRH_SITUATION 23/01/2026 /
--            02/02/2026 ; GRH_PERSONNE 20/01/2026 / 18/02/2026 ; GRH_DECISION
--            02/02/2026 / 18/02/2026.
--            Mouvements saisis par mois : en janvier 2026, 97 E, 119 N et 7 O ;
--            dernières saisies en février 2026 (3 E, 30 N).
-- Constat  : aucune écriture après le 18/02/2026 : la base consultée est une
--            copie figée à cette date. Les clichés DTM de juin à octobre 2026
--            ont exactement le même volume (Q39). La validation des mouvements
--            a presque cessé fin 2025 – début 2026.
-- -----------------------------------------------------------------------------
SELECT 'GRH_MOUVEMENT' AS table_source, MAX(DATE_INSERT) AS derniere_insertion, MAX(DATE_UPDATE) AS derniere_maj FROM USER_GRH.GRH_MOUVEMENT
UNION ALL SELECT 'GRH_SITUATION', MAX(DATE_INSERT), MAX(DATE_UPDATE) FROM USER_GRH.GRH_SITUATION
UNION ALL SELECT 'GRH_PERSONNE',  MAX(DATE_INSERT), MAX(DATE_UPDATE) FROM USER_GRH.GRH_PERSONNE
UNION ALL SELECT 'GRH_DECISION',  MAX(DATE_INSERT), MAX(DATE_UPDATE) FROM USER_GRH.GRH_DECISION;

SELECT TO_CHAR(DATE_SAISIE, 'YYYY-MM') AS mois_saisie, VALIDE, COUNT(*) AS nb
FROM USER_GRH.GRH_MOUVEMENT
WHERE DATE_SAISIE >= DATE '2025-01-01'
GROUP BY TO_CHAR(DATE_SAISIE, 'YYYY-MM'), VALIDE
ORDER BY mois_saisie, VALIDE;


-- -----------------------------------------------------------------------------
-- Q36 — Qualifications et encadrement (T58, T65)
-- Objectif : connaître les vrais codes de qualification. Le DTM du taux
--            d'encadrement utilise HC, 04, 05, 06 (encadrants) et 01, 02, 03,
--            alors que la doc DataMart parle de S et M : laquelle est juste ?
--            Chercher aussi un champ « encadrement » ailleurs (fonction).
-- Résultat : référentiel : HC cadre supérieur (niveau S), 01 et 02 agent
--            d'exécution (A), 03 agent de maîtrise (I), 04 cadre moyen (M),
--            05 et 06 cadre supérieur (S).
--            Actifs : HC 79, 01 16, 02 214, 03 282, 04 229, 05 29, 06 29 (878).
--            GRH_TYPE_ENCADREMENT (ENCADRE, NENCADRE) n'est référencé par
--            aucune colonne.
--            Type de fonction des actifs : P 630, R 243, p 5.
-- Constat  : les codes du DTM et ceux de la doc désignent les mêmes agents :
--            le niveau S ou M correspond à HC, 04, 05 et 06. Encadrants par
--            qualification : 366 actifs sur 878, soit 41,7 % (le DTM publie
--            31,1 % car son dénominateur compte les partants). Le type de
--            fonction R (243, soit 27,7 %) pourrait être une autre définition ;
--            le sens de P et R est inconnu, et « p » minuscule est une erreur
--            de saisie.
-- -----------------------------------------------------------------------------
SELECT QUAL_CODE, NOM AS libelle, NIVEAU FROM USER_GRH.GRH_QUALIFICATION ORDER BY QUAL_CODE;

WITH der AS (
  SELECT s.PERS_ID, s.QUAL_CODE, s.FNCT_CODE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT r.QUAL_CODE, q.NOM AS libelle, q.NIVEAU, COUNT(*) AS agents_actifs
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = r.QUAL_CODE
WHERE r.rn = 1
GROUP BY r.QUAL_CODE, q.NOM, q.NIVEAU
ORDER BY r.QUAL_CODE;

SELECT table_name, column_name FROM ALL_TAB_COLUMNS
WHERE owner = 'USER_GRH' AND column_name LIKE '%ENCADR%'
ORDER BY table_name;

SELECT * FROM USER_GRH.GRH_TYPE_ENCADREMENT;

WITH der AS (
  SELECT s.PERS_ID, s.FNCT_CODE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT f.FNCT_TYPE, COUNT(*) AS agents_actifs
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
LEFT JOIN USER_GRH.GRH_FONCTION f ON f.FNCT_CODE = r.FNCT_CODE
WHERE r.rn = 1
GROUP BY f.FNCT_TYPE
ORDER BY agents_actifs DESC;


-- -----------------------------------------------------------------------------
-- Q37 — Domaines d'activité des agents actifs (T59)
-- Objectif : vérifier que chaque actif a un domaine d'activité connu et une
--            unité administrative existante.
-- Résultat : référentiel de 9 domaines (IMM, REC, CONT, PRS, PREV, ACT, CPT,
--            PIL, AUT). Actifs : PIL 431, PRS 158, REC 125, CPT 79, ACT 50,
--            IMM 29, PREV 5, CONT 1 ; aucune unité administrative introuvable.
-- Constat  : chaque actif a un domaine connu. Mais le pilotage (PIL) regroupe
--            49 % des agents et le contentieux (CONT) un seul : à faire
--            confirmer par la DRH (valeur par défaut ?).
-- -----------------------------------------------------------------------------
SELECT CODE_DOMAINE, LIB_DOMAINE, RANG_DOMAINE FROM USER_GRH.GRH_DOMAINE_ACTIVITE ORDER BY RANG_DOMAINE;

WITH der AS (
  SELECT s.PERS_ID, s.CODE_DOMAINE_ACT, s.UA_CODE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT r.CODE_DOMAINE_ACT, da.LIB_DOMAINE, COUNT(*) AS agents_actifs,
       SUM(CASE WHEN u.UA_CODE IS NULL THEN 1 ELSE 0 END) AS ua_introuvable
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
LEFT JOIN USER_GRH.GRH_DOMAINE_ACTIVITE da ON da.CODE_DOMAINE = r.CODE_DOMAINE_ACT
LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u ON u.UA_CODE = r.UA_CODE
WHERE r.rn = 1
GROUP BY r.CODE_DOMAINE_ACT, da.LIB_DOMAINE
ORDER BY agents_actifs DESC;


-- -----------------------------------------------------------------------------
-- Q38 — Âge et ancienneté des agents actifs (T58, T61)
-- Objectif : vérifier les dates de naissance et choisir la date d'ancienneté
--            (embauche ou prise de service : question ouverte à la direction).
-- Résultat : premier passage en échec (ORA-00972, alias de 33 caractères).
--            Alias raccourci en emb_diff_prise_service. Plus besoin de la
--            relancer : Q46 (âges) et Q48 (embauche ou prise de service) du
--            lot 8 répondent à la question.
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS actifs,
       SUM(CASE WHEN p.DATE_NAISS IS NULL THEN 1 ELSE 0 END) AS sans_naissance,
       SUM(CASE WHEN MONTHS_BETWEEN(SYSDATE, p.DATE_NAISS) / 12 NOT BETWEEN 16 AND 70 THEN 1 ELSE 0 END) AS age_hors_16_70,
       SUM(CASE WHEN p.DATE_PRISE_SERVICE IS NULL THEN 1 ELSE 0 END) AS sans_prise_service,
       SUM(CASE WHEN p.DATE_PRISE_SERVICE <> p.DATE_EMBAUCHE THEN 1 ELSE 0 END) AS emb_diff_prise_service,
       SUM(CASE WHEN p.DATE_PRISE_SERVICE < p.DATE_EMBAUCHE THEN 1 ELSE 0 END) AS prise_service_avant_embauche,
       SUM(CASE WHEN p.DATE_EMBAUCHE > SYSDATE THEN 1 ELSE 0 END) AS embauche_future
FROM USER_GRH.GRH_PERSONNE p
WHERE p.ETAT_EMPLOYE = 'ACTIF';


-- -----------------------------------------------------------------------------
-- Q39 — Ce que publie aujourd'hui le DTM (schéma DTM, pas USER_GRH)
-- Objectif : comparer l'effectif et le taux d'encadrement publiés dans OBIEE
--            avec les 879 actifs de la source.
-- :cliche = cliché au format MMAAAA (ex. '102026'), donné par la 1re requête.
-- Résultat : clichés 062026 à 102026 : 32 950 lignes chacun.
--            DTM_GRH_EFFECTIF (somme NB_AGENTS / NB_AGENTS_ACTIFS) :
--            2019 1 794 / 976 · 2020 1 907 / 980 · 2021 1 806 / 847 ·
--            2022 2 047 / 1 054 · 2023 1 898 / 1 014 · 2024 1 975 / 985 ·
--            2025 2 118 / 1 104 · 2026 2 149 / 1 097.
--            DTM_GRH_RATIO (NB_AGENTS_TOTAL, encadrants, taux) :
--            2019 1 351, 370, 27,4 % · 2024 1 457, 444, 30,5 % ·
--            2025 1 636, 509, 31,1 % · 2026 1 640, 509, 31,0 %.
-- Constat  : - le DTM publie 2 118 agents et 1 104 actifs pour 2025, et
--              1 636 actifs dans le ratio, contre 879 actifs réels ;
--            - les deux tables du DTM ne donnent pas le même nombre d'actifs ;
--            - le ratio reproduit la règle testée en Q24 (1 638 en 2025) ;
--            - le DTM publie une année 2026 au 31/12/2026, une date future ;
--            - l'effectif publié monte et descend d'une année à l'autre sans
--              raison visible (hypothèse : tranches d'âge, voir lot 7, Q40) ;
--            - les cinq clichés sont identiques, ce qui confirme une source figée.
-- -----------------------------------------------------------------------------
SELECT CLICHE, COUNT(*) AS lignes FROM DTM.DTM_GRH_EFFECTIF GROUP BY CLICHE ORDER BY CLICHE;

SELECT ID_TEMPS, SUM(NB_AGENTS) AS somme_nb_agents, SUM(NB_AGENTS_ACTIFS) AS somme_nb_actifs
FROM DTM.DTM_GRH_EFFECTIF
WHERE CLICHE = :cliche AND ID_TEMPS >= 20190101
GROUP BY ID_TEMPS
ORDER BY ID_TEMPS;

SELECT ID_TEMPS, NB_AGENTS_TOTAL, NB_ENCADRANTS, NB_NON_ENCADRANTS, NB_QUAL_INCONNU, TAUX_ENCADREMENT
FROM DTM.DTM_GRH_RATIO
WHERE CLICHE = :cliche AND ID_TEMPS >= 20190101
ORDER BY ID_TEMPS;
