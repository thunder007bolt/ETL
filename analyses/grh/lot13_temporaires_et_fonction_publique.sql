-- =============================================================================
-- LOT 13 — Agents temporaires et agents de la fonction publique
-- Rédigé le 24/10/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) ; tout est en SELECT et ne renvoie que des
-- comptages (aucune donnée nominative). Alias limités à 30 caractères, sans
-- accents.
-- Pourquoi ce lot : préciser, pour le glossaire du questionnaire CIPRES, ce que
-- recouvrent « agent temporaire » et « agent de la fonction publique ».
-- Les chiffres déjà cités viennent de requêtes exécutées :
--   - codes QUALITE_EMPLOYE (A agent CNSS, B fonction publique, C affecté) et
--     NATURE_EMPLOIE (permanent, temporaire) : lot 7, Q43, et lot 8, Q59 ;
--   - 879 actifs dont 6 temporaires et 3 agents de la fonction publique :
--     lot 8, Q60 ;
--   - 387 radiés dont 233 temporaires : lot 6, Q28 ;
--   - indemnité de fin de contrat (35) environ 9 mois après le dernier
--     engagement temporaire (50) : lot 7, Q41.
-- Les requêtes ci-dessous complètent ces résultats.
-- Exécuté le 24/10/2026 : résultats reportés ci-dessous.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q85 — Sens des codes de qualité, de nature d'emploi et d'engagement
-- Objectif : relire les libellés officiels de l'application pour les agents
--            temporaires, détachés et de la fonction publique.
-- Tableaux : T58 à T61, T65 (périmètre de l'effectif), T60 (entrées).
-- Résultat (24/10/2026) : QUALITE_EMPLOYE A Agent CNSS, B Agent Fonction
--            Publique, C Agent Affecté ; NATURE_EMPLOIE Permanent, Temporaire ;
--            LOV_ENG_TEMPORAIRE 50 Eng Tempo (40 annulation, 41 modification) ;
--            LOV_ENG_DETACHES 37 Eng Détachés (40, 41).
-- Constat  : l'application range la décision 37 parmi les engagements : un
--            détaché accueilli est une entrée.
-- -----------------------------------------------------------------------------
SELECT RV_DOMAIN, RV_LOW_VALUE, RV_MEANING
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN IN ('QUALITE_EMPLOYE', 'NATURE_EMPLOIE',
                    'LOV_ENG_TEMPORAIRE', 'LOV_ENG_DETACHES')
ORDER BY RV_DOMAIN, RV_LOW_VALUE;


-- -----------------------------------------------------------------------------
-- Q86 — Tous les agents par qualité, nature d'emploi et état
-- Objectif : voir combien d'agents de la fonction publique (B) et d'agents
--            affectés (C) l'application a connus, et s'ils sont permanents ou
--            temporaires. Lot 8, Q60, ne portait que sur les actifs.
-- Tableaux : T58 à T61, T65.
-- Résultat (24/10/2026) : qualité · nature · état · agents (années d'embauche)
--            A · permanent · ACTIF 869 (1987-2025) ; INACTIF 1 281 ; vide 16
--            A · temporaire · ACTIF 6 (2014-2025) ; INACTIF 784 ; vide 47
--            A · sans situation : ACTIF 1, INACTIF 111, vide 3
--            B · permanent · ACTIF 3 (2017-2024) ; INACTIF 30 ; B sans situation 7
--            C · permanent · INACTIF 13 ; temporaire · INACTIF 8 (1979-2006)
--            sans qualité : 1 temporaire. Total 3 180.
-- Constat  : - 846 agents ont fini leur parcours comme temporaires, dont 6
--              actifs : les temporaires pèsent peu dans l'effectif, beaucoup
--              dans les sorties ;
--            - 40 agents de la fonction publique ont été connus, 3 sont actifs ;
--            - les 21 agents affectés (C) sont tous inactifs, embauchés avant
--              2007 : cette qualité n'est plus utilisée.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.NATURE_EMPLOIE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT NVL(TO_CHAR(p.QUALITE), '-') AS qualite,
       NVL(TO_CHAR(r.NATURE_EMPLOIE), '-') AS nature_emploi,
       NVL(p.ETAT_EMPLOYE, '-') AS etat_agent,
       COUNT(*) AS agents,
       MIN(EXTRACT(YEAR FROM p.DATE_EMBAUCHE)) AS embauche_an_min,
       MAX(EXTRACT(YEAR FROM p.DATE_EMBAUCHE)) AS embauche_an_max
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
GROUP BY NVL(TO_CHAR(p.QUALITE), '-'), NVL(TO_CHAR(r.NATURE_EMPLOIE), '-'),
         NVL(p.ETAT_EMPLOYE, '-')
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q87 — Contrats temporaires : combien d'engagements par agent ?
-- Objectif : savoir si un temporaire enchaîne plusieurs contrats (décisions
--            50 successives), et combien ont une indemnité de fin de contrat.
-- Tableaux : T60 (recrutements, fins de contrat).
-- Résultat (24/10/2026) : engagements 50 · état · agents · avec indemnité 35 ·
--            devenus permanents (51)
--            1 · INACTIF 237 · 93 · 0 ; ACTIF 2 · 0 · 0 ; vide 38 · 0 · 0
--            2 · INACTIF 117 · 53 · 0 ; vide 3 · 1 · 0
--            3 et plus · INACTIF 206 · 140 · 0 ; ACTIF 4 · 4 · 0 ; vide 5 · 1 · 0
--            Total : 612 temporaires engagés par décision 50, 292 avec une 35.
-- Constat  : - un temporaire sur trois enchaîne trois contrats ou plus, et 4
--              des 6 actifs : les engagements 50 sont souvent des
--              renouvellements, pas de nouveaux recrutements ;
--            - aucun temporaire n'est devenu permanent par une décision 51 ;
--            - 846 temporaires en Q86 contre 612 avec une décision 50 : 234
--              n'ont aucune décision d'engagement.
-- -----------------------------------------------------------------------------
WITH t AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
         SUM(CASE WHEN d.CDEC_CODE = '50' THEN 1 ELSE 0 END) AS nb_50,
         MAX(CASE WHEN d.CDEC_CODE = '35' THEN 1 ELSE 0 END) AS a_35,
         MAX(CASE WHEN d.CDEC_CODE = '51' THEN 1 ELSE 0 END) AS a_51
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE IN ('35', '50', '51')
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
  HAVING SUM(CASE WHEN d.CDEC_CODE = '50' THEN 1 ELSE 0 END) > 0
)
SELECT CASE WHEN t.nb_50 >= 3 THEN '3 et plus' ELSE TO_CHAR(t.nb_50) END AS engagements_50,
       NVL(p.ETAT_EMPLOYE, '-') AS etat_agent,
       COUNT(*) AS agents,
       SUM(t.a_35) AS avec_indemnite_35,
       SUM(t.a_51) AS devenus_permanents_51
FROM t
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = t.PERS_ID
GROUP BY CASE WHEN t.nb_50 >= 3 THEN '3 et plus' ELSE TO_CHAR(t.nb_50) END,
         NVL(p.ETAT_EMPLOYE, '-')
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q88 — Engagements permanents (51) et temporaires (50) par année
-- Objectif : chiffrer, année par année, ce que la question 13 du questionnaire
--            classe en « Recrutements » ou en « Autres entrées ».
-- Tableaux : T60.
-- Résultat (24/10/2026) : année · permanents (51) · temporaires (50)
--            2019 · 84 · 54 ; 2020 · 1 · 22 ; 2021 · 0 · 22 ; 2022 · 0 · 25 ;
--            2023 · 1 · 27 ; 2024 · 106 · 62 ; 2025 · 157 · 84.
-- Constat  : - les engagements permanents se font par vagues (2019, 2024,
--              2025) ; les 84 de 2019 sont les engagés radiés sans date ;
--            - 84 temporaires engagés en 2025, contre 45 nouvelles embauches
--              temporaires : la différence est faite de renouvellements. Compter
--              les décisions 50 surestimerait les entrées de T60 ; il faut
--              compter les nouvelles embauches (question 13).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM d.DATE_VIGUEUR) AS annee,
       COUNT(DISTINCT CASE WHEN d.CDEC_CODE = '51' THEN NVL(d.PERS_ID, dp.PERS_ID) END) AS permanents_51,
       COUNT(DISTINCT CASE WHEN d.CDEC_CODE = '50' THEN NVL(d.PERS_ID, dp.PERS_ID) END) AS temporaires_50
FROM USER_GRH.GRH_DECISION d
LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
WHERE d.CDEC_CODE IN ('50', '51')
  AND d.DATE_VIGUEUR >= DATE '2019-01-01'
  AND d.DATE_VIGUEUR <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM d.DATE_VIGUEUR)
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q89 — Agents de la fonction publique et affectés : décisions associées
-- Objectif : savoir si les agents B et C arrivent par un engagement de détachés
--            (37) ou une affectation (15), et s'ils ont une décision de départ.
-- Tableaux : T60 (autres entrées, autres sorties), T58 (périmètre).
-- Résultat (24/10/2026) : qualité · état · agents · engagement de détachés 37 ·
--            affectation 15 · engagement 50 ou 51 · décision de départ
--            B · ACTIF 3 · 1 · 2 · 0 · 0 ; B · INACTIF 37 · 5 · 3 · 2 · 17
--            C · INACTIF 21 · 0 · 0 · 0 · 0 ; sans qualité 1 · 0 · 0 · 1 · 0
-- Constat  : - les agents de la fonction publique arrivent par un détachement
--              (37) ou une affectation (15), presque jamais par un engagement :
--              dans T60, ce sont des « autres entrées » (question 14) ;
--            - moins de la moitié des partis ont une décision de départ (17 sur
--              37) ;
--            - les agents affectés (C) n'ont aucune décision.
-- -----------------------------------------------------------------------------
WITH deci AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
         MAX(CASE WHEN d.CDEC_CODE = '37' THEN 1 ELSE 0 END) AS a_37,
         MAX(CASE WHEN d.CDEC_CODE = '15' THEN 1 ELSE 0 END) AS a_15,
         MAX(CASE WHEN d.CDEC_CODE IN ('50', '51') THEN 1 ELSE 0 END) AS a_engagement,
         MAX(CASE WHEN d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54') THEN 1 ELSE 0 END) AS a_depart
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
)
SELECT NVL(TO_CHAR(p.QUALITE), '-') AS qualite,
       NVL(p.ETAT_EMPLOYE, '-') AS etat_agent,
       COUNT(*) AS agents,
       SUM(NVL(x.a_37, 0)) AS engagement_detaches_37,
       SUM(NVL(x.a_15, 0)) AS affectation_15,
       SUM(NVL(x.a_engagement, 0)) AS engagement_50_51,
       SUM(NVL(x.a_depart, 0)) AS decision_depart
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN deci x ON x.PERS_ID = p.PERS_ID
WHERE NVL(TO_CHAR(p.QUALITE), '-') <> 'A'
GROUP BY NVL(TO_CHAR(p.QUALITE), '-'), NVL(p.ETAT_EMPLOYE, '-')
ORDER BY 1, 2;
