-- =============================================================================
-- LOT 7 — Dater les sorties et encadrer l'effectif réel
-- Rédigé et exécuté le 21/10/2026 sur USER_GRH (Q40 sur le schéma DTM).
-- À relancer aussi : Q38 du lot 6 (alias corrigé).
-- Exécution : en mode script (F5) ; tout est en SELECT et ne renvoie que des
-- comptages (aucune donnée nominative). Alias limités à 30 caractères.
-- Tableaux CIPRES visés : T60 (sorties, effectif au 1er janvier et au 31/12)
-- et T58, T59, T61, T65 (effectif sur cinq exercices).
-- Point de départ : environ 1 380 agents partis n'ont ni décision de sortie
-- ni date de décès (lot 6, Q24). Ce lot cherche d'autres dates de sortie
-- (décisions 34 et 35) et mesure ce qui restera sans date.
-- Conclusions du lot :
--   - le DTM perd des agents à cause des bornes de ses tranches (Q40) ;
--   - l'indemnité de fin de contrat (35) date la fin de la moitié des
--     temporaires, la liquidation des droits (34) date les décès (Q41, Q42) ;
--   - aucune date de décès n'est saisie depuis 2010 (Q42) ;
--   - 817 départs restent sans aucune date : l'effectif des années passées ne
--     peut pas être reconstitué à partir des données (Q44, Q45).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q40 — Tranches d'âge et d'ancienneté du DTM, effectif publié par tranche
-- Objectif : expliquer pourquoi l'effectif publié (2 118 en 2025) est plus bas
--            que la règle testée (2 845) et pourquoi il monte et descend : des
--            agents partis depuis longtemps sortent-ils des tranches d'âge ?
-- Schéma   : DTM. :cliche = '102026' (dernier cliché).
-- Résultat : tranches d'âge : < 25 (0 à 24), 25-29, 30-34, 35-39, 40-44,
--            45-49, 50-54, 55 et plus (sans borne haute).
--            Tranches d'ancienneté : < 1 (0 à 0), 1-4, 5-9, 10-14, 15-19,
--            20-24, 25-29, 30 et plus. Les bornes hautes sont INCLUSES.
--            Effectif publié au 31/12/2025 (agents / actifs) : < 25 5 / 3 ·
--            25-29 32 / 22 · 30-34 93 / 43 · 35-39 139 / 78 · 40-44 194 / 136 ·
--            45-49 206 / 173 · 50-54 186 / 165 · 55 et plus 1 263 / 484.
-- Constat  : - la tranche « 55 et plus » porte 1 263 des 2 118 agents publiés :
--              c'est là que s'accumulent les partants ;
--            - la requête DTM_GRH_EFFECTIF compare l'âge à la borne haute avec
--              « < » alors que cette borne est incluse. Les agents de 24, 29,
--              34, 39, 44, 49 et 54 ans disparaissent, ainsi que ceux de 0, 4,
--              9, 14, 19, 24 et 29 ans d'ancienneté. La tranche « moins de
--              1 an » (0 à 0) ne reçoit jamais personne : tous les recrutés de
--              l'année sont exclus. C'est ce qui explique l'écart avec 2 845 et
--              les variations d'une année à l'autre (à confirmer par Q58).
-- -----------------------------------------------------------------------------
SELECT * FROM DTM.DIM_GRH_TRANCHE_AGE ORDER BY AGE_MIN;

SELECT * FROM DTM.DIM_GRH_TRANCHE_ANCIENNETE ORDER BY ANC_MIN;

SELECT TRANCHE_AGE_CODE, SUM(NB_AGENTS) AS nb_agents, SUM(NB_AGENTS_ACTIFS) AS nb_actifs
FROM DTM.DTM_GRH_EFFECTIF
WHERE CLICHE = :cliche AND ID_TEMPS = 20251231
GROUP BY TRANCHE_AGE_CODE
ORDER BY TRANCHE_AGE_CODE;


-- -----------------------------------------------------------------------------
-- Q41 — Fin des engagements temporaires : la décision 35 la date-t-elle ?
-- Objectif : savoir si l'indemnité de fin de contrat (35) suit le dernier
--            engagement temporaire (50), et peut donc dater la sortie.
-- Tableau  : T60 (fin de contrat, autres sorties).
-- Résultat : INACTIF : 560 temporaires, 286 avec une indemnité 35, dont 260
--            après le dernier engagement, en moyenne 9,1 mois après ; derniers
--            engagements de 2003 à 2026.
--            État vide : 46 temporaires, 2 avec indemnité (5,4 mois après).
--            ACTIF : 6 temporaires, 4 avec une indemnité antérieure à leur
--            engagement actuel (un ancien contrat).
-- Constat  : l'indemnité de fin de contrat date la fin d'environ la moitié des
--            temporaires partis (260 sur 560), avec un décalage d'environ
--            9 mois. L'autre moitié n'a aucune date de fin.
-- -----------------------------------------------------------------------------
SELECT NVL(p.ETAT_EMPLOYE, '-') AS etat_agent,
       COUNT(*) AS temporaires,
       SUM(CASE WHEN t.dt_35 IS NOT NULL THEN 1 ELSE 0 END) AS avec_indemnite_35,
       SUM(CASE WHEN t.dt_35 >= t.dt_dernier_50 THEN 1 ELSE 0 END) AS indemnite_apres_dernier_50,
       ROUND(AVG(MONTHS_BETWEEN(t.dt_35, t.dt_dernier_50)), 1) AS ecart_moy_mois,
       MIN(EXTRACT(YEAR FROM t.dt_dernier_50)) AS dernier_50_an_min,
       MAX(EXTRACT(YEAR FROM t.dt_dernier_50)) AS dernier_50_an_max
FROM (SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
             MAX(CASE WHEN d.CDEC_CODE = '50' THEN d.DATE_VIGUEUR END) AS dt_dernier_50,
             MAX(CASE WHEN d.CDEC_CODE = '35' THEN d.DATE_VIGUEUR END) AS dt_35
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
       WHERE d.CDEC_CODE IN ('35','50')
       GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
      HAVING MAX(CASE WHEN d.CDEC_CODE = '50' THEN 1 ELSE 0 END) = 1) t
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = t.PERS_ID
GROUP BY NVL(p.ETAT_EMPLOYE, '-')
ORDER BY temporaires DESC;


-- -----------------------------------------------------------------------------
-- Q42 — Décès : la liquidation des droits (34) date-t-elle le décès ?
--       Et combien de décès surviennent après la retraite ?
-- Objectif : (1) pour les 59 décès datés, mesurer l'écart entre la date de
--            décès et la liquidation (34), et repérer les décès après une
--            retraite, qui ne sont pas une sortie du personnel ;
--            (2) pour les décédés sans date, voir l'année de leur liquidation.
-- Tableau  : T60 (ligne décès).
-- Résultat : (1) décès en activité : 58 agents, 45 avec une liquidation,
--            en moyenne 8,5 mois après le décès ; décès datés de 1984 à 2010.
--            Décès après retraite : 1 (2003).
--            (2) 93 décédés sans date ont une liquidation : 22 en 2003, puis
--            1 à 6 par an ; de 2019 à 2024 : 2, 3, 2, 4, 3 et 5.
-- Constat  : aucune date de décès n'est saisie après 2010. La liquidation des
--            droits suit le décès d'environ 8,5 mois et peut servir de date
--            approchée pour la ligne « Décès » de T60 (2 à 5 par an depuis
--            2019). À faire valider par la DRH.
-- -----------------------------------------------------------------------------
SELECT CASE WHEN r.dt_02 IS NOT NULL AND r.dt_02 <= p.DATE_DECES THEN 'deces apres retraite'
            ELSE 'deces en activite' END AS cas,
       COUNT(*) AS agents,
       SUM(CASE WHEN r.dt_34 IS NOT NULL THEN 1 ELSE 0 END) AS avec_liquidation_34,
       ROUND(AVG(MONTHS_BETWEEN(r.dt_34, p.DATE_DECES)), 1) AS ecart_moy_mois_34,
       MIN(EXTRACT(YEAR FROM p.DATE_DECES)) AS deces_an_min,
       MAX(EXTRACT(YEAR FROM p.DATE_DECES)) AS deces_an_max
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN (SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
                  MAX(CASE WHEN d.CDEC_CODE = '02' THEN d.DATE_VIGUEUR END) AS dt_02,
                  MIN(CASE WHEN d.CDEC_CODE = '34' THEN d.DATE_VIGUEUR END) AS dt_34
             FROM USER_GRH.GRH_DECISION d
             LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
            WHERE d.CDEC_CODE IN ('02','34')
            GROUP BY NVL(d.PERS_ID, dp.PERS_ID)) r ON r.PERS_ID = p.PERS_ID
WHERE p.DATE_DECES IS NOT NULL
GROUP BY CASE WHEN r.dt_02 IS NOT NULL AND r.dt_02 <= p.DATE_DECES THEN 'deces apres retraite'
              ELSE 'deces en activite' END;

WITH der AS (
  SELECT s.PERS_ID, s.STATUT_EMPLOYE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
),
liq AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, MIN(d.DATE_VIGUEUR) AS dt_34
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE = '34'
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
)
SELECT EXTRACT(YEAR FROM l.dt_34) AS annee_liquidation, COUNT(*) AS decedes_sans_date
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID
JOIN liq l ON l.PERS_ID = r.PERS_ID
WHERE r.rn = 1 AND r.STATUT_EMPLOYE = 'Y' AND p.DATE_DECES IS NULL
GROUP BY EXTRACT(YEAR FROM l.dt_34)
ORDER BY annee_liquidation;


-- -----------------------------------------------------------------------------
-- Q43 — Listes de codes encore inconnues (type de fonction, encadrement…)
-- Objectif : trouver le sens de FNCT_TYPE (P, R) pour la définition des
--            encadrants (T65), et voir toutes les listes de codes disponibles.
-- Résultat : aucun domaine pour FNCT_TYPE. QUALIFICATION : A agents
--            d'exécution, I agents de maîtrise, M cadre moyen, S cadre
--            supérieur. QUALITE_EMPLOYE : A agent CNSS, B agent de la fonction
--            publique, C agent affecté. TYPE_ELEMENT : 1 rémunération propre,
--            2 logement, 3 transport, 4 fonction, 9 inconnu.
--            146 listes de codes, dont LOV_DEPART (7 codes), MOTIF_DEPART (4),
--            CODE_ALLOC_DEPART (5), CATEGORIE_ELEMENT (7), CLASSE_ELEMENT (9),
--            TRANCHE_ANCIENNETE (7, contre 8 tranches dans le DTM).
--            Fonctions des actifs : type P = postes ordinaires (agent
--            administratif 100, chauffeur 61, secrétaire 56…) ; type R = postes
--            pourvus par nomination : chef de section 79, chef de service 68,
--            caissier 21, secrétaire de direction 19, directeur de service 13,
--            responsable de guichet 10, chef d'agence 8, directeur régional 5,
--            chef de division 4, gardien concierge 3, chauffeurs de direction 5,
--            etc. Type « p » : 5 assistants en sécurité sociale (faute de frappe).
-- Constat  : - les catégories CIPRES de T58 se lisent directement dans le
--              niveau de qualification (A, I, M, S) ;
--            - le type R n'est pas l'encadrement : il mêle chefs et directeurs,
--              caissiers, secrétaires, gardiens et chauffeurs. Les seuls postes
--              de commandement (chefs de section, de service, de division,
--              d'agence, directeurs, DG, SG) comptent 179 actifs, soit 20,4 % ;
--            - la qualité de l'agent (CNSS, fonction publique, affecté) peut
--              changer le périmètre de l'effectif CIPRES (voir Q60) ;
--            - des listes de motifs de départ existent (LOV_DEPART,
--              MOTIF_DEPART) : à rechercher (Q59, Q61) ;
--            - des éléments de paie existent (TYPE_ELEMENT…) : une masse
--              salariale est peut-être calculable dans USER_GRH (Q55, Q61).
-- -----------------------------------------------------------------------------
SELECT RV_DOMAIN, RV_LOW_VALUE, RV_MEANING
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN LIKE '%FNCT%' OR RV_DOMAIN LIKE '%FONCT%' OR RV_DOMAIN LIKE '%TYPE%'
   OR RV_DOMAIN LIKE '%ENCAD%' OR RV_DOMAIN LIKE '%QUAL%' OR RV_DOMAIN LIKE '%NIVEAU%'
ORDER BY RV_DOMAIN, RV_LOW_VALUE;

SELECT RV_DOMAIN, COUNT(*) AS nb_codes
FROM USER_GRH.GRH_REF_CODES
GROUP BY RV_DOMAIN
ORDER BY RV_DOMAIN;

WITH der AS (
  SELECT s.PERS_ID, s.FNCT_CODE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT f.FNCT_TYPE, f.NOM AS fonction, COUNT(*) AS agents_actifs
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
LEFT JOIN USER_GRH.GRH_FONCTION f ON f.FNCT_CODE = r.FNCT_CODE
WHERE r.rn = 1
GROUP BY f.FNCT_TYPE, f.NOM
ORDER BY f.FNCT_TYPE, agents_actifs DESC;


-- -----------------------------------------------------------------------------
-- Q44 — Agents inactifs : combien ont une date de sortie exploitable ?
-- Objectif : mesurer, par statut, la part des départs qu'on peut dater :
--            d'abord par une décision de sortie (02, 36, 52, 54), sinon par la
--            date de décès, sinon par l'indemnité de fin de contrat (35),
--            sinon par la liquidation des droits (34). Le reste n'a aucune date.
-- Tableau  : T60 (sorties) et historique de l'effectif (T58 à T65).
-- Résultat : (statut · inactifs · décision · décès · fin de contrat 35 ·
--            liquidation 34 · sans date)
--            A · 691 · 184 · 25 · 225 ·  56 · 201
--            R · 584 · 569 ·  0 ·   0 ·   5 ·  10
--            Y · 409 ·  64 · 31 ·  23 ·  91 · 200
--            X · 387 ·  30 ·  0 ·  84 ·   1 · 272
--            - · 130 ·   8 ·  0 ·   9 ·   2 · 111
--            E 14, F 9, B 5, D 4, H 1 : 23 sans date au total.
--            Total : 2 234 inactifs ; 859 datés par une décision, 57 par le
--            décès, 345 par la fin de contrat, 156 par la liquidation ;
--            817 sans aucune date.
-- Constat  : 63 % des départs peuvent être datés. Les 817 autres (37 %) sont
--            surtout des radiés (272), des inactifs restés au statut A (201),
--            des décédés (200) et d'anciens agents sans situation (111).
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.STATUT_EMPLOYE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
),
sortie AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
         MAX(CASE WHEN d.CDEC_CODE IN ('02','36','52','54') THEN 1 ELSE 0 END) AS f_dec,
         MAX(CASE WHEN d.CDEC_CODE = '35' THEN 1 ELSE 0 END) AS f_35,
         MAX(CASE WHEN d.CDEC_CODE = '34' THEN 1 ELSE 0 END) AS f_34
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE IN ('02','34','35','36','52','54')
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
)
SELECT NVL(r.STATUT_EMPLOYE, '-') AS statut,
       COUNT(*) AS inactifs,
       SUM(CASE WHEN NVL(x.f_dec, 0) = 1 THEN 1 ELSE 0 END) AS sortie_par_decision,
       SUM(CASE WHEN NVL(x.f_dec, 0) = 0 AND p.DATE_DECES IS NOT NULL THEN 1 ELSE 0 END) AS sinon_date_deces,
       SUM(CASE WHEN NVL(x.f_dec, 0) = 0 AND p.DATE_DECES IS NULL
                 AND NVL(x.f_35, 0) = 1 THEN 1 ELSE 0 END) AS sinon_fin_contrat_35,
       SUM(CASE WHEN NVL(x.f_dec, 0) = 0 AND p.DATE_DECES IS NULL
                 AND NVL(x.f_35, 0) = 0 AND NVL(x.f_34, 0) = 1 THEN 1 ELSE 0 END) AS sinon_liquidation_34,
       SUM(CASE WHEN NVL(x.f_dec, 0) = 0 AND p.DATE_DECES IS NULL
                 AND NVL(x.f_35, 0) = 0 AND NVL(x.f_34, 0) = 0 THEN 1 ELSE 0 END) AS sans_date_de_sortie
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
LEFT JOIN sortie x ON x.PERS_ID = p.PERS_ID
WHERE p.ETAT_EMPLOYE = 'INACTIF'
GROUP BY NVL(r.STATUT_EMPLOYE, '-')
ORDER BY inactifs DESC;


-- -----------------------------------------------------------------------------
-- Q45 — Effectif réel au 31/12 de 2019 à 2025 : borne basse et borne haute
-- Objectif : encadrer l'effectif réel des cinq derniers exercices, faute de
--            date de sortie pour tous :
--            - borne basse = embauchés avant le 31/12 et encore actifs
--              aujourd'hui (on oublie ceux qui sont partis depuis) ;
--            - borne haute = embauchés avant le 31/12, sans décès ni sortie
--              datée (02, 34, 35, 36, 52, 54) avant cette date.
--            L'effectif réel est entre les deux. À comparer avec le DTM (Q39).
-- Tableaux : T58, T59, T61, T65 ; effectif au 1er janvier et au 31/12 de T60.
-- Résultat : (année · borne basse · borne haute)
--            2019 · 621 · 1 613 ;  2020 · 622 · 1 560 ;  2021 · 622 · 1 528 ;
--            2022 · 622 · 1 502 ;  2023 · 624 · 1 476 ;  2024 · 726 · 1 575 ;
--            2025 · 879 · 1 751.
-- Constat  : en 2025, l'effectif réel est connu (879), mais la borne haute
--            atteint 1 751 : l'écart vient des 817 départs sans date. Pour les
--            années passées, l'effectif est entre 621 et 1 613 en 2019, une
--            fourchette trop large pour être publiée. Il ne peut pas être
--            reconstitué à partir des données : il faut un chiffre officiel de
--            la DRH par exercice, ou un registre des sorties.
-- -----------------------------------------------------------------------------
WITH ans AS (
  SELECT 2018 + LEVEL AS annee,
         TO_DATE(TO_CHAR(2018 + LEVEL) || '1231', 'YYYYMMDD') AS dt
  FROM dual CONNECT BY LEVEL <= 7
),
sortie AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, MIN(d.DATE_VIGUEUR) AS dt_sortie
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE IN ('02','34','35','36','52','54')
  GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
)
SELECT a.annee,
       (SELECT COUNT(*)
          FROM USER_GRH.GRH_PERSONNE p
         WHERE p.DATE_EMBAUCHE <= a.dt
           AND p.ETAT_EMPLOYE = 'ACTIF') AS borne_basse_actifs_auj,
       (SELECT COUNT(*)
          FROM USER_GRH.GRH_PERSONNE p
         WHERE p.DATE_EMBAUCHE <= a.dt
           AND (p.DATE_DECES IS NULL OR p.DATE_DECES > a.dt)
           AND NOT EXISTS (SELECT 1 FROM sortie x
                            WHERE x.PERS_ID = p.PERS_ID AND x.dt_sortie <= a.dt)) AS borne_haute_estimee
FROM ans a
ORDER BY a.annee;
