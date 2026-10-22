-- =============================================================================
-- LOT 8 — Détail de l'effectif (T58, T59, T61), formation (T62, T64),
--          frais de personnel (T63, T64) et indicateurs publiés
-- Rédigé le 21/10/2026, complété après le lot 7 (Q58 à Q61), exécuté le même
-- jour sur USER_GRH (Q56 sur le schéma DTM).
-- Exécution : en mode script (F5) ; tout est en SELECT et ne renvoie que des
-- comptages ou des montants agrégés (aucune donnée nominative). Alias limités
-- à 30 caractères, sans accents.
-- Effectif de référence utilisé ici : agents à l'état ACTIF dans GRH_PERSONNE
-- (879), puisque la règle du DTM compte les partants (lot 6).
--
-- Anomalies vues dans le code du DTM avant exécution, et ce qu'en dit ce lot :
--   - DTM_GRH_FORMATION ne garde que les formations de statut 'T' : or toutes
--     les formations ont le statut 'N'. La table de formation du DTM est vide,
--     et T62 et T64 ne publient rien (Q50, Q56) ;
--   - les 7 frais sont additionnés avec « + » (un frais vide efface le coût) et
--     le budget est compté une fois par inscrit : à mesurer au lot 9, car
--     Q51 et Q52 filtraient aussi sur le statut 'T' ;
--   - le budget n'est retenu que s'il est saisi en chiffres : il l'est
--     toujours (Q53), la perte de 75,6 % annoncée par la doc ne se vérifie pas ;
--   - GRH_CAT_FORMATION, lue par l'ETL pour la dimension des catégories de
--     formation, n'existe pas dans USER_GRH (ORA-00942 sur Q49 et Q54) ;
--   - DTM_INDICATEURS_FINANCES_GRH publie un troisième taux d'encadrement :
--     13,6 % en 2025, contre 31,1 % dans DTM_GRH_RATIO (Q56) ;
--   - le défaut des tranches est confirmé à l'unité : 2 822 - 704 = 2 118 (Q58) ;
--   - aucune table de paie du personnel n'est disponible : les vues de paie de
--     USER_GRH sont toutes invalides (Q55, Q61).
-- Q51, Q52 et Q57 sont relancées sans filtre de statut au lot 9 ; Q54 aussi,
-- sans la table manquante. Q38 (lot 6) n'est plus utile : Q46 et Q48 couvrent
-- les âges et l'ancienneté.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q46 — Actifs par catégorie, sexe et âge (T58)
-- Objectif : produire la base de T58 sur les seuls actifs, et vérifier les
--            dates de naissance. Catégories CIPRES : niveau A = employés
--            (exécution), I = agents de maîtrise, M et S = cadres.
-- Résultat : (niveau · sexe · actifs · âge moyen · âge min · âge max)
--            A · F ·  67 · 44,0 · 20 · 55     A · M · 163 · 42,4 · 21 · 56
--            I · F · 153 · 42,9 · 21 · 58     I · M · 129 · 38,6 · 20 · 57
--            M · F · 131 · 44,6 · 24 · 59     M · M ·  98 · 44,0 · 23 · 59
--            S · F ·  52 · 44,5 · 24 · 59     S · M ·  85 · 42,8 · 25 · 59
--            sans niveau · F · 1 (l'agent sans situation).
--            Aucune date de naissance manquante, aucun actif de 60 ans ou plus.
--            Tranches d'âge (hommes / femmes) : 20-24 14 / 8 · 25-29 38 / 22 ·
--            30-34 71 / 48 · 35-39 93 / 63 · 40-44 65 / 70 · 45-49 84 / 69 ·
--            50-54 73 / 78 · 55-59 37 / 46.
-- Constat  : sur les seuls actifs, T58 se calcule sans difficulté : 230
--            employés, 282 agents de maîtrise, 366 cadres (229 moyens,
--            137 supérieurs), 475 hommes et 404 femmes. Les âges maximum par
--            catégorie (55 à 59 ans) concordent avec les âges de retraite (Q32).
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.QUAL_CODE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT NVL(q.NIVEAU, '-') AS niveau, p.SEXE,
       COUNT(*) AS actifs,
       SUM(CASE WHEN p.DATE_NAISS IS NULL THEN 1 ELSE 0 END) AS sans_naissance,
       ROUND(AVG(MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_NAISS) / 12), 1) AS age_moyen,
       FLOOR(MIN(MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_NAISS) / 12)) AS age_min,
       FLOOR(MAX(MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_NAISS) / 12)) AS age_max,
       SUM(CASE WHEN MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_NAISS) / 12 >= 60 THEN 1 ELSE 0 END) AS age_60_et_plus
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = r.QUAL_CODE
WHERE p.ETAT_EMPLOYE = 'ACTIF'
GROUP BY NVL(q.NIVEAU, '-'), p.SEXE
ORDER BY niveau, p.SEXE;

SELECT tranche_age,
       SUM(CASE WHEN SEXE = 'M' THEN 1 ELSE 0 END) AS hommes,
       SUM(CASE WHEN SEXE = 'F' THEN 1 ELSE 0 END) AS femmes
FROM (SELECT SEXE,
             CASE WHEN b IS NULL THEN 'inconnu'
                  ELSE LPAD(b, 2, '0') || '-' || LPAD(b + 4, 2, '0') END AS tranche_age
        FROM (SELECT p.SEXE, FLOOR(MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_NAISS) / 60) * 5 AS b
                FROM USER_GRH.GRH_PERSONNE p
               WHERE p.ETAT_EMPLOYE = 'ACTIF'))
GROUP BY tranche_age
ORDER BY tranche_age;


-- -----------------------------------------------------------------------------
-- Q47 — Actifs par domaine d'activité et sexe (T59), et par nature d'unité
-- Objectif : produire la base de T59, et comprendre pourquoi le pilotage (PIL)
--            regroupe 49 % des actifs (unités de direction ?).
-- Résultat : (domaine · hommes · femmes · total) PIL 266 · 165 · 431 ;
--            PRS 74 · 84 · 158 ; REC 64 · 61 · 125 ; CPT 40 · 39 · 79 ;
--            ACT 9 · 41 · 50 ; IMM 19 · 10 · 29 ; PREV 3 · 2 · 5 ; CONT 0 · 1 · 1.
--            PIL : 230 agents dans des services, 201 dans des directions.
--            Les autres domaines sont surtout rattachés à des services.
-- Constat  : T59 se calcule sans difficulté. Le pilotage contient la quasi-
--            totalité des agents rattachés à une direction (201) : il sert
--            peut-être de domaine par défaut pour les directions. À faire
--            confirmer par la DRH, comme le contentieux à un seul agent.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.CODE_DOMAINE_ACT,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT r.CODE_DOMAINE_ACT,
       SUM(CASE WHEN p.SEXE = 'M' THEN 1 ELSE 0 END) AS hommes,
       SUM(CASE WHEN p.SEXE = 'F' THEN 1 ELSE 0 END) AS femmes,
       COUNT(*) AS actifs
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
WHERE r.rn = 1
GROUP BY r.CODE_DOMAINE_ACT
ORDER BY actifs DESC;

WITH der AS (
  SELECT s.PERS_ID, s.CODE_DOMAINE_ACT, s.UA_CODE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT r.CODE_DOMAINE_ACT, u.UA_NATURE, COUNT(*) AS actifs
FROM der r
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = r.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u ON u.UA_CODE = r.UA_CODE
WHERE r.rn = 1
GROUP BY r.CODE_DOMAINE_ACT, u.UA_NATURE
ORDER BY r.CODE_DOMAINE_ACT, actifs DESC;


-- -----------------------------------------------------------------------------
-- Q48 — Ancienneté des actifs : date d'embauche ou date de prise de service ?
-- Objectif : mesurer l'effet du choix de la date de départ de l'ancienneté sur
--            les tranches de T61 (tranches du DTM : < 1 an, 1-4, 5-9 … 30 et +).
-- Résultat : (tranche · selon l'embauche · selon la prise de service)
--            < 1 an 153 · 154 ; 1-4 104 · 104 ; 5-9 88 · 89 ; 10-14 255 · 252 ;
--            15-19 78 · 87 ; 20-24 138 · 130 ; 25-29 34 · 34 ; 30 et + 29 · 29.
-- Constat  : le choix de la date ne change presque rien (9 agents au plus par
--            tranche). Les 153 agents de moins d'un an d'ancienneté sont les
--            recrutés de 2025, que le DTM exclut (Q58).
-- -----------------------------------------------------------------------------
SELECT tranche, SUM(selon_emb) AS selon_embauche, SUM(selon_ps) AS selon_prise_service
FROM (SELECT CASE WHEN a IS NULL THEN '99 inconnue'
                  WHEN a < 1  THEN '01 moins de 1 an'
                  WHEN a < 5  THEN '02 de 1 a 4 ans'
                  WHEN a < 10 THEN '03 de 5 a 9 ans'
                  WHEN a < 15 THEN '04 de 10 a 14 ans'
                  WHEN a < 20 THEN '05 de 15 a 19 ans'
                  WHEN a < 25 THEN '06 de 20 a 24 ans'
                  WHEN a < 30 THEN '07 de 25 a 29 ans'
                  ELSE '08 30 ans et plus' END AS tranche,
             emb AS selon_emb, ps AS selon_ps
        FROM (SELECT FLOOR(MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_EMBAUCHE) / 12) AS a, 1 AS emb, 0 AS ps
                FROM USER_GRH.GRH_PERSONNE p
               WHERE p.ETAT_EMPLOYE = 'ACTIF'
              UNION ALL
              SELECT FLOOR(MONTHS_BETWEEN(DATE '2025-12-31', p.DATE_PRISE_SERVICE) / 12), 0, 1
                FROM USER_GRH.GRH_PERSONNE p
               WHERE p.ETAT_EMPLOYE = 'ACTIF'))
GROUP BY tranche
ORDER BY tranche;


-- -----------------------------------------------------------------------------
-- Q49 — Structure et codes des tables de formation (T62, T64)
-- Objectif : connaître les colonnes de GRH_FORMATION, GRH_INSCRIPTION et
--            GRH_CAT_FORMATION, le sens des statuts et les finalités.
-- Résultat : GRH_FORMATION : CATEGORIE VARCHAR2(9), STATUT VARCHAR2(1),
--            BUDGET VARCHAR2(10), COUT_TOTAL, COUT_HONORAIRE, NBRE_INSCRIT,
--            LIEU_STAGE, dates de début, de fin, de départ et de retour.
--            GRH_INSCRIPTION : CLA_AVANT et CLA_APRES (classement avant et
--            après la formation), CDIP_NO (diplôme), DUREE et UNITE (unité de
--            la durée), 7 frais, COUT_INS, QUAL_CODE_INS, CATEG_INS.
--            GRH_CAT_FORMATION : absente des colonnes, et le SELECT échoue
--            (ORA-00942) : la table n'existe pas dans USER_GRH.
--            Listes de codes : CATEGORIE_FORMATION = DIPLO (diplômante) et PERF
--            (perfectionnement) ; LIEU_STAGE = Burkina, Afrique, Europe, Asie,
--            Amérique. Aucune liste pour le statut d'une formation.
-- Constat  : - la finalité « avec ou sans promotion » de T62 peut se mesurer
--              par le classement avant et après (CLA_AVANT, CLA_APRES) ;
--            - la durée a une unité (UNITE) que le DTM ignore quand il
--              additionne les jours de formation (à mesurer au lot 9) ;
--            - l'ETL lit une table GRH_CAT_FORMATION qui n'existe pas : seules
--              deux catégories (DIPLO, PERF) sont définies dans l'application.
-- -----------------------------------------------------------------------------
SELECT table_name, column_name, data_type, data_length, nullable
FROM ALL_TAB_COLUMNS
WHERE owner = 'USER_GRH' AND table_name IN ('GRH_FORMATION','GRH_INSCRIPTION','GRH_CAT_FORMATION')
ORDER BY table_name, column_id;

SELECT RV_DOMAIN, RV_LOW_VALUE, RV_MEANING
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN LIKE '%FORM%' OR RV_DOMAIN LIKE '%INSC%' OR RV_DOMAIN LIKE '%STAGE%'
ORDER BY RV_DOMAIN, RV_LOW_VALUE;

SELECT * FROM USER_GRH.GRH_CAT_FORMATION ORDER BY RANG;


-- -----------------------------------------------------------------------------
-- Q50 — Formations par statut et par année depuis 2015
-- Objectif : voir combien de formations le DTM écarte (statut autre que T),
--            si le nombre d'inscrits déclaré concorde avec les inscriptions, et
--            si le budget et le coût total sont renseignés.
-- Tableaux : T62, T64.
-- Résultat : toutes les formations ont le statut N.
--            (année · formations · inscrits déclarés · inscriptions réelles ·
--            budget renseigné · en chiffres · coût total renseigné)
--            2015 · 29 · 0 · 245 · 0 · 0 · 29      2016 · 41 · 0 · 266 · 0 · 0 · 41
--            2017 · 66 · 0 · 872 · 0 · 0 · 66      2018 · 32 · 0 · 416 · 0 · 0 · 32
--            2019 · 31 · 0 · 602 · 0 · 0 · 31      2020 · 28 · 0 · 435 · 28 · 28 · 28
--            2021 · 37 · 471 · 556 · 37 · 37 · 34  2022 · 39 · 933 · 886 · 39 · 39 · 31
--            2023 · 36 · 688 · 735 · 36 · 36 · 10  2024 · 45 · 538 · 525 · 44 · 44 · 32
--            2025 · 35 · 721 · 691 · 35 · 35 · 21  sans date · 7 · 0 · 18 · 0 · 0 · 7
-- Constat  : - aucune formation n'a le statut T : le DTM n'en retient aucune,
--              et T62 et T64 sont vides ;
--            - 30 à 45 formations et 435 à 886 inscriptions par an ;
--            - le budget n'est saisi que depuis 2020, toujours en chiffres ;
--            - le coût total est de moins en moins saisi (10 formations sur 36
--              en 2023) ;
--            - le nombre d'inscrits déclaré, saisi depuis 2021, est proche des
--              inscriptions réelles.
-- -----------------------------------------------------------------------------
SELECT f.STATUT, EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       COUNT(*) AS formations,
       SUM(NVL(f.NBRE_INSCRIT, 0)) AS inscrits_declares,
       SUM(NVL(i.nb, 0)) AS inscriptions_reelles,
       SUM(CASE WHEN f.BUDGET IS NOT NULL THEN 1 ELSE 0 END) AS budget_renseigne,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+$') THEN 1 ELSE 0 END) AS budget_en_chiffres,
       SUM(CASE WHEN f.COUT_TOTAL IS NOT NULL THEN 1 ELSE 0 END) AS cout_total_renseigne
FROM USER_GRH.GRH_FORMATION f
LEFT JOIN (SELECT FORM_NO, COUNT(*) AS nb FROM USER_GRH.GRH_INSCRIPTION GROUP BY FORM_NO) i
       ON i.FORM_NO = f.FORM_NO
WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2015-01-01'
   OR COALESCE(f.DATE_FIN, f.DATE_DEBUT) IS NULL
GROUP BY f.STATUT, EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee, f.STATUT;


-- -----------------------------------------------------------------------------
-- Q51 — Coût des inscriptions : effet des frais vides
-- Objectif : comparer le coût calculé comme le DTM (somme avec « + », qui
--            devient vide dès qu'un frais manque) au coût complet (frais vides
--            comptés à zéro), par année, pour les formations terminées.
-- Tableau  : T64 (dépenses de formation).
-- Résultat : vide, car aucune formation n'a le statut T (Q50). Relancée sans
--            filtre de statut au lot 9 (Q62).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       COUNT(*) AS inscriptions,
       SUM(CASE WHEN i.FRAIS_INSCRIPTION + i.FRAIS_SCOLARITE + i.FRAIS_TRANSPORT + i.FRAIS_SEJOUR
                   + i.FRAIS_DOSSIER + i.FRAIS_ASSURANCE + i.FRAIS_ACCESSOIRE IS NULL
                THEN 1 ELSE 0 END) AS cout_dtm_vide,
       SUM(i.FRAIS_INSCRIPTION + i.FRAIS_SCOLARITE + i.FRAIS_TRANSPORT + i.FRAIS_SEJOUR
           + i.FRAIS_DOSSIER + i.FRAIS_ASSURANCE + i.FRAIS_ACCESSOIRE) AS cout_facon_dtm,
       SUM(NVL(i.FRAIS_INSCRIPTION, 0) + NVL(i.FRAIS_SCOLARITE, 0) + NVL(i.FRAIS_TRANSPORT, 0)
           + NVL(i.FRAIS_SEJOUR, 0) + NVL(i.FRAIS_DOSSIER, 0) + NVL(i.FRAIS_ASSURANCE, 0)
           + NVL(i.FRAIS_ACCESSOIRE, 0)) AS cout_complet,
       SUM(CASE WHEN i.DUREE IS NULL THEN 1 ELSE 0 END) AS duree_vide
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
WHERE f.STATUT = 'T' AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q52 — Budget de formation : compté une fois ou une fois par inscrit ?
-- Objectif : mesurer la surestimation du budget dans le DTM, qui additionne le
--            budget d'une formation sur chacune de ses inscriptions.
-- Tableau  : T64 (dépenses de formation rapportées au budget).
-- Résultat : vide, pour la même raison que Q51. Relancée au lot 9 (Q63).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       COUNT(*) AS formations,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+$') THEN TO_NUMBER(f.BUDGET) END) AS budget_une_fois,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+$') THEN TO_NUMBER(f.BUDGET) * NVL(i.nb, 0) END) AS budget_facon_dtm,
       SUM(CASE WHEN f.BUDGET IS NOT NULL AND NOT REGEXP_LIKE(f.BUDGET, '^[0-9]+$')
                THEN 1 ELSE 0 END) AS budget_non_numerique
FROM USER_GRH.GRH_FORMATION f
LEFT JOIN (SELECT FORM_NO, COUNT(*) AS nb FROM USER_GRH.GRH_INSCRIPTION GROUP BY FORM_NO) i
       ON i.FORM_NO = f.FORM_NO
WHERE f.STATUT = 'T' AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q53 — Comment le budget est-il saisi ?
-- Objectif : voir les formes du texte saisi (chaque suite de chiffres est
--            remplacée par N) pour savoir si les budgets écartés peuvent être
--            récupérés (espaces, séparateurs, « FCFA »…).
-- Tableau  : T64.
-- Résultat : une seule forme : N (que des chiffres), 220 budgets.
-- Constat  : le budget est toujours saisi en chiffres. Rien n'est perdu à la
--            conversion ; le budget manque simplement avant 2020.
-- -----------------------------------------------------------------------------
SELECT * FROM (
  SELECT REGEXP_REPLACE(TRIM(f.BUDGET), '[0-9]+', 'N') AS forme, COUNT(*) AS nb
  FROM USER_GRH.GRH_FORMATION f
  WHERE f.BUDGET IS NOT NULL
  GROUP BY REGEXP_REPLACE(TRIM(f.BUDGET), '[0-9]+', 'N')
  ORDER BY nb DESC
) WHERE ROWNUM <= 20;


-- -----------------------------------------------------------------------------
-- Q54 — Agents formés par année, catégorie de formation et finalité (T62)
-- Objectif : produire la base de T62 et vérifier les liens : agent retrouvé,
--            qualification à l'inscription, sexe. T62 demande une finalité
--            « avec ou sans promotion », alors que le référentiel donne
--            adaptation, perfectionnement, reconversion : à rapprocher.
-- Résultat : échec ORA-00942 : GRH_CAT_FORMATION n'existe pas. Remplacée au
--            lot 9 par Q64, qui se passe de cette table.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       f.CATEGORIE, c.CAT_FORM_FINALITE,
       COUNT(*) AS inscriptions,
       COUNT(DISTINCT i.PERS_ID) AS agents,
       SUM(CASE WHEN p.PERS_ID IS NULL THEN 1 ELSE 0 END) AS agent_introuvable,
       SUM(CASE WHEN i.QUAL_CODE_INS IS NULL THEN 1 ELSE 0 END) AS qual_vide,
       SUM(CASE WHEN p.SEXE = 'F' THEN 1 ELSE 0 END) AS inscriptions_femmes
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
LEFT JOIN USER_GRH.GRH_CAT_FORMATION c ON c.CAT_FORM_CODE = f.CATEGORIE
LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = i.PERS_ID
WHERE f.STATUT = 'T' AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)), f.CATEGORIE, c.CAT_FORM_FINALITE
ORDER BY annee, f.CATEGORIE;


-- -----------------------------------------------------------------------------
-- Q55 — Où trouver la masse salariale et les charges ? (T63, T64)
-- Objectif : chercher des tables de paie (bulletins, rubriques, précomptes,
--            relevés) et de budget dans les schémas visibles, et l'état de la
--            vue V_SALAIRES_ANNEE (cassée d'après la doc : ORA-04063).
-- Résultat : aucune table de paie dans USER_GRH. Les 22 vues de paie de
--            USER_GRH (V_SALAIRES_ANNEE, V_BULL_PAIE, V_LIVRE_PAIE,
--            V_ELEMENT_SALAIRE_ANNEE, V_AGENT_PAIE_ACTIF…) sont toutes
--            INVALIDES. Les tables SALAIRE* de USER_DWH, DWH, ODS et DTM sont
--            les salaires déclarés par les employeurs (recouvrement), pas la
--            paie du personnel de la CNSS.
--            Pistes comptables : DTM.DTM_CHARGE_FONCT_PROD_TECH (18 lignes),
--            DTM.DTM_EXECUTION_BUDGETAIRE (vide), DTM.DIM_GESTION_BUDGETAIRE
--            (2 lignes), STG_GL.STG_BUDGET (9 272 lignes).
--            Un schéma DATA_MASK_OWNER contient des copies de tables datées
--            du 26 et du 27/02/2026.
-- Constat  : la paie du personnel n'est pas dans la copie consultée : les
--            vues existent, mais leurs tables manquent. T63 et T64 ne peuvent
--            pas être calculés depuis la GRH. La piste est la comptabilité
--            (vue comptable du domaine des finances, voir lot 9, Q66). Le
--            schéma DATA_MASK_OWNER laisse penser que l'environnement est une
--            copie masquée de la production, faite fin février 2026 (à
--            confirmer par la DSI).
-- -----------------------------------------------------------------------------
SELECT owner, table_name, num_rows, last_analyzed
FROM ALL_TABLES
WHERE (table_name LIKE '%BULL%' OR table_name LIKE '%PAIE%' OR table_name LIKE '%SALAIRE%'
       OR table_name LIKE '%RUBRIQUE%' OR table_name LIKE '%PRECOMPTE%' OR table_name LIKE '%RELEVE%'
       OR table_name LIKE '%BUDGET%' OR table_name LIKE '%CHARGE%')
  AND owner NOT IN ('SYS','SYSTEM')
ORDER BY owner, table_name;

SELECT owner, object_name, object_type, status, last_ddl_time
FROM ALL_OBJECTS
WHERE (object_name LIKE '%SALAIRE%' OR object_name LIKE '%BULLETIN%' OR object_name LIKE '%PAIE%')
  AND object_type IN ('TABLE','VIEW','MATERIALIZED VIEW')
ORDER BY owner, object_name;


-- -----------------------------------------------------------------------------
-- Q56 — Ce que publie le DTM pour la formation et les indicateurs (schéma DTM)
-- Objectif : comparer la formation publiée (T62, T64) à la source (Q51, Q52),
--            et lire le troisième taux d'encadrement (T65).
-- :cliche = '102026'.
-- Résultat : DTM_GRH_FORMATION : aucune ligne pour 2019 et après.
--            DTM_INDICATEURS_FINANCES_GRH (taux · cadres · effectif) :
--            2019 13,4 % · 241 · 1 794 ; 2020 13,7 % · 262 · 1 907 ;
--            2021 13,1 % · 236 · 1 806 ; 2022 13,0 % · 265 · 2 047 ;
--            2023 13,9 % · 263 · 1 898 ; 2024 13,2 % · 261 · 1 975 ;
--            2025 13,6 % · 287 · 2 118 ; 2026 13,4 % · 287 · 2 149.
-- Constat  : - T62 et T64 n'ont aucune donnée publiée ;
--            - pour T65 en 2025, le DTM publie 13,6 % dans une table et 31,1 %
--              dans l'autre. Le premier ne compte que les cadres supérieurs et
--              rapporte au total faussé de l'effectif ; sur les seuls actifs,
--              les cadres supérieurs font 137 sur 878, soit 15,6 % ;
--            - l'année 2026, future, est publiée là aussi.
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS, SUM(NB_STAGIAIRES) AS somme_stagiaires, SUM(NB_INSCRIPTIONS) AS inscriptions,
       SUM(NB_JOURS_FORMATION) AS jours, SUM(COUT_TOTAL) AS cout, SUM(BUDGET_FORMATION) AS budget
FROM DTM.DTM_GRH_FORMATION
WHERE CLICHE = :cliche AND ID_TEMPS >= 20190101
GROUP BY ID_TEMPS
ORDER BY ID_TEMPS;

SELECT AN_ID, TX_ENCADREMENT, NB_CADRES, NB_EFFECTIF_TOTAL
FROM DTM.DTM_INDICATEURS_FINANCES_GRH
WHERE CLICHE = :cliche AND AN_ID >= 2019
ORDER BY AN_ID;


-- -----------------------------------------------------------------------------
-- Q57 — Formations suivies d'un avancement ou d'un reclassement (T62)
-- Objectif : donner un ordre de grandeur pour la finalité « avec promotion » :
--            agents formés qui reçoivent un avancement (60) ou un reclassement
--            (61) dans les deux ans après la fin de la formation.
-- Résultat : vide, car aucune formation n'a le statut T. Remplacée au lot 9 par
--            Q64 (classement avant et après la formation).
-- -----------------------------------------------------------------------------
WITH avanc AS (
  SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR
  FROM USER_GRH.GRH_DECISION d
  LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
  WHERE d.CDEC_CODE IN ('60','61')
)
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       COUNT(DISTINCT i.PERS_ID) AS agents_formes,
       COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM avanc a
                                         WHERE a.PERS_ID = i.PERS_ID
                                           AND a.DATE_VIGUEUR BETWEEN COALESCE(f.DATE_FIN, f.DATE_DEBUT)
                                                                  AND ADD_MONTHS(COALESCE(f.DATE_FIN, f.DATE_DEBUT), 24))
                           THEN i.PERS_ID END) AS avancement_sous_2_ans
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
WHERE f.STATUT = 'T' AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q58 — Agents perdus par les tranches du DTM au 31/12/2025
-- Objectif : confirmer le défaut vu au lot 7 (Q40) : la requête de l'effectif
--            compare l'âge et l'ancienneté à la borne haute des tranches avec
--            « < », alors que cette borne est incluse. On refait ici le calcul
--            du DTM (mêmes formules d'âge et d'ancienneté) et on compte les
--            agents qui tombent sur une borne haute.
--            Attendu : comptes_par_la_regle - perdus_au_total = 2 118, le
--            chiffre publié pour 2025 (lot 6, Q39).
-- Tableaux : T58, T59, T61 (et T65 par la table des indicateurs).
-- Résultat : 2 822 agents comptés par la règle ; 329 perdus par l'âge, 461
--            par l'ancienneté, 704 au total ; 153 actifs recrutés en 2025
--            perdus.
-- Constat  : 2 822 - 704 = 2 118, exactement le chiffre publié. Le défaut des
--            tranches est confirmé : il retire un quart des agents comptés,
--            dont tous les recrutés de l'année.
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS comptes_par_la_regle,
       SUM(CASE WHEN age IN (24, 29, 34, 39, 44, 49, 54) THEN 1 ELSE 0 END) AS perdus_par_age,
       SUM(CASE WHEN anc IN (0, 4, 9, 14, 19, 24, 29) THEN 1 ELSE 0 END) AS perdus_par_anciennete,
       SUM(CASE WHEN age IN (24, 29, 34, 39, 44, 49, 54)
                  OR anc IN (0, 4, 9, 14, 19, 24, 29) THEN 1 ELSE 0 END) AS perdus_au_total,
       SUM(CASE WHEN etat = 'ACTIF' AND anc = 0 THEN 1 ELSE 0 END) AS recrutes_annee_perdus
FROM (SELECT FLOOR((DATE '2025-12-31' - p.DATE_NAISS) / 365.25) AS age,
             GREATEST(FLOOR((DATE '2025-12-31' - COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)) / 365.25), 0) AS anc,
             p.ETAT_EMPLOYE AS etat
        FROM USER_GRH.GRH_SITUATION s
        JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = s.PERS_ID
       WHERE s.DATE_DEBUT <= DATE '2025-12-31'
         AND (s.DATE_FIN IS NULL OR s.DATE_FIN > DATE '2025-12-31')
         AND UPPER(TRIM(s.NATURE_SITUATION)) IN ('AVA','CLA','AFF','NOM','REC','MUT','PRO','RET','BON')
         AND p.DATE_NAISS IS NOT NULL
         AND EXTRACT(YEAR FROM p.DATE_NAISS) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE));


-- -----------------------------------------------------------------------------
-- Q59 — Contenu des listes de codes repérées au lot 7 (Q43)
-- Objectif : lire les motifs de départ (LOV_DEPART, MOTIF_DEPART,
--            CODE_ALLOC_DEPART), la qualité de l'agent, la nature d'emploi,
--            les tranches d'ancienneté de l'application, les éléments de paie
--            et les listes de formation.
-- Tableaux : T60 (motifs de sortie), T58 à T61 (périmètre), T62 à T64.
-- Résultat : LOV_DEPART (décisions de départ de l'application) : 02 admission
--            à la retraite, 34 liquidation des droits, 36 démission,
--            54 licenciement, 75 frais funéraires, 40 annulation,
--            41 modification. LOV_ENG_TEMPORAIRE : 50, 40, 41.
--            LOV_ENG_DETACHES : 37 engagement de détachés, 40, 41.
--            MOTIF_DEPART : décès, démission, licenciement, retraite.
--            CODE_ALLOC_DEPART (indemnités de paie) : départ à la retraite,
--            fin d'engagement, licenciement, fin de contrat.
--            NATURE_EMPLOIE : permanent, temporaire.
--            TRANCHE_ANCIENNETE de l'application : 0-5, 6-10, 11-15, 16-20,
--            21-25, 26-30, 30 et plus.
--            CATEGORIE_ELEMENT (paie) : salaire, avantages, prêts, indemnités,
--            retenues, précomptes. CATEGORIE_FORMATION : DIPLO, PERF.
-- Constat  : - l'application classe elle-même 02, 34, 36, 54 et 75 comme des
--              décisions de départ : cela conforte les règles retenues pour
--              T60, et confirme que 34 accompagne un décès ;
--            - la décision 37 est un engagement de détachés : c'est une entrée ;
--            - les tranches d'ancienneté de l'application (0-5, 6-10…)
--              diffèrent de celles du DTM (< 1, 1-4, 5-9…) : laquelle suit
--              CIPRES ? Question DRH ;
--            - les motifs de départ (MOTIF_DEPART) ne sont utilisés par aucune
--              colonne de la copie consultée (Q61).
-- -----------------------------------------------------------------------------
SELECT RV_DOMAIN, RV_LOW_VALUE, RV_MEANING
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN IN ('LOV_DEPART','MOTIF_DEPART','CODE_ALLOC_DEPART','MOTIF_ALLOC_INDEMN',
                    'QUALITE_EMPLOYE','NATURE_EMPLOIE','SITUATION','STATUT','TRANCHE_ANCIENNETE',
                    'TYPE_ELEMENT','CATEGORIE_ELEMENT','CLASSE_ELEMENT',
                    'LOV_ENG_TEMPORAIRE','LOV_ENG_DETACHES','CATEGORIE_FORMATION','FORMATION_DIPL',
                    'LOV_FORM_STAGE')
ORDER BY RV_DOMAIN, RV_LOW_VALUE;


-- -----------------------------------------------------------------------------
-- Q60 — Qualité et nature d'emploi des agents actifs
-- Objectif : savoir combien d'actifs sont agents CNSS, agents de la fonction
--            publique ou agents affectés, et combien sont temporaires : cela
--            peut changer le périmètre de l'effectif CIPRES.
-- Tableaux : T58 à T61, T65.
-- Résultat : agent CNSS permanent 869 ; agent CNSS temporaire 6 ; agent de la
--            fonction publique permanent 3 ; agent CNSS sans situation 1.
-- Constat  : le périmètre ne pose presque pas de question : 6 temporaires et
--            3 agents de la fonction publique sur 879. Reste à savoir si la
--            DRH les compte dans l'effectif CIPRES.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.NATURE_EMPLOIE,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT NVL(TO_CHAR(p.QUALITE), '-') AS qualite,
       NVL(TO_CHAR(r.NATURE_EMPLOIE), '-') AS nature_emploi,
       COUNT(*) AS actifs,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION d
                             WHERE d.CDEC_CODE = '50' AND d.PERS_ID = p.PERS_ID)
                THEN 1 ELSE 0 END) AS dont_temporaires
FROM USER_GRH.GRH_PERSONNE p
LEFT JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
WHERE p.ETAT_EMPLOYE = 'ACTIF'
GROUP BY NVL(TO_CHAR(p.QUALITE), '-'), NVL(TO_CHAR(r.NATURE_EMPLOIE), '-')
ORDER BY actifs DESC;


-- -----------------------------------------------------------------------------
-- Q61 — Colonnes de paie, d'allocations et de motifs de départ
-- Objectif : trouver où sont utilisés les éléments de paie (T63, T64) et les
--            motifs de départ (T60) repérés dans les listes de codes.
-- Résultat : seulement GRH_FRAIS.BRUT, GRH_TAUX.GRILLE_SALAIRE, et les colonnes
--            déjà connues (motifs d'absence et de sanction, dates de départ
--            en congé ou en formation).
-- Constat  : ni tables de paie ni colonne de motif de départ dans la copie
--            consultée.
-- -----------------------------------------------------------------------------
SELECT table_name, column_name, data_type
FROM ALL_TAB_COLUMNS
WHERE owner = 'USER_GRH'
  AND table_name NOT LIKE 'V\_%' ESCAPE '\'
  AND (column_name LIKE '%ALLOC%' OR column_name LIKE '%ELEMENT%' OR column_name LIKE '%BULL%'
       OR column_name LIKE '%BRUT%' OR column_name LIKE '%MASSE%' OR column_name LIKE '%SALAIRE%'
       OR column_name LIKE '%MOTIF%' OR column_name LIKE '%DEPART%' OR column_name LIKE '%RUBRIQUE%')
ORDER BY table_name, column_name;
