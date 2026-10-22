-- =============================================================================
-- LOT 9 — Formation sans filtre de statut (T62, T64) et piste comptable
--          pour les frais de personnel (T63, T64)
-- Rédigé le 21/10/2026 — À EXÉCUTER sur USER_GRH (Q66 et Q68 en partie sur
-- les schémas DTM et STG_GL).
-- Exécution : en mode script (F5) ; tout est en SELECT et ne renvoie que des
-- comptages ou des montants agrégés (aucune donnée nominative). Alias limités
-- à 30 caractères, sans accents.
-- Pourquoi ce lot : au lot 8, toutes les formations ont le statut 'N' (Q50),
-- donc les requêtes filtrées sur le statut 'T' sont revenues vides (Q51, Q52,
-- Q57), et GRH_CAT_FORMATION n'existe pas (Q54). On reprend ici sans ce filtre
-- ni cette table. L'année d'une formation est celle de sa fin, ou à défaut de
-- son début, comme dans le DTM.
-- Exécuté le 21/10/2026. Résultats de Q62 et Q63 non reçus : à recoller.
-- Conclusions du lot :
--   - le statut des formations n'est plus tenu depuis 2015 (Q69) ;
--   - T62 est calculable : 350 à 608 agents formés par an, presque tous en
--     perfectionnement ; la finalité « avec promotion » n'est pas mesurable,
--     car le classement avant et après n'est saisi que 2 fois (Q64) ;
--   - les durées mélangent jours, mois et années mal saisis (Q65) ;
--   - la dimension des catégories du DTM a été remplie à la main (Q68) ;
--   - la comptabilité (STG_GL) existe, mais la table des charges de
--     fonctionnement du DTM contient des valeurs 2025 artificielles (Q66).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q62 — Coût des inscriptions par année : effet des frais vides (T64)
-- Objectif : comparer le coût calculé comme le DTM (somme des 7 frais avec
--            « + », vide dès qu'un frais manque) au coût complet (frais vides
--            comptés à zéro), et au coût saisi directement (COUT_INS).
-- Résultat : non reçu (à recoller).
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
       SUM(i.COUT_INS) AS cout_ins_saisi,
       SUM(CASE WHEN i.COUT_INS IS NOT NULL THEN 1 ELSE 0 END) AS cout_ins_renseigne
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q63 — Budget et coût au niveau des formations (T64)
-- Objectif : mesurer la surestimation du budget dans le DTM (budget compté une
--            fois par inscrit), et comparer avec le coût total et les
--            honoraires saisis sur la formation.
-- Résultat : non reçu (à recoller).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       COUNT(*) AS formations,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+$') THEN TO_NUMBER(f.BUDGET) END) AS budget_une_fois,
       SUM(CASE WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+$') THEN TO_NUMBER(f.BUDGET) * NVL(i.nb, 0) END) AS budget_facon_dtm,
       SUM(f.COUT_TOTAL) AS cout_total_formations,
       SUM(f.COUT_HONORAIRE) AS honoraires
FROM USER_GRH.GRH_FORMATION f
LEFT JOIN (SELECT FORM_NO, COUNT(*) AS nb FROM USER_GRH.GRH_INSCRIPTION GROUP BY FORM_NO) i
       ON i.FORM_NO = f.FORM_NO
WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q64 — Agents formés par année et catégorie, avec ou sans promotion (T62)
-- Objectif : produire la base de T62 : agents distincts, femmes, et part des
--            inscriptions dont le classement change après la formation
--            (CLA_APRES différent de CLA_AVANT), qui pourrait définir la
--            finalité « avec promotion ». Puis, pour 2025, les agents formés
--            par niveau de qualification (catégorie CIPRES) et par sexe.
-- Résultat : (année · catégorie · inscriptions · agents · femmes · classement
--            renseigné · classement changé · agent introuvable · qualification
--            vide · inscrits encore actifs)
--            2019 DIPLO 30 · 30 · 14 · 0 · 0 · 0 · 0 · 27
--            2019 PERF 572 · 485 · 197 · 0 · 0 · 0 · 0 · 414
--            2020 PERF 435 · 350 · 189 · 0 · 0 · 0 · 0 · 310
--            2021 DIPLO 2 · 2 · 0 · 0 · 0 · 0 · 0 · 2
--            2021 PERF 554 · 492 · 214 · 0 · 0 · 0 · 0 · 399
--            2022 PERF 886 · 524 · 243 · 0 · 0 · 0 · 0 · 724
--            2023 PERF 735 · 548 · 232 · 0 · 0 · 0 · 0 · 587
--            2024 CONF 1 · 1 · 0 · 0 · 0 · 0 · 0 · 0
--            2024 DIPLO 2 · 2 · 2 · 2 · 2 · 0 · 0 · 2
--            2024 PERF 522 · 407 · 191 · 0 · 0 · 0 · 0 · 492
--            2025 PERF 691 · 608 · 277 · 0 · 0 · 0 · 0 · 675
--            Agents formés en 2025 par niveau (hommes / femmes), tous en
--            perfectionnement : A 103 / 40 · I 97 / 107 · M 73 / 99 · S 58 / 31.
-- Constat  : - T62 se calcule : chaque inscription retrouve son agent et sa
--              qualification. En 2025, 608 agents formés (331 hommes,
--              277 femmes), soit 69 % des actifs ;
--            - presque tout est du perfectionnement ; les formations
--              diplômantes sont rares (30 inscriptions en 2019, 2 en 2021 et
--              2 en 2024) ;
--            - le classement avant et après la formation n'est saisi que pour
--              2 inscriptions : la finalité « avec promotion » ne peut pas se
--              mesurer ainsi.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) AS annee,
       f.CATEGORIE,
       COUNT(*) AS inscriptions,
       COUNT(DISTINCT i.PERS_ID) AS agents,
       COUNT(DISTINCT CASE WHEN p.SEXE = 'F' THEN i.PERS_ID END) AS femmes,
       SUM(CASE WHEN i.CLA_AVANT IS NOT NULL AND i.CLA_APRES IS NOT NULL THEN 1 ELSE 0 END) AS classement_renseigne,
       SUM(CASE WHEN i.CLA_APRES <> i.CLA_AVANT THEN 1 ELSE 0 END) AS classement_change,
       SUM(CASE WHEN p.PERS_ID IS NULL THEN 1 ELSE 0 END) AS agent_introuvable,
       SUM(CASE WHEN i.QUAL_CODE_INS IS NULL THEN 1 ELSE 0 END) AS qual_vide,
       SUM(CASE WHEN p.ETAT_EMPLOYE = 'ACTIF' THEN 1 ELSE 0 END) AS inscrits_encore_actifs
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = i.PERS_ID
WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)), f.CATEGORIE
ORDER BY annee, f.CATEGORIE;

SELECT NVL(q.NIVEAU, '-') AS niveau, f.CATEGORIE,
       COUNT(DISTINCT CASE WHEN p.SEXE = 'M' THEN i.PERS_ID END) AS hommes,
       COUNT(DISTINCT CASE WHEN p.SEXE = 'F' THEN i.PERS_ID END) AS femmes
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = i.PERS_ID
LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = i.QUAL_CODE_INS
WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2025-01-01'
  AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) < DATE '2026-01-01'
GROUP BY NVL(q.NIVEAU, '-'), f.CATEGORIE
ORDER BY niveau, f.CATEGORIE;


-- -----------------------------------------------------------------------------
-- Q65 — Unités de la durée des formations
-- Objectif : vérifier si la durée est saisie dans plusieurs unités (jours,
--            heures, mois…), ce que le DTM ignore quand il additionne les
--            « jours de formation ».
-- Tableaux : T62, T64.
-- Résultat : (unité · inscriptions · durée vide · min · max · moyenne)
--            JOUR 9 357 · 5 · -17 · 852 · 7,1 ; MOIS 139 · 0 · 3 · 365 · 67,5 ;
--            ANNEE 11 · 0 · 1 · 730 · 358,5 ; vide 2 · 2 ; KG 1 · 5 ; CFA 1 · 12.
-- Constat  : les durées « en mois » (moyenne 67,5) et « en années » (moyenne
--            358,5) sont en réalité des jours. Il existe aussi des unités KG et
--            CFA et des durées négatives. Le DTM additionne ces durées comme des
--            jours sans regarder l'unité : correct pour les mois et années mal
--            saisis, faux si certains sont de vrais mois. À clarifier.
-- -----------------------------------------------------------------------------
SELECT NVL(UPPER(TRIM(i.UNITE)), '-') AS unite,
       COUNT(*) AS inscriptions,
       SUM(CASE WHEN i.DUREE IS NULL THEN 1 ELSE 0 END) AS duree_vide,
       MIN(i.DUREE) AS duree_min,
       MAX(i.DUREE) AS duree_max,
       ROUND(AVG(i.DUREE), 1) AS duree_moyenne
FROM USER_GRH.GRH_INSCRIPTION i
GROUP BY NVL(UPPER(TRIM(i.UNITE)), '-')
ORDER BY inscriptions DESC;


-- -----------------------------------------------------------------------------
-- Q66 — Piste comptable pour les frais de personnel et les charges (T63, T64)
-- Objectif : la paie du personnel est absente de la copie (lot 8, Q55). On
--            regarde ce que contiennent les tables comptables et budgétaires
--            déjà présentes : schéma STG_GL (grand livre), table des charges
--            de fonctionnement du DTM, gestion budgétaire.
-- Résultat : STG_GL contient 1 518 974 écritures (STG_ECRITURE, et autant de
--            lignes dans STG_BALANCES et STG_PORTEFEUILLE), 9 272 lignes de
--            budget (et autant d'engagements), 9 314 dépenses, 1 625 comptes,
--            8 028 comptes de comptabilité générale ; les tables GL_* et
--            STG_RECETTES, STG_REGLEMENTS sont vides. Tout est daté du 27/02/2026.
--            STG_BUDGET : montant, date, gestion, direction, catégorie et
--            sous-catégorie budgétaires.
--            DTM_CHARGE_FONCT_PROD_TECH : par année (2020 à 2025) et branche
--            (PF, PE, AT/MP) : charges de fonctionnement (CF), produits
--            techniques (PT), charges techniques (CT), PRF, et cinq ratios
--            tous vides ; un seul cliché (062026).
--            Exemple PF : CF 4,31 milliards en 2020, 6,87 en 2024, 6,88 en 2025.
--            DIM_GESTION_BUDGETAIRE : gestion administrative, action sanitaire
--            et sociale. DTM_EXECUTION_BUDGETAIRE : vide.
-- Constat  : - la comptabilité est présente et pourra fournir les frais de
--              personnel et les charges de fonctionnement par compte (domaine
--              des finances) ;
--            - dans DTM_CHARGE_FONCT_PROD_TECH, chaque valeur de 2025 est celle
--              de 2024 plus un montant rond (10 millions, 100 millions ou
--              1 milliard) : données de test ;
--            - les charges de fonctionnement de la branche PE tombent de
--              16,64 milliards en 2023 à 1,88 milliard en 2024 ;
--            - les ratios de cette table ne sont pas calculés.
-- -----------------------------------------------------------------------------
SELECT table_name, num_rows, last_analyzed
FROM ALL_TABLES
WHERE owner = 'STG_GL'
ORDER BY table_name;

SELECT owner, table_name, column_name, data_type
FROM ALL_TAB_COLUMNS
WHERE (owner = 'DTM' AND table_name IN ('DTM_CHARGE_FONCT_PROD_TECH','DTM_EXECUTION_BUDGETAIRE','DIM_GESTION_BUDGETAIRE'))
   OR (owner = 'STG_GL' AND table_name = 'STG_BUDGET')
ORDER BY owner, table_name, column_id;

SELECT * FROM DTM.DTM_CHARGE_FONCT_PROD_TECH;

SELECT * FROM DTM.DIM_GESTION_BUDGETAIRE;


-- -----------------------------------------------------------------------------
-- Q67 — Agents « formés ou en formation dans l'année » (T62)
-- Objectif : T62 compte les agents formés OU en formation dans l'année. On
--            compare trois comptages par année : formations finies dans
--            l'année (règle du DTM), formations en cours au 31/12, et toute
--            formation qui chevauche l'année.
-- Résultat : (année · formés dans l'année · en formation au 31/12 · formés
--            ou en formation) 2019 506 · 0 · 506 ; 2020 350 · 0 · 350 ;
--            2021 494 · 0 · 494 ; 2022 524 · 0 · 524 ; 2023 548 · 2 · 548 ;
--            2024 410 · 0 · 410 ; 2025 608 · 0 · 608.
-- Constat  : presque aucune formation ne chevauche le 31 décembre. Compter
--            les agents formés dans l'année suffit pour T62.
-- -----------------------------------------------------------------------------
WITH ans AS (
  SELECT 2018 + LEVEL AS annee,
         TO_DATE(TO_CHAR(2018 + LEVEL) || '0101', 'YYYYMMDD') AS dt_deb,
         TO_DATE(TO_CHAR(2018 + LEVEL) || '1231', 'YYYYMMDD') AS dt_fin
  FROM dual CONNECT BY LEVEL <= 7
)
SELECT a.annee,
       (SELECT COUNT(DISTINCT i.PERS_ID)
          FROM USER_GRH.GRH_INSCRIPTION i
          JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
         WHERE EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) = a.annee) AS agents_formes_annee,
       (SELECT COUNT(DISTINCT i.PERS_ID)
          FROM USER_GRH.GRH_INSCRIPTION i
          JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
         WHERE f.DATE_DEBUT <= a.dt_fin AND f.DATE_FIN > a.dt_fin) AS en_formation_au_31_12,
       (SELECT COUNT(DISTINCT i.PERS_ID)
          FROM USER_GRH.GRH_INSCRIPTION i
          JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
         WHERE f.DATE_DEBUT <= a.dt_fin
           AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= a.dt_deb) AS formes_ou_en_formation
FROM ans a
ORDER BY a.annee;


-- -----------------------------------------------------------------------------
-- Q68 — Catégories de formation : source et dimension du DTM
-- Objectif : la dimension DTM des catégories est censée venir d'une table
--            GRH_CAT_FORMATION qui n'existe pas (lot 8, Q49). On compare les
--            catégories réellement saisies avec ce que contient la dimension.
-- Résultat : catégories saisies : PERF 821 formations (2002 à 2025), DIPLO 71
--            (1979 à 2023), CONF 8 (2002 à 2024), RECY 4 (2004 à 2005).
--            Dimension du DTM : CONF conférence ou séminaire → perfectionnement ;
--            DIPLO formation diplômante → adaptation ; PERF → perfectionnement ;
--            RECY recyclage ou reconversion → reconversion ; NC non classifié.
-- Constat  : quatre catégories sont saisies, mais la liste de l'application
--            n'en connaît que deux (DIPLO, PERF). La dimension du DTM a été
--            remplie à la main, puisque sa table source n'existe pas. Elle range
--            la formation diplômante en « adaptation », alors que T62 demande
--            « avec ou sans promotion ». Question DRH.
-- -----------------------------------------------------------------------------
SELECT CATEGORIE, COUNT(*) AS formations,
       MIN(EXTRACT(YEAR FROM DATE_DEBUT)) AS an_min,
       MAX(EXTRACT(YEAR FROM DATE_DEBUT)) AS an_max
FROM USER_GRH.GRH_FORMATION
GROUP BY CATEGORIE
ORDER BY formations DESC;

SELECT * FROM DTM.DIM_GRH_CAT_FORMATION;


-- -----------------------------------------------------------------------------
-- Q69 — Le statut d'une formation est-il tenu à jour ?
-- Objectif : toutes les formations ont le statut 'N'. On regarde si certaines
--            sont pourtant finies d'après leurs dates (avant le 18/02/2026,
--            date de la copie), à venir, ou sans date de fin.
-- Résultat : statut C : 40 formations, toutes finies, avec date de retour.
--            Statut T : 153, toutes finies, avec date de retour.
--            Statut N : 711, dont 697 finies et 14 sans date de fin ; 617 ont
--            une date de retour.
-- Constat  : les statuts T et C ont été utilisés avant 2015, puis abandonnés.
--            Depuis, toutes les formations restent en N, même finies. Le DTM,
--            qui ne garde que T, ne voit que les formations d'avant 2015.
-- -----------------------------------------------------------------------------
SELECT f.STATUT,
       COUNT(*) AS formations,
       SUM(CASE WHEN f.DATE_FIN < DATE '2026-02-18' THEN 1 ELSE 0 END) AS finies_par_la_date,
       SUM(CASE WHEN f.DATE_FIN >= DATE '2026-02-18' THEN 1 ELSE 0 END) AS a_venir_par_la_date,
       SUM(CASE WHEN f.DATE_FIN IS NULL THEN 1 ELSE 0 END) AS sans_date_fin,
       SUM(CASE WHEN f.DATE_RETOUR IS NOT NULL THEN 1 ELSE 0 END) AS date_retour_saisie
FROM USER_GRH.GRH_FORMATION f
GROUP BY f.STATUT;
