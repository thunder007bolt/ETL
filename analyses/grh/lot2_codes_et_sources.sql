-- =============================================================================
-- LOT 2 — Sens des codes et recherche des sources d'entrées et de sorties
-- Exécuté le 21/10/2026 sur USER_GRH.
-- Tableaux CIPRES visés : T60 ; indirectement T58, T59, T61, T65 (effectif).
-- Objectif du lot : comprendre les codes de situation et de statut, et trouver
-- où sont enregistrés les recrutements et les départs.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q08 — Listes de codes de l'application (GRH_REF_CODES)
-- Objectif : connaître le sens officiel de VALIDE, des natures de situation
--            et des statuts.
-- Résultat : aucun domaine pour VALIDE (le domaine STATUT ne connaît que O, N, Z).
--            NATURE_SITUATION : REC = Reclassement (pas recrutement),
--            RET = Rétrogradation (pas retraite), CLA = Classement à l'embauche,
--            DIS = Disposition (mise à disposition).
--            STATUT_EMPLOYE : A activité normale, B détachement, C stage,
--            D mise à disposition, E disponibilité, F suspendu, G congé payé,
--            H sous les drapeaux, R retraite, X radié, Y décédé.
--            LOV_MOUVEMENT : 14 Disposition, 15 Affectation, 18 Remise en
--            activité, 19 à 71 nominations à un poste.
-- Constat  : le DTM convertit DIS en DISP (disponibilité) alors que DIS veut
--            dire mise à disposition.
-- -----------------------------------------------------------------------------
SELECT RV_DOMAIN, RV_LOW_VALUE, RV_MEANING
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN LIKE '%VALID%' OR RV_DOMAIN LIKE '%MVT%' OR RV_DOMAIN LIKE '%MOUV%'
   OR RV_DOMAIN LIKE '%SITU%'  OR RV_DOMAIN LIKE '%ETAT%' OR RV_DOMAIN LIKE '%STATUT%'
ORDER BY RV_DOMAIN, RV_LOW_VALUE;


-- -----------------------------------------------------------------------------
-- Q09 — Natures de situation croisées avec le statut de l'agent
-- Objectif : voir si les entrées et sorties sont enregistrées comme situations.
-- Résultat : 10 906 situations. CLA (classement à l'embauche) : 3 418, environ
--            une par agent. Les statuts de sortie (R, X, Y) sont portés par
--            des situations de toute nature (AVA, AFF, CLA…). Plus de 3 300
--            situations sans statut, surtout avant 2003. Années aberrantes :
--            212 (CLA), 2201 (REC), 3015 (AFF, CLA, NOM).
-- -----------------------------------------------------------------------------
SELECT UPPER(TRIM(NATURE_SITUATION)) AS nature, STATUT_EMPLOYE, COUNT(*) AS nb,
       MIN(EXTRACT(YEAR FROM DATE_DEBUT)) AS an_min, MAX(EXTRACT(YEAR FROM DATE_DEBUT)) AS an_max
FROM USER_GRH.GRH_SITUATION
GROUP BY UPPER(TRIM(NATURE_SITUATION)), STATUT_EMPLOYE
ORDER BY nature, STATUT_EMPLOYE;


-- -----------------------------------------------------------------------------
-- Q10 — État des agents et dates d'entrée et de sortie (GRH_PERSONNE)
-- Objectif : voir quelles dates d'entrée et de sortie sont renseignées.
-- Résultat : INACTIF 2 234 (59 dates de décès), ACTIF 879, vide 67 (embauches
--            récentes, jusqu'au 28/01/2026). Date d'embauche renseignée pour
--            100 % des agents.
-- -----------------------------------------------------------------------------
SELECT ETAT_EMPLOYE, COUNT(*) AS nb,
       COUNT(DATE_EMBAUCHE) AS avec_embauche, COUNT(DATE_PRISE_SERVICE) AS avec_prise_service,
       COUNT(DATE_DECES) AS avec_deces, MAX(DATE_EMBAUCHE) AS derniere_embauche
FROM USER_GRH.GRH_PERSONNE
GROUP BY ETAT_EMPLOYE
ORDER BY nb DESC;


-- -----------------------------------------------------------------------------
-- Q11 — Embauches par année depuis 2018
-- Objectif : ordre de grandeur des recrutements attendus dans T60.
-- Résultat : 2018 16 · 2019 122 · 2020 6 · 2021 7 · 2022 12 · 2023 11 ·
--            2024 140 · 2025 201 · 2026 4.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM NVL(DATE_EMBAUCHE, DATE_PRISE_SERVICE)) AS annee, COUNT(*) AS nb
FROM USER_GRH.GRH_PERSONNE
WHERE NVL(DATE_EMBAUCHE, DATE_PRISE_SERVICE) >= DATE '2018-01-01'
GROUP BY EXTRACT(YEAR FROM NVL(DATE_EMBAUCHE, DATE_PRISE_SERVICE))
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q12 — Colonnes qui ressemblent à un départ ou une arrivée
-- Objectif : trouver une date de retraite, de radiation ou de sortie.
-- Résultat : aucune. Seule GRH_PERSONNE.DATE_DECES est une date de sortie.
--            GRH_DECISION_PERSONNE.DEPART concerne les congés (avec RETOUR,
--            JOUR_DEDUCT, CONGE_SUPP_*).
-- -----------------------------------------------------------------------------
SELECT table_name, column_name, data_type
FROM ALL_TAB_COLUMNS
WHERE owner = 'USER_GRH'
  AND (column_name LIKE '%DEPART%' OR column_name LIKE '%SORTIE%' OR column_name LIKE '%CESS%'
       OR column_name LIKE '%RETRAI%' OR column_name LIKE '%RECRUT%' OR column_name LIKE '%DECES%'
       OR column_name LIKE '%MOTIF%')
ORDER BY table_name, column_name;
