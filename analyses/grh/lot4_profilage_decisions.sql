-- =============================================================================
-- LOT 4 — Profilage des décisions RH (GRH_DECISION)
-- Exécuté le 21/10/2026 sur USER_GRH.
-- Tableau CIPRES visé : T60 (recrutements, retraites, autres entrées et sorties).
-- Objectif du lot : savoir si les décisions sont exploitables (validité, lien
-- avec l'agent, volumes par an).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q16 — Drapeaux de validité des décisions utiles
-- Objectif : trouver un filtre « décision valable ».
-- Résultat : STATUT = N, VALIDITE = N, DECI_EXCLURE = O pour toutes les
--            décisions ; DECI_VERIFIE vaut presque toujours N.
-- Constat  : aucun drapeau n'est utilisable comme filtre.
-- -----------------------------------------------------------------------------
SELECT d.CDEC_CODE, d.STATUT, d.VALIDITE, d.DECI_EXCLURE, d.DECI_VERIFIE, COUNT(*) AS nb
FROM USER_GRH.GRH_DECISION d
WHERE d.CDEC_CODE IN ('02','07','14','18','35','36','37','50','51','52','54')
GROUP BY d.CDEC_CODE, d.STATUT, d.VALIDITE, d.DECI_EXCLURE, d.DECI_VERIFIE
ORDER BY d.CDEC_CODE, nb DESC;


-- -----------------------------------------------------------------------------
-- Q17 — Où se trouve l'agent visé par une décision
-- Objectif : savoir si l'agent est sur GRH_DECISION ou dans une table de lien.
-- Résultat : décisions individuelles (02, 07, 36, 50, 54) : PERS_ID sur
--            GRH_DECISION. Décisions collectives (14, 15, 18, 27, 30, 51) :
--            agents dans GRH_DECISION_PERSONNE. GRH_DECISION_CARRIERE jamais
--            utilisée. Quelques décisions sans agent (112 affectations,
--            12 engagements…).
-- Règle    : agent = NVL(d.PERS_ID, dp.PERS_ID).
-- -----------------------------------------------------------------------------
SELECT d.CDEC_CODE, COUNT(*) AS nb_deci,
       COUNT(d.PERS_ID) AS avec_pers_id,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION_PERSONNE dp WHERE dp.DECI_NO = d.DECI_NO)
                THEN 1 ELSE 0 END) AS avec_deci_personne,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION_CARRIERE dc WHERE dc.DECI_NO = d.DECI_NO)
                THEN 1 ELSE 0 END) AS avec_deci_carriere
FROM USER_GRH.GRH_DECISION d
WHERE d.CDEC_CODE IN ('02','07','14','15','18','27','30','36','50','51','54')
GROUP BY d.CDEC_CODE
ORDER BY d.CDEC_CODE;


-- -----------------------------------------------------------------------------
-- Q18 — Volumes par année depuis 2019 (décisions individuelles)
-- Objectif : vérifier la vraisemblance des volumes de T60.
-- Résultat : retraites (agents) 40, 39, 32, 30, 30, 25, 23 de 2019 à 2025 ;
--            temporaires 54, 22, 22, 25, 27, 62, 84 agents (beaucoup de
--            renouvellements) ; licenciements 0 à 3 par an ; démissions 0 à 2.
--            La colonne nb_agents est à 0 pour 51 car ses agents sont dans
--            GRH_DECISION_PERSONNE (voir Q19).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM d.DATE_VIGUEUR) AS annee, d.CDEC_CODE,
       COUNT(*) AS nb_deci, COUNT(DISTINCT d.PERS_ID) AS nb_agents
FROM USER_GRH.GRH_DECISION d
WHERE d.CDEC_CODE IN ('02','36','50','51','54')
  AND d.DATE_VIGUEUR >= DATE '2019-01-01' AND d.DATE_VIGUEUR < DATE '2027-01-01'
GROUP BY EXTRACT(YEAR FROM d.DATE_VIGUEUR), d.CDEC_CODE
ORDER BY annee, d.CDEC_CODE;
