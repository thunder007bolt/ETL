-- =============================================================================
-- LOT 5 — Recoupements entre décisions, embauches et état des agents
-- Exécuté le 21/10/2026 sur USER_GRH.
-- Tableau CIPRES visé : T60.
-- Objectif du lot : confirmer que les décisions concordent avec GRH_PERSONNE,
-- et comprendre les annulations et modifications.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q19 — Agents visés par les décisions collectives, par année
-- Objectif : compter les agents réels derrière les décisions 14, 18 et 51.
-- Résultat : 51 engagement d'agent : 84 agents en 2019 (25 décisions),
--            106 en 2024 (30), 157 en 2025 (32). 18 remise en activité :
--            3 à 6 agents par an. 14 mise à disposition : 1 à 3 par an.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM d.DATE_VIGUEUR) AS annee, d.CDEC_CODE,
       COUNT(DISTINCT d.DECI_NO) AS nb_deci,
       COUNT(DISTINCT NVL(d.PERS_ID, dp.PERS_ID)) AS nb_agents
FROM USER_GRH.GRH_DECISION d
LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
WHERE d.CDEC_CODE IN ('14','18','51')
  AND d.DATE_VIGUEUR >= DATE '2019-01-01' AND d.DATE_VIGUEUR < DATE '2027-01-01'
GROUP BY EXTRACT(YEAR FROM d.DATE_VIGUEUR), d.CDEC_CODE
ORDER BY annee, d.CDEC_CODE;


-- -----------------------------------------------------------------------------
-- Q20 — Embauches de GRH_PERSONNE et décisions d'engagement
-- Objectif : choisir la source de la ligne « Recrutements » de T60.
-- Résultat : 2019 : 122 embauches = 84 en 51 + 38 en 50, 1 seul encore actif.
--            2024 : 140 = 103 en 51 + 35 en 50, 102 encore actifs.
--            2025 : 201 = 156 en 51 + 42 en 50, 153 encore actifs.
--            2020 à 2023 : presque uniquement des temporaires (50).
-- Constat  : sources concordantes ; anomalie des 84 permanents de 2019 partis.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM p.DATE_EMBAUCHE) AS annee,
       COUNT(*) AS embauches,
       SUM(CASE WHEN p.ETAT_EMPLOYE = 'ACTIF' THEN 1 ELSE 0 END) AS encore_actifs,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION d
                             LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
                             WHERE d.CDEC_CODE = '51' AND NVL(d.PERS_ID, dp.PERS_ID) = p.PERS_ID)
                THEN 1 ELSE 0 END) AS avec_engagement_51,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION d
                             WHERE d.CDEC_CODE = '50' AND d.PERS_ID = p.PERS_ID)
                THEN 1 ELSE 0 END) AS avec_temporaire_50
FROM USER_GRH.GRH_PERSONNE p
WHERE p.DATE_EMBAUCHE >= DATE '2019-01-01'
GROUP BY EXTRACT(YEAR FROM p.DATE_EMBAUCHE)
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- Q21 — Ce que visent les annulations (40) et les modifications (41)
-- Objectif : savoir quelles décisions écarter ou corriger.
-- Résultat : 41 modifie surtout des retraites (218), liquidations (111),
--            congés (66) et temporaires (43) ; aussi 6 licenciements et
--            3 démissions. 40 annule environ 85 décisions, dont 17 autorisations
--            d'absence, 10 temporaires et 3 retraites. 22 retraites sont
--            reprises par une nouvelle décision de retraite (02 → 02).
-- Note     : les TO_CHAR évitent un ORA-01722 si NO_DECI_MODIF contient du texte.
-- -----------------------------------------------------------------------------
SELECT a.CDEC_CODE AS cat_acte, o.CDEC_CODE AS cat_visee, COUNT(*) AS nb
FROM USER_GRH.GRH_DECISION a
LEFT JOIN USER_GRH.GRH_DECISION o ON TO_CHAR(o.DECI_NO) = TO_CHAR(a.NO_DECI_MODIF)
WHERE a.CDEC_CODE IN ('40','41') OR a.NO_DECI_MODIF IS NOT NULL
GROUP BY a.CDEC_CODE, o.CDEC_CODE
ORDER BY nb DESC;


-- -----------------------------------------------------------------------------
-- Q22 — Cohérence des retraites : décision de retraite et état de l'agent
-- Objectif : vérifier que les retraités par décision sont bien inactifs.
-- Résultat : 816 agents ont une décision de retraite : 812 INACTIF (dernière
--            vigueur le 05/01/2026), 4 ACTIF (dernière vigueur le 15/12/2025,
--            sans doute les retraites annulées).
-- Constat  : les décisions sont plus fiables que le statut R (584 agents).
-- -----------------------------------------------------------------------------
SELECT p.ETAT_EMPLOYE, COUNT(DISTINCT d.PERS_ID) AS agents_avec_retraite,
       MAX(d.DATE_VIGUEUR) AS derniere_vigueur
FROM USER_GRH.GRH_DECISION d
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
WHERE d.CDEC_CODE = '02'
GROUP BY p.ETAT_EMPLOYE;
