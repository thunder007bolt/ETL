-- =============================================================================
-- Point 129. Les frais médicaux de maternité
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « la liste des types de prestation prévoit ces 7
--   frais, mais la base n'a aucun paiement (DEBOURS) de ces frais de 2020 à
--   2025. » ; « l'annuaire CNSS 2024 en publie 25,0 M payés en 2024 (tableaux
--   40 et 41). »
-- Requêtes :
--   1. Les types de prestation en nature et leurs catégories, dont les frais
--      de maternité.
--   2. Les débours de 2020 à 2025 par branche et par type, pour voir qu'aucun
--      soin n'est remboursé en maternité.
-- Reprise de : lot3_listes_de_codes.sql, P35 (exécutée le 29/10/2026) ;
--   lot8_evolution_5_ans.sql, P95 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t27_requete_proposee.sql (la ligne « Frais médicaux
--   de maternité » reste vide) ; t50_t57_requete_proposee.sql, bloc paie de
--   T56. Fiche des règles : règle 36 et la ligne « À faire (DSI) ».
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Types de prestation en nature (TPN_CODE) et leurs catégories
-- (CPN_CODE), avec la branche.
-- -----------------------------------------------------------------------------
SELECT c.CPN_CODE, c.CPN_DESCRIPTION, c.TDOS_CODE AS tdos_categorie,
       n.TPN_CODE, n.TPN_DESCRIPTION, n.CODE_BRANCHE
FROM USER_DWH.TYPE_PRESTATION_NAT n
LEFT JOIN USER_DWH.CATEGORIE_PRESTATION_NAT c ON c.CPN_CODE = n.CPN_CODE
ORDER BY c.CPN_CODE, n.TPN_CODE;

-- Résultat obtenu (P35, exécutée le 29/10/2026 ; détail dans
-- resultats/lot3_P35_prestations_nature.tsv) : 51 types en 24 catégories ;
-- une catégorie de la maternité, FM frais de maternité, avec 7 types :
-- ACCOUCHE, CONSULT, EXAMENS_LABO, HOSPITALIS, KITOPERAT, PHARMACIE,
-- RADIO_MAT.


-- -----------------------------------------------------------------------------
-- Requête 2. Débours au statut D ou P de 2020 à 2025, par année, branche et
-- type (PM paiements, RB soins remboursés, AJ ajustements).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                      AS annee,
       NVL(deb.CODE_BRANCHE, '?')                                 AS branche,
       deb.DEB_TYPE                                               AS type_debours,
       COUNT(*)                                                   AS debours,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant,
       SUM(CASE WHEN deb.PE_ID IS NULL THEN 1 ELSE 0 END)         AS sans_prestation,
       ROUND(SUM(CASE WHEN deb.PE_ID IS NULL
                      THEN deb.DEB_MONTANT END))                  AS montant_sans_prestation
FROM USER_DWH.DEBOURS deb
WHERE deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
  AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM deb.DEB_DATE_EFFET), NVL(deb.CODE_BRANCHE, '?'), deb.DEB_TYPE
ORDER BY annee, branche, type_debours;

-- Résultat obtenu (P95, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P95_montants_par_branche.tsv) : les soins remboursés (RB)
-- n'existent qu'en accidents du travail (A) ; la maternité (M) n'a que des
-- paiements (PM) et des ajustements (AJ), de 2020 à 2025.
-- Face aux tableaux 40 (p. 32) et 41 (p. 33) de l'annuaire CNSS 2024 :
-- 25 030 869 de frais médicaux de maternité en 2024.
-- Lecture : les 7 frais sont prévus dans la liste des types, mais aucun n'est
--   payé dans la base : les 25,0 M publiés sont enregistrés ailleurs.
