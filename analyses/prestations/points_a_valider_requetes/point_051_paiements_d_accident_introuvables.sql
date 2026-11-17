-- =============================================================================
-- Point 51. Des paiements d'accident introuvables
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « les paiements (DEBOURS, DEB_TYPE = PM, statut D ou
--   P) et les remboursements de soins (DEB_TYPE = RB) des dossiers d'accident
--   du travail (DOSSIER, TDOS_CODE = A). » ; « soins de 2024 : 18,8 M dans la
--   base pour 139,7 M publiés. Rentes viagères de 2024 : 399,4 M pour
--   455,0 M publiés (tableau 90). »
-- Requêtes :
--   1. Les montants servis de 2024 par branche et par type de débours.
--   2. Les paiements de 2024 par code de prestation, selon la date retenue.
-- Reprise de : lot8_evolution_5_ans.sql, P95 et P98 (exécutées le
--   01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Les deux
--   requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc paie de T56
--   (paiements PM et soins RB). Fiche des règles : règle 72.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Débours au statut D ou P de 2024, par branche et par type.
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
-- resultats/lot8_P95_montants_par_branche.tsv), accidents du travail (A) en
-- 2024 : paiements 744 609 484 ; soins remboursés 18 787 945.
-- Face au tableau 82 de l'annuaire CNSS 2024 (p. 63) : 139 668 695 de
-- prestations en nature.


-- -----------------------------------------------------------------------------
-- Requête 2. Paiements (PM) au statut D ou P de 2024 par code de prestation.
-- -----------------------------------------------------------------------------
SELECT p.TPE_CODE                                                 AS code,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_EFFET >= DATE '2024-01-01'
                       AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS mois_paye_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                       AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS saisi_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_APPROBATION >= DATE '2024-01-01'
                       AND deb.DEB_DATE_APPROBATION <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS approuve_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_DECAISSEMENT >= DATE '2024-01-01'
                       AND deb.DEB_DATE_DECAISSEMENT <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS decaisse_2024,
       SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                 AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                 AND deb.DEB_DATE_DECAISSEMENT IS NULL
                THEN 1 ELSE 0 END)                                AS saisis_sans_decaissement
FROM USER_DWH.DEBOURS deb
JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID = deb.PE_ID
WHERE deb.DEB_TYPE = 'PM'
  AND deb.DEB_STATUT IN ('D', 'P')
  AND (   (deb.DEB_DATE_EFFET        >= DATE '2024-01-01' AND deb.DEB_DATE_EFFET        < DATE '2025-01-01')
       OR (deb.DEB_DATE_INSERT       >= DATE '2024-01-01' AND deb.DEB_DATE_INSERT       < DATE '2025-01-01')
       OR (deb.DEB_DATE_APPROBATION  >= DATE '2024-01-01' AND deb.DEB_DATE_APPROBATION  < DATE '2025-01-01')
       OR (deb.DEB_DATE_DECAISSEMENT >= DATE '2024-01-01' AND deb.DEB_DATE_DECAISSEMENT < DATE '2025-01-01'))
GROUP BY p.TPE_CODE
ORDER BY p.TPE_CODE;

-- Résultat obtenu (P98, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P98_montants_2024_par_date.tsv), rentes viagères (ARI) :
-- 399 428 069 au mois payé, 397 834 986 à la saisie.
-- Face au tableau 90 de l'annuaire CNSS 2024 (p. 71, ligne Total) :
-- 455 021 317 de rentes viagères.
-- Lecture : 18,8 M de soins pour 139,7 M publiés ; 399,4 M de rentes viagères
--   pour 455,0 M. Une partie de l'argent des accidents du travail est payée
--   hors de la base des prestations.
