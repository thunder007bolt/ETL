-- =============================================================================
-- Point 12. Les accessoires de pension
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « PRESTATION_ESP, codes RBEC, RME et RME-COL, RCCM. » ;
--   « l'annuaire CNSS 2024 fait pareil : il publie la majoration pour enfant
--   (537,3 M en 2024) à part, hors de la pension de retraite (38,24 Md). »
-- Requête : les paiements de 2024 par code de prestation, selon la date
--   retenue.
-- Reprise de : lot8_evolution_5_ans.sql, P98 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc perimetre (les
--   accessoires ne font pas de bénéficiaire et leur argent n'entre pas dans
--   T32). Fiche des règles : règles 9 et 53.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Paiements (PM) au statut D ou P de 2024, par code de prestation : au mois
-- payé, à la saisie, à l'approbation, au décaissement.
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
-- resultats/lot8_P98_montants_2024_par_date.tsv), les accessoires et la
-- vieillesse :
--   Code      Mois payé 2024    Saisi 2024
--   RBEC         554 254 000       539 376 000
--   RME            3 942 912         4 006 000
--   RME-COL          146 880            73 440
--   RCCM          10 370 122        10 315 942
--   RPV       38 496 066 498    38 821 571 458
-- Face au tableau 34 de l'annuaire CNSS 2024 (p. 28) : majoration pour enfant
-- en charge 537 343 000 ; PRV 38 235 880 033, sur des lignes séparées.
-- Lecture : l'annuaire publie la majoration à part, hors de la pension de
--   retraite ; la base l'a sous son propre code (RBEC).
