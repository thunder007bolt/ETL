-- =============================================================================
-- Point 60. La date de décaissement
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « DEBOURS, DEB_DATE_DECAISSEMENT, et EFFET,
--   EFP_DATE_DECAISSEMENT. Seuls des paiements au statut D l'ont ; ceux au
--   statut P jamais. » ; « elle manque pour 80 % des paiements des
--   prestations familiales et 43 % de ceux de vieillesse de 2025. Là où elle
--   existe, elle colle parfois mieux : en 2024, les indemnités journalières
--   d'accident du travail font 60,8 M au versement, plus près des 65,4 M de
--   l'annuaire CNSS 2024 que la date de saisie. »
-- Requêtes :
--   1. Les débours de 2025 par branche et par type : sans date de
--      décaissement.
--   2. Les paiements de 2024 par code, au mois payé, à la saisie et au
--      décaissement.
-- Reprise de : lot1_inventaire_prestations.sql, P07 (exécutée le
--   26/10/2026) ; lot8_evolution_5_ans.sql, P98 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : le réglage date_montant ('MOIS') des requêtes
--   proposées ; la date de décaissement n'y sert pas. Fiche des règles :
--   règle 8.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Débours de 2025 par branche et par type : nombre, montant, sans
-- effet, sans date de décaissement.
-- -----------------------------------------------------------------------------
SELECT deb.CODE_BRANCHE, d.TDOS_CODE AS branche_dossier, deb.DEB_TYPE, td.DESC_TYPE_DEB,
       CASE WHEN deb.TPE_CODE IS NOT NULL THEN 'ESPECES'
            WHEN deb.TPN_CODE IS NOT NULL THEN 'NATURE'
            ELSE 'SANS CODE' END                                        AS code_prestation,
       COUNT(*)                                                          AS nb_debours,
       SUM(deb.DEB_MONTANT)                                              AS montant,
       SUM(CASE WHEN deb.DEB_MONTANT < 0 THEN 1 ELSE 0 END)              AS nb_negatifs,
       SUM(CASE WHEN deb.DEB_DATE_ANNUL IS NOT NULL THEN 1 ELSE 0 END)   AS nb_annules,
       SUM(CASE WHEN deb.DEB_DATE_ANNUL IS NOT NULL
                THEN deb.DEB_MONTANT ELSE 0 END)                         AS montant_annule,
       SUM(CASE WHEN deb.EFP_ID IS NULL THEN 1 ELSE 0 END)               AS nb_sans_effet,
       SUM(CASE WHEN deb.DEB_DATE_DECAISSEMENT IS NULL THEN 1 ELSE 0 END) AS nb_non_decaisses
FROM USER_DWH.DEBOURS deb
LEFT JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE       = deb.DOS_CODE
LEFT JOIN USER_DWH.TYPE_DEBOURS td ON td.CODE_TYPE_DEB = deb.DEB_TYPE
WHERE deb.DEB_DATE_EFFET >= DATE '2025-01-01' AND deb.DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY deb.CODE_BRANCHE, d.TDOS_CODE, deb.DEB_TYPE, td.DESC_TYPE_DEB,
         CASE WHEN deb.TPE_CODE IS NOT NULL THEN 'ESPECES'
              WHEN deb.TPN_CODE IS NOT NULL THEN 'NATURE'
              ELSE 'SANS CODE' END
ORDER BY 1, 2, 3, 5;

-- Résultat obtenu (P07, exécutée le 26/10/2026) : paiements de 2025 sans
-- date de décaissement : F 2 549 446 (80 %), V 390 509 (43 %).


-- -----------------------------------------------------------------------------
-- Requête 2. Paiements (PM) au statut D ou P de 2024 par code : au mois payé,
-- à la saisie, à l'approbation, au décaissement.
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
-- resultats/lot8_P98_montants_2024_par_date.tsv), IJ d'accident du travail
-- (AIJ) en 2024 : 46 020 175 au mois payé, 51 859 178 à la saisie,
-- 60 773 127 au décaissement.
-- Face au tableau 97 de l'annuaire CNSS 2024 (p. 74) : 65 420 082.
-- Lecture : au décaissement, 60,8 M pour 65,4 M publiés, plus près que la
--   saisie (51,9 M) ; mais la date manque pour 80 % des paiements PF.
