-- =============================================================================
-- Point 59. Les paiements sans effet
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « un débours de paiement (DEB_TYPE = PM) au statut P
--   (DEB_STATUT) n'a pas d'effet (EFFET). Ce circuit existe depuis 2019. » ;
--   « 62 % des paiements des prestations familiales de 2025 n'ont pas d'effet
--   de paiement. »
-- Requêtes :
--   1. Les débours de 2025 par branche et par type : sans effet, sans date de
--      décaissement.
--   2. Les statuts des paiements par année et par branche, de 2019 à 2025.
-- Reprise de : lot1_inventaire_prestations.sql, P07 (exécutée le
--   26/10/2026) ; lot2_statuts_stocks_nouveaux.sql, P22 (exécutée le
--   26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc payees, et
--   t32_t41_requete_proposee.sql, bloc paie (le statut P compte comme payé).
--   Fiche des règles : règle 7.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Débours de 2025 par branche et par type : nombre, montant, sans
-- effet (EFP_ID vide), sans date de décaissement.
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

-- Résultat obtenu (P07, exécutée le 26/10/2026) : paiements (PM) de 2025
-- sans effet : F 1 993 982 (62 %), V 208 966 (23 %), M 2 540, A 2 764.


-- -----------------------------------------------------------------------------
-- Requête 2. Paiements (PM) par année et par branche, de 2019 à 2025, selon
-- leur statut.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM DEB_DATE_EFFET) AS annee, CODE_BRANCHE,
       SUM(CASE WHEN DEB_STATUT = 'A' THEN 1 ELSE 0 END) AS statut_a,
       SUM(CASE WHEN DEB_STATUT = 'D' THEN 1 ELSE 0 END) AS statut_d,
       SUM(CASE WHEN DEB_STATUT = 'I' THEN 1 ELSE 0 END) AS statut_i,
       SUM(CASE WHEN DEB_STATUT = 'N' THEN 1 ELSE 0 END) AS statut_n,
       SUM(CASE WHEN DEB_STATUT = 'P' THEN 1 ELSE 0 END) AS statut_p,
       SUM(CASE WHEN DEB_STATUT = 'S' THEN 1 ELSE 0 END) AS statut_s,
       SUM(CASE WHEN DEB_STATUT = 'V' THEN 1 ELSE 0 END) AS statut_v,
       SUM(CASE WHEN DEB_STATUT IS NULL
                  OR DEB_STATUT NOT IN ('A','D','I','N','P','S','V')
                THEN 1 ELSE 0 END)                       AS autre
FROM USER_DWH.DEBOURS
WHERE DEB_TYPE = 'PM'
  AND DEB_DATE_EFFET >= DATE '2019-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM DEB_DATE_EFFET), CODE_BRANCHE
ORDER BY CODE_BRANCHE, annee;

-- Résultat obtenu (P22, exécutée le 26/10/2026) : de 2019 à 2025, F : D
-- 2 293 821 → 1 092 072, P 614 288 → 1 983 029 ; V : P 137 692 → 206 963.
-- Lecture : le statut P existe depuis 2019 : c'est un circuit de paiement
--   stable ; en 2025, 62 % des paiements des prestations familiales n'ont pas
--   d'effet, donc pas de mode de versement connu (virement ou non).
