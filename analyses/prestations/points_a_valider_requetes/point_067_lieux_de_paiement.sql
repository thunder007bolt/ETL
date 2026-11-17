-- =============================================================================
-- Point 67. Les lieux de paiement
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « dans LIEU_PAIEMENT, le lieu 201, « caisse
--   tenkodogo », est rattaché à la DRB (DR_NO), sans service (SP_NO), alors
--   que Tenkodogo dépend de la DRF. »
-- Requêtes :
--   1. Les lieux de paiement : direction régionale, service, libellé.
--   2. Les services provinciaux, pour voir à quelle direction dépend
--      Tenkodogo.
-- Reprise de : lot3_listes_de_codes.sql, P47 et P46 (exécutées le
--   29/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Listes de référence seulement (lieux et services), aucune
--   donnée nominative.
-- Règle appliquée dans : aucune requête proposée ne lit LIEU_PAIEMENT ; la
--   direction et l'agence viennent du dossier (point 132).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Les lieux de paiement (LIEU_PAIEMENT) : numéro, direction
-- régionale (DR_NO), service (SP_NO), libellé.
-- -----------------------------------------------------------------------------
SELECT LP_NO, DR_NO, SP_NO, DR_OU_SP, LP_DESC
FROM USER_DWH.LIEU_PAIEMENT
ORDER BY DR_NO, SP_NO, LP_NO;

-- Résultat obtenu (P47, exécutée le 29/10/2026 ; détail dans
-- resultats/lot3_P47_lieux_paiement.tsv), le lieu 201 :
--   LP_NO   DR_NO   SP_NO   Libellé
--   201     20      (vide)  caisse tenkodogo
-- 350 lieux en tout.


-- -----------------------------------------------------------------------------
-- Requête 2. Les services provinciaux (SERVICE_PROVINCIAL) : numéro, direction
-- régionale, libellé, ville, actif ou non.
-- -----------------------------------------------------------------------------
SELECT SP_NO, DR_NO, SP_SIGLE, SP_DESC, SP_VILLE, SP_ACTIVE
FROM USER_DWH.SERVICE_PROVINCIAL
ORDER BY DR_NO, SP_NO;

-- Résultat obtenu (P46, exécutée le 29/10/2026 ; détail dans
-- resultats/lot3_P46_services_provinciaux.tsv) : l'agence de Tenkodogo
-- (SP_NO 4110) dépend de la direction 40, la DRF ; la direction 20 est la DRB.
-- Lecture : le lieu 201 « caisse tenkodogo » est rattaché à la DRB, alors que
--   Tenkodogo dépend de la DRF.
