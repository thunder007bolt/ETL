-- =============================================================================
-- Point 65. La copie de la base
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « la copie date du 02/02/2026. »
-- Requête : le volume de chaque table source et la date de sa dernière
--   saisie.
-- Reprise de : lot1_inventaire_prestations.sql, P01 (exécutée le
--   26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. Lit DEBOURS
--   en entier : plusieurs minutes.
-- Règle appliquée dans : toutes les requêtes proposées reçoivent la photo de
--   la base en paramètre (:1, la colonne CLICHE). Fiche des règles :
--   règles 1 et 2.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par table source : nombre de lignes et date de la dernière saisie.
-- -----------------------------------------------------------------------------
SELECT 'RECEPTION_DOSSIER' AS table_source, COUNT(*) AS nb_lignes, MAX(RD_DATE_INSERT) AS derniere_saisie
FROM USER_DWH.RECEPTION_DOSSIER
UNION ALL
SELECT 'DOSSIER', COUNT(*), MAX(DOS_DATE_INSERT) FROM USER_DWH.DOSSIER
UNION ALL
SELECT 'DOSSIER_REJETE', COUNT(*), MAX(DOSRE_DATE_INSERT) FROM USER_DWH.DOSSIER_REJETE
UNION ALL
SELECT 'PRESTATION_ESP', COUNT(*), MAX(PE_DATE_INSERT) FROM USER_DWH.PRESTATION_ESP
UNION ALL
SELECT 'NOTIFICATION_PREST', COUNT(*), MAX(NOT_DATE_INSERT) FROM USER_DWH.NOTIFICATION_PREST
UNION ALL
SELECT 'ETAPE', COUNT(*), MAX(EA_DATE_INSERT) FROM USER_DWH.ETAPE
UNION ALL
SELECT 'DEBOURS', COUNT(*), MAX(DEB_DATE_INSERT) FROM USER_DWH.DEBOURS
UNION ALL
SELECT 'EFFET', COUNT(*), MAX(EFP_DATE_INSERT) FROM USER_DWH.EFFET
UNION ALL
SELECT 'AJUSTEMENT', COUNT(*), MAX(AJ_DATE_INSERT) FROM USER_DWH.AJUSTEMENT
UNION ALL
SELECT 'DOSSIERS_CONTROLES', COUNT(*), MAX(DC_DATE_INSERT) FROM USER_DWH.DOSSIERS_CONTROLES
UNION ALL
SELECT 'INDIVIDU', COUNT(*), MAX(IND_DATE_INSERT) FROM USER_DWH.INDIVIDU
UNION ALL
SELECT 'LIEN', COUNT(*), MAX(LN_DATE_INSERT) FROM USER_DWH.LIEN
UNION ALL
SELECT 'TSINISTRE', COUNT(*), MAX(DOS_DATE_ACCIDENT) FROM USER_DWH.TSINISTRE;

-- Résultat obtenu (P01, exécutée le 26/10/2026) : copie arrêtée au
-- 02/02/2026 : dernière saisie le 02/02/26 pour 10 tables. DEBOURS
-- 71 709 877 lignes, EFFET 8 711 190, ETAPE 1 705 341, INDIVIDU 1 543 286,
-- AJUSTEMENT 1 355 405, LIEN 1 308 962, PRESTATION_ESP 947 953, DOSSIER
-- 367 947, NOTIFICATION_PREST 304 355, RECEPTION_DOSSIER 273 005, TSINISTRE
-- 25 086 (dernier accident le 30/12/24), DOSSIERS_CONTROLES 1 542 (dernière
-- saisie le 09/07/25), DOSSIER_REJETE 486 (20/01/26).
-- Lecture : la copie est figée au 02/02/2026 ; pour 2025, il lui manque ce
--   qui a été saisi en février et en mars 2026.
