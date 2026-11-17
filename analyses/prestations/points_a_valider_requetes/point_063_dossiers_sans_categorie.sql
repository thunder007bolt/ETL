-- =============================================================================
-- Point 63. Les dossiers sans catégorie
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « DOSSIER, DOS_CATEGORIE vide. Les autres valeurs sont
--   COOR_AC, COOR_CNSS et CNSS_AC (voir le point 136). » ; « 120 518 dossiers
--   n'ont pas de catégorie. »
-- Requête : les catégories de dossier, avec leur libellé et leur nombre.
-- Reprise de : lot3_listes_de_codes.sql, P33 (exécutée le 29/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t25_requete_proposee.sql, bloc prest
--   (NVL(DOS_CATEGORIE, 'ORDINAIRE')) : un dossier sans catégorie est traité
--   comme ordinaire. Fiche des règles : règle 18.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Catégories de dossier (DOS_CATEGORIE) : libellé et nombre de dossiers, et
-- dossiers sans catégorie.
-- -----------------------------------------------------------------------------
SELECT c.DOS_CATEGORIE, c.CDP_DESCRIPTION,
       (SELECT COUNT(*) FROM USER_DWH.DOSSIER d WHERE d.DOS_CATEGORIE = c.DOS_CATEGORIE) AS nb_dossiers
FROM USER_DWH.CATEGORIE_DOSSIER_PENSION c
ORDER BY c.DOS_CATEGORIE;

-- Résultat obtenu (P33, exécutée le 29/10/2026) : ORDINAIRE 240 472,
-- COOR_CNSS 3 473, COOR_AC 3 326, CNSS_AC 158 ; HORS_CONV jamais utilisé.
-- 247 429 dossiers ont une catégorie (toutes branches), 120 518 n'en ont pas.
-- Lecture : un dossier sur trois n'a pas de catégorie ; il compte partout
--   comme un dossier ordinaire.
