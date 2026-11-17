-- =============================================================================
-- Point 132. L'agence d'un dossier
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « le service du dossier (DOSSIER, SP_NO), sinon celui
--   de sa réception (RECEPTION_DOSSIER, SP_NO). La liste est
--   SERVICE_PROVINCIAL : 15 agences et les 5 sièges des directions
--   régionales. » ; « l'annuaire CNSS 2024 cite 16 agences. Quatre d'entre
--   elles sont inactives dans la base (SP_ACTIVE = N) : Tanghin, Sakaby,
--   Diapaga, Tougan. »
-- Requête : les services provinciaux : direction régionale, libellé, ville,
--   actif ou non.
-- Reprise de : lot3_listes_de_codes.sql, P46 (exécutée le 29/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Liste de référence seulement, aucune donnée nominative.
-- Règle appliquée dans : dtm_dossier_requete_proposee.sql (NVL(d.SP_NO,
--   r.SP_NO) : le service du dossier, sinon celui de sa réception) ; pas
--   encore dans t25 à t27 (regles_t25_t27_dossiers.md, N2).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Services provinciaux (SERVICE_PROVINCIAL) : numéro, direction régionale,
-- sigle, libellé, ville, actif (SP_ACTIVE).
-- -----------------------------------------------------------------------------
SELECT SP_NO, DR_NO, SP_SIGLE, SP_DESC, SP_VILLE, SP_ACTIVE
FROM USER_DWH.SERVICE_PROVINCIAL
ORDER BY DR_NO, SP_NO;

-- Résultat obtenu (P46, exécutée le 29/10/2026 ; détail dans
-- resultats/lot3_P46_services_provinciaux.tsv) : 20 services, 15 agences et
-- 5 sièges de direction régionale. DRO : Kilwin, Dassasgho, Tanghin, siège,
-- Koudougou ; DRB : Sarfalao, Sakaby, siège, Houndé, Banfora, Diébougou,
-- Gaoua ; DRN : Dori, Kaya, siège ; DRF : siège, Diapaga, Tenkodogo ; DRD :
-- Tougan, siège. Inactifs (SP_ACTIVE = N) : Tanghin, Sakaby, Diapaga, Tougan.
-- Lecture : 15 agences dans la base, dont 4 inactives ; l'annuaire CNSS 2024
--   en cite 16.
