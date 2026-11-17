-- =============================================================================
-- Point 66. La vue des décédés (T57)
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « elle prend toutes les prestations notifiées dont
--   l'ayant droit a une date de décès, sans filtre de type ni d'année, avec le
--   nom, le prénom, l'adresse et le téléphone des décédés. »
-- Requête : le texte de la vue V_PRESTATAIRES_DECEDES_BI, lu dans le
--   catalogue (ALL_VIEWS). Seul le texte SQL de la vue sort, aucune ligne de
--   la vue.
-- Reprise de : lot8_evolution_5_ans.sql, P101 (exécutée le 01/11/2026, sur
--   une capture d'écran).
-- Base : la base source, comme les lots. SQL Developer, mode script (F5).
--   Ne lit que le catalogue, aucune donnée.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, blocs deces, en_cours,
--   victimes, allocataires et decedes : T57 part des tables INDIVIDU,
--   PRESTATION_ESP et DOSSIER, pas de la vue. Fiche des règles : règle 74.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Le texte de la vue (catalogue ALL_VIEWS).
-- -----------------------------------------------------------------------------
SELECT VIEW_NAME,
       TEXT_LENGTH,
       TEXT
FROM ALL_VIEWS
WHERE OWNER     = 'USER_DWH'
  AND VIEW_NAME = 'V_PRESTATAIRES_DECEDES_BI';

-- Résultat obtenu (P101, exécutée le 01/11/2026 ; texte relevé dans
-- resultats/lot8_P101_vue_prestataires_decedes.sql) : la vue joint
-- PRESTATION_ESP et INDIVIDU sur l'ayant droit (IND_ID_PREST), et garde les
-- prestations notifiées (PE_DATE_NOTIFICATION remplie) dont l'ayant droit a
-- une date de décès. Ses colonnes : celles de la prestation et de la
-- personne, plus le nom, le prénom, l'adresse, la situation matrimoniale et
-- le téléphone. Aucun filtre de type, d'année ni de prestation en cours.
-- Lecture : la vue peut servir de source, mais elle expose des données
--   nominatives que la BI n'a pas à lire ; T57 garde ses propres filtres.
