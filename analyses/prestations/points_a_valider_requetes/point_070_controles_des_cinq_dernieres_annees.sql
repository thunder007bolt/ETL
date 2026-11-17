-- =============================================================================
-- Point 70. Les contrôles des cinq dernières années
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « la vue proposée à la DSI, V_BI_CONTROLE_POSTERIORI :
--   une ligne par contrôle [...] » ; « Aujourd'hui : rien, la vue est
--   seulement proposée. »
-- Requête : la vue existe-t-elle dans la base ? Tant que la DSI ne l'a pas
--   créée, la requête ne renvoie aucune ligne.
-- Nouvelle requête : aucun lot ne l'avait mesuré. La maigreur de la table des
--   contrôles d'aujourd'hui est mesurée aux points 29 et 69.
-- Base : la base source, comme les lots. SQL Developer, mode script (F5).
--   Ne lit que le catalogue de la base (ALL_OBJECTS), aucune donnée.
-- Source de la vue proposée : schéma « vues cnss », planche
--   excalidraw/schemas/vues cnss/02 controles a posteriori.excalidraw
--   (07/11/2026), fait par excalidraw/sources/vues_cnss/vues_cnss.py.
-- Règle appliquée dans : aucune requête proposée. Fiche des règles : la ligne
--   « À faire (DAI et DSI) » : la DAI enregistre chaque contrôle, la DSI les
--   met dans la vue V_BI_CONTROLE_POSTERIORI.
-- =============================================================================

SELECT OWNER,
       OBJECT_NAME,
       OBJECT_TYPE,
       STATUS,
       CREATED
FROM ALL_OBJECTS
WHERE OBJECT_NAME = 'V_BI_CONTROLE_POSTERIORI';

-- Résultat : pas encore exécutée. Attendu : aucune ligne, la vue n'est que
--   proposée. Une ligne apparaîtra quand la DSI l'aura créée.
