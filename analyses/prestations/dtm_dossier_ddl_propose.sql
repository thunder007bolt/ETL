-- =============================================================================
-- PROPOSITION du 29/10/2026 — Ajout de SOMME_DELAIS_JOURS dans DTM_DOSSIER
-- Va avec dtm_dossier_requete_proposee.sql. À exécuter AVANT de déployer la
-- nouvelle requête (sinon ORA-00904 au chargement), et seulement après
-- l'accord de Jules (Oracle Analytics) et la validation des règles T25–T27.
--
-- SOMME_DELAIS_JOURS : somme des durées de liquidation (en jours) des dossiers
-- liquidés de la ligne. Durée moyenne d'un exercice = somme de cette colonne
-- / somme de (NB_INF_15J + NB_15_45J + NB_SUP_45J).
-- Les lignes des clichés déjà chargés gardent cette colonne vide.
-- Étapes 1 et 2 : user DTM (ou DBA). Étape 3 : user ODS (ou DBA).
-- =============================================================================


-- 1. Table du DTM
ALTER TABLE DTM.DTM_DOSSIER ADD (
    SOMME_DELAIS_JOURS NUMBER
);


-- 2. Commentaire de la colonne
COMMENT ON COLUMN DTM.DTM_DOSSIER.SOMME_DELAIS_JOURS IS 'Somme des durees de liquidation (jours, reception -> premiere notification) des dossiers liquides de la ligne';


-- 3. Table miroir de l'ODS (archivage des clichés)
-- L'archivage ne copie que les colonnes communes au DTM et à l'ODS : sans cet
-- ajout, la colonne serait perdue à l'archivage, sans erreur.
ALTER TABLE ODS.DTM_DOSSIER ADD (
    SOMME_DELAIS_JOURS NUMBER
);


-- 4. Contrôle : 2 lignes attendues (DTM et ODS)
SELECT owner, table_name, column_name, data_type
FROM   all_tab_columns
WHERE  owner       IN ('DTM', 'ODS')
  AND  table_name  = 'DTM_DOSSIER'
  AND  column_name = 'SOMME_DELAIS_JOURS'
ORDER  BY owner;


-- Retour arrière, seulement après avoir remis l'ancienne requête :
-- ALTER TABLE DTM.DTM_DOSSIER DROP (SOMME_DELAIS_JOURS);
-- ALTER TABLE ODS.DTM_DOSSIER DROP (SOMME_DELAIS_JOURS);
