-- =============================================================================
-- 25/10/2026 — Ajout du type d'agent dans DTM_GRH_EFFECTIF et DTM_GRH_FORMATION
-- Suivi : domaines/grh/dtm/SUIVI_MODIFICATIONS_DWH.md (modifications 1 et 2)
--
-- Deux colonnes d'axe, alimentées par grh_effectif.sql et grh_formation.sql :
--   QUALITE_EMPLOYE : A agent CNSS, B agent de la fonction publique, C agent affecté
--   NATURE_EMPLOI   : PERMANENT, TEMPORAIRE
--
-- À exécuter AVANT de déployer les nouvelles requêtes : le chargement insère ces
-- colonnes, et l'insertion échoue (ORA-00904) si elles manquent.
-- Les lignes des clichés déjà chargés gardent ces colonnes vides.
-- Étapes 1 et 2 : user DTM (ou DBA). Étape 3 : user ODS (ou DBA).
-- =============================================================================


-- 1. Tables du DTM
ALTER TABLE DTM.DTM_GRH_EFFECTIF ADD (
    QUALITE_EMPLOYE VARCHAR2(10 CHAR),
    NATURE_EMPLOI   VARCHAR2(20 CHAR)
);

ALTER TABLE DTM.DTM_GRH_FORMATION ADD (
    QUALITE_EMPLOYE VARCHAR2(10 CHAR),
    NATURE_EMPLOI   VARCHAR2(20 CHAR)
);


-- 2. Commentaires des colonnes
COMMENT ON COLUMN DTM.DTM_GRH_EFFECTIF.QUALITE_EMPLOYE  IS 'Qualite de l''agent : A agent CNSS, B agent de la fonction publique, C agent affecte';
COMMENT ON COLUMN DTM.DTM_GRH_EFFECTIF.NATURE_EMPLOI    IS 'Nature d''emploi de la situation au 31/12 : PERMANENT ou TEMPORAIRE';
COMMENT ON COLUMN DTM.DTM_GRH_FORMATION.QUALITE_EMPLOYE IS 'Qualite de l''agent : A agent CNSS, B agent de la fonction publique, C agent affecte';
COMMENT ON COLUMN DTM.DTM_GRH_FORMATION.NATURE_EMPLOI   IS 'Nature d''emploi au 31/12 de l''exercice de la formation : PERMANENT ou TEMPORAIRE';


-- 3. Tables miroirs de l'ODS (archivage des clichés)
-- L'archivage ne copie que les colonnes communes au DTM et à l'ODS : sans cet
-- ajout, les deux colonnes seraient perdues à l'archivage, sans erreur.
ALTER TABLE ODS.DTM_GRH_EFFECTIF ADD (
    QUALITE_EMPLOYE VARCHAR2(10 CHAR),
    NATURE_EMPLOI   VARCHAR2(20 CHAR)
);

ALTER TABLE ODS.DTM_GRH_FORMATION ADD (
    QUALITE_EMPLOYE VARCHAR2(10 CHAR),
    NATURE_EMPLOI   VARCHAR2(20 CHAR)
);


-- 4. Contrôle : 8 lignes attendues (2 colonnes × 2 tables × DTM et ODS)
SELECT owner, table_name, column_name, data_type, data_length
FROM   all_tab_columns
WHERE  owner       IN ('DTM', 'ODS')
  AND  table_name  IN ('DTM_GRH_EFFECTIF', 'DTM_GRH_FORMATION')
  AND  column_name IN ('QUALITE_EMPLOYE', 'NATURE_EMPLOI')
ORDER  BY owner, table_name, column_name;


-- Retour arrière, seulement après avoir remis les anciennes requêtes :
-- ALTER TABLE DTM.DTM_GRH_EFFECTIF  DROP (QUALITE_EMPLOYE, NATURE_EMPLOI);
-- ALTER TABLE DTM.DTM_GRH_FORMATION DROP (QUALITE_EMPLOYE, NATURE_EMPLOI);
-- ALTER TABLE ODS.DTM_GRH_EFFECTIF  DROP (QUALITE_EMPLOYE, NATURE_EMPLOI);
-- ALTER TABLE ODS.DTM_GRH_FORMATION DROP (QUALITE_EMPLOYE, NATURE_EMPLOI);
