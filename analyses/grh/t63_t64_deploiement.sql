-- =============================================================================
-- T63 ET T64 — DÉPLOIEMENT DE LA REQUÊTE PROPOSÉE (07/11/2026)
-- À exécuter seulement après le visa de la DFC et la création, par la DSI, de
-- STG_GL.BI_GRH_SOLDE_COMPTABLE (t63_t64_structure_comptable.sql).
--
-- Le chemin des montants :
--   Oracle GL ──(DSI)──> STG_GL.BI_GRH_SOLDE_COMPTABLE   (base source)
--             ──(ETL, faits GRH)──> DWH.FAIT_GRH_SOLDE_COMPTABLE (+ CLICHE)
--             ──(DTM, t63_t64_requete_proposee.sql)──> DTM.DTM_INDICATEURS_FINANCES_GRH
--             ──> OBIEE : T63, T64
--
-- Étapes :
--   1. user DWH (ou DBA) : la table des faits et sa table d'archive ODS ;
--   2. dans le dépôt : le fichier SQL du fait et sa ligne dans fact_config.py ;
--   3. user DTM, puis ODS (ou DBA) : les colonnes ajoutées à
--      DTM_INDICATEURS_FINANCES_GRH, avant de déployer la nouvelle requête
--      (sinon l'insertion échoue, ORA-00904) ;
--   4. remplacer domaines/grh/dtm/sql/indicateurs_finances_grh.sql par le
--      corps de t63_t64_requete_proposee.sql (à partir de WITH) ;
--   5. contrôles.
-- Le fait doit être chargé avant le DTM, dans le même mois (même cliché).
-- =============================================================================


-- ── 1. Les tables du fait : DWH (dernier cliché) et ODS (archive) ──────────
-- Mêmes colonnes que la table de la DSI, plus CLICHE (MMYYYY), comme les
-- autres faits GRH. À chaque chargement, le pipeline archive le cliché
-- précédent dans l'ODS, vide la table du DWH, puis la remplit.
CREATE TABLE DWH.FAIT_GRH_SOLDE_COMPTABLE (
    EXERCICE            NUMBER(4)           NOT NULL,
    MOIS                NUMBER(2)           NOT NULL,
    CODE_GESTION        VARCHAR2(5 CHAR)    NOT NULL,
    COMPTE              VARCHAR2(20 CHAR)   NOT NULL,
    LIBELLE_COMPTE      VARCHAR2(240 CHAR),
    SECTION_ANALYTIQUE  VARCHAR2(30 CHAR)   NOT NULL,
    UA_CODE             VARCHAR2(12 CHAR),
    NATURE_MONTANT      VARCHAR2(10 CHAR)   NOT NULL,
    MONTANT_DEBIT       NUMBER(18,2)        NOT NULL,
    MONTANT_CREDIT      NUMBER(18,2)        NOT NULL,
    PERIODE_CLOSE       CHAR(1 CHAR)        NOT NULL,
    SOURCE              VARCHAR2(20 CHAR)   NOT NULL,
    DATE_EXTRACTION     DATE                NOT NULL,
    CLICHE              CHAR(6)             NOT NULL,
    CONSTRAINT PK_DWH_GRH_SOLDE_COMPTA
        PRIMARY KEY (EXERCICE, MOIS, CODE_GESTION, COMPTE, SECTION_ANALYTIQUE,
                     NATURE_MONTANT, CLICHE)
);
CREATE INDEX IDX_CLICHE_GRH_SOLDE ON DWH.FAIT_GRH_SOLDE_COMPTABLE (CLICHE);

CREATE TABLE ODS.FAIT_GRH_SOLDE_COMPTABLE (
    EXERCICE            NUMBER(4)           NOT NULL,
    MOIS                NUMBER(2)           NOT NULL,
    CODE_GESTION        VARCHAR2(5 CHAR)    NOT NULL,
    COMPTE              VARCHAR2(20 CHAR)   NOT NULL,
    LIBELLE_COMPTE      VARCHAR2(240 CHAR),
    SECTION_ANALYTIQUE  VARCHAR2(30 CHAR)   NOT NULL,
    UA_CODE             VARCHAR2(12 CHAR),
    NATURE_MONTANT      VARCHAR2(10 CHAR)   NOT NULL,
    MONTANT_DEBIT       NUMBER(18,2)        NOT NULL,
    MONTANT_CREDIT      NUMBER(18,2)        NOT NULL,
    PERIODE_CLOSE       CHAR(1 CHAR)        NOT NULL,
    SOURCE              VARCHAR2(20 CHAR)   NOT NULL,
    DATE_EXTRACTION     DATE                NOT NULL,
    CLICHE              CHAR(6)             NOT NULL
);
CREATE INDEX IDX_ODS_CLICHE_GRH_SOLDE ON ODS.FAIT_GRH_SOLDE_COMPTABLE (CLICHE);

-- Archivage : le user DWH écrit et supprime dans l'ODS (comme pour les
-- autres faits GRH).
GRANT SELECT, INSERT, DELETE ON ODS.FAIT_GRH_SOLDE_COMPTABLE TO DWH;

COMMENT ON TABLE DWH.FAIT_GRH_SOLDE_COMPTABLE IS
    'Copie de STG_GL.BI_GRH_SOLDE_COMPTABLE (montants comptables par exercice, mois, gestion, compte, section analytique ; realise et budget), dernier cliche. Sert a T63 et T64.';


-- ── 2. Dans le dépôt : le fait et sa configuration ─────────────────────────
-- a) Créer domaines/grh/facts/sql/grh_solde_comptable.sql avec ce contenu
--    (le pipeline lit la base source et ajoute CLICHE lui-même) :
--
--    -- FAIT_GRH_SOLDE_COMPTABLE — Source : STG_GL.BI_GRH_SOLDE_COMPTABLE
--    -- (table alimentée par la DSI depuis Oracle GL ; full reload)
--    SELECT
--        EXERCICE,
--        MOIS,
--        CODE_GESTION,
--        COMPTE,
--        LIBELLE_COMPTE,
--        SECTION_ANALYTIQUE,
--        UA_CODE,
--        NATURE_MONTANT,
--        MONTANT_DEBIT,
--        MONTANT_CREDIT,
--        PERIODE_CLOSE,
--        SOURCE,
--        DATE_EXTRACTION
--    FROM STG_GL.BI_GRH_SOLDE_COMPTABLE
--
-- b) Ajouter dans FACT_CONFIG de domaines/grh/facts/fact_config.py :
--
--    {
--        "sql_file": "grh_solde_comptable.sql",
--        "target":   "FAIT_GRH_SOLDE_COMPTABLE",
--    },
--
-- c) Le user de lecture de la source doit pouvoir lire la table de la DSI :
--    GRANT SELECT ON STG_GL.BI_GRH_SOLDE_COMPTABLE TO <user source de l'ETL>;
--    (la structure ne donne ce droit qu'à DWH : à confirmer avec la DSI.)


-- ── 3. Les colonnes ajoutées à DTM_INDICATEURS_FINANCES_GRH ────────────────
-- 0. Avant : la structure actuelle, et la taille de OBSERVATION (les textes
--    de la nouvelle requête font au plus 75 caractères, sans accent).
SELECT owner, table_name, column_name, data_type, data_length, char_length
FROM   all_tab_columns
WHERE  owner IN ('DTM', 'ODS')
  AND  table_name = 'DTM_INDICATEURS_FINANCES_GRH'
ORDER  BY owner, column_id;

ALTER TABLE DTM.DTM_INDICATEURS_FINANCES_GRH ADD (
    MT_CHARGES_FONCT     NUMBER(18,2),
    MT_FRAIS_PERSONNEL   NUMBER(18,2),
    MT_FORMATION         NUMBER(18,2),
    MT_MASSE_SALARIALE   NUMBER(18,2),
    MT_TOTAL_BUDGET      NUMBER(18,2),
    TX_FORMATION_BUDGET  NUMBER(9,4),
    EXERCICE_CLOS        CHAR(1 CHAR)
);

COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.MT_CHARGES_FONCT    IS 'T63 : charges de fonctionnement (CF), FCFA, realise net de l''exercice';
COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.MT_FRAIS_PERSONNEL  IS 'T63 : frais de personnel (FP), FCFA, realise net de l''exercice';
COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.MT_FORMATION        IS 'T64 : depenses de formation (FOR), FCFA, realise net de l''exercice';
COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.MT_MASSE_SALARIALE  IS 'T64 : masse salariale (MSAL), FCFA, realise net de l''exercice';
COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.MT_TOTAL_BUDGET     IS 'T64 : total budget (BUD), FCFA, budget de l''exercice';
COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.TX_FORMATION_BUDGET IS 'T64 : depenses de formation / total budget (fraction, 4 decimales)';
COMMENT ON COLUMN DTM.DTM_INDICATEURS_FINANCES_GRH.EXERCICE_CLOS       IS 'O si les 12 mois de l''exercice sont clos dans Oracle GL, sinon N (taux vides)';

-- Table miroir de l'ODS : l'archivage ne copie que les colonnes communes ;
-- sans cet ajout, les colonnes seraient perdues à l'archivage, sans erreur.
ALTER TABLE ODS.DTM_INDICATEURS_FINANCES_GRH ADD (
    MT_CHARGES_FONCT     NUMBER(18,2),
    MT_FRAIS_PERSONNEL   NUMBER(18,2),
    MT_FORMATION         NUMBER(18,2),
    MT_MASSE_SALARIALE   NUMBER(18,2),
    MT_TOTAL_BUDGET      NUMBER(18,2),
    TX_FORMATION_BUDGET  NUMBER(9,4),
    EXERCICE_CLOS        CHAR(1 CHAR)
);


-- ── 4. Contrôles ────────────────────────────────────────────────────────────
-- Les colonnes : 14 lignes attendues (7 colonnes × DTM et ODS).
SELECT owner, table_name, column_name, data_type
FROM   all_tab_columns
WHERE  owner IN ('DTM', 'ODS')
  AND  table_name = 'DTM_INDICATEURS_FINANCES_GRH'
  AND  column_name IN ('MT_CHARGES_FONCT', 'MT_FRAIS_PERSONNEL', 'MT_FORMATION',
                       'MT_MASSE_SALARIALE', 'MT_TOTAL_BUDGET',
                       'TX_FORMATION_BUDGET', 'EXERCICE_CLOS')
ORDER  BY owner, column_name;

-- Après le chargement du fait : le même nombre de lignes que la table de la
-- DSI, et les mêmes totaux (à lancer le jour du chargement).
SELECT 'STG_GL' AS ou, COUNT(*) AS lignes,
       SUM(MONTANT_DEBIT) AS debit, SUM(MONTANT_CREDIT) AS credit
FROM   STG_GL.BI_GRH_SOLDE_COMPTABLE
UNION ALL
SELECT 'DWH', COUNT(*), SUM(MONTANT_DEBIT), SUM(MONTANT_CREDIT)
FROM   DWH.FAIT_GRH_SOLDE_COMPTABLE;


-- Retour arrière, seulement après avoir remis l'ancienne requête :
-- ALTER TABLE DTM.DTM_INDICATEURS_FINANCES_GRH DROP (MT_CHARGES_FONCT,
--     MT_FRAIS_PERSONNEL, MT_FORMATION, MT_MASSE_SALARIALE, MT_TOTAL_BUDGET,
--     TX_FORMATION_BUDGET, EXERCICE_CLOS);
-- ALTER TABLE ODS.DTM_INDICATEURS_FINANCES_GRH DROP (MT_CHARGES_FONCT,
--     MT_FRAIS_PERSONNEL, MT_FORMATION, MT_MASSE_SALARIALE, MT_TOTAL_BUDGET,
--     TX_FORMATION_BUDGET, EXERCICE_CLOS);
-- DROP TABLE DWH.FAIT_GRH_SOLDE_COMPTABLE;
-- DROP TABLE ODS.FAIT_GRH_SOLDE_COMPTABLE;
