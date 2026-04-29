-- ════════════════════════════════════════════════════════════════════════════
-- FAIT_DOSSIER_REJETE — Scripts de création ODS et DWH
-- Source ODS : table DOSSIER_REJETE (staging brut)
-- Cible DWH  : FAIT_DOSSIER_REJETE (snapshot CLICHE, grain DOSRE_ID × CLICHE)
-- ════════════════════════════════════════════════════════════════════════════


-- ── 1. TABLE ODS (staging brut — miroir de la source) ────────────────────
-- Schéma : ODS   |   Pas de colonne CLICHE (raw, full reload)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE ODS.FAIT_DOSSIER_REJETE
(
    DOSRE_ID              NUMBER(10,0)       NOT NULL,
    TDOS_CODE             VARCHAR2(1  CHAR),
    DR_NO                 NUMBER(2,0),
    LP_NO                 NUMBER(4,0),
    RD_ID                 NUMBER(10,0),
    TR_ID                 VARCHAR2(13 CHAR),
    DOSRE_DATE_REJET      DATE,
    DOSRE_MOTIF           VARCHAR2(40 CHAR),
    DOSRE_REMARQUE        VARCHAR2(100 CHAR),
    DOS_CODE              VARCHAR2(11 CHAR),
    DOSRE_USAGER_INSERT   VARCHAR2(15 CHAR),
    DOSRE_DATE_INSERT     DATE,
    DOSRE_USAGER_UPDATE   VARCHAR2(14 CHAR),
    DOSRE_DATE_UPDATE     DATE,
    CONSTRAINT PK_ODS_DOSSIER_REJETE PRIMARY KEY (DOSRE_ID)
);

-- Index ODS sur DOS_CODE pour les jointures aval
CREATE INDEX IDX_ODS_DOSRE_DOS_CODE ON ODS.FAIT_DOSSIER_REJETE (DOS_CODE);

-- Grant DWH en lecture/écriture/suppression
GRANT SELECT, INSERT, DELETE ON ODS.FAIT_DOSSIER_REJETE TO DWH;


-- ── 2. TABLE DWH (historisée par CLICHE) ─────────────────────────────────
-- Schéma : DWH   |   Ajout de la colonne CLICHE CHAR(6) — snapshot mensuel
-- Grain  : DOSRE_ID × CLICHE
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE DWH.FAIT_DOSSIER_REJETE
(
    DOSRE_ID              NUMBER(10,0)       NOT NULL,
    TDOS_CODE             VARCHAR2(1  CHAR),
    DR_NO                 NUMBER(2,0),
    LP_NO                 NUMBER(4,0),
    RD_ID                 NUMBER(10,0),
    TR_ID                 VARCHAR2(13 CHAR),
    DOSRE_DATE_REJET      DATE,
    DOSRE_MOTIF           VARCHAR2(40 CHAR),
    DOSRE_REMARQUE        VARCHAR2(100 CHAR),
    DOS_CODE              VARCHAR2(11 CHAR),
    DOSRE_USAGER_INSERT   VARCHAR2(15 CHAR),
    DOSRE_DATE_INSERT     DATE,
    DOSRE_USAGER_UPDATE   VARCHAR2(14 CHAR),
    DOSRE_DATE_UPDATE     DATE,
    CLICHE                CHAR(6)            NOT NULL,
    CONSTRAINT PK_DWH_DOSSIER_REJETE PRIMARY KEY (DOSRE_ID, CLICHE)
);

-- Index CLICHE — utilisé par le pipeline DELETE/INSERT sur snapshot mensuel
CREATE INDEX IDX_CLICHE_DOSRE ON DWH.FAIT_DOSSIER_REJETE (CLICHE);

-- Index DOS_CODE — utilisé par dossier.sql (CTE dossiers_rejetes)
CREATE INDEX IDX_DWH_DOSRE_DOS_CODE ON DWH.FAIT_DOSSIER_REJETE (DOS_CODE);
