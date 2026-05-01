-- ════════════════════════════════════════════════════════════════════════════
-- DTM_CONTROLE_FREQ_TRIM_EMP — Scripts de création ODS et DTM
-- Grain : ANNEE × TRIMESTRE × CTL_TYPE × CLICHE
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. TABLE ODS (miroir staging — sans contraintes) ─────────────────────
CREATE TABLE ODS.DTM_CONTROLE_FREQ_TRIM_EMP
(
    ANNEE                 NUMBER(4),
    TRIMESTRE             VARCHAR2(1 CHAR),
    CTL_TYPE              VARCHAR2(20 CHAR),
    NB_CONTROLES          NUMBER,
    CLICHE                CHAR(6)
);

-- Index pour les purges du pipeline
CREATE INDEX IDX_ODS_CTL_FREQ_TRIM_CLICHE ON ODS.DTM_CONTROLE_FREQ_TRIM_EMP (CLICHE);

-- Grants
GRANT SELECT, INSERT, DELETE ON ODS.DTM_CONTROLE_FREQ_TRIM_EMP TO DWH;


-- ── 2. TABLE DTM (Data Warehouse — avec index) ───────────────────────────
CREATE TABLE DTM.DTM_CONTROLE_FREQ_TRIM_EMP
(
    ANNEE                 NUMBER(4)          NOT NULL,
    TRIMESTRE             VARCHAR2(1 CHAR)   NOT NULL,
    CTL_TYPE              VARCHAR2(20 CHAR),
    NB_CONTROLES          NUMBER,
    CLICHE                CHAR(6)            NOT NULL
);

-- Index pour les filtres BI et purges
CREATE INDEX IDX_DTM_CTL_FREQ_TRIM_CLICHE ON DTM.DTM_CONTROLE_FREQ_TRIM_EMP (CLICHE);
CREATE INDEX IDX_DTM_CTL_FREQ_TRIM_AN_TRIM ON DTM.DTM_CONTROLE_FREQ_TRIM_EMP (ANNEE, TRIMESTRE);
