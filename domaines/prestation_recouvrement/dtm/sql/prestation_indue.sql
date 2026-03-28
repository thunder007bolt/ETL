-- DTM_PRESTATION_INDUE V3
-- Sources : DWH.FAIT_AJUSTEMENT   (principal)
--           JOIN DWH.FAIT_DOSSIER  (branche, géographie)
--           JOIN DWH.FAIT_INDIVIDU (sexe, date naissance)
--           LEFT JOIN montants_recouvres (débours négatifs statut 'D')
-- Grain   : ID_TEMPS × CODE_BRANCHE × CODE_BRANCHE_PRESTATION × TAJ_CODE × AJ_STATUT_GROUPE
--           × DR_NO × SP_NO × LP_NO × SEXE
-- R1      : MONTANT_RECOUVRE = SUM(ABS(DEB_MONTANT)) WHERE DEB_MONTANT<0 AND DEB_STATUT='D'
-- R2      : AJ_STATUT → RC (C) | IR (I) | EN (autres)
-- Exclus  : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH

-- ── R1 : montants récupérés depuis FAIT_DEBOURS ───────────────
montants_recouvres AS (
    SELECT   deb.DOS_CODE,
             SUM(ABS(deb.DEB_MONTANT))  AS MONTANT_RECOUVRE
    FROM     DWH.FAIT_DEBOURS           deb
    WHERE    deb.DEB_MONTANT < 0
      AND    deb.DEB_STATUT  = 'D'
      AND    deb.CLICHE      = :1
    GROUP BY deb.DOS_CODE
),

-- ── Base : ajustements indus enrichis ────────────────────────
base AS (
    SELECT
        aj.AJ_ID,
        aj.DOS_CODE,
        aj.IND_ID,
        aj.TAJ_CODE,
        aj.AJ_MONTANT,
        aj.AJ_STATUT,
        aj.AJ_DATE_ETABLISSEMENT,
        aj.AJ_NB_PRECOMPTE,
        aj.AJ_VERIFIE,
        aj.AJ_ORDRE_DE_RECETTE,
        aj.CLICHE,

        -- Branche depuis TDOS_CODE
        CASE dos.TDOS_CODE
            WHEN 'V' THEN 'V'
            WHEN 'A' THEN 'A'
            WHEN 'F' THEN 'F'
            WHEN 'M' THEN 'M'
            ELSE          'V'
        END                                              AS CODE_BRANCHE,

        -- Géographie
        dos.LP_NO,
        dos.SP_NO,
        dos.DR_NO,

        -- Démographie bénéficiaire
        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,

        -- Montant recouvré (R1)
        NVL(recouv.MONTANT_RECOUVRE, 0)                 AS MONTANT_RECOUVRE

    FROM      DWH.FAIT_AJUSTEMENT         aj
    JOIN      DWH.FAIT_DOSSIER            dos ON dos.DOS_CODE = aj.DOS_CODE AND dos.CLICHE = :1
    JOIN      DWH.FAIT_INDIVIDU           ind ON ind.IND_ID   = aj.IND_ID   AND ind.CLICHE = :1
    LEFT JOIN montants_recouvres          recouv ON recouv.DOS_CODE = aj.DOS_CODE

    WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP')
      AND aj.CLICHE = :1
)

SELECT
    -- ── TEMPOREL ────────────────────────────────────────────────────
    TO_NUMBER(TO_CHAR(TRUNC(b.AJ_DATE_ETABLISSEMENT, 'MM'), 'YYYYMMDD')) AS ID_TEMPS,

    -- ── BRANCHE ─────────────────────────────────────────────────────
    b.CODE_BRANCHE                                                  AS TDOS_CODE,

    -- ── TYPE AJUSTEMENT ─────────────────────────────────────────────
    b.TAJ_CODE,

    -- ── STATUT RECOUVREMENT (R2) ─────────────────────────────────────
    CASE b.AJ_STATUT
        WHEN 'C' THEN 'RC'
        WHEN 'I' THEN 'IR'
        ELSE          'EN'
    END                                                     AS AJ_STATUT,

    -- ── GÉOGRAPHIE ──────────────────────────────────────────────────
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,

    -- ── DÉMOGRAPHIE ─────────────────────────────────────────────────
    b.IND_SEXE                                              AS SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END                                                     AS LIBELLE_SEXE,

    tag.TAG_CODE                                            AS TAG_CODE,

    -- ── MESURES VOLUMES ──────────────────────────────────────────────
    COUNT(b.AJ_ID)                                          AS NB_PRESTATIONS_INDUS,
    COUNT(DISTINCT b.DOS_CODE)                              AS NB_DOSSIERS,
    COUNT(DISTINCT b.IND_ID)                                AS NB_BENEFICIAIRES,

    -- ── MESURES MONTANTS ─────────────────────────────────────────────
    SUM(b.AJ_MONTANT)                                       AS MONTANT_INDU,
    SUM(b.MONTANT_RECOUVRE)                                 AS MONTANT_RECOUVRE,
    SUM(b.AJ_MONTANT) - SUM(b.MONTANT_RECOUVRE)            AS RESTE_A_RECOUVRIR,

    -- ── MESURES RECOUVREMENT (R2/R3) ────────────────────────────────
    SUM(NVL(b.AJ_NB_PRECOMPTE, 0))                         AS NB_ECHEANCES_PRECOMPTE,
    SUM(CASE WHEN b.AJ_STATUT = 'C' THEN 1 ELSE 0 END)     AS NB_RECOUVRES,
    SUM(CASE WHEN b.AJ_STATUT = 'I' THEN 1 ELSE 0 END)     AS NB_IRRECUPERABLES,

    -- ── FLAGS AGRÉGÉS (R4) ───────────────────────────────────────────
    SUM(CASE WHEN b.AJ_VERIFIE          = 'O' THEN 1 ELSE 0 END) AS NB_VERIFIES,
    SUM(CASE WHEN b.AJ_ORDRE_DE_RECETTE = 'O' THEN 1 ELSE 0 END) AS NB_ORDRE_RECETTE,

    -- ── CLICHE ──────────────────────────────────────────────────────
    b.CLICHE                                                AS CLICHE

FROM base b
LEFT JOIN DTM.DIM_TRANCHE_AGE              tag ON TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'), b.IND_DATE_NAISSANCE)/12) BETWEEN tag.INF AND tag.SUP

GROUP BY
    TO_NUMBER(TO_CHAR(TRUNC(b.AJ_DATE_ETABLISSEMENT, 'MM'), 'YYYYMMDD')),
    b.CODE_BRANCHE,
    b.TAJ_CODE,
    CASE b.AJ_STATUT WHEN 'C' THEN 'RC' WHEN 'I' THEN 'IR' ELSE 'EN' END,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE,
    CASE b.IND_SEXE WHEN 1 THEN 'Masculin' WHEN 2 THEN 'Feminin' ELSE NULL END,
    tag.TAG_CODE,
    b.CLICHE
