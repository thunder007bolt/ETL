-- DTM_PRESTATION_INDUE V2
-- Sources : DWH.FAIT_AJUSTEMENT   (principal)
--           JOIN DWH.FAIT_DOSSIER  (branche, géographie)
--           JOIN DWH.FAIT_INDIVIDU (sexe, date naissance)
--           LEFT JOIN montants_recouvres (débours négatifs statut 'D')
-- Grain   : ANNEE × MOIS × CODE_BRANCHE × TAJ_CODE × AJ_STATUT_GROUPE
--           × DR_NO × SP_NO × LP_NO × SEXE × TRANCHE_AGE
-- R1      : MONTANT_RECOUVRE = SUM(ABS(DEB_MONTANT)) WHERE DEB_MONTANT<0 AND DEB_STATUT='D'
-- R2      : AJ_STATUT → RC (C) | IR (I) | EN (autres)
-- TRANCHE_AGE : au 31/12/ANNEE (règle CIPRES — tranches 5 ans)
-- Exclus  : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH

-- ── R1 : montants récupérés depuis FAIT_DEBOURS ───────────────
montants_recouvres AS (
    SELECT   deb.DOS_CODE,
             SUM(ABS(deb.DEB_MONTANT))  AS MONTANT_RECOUVRE
    FROM     DWH.FAIT_DEBOURS           deb
    WHERE    deb.DEB_MONTANT < 0
      AND    deb.DEB_STATUT  = 'D'
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
    JOIN      DWH.FAIT_DOSSIER            dos ON dos.DOS_CODE = aj.DOS_CODE
    JOIN      DWH.FAIT_INDIVIDU           ind ON ind.IND_ID   = aj.IND_ID
    LEFT JOIN montants_recouvres          recouv ON recouv.DOS_CODE = aj.DOS_CODE

    WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP')
      AND aj.CLICHE = :1
)

SELECT
    -- ── TEMPOREL ────────────────────────────────────────────────────
    EXTRACT(YEAR  FROM b.AJ_DATE_ETABLISSEMENT)             AS ANNEE,
    EXTRACT(MONTH FROM b.AJ_DATE_ETABLISSEMENT)             AS MOIS,
    CEIL(EXTRACT(MONTH FROM b.AJ_DATE_ETABLISSEMENT) / 3)   AS TRIMESTRE,

    -- ── BRANCHE ─────────────────────────────────────────────────────
    b.CODE_BRANCHE,

    -- ── TYPE AJUSTEMENT ─────────────────────────────────────────────
    b.TAJ_CODE,

    -- ── STATUT RECOUVREMENT (R2) ─────────────────────────────────────
    CASE b.AJ_STATUT
        WHEN 'C' THEN 'RC'
        WHEN 'I' THEN 'IR'
        ELSE          'EN'
    END                                                     AS AJ_STATUT_GROUPE,
    CASE b.AJ_STATUT
        WHEN 'C' THEN 'Recouvre'
        WHEN 'I' THEN 'Irrecuperable'
        ELSE          'En cours'
    END                                                     AS LIBELLE_STATUT_INDU,

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

    -- Tranche âge au 31/12/ANNEE (règle CIPRES — tranches 5 ans)
    CASE
        WHEN b.IND_DATE_NAISSANCE IS NULL THEN NULL
        ELSE CASE
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 20 THEN '<20'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 25 THEN '20-24'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 30 THEN '25-29'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 35 THEN '30-34'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 40 THEN '35-39'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 45 THEN '40-44'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 50 THEN '45-49'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 55 THEN '50-54'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 60 THEN '55-59'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 65 THEN '60-64'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 70 THEN '65-69'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 75 THEN '70-74'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 80 THEN '75-79'
            ELSE '80+'
        END
    END                                                     AS TRANCHE_AGE,

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

GROUP BY
    EXTRACT(YEAR  FROM b.AJ_DATE_ETABLISSEMENT),
    EXTRACT(MONTH FROM b.AJ_DATE_ETABLISSEMENT),
    CEIL(EXTRACT(MONTH FROM b.AJ_DATE_ETABLISSEMENT) / 3),
    b.CODE_BRANCHE,
    b.TAJ_CODE,
    CASE b.AJ_STATUT WHEN 'C' THEN 'RC' WHEN 'I' THEN 'IR' ELSE 'EN' END,
    CASE b.AJ_STATUT WHEN 'C' THEN 'Recouvre'
                     WHEN 'I' THEN 'Irrecuperable'
                     ELSE          'En cours' END,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE,
    CASE b.IND_SEXE WHEN 1 THEN 'Masculin' WHEN 2 THEN 'Feminin' ELSE NULL END,
    CASE
        WHEN b.IND_DATE_NAISSANCE IS NULL THEN NULL
        ELSE CASE
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 20 THEN '<20'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 25 THEN '20-24'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 30 THEN '25-29'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 35 THEN '30-34'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 40 THEN '35-39'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 45 THEN '40-44'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 50 THEN '45-49'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 55 THEN '50-54'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 60 THEN '55-59'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 65 THEN '60-64'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 70 THEN '65-69'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 75 THEN '70-74'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(EXTRACT(YEAR FROM b.AJ_DATE_ETABLISSEMENT)),'DD/MM/YYYY'),b.IND_DATE_NAISSANCE)/12) < 80 THEN '75-79'
            ELSE '80+'
        END
    END,
    b.CLICHE
