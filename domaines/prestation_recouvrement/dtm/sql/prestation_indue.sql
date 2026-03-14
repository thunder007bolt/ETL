-- DTM_PRESTATION_INDUE V1
-- Source    : DWH.FAIT_AJUSTEMENT (principal)
--             LEFT JOIN DWH.FAIT_DOSSIER  (DR/SP/LP)
--             LEFT JOIN DWH.FAIT_INDIVIDU (sexe, date naissance)
--             LEFT JOIN DWH.FAIT_DEBOURS  (montants recouvrés — CTE montants_recouvres)
-- Grain     : ANNEE × MOIS × AJ_STATUT_GROUPE × TAJ_CODE × DR_NO × SP_NO × LP_NO × SEXE × TRANCHE_AGE
-- Filtre    : TAJ_CODE IN ('TP','TP-CONV','REM-TP') — trop-perçus uniquement
-- AJ_STATUT : EN = en cours | RC = recouvré (AJ_STATUT='C') | IR = irrécupérable (AJ_STATUT='I')
-- MONTANT_RECOUVRE : DEB_MONTANT < 0 AND DEB_STATUT = 'D' dans FAIT_DEBOURS
-- TRANCHE_AGE : calculée au 31/12/ANNEE (règle CIPRES — tranches 5 ans)
-- Exclus    : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH
-- ── CTE : montants recouvrés depuis FAIT_DEBOURS ─────────────────
montants_recouvres AS (
    SELECT
        deb.DOS_CODE,
        SUM(ABS(deb.DEB_MONTANT))                                   AS MONTANT_RECOUVRE
    FROM DWH.FAIT_DEBOURS deb
    WHERE deb.DEB_MONTANT < 0
      AND deb.DEB_STATUT  = 'D'
    GROUP BY deb.DOS_CODE
),
-- ── Base ajustements (trop-perçus) ───────────────────────────────
base AS (
    SELECT
        EXTRACT(YEAR  FROM aj.AJ_DATE)                              AS ANNEE,
        EXTRACT(MONTH FROM aj.AJ_DATE)                              AS MOIS,
        CASE
            WHEN aj.AJ_STATUT = 'C' THEN 'RC'
            WHEN aj.AJ_STATUT = 'I' THEN 'IR'
            ELSE                         'EN'
        END                                                         AS AJ_STATUT_GROUPE,
        aj.TAJ_CODE,
        NVL(dos.DR_NO, 0)                                           AS DR_NO,
        NVL(dos.SP_NO, 0)                                           AS SP_NO,
        NVL(dos.LP_NO, 0)                                           AS LP_NO,
        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,
        aj.DOS_CODE,
        NVL(aj.AJ_MONTANT, 0)                                       AS AJ_MONTANT,
        NVL(mr.MONTANT_RECOUVRE, 0)                                 AS MONTANT_RECOUVRE,
        aj.CLICHE
    FROM DWH.FAIT_AJUSTEMENT aj
    LEFT JOIN DWH.FAIT_DOSSIER   dos ON dos.DOS_CODE = aj.DOS_CODE
                                    AND dos.CLICHE   = aj.CLICHE
    LEFT JOIN DWH.FAIT_INDIVIDU  ind ON ind.IND_ID   = dos.IND_ID
                                    AND ind.CLICHE   = aj.CLICHE
    LEFT JOIN montants_recouvres mr  ON mr.DOS_CODE  = aj.DOS_CODE
    WHERE aj.CLICHE   = :1
      AND aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP')
      AND aj.AJ_DATE  IS NOT NULL
)
SELECT
    -- ── TEMPOREL ────────────────────────────────────────────────────
    b.ANNEE,
    b.MOIS,
    CEIL(b.MOIS / 3)                                                AS TRIMESTRE,
    -- ── STATUT ──────────────────────────────────────────────────────
    b.AJ_STATUT_GROUPE,
    CASE b.AJ_STATUT_GROUPE
        WHEN 'EN' THEN 'En cours'
        WHEN 'RC' THEN 'Recouvre'
        WHEN 'IR' THEN 'Irrecoverable'
        ELSE            b.AJ_STATUT_GROUPE
    END                                                             AS LIBELLE_STATUT,
    b.TAJ_CODE,
    -- ── GÉOGRAPHIE ──────────────────────────────────────────────────
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    -- ── DÉMOGRAPHIE ─────────────────────────────────────────────────
    b.IND_SEXE                                                      AS SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END                                                             AS LIBELLE_SEXE,
    -- Tranche âge au 31/12/ANNEE (règle CIPRES — tranches 5 ans)
    CASE
        WHEN b.IND_DATE_NAISSANCE IS NULL THEN NULL
        ELSE CASE
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 20 THEN '<20'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 25 THEN '20-24'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 30 THEN '25-29'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 35 THEN '30-34'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 40 THEN '35-39'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 45 THEN '40-44'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 50 THEN '45-49'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 55 THEN '50-54'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 60 THEN '55-59'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 65 THEN '60-64'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 70 THEN '65-69'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 75 THEN '70-74'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 80 THEN '75-79'
            ELSE '80+'
        END
    END                                                             AS TRANCHE_AGE,
    -- ── MESURES VOLUMES ─────────────────────────────────────────────
    COUNT(b.DOS_CODE)                                               AS NB_INDUS,
    COUNT(DISTINCT b.DOS_CODE)                                      AS NB_BENEFICIAIRES,
    -- ── MESURES FINANCIÈRES ─────────────────────────────────────────
    SUM(b.AJ_MONTANT)                                               AS MONTANT_INDU,
    SUM(b.MONTANT_RECOUVRE)                                         AS MONTANT_RECOUVRE,
    SUM(b.AJ_MONTANT) - SUM(b.MONTANT_RECOUVRE)                    AS MONTANT_RESTANT,
    ROUND(SUM(b.MONTANT_RECOUVRE)
          / NULLIF(SUM(b.AJ_MONTANT), 0) * 100, 2)                 AS TAUX_RECOUVREMENT,
    -- ── CLICHE ──────────────────────────────────────────────────────
    b.CLICHE                                                        AS CLICHE
FROM base b
GROUP BY
    b.ANNEE,
    b.MOIS,
    CEIL(b.MOIS / 3),
    b.AJ_STATUT_GROUPE,
    CASE b.AJ_STATUT_GROUPE
        WHEN 'EN' THEN 'En cours'
        WHEN 'RC' THEN 'Recouvre'
        WHEN 'IR' THEN 'Irrecoverable'
        ELSE            b.AJ_STATUT_GROUPE
    END,
    b.TAJ_CODE,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE,
    CASE b.IND_SEXE WHEN 1 THEN 'Masculin' WHEN 2 THEN 'Feminin' ELSE NULL END,
    CASE
        WHEN b.IND_DATE_NAISSANCE IS NULL THEN NULL
        ELSE CASE
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 20 THEN '<20'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 25 THEN '20-24'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 30 THEN '25-29'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 35 THEN '30-34'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 40 THEN '35-39'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 45 THEN '40-44'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 50 THEN '45-49'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 55 THEN '50-54'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 60 THEN '55-59'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 65 THEN '60-64'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 70 THEN '65-69'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 75 THEN '70-74'
            WHEN TRUNC(MONTHS_BETWEEN(TO_DATE('3112'||TO_CHAR(b.ANNEE),'DDMMYYYY'),b.IND_DATE_NAISSANCE)/12) < 80 THEN '75-79'
            ELSE '80+'
        END
    END,
    b.CLICHE
