-- DTM_ACCIDENT_TRAVAIL V1
-- Sources : DWH.FAIT_TSINISTRE (principal, dénormalisé)
--           LEFT JOIN DWH.FAIT_PRESTATION_ESP (IPP, cotisations, salaires)
--           LEFT JOIN DWH.FAIT_DEBOURS       (IJ + rentes — CTE montants_at)
-- Grain   : ANNEE × MOIS × FLAG_IPP_GROUPE × DR_NO × SP_NO × LP_NO × SEXE × TRANCHE_AGE
-- R1      : FLAG_IPP depuis FAIT_TSINISTRE.TIPP (0/1 direct)
-- R2      : TAUX_IPP → NULL si PE_TAUX_INCAPACITE > 100
-- R3      : date référence = DOS_DATE_ACCIDENT (pas DOS_DATE_OUVERTURE)
-- R6      : NB_MOIS_COTISES global / obligatoire / volontaire
-- R7      : MONTANT_IJ (TPE_CODE='IJ') et MONTANT_RENTE (TPE_CODE='RV')
-- Exclus  : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH
-- ── R7 : montants IJ et rentes depuis FAIT_DEBOURS ────────────
montants_at AS (
    SELECT
        deb.DOS_CODE,
        SUM(CASE WHEN deb.TPE_CODE = 'IJ' THEN deb.DEB_MONTANT ELSE 0 END) AS MONTANT_IJ,
        SUM(CASE WHEN deb.TPE_CODE = 'RV' THEN deb.DEB_MONTANT ELSE 0 END) AS MONTANT_RENTE
    FROM DWH.FAIT_DEBOURS deb
    WHERE deb.TPE_CODE IN ('IJ', 'RV')
    GROUP BY deb.DOS_CODE
),
-- ── Base sinistres AT/MP enrichis ─────────────────────────────
base AS (
    SELECT
        sin.ANNEE,
        EXTRACT(MONTH FROM sin.DOS_DATE_ACCIDENT)           AS MOIS,
        CASE WHEN NVL(sin.TIPP, 0) > 0 THEN 1 ELSE 0 END   AS FLAG_IPP,
        CASE WHEN pe.PE_TAUX_INCAPACITE > 100 THEN NULL
             ELSE pe.PE_TAUX_INCAPACITE
        END                                                 AS TAUX_IPP,
        sin.LP_NO,
        sin.DR_NO,
        sin.IND_SEXE,
        sin.IND_DATE_NAISSANCE,
        sin.DOS_CODE,
        NVL(sin.NBREJOUR, 0)                                AS NBREJOUR,
        NVL(sin.DOS_DECES_IMMEDIAT, 0)                      AS FLAG_DECES_IMMEDIAT,
        NVL(sin.ENQUETE, 0)                                 AS FLAG_ENQUETE,
        CASE WHEN sin.DOS_DATE_NOTIFICATION IS NOT NULL
              AND sin.DOS_DATE_ACCIDENT    IS NOT NULL
             THEN sin.DOS_DATE_NOTIFICATION - sin.DOS_DATE_ACCIDENT
        END                                                 AS DELAI_NOTIFICATION_JOURS,
        pe.PE_MOIS_COTISATION,
        pe.PE_MOIS_COTI_ASSU_OBLI,
        pe.PE_MOIS_COTI_ASSU_VOL,
        pe.PE_SAL_MOYEN,
        pe.PE_SAL_MOYEN_ASSU_OBLI,
        pe.PE_SAL_MOYEN_ASSU_VOL,
        pe.PE_MT_ANNUEL_ME_COL,
        NVL(mat.MONTANT_IJ,    0)                           AS MONTANT_IJ,
        NVL(mat.MONTANT_RENTE, 0)                           AS MONTANT_RENTE,
        sin.CLICHE
    FROM DWH.FAIT_TSINISTRE sin
    LEFT JOIN (
        SELECT
            DOS_CODE,
            MAX(PE_TAUX_INCAPACITE)     AS PE_TAUX_INCAPACITE,
            MAX(PE_MOIS_COTISATION)     AS PE_MOIS_COTISATION,
            MAX(PE_MOIS_COTI_ASSU_OBLI) AS PE_MOIS_COTI_ASSU_OBLI,
            MAX(PE_MOIS_COTI_ASSU_VOL)  AS PE_MOIS_COTI_ASSU_VOL,
            MAX(PE_SAL_MOYEN)           AS PE_SAL_MOYEN,
            MAX(PE_SAL_MOYEN_ASSU_OBLI) AS PE_SAL_MOYEN_ASSU_OBLI,
            MAX(PE_SAL_MOYEN_ASSU_VOL)  AS PE_SAL_MOYEN_ASSU_VOL,
            MAX(PE_MT_ANNUEL_ME_COL)    AS PE_MT_ANNUEL_ME_COL
        FROM DWH.FAIT_PRESTATION_ESP
        GROUP BY DOS_CODE
    ) pe  ON pe.DOS_CODE  = sin.DOS_CODE
    LEFT JOIN montants_at mat ON mat.DOS_CODE = sin.DOS_CODE
    WHERE sin.CLICHE = :1
      AND sin.ANNEE IS NOT NULL
      AND sin.DOS_DATE_ACCIDENT IS NOT NULL
)
SELECT
    -- ── TEMPOREL ───────────────────────────────────────────────────
    b.ANNEE,
    b.MOIS,
    CEIL(b.MOIS / 3)                                                AS TRIMESTRE,
    -- ── IPP ────────────────────────────────────────────────────────
    b.FLAG_IPP                                                      AS FLAG_IPP_GROUPE,
    CASE b.FLAG_IPP
        WHEN 1 THEN 'Avec IPP'
        ELSE        'Sans IPP'
    END                                                             AS LIBELLE_IPP,
    -- ── GÉOGRAPHIE ─────────────────────────────────────────────────
    b.DR_NO,
    NULL                                                            AS SP_NO,
    b.LP_NO,
    -- ── DÉMOGRAPHIE ────────────────────────────────────────────────
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
    -- ── MESURES VOLUMES ────────────────────────────────────────────
    COUNT(b.DOS_CODE)                                               AS NB_SINISTRES,
    COUNT(DISTINCT b.DOS_CODE)                                      AS NB_BENEFICIAIRES,
    -- ── MESURES AT/MP ──────────────────────────────────────────────
    SUM(b.NBREJOUR)                                                 AS NB_JOURS_ARRET,
    SUM(b.FLAG_DECES_IMMEDIAT)                                      AS NB_DECES_IMMEDIAT,
    SUM(b.FLAG_ENQUETE)                                             AS NB_AVEC_ENQUETE,
    ROUND(AVG(b.DELAI_NOTIFICATION_JOURS), 2)                      AS DELAI_NOTIF_MOYEN_JOURS,
    -- ── MESURES IPP (R2 : > 100 déjà NULLifié dans base) ──────────
    ROUND(AVG(CASE WHEN b.FLAG_IPP = 1 THEN b.TAUX_IPP END), 2)   AS TAUX_IPP_MOYEN,
    MAX(CASE WHEN b.FLAG_IPP = 1 THEN b.TAUX_IPP END)              AS TAUX_IPP_MAX,
    -- ── MESURES COTISATIONS (R6) ───────────────────────────────────
    SUM(b.PE_MOIS_COTISATION)                                       AS NB_MOIS_COTISES,
    SUM(b.PE_MOIS_COTI_ASSU_OBLI)                                  AS NB_MOIS_COTISES_OBLI,
    SUM(b.PE_MOIS_COTI_ASSU_VOL)                                   AS NB_MOIS_COTISES_VOL,
    -- ── MESURES SALAIRES (R5) ──────────────────────────────────────
    ROUND(AVG(b.PE_SAL_MOYEN), 2)                                   AS SALAIRE_MOYEN_MOY,
    ROUND(AVG(b.PE_SAL_MOYEN_ASSU_OBLI), 2)                       AS SALAIRE_MOYEN_OBLI_MOY,
    ROUND(AVG(b.PE_SAL_MOYEN_ASSU_VOL), 2)                        AS SALAIRE_MOYEN_VOL_MOY,
    SUM(b.PE_MT_ANNUEL_ME_COL)                                     AS MONTANT_ANNUEL_MEP,
    -- ── MESURES PRESTATIONS (R7) ───────────────────────────────────
    SUM(b.MONTANT_IJ)                                               AS MONTANT_IJ,
    SUM(b.MONTANT_RENTE)                                            AS MONTANT_RENTE,
    -- ── CLICHE ─────────────────────────────────────────────────────
    b.CLICHE                                                        AS CLICHE
FROM base b
GROUP BY
    b.ANNEE,
    b.MOIS,
    CEIL(b.MOIS / 3),
    b.FLAG_IPP,
    CASE b.FLAG_IPP WHEN 1 THEN 'Avec IPP' ELSE 'Sans IPP' END,
    b.DR_NO,
    NULL,
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
