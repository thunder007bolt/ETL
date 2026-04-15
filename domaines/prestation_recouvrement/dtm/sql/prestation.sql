-- DTM_PRESTATION V3
-- Sources : DWH.FAIT_DEBOURS          (principal — grain DEB_ID)
--           DWH.FAIT_DOSSIER          (LP_NO)
--           DWH.FAIT_EFFET            (IND_ID_BENEF, NET_A_PAYER, DR_NO, SP_NO)
--           DWH.FAIT_INDIVIDU         (SEXE, DATE_NAISSANCE, DATE_DECES)
--           DWH.FAIT_PRESTATION_ESP   (mois cotisés, nouveaux bénéficiaires, premier effet, taux IPP)
-- Grain   : ID_TEMPS × TDOS_CODE × CODE_PRESTATION × TYPE_PREST
--           × DR_NO × SP_NO × LP_NO × SEXE × TAG_CODE
-- Filtré  : CODE_BRANCHE='V', DR_NO=10 (PVID)
-- Métriques : NB_PRESTATIONS, NB_BENEFICIAIRES, NB_MOIS_COTISES,
--             MONTANT_TOTAL, MONTANT_NET, NB_CONTROLES_POST,
--             NB_DECES, NB_NOUVEAUX, TAUX_IPP_MOYEN (ARI)
-- :1 = CLICHE (MMYYYY) — snapshot DWH uniforme

WITH

-- ── CTE 1 : nouveaux bénéficiaires — historique toutes années ────────────
-- Détermine l'ANNEE_ENTREE de chaque (bénéficiaire, type prestation)
-- Non filtré par CLICHE sur pe car besoin de l'historique complet du snapshot courant
nouveaux_beneficiaires AS (
    SELECT pe.IND_ID_BENEF,
           pe.TPE_CODE,
           EXTRACT(YEAR FROM MIN(pe.PE_DATE_EFFET)) AS ANNEE_ENTREE
    FROM   DWH.FAIT_PRESTATION_ESP pe
    JOIN   DWH.FAIT_DOSSIER        dos ON dos.DOS_CODE  = pe.DOS_CODE
                                      AND dos.CLICHE    = :1
    WHERE  pe.CLICHE              = :1
      AND  pe.PE_STATUT          != 'R'
      AND  pe.PE_DATE_EFFET      >= DATE '1990-01-01'
      AND  pe.IND_ID_BENEF        IS NOT NULL
      AND  dos.TDOS_CODE          = 'V'
      AND  dos.DR_NO              = 10
    GROUP BY pe.IND_ID_BENEF, pe.TPE_CODE
),

-- ── CTE 2 : premier mois de droit (Option A CIPRES — PE_DATE_EFFET) ──────
-- PREMIER_ID_TEMPS : ID_TEMPS du premier mois d'ouverture de droit
premier_effet AS (
    SELECT pe.IND_ID_BENEF,
           pe.TPE_CODE,
           TO_NUMBER(TO_CHAR(
               TRUNC(MIN(pe.PE_DATE_EFFET), 'MM'),
               'YYYYMMDD'))                              AS PREMIER_ID_TEMPS
    FROM   DWH.FAIT_PRESTATION_ESP pe
    JOIN   DWH.FAIT_DOSSIER        dos ON dos.DOS_CODE  = pe.DOS_CODE
                                      AND dos.CLICHE    = :1
    WHERE  pe.CLICHE              = :1
      AND  pe.PE_STATUT          != 'R'
      AND  pe.PE_DATE_EFFET      >= DATE '1990-01-01'
      AND  pe.IND_ID_BENEF        IS NOT NULL
      AND  dos.TDOS_CODE          = 'V'
      AND  dos.DR_NO              = 10
    GROUP BY pe.IND_ID_BENEF, pe.TPE_CODE
),

-- ── CTE 3 : taux IPP à l'entrée pour les dossiers ARI ───────────────────
-- Prend le taux de la première ligne ordonnée par PE_DATE_EFFET
taux_ipp_entree AS (
    SELECT DOS_CODE, PE_TAUX_INCAPACITE
    FROM (
        SELECT pe.DOS_CODE,
               pe.PE_TAUX_INCAPACITE,
               ROW_NUMBER() OVER (
                   PARTITION BY pe.DOS_CODE
                   ORDER BY pe.PE_DATE_EFFET ASC
               )                                        AS rn
        FROM   DWH.FAIT_PRESTATION_ESP pe
        WHERE  pe.CLICHE              = :1
          AND  pe.TPE_CODE            = 'ARI'
          AND  pe.PE_TAUX_INCAPACITE  IS NOT NULL
    )
    WHERE rn = 1
),

-- ── CTE 4 : mois cotisés — dédupliqué par MAX par (DOS_CODE, TPE_CODE) ───
mois_cotises AS (
    SELECT DOS_CODE,
           TPE_CODE,
           MAX(PE_MOIS_COTISATION) AS PE_MOIS_COTISATION
    FROM   DWH.FAIT_PRESTATION_ESP
    WHERE  CLICHE = :1
    GROUP BY DOS_CODE, TPE_CODE
),

-- ── CTE 5 : base débours enrichis ────────────────────────────────────────
base AS (
    SELECT
        -- Temporel
        TO_NUMBER(TO_CHAR(
            TRUNC(deb.DEB_DATE_EFFET, 'MM'),
            'YYYYMMDD'))                                AS ID_TEMPS,
        EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)           AS ANNEE,
        -- Clés
        deb.DEB_ID,
        deb.DOS_CODE,
        deb.CODE_BRANCHE                                AS TDOS_CODE,
        -- Code prestation : TPN pour remboursements (RB), TPE sinon
        CASE deb.DEB_TYPE
            WHEN 'RB' THEN deb.TPN_CODE
            ELSE           deb.TPE_CODE
        END                                             AS CODE_PRESTATION,
        -- Type prestation
        CASE deb.DEB_TYPE
            WHEN 'RB' THEN 'NAT'
            ELSE           'ESP'
        END                                             AS TYPE_PREST,
        -- Géographie
        efp.DR_NO,
        efp.SP_NO,
        dos.LP_NO,
        -- Démographie
        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,
        ind.IND_DATE_DECES,
        efp.IND_ID_BENEF,
        -- Tranche d'âge (31 décembre de l'année du débours)
        tag.TAG_CODE,
        -- Mesures
        mc.PE_MOIS_COTISATION,
        deb.DEB_MONTANT,
        efp.NET_A_PAYER,
        deb.DEB_VERIFIE,
        -- Métriques nouveaux
        pb.ANNEE_ENTREE,
        pd.PREMIER_ID_TEMPS,
        ti.PE_TAUX_INCAPACITE                           AS TAUX_IPP_ENTREE

    FROM      DWH.FAIT_DEBOURS                deb

    LEFT JOIN DWH.FAIT_DOSSIER                dos
           ON dos.DOS_CODE      = deb.DOS_CODE
          AND dos.CLICHE        = :1

    LEFT JOIN DWH.FAIT_EFFET                  efp
           ON efp.EFP_ID        = deb.EFP_ID
          AND efp.CLICHE        = :1

    LEFT JOIN DWH.FAIT_INDIVIDU               ind
           ON ind.IND_ID        = efp.IND_ID_BENEF
          AND ind.CLICHE        = :1

    -- Âge au 31/12 de l'année du débours
    LEFT JOIN DTM.DIM_TRANCHE_AGE             tag
           ON TRUNC(
                  MONTHS_BETWEEN(
                      ADD_MONTHS(TRUNC(deb.DEB_DATE_EFFET, 'YYYY'), 12) - 1,
                      ind.IND_DATE_NAISSANCE
                  ) / 12
              ) BETWEEN tag.INF AND tag.SUP

    LEFT JOIN mois_cotises                    mc
           ON mc.DOS_CODE       = deb.DOS_CODE
          AND mc.TPE_CODE       = deb.TPE_CODE

    LEFT JOIN nouveaux_beneficiaires          pb
           ON pb.IND_ID_BENEF   = efp.IND_ID_BENEF
          AND pb.TPE_CODE       = deb.TPE_CODE

    LEFT JOIN premier_effet                   pd
           ON pd.IND_ID_BENEF   = efp.IND_ID_BENEF
          AND pd.TPE_CODE       = deb.TPE_CODE

    LEFT JOIN taux_ipp_entree                 ti
           ON ti.DOS_CODE       = deb.DOS_CODE
          AND deb.TPE_CODE      = 'ARI'

    WHERE deb.CLICHE            = :1
      AND deb.CODE_BRANCHE      = 'V'
      AND efp.DR_NO             = 10
      AND deb.DEB_DATE_EFFET    IS NOT NULL
)

SELECT
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.CODE_PRESTATION,
    b.TYPE_PREST,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE                                          AS SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END                                                 AS LIBELLE_SEXE,
    b.TAG_CODE,

    COUNT(b.DEB_ID)                                     AS NB_PRESTATIONS,
    COUNT(DISTINCT b.IND_ID_BENEF)                      AS NB_BENEFICIAIRES,
    SUM(b.PE_MOIS_COTISATION)                           AS NB_MOIS_COTISES,
    SUM(b.DEB_MONTANT)                                  AS MONTANT_TOTAL,
    SUM(b.NET_A_PAYER)                                  AS MONTANT_NET,

    COUNT(CASE WHEN b.DEB_VERIFIE = 'O'
               THEN b.DEB_ID END)                       AS NB_CONTROLES_POST,

    -- NB_DECES : bénéficiaires décédés dans l'année du débours
    -- ⚠ source partielle (MAX=2011 au 11/03/2026) — activé pour usage futur
    COUNT(DISTINCT CASE
        WHEN b.IND_DATE_DECES IS NOT NULL
         AND EXTRACT(YEAR FROM b.IND_DATE_DECES) = b.ANNEE
        THEN b.IND_ID_BENEF END)                        AS NB_DECES,

    -- NB_NOUVEAUX : bénéficiaires entrant pour la 1ère fois ce mois-ci
    COUNT(DISTINCT CASE
        WHEN b.ANNEE_ENTREE = b.ANNEE
         AND b.ID_TEMPS     = b.PREMIER_ID_TEMPS
        THEN b.IND_ID_BENEF END)                        AS NB_NOUVEAUX,

    -- TAUX_IPP_MOYEN : moyenne du taux IPP à l'entrée pour les ARI uniquement
    AVG(CASE WHEN b.CODE_PRESTATION = 'ARI'
             THEN b.TAUX_IPP_ENTREE
             ELSE NULL END)                             AS TAUX_IPP_MOYEN,

    :1                                                  AS CLICHE

FROM base b

GROUP BY
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.CODE_PRESTATION,
    b.TYPE_PREST,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END,
    b.TAG_CODE,
    :1
