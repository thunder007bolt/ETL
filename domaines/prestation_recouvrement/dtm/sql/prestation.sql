-- DTM_PRESTATION V5
-- Sources : DWH.FAIT_DEBOURS          (principal — grain DEB_ID)
--           DWH.FAIT_DOSSIER          (LP_NO)
--           DWH.FAIT_EFFET            (IND_ID_BENEF, NET_A_PAYER, DR_NO, SP_NO)
--           DWH.FAIT_INDIVIDU         (SEXE, DATE_NAISSANCE, DATE_DECES)
--           DWH.FAIT_PRESTATION_ESP   (mois cotisés, taux IPP)
-- Grain   : ID_TEMPS × TDOS_CODE × CODE_PRESTATION × TYPE_PREST
--           × DR_NO × SP_NO × LP_NO × SEXE × TAG_CODE
-- Branches : V (PVID), A (AT/MP), F (Prestations Familiales), M (Maladie)
-- Métriques : NB_PRESTATIONS, NB_BENEFICIAIRES, NB_MOIS_COTISES,
--             MONTANT_TOTAL, MONTANT_NET, NB_CONTROLES_POST,
--             NB_DECES, NB_NOUVEAUX, TAUX_IPP_MOYEN (ARI)
-- nouveaux_beneficiaires / premier_effet : source FAIT_DEBOURS+FAIT_EFFET
--   (137 324 bénéficiaires vs 26 988 dans FAIT_PRESTATION_ESP)
-- :1 = CLICHE (MMYYYY) — snapshot DWH uniforme

WITH

-- ── CTE 1 : nouveaux bénéficiaires (R7) ──────────────────────────────────
-- Nouveau = IND_ID_BENEF dont MIN(DEB_DATE_EFFET) pour un TPE_CODE
-- tombe dans l'année du débours — Source : FAIT_DEBOURS (paiement effectif)
-- FAIT_PRESTATION_ESP abandonné : couvre 26 988 bénéf vs 137 324 dans FAIT_DEBOURS
-- Grain IND_ID_BENEF + TPE_CODE
nouveaux_beneficiaires AS (
    SELECT
        efp2.IND_ID_BENEF,
        deb2.TPE_CODE,
        EXTRACT(YEAR FROM MIN(deb2.DEB_DATE_EFFET)) AS ANNEE_ENTREE
    FROM   DWH.FAIT_DEBOURS  deb2
    JOIN   DWH.FAIT_EFFET    efp2 ON efp2.EFP_ID       = deb2.EFP_ID
                                 AND efp2.CLICHE        = :1
    WHERE  deb2.CLICHE          = :1
      AND  deb2.TPE_CODE        IS NOT NULL
      AND  efp2.IND_ID_BENEF    IS NOT NULL
      AND  deb2.DEB_DATE_EFFET  IS NOT NULL
    GROUP BY efp2.IND_ID_BENEF, deb2.TPE_CODE
),

-- ── CTE 2 : premier mois de débours par nouveau bénéficiaire ─────────────
-- Basé sur DEB_DATE_EFFET (premier paiement effectif)
-- NB_NOUVEAUX = 1 uniquement sur le premier ID_TEMPS du débours
-- Évite double comptage sur plusieurs mois de l'année
premier_effet AS (
    SELECT
        efp2.IND_ID_BENEF,
        deb2.TPE_CODE,
        TO_NUMBER(TO_CHAR(
            TRUNC(MIN(deb2.DEB_DATE_EFFET), 'MM'),
            'YYYYMMDD'))                                AS PREMIER_ID_TEMPS
    FROM   DWH.FAIT_DEBOURS  deb2
    JOIN   DWH.FAIT_EFFET    efp2 ON efp2.EFP_ID       = deb2.EFP_ID
                                 AND efp2.CLICHE        = :1
    WHERE  deb2.CLICHE          = :1
      AND  deb2.TPE_CODE        IS NOT NULL
      AND  efp2.IND_ID_BENEF    IS NOT NULL
      AND  deb2.DEB_DATE_EFFET  IS NOT NULL
    GROUP BY efp2.IND_ID_BENEF, deb2.TPE_CODE
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
                      TO_DATE('31/12/' ||
                          TO_CHAR(EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)),
                          'DD/MM/YYYY'),
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

    WHERE deb.DEB_DATE_EFFET  <= LAST_DAY(TO_DATE(:1, 'MMYYYY'))
      AND deb.CODE_BRANCHE      IN ('V', 'A', 'F', 'M')
      AND deb.DEB_DATE_EFFET    IS NOT NULL
      -- Exclure AJ sans code prestation — ajustements comptables génériques
      -- non rattachés à un type CIPRES (175 050 débours, -1 198 446 614 FCFA)
      AND NOT (    deb.DEB_TYPE  = 'AJ'
               AND deb.TPE_CODE IS NULL
               AND deb.TPN_CODE IS NULL)
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
    b.TAG_CODE,

    COUNT(b.DEB_ID)                                     AS NB_PRESTATIONS,
    COUNT(DISTINCT b.IND_ID_BENEF)                      AS NB_BENEFICIAIRES,
    SUM(b.PE_MOIS_COTISATION)                           AS NB_MOIS_COTISES,
    SUM(b.DEB_MONTANT)                                  AS MONTANT_TOTAL,
    SUM(b.NET_A_PAYER)                                  AS MONTANT_NET,

    COUNT(CASE WHEN b.DEB_VERIFIE = 'O'
               THEN b.DEB_ID END)                       AS NB_CONTROLES_POST,

    -- NB_DECES : bénéficiaires décédés dans l'année du débours
    COUNT(DISTINCT CASE
        WHEN b.IND_DATE_DECES IS NOT NULL
         AND EXTRACT(YEAR FROM b.IND_DATE_DECES) = b.ANNEE
        THEN b.IND_ID_BENEF END)                        AS NB_DECES,

    -- NB_NOUVEAUX : bénéficiaires entrant pour la 1ère fois ce mois-ci
    -- Comptés uniquement sur le premier mois de débours — évite double comptage
    COUNT(DISTINCT CASE
        WHEN b.ANNEE_ENTREE = b.ANNEE
         AND b.ID_TEMPS     = b.PREMIER_ID_TEMPS
        THEN b.IND_ID_BENEF END)                        AS NB_NOUVEAUX,

    -- TAUX_IPP_MOYEN : moyenne du taux IPP à l'entrée pour les ARI uniquement
    AVG(CASE WHEN b.CODE_PRESTATION = 'ARI'
             THEN b.TAUX_IPP_ENTREE
             ELSE NULL END)                             AS TAUX_IPP_MOYEN,

    SYSDATE                                             AS DATE_CHARGEMENT,
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
    b.TAG_CODE,
    :1
