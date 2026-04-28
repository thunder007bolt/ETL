-- DTM_DOSSIER V3
-- Sources : DWH.FAIT_DOSSIER            (principal — grain DOS_CODE)
--           DWH.FAIT_INDIVIDU           (sexe, date naissance)
--           DWH.FAIT_RECEPTION_DOSSIER  (RD_DATE_RECEPTION — délai R2)
--           DWH.FAIT_PRESTATION_ESP     (PE_DATE_NOTIFICATION, mois cotisés)
--           DWH.FAIT_DEBOURS            (tpe_dominant — code prestation dominant)
--           USER_DWH.DOSSIER_REJETE     (dossiers rejetés)
--           DTM.DIM_TYPE_PRESTATION     (DESCRIPTION_PRESTATION)
-- Grain   : ID_TEMPS × TDOS_CODE × CODE_PRESTATION × DESCRIPTION_PRESTATION
--           × DR_NO × SP_NO × LP_NO × SEXE × TAG_CODE
-- Branches : V (PVID), A (AT/MP), F (Prestations Familiales), M (Maternité)
-- R2      : délai ESP (V/A/F) = PE_DATE_NOTIFICATION - RD_DATE_RECEPTION
-- R3      : délai NAT (M)    = DEB_DATE_APPROBATION - DEB_DATE_INSERT
-- R4      : bandes délai dossiers liquidés non rejetés (INF_15J / 15_45J / SUP_45J)
-- R7      : CODE_PRESTATION = TPE_CODE dominant par fréquence de débours
-- R8      : FLAG_REJETE depuis USER_DWH.DOSSIER_REJETE — indépendant de DOS_STATUT
-- Métriques : NB_DOSSIERS, NB_MOIS_COTISES,
--             NB_LIQUIDES, NB_INSTANCE, NB_REJETES,
--             NB_INF_15J, NB_15_45J, NB_SUP_45J,
--             DELAI_MOYEN_JOURS, DELAI_MAX_JOURS, DELAI_MIN_JOURS
-- :1 = CLICHE (YYYYMM) — snapshot DWH uniforme

WITH

-- ── CTE 1 : TPE_CODE dominant par dossier (R7) ───────────────────────────
-- Choisit le code prestation le plus fréquent dans les débours du dossier
-- Départage alphabétique en cas d'ex-aequo
tpe_dominant AS (
    SELECT DOS_CODE, TPE_CODE
    FROM (
        SELECT deb.DOS_CODE,
               deb.TPE_CODE,
               ROW_NUMBER() OVER (
                   PARTITION BY deb.DOS_CODE
                   ORDER BY COUNT(*) DESC, deb.TPE_CODE ASC
               )                                         AS rang
        FROM   DWH.FAIT_DEBOURS deb
        WHERE  deb.CLICHE   = :1
          AND  deb.TPE_CODE IS NOT NULL
        GROUP BY deb.DOS_CODE, deb.TPE_CODE
    )
    WHERE rang = 1
),

-- ── CTE 2 : dossiers rejetés (R8) ────────────────────────────────────────
-- Pas de filtre CLICHE — table de référence métier
dossiers_rejetes AS (
    SELECT DISTINCT DOS_CODE
    FROM   USER_DWH.DOSSIER_REJETE
),

-- ── CTE 3 : date notification + mois cotisés par dossier ─────────────────
-- MAX() pour dédupliquer sans cumul erroné (PE_MOIS_COTISATION constant par DOS_CODE)
notif_esp AS (
    SELECT pe.DOS_CODE,
           MAX(pe.PE_DATE_NOTIFICATION) AS PE_DATE_NOTIFICATION,
           MAX(pe.PE_MOIS_COTISATION)   AS PE_MOIS_COTISATION
    FROM   DWH.FAIT_PRESTATION_ESP pe
    WHERE  pe.CLICHE = :1
    GROUP BY pe.DOS_CODE
),

-- ── CTE 4 : premier débours RB Maternité (R3) ────────────────────────────
-- Utilisé uniquement pour TDOS_CODE='M' — délai approbation/insertion
premier_deb_nat AS (
    SELECT deb.DOS_CODE,
           MIN(deb.DEB_DATE_APPROBATION) AS DEB_DATE_APPROBATION,
           MIN(deb.DEB_DATE_INSERT)      AS DEB_DATE_INSERT
    FROM   DWH.FAIT_DEBOURS deb
    WHERE  deb.CLICHE   = :1
      AND  deb.DEB_TYPE = 'RB'
    GROUP BY deb.DOS_CODE
),

-- ── CTE 5 : base dossiers enrichis ───────────────────────────────────────
base AS (
    SELECT
        -- Temporel R1
        CASE 
            WHEN TO_NUMBER(TO_CHAR(TRUNC(dos.DOS_DATE_OUVERTURE, 'MM'), 'YYYYMMDD'))
                 BETWEEN 19500101 AND 20351231
            THEN TO_NUMBER(TO_CHAR(TRUNC(dos.DOS_DATE_OUVERTURE, 'MM'), 'YYYYMMDD'))
            ELSE 20000101
        END                                                  AS ID_TEMPS,

        -- Identifiants
        dos.DOS_CODE,
        dos.IND_ID,
        dos.TDOS_CODE,
        dos.DOS_STATUT,

        -- Géographie
        dos.DR_NO,
        dos.SP_NO,
        dos.LP_NO,

        -- Démographie
        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,

        -- Tranche d'âge au 31/12 de l'année d'ouverture (R6)
        NVL(tag.TAG_CODE, 1000)                              AS TAG_CODE,

        -- Mois cotisés
        ne.PE_MOIS_COTISATION,

        -- Code prestation dominant R7
        td.TPE_CODE                                          AS CODE_PRESTATION,
        dp.DESCRIPTION                                       AS DESCRIPTION_PRESTATION,

        -- Flag rejeté R8
        CASE WHEN dr.DOS_CODE IS NOT NULL
             THEN 1 ELSE 0 END                               AS FLAG_REJETE,

        -- Délai liquidation R2 (ESP : V/A/F) / R3 (NAT : M)
        CASE dos.TDOS_CODE
            WHEN 'V' THEN
                CASE WHEN ne.PE_DATE_NOTIFICATION IS NOT NULL
                      AND rd.RD_DATE_RECEPTION    IS NOT NULL
                     THEN ne.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                     ELSE NULL END
            WHEN 'A' THEN
                CASE WHEN ne.PE_DATE_NOTIFICATION IS NOT NULL
                      AND rd.RD_DATE_RECEPTION    IS NOT NULL
                     THEN ne.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                     ELSE NULL END
            WHEN 'F' THEN
                CASE WHEN ne.PE_DATE_NOTIFICATION IS NOT NULL
                      AND rd.RD_DATE_RECEPTION    IS NOT NULL
                     THEN ne.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                     ELSE NULL END
            WHEN 'M' THEN
                CASE WHEN pdn.DEB_DATE_APPROBATION IS NOT NULL
                      AND pdn.DEB_DATE_INSERT       IS NOT NULL
                     THEN pdn.DEB_DATE_APPROBATION - pdn.DEB_DATE_INSERT
                     ELSE NULL END
            ELSE NULL
        END                                                  AS DELAI_JOURS

    FROM      DWH.FAIT_DOSSIER               dos

    LEFT JOIN DWH.FAIT_INDIVIDU              ind
           ON ind.IND_ID         = dos.IND_ID
          AND ind.CLICHE         = :1

    -- Âge au 31/12 de l'année d'ouverture du dossier
    LEFT JOIN DTM.DIM_TRANCHE_AGE            tag
           ON TRUNC(
                  MONTHS_BETWEEN(
                      ADD_MONTHS(TRUNC(dos.DOS_DATE_OUVERTURE, 'YYYY'), 12) - 1,
                      ind.IND_DATE_NAISSANCE
                  ) / 12
              ) BETWEEN tag.INF AND tag.SUP

    LEFT JOIN tpe_dominant                   td
           ON td.DOS_CODE        = dos.DOS_CODE

    LEFT JOIN DTM.DIM_TYPE_PRESTATION        dp
           ON dp.CODE_PRESTATION = td.TPE_CODE

    LEFT JOIN dossiers_rejetes               dr
           ON dr.DOS_CODE        = dos.DOS_CODE

    -- Réception R2 — via DOSSIER.RD_ID (pas DOS_CODE)
    LEFT JOIN DWH.FAIT_RECEPTION_DOSSIER     rd
           ON rd.RD_ID           = dos.RD_ID
          AND rd.CLICHE          = :1

    LEFT JOIN notif_esp                      ne
           ON ne.DOS_CODE        = dos.DOS_CODE

    LEFT JOIN premier_deb_nat               pdn
           ON pdn.DOS_CODE       = dos.DOS_CODE

    WHERE dos.CLICHE             = :1
      AND dos.TDOS_CODE         IN ('V', 'A', 'F', 'M')
      AND dos.DOS_DATE_OUVERTURE IS NOT NULL
)

-- ── Agrégation finale ─────────────────────────────────────────────────────
SELECT
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.CODE_PRESTATION,
    b.DESCRIPTION_PRESTATION,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE                                               AS SEXE,
    CASE b.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END                                                      AS LIBELLE_SEXE,
    b.TAG_CODE,

    COUNT(b.DOS_CODE)                                        AS NB_DOSSIERS,
    SUM(b.PE_MOIS_COTISATION)                                AS NB_MOIS_COTISES,

    -- Statuts R8
    SUM(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
             THEN 1 ELSE 0 END)                              AS NB_LIQUIDES,

    SUM(CASE WHEN b.DOS_STATUT  = 'A'
              AND b.FLAG_REJETE = 0
             THEN 1 ELSE 0 END)                              AS NB_INSTANCE,

    SUM(b.FLAG_REJETE)                                       AS NB_REJETES,

    -- Bandes de délai — dossiers liquidés non rejetés avec délai positif
    SUM(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
              AND b.DELAI_JOURS > 0
              AND b.DELAI_JOURS < 15
             THEN 1 ELSE 0 END)                              AS NB_INF_15J,

    SUM(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
              AND b.DELAI_JOURS BETWEEN 15 AND 45
             THEN 1 ELSE 0 END)                              AS NB_15_45J,

    SUM(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
              AND b.DELAI_JOURS > 45
             THEN 1 ELSE 0 END)                              AS NB_SUP_45J,

    AVG(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
              AND b.DELAI_JOURS > 0
             THEN b.DELAI_JOURS END)                         AS DELAI_MOYEN_JOURS,

    MAX(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
              AND b.DELAI_JOURS > 0
             THEN b.DELAI_JOURS END)                         AS DELAI_MAX_JOURS,

    MIN(CASE WHEN b.DOS_STATUT  = 'F'
              AND b.FLAG_REJETE = 0
              AND b.DELAI_JOURS > 0
             THEN b.DELAI_JOURS END)                         AS DELAI_MIN_JOURS,

    :1                                                       AS CLICHE

FROM base b

GROUP BY
    b.ID_TEMPS,
    b.TDOS_CODE,
    b.CODE_PRESTATION,
    b.DESCRIPTION_PRESTATION,
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
