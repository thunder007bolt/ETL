-- DTM_DOSSIER V5
-- Sources : DWH.FAIT_DOSSIER            (principal — grain DOS_CODE)
--           DWH.FAIT_INDIVIDU           (sexe, date naissance)
--           DWH.FAIT_RECEPTION_DOSSIER  (RD_DATE_RECEPTION — délai R2)
--           DWH.FAIT_PRESTATION_ESP     (PE_DATE_NOTIFICATION, mois cotisés, TPE_CODE)
--           DWH.FAIT_DEBOURS            (driver base, CODE_PRESTATION NAT, DEB_TYPE=RB)
--           DWH.FAIT_DOSSIER_REJETE     (dossiers rejetés — R8)
--           DTM.DIM_TYPE_PRESTATION     (DESCRIPTION_PRESTATION)
-- Grain   : ID_TEMPS × TDOS_CODE × CODE_PRESTATION × DESCRIPTION_PRESTATION
--           × DR_NO × SP_NO × LP_NO × SEXE × TAG_CODE
-- Branches : V (PVID), A (AT/MP), F (Prestations Familiales), M (Maternité)
-- R2      : délai ESP (V/A/F) = PE_DATE_NOTIFICATION - RD_DATE_RECEPTION  (>= 2018-01-01)
-- R3      : délai NAT (M)    = PE_DATE_NOTIFICATION - RD_DATE_RECEPTION   (>= 2017-01-01, priorité)
--                              fallback : DEB_DATE_APPROBATION - DEB_DATE_INSERT (>= 2017-01-01)
-- R4      : bandes délai dossiers liquidés non rejetés (INF_15J / 15_45J / SUP_45J)
-- R7      : CODE_PRESTATION dominant — ESP (TPE_CODE) + NAT (TPN_CODE, DEB_TYPE=RB)
-- R8      : FLAG_REJETE depuis DWH.FAIT_DOSSIER_REJETE — indépendant de DOS_STATUT
-- Métriques : NB_DOSSIERS, NB_MOIS_COTISES,
--             NB_LIQUIDES, NB_INSTANCE, NB_REJETES,
--             NB_INF_15J, NB_15_45J, NB_SUP_45J,
--             DELAI_MOYEN_JOURS, DELAI_MAX_JOURS, DELAI_MIN_JOURS, DATE_CHARGEMENT
-- Stratégie : full reload — tout l'historique rechargé à chaque exécution
--             Les sources DWH ne sont plus filtrées par CLICHE.
--             CLICHE = snapshot courant (:1, MMYYYY) fourni par le pipeline.

WITH

-- ── CTE 1 : CODE_PRESTATION dominant par dossier (R7) ──────────────────────
-- Sources combinées : ESP (FAIT_PRESTATION_ESP) + NAT (FAIT_DEBOURS DEB_TYPE=RB)
-- ESP couvre 341 097 dossiers (y compris anciens sans débours)
-- NAT ajoute 13 489 dossiers uniquement frais médicaux
-- ROW_NUMBER() ORDER BY NB DESC — CODE_PRESTATION le plus fréquent par DOS_CODE
-- Départage alphabétique en cas d'ex-aequo
tpe_dominant AS (
    SELECT DOS_CODE, CODE_PRESTATION
    FROM (
        SELECT DOS_CODE, CODE_PRESTATION, NB,
               ROW_NUMBER() OVER (
                   PARTITION BY DOS_CODE
                   ORDER BY NB DESC, CODE_PRESTATION ASC
               ) AS rang
        FROM (
            -- Source ESP : TPE_CODE depuis FAIT_PRESTATION_ESP
            SELECT pe.DOS_CODE,
                   pe.TPE_CODE  AS CODE_PRESTATION,
                   COUNT(*)     AS NB
            FROM   DWH.FAIT_PRESTATION_ESP pe
            WHERE  pe.CLICHE   = :1
              AND  pe.TPE_CODE IS NOT NULL
            GROUP BY pe.DOS_CODE, pe.TPE_CODE

            UNION ALL

            -- Source NAT : TPN_CODE depuis FAIT_DEBOURS (DEB_TYPE='RB')
            SELECT deb.DOS_CODE,
                   deb.TPN_CODE  AS CODE_PRESTATION,
                   COUNT(*)      AS NB
            FROM   DWH.FAIT_DEBOURS deb
            WHERE  deb.CLICHE   = :1
              AND  deb.DEB_TYPE  = 'RB'
              AND  deb.TPN_CODE  IS NOT NULL
            GROUP BY deb.DOS_CODE, deb.TPN_CODE
        )
        GROUP BY DOS_CODE, CODE_PRESTATION, NB
    )
    WHERE rang = 1
),

-- ── CTE 2 : dossiers rejetés (R8) ────────────────────────────────────────
-- Rejeté = indépendant de DOS_STATUT — pas de filtre CLICHE
dossiers_rejetes AS (
    SELECT DISTINCT DOS_CODE
    FROM   DWH.FAIT_DOSSIER_REJETE
),

-- ── CTE 3 : date notification + mois cotisés par dossier ─────────────────
-- MAX() pour dédupliquer sans cumul erroné (PE_MOIS_COTISATION constant par DOS_CODE)
notif_esp AS (
    SELECT
        pe.DOS_CODE,
        MAX(pe.PE_DATE_NOTIFICATION) AS PE_DATE_NOTIFICATION,
        MAX(pe.PE_MOIS_COTISATION)   AS PE_MOIS_COTISATION
    FROM   DWH.FAIT_PRESTATION_ESP pe
    WHERE  pe.CLICHE = :1
    GROUP BY pe.DOS_CODE
),

-- ── CTE 4 : premier débours RB Maternité (R3 — fallback délai) ───────────
-- Utilisé si PE_DATE_NOTIFICATION absent — 417 dossiers MAT avec DEB_TYPE='RB'
premier_deb_nat AS (
    SELECT
        deb.DOS_CODE,
        MIN(deb.DEB_DATE_APPROBATION) AS DEB_DATE_APPROBATION,
        MIN(deb.DEB_DATE_INSERT)      AS DEB_DATE_INSERT
    FROM   DWH.FAIT_DEBOURS deb
    WHERE  deb.CLICHE    = :1
      AND  deb.DEB_TYPE  = 'RB'
    GROUP BY deb.DOS_CODE
),

-- ── CTE 5 : base dossiers enrichis ───────────────────────────────────────
-- Driver : FAIT_DEBOURS (dossiers ayant au moins un débours)
-- Jointure FAIT_DOSSIER pour statut, géographie, dates
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
        td.CODE_PRESTATION,
        dp.DESCRIPTION                                       AS DESCRIPTION_PRESTATION,

        -- Flag rejeté R8
        CASE WHEN dr.DOS_CODE IS NOT NULL
             THEN 1 ELSE 0 END                               AS FLAG_REJETE,

        -- Délai liquidation R2 (ESP : V/A/F >= 2018-01-01) / R3 (NAT : M >= 2017-01-01)
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
            -- R3 : Maternité — priorité PE_DATE_NOTIFICATION, fallback DEB_DATE_APPROBATION
            WHEN 'M' THEN
                CASE
                    -- Priorité : PE_DATE_NOTIFICATION - RD_DATE_RECEPTION (3 938 dossiers)
                    WHEN ne.PE_DATE_NOTIFICATION IS NOT NULL
                     AND rd.RD_DATE_RECEPTION    IS NOT NULL
                    THEN ne.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                    -- Fallback : DEB_DATE_APPROBATION - DEB_DATE_INSERT (417 dossiers RB)
                    WHEN pdn.DEB_DATE_APPROBATION IS NOT NULL
                     AND pdn.DEB_DATE_INSERT      IS NOT NULL
                    THEN pdn.DEB_DATE_APPROBATION - pdn.DEB_DATE_INSERT
                    ELSE NULL
                END
            ELSE NULL
        END                                                  AS DELAI_JOURS

    -- SOURCE PRINCIPALE : dossiers ayant au moins un débours
    FROM (SELECT DISTINCT DOS_CODE FROM DWH.FAIT_DEBOURS)   deb

    -- DOSSIER — statut, géographie, dates
    JOIN      DWH.FAIT_DOSSIER               dos
           ON dos.DOS_CODE            = deb.DOS_CODE
          AND dos.TDOS_CODE           IN ('V', 'A', 'F', 'M')
          AND dos.DOS_DATE_OUVERTURE  IS NOT NULL

    -- INDIVIDU
    LEFT JOIN DWH.FAIT_INDIVIDU              ind
           ON ind.IND_ID         = dos.IND_ID

    -- Âge au 31/12 de l'année d'ouverture du dossier
    LEFT JOIN DTM.DIM_TRANCHE_AGE            tag
           ON TRUNC(
                  MONTHS_BETWEEN(
                      TO_DATE('31/12/' ||
                          TO_CHAR(EXTRACT(YEAR FROM dos.DOS_DATE_OUVERTURE)),
                          'DD/MM/YYYY'),
                      ind.IND_DATE_NAISSANCE
                  ) / 12
              ) BETWEEN tag.INF AND tag.SUP

    -- CODE_PRESTATION dominant R7
    LEFT JOIN tpe_dominant                   td
           ON td.DOS_CODE        = deb.DOS_CODE

    -- Libellé prestation
    LEFT JOIN DTM.DIM_TYPE_PRESTATION        dp
           ON dp.CODE_PRESTATION = td.CODE_PRESTATION

    -- Rejetés R8
    LEFT JOIN dossiers_rejetes               dr
           ON dr.DOS_CODE        = deb.DOS_CODE

    -- Réception R2 — via DOSSIER.RD_ID (pas DOS_CODE)
    LEFT JOIN DWH.FAIT_RECEPTION_DOSSIER     rd
           ON rd.RD_ID           = dos.RD_ID

    -- Notification ESP
    LEFT JOIN notif_esp                      ne
           ON ne.DOS_CODE        = deb.DOS_CODE

    -- Débours NAT Maternité R3 — fallback délai si PE_DATE_NOTIFICATION absent
    LEFT JOIN premier_deb_nat               pdn
           ON pdn.DOS_CODE       = deb.DOS_CODE
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

    SYSDATE                                                  AS DATE_CHARGEMENT,
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
