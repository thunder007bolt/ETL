-- DTM_DOSSIER V1
-- Sources : DWH.FAIT_DOSSIER            (principal)
--           JOIN DWH.FAIT_INDIVIDU      (sexe, date naissance)
--           LEFT JOIN mois_cotises      (PE_MOIS_COTISATION, PE_DATE_NOTIFICATION)
--           LEFT JOIN FAIT_RECEPTION_DOSSIER (RD_DATE_RECEPTION — délai R2)
--           LEFT JOIN premier_deb_nat   (DEB_DATE_APPROBATION/INSERT — délai R3)
-- Grain   : ANNEE × MOIS × TDOS_CODE × EST_CONFORME_CIPRES × GROUPE
--           × DR_NO × SP_NO × LP_NO × SEXE × TRANCHE_AGE
-- R1      : date référence = DOS_DATE_OUVERTURE
-- R2      : délai ESP = PE_DATE_NOTIFICATION - RD_DATE_RECEPTION (filtre >= 2018-01-01)
-- R3      : délai NAT (Maternité) = DEB_DATE_APPROBATION - DEB_DATE_INSERT (filtre >= 2017-01-01)
-- R4      : conformité CIPRES = délai <= 45 jours
-- R5      : groupe = LN_TYPE depuis FAIT_LIEN (C=VEU, E=ORP, A=ASC, sinon TIT)
-- R6      : TRANCHE_AGE au 31/12/ANNEE (règle CIPRES — tranches 5 ans)
-- Exclus  : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
WITH

-- ── R3 : premier débours RB (NAT) par dossier ────────────────
-- Utilisé uniquement pour TDOS_CODE='M' (Maternité)
premier_deb_nat AS (
    SELECT
        deb.DOS_CODE,
        MIN(deb.DEB_DATE_APPROBATION) AS DEB_DATE_APPROBATION,
        MIN(deb.DEB_DATE_INSERT)      AS DEB_DATE_INSERT
    FROM  DWH.FAIT_DEBOURS deb
    WHERE deb.DEB_TYPE = 'RB'
    GROUP BY deb.DOS_CODE
),

-- ── Mois cotisés par dossier ─────────────────────────────────
-- PE_MOIS_COTISATION identique pour toutes les lignes d'un même DOS_CODE
-- MAX() pour dédupliquer sans cumul erroné
mois_cotises AS (
    SELECT
        pe.DOS_CODE,
        MAX(pe.PE_MOIS_COTISATION)   AS PE_MOIS_COTISATION,
        MAX(pe.PE_DATE_NOTIFICATION) AS PE_DATE_NOTIFICATION
    FROM  DWH.FAIT_PRESTATION_ESP pe
    GROUP BY pe.DOS_CODE
),

-- ── Base : dossiers enrichis ──────────────────────────────────
base AS (
    SELECT
        -- Temporel — R1 : date ouverture dossier
        EXTRACT(YEAR  FROM dos.DOS_DATE_OUVERTURE)          AS ANNEE,
        EXTRACT(MONTH FROM dos.DOS_DATE_OUVERTURE)          AS MOIS,

        -- Identifiants
        dos.DOS_CODE,
        dos.IND_ID,
        dos.TDOS_CODE,
        dos.CLICHE,

        -- Géographie
        dos.LP_NO,
        dos.SP_NO,
        dos.DR_NO,

        -- Démographie
        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,

        -- Mois cotisés
        mc.PE_MOIS_COTISATION,

        -- Groupe bénéficiaire — R5
        CASE
            WHEN dos.IND_ID IS NULL THEN 'TIT'
            ELSE NVL(
                (SELECT CASE lien.LN_TYPE
                            WHEN 'C' THEN 'VEU'
                            WHEN 'E' THEN 'ORP'
                            WHEN 'A' THEN 'ASC'
                            ELSE          'TIT'
                        END
                 FROM  DWH.FAIT_LIEN lien
                 WHERE lien.IND_ID_1 = dos.IND_ID
                   AND lien.LN_ACTIF = 'O'
                   AND ROWNUM = 1),
                'TIT'
            )
        END                                                 AS GROUPE,

        -- Délai liquidation — R2 (ESP) / R3 (NAT Maternité)
        CASE dos.TDOS_CODE
            WHEN 'V' THEN
                CASE WHEN mc.PE_DATE_NOTIFICATION IS NOT NULL
                      AND rd.RD_DATE_RECEPTION    IS NOT NULL
                      AND mc.PE_DATE_NOTIFICATION >= DATE '2018-01-01'
                      AND rd.RD_DATE_RECEPTION    >= DATE '2018-01-01'
                     THEN mc.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                     ELSE NULL
                END
            WHEN 'A' THEN
                CASE WHEN mc.PE_DATE_NOTIFICATION IS NOT NULL
                      AND rd.RD_DATE_RECEPTION    IS NOT NULL
                      AND mc.PE_DATE_NOTIFICATION >= DATE '2018-01-01'
                      AND rd.RD_DATE_RECEPTION    >= DATE '2018-01-01'
                     THEN mc.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                     ELSE NULL
                END
            WHEN 'F' THEN
                CASE WHEN mc.PE_DATE_NOTIFICATION IS NOT NULL
                      AND rd.RD_DATE_RECEPTION    IS NOT NULL
                      AND mc.PE_DATE_NOTIFICATION >= DATE '2018-01-01'
                      AND rd.RD_DATE_RECEPTION    >= DATE '2018-01-01'
                     THEN mc.PE_DATE_NOTIFICATION - rd.RD_DATE_RECEPTION
                     ELSE NULL
                END
            WHEN 'M' THEN
                CASE WHEN deb_nat.DEB_DATE_APPROBATION IS NOT NULL
                      AND deb_nat.DEB_DATE_INSERT       IS NOT NULL
                      AND deb_nat.DEB_DATE_INSERT       >= DATE '2017-01-01'
                     THEN deb_nat.DEB_DATE_APPROBATION - deb_nat.DEB_DATE_INSERT
                     ELSE NULL
                END
            ELSE NULL
        END                                                 AS DELAI_JOURS

    FROM      DWH.FAIT_DOSSIER            dos
    JOIN      DWH.FAIT_INDIVIDU           ind
        ON    ind.IND_ID    = dos.IND_ID
    LEFT JOIN mois_cotises                mc
        ON    mc.DOS_CODE   = dos.DOS_CODE
    LEFT JOIN (
        SELECT DOS_CODE, MIN(RD_DATE_RECEPTION) AS RD_DATE_RECEPTION
        FROM   DWH.FAIT_RECEPTION_DOSSIER
        GROUP BY DOS_CODE
    )                                     rd
        ON    rd.DOS_CODE   = dos.DOS_CODE
    LEFT JOIN premier_deb_nat             deb_nat
        ON    deb_nat.DOS_CODE = dos.DOS_CODE

    WHERE dos.CLICHE = :1
)

SELECT

    -- ── TEMPOREL ────────────────────────────────────────────────────
    b.ANNEE,
    b.MOIS,
    CEIL(b.MOIS / 3)                                        AS TRIMESTRE,

    -- ── BRANCHE ─────────────────────────────────────────────────────
    b.TDOS_CODE,

    -- ── GROUPE BÉNÉFICIAIRE ──────────────────────────────────────────
    b.GROUPE,
    CASE b.GROUPE
        WHEN 'TIT' THEN 'Titulaire'
        WHEN 'VEU' THEN 'Veuf / Veuve'
        WHEN 'ORP' THEN 'Orphelin'
        WHEN 'ASC' THEN 'Ascendant'
    END                                                     AS LIBELLE_GROUPE,

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
    COUNT(b.DOS_CODE)                                       AS NB_DOSSIERS,
    COUNT(DISTINCT b.IND_ID)                                AS NB_BENEFICIAIRES,
    SUM(NVL(b.PE_MOIS_COTISATION, 0))                       AS NB_MOIS_COTISES,

    -- ── MESURES DÉLAIS ───────────────────────────────────────────────
    AVG(CASE WHEN b.DELAI_JOURS IS NOT NULL THEN b.DELAI_JOURS END) AS DELAI_MOYEN_JOURS,
    MAX(b.DELAI_JOURS)                                      AS DELAI_MAX_JOURS,
    MIN(b.DELAI_JOURS)                                      AS DELAI_MIN_JOURS,

    -- ── MESURES CONFORMITÉ ───────────────────────────────────────────
    SUM(CASE WHEN b.DELAI_JOURS IS NULL  THEN 0
             WHEN b.DELAI_JOURS <= 45    THEN 1
             ELSE 0 END)                                    AS NB_CONFORMES,
    SUM(CASE WHEN b.DELAI_JOURS IS NULL  THEN 0
             WHEN b.DELAI_JOURS > 45     THEN 1
             ELSE 0 END)                                    AS NB_NON_CONFORMES,
    SUM(CASE WHEN b.DELAI_JOURS IS NULL  THEN 1
             ELSE 0 END)                                    AS NB_NON_CALCULABLES,

    -- ── CLICHE ──────────────────────────────────────────────────────
    b.CLICHE                                                AS CLICHE

FROM base b
LEFT JOIN DTM.DIM_TRANCHE_AGE              tag ON TRUNC(MONTHS_BETWEEN(TO_DATE('31/12/'||TO_CHAR(b.ANNEE),'DD/MM/YYYY'), b.IND_DATE_NAISSANCE)/12) BETWEEN tag.INF AND tag.SUP

GROUP BY
    b.ANNEE,
    b.MOIS,
    CEIL(b.MOIS / 3),
    b.TDOS_CODE,
    b.GROUPE,
    CASE b.GROUPE
        WHEN 'TIT' THEN 'Titulaire' WHEN 'VEU' THEN 'Veuf / Veuve'
        WHEN 'ORP' THEN 'Orphelin'  WHEN 'ASC' THEN 'Ascendant'
    END,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    b.IND_SEXE,
    CASE b.IND_SEXE WHEN 1 THEN 'Masculin' WHEN 2 THEN 'Feminin' ELSE NULL END,
    tag.TAG_CODE,
    b.CLICHE
