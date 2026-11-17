-- =============================================================================
-- Point 23. La déclaration d'accident sans suite (T26)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « un dossier d'accident du travail (DOSSIER,
--   TDOS_CODE = A) sans aucune prestation ensuite (PRESTATION_ESP) ni
--   remboursement de soins (DEBOURS). » ; « 629 des 1 189 dossiers d'accident
--   du travail ouverts en 2024 n'ont rien donné. L'annuaire CNSS 2024 compte
--   aussi toutes les déclarations : 1 171 en 2024 (tableau 47). »
-- Requête : par branche et par année, les réceptions, les réceptions sans
--   suite, les dossiers ouverts, ceux qui n'ont rien donné, les fermés et les
--   rejets.
-- Reprise de : lot4_controles_t26_t27.sql, P68 (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. Lit DEBOURS
--   (les soins) : plusieurs minutes.
-- Règle appliquée dans : t26_requete_proposee.sql, bloc depot (une
--   déclaration sans demande va au rang 6, « Pas encore de ligne », compté
--   dans le Total), blocs parametres (sans_suite_mois = 24) et instance_fin
--   (au-delà de 24 mois sans suite, elle sort du stock). Fiche des règles :
--   règles 29 et 32.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Accidents du travail (A), prestations familiales (F) et
-- maternité (M), de 2019 à 2025 : réceptions, réceptions sans suite, dossiers
-- ouverts, dossiers sans aucune prestation (ni prestation ni soins), dont
-- fermés, avec un rejet, rejets de l'année.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 7
),
branches AS (
    SELECT 'A' AS tdos FROM DUAL
    UNION ALL SELECT 'F' FROM DUAL
    UNION ALL SELECT 'M' FROM DUAL
),
recep0 AS (
    SELECT r.RD_ID,
           r.TDOS_CODE                                            AS tdos,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION)                 AS annee,
           r.DOS_CODE
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE IN ('A', 'F', 'M')
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
),
ouvre AS (
    SELECT DISTINCT RD_ID FROM USER_DWH.DOSSIER WHERE RD_ID IS NOT NULL
),
rejete_rd AS (
    SELECT DISTINCT RD_ID FROM USER_DWH.DOSSIER_REJETE WHERE RD_ID IS NOT NULL
),
rec_agg AS (
    SELECT r.tdos,
           r.annee,
           COUNT(*)                                               AS nb,
           SUM(CASE WHEN o.RD_ID IS NULL
                     AND x.RD_ID IS NULL
                     AND r.DOS_CODE IS NULL
                    THEN 1 ELSE 0 END)                            AS sans_suite
    FROM recep0 r
    LEFT JOIN ouvre     o ON o.RD_ID = r.RD_ID
    LEFT JOIN rejete_rd x ON x.RD_ID = r.RD_ID
    GROUP BY r.tdos, r.annee
),
dos AS (
    SELECT d.DOS_CODE,
           d.TDOS_CODE                                            AS tdos,
           d.DOS_STATUT,
           EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)                AS annee
    FROM USER_DWH.DOSSIER d
    WHERE d.TDOS_CODE IN ('A', 'F', 'M')
      AND d.DOS_DATE_OUVERTURE >= DATE '2019-01-01'
      AND d.DOS_DATE_OUVERTURE <  DATE '2026-01-01'
),
esp AS (
    SELECT DISTINCT pe.DOS_CODE
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dos ON dos.DOS_CODE = pe.DOS_CODE
),
soins AS (
    SELECT DISTINCT deb.DOS_CODE
    FROM USER_DWH.DEBOURS deb
    JOIN dos ON dos.DOS_CODE = deb.DOS_CODE
    WHERE dos.tdos = 'A'
      AND deb.TPN_CODE IS NOT NULL
),
rejete_dos AS (
    SELECT DISTINCT DOS_CODE FROM USER_DWH.DOSSIER_REJETE WHERE DOS_CODE IS NOT NULL
),
dos_agg AS (
    SELECT d.tdos,
           d.annee,
           COUNT(*)                                               AS ouverts,
           SUM(CASE WHEN e.DOS_CODE IS NULL AND s.DOS_CODE IS NULL
                    THEN 1 ELSE 0 END)                            AS sans_prestation,
           SUM(CASE WHEN e.DOS_CODE IS NULL AND s.DOS_CODE IS NULL
                     AND d.DOS_STATUT = 'F'
                    THEN 1 ELSE 0 END)                            AS sans_prestation_fermes,
           SUM(CASE WHEN j.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END) AS avec_rejet
    FROM dos d
    LEFT JOIN esp        e ON e.DOS_CODE = d.DOS_CODE
    LEFT JOIN soins      s ON s.DOS_CODE = d.DOS_CODE
    LEFT JOIN rejete_dos j ON j.DOS_CODE = d.DOS_CODE
    GROUP BY d.tdos, d.annee
),
rej_agg AS (
    SELECT TDOS_CODE                                              AS tdos,
           EXTRACT(YEAR FROM DOSRE_DATE_REJET)                    AS annee,
           COUNT(*)                                               AS nb
    FROM USER_DWH.DOSSIER_REJETE
    WHERE TDOS_CODE IN ('A', 'F', 'M')
    GROUP BY TDOS_CODE, EXTRACT(YEAR FROM DOSRE_DATE_REJET)
)
SELECT b.tdos                                                     AS branche,
       a.annee,
       NVL(rc.nb, 0)                                              AS receptions,
       NVL(rc.sans_suite, 0)                                      AS receptions_sans_suite,
       NVL(dg.ouverts, 0)                                         AS dossiers_ouverts,
       NVL(dg.sans_prestation, 0)                                 AS ouverts_sans_prestation,
       NVL(dg.sans_prestation_fermes, 0)                          AS dont_fermes,
       NVL(dg.avec_rejet, 0)                                      AS ouverts_avec_rejet,
       NVL(rj.nb, 0)                                              AS rejets_de_l_annee
FROM branches b
CROSS JOIN annees a
LEFT JOIN rec_agg rc ON rc.tdos = b.tdos AND rc.annee = a.annee
LEFT JOIN dos_agg dg ON dg.tdos = b.tdos AND dg.annee = a.annee
LEFT JOIN rej_agg rj ON rj.tdos = b.tdos AND rj.annee = a.annee
ORDER BY b.tdos, a.annee;

-- Résultat obtenu (P68, exécutée le 30/10/2026), accidents du travail :
--   Année   Réceptions   Sans suite   Dossiers ouverts   Sans aucune prestation   dont fermés   Avec rejet   Rejets
--   2019    1 638        108          1 328              556                      14            2            1
--   2020    1 639         49          1 403              538                       7            5            7
--   2021    1 545         35          1 353              469                       8            3            2
--   2022    1 542         35          1 370              541                       2            3            4
--   2023    1 379         34          1 226              604                       2            6            7
--   2024    1 394         70          1 189              629                       1            0            0
--   2025    1 481        104          1 237              821                       0            0            1
--   Prestations familiales : 20 à 57 dossiers sans prestation par an, 196 à
--   511 réceptions sans suite (483 en 2025). Maternité : 4 à 9 dossiers sans
--   prestation, 37 à 141 réceptions sans suite.
-- Face au tableau 47 de l'annuaire CNSS 2024 (p. 38) : 1 171 accidents du
-- travail et maladies professionnelles déclarés en 2024.
-- Lecture : en 2024, 629 des 1 189 dossiers d'accident ouverts n'ont rien
--   donné, et ils ne sont presque jamais fermés. L'annuaire compte toutes les
--   déclarations (1 171), qu'elles aient une suite ou non.
