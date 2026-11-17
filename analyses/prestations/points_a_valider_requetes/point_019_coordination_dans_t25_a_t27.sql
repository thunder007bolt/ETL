-- =============================================================================
-- Point 19. La coordination dans T25 à T27
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « la catégorie du dossier (DOSSIER, DOS_CATEGORIE)
--   vaut COOR_AC : 3 326 dossiers, dont 76 % reçus au guichet de la CNSS. Les
--   dossiers transférés à une autre caisse (CNSS_AC, sens à confirmer) sont
--   écartés. » ; « 238 dépôts et 292 liquidations de pension en 2025, soit 6 à
--   8 % de T25. L'annuaire CNSS 2024 compte ces pensions dans ses
--   pensionnés. »
-- Requêtes :
--   1. Les dossiers de pension par catégorie : avec réception, notifiés,
--      déposés et liquidés en 2025.
--   2. Les pensions de T32 en 2025 par catégorie : comptées, payées par la
--      CNSS, montant.
-- Reprise de : lot3b_controles_t25.sql, P48 (exécutée le 30/10/2026) ;
--   lot5_beneficiaires_31_12.sql, P78 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t25_requete_proposee.sql, blocs prest et recep_cand
--   (seuls les dossiers CNSS_AC sont écartés ; les COOR_AC restent). Fiche
--   des règles : règle 18.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers de pension (TDOS_CODE = V) par catégorie
-- (DOS_CATEGORIE) : nombre, avec une réception, avec une notification,
-- déposés en 2025, liquidés en 2025 (première notification), durée médiane.
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DOS_CODE, TRUNC(MIN(PE_DATE_NOTIFICATION)) AS date_liquidation
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
)
SELECT NVL(d.DOS_CATEGORIE, '(vide)')                                          AS categorie,
       COUNT(*)                                                                AS nb_dossiers,
       SUM(CASE WHEN d.DOS_CODE_AUTRE_CAISSE IS NOT NULL THEN 1 ELSE 0 END)    AS avec_code_autre_caisse,
       SUM(CASE WHEN r.RD_ID IS NOT NULL THEN 1 ELSE 0 END)                    AS avec_reception,
       SUM(CASE WHEN n.date_liquidation IS NOT NULL THEN 1 ELSE 0 END)         AS avec_notification,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS deposes_2025,
       SUM(CASE WHEN n.date_liquidation >= DATE '2025-01-01'
                 AND n.date_liquidation <  DATE '2026-01-01' THEN 1 ELSE 0 END)  AS liquides_2025,
       ROUND(MEDIAN(CASE WHEN n.date_liquidation >= DATE '2025-01-01'
                          AND n.date_liquidation <  DATE '2026-01-01'
                         THEN n.date_liquidation - TRUNC(r.RD_DATE_RECEPTION)
                    END))                                                      AS duree_mediane_2025_j
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID    = d.RD_ID
LEFT JOIN notif                      n ON n.DOS_CODE = d.DOS_CODE
WHERE d.TDOS_CODE = 'V'
GROUP BY NVL(d.DOS_CATEGORIE, '(vide)')
ORDER BY nb_dossiers DESC;

-- Résultat obtenu (P48, exécutée le 30/10/2026) :
--   Catégorie        Dossiers   Avec réception   Avec notification   Déposés 2025   Liquidés 2025   Durée médiane 2025
--   ORDINAIRE        37 864     31 636           32 195              3 332          3 401            70 j
--   sans catégorie   30 774         35           23 628                  1              3           166 j
--   COOR_CNSS         3 466      1 429            3 217                184            204            75 j
--   COOR_AC           3 326      2 530            1 702                238            292            88 j
--   CNSS_AC             153         12              128                  0              1          2 536 j
-- Lecture (calculé ici) :
--   · COOR_AC : 2 530 dossiers sur 3 326 ont une réception à la CNSS, soit
--     76 %.
--   · En 2025 : 238 dépôts sur 3 755 (6,3 %) et 292 liquidations sur 3 901
--     (7,5 %) : « 6 à 8 % de T25 ».
--   · CNSS_AC : 153 dossiers, rien de déposé en 2025 ; ils sont écartés.


-- -----------------------------------------------------------------------------
-- Requête 2. Pensions de T32 au 31/12/2025 par catégorie du dossier :
-- comptées, payées par la CNSS (un débours dans l'année), montant.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
),
compte AS (
    SELECT p.PE_ID,
           pm.tableau,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2025-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2025-12-31')
      AND (pm.tableau <> 'T35'
           OR 2025 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
),
paie AS (
    SELECT deb.PE_ID, SUM(deb.DEB_MONTANT) AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2025-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM compte)
    GROUP BY deb.PE_ID
)
SELECT c.tableau,
       c.categorie,
       COUNT(*)                                                   AS pensions_comptees,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)      AS payees_par_la_cnss_2025,
       ROUND(SUM(pa.montant))                                     AS montant_2025
FROM compte c
LEFT JOIN paie pa ON pa.PE_ID = c.PE_ID
GROUP BY c.tableau, c.categorie
ORDER BY c.tableau, pensions_comptees DESC;

-- Résultat obtenu (P78, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P78_coordination_2025.tsv) :
--   Catégorie        Pensions comptées   Payées par la CNSS   Montant
--   ORDINAIRE        21 060              20 656               33 057 988 669
--   sans catégorie    6 844               6 448                5 595 141 728
--   COOR_AC           2 901                   0                            0
--   COOR_CNSS         2 012               1 944                2 312 485 002
--   CNSS_AC              34                   2                    1 553 100
-- Lecture : les pensions COOR_AC n'ont aucun paiement de la CNSS. Sans elles,
--   T32 de 2024 tomberait vers 28 200, 8 % sous le publié ; avec elles, il
--   est à +0,9 % : l'annuaire CNSS 2024 les compte dans ses pensionnés.
