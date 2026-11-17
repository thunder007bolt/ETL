-- =============================================================================
-- Point 20. La date du dépôt
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « la date de réception (RECEPTION_DOSSIER,
--   RD_DATE_RECEPTION). » ; « 4 671 réceptions de pension en 2025 ; 96 % des
--   pièces sont saisies le jour de la réception. »
-- Requête : les réceptions de pension de 2025 et les dates autour du dépôt :
--   saisie de la réception, rendez-vous, saisie des pièces, ouverture du
--   dossier, date de demande de la prestation.
-- Reprise de : lot3b_controles_t25.sql, P49 (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t25_requete_proposee.sql, blocs recep_cand, recep et
--   depot (le dépôt est la réception ; sans réception, la date de demande de
--   la prestation). Fiche des règles : règle 20.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Réceptions de pension (TDOS_CODE = V) de 2025 : pièces fournies
-- (RECEPTION_DOS_PIECE) et écart entre la réception et la saisie de la
-- dernière pièce, le rendez-vous, l'ouverture du dossier et la date de
-- demande de la prestation.
-- -----------------------------------------------------------------------------
WITH pieces AS (
    SELECT RD_ID,
           COUNT(*)                     AS nb_pieces,
           TRUNC(MAX(RDP_DATE_INSERT))  AS derniere_piece
    FROM USER_DWH.RECEPTION_DOS_PIECE
    GROUP BY RD_ID
),
demande AS (
    SELECT DOS_CODE, TRUNC(MIN(PE_DATE_DEMANDE)) AS date_demande
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
base AS (
    SELECT TRUNC(r.RD_DATE_RECEPTION)   AS date_reception,
           TRUNC(r.RD_DATE_INSERT)      AS date_saisie,
           TRUNC(r.RD_DATE_RDV)         AS date_rdv,
           p.nb_pieces,
           p.derniere_piece,
           TRUNC(d.DOS_DATE_OUVERTURE)  AS date_ouverture,
           dm.date_demande
    FROM USER_DWH.RECEPTION_DOSSIER r
    LEFT JOIN pieces           p  ON p.RD_ID     = r.RD_ID
    LEFT JOIN USER_DWH.DOSSIER d  ON d.RD_ID     = r.RD_ID
    LEFT JOIN demande          dm ON dm.DOS_CODE = d.DOS_CODE
    WHERE r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION >= DATE '2025-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
)
SELECT COUNT(*)                                                                AS receptions_2025,
       SUM(CASE WHEN nb_pieces IS NULL THEN 1 ELSE 0 END)                      AS sans_piece_saisie,
       ROUND(AVG(nb_pieces), 1)                                                AS pieces_par_reception,
       SUM(CASE WHEN derniere_piece - date_reception <= 0 THEN 1 ELSE 0 END)   AS pieces_le_jour_meme,
       SUM(CASE WHEN derniere_piece - date_reception BETWEEN 1 AND 7
                THEN 1 ELSE 0 END)                                             AS derniere_piece_1_a_7_j,
       SUM(CASE WHEN derniere_piece - date_reception BETWEEN 8 AND 30
                THEN 1 ELSE 0 END)                                             AS derniere_piece_8_a_30_j,
       SUM(CASE WHEN derniere_piece - date_reception > 30 THEN 1 ELSE 0 END)   AS derniere_piece_plus_30_j,
       ROUND(MEDIAN(date_saisie - date_reception))                             AS med_reception_a_saisie_j,
       SUM(CASE WHEN date_rdv IS NOT NULL THEN 1 ELSE 0 END)                   AS avec_rendez_vous,
       ROUND(MEDIAN(date_rdv - date_reception))                                AS med_reception_a_rdv_j,
       SUM(CASE WHEN date_ouverture IS NOT NULL THEN 1 ELSE 0 END)             AS avec_dossier,
       ROUND(MEDIAN(date_ouverture - date_reception))                          AS med_reception_a_ouverture_j,
       SUM(CASE WHEN date_ouverture - date_reception > 30 THEN 1 ELSE 0 END)   AS ouverture_plus_30_j_apres,
       SUM(CASE WHEN date_demande < date_reception THEN 1 ELSE 0 END)          AS demande_avant_reception,
       SUM(CASE WHEN date_demande > date_reception THEN 1 ELSE 0 END)          AS demande_apres_reception,
       ROUND(MEDIAN(date_demande - date_reception))                            AS med_reception_a_demande_j
FROM base;

-- Résultat obtenu (P49, exécutée le 30/10/2026) :
--   · 4 671 réceptions de 2025 ; 15 sans pièce ; 7,3 pièces par réception.
--   · Dernière pièce saisie : le jour même 4 485 (96 %), 1 à 7 jours après
--     111, 8 à 30 jours 35, plus de 30 jours 25.
--   · Réception saisie le jour même (médiane 0 jour). Toutes ont un
--     rendez-vous, 45 jours après en médiane.
--   · 3 755 ont un dossier, ouvert le jour même en médiane ; 423 ouverts plus
--     de 30 jours après.
--   · Date de demande de la prestation = date de réception en médiane (116
--     avant, 839 après).
-- Lecture : le dossier est complet à la réception, comme l'avait demandé
--   l'atelier de Kaya en 2017 : 96 % des pièces sont saisies le jour même ;
--   au plus 171 dossiers (4 %) ont été complétés après.
