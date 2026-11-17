-- =============================================================================
-- Point 76. Les départs à la retraite de l'annuaire (T25)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « on y comparera les demandes de pension directe
--   reçues en 2024, sans l'allocation de vieillesse. » ; « 3 729 départs
--   publiés en 2024, soit 12,3 % de plus que les 3 322 entrées publiées
--   (nouveaux retraités et allocations de vieillesse). »
-- Requête : les réceptions de pension de 2024, selon ce que porte le dossier
--   qu'elles ont ouvert, face aux départs et aux entrées publiés.
-- Nouvelle requête : aucun lot ne l'avait mesuré ; c'est la comparaison
--   annoncée par le point.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : aucune requête proposée.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Réceptions de pension (RECEPTION_DOSSIER, TDOS_CODE = V) de 2024. Le dossier
-- ouvert par la réception (DOSSIER.RD_ID) porte une pension directe (RPV,
-- RPRA ou RPI), seulement une allocation de vieillesse (RAV), seulement des
-- survivants, ou rien. Une réception sans dossier est souvent une demande
-- déposée sur un dossier existant (une réversion) ou une demande en attente.
-- Les chiffres publiés de l'annuaire CNSS 2024 sont écrits dans la requête.
-- -----------------------------------------------------------------------------
WITH code_dossier AS (
    SELECT DOS_CODE,
           MAX(CASE WHEN TPE_CODE IN ('RPV', 'RPRA', 'RPI')
                    THEN 1 ELSE 0 END)                            AS pension_directe,
           MAX(CASE WHEN TPE_CODE = 'RAV' THEN 1 ELSE 0 END)      AS allocation_vieillesse,
           MAX(CASE WHEN TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA', 'RAS', 'RASC', 'RASO', 'RASA')
                    THEN 1 ELSE 0 END)                            AS survivants
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
recep AS (
    SELECT r.RD_ID,
           d.DOS_CODE,
           c.pension_directe,
           c.allocation_vieillesse,
           c.survivants
    FROM USER_DWH.RECEPTION_DOSSIER r
    LEFT JOIN USER_DWH.DOSSIER d ON d.RD_ID    = r.RD_ID
    LEFT JOIN code_dossier     c ON c.DOS_CODE = d.DOS_CODE
    WHERE r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION >= DATE '2024-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2025-01-01'
),
publie AS (
    -- Annuaire CNSS 2024 : tableau 27 (p. 21), départs à la retraite ;
    -- tableau 23 (p. 19), nouvelles pensions de vieillesse (2 486) et
    -- anticipées (3) ; tableau 24 (p. 19), allocations de vieillesse (833).
    SELECT 3729 AS departs_tableau_27,
           2486 + 3 + 833 AS entrees_publiees
    FROM DUAL
)
SELECT COUNT(*)                                                   AS receptions_2024,
       SUM(CASE WHEN pension_directe = 1 THEN 1 ELSE 0 END)       AS dossier_pension_directe,
       SUM(CASE WHEN NVL(pension_directe, 0) = 0
                 AND allocation_vieillesse = 1 THEN 1 ELSE 0 END) AS dossier_allocation_seule,
       SUM(CASE WHEN NVL(pension_directe, 0) = 0
                 AND NVL(allocation_vieillesse, 0) = 0
                 AND survivants = 1 THEN 1 ELSE 0 END)            AS dossier_survivants_seuls,
       SUM(CASE WHEN DOS_CODE IS NOT NULL
                 AND pension_directe IS NULL THEN 1 ELSE 0 END)   AS dossier_sans_prestation,
       SUM(CASE WHEN DOS_CODE IS NULL THEN 1 ELSE 0 END)          AS sans_dossier,
       MAX(p.departs_tableau_27)                                  AS departs_tableau_27,
       MAX(p.entrees_publiees)                                    AS entrees_publiees
FROM recep
CROSS JOIN publie p;

-- Résultat : pas encore exécutée.
-- Chiffres publiés (calculé ici) : 2 486 + 3 + 833 = 3 322 entrées ;
--   3 729 ÷ 3 322 = +12,3 %.
-- Lecture attendue : si « dossier pension directe » retombe près de 3 729,
--   le tableau 27 compte les demandes de pension directe reçues au guichet ;
--   s'il retombe près de 2 489, il compte les nouveaux retraités.
