-- =============================================================================
-- Point 15. La formule du taux restant à traiter
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « pour les pensions fin 2025 (estimation) : environ
--   70 % avec la formule du recueil, de l'ordre de 40 % avec celle de la
--   grille. Hors norme dans les deux cas. »
-- Requête : le stock de demandes de pension en instance au 31/12/2024 et au
--   31/12/2025, estimé avec les règles d'aujourd'hui, puis le taux avec les
--   deux formules.
-- Nouvelle requête : elle refait l'estimation du 08/11/2026, tirée de P50
--   (lot3b_controles_t25.sql, exécutée le 30/10/2026), aux deux dates.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t25_requete_proposee.sql, colonne
--   TAUX_RESTANT_A_TRAITER du SELECT final, avec les blocs instance_fin et
--   instance_agg. Fiche des règles : règle 26.
-- À noter : le point dit « on prend la formule du recueil ». La requête t25
--   calcule encore une troisième formule : en instance au 31/12 ÷ (en
--   instance au 1er janvier + déposés). Le réglage formule_taux, qui
--   choisirait entre le recueil et la feuille 04, reste à créer
--   (regles_t25_t27_dossiers.md, N4, Q19).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Les deux formules :
--   · recueil : en instance à la fin ÷ reçus dans l'année × 100 ;
--   · feuille 04 de la grille de la CNSS : en instance à la fin ÷ (en instance
--     au début + reçus − rejetés) × 100.
-- En instance : reçue au guichet (RECEPTION_DOSSIER), ni liquidée (pas de
-- notification avant la date) ni rejetée (DOSSIER_REJETE), hors coordination,
-- comme P50. Avec les règles d'aujourd'hui, restent :
--   · les réceptions depuis 2016 sans dossier et qui ne visent aucun dossier ;
--   · les demandes dont le dossier n'est pas fermé (DOS_STATUT <> F).
-- Limite : DOS_STATUT est le statut du 02/02/2026, pas celui du 31/12/2024.
-- -----------------------------------------------------------------------------
WITH bornes AS (
    SELECT 2024 AS annee, DATE '2025-01-01' AS fin FROM DUAL
    UNION ALL SELECT 2025, DATE '2026-01-01' FROM DUAL
),
liq AS (
    SELECT d.DOS_CODE, d.RD_ID, d.DOS_STATUT, d.DOS_CATEGORIE,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))  AS date_liquidation
    FROM USER_DWH.DOSSIER d
    LEFT JOIN USER_DWH.PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
    WHERE d.TDOS_CODE = 'V'
    GROUP BY d.DOS_CODE, d.RD_ID, d.DOS_STATUT, d.DOS_CATEGORIE
),
instance AS (
    SELECT b.annee,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION) AS annee_reception,
           r.DOS_CODE                             AS dossier_vise,
           l.DOS_CODE,
           l.DOS_STATUT
    FROM bornes b
    JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_DATE_RECEPTION < b.fin
    LEFT JOIN liq l ON l.RD_ID = r.RD_ID
    WHERE r.TDOS_CODE = 'V'
      AND NVL(l.DOS_CATEGORIE, 'ORDINAIRE') NOT IN ('COOR_AC', 'CNSS_AC')
      AND (l.date_liquidation IS NULL OR l.date_liquidation >= b.fin)
      AND NOT EXISTS (SELECT 1
                      FROM   USER_DWH.DOSSIER_REJETE x
                      WHERE  x.RD_ID = r.RD_ID
                        AND  x.DOSRE_DATE_REJET < b.fin)
),
stock AS (
    SELECT annee,
           SUM(CASE WHEN DOS_CODE IS NULL AND dossier_vise IS NULL
                     AND annee_reception >= 2016 THEN 1 ELSE 0 END)
         + SUM(CASE WHEN DOS_CODE IS NOT NULL AND NVL(DOS_STATUT, '-') <> 'F'
                    THEN 1 ELSE 0 END)                            AS en_instance
    FROM instance
    GROUP BY annee
),
flux AS (
    SELECT (SELECT COUNT(*)
            FROM   USER_DWH.RECEPTION_DOSSIER
            WHERE  TDOS_CODE = 'V'
              AND  RD_DATE_RECEPTION >= DATE '2025-01-01'
              AND  RD_DATE_RECEPTION <  DATE '2026-01-01')        AS recus_2025,
           (SELECT COUNT(*)
            FROM   USER_DWH.DOSSIER_REJETE
            WHERE  TDOS_CODE = 'V'
              AND  DOSRE_DATE_REJET >= DATE '2025-01-01'
              AND  DOSRE_DATE_REJET <  DATE '2026-01-01')         AS rejetes_2025
    FROM DUAL
)
SELECT d.en_instance                                              AS en_instance_debut_2025,
       f.en_instance                                              AS en_instance_fin_2025,
       x.recus_2025,
       x.rejetes_2025,
       ROUND(100 * f.en_instance / x.recus_2025, 1)               AS taux_recueil_pct,
       ROUND(100 * f.en_instance
             / (d.en_instance + x.recus_2025 - x.rejetes_2025), 1) AS taux_feuille_04_pct
FROM stock d
JOIN stock f ON f.annee = 2025
CROSS JOIN flux x
WHERE d.annee = 2024;

-- Résultat : pas encore exécutée. Attendu, d'après P50 (fin 2025) et P09
-- (4 671 réceptions et 14 rejets de pension en 2025, lot 1) :
--   En instance fin 2025 : environ 3 366 (1 337 + 1 132 + 897, le calcul est
--   dans point_021_demandes_qui_trainent.sql, requête 2).
--   Formule du recueil : 3 366 ÷ 4 671 = 72,1 %.
--   Feuille 04 : 3 366 ÷ (stock du début + 4 671 − 14). Le stock du début
--   n'a jamais été mesuré ; s'il est du même ordre que celui de la fin,
--   3 366 ÷ 8 023 = 42,0 % (calculé ici).
-- Lecture : environ 70 % avec la formule du recueil, de l'ordre de 40 % avec
--   celle de la feuille 04 ; hors de la norme de 15 % dans les deux cas.
