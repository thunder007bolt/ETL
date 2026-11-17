-- =============================================================================
-- Point 5. La pension de la veuve (T25)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « la pension de la veuve (RPSV ou RPSF,
--   PRESTATION_ESP) est ouverte dans un nouveau dossier ou ajoutée au dossier
--   du défunt. La moitié des pensions de veuve sont dans le dossier du
--   défunt. » ; « l'annuaire CNSS 2024 compte autrement : des personnes, 1 542
--   nouveaux survivants en 2024 (tableau 23). La ligne des survivants de T25
--   sera plus basse. »
-- Requêtes :
--   1. Les pensions de survivant rangées dans le dossier du défunt, et celles
--      de 2025 qui ne sont pas la première notification de leur dossier.
--   2. Les nouvelles pensions de 2024 par type, face au tableau 23.
-- Reprise de : lot3b_controles_t25.sql, P51 (exécutée le 30/10/2026) ;
--   comparaison_annuaire_cnss_2024/requetes_comparaison.sql, A03 (exécutée le
--   30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 2
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t25_requete_proposee.sql, bloc demande (une demande
--   par dossier et par ligne ; liquidation = première notification de la
--   demande) et bloc recep_cand (une réception ANCIEN va à la réversion).
--   Fiche des règles : règle 17.
-- À noter :
--   · Le point écrit « notifiés ensemble ». Le bloc demande va plus loin :
--     tous les survivants d'un même dossier font une seule demande, même
--     notifiés à des dates différentes.
--   · Un même décès peut faire deux demandes si ses survivants sont dans deux
--     dossiers. Ce cas n'a jamais été mesuré.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Pensions et allocations de survivant (PRESTATION_ESP) des
-- dossiers de pension (DOSSIER, TDOS_CODE = V). « Dans un dossier avec droit
-- propre » : le dossier porte aussi une pension de l'assuré (RPV, RPRA, RPI ou
-- RAV), c'est le dossier du défunt. « Notifiée après la 1re notification » :
-- plus de 30 jours après la première notification du dossier.
-- -----------------------------------------------------------------------------
WITH dossier_v AS (
    SELECT DOS_CODE,
           MAX(CASE WHEN TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV')
                    THEN 1 ELSE 0 END)              AS a_droit_propre,
           TRUNC(MIN(PE_DATE_NOTIFICATION))         AS premiere_notif_dossier
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
assure_v AS (
    SELECT IND_ID, COUNT(*) AS nb_dossiers_v
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'V' AND IND_ID IS NOT NULL
    GROUP BY IND_ID
),
survivant AS (
    SELECT pe.TPE_CODE,
           TRUNC(pe.PE_DATE_NOTIFICATION)  AS date_notif,
           dv.a_droit_propre,
           dv.premiere_notif_dossier,
           av.nb_dossiers_v,
           d.RD_ID,
           r.RD_DATE_RECEPTION,
           i.IND_DATE_DECES
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN      USER_DWH.DOSSIER           d  ON d.DOS_CODE  = pe.DOS_CODE
    JOIN      dossier_v                  dv ON dv.DOS_CODE = pe.DOS_CODE
    LEFT JOIN assure_v                   av ON av.IND_ID   = d.IND_ID
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r  ON r.RD_ID     = d.RD_ID
    LEFT JOIN USER_DWH.INDIVIDU          i  ON i.IND_ID    = d.IND_ID
    WHERE d.TDOS_CODE = 'V'
      AND pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA', 'RAS', 'RASC', 'RASO', 'RASA')
)
SELECT TPE_CODE,
       COUNT(*)                                                                AS nb_pensions,
       SUM(a_droit_propre)                                                     AS dans_dossier_avec_droit_propre,
       SUM(CASE WHEN date_notif > premiere_notif_dossier + 30
                THEN 1 ELSE 0 END)                                             AS notifiee_apres_1re_notif,
       SUM(CASE WHEN nb_dossiers_v > 1 THEN 1 ELSE 0 END)                      AS assure_a_plusieurs_dossiers_v,
       SUM(CASE WHEN RD_ID IS NOT NULL THEN 1 ELSE 0 END)                      AS dossier_avec_reception,
       SUM(CASE WHEN IND_DATE_DECES IS NOT NULL THEN 1 ELSE 0 END)             AS deces_assure_connu,
       SUM(CASE WHEN RD_DATE_RECEPTION >= IND_DATE_DECES THEN 1 ELSE 0 END)    AS reception_apres_deces,
       SUM(CASE WHEN date_notif >= DATE '2025-01-01'
                 AND date_notif <  DATE '2026-01-01' THEN 1 ELSE 0 END)        AS notifiees_2025,
       SUM(CASE WHEN date_notif >= DATE '2025-01-01'
                 AND date_notif <  DATE '2026-01-01'
                 AND date_notif > premiere_notif_dossier + 30
                THEN 1 ELSE 0 END)                                             AS notifiees_2025_hors_1re_notif
FROM survivant
GROUP BY TPE_CODE
ORDER BY nb_pensions DESC;

-- Résultat obtenu (P51, exécutée le 30/10/2026 ; colonnes recopiées dans le
-- lot, la colonne deces_assure_connu ne l'a pas été) :
--   Code   Pensions   Dossier avec    Notifiée après   Dossier avec   Réception     Notifiées   dont hors
--                     droit propre    la 1re notif.    réception      après décès   en 2025     1re notif.
--   RPSV   25 541     13 104          5 266            2 576          888           800         675
--   RPSE   29 689      9 977          3 448            6 190          3 005         797         420
--   RPSF      424        183            116              253            138          40          22
--   RPA        81          7              3               44             32           0           0
--   RASO    7 580         25              3            5 537          3 740         494           0
--   RASC    1 469         12              0            1 013            663          91           0
--   RASA      327          0              0              217            122          11           0
--   RAS       864          1              0                0              0           0           0
--   Assuré avec plusieurs dossiers de pension : RPSE 994, RPSV 698.
-- Lecture :
--   · 13 104 pensions de veuve sur 25 541 (51 %) sont dans le dossier du
--     défunt : « la moitié ». Les autres sont dans un dossier à part.
--   · En 2025, 675 des 800 pensions de veuve notifiées ne sont pas la première
--     notification de leur dossier. Compter un dossier une seule fois les
--     perdrait ; la règle les compte comme une demande à part.


-- -----------------------------------------------------------------------------
-- Requête 2. Les pensions de 2024 par type et selon la date retenue :
-- notification, date d'effet, saisie de la pension, premier paiement.
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.PE_DATE_NOTIFICATION,
           p.PE_DATE_EFFET,
           p.PE_DATE_INSERT
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
premier AS (
    SELECT d.PE_ID,
           MIN(d.DEB_DATE_EFFET)                                  AS premier_paiement
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID
)
SELECT CASE pe.TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(CASE WHEN pe.PE_DATE_NOTIFICATION >= DATE '2024-01-01'
                 AND pe.PE_DATE_NOTIFICATION <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS notifiees_2024,
       SUM(CASE WHEN pe.PE_DATE_EFFET >= DATE '2024-01-01'
                 AND pe.PE_DATE_EFFET <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS effet_2024,
       SUM(CASE WHEN pe.PE_DATE_INSERT >= DATE '2024-01-01'
                 AND pe.PE_DATE_INSERT <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS saisies_2024,
       SUM(CASE WHEN pr.premier_paiement >= DATE '2024-01-01'
                 AND pr.premier_paiement <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS premier_paiement_2024
FROM pe
LEFT JOIN premier pr ON pr.PE_ID = pe.PE_ID
GROUP BY CASE pe.TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY type_pension;

-- Résultat obtenu (A03, exécutée le 30/10/2026), face au tableau 23 de
-- l'annuaire CNSS 2024 (p. 19), qui compte des personnes :
--   Type          Notifiées   Date d'effet   Saisies   1er paiement   Publiés (tableau 23)
--   vieillesse    2 408       2 717          2 520     2 275          2 486
--   veuves          696         736            740       672            732
--   veufs            36          37             35        32             34
--   orphelins       734         727            779       621            769
--   invalidité        5           7              5         7              6
--   anticipée         3          10              4         8              3
--   ascendants        6           1              7         1              7
--   Total         3 888       4 235          4 090     3 616          4 037
-- Lecture : l'annuaire compte 1 542 nouveaux survivants en 2024 (732 veuves,
--   34 veufs, 769 orphelins et 7 ascendants), une personne par survivant. T25
--   compte une demande par dossier et par ligne : la veuve et ses orphelins
--   du même dossier font une seule demande. Sa ligne des survivants sera donc
--   plus basse que 1 542.
