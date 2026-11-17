-- =============================================================================
-- Point 135. La réception au statut A (T25)
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « RECEPTION_DOSSIER, RD_STATUT = A, pour une réception
--   de pension reliée à aucun dossier. » ; « 111 réceptions de pension au
--   statut A sans dossier depuis mai 2025. »
-- Requête : le statut des réceptions de pension face au dossier et au rejet.
-- Reprise de : lot3b_controles_t25.sql, P55 (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t25_requete_proposee.sql, bloc instance_fin (une
--   réception sans dossier ni rejet reste en instance, quel que soit son
--   statut). Fiche des règles : règle 23.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- RD_STATUT des réceptions de pension (TDOS_CODE = V) : réceptions, en 2025,
-- avec rejet, ni dossier ni rejet, dossier notifié, dossier fermé sans
-- notification, dossier actif sans notification, première et dernière date.
-- -----------------------------------------------------------------------------
WITH dos AS (
    SELECT d.RD_ID,
           MAX(CASE WHEN pe.PE_DATE_NOTIFICATION IS NOT NULL THEN 1 ELSE 0 END)  AS notifie,
           MAX(CASE WHEN d.DOS_STATUT = 'F' THEN 1 ELSE 0 END)                   AS ferme
    FROM USER_DWH.DOSSIER d
    LEFT JOIN USER_DWH.PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
    WHERE d.RD_ID IS NOT NULL
    GROUP BY d.RD_ID
),
rej AS (
    SELECT DISTINCT RD_ID
    FROM USER_DWH.DOSSIER_REJETE
    WHERE RD_ID IS NOT NULL
)
SELECT NVL(TO_CHAR(r.RD_STATUT), '(vide)')                                     AS rd_statut,
       COUNT(*)                                                                AS receptions_v,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS receptions_2025,
       SUM(CASE WHEN x.RD_ID IS NOT NULL THEN 1 ELSE 0 END)                    AS avec_rejet,
       SUM(CASE WHEN o.RD_ID IS NULL AND x.RD_ID IS NULL THEN 1 ELSE 0 END)    AS ni_dossier_ni_rejet,
       SUM(CASE WHEN o.notifie = 1 THEN 1 ELSE 0 END)                          AS dossier_notifie,
       SUM(CASE WHEN o.notifie = 0 AND o.ferme = 1 THEN 1 ELSE 0 END)          AS dossier_ferme_sans_notif,
       SUM(CASE WHEN o.notifie = 0 AND o.ferme = 0 THEN 1 ELSE 0 END)          AS dossier_actif_sans_notif,
       MIN(r.RD_DATE_RECEPTION)                                                AS reception_min,
       MAX(r.RD_DATE_RECEPTION)                                                AS reception_max
FROM USER_DWH.RECEPTION_DOSSIER r
LEFT JOIN dos o ON o.RD_ID = r.RD_ID
LEFT JOIN rej x ON x.RD_ID = r.RD_ID
WHERE r.TDOS_CODE = 'V'
GROUP BY NVL(TO_CHAR(r.RD_STATUT), '(vide)')
ORDER BY receptions_v DESC;

-- Résultat obtenu (P55, exécutée le 30/10/2026) : statut A : 111 réceptions,
-- 70 en 2025, 2 avec un rejet, 109 sans dossier ni rejet, aucune avec un
-- dossier ; depuis le 26/05/2025.
-- Lecture : le statut A est récent et toujours sans dossier : « en attente »
--   ou « annulée » ? Annulée, la réception sortirait du stock en instance.
