-- =============================================================================
-- Point 90. Le dénominateur du taux d'indus (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « le montant des dossiers contrôlés n'est nulle part :
--   il faudrait l'ajouter à la liste des contrôles de la DAI (voir le point
--   70). » ; « en 2024, 0,39 % des prestations payées. »
-- Requêtes :
--   1. T31 de 2024 : payées, trop-perçus et taux d'indus.
--   2. Les colonnes de la table des contrôles : aucune ne porte un montant.
-- Reprise de : lot9_indus.sql, P105 (exécutée le 01/11/2026) ; la requête 2
--   est nouvelle.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 1 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, colonne TAUX_INDUS_PCT
--   (trop-perçus ÷ payées). Les réglages taux_numerateur et taux_denominateur
--   sont proposés dans regles_t28_t31_controles_indus.md, pas encore dans la
--   requête. Fiche des règles : règle 46.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. T31 de 2024 : par mois et par branche, payées (débours PM au
-- statut D ou P), trop-perçus et taux d'indus.
-- -----------------------------------------------------------------------------
WITH mois AS (
    SELECT LEVEL AS mois FROM DUAL CONNECT BY LEVEL <= 12
),
branches AS (
    SELECT 'PVID' AS branche FROM DUAL
    UNION ALL SELECT 'AT/MP' FROM DUAL
    UNION ALL SELECT 'PF'    FROM DUAL
),
payees_mois AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM deb.DEB_DATE_EFFET)                 AS mois,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
    GROUP BY CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM deb.DEB_DATE_EFFET)
),
payees_saisie AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM deb.DEB_DATE_INSERT)                AS mois,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_INSERT >= DATE '2024-01-01'
      AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
    GROUP BY CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM deb.DEB_DATE_INSERT)
),
ajustements AS (
    SELECT CASE d.TDOS_CODE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM aj.AJ_DATE_ETABLISSEMENT)           AS mois,
           SUM(CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV')
                    THEN aj.AJ_MONTANT ELSE 0 END)                AS trop_percus,
           SUM(CASE WHEN aj.TAJ_CODE IN ('MP', 'RC')
                    THEN aj.AJ_MONTANT ELSE 0 END)                AS moins_percus,
           SUM(CASE WHEN aj.TAJ_CODE = 'REM-TP'
                    THEN aj.AJ_MONTANT ELSE 0 END)                AS remb_trop_percus
    FROM USER_DWH.AJUSTEMENT aj
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = aj.DOS_CODE
    WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV', 'MP', 'RC', 'REM-TP')
      AND d.TDOS_CODE IN ('V', 'A', 'F', 'M')
      AND aj.AJ_DATE_ETABLISSEMENT >= DATE '2024-01-01'
      AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2025-01-01'
    GROUP BY CASE d.TDOS_CODE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM aj.AJ_DATE_ETABLISSEMENT)
)
SELECT m.mois,
       b.branche,
       ROUND(NVL(pm.montant, 0))                                  AS payees_mois_paye,
       ROUND(NVL(ps.montant, 0))                                  AS payees_saisie,
       ROUND(NVL(a.trop_percus, 0))                               AS trop_percus,
       ROUND(NVL(a.moins_percus, 0))                              AS moins_percus,
       ROUND(NVL(a.remb_trop_percus, 0))                          AS remb_trop_percus,
       ROUND(100 * NVL(a.trop_percus, 0) / NULLIF(pm.montant, 0), 3) AS taux_indus_pct
FROM mois m
CROSS JOIN branches b
LEFT JOIN payees_mois   pm ON pm.branche = b.branche AND pm.mois = m.mois
LEFT JOIN payees_saisie ps ON ps.branche = b.branche AND ps.mois = m.mois
LEFT JOIN ajustements   a  ON a.branche  = b.branche AND a.mois  = m.mois
ORDER BY b.branche, m.mois;

-- Résultat obtenu (P105, exécutée le 01/11/2026), l'année 2024 :
--   PVID 0,47 % ; AT/MP 0,47 % ; PF 0,045 % ; toutes branches 0,39 %
--   (229 484 759 de trop-perçus pour 58 732 916 866 payés).


-- -----------------------------------------------------------------------------
-- Requête 2. Les colonnes de DOSSIERS_CONTROLES (catalogue ALL_TAB_COLUMNS).
-- -----------------------------------------------------------------------------
SELECT COLUMN_ID,
       COLUMN_NAME,
       DATA_TYPE
FROM ALL_TAB_COLUMNS
WHERE OWNER      = 'USER_DWH'
  AND TABLE_NAME = 'DOSSIERS_CONTROLES'
ORDER BY COLUMN_ID;

-- Résultat : pas encore exécutée. Attendu, d'après le chargement des faits
-- (domaines/prestation_recouvrement/facts/sql/controle_prestation.sql) :
-- DC_ID, TDOS_CODE, DOS_CODE, TPE_CODE, DC_DATE_CONTROLE, AUD_ID, MCI_ID,
-- DC_REMARQUE, DC_DATE_INSERT, DC_USAGER_INSERT, DC_DATE_UPDATE,
-- DC_USAGER_UPDATE, DC_RECOMMANDE : aucun montant.
-- Lecture : le taux sur le seul montant des dossiers contrôlés ne peut pas se
--   calculer avec la base : il faudrait ce montant dans la liste de la DAI.
