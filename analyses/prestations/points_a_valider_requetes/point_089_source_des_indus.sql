-- =============================================================================
-- Point 89. La source des indus (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « AJUSTEMENT, TAJ_CODE = TP ou TP-CONV, par date
--   d'établissement (AJ_DATE_ETABLISSEMENT). » ; « en 2024, 684 trop-perçus
--   dans la base, pour 229,5 M : 0,39 % des prestations payées. »
-- Requêtes :
--   1. Les trop-perçus établis en 2024 : nombre, montant, lien avec une
--      prestation.
--   2. T31 de 2024 : payées, trop-perçus et taux d'indus.
-- Reprise de : lot9_indus.sql, P106 et P105 (exécutées le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (les
--   trop-perçus de l'application). Le réglage source_indus est proposé dans
--   regles_t28_t31_controles_indus.md, pas encore dans la requête. Fiche des
--   règles : règle 47.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Trop-perçus (TP et TP-CONV) établis en 2024 (AJ_DATE_ETABLISSEMENT).
-- -----------------------------------------------------------------------------
WITH code_lie AS (
    SELECT pa.AJ_ID,
           MIN(p.TPE_CODE)                                        AS code
    FROM USER_DWH.PE_AJ pa
    JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID = pa.PE_ID
    GROUP BY pa.AJ_ID
)
SELECT NVL(c.code, '(sans prestation)')                           AS code_prestation,
       COUNT(*)                                                   AS trop_percus,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       ROUND(MEDIAN(aj.AJ_MONTANT))                               AS mediane,
       SUM(CASE WHEN i.IND_DATE_DECES < aj.AJ_DATE_ETABLISSEMENT
                THEN 1 ELSE 0 END)                                AS apres_deces,
       ROUND(SUM(CASE WHEN i.IND_DATE_DECES < aj.AJ_DATE_ETABLISSEMENT
                      THEN aj.AJ_MONTANT END))                    AS montant_apres_deces
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN code_lie c           ON c.AJ_ID  = aj.AJ_ID
LEFT JOIN USER_DWH.INDIVIDU i  ON i.IND_ID = aj.IND_ID
WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV')
  AND aj.AJ_DATE_ETABLISSEMENT >= DATE '2024-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2025-01-01'
GROUP BY NVL(c.code, '(sans prestation)')
ORDER BY montant DESC;

-- Résultat obtenu (P106, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P106_trop_percus_2024.tsv) : 684 trop-perçus en 2024, pour
-- 229 484 759 ; aucun n'a de prestation liée (PE_AJ) ; médiane 110 000.


-- -----------------------------------------------------------------------------
-- Requête 2. T31 de 2024 : par mois et par branche, payées et trop-perçus.
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

-- Résultat obtenu (P105, exécutée le 01/11/2026), l'année 2024 : payées
-- 58 732 916 866 au mois payé ; trop-perçus 229 484 759 ; taux d'indus
-- 0,39 %.
-- Lecture : la base a tous les trop-perçus enregistrés par les services ; le
--   fichier de la DAI, quand il sera fourni, servira à comparer.
