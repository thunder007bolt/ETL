-- =============================================================================
-- Point 91. Les moins-perçus dans le taux d'indus (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « AJUSTEMENT, TAJ_CODE = MP ou RC. » ; « en 2024,
--   0,39 % sans eux, 1,10 % avec eux : sous la norme de 2 % dans les deux
--   cas. »
-- Requête : T31 de 2024, par mois et par branche : payées, trop-perçus,
--   moins-perçus.
-- Reprise de : lot9_indus.sql, P105 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements et
--   colonne TAUX_INDUS_PCT (les trop-perçus seulement). Fiche des règles :
--   règle 43.
-- À noter, deux défauts de rédaction du point, à corriger dans la fiche :
--   · « l'Annuaire CIPRES parle d'indus « en trop ou en moins » » : la
--     citation n'est pas exacte. L'Annuaire CIPRES (p. 13, description de
--     T31) écrit « prestations [...] indues (trop perçu ou moins perçu par
--     rapport à la prestation due) ». Le sens est le même.
--   · « le taux des deux ensemble est affiché à côté » : t31_requete_proposee.sql
--     ne sort qu'un taux, TAUX_INDUS_PCT, sur les trop-perçus. Le taux avec les
--     moins-perçus n'est calculé qu'ici, à la main.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- T31 de 2024 : par mois et par branche, payées (débours PM au statut D ou
-- P), trop-perçus (TP et TP-CONV) et moins-perçus (MP et RC) établis dans le
-- mois.
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

-- Résultat obtenu (P105, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P105_t31_2024.tsv), l'année 2024 :
--   Payées           Trop-perçus    Moins-perçus   Taux sans eux   Taux avec eux
--   58 732 916 866   229 484 759    414 442 709    0,39 %          1,10 %
-- Lecture (calculé ici) : (229 484 759 + 414 442 709) ÷ 58 732 916 866 =
--   1,10 %. Sous la norme de 2 % dans les deux cas.
