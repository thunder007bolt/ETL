-- =============================================================================
-- Point 17. Les indus (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « dans AJUSTEMENT, trop-perçus TP ou TP-CONV
--   (TAJ_CODE), moins-perçus MP ou RC, montant AJ_MONTANT, branche du dossier
--   (TDOS_CODE), la maternité allant avec les prestations familiales. Les
--   autres codes et le statut R (AJ_STATUT) sont écartés. » ; « en 2024, 684
--   trop-perçus pour 229,5 M, et 414,4 M de moins-perçus. »
-- Requêtes :
--   1. Les ajustements par année, branche et type, établis de 2023 à 2025.
--   2. T31 de 2024, mois par mois et par branche.
-- Reprise de : lot9_indus.sql, P102 et P105 (exécutées le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (TP et
--   TP-CONV contre MP et RC, AJ_STATUT <> R, mois de AJ_DATE_ETABLISSEMENT,
--   branche par DOSSIER.TDOS_CODE). Fiche des règles : règles 43 et 45.
-- À noter : P105 n'écarte pas le statut R. C'est sans effet : de 2023 à 2025,
--   les ajustements n'ont que les statuts A et C (P102).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Ajustements (AJUSTEMENT) établis de 2023 à 2025
-- (AJ_DATE_ETABLISSEMENT), par branche du dossier et par type (TAJ_CODE) :
-- nombre, montant, et répartition par statut (AJ_STATUT).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT)                AS annee,
       CASE d.TDOS_CODE
           WHEN 'V' THEN 'PVID'
           WHEN 'A' THEN 'AT/MP'
           WHEN 'F' THEN 'PF'
           WHEN 'M' THEN 'PF maternité'
           ELSE          '(sans dossier)'
       END                                                        AS branche,
       CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC', 'AV')
            THEN aj.TAJ_CODE ELSE 'AUTRES' END                    AS type_ajustement,
       COUNT(*)                                                   AS ajustements,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       SUM(CASE WHEN aj.AJ_STATUT = 'A' THEN 1 ELSE 0 END)        AS statut_a,
       SUM(CASE WHEN aj.AJ_STATUT = 'C' THEN 1 ELSE 0 END)        AS statut_c,
       SUM(CASE WHEN aj.AJ_STATUT = 'I' THEN 1 ELSE 0 END)        AS statut_i,
       SUM(CASE WHEN aj.AJ_STATUT = 'R' THEN 1 ELSE 0 END)        AS statut_r
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = aj.DOS_CODE
WHERE aj.AJ_DATE_ETABLISSEMENT >= DATE '2023-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT),
         CASE d.TDOS_CODE
             WHEN 'V' THEN 'PVID'
             WHEN 'A' THEN 'AT/MP'
             WHEN 'F' THEN 'PF'
             WHEN 'M' THEN 'PF maternité'
             ELSE          '(sans dossier)'
         END,
         CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC', 'AV')
              THEN aj.TAJ_CODE ELSE 'AUTRES' END
ORDER BY annee, branche, type_ajustement;

-- Résultat obtenu (P102, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P102_ajustements_par_type.tsv), l'année 2024, nombre /
-- montant :
--   PVID      TP 581 / 209 020 712 ; TP-CONV 15 / 12 135 453 ; MP 204 /
--             134 626 996 ; RC 1 239 / 136 244 163
--   PF        TP 47 / 2 638 651 ; MP 17 126 / 132 014 360
--   maternité TP 15 / 2 158 495 ; MP 61 / 8 130 978
--   AT/MP     TP 26 / 3 531 448 ; MP 22 / 3 426 212
--   Trop-perçus (TP et TP-CONV), toutes branches : 684 pour 229 484 759.
-- Lecture (calculé ici) : 684 trop-perçus pour 229,5 M ; moins-perçus (MP et
--   RC) : 134 626 996 + 136 244 163 + 132 014 360 + 8 130 978 + 3 426 212 =
--   414 442 709, soit 414,4 M. Les trop-perçus sont tous au statut A.


-- -----------------------------------------------------------------------------
-- Requête 2. T31 de 2024 : par mois et par branche, les payées, les
-- trop-perçus et les moins-perçus établis dans le mois.
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
--   Branche   Payées au mois payé   Trop-perçus    Moins-perçus   Taux d'indus
--   PVID      47 237 434 040        221 156 165    270 871 159    0,47 %
--   AT/MP        744 609 484          3 531 448      3 426 212    0,47 %
--   PF        10 750 873 342          4 797 146    140 145 338    0,045 %
--   Total     58 732 916 866        229 484 759    414 442 709    0,39 %
-- Lecture : chaque trop-perçu compte une fois, le mois où il est établi ; les
--   moins-perçus sont montrés à part, hors du taux.
