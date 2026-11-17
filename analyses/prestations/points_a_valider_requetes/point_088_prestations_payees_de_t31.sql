-- =============================================================================
-- Point 88. Les prestations payées de T31
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « T31 prend les débours PM au statut D ou P
--   (DEBOURS), sans les soins des accidents du travail (DEB_TYPE = RB) ni les
--   rachats de rente (AJUSTEMENT, TAJ_CODE = RR). Les frais médicaux de
--   maternité ne sont pas dans la base. » ; « en 2024, 58,73 Md dans la base
--   pour 58,88 Md dans l'annuaire CNSS 2024 (tableau 106), mais les branches
--   se compensent : en accidents du travail, 744,6 M pour 934,4 M publiés en
--   argent (tableaux 90, 96 et 97). »
-- Requête : T31 de 2024, par mois et par branche : payées, trop-perçus,
--   moins-perçus.
-- Reprise de : lot9_indus.sql, P105 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc payees (DEB_TYPE =
--   PM, DEB_STATUT D ou P). Le réglage payees_types est proposé dans
--   regles_t28_t31_controles_indus.md, pas encore dans la requête. Fiche des
--   règles : règle 46.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- T31 de 2024 : par mois et par branche (CODE_BRANCHE : V = PVID, A = AT/MP,
-- les autres = PF), les payées (débours PM au statut D ou P) au mois payé et
-- à la saisie, les trop-perçus, les moins-perçus, les remboursements.
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
-- resultats/lot9_P105_t31_2024.tsv), payées de 2024 au mois payé, sommées
-- par branche, face à l'annuaire CNSS 2024 :
--   Branche   Base              Publié
--   PVID      47 237 434 040    46 629 625 556   tableau 106 (p. 80), Pension
--   PF        10 750 873 342    11 180 191 425   tableau 106, PF
--   AT/MP        744 609 484       934 445 128   en argent : tableau 90 (p. 71),
--                                                 tableaux 96 et 97 (p. 74)
--                                 1 074 113 823   avec les soins : tableau 106, RP
--   Total     58 732 916 866    58 883 930 804   tableau 106, trois branches
--                                                 sans l'action sanitaire
-- Lecture : 58,73 Md pour 58,88 Md au total ; mais l'AT/MP n'a que 744,6 M
--   pour 934,4 M publiés en argent, et la PVID dépasse le publié de 1,3 % :
--   les branches se compensent.
