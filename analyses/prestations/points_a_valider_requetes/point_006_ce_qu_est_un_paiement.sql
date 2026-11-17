-- =============================================================================
-- Point 6. Ce qu'est un paiement
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « un débours de type PM (DEB_TYPE = PM) au statut D,
--   payé par effet, ou P, payé sans effet (DEB_STATUT). » ; « en 2024, 58,73 Md
--   payés dans les trois branches, pour 58,88 Md de prestations servies dans
--   l'annuaire CNSS 2024 (−0,26 %). Les pensions sont à 1,3 % près ;
--   l'annuaire publie plus pour les prestations familiales et les accidents du
--   travail. »
-- Requêtes :
--   1. Les débours de 2025 par statut et par type, et ce qui les accompagne
--      (effet, approbation, décaissement).
--   2. Les paiements de 2024 par branche et par mois, au mois payé : la somme
--      de l'année, face à l'annuaire.
-- Reprise de : lot2_statuts_stocks_nouveaux.sql, P20 (exécutée le
--   26/10/2026) ; lot9_indus.sql, P105 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc payees ;
--   t32_t41_requete_proposee.sql, bloc paie (DEB_TYPE = PM, DEB_STATUT D ou P,
--   montant DEB_MONTANT). Fiche des règles : règle 7.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Débours dont le mois payé (DEB_DATE_EFFET) tombe en 2025, par
-- statut (DEB_STATUT) et par type (DEB_TYPE) : nombre, montant, et la part
-- qui a un effet, une approbation, un décaissement, une écriture comptable.
-- -----------------------------------------------------------------------------
SELECT DEB_STATUT, DEB_TYPE,
       COUNT(*)                                                           AS nb_debours,
       SUM(DEB_MONTANT)                                                   AS montant,
       SUM(CASE WHEN EFP_ID IS NOT NULL THEN 1 ELSE 0 END)                AS avec_effet,
       SUM(CASE WHEN PE_ID IS NOT NULL THEN 1 ELSE 0 END)                 AS avec_prestation,
       SUM(CASE WHEN DEB_DATE_APPROBATION IS NOT NULL THEN 1 ELSE 0 END)  AS avec_approbation,
       SUM(CASE WHEN DEB_DATE_DECAISSEMENT IS NOT NULL THEN 1 ELSE 0 END) AS avec_decaissement,
       SUM(CASE WHEN DEB_DATE_ANNUL IS NOT NULL THEN 1 ELSE 0 END)        AS avec_annulation,
       SUM(CASE WHEN DATE_COMPTA IS NOT NULL THEN 1 ELSE 0 END)           AS avec_date_compta,
       ROUND(MEDIAN(DEB_DATE_INSERT - DEB_DATE_EFFET))                    AS ecart_median_saisie_j,
       SUM(CASE WHEN DEB_DATE_INSERT >= ADD_MONTHS(TRUNC(DEB_DATE_EFFET, 'MM'), 2)
                THEN 1 ELSE 0 END)                                        AS saisis_2_mois_apres
FROM USER_DWH.DEBOURS
WHERE DEB_DATE_EFFET >= DATE '2025-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY DEB_STATUT, DEB_TYPE
ORDER BY DEB_STATUT, DEB_TYPE;

-- Résultat obtenu (P20, exécutée le 26/10/2026), les paiements (PM) de 2025 :
--   Statut   Débours      Montant
--   D        1 843 527    53,72 Md
--   P        2 194 991     7,63 Md
--   V          125 258     0,64 Md
--   I           12 499     0,12 Md
--   N            4 558
--   A              544
--   S               56
--   D : effet 100 %, approbation 100 %, décaissement 66 %. P : aucun effet,
--   approbation 100 %. V : effet 94 %, aucun décaissement. N, A, S : presque
--   jamais approuvés.
-- Lecture : les vrais paiements sont D (payé par un effet décaissé) et P
--   (payé sans effet) ; V, I, N, A et S ne sont pas décaissés (sens à
--   confirmer par la DSI).


-- -----------------------------------------------------------------------------
-- Requête 2. T31 de 2024 : par mois et par branche, les prestations payées
-- (PM au statut D ou P) au mois payé et à la saisie, les trop-perçus, les
-- moins-perçus, les remboursements de trop-perçus.
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
-- par branche (calculé ici), face au tableau 106 de l'annuaire CNSS 2024
-- (p. 80, prestations servies) :
--   Branche   Base              Publié (tableau 106)   Écart
--   PVID      47 237 434 040    46 629 625 556 (Pension)   +1,30 %
--   PF        10 750 873 342    11 180 191 425 (PF)        −3,84 %
--   AT/MP        744 609 484     1 074 113 823 (RP)        −30,68 %
--   Total     58 732 916 866    58 883 930 804             −0,26 %
--   Le total publié est celui des trois branches, sans l'action sanitaire et
--   sociale (650 700 099). En AT/MP, l'annuaire compte aussi les soins
--   (139 668 695, tableau 82) : en argent seul, 934 445 128 (tableaux 90, 96
--   et 97).
-- Lecture : 58,73 Md payés pour 58,88 Md publiés (−0,26 %). Les pensions
--   sont à 1,3 % près ; les prestations familiales et les accidents du
--   travail sont sous le publié, et le total ne tombe juste que parce que ces
--   écarts se compensent (point 51).
