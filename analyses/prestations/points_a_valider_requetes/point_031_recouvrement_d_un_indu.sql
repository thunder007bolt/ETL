-- =============================================================================
-- Point 31. Le recouvrement d'un indu (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « dans AJUSTEMENT, depuis 2017, aucun trop-perçu
--   n'est soldé (AJ_STATUT = C), tous restent en cours (voir le point 61). Les
--   remboursements ont leur code, TAJ_CODE = REM-TP. » ; « en 2024, 13,0 M de
--   remboursements pour 229,5 M de trop-perçus. Les déduire ferait passer le
--   taux d'indus de 0,39 % à 0,37 %. »
-- Requêtes :
--   1. Les ajustements par type et par statut, toutes années.
--   2. T31 de 2024 : payées, trop-perçus et remboursements de trop-perçus.
-- Reprise de : lot9_indus.sql, P103 et P105 (exécutées le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (REM-TP
--   reste à part et ne diminue pas les trop-perçus). Le réglage rem_tp_deduit
--   est proposé dans regles_t28_t31_controles_indus.md, pas encore dans la
--   requête. Fiche des règles : règle 44.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Ajustements (AJUSTEMENT) par type (TAJ_CODE) et par statut
-- (AJ_STATUT), toutes années : nombre, montant, avec un débours, montant des
-- débours payés, avec un précompte (retenue sur les paiements suivants).
-- -----------------------------------------------------------------------------
WITH lies AS (
    SELECT deb.AJ_ID,
           COUNT(*)                                               AS debours,
           SUM(CASE WHEN deb.DEB_STATUT IN ('D', 'P')
                    THEN deb.DEB_MONTANT END)                     AS montant_paye
    FROM USER_DWH.DEBOURS deb
    WHERE deb.AJ_ID IS NOT NULL
    GROUP BY deb.AJ_ID
)
SELECT aj.TAJ_CODE                                                AS type_ajustement,
       aj.AJ_STATUT                                               AS statut,
       COUNT(*)                                                   AS ajustements,
       SUM(CASE WHEN aj.AJ_DATE_ETABLISSEMENT IS NULL
                THEN 1 ELSE 0 END)                                AS sans_date,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       SUM(CASE WHEN l.AJ_ID IS NOT NULL THEN 1 ELSE 0 END)       AS avec_debours,
       ROUND(SUM(l.montant_paye))                                 AS montant_debours_payes,
       SUM(CASE WHEN aj.AJ_NB_PRECOMPTE > 0 THEN 1 ELSE 0 END)    AS avec_precompte,
       COUNT(aj.AJ_DECAISSE)                                      AS decaisse_rempli,
       MIN(TRUNC(aj.AJ_DATE_ETABLISSEMENT))                       AS premiere_date,
       MAX(TRUNC(aj.AJ_DATE_ETABLISSEMENT))                       AS derniere_date
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN lies l ON l.AJ_ID = aj.AJ_ID
WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC')
GROUP BY aj.TAJ_CODE, aj.AJ_STATUT
ORDER BY aj.TAJ_CODE, aj.AJ_STATUT;

-- Résultat obtenu (P103, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P103_statuts.tsv) :
--   Type     Statut   Ajustements   Montant          Avec débours   Débours payés      Avec précompte
--   TP       A        6 879         1 470 074 632    6 870            233 455 778      6 877
--   TP       C        5 601           426 683 568    5 601           −426 030 808      5 601
--   TP-CONV  A          936           130 315 259      936             52 468 716        936
--   REM-TP   A           15             3 189 990       10             −1 624 928
--   REM-TP   C          227            67 216 092      227            −59 519 758
--   (MP, RC et les autres statuts : voir le TSV.) TP au statut C : de 2001 à
--   2017 seulement.
-- Lecture : C veut dire soldé (un trop-perçu C est repris en entier). Depuis
--   2017, aucun trop-perçu n'est soldé : ils restent au statut A, repris par
--   précomptes (6 877 sur 6 879).


-- -----------------------------------------------------------------------------
-- Requête 2. T31 de 2024 : par mois et par branche, payées, trop-perçus,
-- moins-perçus et remboursements de trop-perçus (REM-TP), à part.
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
-- resultats/lot9_P105_t31_2024.tsv), l'année 2024, sommes calculées ici :
--   Payées           Trop-perçus    Remboursements (REM-TP)   Taux    Taux avec les remboursements déduits
--   58 732 916 866   229 484 759    13 024 760                0,39 %  0,37 %
-- Lecture : 13,0 M de remboursements pour 229,5 M de trop-perçus ;
--   (229 484 759 − 13 024 760) ÷ 58 732 916 866 = 0,37 %, au lieu de 0,39 %.
