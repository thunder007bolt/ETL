-- =============================================================================
-- Point 87. Les frais médicaux de maternité (T30)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T28 à
--   T30.
-- Ce que dit le point : « aucun soin remboursé (DEBOURS, DEB_TYPE = RB) en
--   2024 sur les dossiers de maternité (TDOS_CODE = M). » ; « l'annuaire CNSS
--   2024 publie 25,0 M de frais médicaux de maternité payés en 2024 (tableaux
--   40 et 41). »
-- Requête : les montants servis de 2024 par branche et par type de débours.
-- Reprise de : lot8_evolution_5_ans.sql, P95 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : aucune requête proposée, T28 à T30 ne sont pas
--   produits ; dans T27, t27_requete_proposee.sql garde la ligne « Frais
--   médicaux de maternité » vide. Fiche des règles : règles 36 et 37.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Débours au statut D ou P dont le mois payé tombe en 2024, par branche
-- (CODE_BRANCHE) et par type (DEB_TYPE) : paiements (PM), remboursements de
-- soins (RB), ajustements (AJ).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                      AS annee,
       NVL(deb.CODE_BRANCHE, '?')                                 AS branche,
       deb.DEB_TYPE                                               AS type_debours,
       COUNT(*)                                                   AS debours,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant,
       SUM(CASE WHEN deb.PE_ID IS NULL THEN 1 ELSE 0 END)         AS sans_prestation,
       ROUND(SUM(CASE WHEN deb.PE_ID IS NULL
                      THEN deb.DEB_MONTANT END))                  AS montant_sans_prestation
FROM USER_DWH.DEBOURS deb
WHERE deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
  AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM deb.DEB_DATE_EFFET), NVL(deb.CODE_BRANCHE, '?'), deb.DEB_TYPE
ORDER BY annee, branche, type_debours;

-- Résultat obtenu (P95, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P95_montants_par_branche.tsv) :
--   Branche   Paiements (PM)    Remboursements (RB)   Ajustements (AJ)
--   V         47 237 434 040    -                     587 162 398
--   F          7 954 774 500    -                     201 000 381
--   M          2 796 098 842    -                       8 766 415
--   A            744 609 484    18 787 945             25 799 280
-- Face à l'annuaire CNSS 2024 : 25 030 869 de frais médicaux de maternité en
-- 2024 (tableau 40, p. 32, et tableau 41, p. 33).
-- Lecture : aucun remboursement de soins (RB) sur la maternité : les 25,0 M
--   publiés ne sont pas dans la base des prestations (voir le point 129).
