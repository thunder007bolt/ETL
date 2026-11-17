-- =============================================================================
-- Point 123. La source des montants (T56)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « les paiements de l'application (DEBOURS), ou les
--   comptes de prestations de la comptabilité (601, 602, 603 et 605), lus par
--   la vue V_BI_BALANCE_COMPTABLE. » ; « en 2024, les paiements, soins
--   remboursés compris, sont à 0,2 % près du total publié des trois branches
--   (tableau 106), mais à −28,9 % en accidents du travail (voir le point
--   51). »
-- Requête : les montants servis de 2024 par branche et par type de débours.
-- Reprise de : lot8_evolution_5_ans.sql, P95 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes. La vue V_BI_BALANCE_COMPTABLE n'existe pas encore (la
--   comptabilité de la copie, STG_GL, n'est pas lue).
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc paie de T56
--   (paiements PM et soins RB). Fiche des règles : règle 72.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Débours au statut D ou P de 2024, par branche (CODE_BRANCHE) et par type :
-- paiements (PM), soins remboursés (RB), ajustements (AJ).
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
-- resultats/lot8_P95_montants_par_branche.tsv), paiements et soins de 2024,
-- face au tableau 106 de l'annuaire CNSS 2024 (p. 80) :
--   Branche        Base (PM + RB)    Publié            Écart
--   Pensions (V)   47 237 434 040    46 629 625 556     +1,30 %
--   PF (F + M)     10 750 873 342    11 180 191 425     −3,84 %
--   AT/MP (A)         763 397 429     1 074 113 823    −28,93 %
--   Total          58 751 704 811    58 883 930 804     −0,22 %
-- Lecture (calculé ici) : à 0,2 % près sur le total des trois branches, mais
--   à −28,9 % en accidents du travail.
