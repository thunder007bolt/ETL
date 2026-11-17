-- =============================================================================
-- Point 7. Les branches
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « la branche du dossier (DOSSIER, TDOS_CODE) : V
--   pensions, A risques professionnels, F prestations familiales, M
--   maternité, qui va avec les prestations familiales. » ; « l'annuaire CNSS
--   2024 fait pareil : ses 11,18 Md de prestations familiales de 2024
--   comprennent la maternité. »
-- Requête : les montants servis de 2024 par branche et par type de débours.
-- Reprise de : lot8_evolution_5_ans.sql, P95 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes.
-- Règle appliquée dans : t31_requete_proposee.sql, blocs payees et
--   ajustements (V = PVID, A = AT/MP, F et M = PF) ; t50_t57_requete_proposee.sql,
--   bloc branches (T56). Fiche des règles : règle 5.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Débours au statut D ou P dont le mois payé tombe en 2024, par branche
-- (CODE_BRANCHE : V, F, M, A) et par type (PM paiements, RB soins, AJ
-- ajustements).
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
-- resultats/lot8_P95_montants_par_branche.tsv), paiements (PM) de 2024 :
--   V 47 237 434 040 ; F 7 954 774 500 ; M 2 796 098 842 ; A 744 609 484
--   (et 18 787 945 de soins remboursés). Aucune autre branche.
-- Face au tableau 106 de l'annuaire CNSS 2024 (p. 80) : PF 11 180 191 425,
-- dont les IJ de maternité, 2 865 630 056 (tableau 40, p. 32).
-- Lecture (calculé ici) : F + M = 10 750 873 342 pour 11 180 191 425 publiés
--   en PF : la maternité (M) va avec les prestations familiales, comme dans
--   l'annuaire.
