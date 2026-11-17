-- =============================================================================
-- Point 78. Le rachat de rente (T26)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « ce n'est pas une prestation (PRESTATION_ESP), mais
--   un débours d'ajustement (DEBOURS, DEB_TYPE = AJ) lié à un ajustement de
--   code RR (AJUSTEMENT, TAJ_CODE = RR). » ; « 11 rachats versés en 2024 dans
--   la base, pour 21,5 M ; l'annuaire CNSS 2024 en publie 25,4 M (tableau
--   90), comme une prestation à part. »
-- Requête : les débours d'ajustement payés pour un mois de 2024, par branche
--   et par type d'ajustement.
-- Reprise de : lot9_indus.sql, P104 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t26_requete_proposee.sql ne lit que les prestations
--   (PRESTATION_ESP) et les soins (débours avec un TPN_CODE), bloc demande :
--   un rachat n'y fait pas de demande. Pour T56, fiche des règles : règle 72.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Débours d'ajustement (DEB_TYPE = AJ) au statut D ou P dont le mois payé
-- tombe en 2024, par branche (CODE_BRANCHE) et par type d'ajustement
-- (AJUSTEMENT, TAJ_CODE) : nombre, montant, plus petit et plus grand.
-- -----------------------------------------------------------------------------
SELECT NVL(deb.CODE_BRANCHE, '?')                                 AS branche,
       NVL(aj.TAJ_CODE, '(sans ajustement)')                      AS type_ajustement,
       COUNT(*)                                                   AS debours,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant,
       MIN(deb.DEB_MONTANT)                                       AS plus_petit,
       MAX(deb.DEB_MONTANT)                                       AS plus_grand
FROM USER_DWH.DEBOURS deb
LEFT JOIN USER_DWH.AJUSTEMENT aj ON aj.AJ_ID = deb.AJ_ID
WHERE deb.DEB_TYPE = 'AJ'
  AND deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
  AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
GROUP BY NVL(deb.CODE_BRANCHE, '?'), NVL(aj.TAJ_CODE, '(sans ajustement)')
ORDER BY branche, montant DESC;

-- Résultat obtenu (P104, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P104_debours_aj_2024.tsv), la ligne des rachats :
--   Branche   Type   Débours   Montant      Plus petit   Plus grand
--   A         RR     11        21 519 361   480 867      6 780 655
-- Face à l'annuaire CNSS 2024 : 25 366 909 de rachats de rentes en 2024
-- (tableau 90, p. 71, ligne Total ; aussi tableaux 96 et 97, p. 74).
-- Lecture : 11 rachats pour 21,5 M dans la base, 25,4 M publiés. L'annuaire
--   les publie comme une prestation à part ; la base n'en fait pas une
--   prestation, mais un ajustement.
