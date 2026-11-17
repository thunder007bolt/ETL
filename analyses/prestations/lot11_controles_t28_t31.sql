-- =============================================================================
-- LOT 11 — Contrôles pour T28 à T31 (contrôles a posteriori, indus)
-- Ouvert le 12/11/2026, pendant la relecture des points à valider (points 79,
-- 90 et 92). Requêtes données en chat, puis historisées ici.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 9.
-- Exécution : en mode script (F5) dans SQL Developer ; description de table,
-- comptages et sommes seulement.
-- Données nominatives : jamais lues. Les textes libres de DOSSIERS_CONTROLES
-- (DC_REMARQUE, DC_RECOMMANDE) et d'AJUSTEMENT (AJ_REMARQUE) ne sont pas lus.
-- Numérotation : P120 et suivantes, réservées à T28-T31 le 08/11/2026 (P107 à
-- P114 restent au lot 10 de T42-T49, P115 à P119 à T50-T57).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P120 — La structure de la table des contrôles
-- Question : la table des contrôles a posteriori (DOSSIERS_CONTROLES) a-t-elle
--            une référence vers une prestation précise (PE_ID), ou seulement
--            le dossier et le type de prestation ?
-- Objectif : les colonnes, leurs types et tailles, et si elles peuvent être
--            vides. Seule la description de la table est lue.
-- Tableaux : T28 à T30 ; point 79 (ce que vise un contrôle), point 90 (montant
--            des dossiers contrôlés).
-- Ce qu'on sait déjà : le chargement des faits
--            (domaines/prestation_recouvrement/facts/sql/controle_prestation.sql)
--            copie 13 colonnes : DC_ID, TDOS_CODE, DOS_CODE, TPE_CODE,
--            DC_DATE_CONTROLE, AUD_ID, MCI_ID, DC_REMARQUE, DC_DATE_INSERT,
--            DC_USAGER_INSERT, DC_DATE_UPDATE, DC_USAGER_UPDATE, DC_RECOMMANDE.
--            Pas de PE_ID : un contrôle ne se relie qu'au dossier et au type de
--            prestation, pas à la prestation d'une personne précise.
-- -----------------------------------------------------------------------------
SELECT COLUMN_ID, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'USER_DWH' AND TABLE_NAME = 'DOSSIERS_CONTROLES'
ORDER BY COLUMN_ID;
-- Résultat : pas encore exécutée.


-- -----------------------------------------------------------------------------
-- P121 — Les trop-perçus sur convention et la catégorie du dossier
-- Question : les trop-perçus sur convention (TP-CONV) sont-ils tous sur des
--            dossiers de coordination ? Le bond de 2025 (758 contre 15 en
--            2024, lot 9 P102) vient-il de beaucoup de dossiers, ou de
--            plusieurs lignes par dossier ?
-- Objectif : par année d'établissement et par catégorie du dossier
--            (DOSSIER.DOS_CATEGORIE : COOR_CNSS, COOR_AC, CNSS_AC, ORDINAIRE,
--            vide), le nombre de TP-CONV, de dossiers distincts et le montant.
-- Tableaux : T31 ; point 92.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT)          AS annee,
       NVL(d.DOS_CATEGORIE, '(sans catégorie)')             AS categorie,
       COUNT(*)                                              AS nb_tp_conv,
       COUNT(DISTINCT aj.DOS_CODE)                           AS nb_dossiers,
       ROUND(SUM(aj.AJ_MONTANT))                             AS montant
FROM USER_DWH.AJUSTEMENT aj
JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = aj.DOS_CODE
WHERE aj.TAJ_CODE = 'TP-CONV'
  AND aj.AJ_DATE_ETABLISSEMENT >= DATE '2023-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT), d.DOS_CATEGORIE
ORDER BY 1, 2;
-- Résultat (exécutée le 12/11/2026) :
--   Année  Catégorie         TP-CONV  Dossiers  Montant
--   2023   COOR_CNSS              34        28  14 720 654
--   2023   ORDINAIRE               1         1         364
--   2023   (sans catégorie)        2         2      14 328
--   2024   COOR_CNSS              15        14  12 135 453
--   2025   COOR_AC                 1         1      18 927
--   2025   COOR_CNSS             757       387  54 752 385
-- Constat  : · Les TP-CONV sont bien sur des dossiers de coordination payés
--              par la CNSS (COOR_CNSS) : 806 sur 810 de 2023 à 2025. Les 4
--              autres sont des écarts de saisie minimes (14 692 FCFA en 2023,
--              18 927 en 2025).
--            · Le bond de 2025 est réel en dossiers : 387 dossiers touchés
--              contre 14 en 2024 (×28), avec environ 2 TP-CONV par dossier.
--              Ce n'est pas une ligne par mois sur quelques dossiers.
--            · Le montant moyen par dossier baisse (141 000 FCFA en 2025
--              contre 867 000 en 2024) : beaucoup de petites régularisations,
--              ce qui fait penser à une révision d'ensemble des dossiers de
--              coordination en 2025. À faire confirmer par la DCP (point 92).
