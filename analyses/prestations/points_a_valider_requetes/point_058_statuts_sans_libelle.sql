-- =============================================================================
-- Point 58. Les statuts sans libellé
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « le sens se lit dans les données pour le dossier
--   (DOS_STATUT : A, F) et l'ajustement (AJ_STATUT : A, C, R). Restent
--   inconnus : PE_STATUT (A, F, S), RD_STATUT (E, I, L, R, A), EFP_STATUT (A,
--   D, I, N), NOT_STATUT (A, I), le statut I des ajustements et les cases
--   « vérifié » (DEB_VERIFIE, DOS_VERIFIE…). » ; « ce statut n'apparaît plus
--   depuis 2014. »
-- Requêtes :
--   1. Les valeurs de chaque statut et drapeau des tables des prestations.
--   2. Les ajustements par type et par statut, toutes années.
-- Reprise de : lot1_inventaire_prestations.sql, P06 (exécutée le
--   26/10/2026) ; lot9_indus.sql, P103 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. Les deux
--   requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements
--   (AJ_STATUT <> R). Fiche des règles : règle 45.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par table, colonne et branche : les valeurs des statuts et des
-- drapeaux (PE_STATUT, DEB_STATUT, EFP_STATUT, NOT_STATUT, RD_STATUT,
-- DEB_VERIFIE…), avec leur nombre.
-- -----------------------------------------------------------------------------
SELECT 'PRESTATION_ESP.PE_STATUT' AS champ, TO_CHAR(d.TDOS_CODE) AS branche,
       TO_CHAR(pe.PE_STATUT) AS valeur, COUNT(*) AS nb
FROM USER_DWH.PRESTATION_ESP pe
LEFT JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
GROUP BY d.TDOS_CODE, pe.PE_STATUT
UNION ALL
SELECT 'DEBOURS.DEB_STATUT', TO_CHAR(CODE_BRANCHE), TO_CHAR(DEB_STATUT), COUNT(*)
FROM USER_DWH.DEBOURS GROUP BY CODE_BRANCHE, DEB_STATUT
UNION ALL
SELECT 'DEBOURS.DEB_VERIFIE', TO_CHAR(CODE_BRANCHE), TO_CHAR(DEB_VERIFIE), COUNT(*)
FROM USER_DWH.DEBOURS GROUP BY CODE_BRANCHE, DEB_VERIFIE
UNION ALL
SELECT 'EFFET.EFP_STATUT', TO_CHAR(CODE_BRANCHE), TO_CHAR(EFP_STATUT), COUNT(*)
FROM USER_DWH.EFFET GROUP BY CODE_BRANCHE, EFP_STATUT
UNION ALL
SELECT 'RECEPTION_DOSSIER.RD_STATUT', TO_CHAR(TDOS_CODE), TO_CHAR(RD_STATUT), COUNT(*)
FROM USER_DWH.RECEPTION_DOSSIER GROUP BY TDOS_CODE, RD_STATUT
UNION ALL
SELECT 'NOTIFICATION_PREST.NOT_STATUT', TO_CHAR(TDOS_CODE), TO_CHAR(NOT_STATUT), COUNT(*)
FROM USER_DWH.NOTIFICATION_PREST GROUP BY TDOS_CODE, NOT_STATUT
ORDER BY 1, 2, 3;

-- Résultat obtenu (P06, exécutée le 26/10/2026) : PE_STATUT : A, F, S.
-- DEB_STATUT : A, D, I, N, P, S, V. EFP_STATUT : A, D, I, N (et un « v »).
-- NOT_STATUT : A, I. RD_STATUT : E 272 455, I, L, R, A, vide. DEB_VERIFIE = O :
-- 5 942 752 débours. Aucun statut n'a de libellé.


-- -----------------------------------------------------------------------------
-- Requête 2. Ajustements par type et par statut (AJ_STATUT), toutes années :
-- nombre, montant, débours payés, précomptes, et années des statuts.
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
-- resultats/lot9_P103_statuts.tsv) : C = soldé (un moins-perçu C est payé en
-- entier) ; A = en cours ; R = annulé (un moins-perçu R n'a presque rien
-- payé : 25 749 pour 178,8 M). MP au statut R : de 2007 à 2014 seulement.
-- Lecture : le sens de A, C et R se lit dans les données ; R n'apparaît plus
--   depuis 2014, donc l'écarter de T31 ne change rien depuis 2015.
