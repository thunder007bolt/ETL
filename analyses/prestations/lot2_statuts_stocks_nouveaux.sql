-- =============================================================================
-- LOT 2 — Statuts des débours, pensionnés au 31/12, nouveaux pensionnés,
--         mois cotisés (T31 à T57)
-- Rédigé et exécuté le 26/10/2026 sur la base source, schéma USER_DWH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement
-- (aucune donnée nominative).
-- Lot lourd : P22, P23 et P25 à P29 lisent DEBOURS (71,7 millions de lignes).
-- Le contenu des tables DTM n'est pas utilisé : il est considéré comme faux.
-- Les règles sont refaites à partir de la source.
-- Pourquoi ce lot :
--   1. Les débours ont 7 statuts sans libellé (lot 1, P06) : il faut savoir
--      lesquels sont de vrais paiements, et ce que représente DEB_DATE_EFFET
--      (mois payé ou date du paiement ?).
--   2. T32 à T41 demandent les pensionnés présents au 31 décembre : on compare
--      deux méthodes, les dates de la prestation et les paiements du dernier
--      trimestre.
--   3. T42 à T49 demandent les nouveaux pensionnés de l'année : on compare les
--      dates possibles (effet, notification, calcul, premier paiement).
--   4. T42 et T50 demandent les services passés (mois cotisés) : il faut
--      savoir s'ils sont fiables et s'ils se comptent une fois par personne.
-- Numérotation : suite du lot 1 (P19 y est réservée), P20 à P30.
-- =============================================================================


-- =============================================================================
-- 1. STATUTS DES DÉBOURS
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P20 — Débours de 2025 par statut et par type : ce qui les accompagne
-- Objectif : deviner le sens de chaque statut (A, D, I, N, P, S, V) d'après
--            les dates et liens remplis (effet, prestation, approbation,
--            décaissement, annulation, comptabilité). Mesurer aussi l'écart
--            entre la saisie et DEB_DATE_EFFET : un grand écart veut dire que
--            DEB_DATE_EFFET est le mois payé (rappels), pas la date du paiement.
-- Tableaux : T31, T50 à T56.
-- Résultat : Paiements (PM) de 2025 par statut : D 1 843 527 (53,72 Md), P 2
--            194 991 (7,63 Md), V 125 258 (0,64 Md), I 12 499 (0,12 Md), N 4
--            558, A 544, S 56. Tous les paiements ont une prestation (PE_ID).
--            D : effet 100 %, approbation 100 %, décaissement 66 %,
--            comptabilité 95 %. P : aucun effet, approbation 100 %,
--            comptabilité 94 %. V : effet 94 %, approbation 100 %, aucun
--            décaissement. N, A, S : presque jamais approuvés. Écart médian
--            saisie − DEB_DATE_EFFET : −25 j (D), −20 j (P) ; 163 666 débours
--            D (9 %) saisis 2 mois ou plus après.
-- Constat  : D = payé par un effet décaissé ; P = payé sans effet (autre
--            circuit), approuvé et comptabilisé ; V = validé mais pas décaissé
--            ; N, A, S = non approuvés (à confirmer par la DSI). Les vrais
--            paiements sont D et P : 61,35 Md en 2025. DEB_DATE_EFFET est le
--            mois payé : les débours sont préparés avant l'échéance, et 9 %
--            sont des rappels saisis plus tard. PE_ID est toujours rempli : la
--            personne se lit dans PRESTATION_ESP pour tous les paiements, même
--            sans effet.
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


-- -----------------------------------------------------------------------------
-- P21 — Statut du débours face au statut de son effet (2025)
-- Objectif : voir si un statut de débours correspond à un effet décaissé
--            (EFP_STATUT D ?), édité ou annulé.
-- Tableaux : T31, T50 à T56.
-- Résultat : Débours D avec effets D : 2 094 274 débours, 507 631 effets,
--            54,33 Md, tous décaissés. Débours V avec effets N : 118 297
--            débours, 9 873 effets, 0,60 Md, aucun décaissé. Débours I avec
--            effets I : 13 929 débours, 2 683 effets, aucun décaissé. Les
--            autres croisements comptent quelques unités.
-- Constat  : Le statut du débours suit celui de son effet. Seul D correspond à
--            un effet décaissé. Le sens de I est à obtenir.
-- -----------------------------------------------------------------------------
SELECT deb.DEB_STATUT, e.EFP_STATUT,
       COUNT(*)                                                             AS nb_debours,
       COUNT(DISTINCT e.EFP_ID)                                             AS nb_effets,
       SUM(deb.DEB_MONTANT)                                                 AS montant,
       SUM(CASE WHEN e.EFP_DATE_EDITION IS NOT NULL THEN 1 ELSE 0 END)      AS effet_edite,
       SUM(CASE WHEN e.EFP_DATE_DECAISSEMENT IS NOT NULL THEN 1 ELSE 0 END) AS effet_decaisse
FROM USER_DWH.DEBOURS deb
JOIN USER_DWH.EFFET e ON e.EFP_ID = deb.EFP_ID
WHERE deb.DEB_DATE_EFFET >= DATE '2025-01-01' AND deb.DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY deb.DEB_STATUT, e.EFP_STATUT
ORDER BY deb.DEB_STATUT, e.EFP_STATUT;


-- -----------------------------------------------------------------------------
-- P22 — Statuts des paiements par année et par branche (2019–2025)
-- Objectif : un statut qui ne touche que les mois récents (P, V…) désigne un
--            paiement en cours de traitement ; un statut stable dans le temps,
--            un état final.
-- Tableaux : T50 à T56.
-- Résultat : Paiements par an, de 2019 à 2025. F : D 2 293 821 → 1 092 072 ; P
--            614 288 → 1 983 029 ; V 12 090 → 111 736. V : D 650 425 → 688 926
--            ; P 137 692 → 206 963 ; V de 8 634 à 12 491 chaque année ; A 6
--            480 en 2025. M : D 38 249 → 41 835 ; V 1 943 en 2025. A : D 24
--            278 → 20 694 ; P 1 685 → 2 642 ; V de 687 à 1 155 par an.
-- Constat  : P existe depuis 2019 : c'est un circuit de paiement stable, pas
--            un état provisoire. En 2025, les PF passent en masse au paiement
--            sans effet. Le statut V revient chaque année (environ 10 000
--            paiements de vieillesse) : effets jamais retirés ? Le pic de V et
--            de A en 2025 vient des paiements pas encore réglés à la date de
--            la copie.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM DEB_DATE_EFFET) AS annee, CODE_BRANCHE,
       SUM(CASE WHEN DEB_STATUT = 'A' THEN 1 ELSE 0 END) AS statut_a,
       SUM(CASE WHEN DEB_STATUT = 'D' THEN 1 ELSE 0 END) AS statut_d,
       SUM(CASE WHEN DEB_STATUT = 'I' THEN 1 ELSE 0 END) AS statut_i,
       SUM(CASE WHEN DEB_STATUT = 'N' THEN 1 ELSE 0 END) AS statut_n,
       SUM(CASE WHEN DEB_STATUT = 'P' THEN 1 ELSE 0 END) AS statut_p,
       SUM(CASE WHEN DEB_STATUT = 'S' THEN 1 ELSE 0 END) AS statut_s,
       SUM(CASE WHEN DEB_STATUT = 'V' THEN 1 ELSE 0 END) AS statut_v,
       SUM(CASE WHEN DEB_STATUT IS NULL
                  OR DEB_STATUT NOT IN ('A','D','I','N','P','S','V')
                THEN 1 ELSE 0 END)                       AS autre
FROM USER_DWH.DEBOURS
WHERE DEB_TYPE = 'PM'
  AND DEB_DATE_EFFET >= DATE '2019-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM DEB_DATE_EFFET), CODE_BRANCHE
ORDER BY CODE_BRANCHE, annee;


-- -----------------------------------------------------------------------------
-- P23 — Paiements de 2025 avec et sans effet : mode de paiement et statut
-- Objectif : comprendre les 62 % de paiements PF sans effet (lot 1, P07) :
--            virement, bordereau, autre circuit ? Ont-ils une prestation
--            (PE_ID), qui donnerait le bénéficiaire à la place de l'effet ?
-- Tableaux : T41, T50 à T56.
-- Résultat : Paiements de 2025 sans effet : presque tous au statut P, surtout
--            en mode V (F : 1 760 109 débours, 4,40 Md ; V : 161 869, 2,25
--            Md). Modes rencontrés : C, E, M, P, T, V. Avec effet : surtout
--            statut D, mode V aussi. Aucune facture ; date de décaissement
--            seulement sur les débours D.
-- Constat  : Le paiement sans effet est un circuit à part (statut P), quel que
--            soit le mode. Les libellés des modes de paiement (C, E, M, P, T,
--            V) sont à obtenir.
-- -----------------------------------------------------------------------------
SELECT CODE_BRANCHE,
       CASE WHEN EFP_ID IS NULL THEN 'SANS EFFET' ELSE 'AVEC EFFET' END     AS effet,
       DEB_MODE_PAIEMENT, DEB_STATUT,
       COUNT(*)                                                           AS nb_debours,
       SUM(DEB_MONTANT)                                                   AS montant,
       SUM(CASE WHEN PE_ID IS NOT NULL THEN 1 ELSE 0 END)                 AS avec_prestation,
       SUM(CASE WHEN FAC_ID IS NOT NULL THEN 1 ELSE 0 END)                AS avec_facture,
       SUM(CASE WHEN DEB_DATE_DECAISSEMENT IS NOT NULL THEN 1 ELSE 0 END) AS avec_decaissement
FROM USER_DWH.DEBOURS
WHERE DEB_TYPE = 'PM'
  AND DEB_DATE_EFFET >= DATE '2025-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY CODE_BRANCHE, CASE WHEN EFP_ID IS NULL THEN 'SANS EFFET' ELSE 'AVEC EFFET' END,
         DEB_MODE_PAIEMENT, DEB_STATUT
ORDER BY 1, 2, 3, 4;


-- =============================================================================
-- 2. PENSIONNÉS PRÉSENTS AU 31 DÉCEMBRE (T32 à T41)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P24 — État des prestations : statut et dates de suspension, radiation,
--       réactivation
-- Objectif : savoir si ces dates sont remplies et cohérentes avec PE_STATUT
--            (A, F, S), pour dire qui est encore pensionné à une date donnée.
-- Tableaux : T32 à T41.
-- Résultat : Statut et dates concordent. FAF : A 293 702 ; F 199 477 (199 310
--            avec radiation) ; S 121 632 (196 593 avec suspension, 44 369
--            réactivations). RPV : A 33 194 ; F 13 320 (13 311 radiées) ; S
--            322. RPSE : A 13 292, F 15 674, S 723. RPSV : A 20 908, F 4 322,
--            S 311. Aucune prestation active n'a de date de radiation ; toutes
--            ont une date d'effet.
-- Constat  : PE_STATUT est fiable : A = active, F = fermée (radiée), S =
--            suspendue.
-- -----------------------------------------------------------------------------
SELECT TPE_CODE,
       COUNT(*)                                                             AS nb_prestations,
       SUM(CASE WHEN PE_STATUT = 'A' THEN 1 ELSE 0 END)                     AS statut_a,
       SUM(CASE WHEN PE_STATUT = 'F' THEN 1 ELSE 0 END)                     AS statut_f,
       SUM(CASE WHEN PE_STATUT = 'S' THEN 1 ELSE 0 END)                     AS statut_s,
       SUM(CASE WHEN PE_DATE_EFFET IS NULL THEN 1 ELSE 0 END)               AS sans_date_effet,
       SUM(CASE WHEN PE_DATE_RADIATION IS NOT NULL THEN 1 ELSE 0 END)       AS avec_radiation,
       SUM(CASE WHEN PE_DATE_SUSPENSION IS NOT NULL THEN 1 ELSE 0 END)      AS avec_suspension,
       SUM(CASE WHEN PE_DATE_REACTIVATION IS NOT NULL THEN 1 ELSE 0 END)    AS avec_reactivation,
       SUM(CASE WHEN PE_STATUT = 'F' AND PE_DATE_RADIATION IS NULL
                THEN 1 ELSE 0 END)                                          AS fermee_sans_radiation,
       SUM(CASE WHEN PE_STATUT = 'A' AND PE_DATE_RADIATION IS NOT NULL
                THEN 1 ELSE 0 END)                                          AS active_avec_radiation,
       SUM(CASE WHEN PE_STATUT = 'S' AND PE_DATE_SUSPENSION IS NULL
                THEN 1 ELSE 0 END)                                          AS suspendue_sans_date
FROM USER_DWH.PRESTATION_ESP
GROUP BY TPE_CODE
ORDER BY nb_prestations DESC;


-- -----------------------------------------------------------------------------
-- P25 — Pensionnés au 31/12/2025 : méthode des dates face aux paiements
-- Objectif : pour chaque prestation périodique, comparer :
--            - « active par les dates » : effet au plus tard le 31/12, pas
--              radiée avant, pas suspendue au 31/12 (sauf réactivation) ;
--            - « payée au 4e trimestre 2025 » (paiement relié par PE_ID ;
--              trimestre plutôt que mois, car FAF est payée par trimestre) ;
--            - le même test par dossier et code (DOS_CODE + TPE_CODE), au cas
--              où PE_ID serait mal rempli sur les débours.
--            Compter aussi les actives dont l'ayant droit est décédé.
-- Tableaux : T32 à T41.
-- Résultat : Au 31/12/2025 (actives par les dates / payées au 4e trimestre /
--            actives non payées / payées non actives) : FAF 297 664 / 261 402
--            / 38 919 / 2 657 ; RPV 32 974 / 29 643 / 3 537 / 206 ; RPSV 20
--            931 / 19 474 / 1 514 / 57 ; RPSE 13 383 / 4 974 / 8 447 / 38 ;
--            RBEC 42 518 / 17 702 / 24 905 / 89 ; ARI 1 277 / 1 190 / 96 / 9.
--            Actives dont l'ayant droit est décédé : RPV 215, RPSV 439, FAF
--            85. Payées en décembre ≈ payées au trimestre (RPV 29 501).
-- Constat  : Les deux méthodes concordent à 10 % près pour les pensions de
--            vieillesse et de veuve, mais pas pour les orphelins : 8 447
--            pensions d'orphelin actives n'ont aucun paiement au 4e trimestre
--            (limite d'âge atteinte sans radiation ?). Proposition : pensionné
--            au 31/12 = pension active à cette date et payée au 4e trimestre.
--            215 pensionnés de vieillesse actifs sont décédés.
-- -----------------------------------------------------------------------------
WITH paie AS (
    SELECT PE_ID,
           MAX(CASE WHEN DEB_DATE_EFFET >= DATE '2025-12-01' THEN 1 ELSE 0 END) AS paye_dec
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND PE_ID IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-10-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
    GROUP BY PE_ID
),
paie_dos AS (
    SELECT DISTINCT DOS_CODE, TPE_CODE
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND TPE_CODE IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-10-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
),
pe AS (
    SELECT p.PE_ID, p.DOS_CODE, p.TPE_CODE, p.PE_STATUT, p.IND_ID_PREST,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2025-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
                THEN 1 ELSE 0 END AS actif_dates
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                         'ARI', 'ARSV', 'ARSF', 'ARSE', 'ARSA', 'FAF', 'RBEC')
)
SELECT pe.TPE_CODE,
       COUNT(*)                                                             AS nb_prestations,
       SUM(CASE WHEN pe.PE_STATUT = 'A' THEN 1 ELSE 0 END)                  AS statut_a,
       SUM(pe.actif_dates)                                                  AS actives_par_dates,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)                AS payees_au_t4,
       SUM(NVL(pa.paye_dec, 0))                                             AS payees_en_decembre,
       SUM(CASE WHEN pe.actif_dates = 1 AND pa.PE_ID IS NOT NULL
                THEN 1 ELSE 0 END)                                          AS actives_et_payees_t4,
       SUM(CASE WHEN pe.actif_dates = 1 AND pa.PE_ID IS NULL
                THEN 1 ELSE 0 END)                                          AS actives_non_payees_t4,
       SUM(CASE WHEN pe.actif_dates = 0 AND pa.PE_ID IS NOT NULL
                THEN 1 ELSE 0 END)                                          AS payees_non_actives,
       SUM(CASE WHEN pd.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END)             AS dossier_paye_t4,
       SUM(CASE WHEN pe.actif_dates = 1 AND i.IND_DATE_DECES <= DATE '2025-12-31'
                THEN 1 ELSE 0 END)                                          AS actives_ayant_droit_decede,
       COUNT(DISTINCT CASE WHEN pa.PE_ID IS NOT NULL
                           THEN pe.IND_ID_PREST END)                        AS personnes_payees_t4
FROM pe
LEFT JOIN paie                pa ON pa.PE_ID    = pe.PE_ID
LEFT JOIN paie_dos            pd ON pd.DOS_CODE = pe.DOS_CODE AND pd.TPE_CODE = pe.TPE_CODE
LEFT JOIN USER_DWH.INDIVIDU   i  ON i.IND_ID    = pe.IND_ID_PREST
GROUP BY pe.TPE_CODE
ORDER BY nb_prestations DESC;


-- -----------------------------------------------------------------------------
-- P26 — Esquisse de T32 : pensionnés de vieillesse au 31/12/2025
-- Objectif : produire un premier T32 (pension de vieillesse et retraite
--            anticipée), avec les deux méthodes de P25, par tranche d'âge
--            CIPRES et par sexe, avec le montant payé en 2025. L'âge suit
--            l'Annuaire : année 2025 moins année de naissance. La personne
--            comptée est l'ayant droit (IND_ID_PREST).
-- Tableaux : T32.
-- Résultat : Pensionnés de vieillesse (RPV et RPRA) payés au 4e trimestre 2025
--            : 29 721 personnes (23 942 hommes, 5 779 femmes), 41,56 Md versés
--            en 2025 (hommes 31,79 Md, femmes 9,77 Md). Moins de 55 ans : 9 ;
--            55-59 : 3 030 ; 60-64 : 8 332 ; 65-69 : 7 243 ; 70-74 : 5 515 ;
--            75-79 : 3 250 ; 80 ans et plus : 2 342. Actifs par les dates : 33
--            056. 100 ans et plus : 69 payés (91 actifs).
-- Constat  : T32 se calcule depuis la source. Pension moyenne : 1,40 M FCFA
--            par an (hommes 1,33 M, femmes 1,69 M). 69 pensionnés payés ont
--            100 ans ou plus : à vérifier (contrôle d'existence).
-- -----------------------------------------------------------------------------
WITH paie AS (
    SELECT PE_ID,
           SUM(DEB_MONTANT)                                                  AS montant_2025,
           MAX(CASE WHEN DEB_DATE_EFFET >= DATE '2025-10-01' THEN 1 ELSE 0 END) AS paye_t4
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND PE_ID IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
    GROUP BY PE_ID
),
base AS (
    SELECT p.IND_ID_PREST, i.IND_SEXE,
           2025 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) AS age,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2025-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
                THEN 1 ELSE 0 END                         AS actif_dates,
           NVL(pa.paye_t4, 0)                             AS paye_t4,
           pa.montant_2025
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.INDIVIDU i  ON i.IND_ID  = p.IND_ID_PREST
    LEFT JOIN paie        pa ON pa.PE_ID  = p.PE_ID
    WHERE p.TPE_CODE IN ('RPV', 'RPRA')
),
tranche AS (
    SELECT b.*,
           CASE WHEN age < 55   THEN '00 moins de 55'
                WHEN age >= 100 THEN '20 100 et plus'
                ELSE LPAD(TO_CHAR(FLOOR(age / 5)), 2, '0') || ' '
                     || TO_CHAR(FLOOR(age / 5) * 5) || '-' || TO_CHAR(FLOOR(age / 5) * 5 + 4)
           END AS tranche_t32
    FROM base b
)
SELECT tranche_t32, IND_SEXE,
       COUNT(DISTINCT CASE WHEN actif_dates = 1 THEN IND_ID_PREST END) AS effectif_actif_dates,
       COUNT(DISTINCT CASE WHEN paye_t4 = 1 THEN IND_ID_PREST END)     AS effectif_paye_t4,
       SUM(CASE WHEN paye_t4 = 1 THEN montant_2025 END)                AS montant_2025_payes_t4
FROM tranche
GROUP BY tranche_t32, IND_SEXE
ORDER BY tranche_t32, IND_SEXE;


-- =============================================================================
-- 3. NOUVEAUX PENSIONNÉS (T42 à T49) ET BÉNÉFICIAIRES DE L'ANNÉE (T50 à T55)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P27 — Prestations « nouvelles » en 2025 selon la date retenue
-- Objectif : pour chaque code, compter les prestations dont la date d'effet,
--            la demande, la notification, la saisie, le calcul initial (étape
--            CIP) ou le premier paiement tombe en 2025, pour choisir la date
--            qui fait d'une personne un « nouveau pensionné ».
-- Tableaux : T42 à T55.
-- Résultat : Prestations de 2025 selon la date (effet / demande / notification
--            / saisie / calcul initial / premier mois payé / premier paiement
--            saisi) : RPV 2 308 / 2 674 / 2 949 / 2 953 / 2 942 / 1 959 / 2
--            682 ; RAV 747 / 850 / 825 / 904 / 903 / 522 / 785 ; RPSV 420 /
--            774 / 800 / 788 / 788 / 337 / 764 ; RPSE 301 / 727 / 797 / 791 /
--            790 / 227 / 742 ; ARI 72 / 41 / 96 / 85 / 84 / 65 / 82 ; FAF 16
--            691 / 28 739 / 25 753 / 29 794 / 29 794 / 7 624 / 25 494 ; MIJ 4
--            196 / 4 316 / 4 273 / 4 260 / 4 259 / 4 043 / 4 267. Jamais
--            payées : FAF 56 265, RPV 4 034, RPSE 2 333.
-- Constat  : Notification, saisie, calcul initial et premier paiement saisi
--            donnent presque le même nombre : ils marquent la liquidation. La
--            date d'effet et le premier mois payé en donnent beaucoup moins,
--            car la pension remonte souvent avant l'année (rappel).
--            Proposition : nouveau pensionné de l'exercice = pension notifiée
--            dans l'exercice.
-- -----------------------------------------------------------------------------
WITH prem AS (
    SELECT PE_ID,
           MIN(DEB_DATE_EFFET)  AS premier_mois_paye,
           MIN(DEB_DATE_INSERT) AS premier_paiement_saisi
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND PE_ID IS NOT NULL
    GROUP BY PE_ID
),
cip AS (
    SELECT PE_ID, MIN(EA_DATE_INSERT) AS date_calcul_initial
    FROM USER_DWH.ETAPE
    WHERE TEA_CODE = 'CIP'
    GROUP BY PE_ID
)
SELECT p.TPE_CODE,
       SUM(CASE WHEN p.PE_DATE_EFFET >= DATE '2025-01-01'
                 AND p.PE_DATE_EFFET <  DATE '2026-01-01' THEN 1 ELSE 0 END)        AS effet_2025,
       SUM(CASE WHEN p.PE_DATE_DEMANDE >= DATE '2025-01-01'
                 AND p.PE_DATE_DEMANDE <  DATE '2026-01-01' THEN 1 ELSE 0 END)      AS demande_2025,
       SUM(CASE WHEN p.PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                 AND p.PE_DATE_NOTIFICATION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS notifiee_2025,
       SUM(CASE WHEN p.PE_DATE_INSERT >= DATE '2025-01-01'
                 AND p.PE_DATE_INSERT <  DATE '2026-01-01' THEN 1 ELSE 0 END)       AS saisie_2025,
       SUM(CASE WHEN c.date_calcul_initial >= DATE '2025-01-01'
                 AND c.date_calcul_initial <  DATE '2026-01-01' THEN 1 ELSE 0 END)  AS calcul_initial_2025,
       SUM(CASE WHEN pr.premier_mois_paye >= DATE '2025-01-01'
                 AND pr.premier_mois_paye <  DATE '2026-01-01' THEN 1 ELSE 0 END)   AS premier_mois_paye_2025,
       SUM(CASE WHEN pr.premier_paiement_saisi >= DATE '2025-01-01'
                 AND pr.premier_paiement_saisi <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS premier_paiement_saisi_2025,
       SUM(CASE WHEN pr.PE_ID IS NULL THEN 1 ELSE 0 END)                            AS jamais_payee
FROM USER_DWH.PRESTATION_ESP p
LEFT JOIN prem pr ON pr.PE_ID = p.PE_ID
LEFT JOIN cip  c  ON c.PE_ID  = p.PE_ID
GROUP BY p.TPE_CODE
ORDER BY effet_2025 DESC;


-- -----------------------------------------------------------------------------
-- P28 — Délais entre les dates d'une prestation notifiée en 2025
-- Objectif : mesurer l'écart entre date d'effet, notification et premier
--            paiement, pour savoir quelle date fixe l'année d'entrée. Un effet
--            très antérieur à la notification veut dire que la pension est
--            payée avec rappel.
-- Tableaux : T42 à T49.
-- Résultat : Notifiées en 2025 : RPV 2 949, dont 988 avec un effet avant 2025
--            ; médiane effet → notification 134 j, notification → premier
--            paiement 4 j ; 289 pas encore payées. RAV 825 (282 ; 133 j ; 7
--            j). RPSV 800 (482 ; 241 j ; 4 j). RPSE 797 (578 ; 324 j ; 4 j).
--            ARI 96 (37 ; 116 j ; 0 j). Rentes AT de survivants : de 795 à 992
--            j.
-- Constat  : La pension est payée quelques jours après la notification, avec
--            un rappel qui remonte de 4 à 11 mois en médiane. Un tiers des
--            pensions de vieillesse notifiées en 2025 ont un effet en 2024 ou
--            avant : l'année d'effet ne donne pas les entrées de l'année.
-- -----------------------------------------------------------------------------
WITH prem AS (
    SELECT PE_ID,
           MIN(DEB_DATE_EFFET)  AS premier_mois_paye,
           MIN(DEB_DATE_INSERT) AS premier_paiement_saisi
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND PE_ID IS NOT NULL
    GROUP BY PE_ID
)
SELECT p.TPE_CODE,
       COUNT(*)                                                                  AS notifiees_2025,
       SUM(CASE WHEN p.PE_DATE_EFFET < DATE '2025-01-01' THEN 1 ELSE 0 END)      AS effet_avant_2025,
       ROUND(MEDIAN(p.PE_DATE_NOTIFICATION - p.PE_DATE_EFFET))                   AS med_effet_a_notif_j,
       ROUND(MEDIAN(pr.premier_paiement_saisi - p.PE_DATE_NOTIFICATION))         AS med_notif_a_paiement_j,
       ROUND(MEDIAN(pr.premier_paiement_saisi - pr.premier_mois_paye))           AS med_mois_paye_a_saisie_j,
       SUM(CASE WHEN pr.PE_ID IS NULL THEN 1 ELSE 0 END)                         AS pas_encore_payees
FROM USER_DWH.PRESTATION_ESP p
LEFT JOIN prem pr ON pr.PE_ID = p.PE_ID
WHERE p.PE_DATE_NOTIFICATION >= DATE '2025-01-01' AND p.PE_DATE_NOTIFICATION < DATE '2026-01-01'
  AND p.TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                     'ARI', 'ARSV', 'ARSF', 'ARSE', 'ARSA', 'RAV', 'RAS', 'AAI')
GROUP BY p.TPE_CODE
ORDER BY notifiees_2025 DESC;


-- =============================================================================
-- 4. MOIS COTISÉS (T42 et T50 : services passés)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P29 — Mois cotisés : une valeur par dossier ou plusieurs ?
-- Objectif : un dossier peut porter plusieurs prestations du même code
--            (reliquidation, révision, plusieurs orphelins). On vérifie si
--            leurs mois cotisés sont identiques, pour savoir s'il faut les
--            prendre une seule fois par dossier (ou par personne) dans les
--            services passés.
-- Tableaux : T42, T50.
-- Résultat : RPV : 46 281 dossiers ; 528 ont plusieurs prestations RPV,
--            toujours pour la même personne, dont 201 avec des mois différents
--            (6 prestations au plus). RPSV : 4 268 dossiers avec plusieurs
--            veuves (jusqu'à 13). RPSE : 7 985 dossiers avec plusieurs
--            orphelins (jusqu'à 24). RAV : 194 dossiers avec plusieurs
--            prestations pour la même personne. ARI : 328.
-- Constat  : Les mois cotisés sont ceux de l'assuré, recopiés sur chaque
--            prestation du dossier. Une personne peut avoir plusieurs
--            prestations du même code (révision, reliquidation) : compter les
--            personnes, pas les prestations, et prendre les mois de la
--            dernière prestation.
-- -----------------------------------------------------------------------------
WITH v AS (
    SELECT DOS_CODE, TPE_CODE,
           COUNT(*)                            AS nb_prest,
           COUNT(DISTINCT IND_ID_PREST)        AS nb_ayants_droit,
           COUNT(DISTINCT PE_MOIS_COTISATION)  AS nb_valeurs
    FROM USER_DWH.PRESTATION_ESP
    WHERE TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                       'ARI', 'ARSV', 'ARSE')
    GROUP BY DOS_CODE, TPE_CODE
)
SELECT TPE_CODE,
       COUNT(*)                                                     AS nb_dossiers,
       SUM(CASE WHEN nb_prest > 1 THEN 1 ELSE 0 END)                AS dossiers_plusieurs_prest,
       SUM(CASE WHEN nb_prest > nb_ayants_droit THEN 1 ELSE 0 END)  AS meme_ayant_droit_repete,
       SUM(CASE WHEN nb_valeurs > 1 THEN 1 ELSE 0 END)              AS mois_differents,
       MAX(nb_prest)                                                AS max_prest_par_dossier
FROM v
GROUP BY TPE_CODE
ORDER BY nb_dossiers DESC;


-- -----------------------------------------------------------------------------
-- P30 — Qualité des mois cotisés des pensions et rentes
-- Objectif : vérifier l'unité (des mois ?), les valeurs vides ou aberrantes,
--            la cohérence total = obligatoire + volontaire, et la moyenne des
--            nouveaux de 2025 (T42 : total des services passés ; T50 : nombre
--            moyen de services passés des allocations de vieillesse).
-- Tableaux : T42, T50.
-- Résultat : Unité : le mois (RPV : médiane 295, soit 24,6 ans ; moyenne 304).
--            RAV : médiane 92, maximum 489. Maximum de 999 pour RPV, RPSE,
--            RPSV et RPI ; plus de 600 mois : RPV 229, RPSE 519, RPSV 1 133.
--            Total différent de obligatoire + volontaire : RPV 44 152 sur 46
--            836. Avec des mois volontaires : RPV 2 834. Prestations AT et
--            maternité : jamais renseignées. Moyenne des pensions à effet 2025
--            : RPV 295 mois, RAV 98.
-- Constat  : Les mois cotisés des pensions sont utilisables (T42, T50), sauf
--            au-dessus de 600 (999 semble être une valeur par défaut). Le
--            détail obligatoire / volontaire ne s'additionne presque jamais au
--            total : ne garder que le total. Des RAV vont jusqu'à 489 mois :
--            une allocation de vieillesse ne devrait concerner que les durées
--            courtes (à confirmer).
-- -----------------------------------------------------------------------------
SELECT TPE_CODE,
       COUNT(*)                                                           AS nb_prestations,
       SUM(CASE WHEN PE_MOIS_COTISATION IS NULL THEN 1 ELSE 0 END)        AS vides,
       SUM(CASE WHEN PE_MOIS_COTISATION = 0 THEN 1 ELSE 0 END)            AS a_zero,
       MIN(PE_MOIS_COTISATION)                                            AS minimum,
       ROUND(MEDIAN(PE_MOIS_COTISATION))                                  AS mediane,
       ROUND(AVG(PE_MOIS_COTISATION), 1)                                  AS moyenne,
       MAX(PE_MOIS_COTISATION)                                            AS maximum,
       SUM(CASE WHEN PE_MOIS_COTISATION > 600 THEN 1 ELSE 0 END)          AS plus_de_600,
       SUM(CASE WHEN PE_MOIS_COTISATION IS NOT NULL
                 AND NVL(PE_MOIS_COTI_ASSU_OBLI, 0) + NVL(PE_MOIS_COTI_ASSU_VOL, 0)
                     <> PE_MOIS_COTISATION
                THEN 1 ELSE 0 END)                                        AS total_different,
       SUM(CASE WHEN PE_MOIS_COTI_ASSU_VOL > 0 THEN 1 ELSE 0 END)         AS avec_volontaire,
       SUM(CASE WHEN PE_MOIS_NOUVELLE_LOI IS NOT NULL THEN 1 ELSE 0 END)  AS avec_mois_nouvelle_loi,
       ROUND(AVG(CASE WHEN PE_DATE_EFFET >= DATE '2025-01-01'
                       AND PE_DATE_EFFET <  DATE '2026-01-01'
                      THEN PE_MOIS_COTISATION END), 1)                    AS moyenne_effet_2025
FROM USER_DWH.PRESTATION_ESP
WHERE TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                   'ARI', 'ARSV', 'ARSE', 'AIJ', 'MIJ')
GROUP BY TPE_CODE
ORDER BY nb_prestations DESC;
