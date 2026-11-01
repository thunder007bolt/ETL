-- =============================================================================
-- LOT 9 — Contrôles pour T31 (les prestations indues)
-- Rédigé et exécuté le 01/11/2026 ; résultats dans les en-têtes et dans
-- resultats/lot9_*.tsv.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 8.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages et sommes
-- seulement. P103, P104 et P105 lisent DEBOURS : plusieurs minutes chacune.
-- Données nominatives : jamais lues. AJ_REMARQUE (texte libre) n'est pas lue ;
-- la date de décès ne sert qu'à compter.
-- Pourquoi ce lot : T31 répartit, mois par mois et par branche (PVID, AT/MP,
-- PF), les prestations payées, les trop-perçus et les moins-perçus, et donne
-- le taux de prestations indues, dont la norme CIPRES est de 2 % au plus
-- (Annuaire p. 13, maquette p. 36). L'atelier de Kaya 2017 précise : « les
-- prestations indues sont des manques à gagner pour la CNSS (prestations
-- perçues en trop) ; les moins perçus ne sont pas des prestations indues ; la
-- colonne payées est la somme décaissée ». L'annuaire de la CNSS 2024 ne
-- publie pas d'indus : pas de chiffre de contrôle.
-- Ce que l'on sait déjà (lot 1 P11, lot 3 P41) : les indus sont des
-- ajustements (table AJUSTEMENT), de 13 types ; trop-perçus TP et TP-CONV ;
-- moins-perçus MP (« moins-perçu ou rappel ») et RC (rappel sur convention) ;
-- REM-TP est le remboursement d'un trop-perçu, pas un nouvel indu ; avances
-- (AV), mois de pension gratuits (MPG), dons et avances COVID, rachats de rente
-- (RR) ne sont pas des indus. Statuts A, C, I, R sans libellé. Le DTM oublie
-- la maternité. Les débours de type AJ (0,8 Md en 2024, lot 8 P95) n'ont pas
-- de prestation : on ne sait pas encore ce qu'ils paient.
-- Ce lot tranche :
--   · les ajustements par type, branche et année, avec leurs statuts     → P102
--   · le sens des statuts A, C, I, R                                     → P103
--   · ce que paient les débours de type AJ (T31, et T56 du lot 8)        → P104
--   · T31 de 2024 rempli : payées, trop-perçus, moins-perçus, taux      → P105
--   · d'où viennent les trop-perçus de 2024 (prestation, décès)         → P106
-- Branche d'un ajustement : celle de son dossier (TDOS_CODE : V = PVID,
-- A = AT/MP, F et M = PF, maternité comprise).
-- Numérotation : suite du lot 8, P102 à P106.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P102 — Les ajustements par type, branche et année
-- Question : combien de trop-perçus et de moins-perçus chaque année, dans
--            quelle branche, et avec quels statuts ?
-- Objectif : pour les ajustements établis de 2023 à 2025
--            (AJ_DATE_ETABLISSEMENT), par année, branche du dossier et type
--            (TP, TP-CONV, REM-TP, MP, RC, AV, le reste groupé) : nombre,
--            montant, nombre par statut (A, C, I, R).
-- Lecture  : la taille des trop-perçus face aux paiements de l'année (environ
--            61 Md en 2025, lot 1) dit déjà si la norme de 2 % est tenue. Le
--            poids de la maternité (branche M) dit ce que le DTM perd.
-- Tableaux : T31.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot9_P102_ajustements_par_type.tsv. Ajustements établis
--            en 2024, nombre / montant :
--            · PVID : TP 581 / 209 020 712 ; TP-CONV 15 / 12 135 453 ; MP
--              204 / 134 626 996 ; RC 1 239 / 136 244 163 ; REM-TP 12 /
--              8 495 627 ; AV 14 157 / 4 435 988 019 ;
--            · PF : TP 47 / 2 638 651 ; MP 17 126 / 132 014 360 ; AV 19 021 /
--              907 330 003 ; maternité : TP 15 / 2 158 495, MP 61 /
--              8 130 978 ;
--            · AT/MP : TP 26 / 3 531 448 ; MP 22 / 3 426 212 ; AV 443 /
--              41 551 186.
--            · Trop-perçus (TP et TP-CONV), toutes branches : 2023 719 /
--              183 140 527 ; 2024 684 / 229 484 759 ; 2025 1 577 / 292 759 685
--              (dont 758 TP-CONV).
--            · Statuts : seulement A et C de 2023 à 2025 ; les trop-perçus
--              sont tous au statut A.
-- Constat  : · Les trop-perçus sont petits : 229,5 M en 2024 pour 58,7 Md
--              payés (P105).
--            · Les avances (AV) sont de loin les plus gros ajustements : 5,4
--              Md en 2024, surtout la PVID. Versées avant la liquidation puis
--              reprises (REV-AV), elles restent hors de T31.
--            · Les moins-perçus de la PF fondent : 56 362 en 2023, 17 126 en
--              2024, 1 384 en 2025. Changement de pratique ? À demander.
--            · La maternité pèse peu (15 trop-perçus, 2,2 M en 2024) mais doit
--              y être : le DTM l'oubliait. Les trop-perçus sur convention (TP-
--              CONV) bondissent en 2025 (758 contre 15).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT)                AS annee,
       CASE d.TDOS_CODE
           WHEN 'V' THEN 'PVID'
           WHEN 'A' THEN 'AT/MP'
           WHEN 'F' THEN 'PF'
           WHEN 'M' THEN 'PF maternité'
           ELSE          '(sans dossier)'
       END                                                        AS branche,
       CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC', 'AV')
            THEN aj.TAJ_CODE ELSE 'AUTRES' END                    AS type_ajustement,
       COUNT(*)                                                   AS ajustements,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       SUM(CASE WHEN aj.AJ_STATUT = 'A' THEN 1 ELSE 0 END)        AS statut_a,
       SUM(CASE WHEN aj.AJ_STATUT = 'C' THEN 1 ELSE 0 END)        AS statut_c,
       SUM(CASE WHEN aj.AJ_STATUT = 'I' THEN 1 ELSE 0 END)        AS statut_i,
       SUM(CASE WHEN aj.AJ_STATUT = 'R' THEN 1 ELSE 0 END)        AS statut_r
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = aj.DOS_CODE
WHERE aj.AJ_DATE_ETABLISSEMENT >= DATE '2023-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT),
         CASE d.TDOS_CODE
             WHEN 'V' THEN 'PVID'
             WHEN 'A' THEN 'AT/MP'
             WHEN 'F' THEN 'PF'
             WHEN 'M' THEN 'PF maternité'
             ELSE          '(sans dossier)'
         END,
         CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC', 'AV')
              THEN aj.TAJ_CODE ELSE 'AUTRES' END
ORDER BY annee, branche, type_ajustement;


-- -----------------------------------------------------------------------------
-- P103 — Le sens des statuts A, C, I, R
-- Question : que veulent dire les statuts d'un ajustement ? Lequel faut-il
--            écarter de T31 (un trop-perçu annulé, par exemple) ?
-- Objectif : pour les trop-perçus et moins-perçus (TP, TP-CONV, REM-TP, MP,
--            RC), par type et statut : ajustements ; sans date
--            d'établissement ; montant ; avec un débours lié (DEBOURS.AJ_ID),
--            et montant de ces débours au statut D ou P ; avec un nombre de
--            précomptes (retenues) ; AJ_DECAISSE rempli ; première et dernière
--            date d'établissement.
-- Lecture  : un statut dont les ajustements ont tous leurs débours payés est
--            « soldé » ; un statut sans débours ni précompte, peut-être
--            « annulé » ou « en attente ». Un moins-perçu payé par débours est
--            un rappel versé au bénéficiaire.
-- Tableaux : T31.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot9_P103_statuts.tsv. Par type et statut, toutes
--            années, ajustements / montant / avec débours / montant des
--            débours payés / avec précompte :
--            · MP A 37 885 / 939 910 315 / 37 819 / 527 482 397 / 179 ; MP C
--              383 794 / 14 084 842 402 / 383 794 / 14 073 501 511 / 1 281 ;
--              MP R 2 501 / 178 832 920 / 2 501 / 25 749 / 0 ;
--            · RC A 2 326 / 1 161 523 996 / 2 325 / 1 108 311 054 / 4 ; RC C
--              3 946 / 538 176 755 / 3 946 / 537 616 684 / 0 ;
--            · TP A 6 879 / 1 470 074 632 / 6 870 / 233 455 778 / 6 877 ; TP C
--              5 601 / 426 683 568 / 5 601 / −426 030 808 / 5 601 ; TP-CONV A
--              936 / 130 315 259 / 936 / 52 468 716 / 936 ;
--            · REM-TP A 15 / 3 189 990 / 10 / −1 624 928 ; REM-TP C 227 /
--              67 216 092 / 227 / −59 519 758.
--            · Aucune date d'établissement vide. TP C : de 2001 à 2017
--              seulement ; MP R : de 2007 à 2014.
-- Constat  : · C = soldé : un moins-perçu C est entièrement payé (14,07 Md
--              pour 14,08 Md), un trop-perçu C entièrement repris (débours
--              négatifs du même montant). A = en cours. R = rejeté ou annulé :
--              les moins-perçus R n'ont presque rien payé (25 749 pour 178,8
--              M).
--            · Depuis 2017, aucun trop-perçu n'est soldé : ils restent au
--              statut A, repris par précomptes (6 877 sur 6 879). Le statut ne
--              dit plus si un indu est recouvré.
--            · Proposition BI : T31 compte tous les trop-perçus établis dans
--              le mois, sauf le statut R (annulé).
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


-- -----------------------------------------------------------------------------
-- P104 — Ce que paient les débours de type AJ
-- Question : les débours de type AJ (0,8 Md en 2024, sans prestation, lot 8
--            P95) versent-ils des rappels (moins-perçus), des avances, ou
--            retiennent-ils des trop-perçus ?
-- Objectif : pour les débours de type AJ au statut D ou P, dont le mois payé
--            tombe en 2024, par branche du débours et type de l'ajustement lié
--            (AJ_ID) : débours, montant, montant le plus petit et le plus
--            grand ; débours sans ajustement lié.
-- Lecture  : si ce sont surtout des moins-perçus (MP) ou des avances (AV),
--            ce sont des sommes versées aux assurés : elles comptent dans les
--            « payées » de T31 et dans T56. Si ce sont des remboursements de
--            trop-perçus, elles viennent en déduction.
-- Tableaux : T31, T56.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot9_P104_debours_aj_2024.tsv. Débours de type AJ payés
--            pour un mois de 2024, par branche et type d'ajustement, débours /
--            montant :
--            · V : AV 169 081 / 303 622 709 ; RC 1 157 / 132 994 743 ; MP
--              175 / 132 576 956 ; TP 6 839 / 78 175 530 ; TP-CONV 522 /
--              6 122 999 ; REM-TP 12 / −8 495 627 ; REV-AV 170 / −57 834 912 ;
--            · F : MP 17 043 / 131 408 360 ; AV 86 494 / 68 834 531 ; TP 118 /
--              1 552 490 ; REM-TP 6 / −387 000 ; REV-AV 12 / −408 000 ;
--            · M : MP 58 / 8 121 771 ; TP 20 / 2 112 687 ; REM-TP 6 /
--              −1 468 043 ;
--            · A : RR (rachat de rente) 11 / 21 519 361 ; MP 21 / 3 210 681 ;
--              TP 112 / 2 775 394 ; AV 5 953 / 749 365 ; REV-AV 2 / −149 001 ;
--              REM-TP 1 / −2 306 520.
--            · Tous les débours AJ ont un ajustement.
-- Constat  : · Les débours AJ versent surtout de l'argent aux assurés :
--              avances (373 M), rappels de moins-perçus (MP, RC : 408 M),
--              rachats de rente AT/MP (21,5 M). Ils reprennent les avances
--              (REV-AV, −58 M) et les trop-perçus remboursés (REM-TP, −13 M).
--              Les débours liés à un trop-perçu ont les deux signes (de −6,1 M
--              à +6,6 M) : à éclaircir avec la DSI.
--            · Les rachats de rente AT/MP de la base (21,5 M) sont proches de
--              ceux de l'annuaire (25,4 M, tableau 97) : la CNSS compte les
--              rachats dans les prestations servies. T56 devrait les prendre
--              (question).
--            · Pour T31, les payées restent les débours de paiement (PM) : y
--              ajouter les rappels changerait le taux de moins d'un centième
--              de point.
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


-- -----------------------------------------------------------------------------
-- P105 — T31 de 2024, rempli
-- Question : que donne T31 avec les règles proposées, et le taux d'indus
--            tient-il la norme de 2 % ?
-- Objectif : pour chaque mois de 2024 et chaque branche (PVID, AT/MP, PF avec
--            la maternité) : payées (débours de paiement PM au statut D ou P),
--            au mois payé et à la saisie du paiement ; trop-perçus établis dans
--            le mois (TP et TP-CONV) ; moins-perçus établis dans le mois (MP et
--            RC) ; remboursements de trop-perçus (REM-TP), à part.
-- Lecture  : taux d'indus = trop-perçus ÷ payées × 100, par mois et sur
--            l'année ; norme CIPRES : 2 % au plus. Les moins-perçus restent
--            dans le tableau mais ne sont pas des indus (Kaya 2017).
-- Tableaux : T31.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot9_P105_t31_2024.tsv. T31 de 2024, sur l'année :
--            payées au mois payé / à la saisie ; trop-perçus ; moins-perçus ;
--            taux d'indus :
--            · PVID 47 237 434 040 / 47 486 010 786 ; 221 156 165 ;
--              270 871 159 ; 0,47 % ;
--            · AT/MP 744 609 484 / 790 138 121 ; 3 531 448 ; 3 426 212 ;
--              0,47 % ;
--            · PF 10 750 873 342 / 10 802 063 700 ; 4 797 146 ; 140 145 338 ;
--              0,045 % ;
--            · Total 58 732 916 866 / 59 078 212 607 ; 229 484 759 ;
--              414 442 709 ; 0,39 %.
--            · Taux mensuel le plus haut : PVID 0,89 % en novembre ; AT/MP
--              4,08 % en avril (un trop-perçu de 2,3 M, remboursé le même
--              mois).
--            · À la saisie, les payées de la PVID montent en février, mai,
--              août et novembre (4,3 à 4,7 Md contre 3,6 à 3,8 Md) ; au mois
--              payé, elles sont régulières (3,9 Md par mois).
-- Constat  : · La norme de 2 % est tenue de loin : 0,39 % en 2024, quelle que
--              soit la date retenue pour les payées.
--            · Pour un tableau mensuel, le mois payé donne une série
--              régulière ; la saisie suit le calendrier des versements, avec
--              des pics en février, mai, août et novembre. La question R4 se
--              pose aussi ici (TODO, tâche 18).
--            · Un trop-perçu se rembourse parfois dans le mois (AT/MP en
--              avril) : REM-TP reste à part, comme prévu.
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


-- -----------------------------------------------------------------------------
-- P106 — D'où viennent les trop-perçus de 2024
-- Question : sur quelles prestations naissent les trop-perçus, et combien
--            viennent d'un décès déclaré tard (pension payée après la mort) ?
-- Objectif : pour les trop-perçus (TP, TP-CONV) établis en 2024, par code de
--            la prestation liée (PE_AJ) : trop-perçus, montant, médiane ;
--            dont la personne était décédée avant l'établissement, et leur
--            montant ; trop-perçus sans prestation liée.
-- Lecture  : une grande part après décès relie T31 au retard de déclaration
--            des décès (lot 8 P99) : la pension continue d'être payée, puis
--            l'indu est établi. Le code dit la branche réelle de l'indu.
-- Tableaux : T31.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot9_P106_trop_percus_2024.tsv. Les 684 trop-perçus de
--            2024 (229 484 759) n'ont aucune prestation liée dans PE_AJ ;
--            médiane 110 000 ; 7 établis après le décès de la personne
--            (2 714 115).
-- Constat  : · Le lien PE_AJ n'existe pas pour les trop-perçus : leur branche
--              se lit par le dossier, comme dans la requête T31.
--            · Presque aucun trop-perçu après décès (7 en 2024), alors que 175
--              décès de retraités de 2024 ont été déclarés après mars 2025
--              (P99) : les pensions versées après un décès déclaré tard ne
--              deviennent pas des trop-perçus, ou sont reprises autrement.
--              Question à la direction des prestations.
-- -----------------------------------------------------------------------------
WITH code_lie AS (
    SELECT pa.AJ_ID,
           MIN(p.TPE_CODE)                                        AS code
    FROM USER_DWH.PE_AJ pa
    JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID = pa.PE_ID
    GROUP BY pa.AJ_ID
)
SELECT NVL(c.code, '(sans prestation)')                           AS code_prestation,
       COUNT(*)                                                   AS trop_percus,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       ROUND(MEDIAN(aj.AJ_MONTANT))                               AS mediane,
       SUM(CASE WHEN i.IND_DATE_DECES < aj.AJ_DATE_ETABLISSEMENT
                THEN 1 ELSE 0 END)                                AS apres_deces,
       ROUND(SUM(CASE WHEN i.IND_DATE_DECES < aj.AJ_DATE_ETABLISSEMENT
                      THEN aj.AJ_MONTANT END))                    AS montant_apres_deces
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN code_lie c           ON c.AJ_ID  = aj.AJ_ID
LEFT JOIN USER_DWH.INDIVIDU i  ON i.IND_ID = aj.IND_ID
WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV')
  AND aj.AJ_DATE_ETABLISSEMENT >= DATE '2024-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2025-01-01'
GROUP BY NVL(c.code, '(sans prestation)')
ORDER BY montant DESC;
