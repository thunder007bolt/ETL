-- =============================================================================
-- LOT 3 — Partie B : contrôles des questions Q1 à Q5 de la fiche T25
-- Rédigé et exécuté le 30/10/2026. P52 avait échoué (ORA-00972, nom de
-- colonne trop long) : corrigée et relancée le même jour. P58, ajoutée après
-- P52, exécutée le même jour.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 3.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement.
-- Lot léger : ne lit jamais DEBOURS. Tables lues : RECEPTION_DOSSIER,
-- RECEPTION_DOS_PIECE, DOSSIER, DOSSIER_REJETE, PRESTATION_ESP,
-- NOTIFICATION_PREST, INDIVIDU (date de décès seulement).
-- Données nominatives : jamais lues (nom, adresse, téléphone du déposant,
-- remarques). Deux champs libres sont regroupés : le lien du déposant avec
-- l'assuré (P53) et le motif du rejet (P56), coupés à 40 ou 60 caractères et
-- seulement pour les valeurs vues au moins 5 ou 3 fois. Si une valeur contient
-- malgré tout un nom, ne pas la recopier.
-- Pourquoi ce lot : la fiche Excalidraw « T25 dossiers de pension » pose cinq
-- questions à la CNSS. Chaque requête mesure dans les données ce qui permet de
-- trancher, ou de préparer la réponse :
--   Q1 coordination              → P48
--   Q2 date du dépôt             → P49
--   Q3 vieilles demandes         → P50, P58
--   Q4 pension de la veuve       → P51 à P54, P58
--   Q5 refus                     → P55 à P57
-- Chaque en-tête a une ligne « Lecture » : ce que le résultat change dans T25.
-- Numérotation : suite de la partie A du lot 3, P48 à P58 (P58 est rangée
-- avec Q4). Les autres points de la partie B (indus, allocataires, frais
-- médicaux AT/MP) suivront à partir de P59.
-- =============================================================================


-- =============================================================================
-- Q1. LES DOSSIERS DE COORDINATION
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P48 — Dossiers de pension par catégorie : la CNSS les instruit-elle ?
-- Question : Q1. Les dossiers payés par une autre caisse (COOR_AC) et les
--            dossiers transférés (CNSS_AC) doivent-ils compter dans T25 ?
-- Objectif : pour chaque catégorie, compter les dossiers reçus au guichet de
--            la CNSS et ceux dont les droits ont été notifiés par la CNSS, et
--            leur poids dans les déposés et les liquidés de 2025.
-- Lecture  : si COOR_AC et CNSS_AC ont une réception et une notification,
--            la CNSS instruit ces dossiers : les remettre dans T25 se défend.
--            S'ils n'en ont presque pas, ils sont saisis pour mémoire : les
--            exclure (règle 1) ne change presque rien aux chiffres.
-- Tableaux : T25.
-- Résultat : Dossiers V par catégorie (dossiers / avec réception / avec
--            notification / déposés en 2025 / liquidés en 2025, 1re
--            notification / durée médiane 2025) : ORDINAIRE 37 864 / 31 636 /
--            32 195 / 3 332 / 3 401 / 70 j ; sans catégorie 30 774 / 35 /
--            23 628 / 1 / 3 / 166 j ; COOR_CNSS 3 466 / 1 429 / 3 217 / 184 /
--            204 / 75 j ; COOR_AC 3 326 / 2 530 / 1 702 / 238 / 292 / 88 j ;
--            CNSS_AC 153 / 12 / 128 / 0 / 1 / 2 536 j. Code de l'autre caisse
--            rempli : COOR_AC 1 173, COOR_CNSS 523. Liquidés de 2025, toutes
--            catégories : 3 901.
-- Constat  : La CNSS instruit les dossiers COOR_AC : 76 % sont reçus à son
--            guichet, la moitié notifiés par elle, 238 dépôts et 292
--            liquidations en 2025 (6 à 8 % de T25). Les exclure cacherait un
--            travail réel. Proposition révisée pour Q1 : garder COOR_AC dans
--            T25, n'exclure que CNSS_AC (transférés : rien en 2025). Les
--            dossiers sans catégorie sont anciens et n'ont presque jamais de
--            réception : le lien réception–dossier manque pour les vieux
--            dossiers. Avec la première notification (règle 3), 3 901 dossiers
--            sont liquidés en 2025, et non environ 4 800 : P09 prenait la
--            dernière notification (voir P54).
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DOS_CODE, TRUNC(MIN(PE_DATE_NOTIFICATION)) AS date_liquidation
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
)
SELECT NVL(d.DOS_CATEGORIE, '(vide)')                                          AS categorie,
       COUNT(*)                                                                AS nb_dossiers,
       SUM(CASE WHEN d.DOS_CODE_AUTRE_CAISSE IS NOT NULL THEN 1 ELSE 0 END)    AS avec_code_autre_caisse,
       SUM(CASE WHEN r.RD_ID IS NOT NULL THEN 1 ELSE 0 END)                    AS avec_reception,
       SUM(CASE WHEN n.date_liquidation IS NOT NULL THEN 1 ELSE 0 END)         AS avec_notification,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS deposes_2025,
       SUM(CASE WHEN n.date_liquidation >= DATE '2025-01-01'
                 AND n.date_liquidation <  DATE '2026-01-01' THEN 1 ELSE 0 END)  AS liquides_2025,
       ROUND(MEDIAN(CASE WHEN n.date_liquidation >= DATE '2025-01-01'
                          AND n.date_liquidation <  DATE '2026-01-01'
                         THEN n.date_liquidation - TRUNC(r.RD_DATE_RECEPTION)
                    END))                                                      AS duree_mediane_2025_j
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID    = d.RD_ID
LEFT JOIN notif                      n ON n.DOS_CODE = d.DOS_CODE
WHERE d.TDOS_CODE = 'V'
GROUP BY NVL(d.DOS_CATEGORIE, '(vide)')
ORDER BY nb_dossiers DESC;


-- =============================================================================
-- Q2. LA DATE DU DÉPÔT
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P49 — Les dates autour du dépôt des dossiers de pension reçus en 2025
-- Question : Q2. La date de réception est-elle celle du dossier complet ?
-- Objectif : comparer la date de réception à la saisie de la réception, au
--            rendez-vous donné, à la saisie des pièces fournies, à l'ouverture
--            du dossier et à la date de demande portée sur la prestation.
--            Une dernière pièce saisie longtemps après la réception veut dire
--            que le dossier a été complété plus tard.
-- Lecture  : si les pièces sont presque toutes saisies le jour de la
--            réception, le guichet ne reçoit que des dossiers complets (Kaya
--            2017) : la date de réception convient (règles 2 et 4). Si
--            beaucoup de pièces arrivent après, la durée de T25 compte l'attente
--            des pièces : il faudra partir de la dernière pièce, ou le signaler.
-- Tableaux : T25.
-- Résultat : 4 671 réceptions de 2025 ; 15 sans pièce ; 7,3 pièces par
--            réception. Dernière pièce saisie : le jour même 4 485 (96 %), 1 à
--            7 j après 111, 8 à 30 j 35, plus de 30 j 25. Réception saisie le
--            jour même (médiane 0 j). Toutes ont un rendez-vous, 45 j après en
--            médiane. 3 755 ont un dossier, ouvert le jour même en médiane ;
--            423 ouverts plus de 30 j après. Date de demande de la prestation =
--            date de réception en médiane (116 avant, 839 après).
-- Constat  : Le dossier est complet à la réception : 96 % des pièces sont
--            saisies le jour même, comme l'a demandé l'atelier de Kaya 2017. La
--            date de réception convient (Q2, règles 2 et 4) ; au plus 171
--            dossiers (4 %) ont été complétés après. Le rendez-vous à 45 jours,
--            la norme CIPRES, est sans doute la date promise pour la décision
--            (à confirmer).
-- -----------------------------------------------------------------------------
WITH pieces AS (
    SELECT RD_ID,
           COUNT(*)                     AS nb_pieces,
           TRUNC(MAX(RDP_DATE_INSERT))  AS derniere_piece
    FROM USER_DWH.RECEPTION_DOS_PIECE
    GROUP BY RD_ID
),
demande AS (
    SELECT DOS_CODE, TRUNC(MIN(PE_DATE_DEMANDE)) AS date_demande
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
base AS (
    SELECT TRUNC(r.RD_DATE_RECEPTION)   AS date_reception,
           TRUNC(r.RD_DATE_INSERT)      AS date_saisie,
           TRUNC(r.RD_DATE_RDV)         AS date_rdv,
           p.nb_pieces,
           p.derniere_piece,
           TRUNC(d.DOS_DATE_OUVERTURE)  AS date_ouverture,
           dm.date_demande
    FROM USER_DWH.RECEPTION_DOSSIER r
    LEFT JOIN pieces           p  ON p.RD_ID     = r.RD_ID
    LEFT JOIN USER_DWH.DOSSIER d  ON d.RD_ID     = r.RD_ID
    LEFT JOIN demande          dm ON dm.DOS_CODE = d.DOS_CODE
    WHERE r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION >= DATE '2025-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
)
SELECT COUNT(*)                                                                AS receptions_2025,
       SUM(CASE WHEN nb_pieces IS NULL THEN 1 ELSE 0 END)                      AS sans_piece_saisie,
       ROUND(AVG(nb_pieces), 1)                                                AS pieces_par_reception,
       SUM(CASE WHEN derniere_piece - date_reception <= 0 THEN 1 ELSE 0 END)   AS pieces_le_jour_meme,
       SUM(CASE WHEN derniere_piece - date_reception BETWEEN 1 AND 7
                THEN 1 ELSE 0 END)                                             AS derniere_piece_1_a_7_j,
       SUM(CASE WHEN derniere_piece - date_reception BETWEEN 8 AND 30
                THEN 1 ELSE 0 END)                                             AS derniere_piece_8_a_30_j,
       SUM(CASE WHEN derniere_piece - date_reception > 30 THEN 1 ELSE 0 END)   AS derniere_piece_plus_30_j,
       ROUND(MEDIAN(date_saisie - date_reception))                             AS med_reception_a_saisie_j,
       SUM(CASE WHEN date_rdv IS NOT NULL THEN 1 ELSE 0 END)                   AS avec_rendez_vous,
       ROUND(MEDIAN(date_rdv - date_reception))                                AS med_reception_a_rdv_j,
       SUM(CASE WHEN date_ouverture IS NOT NULL THEN 1 ELSE 0 END)             AS avec_dossier,
       ROUND(MEDIAN(date_ouverture - date_reception))                          AS med_reception_a_ouverture_j,
       SUM(CASE WHEN date_ouverture - date_reception > 30 THEN 1 ELSE 0 END)   AS ouverture_plus_30_j_apres,
       SUM(CASE WHEN date_demande < date_reception THEN 1 ELSE 0 END)          AS demande_avant_reception,
       SUM(CASE WHEN date_demande > date_reception THEN 1 ELSE 0 END)          AS demande_apres_reception,
       ROUND(MEDIAN(date_demande - date_reception))                            AS med_reception_a_demande_j
FROM base;


-- =============================================================================
-- Q3. LES VIEILLES DEMANDES
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P50 — Stock en instance au 31/12/2025, par ancienneté de la demande
-- Question : Q3. Une demande en attente depuis des années reste-t-elle en
--            instance ?
-- Objectif : compter les demandes de pension en instance au 31/12/2025, avec
--            la règle 6 de T25 (reçue, ni liquidée ni rejetée à cette date,
--            hors coordination), par année de réception, et dire où elles en
--            sont : sans dossier, dossier sans prestation, prestation pas
--            encore notifiée, dossier déjà fermé.
-- Lecture  : si le stock est surtout fait de demandes récentes, pas besoin de
--            limite. S'il est gonflé par des demandes de plus de 2 ou 5 ans,
--            sans dossier ou au dossier fermé, ce sont des abandons ou des
--            refus non saisis : il faut une limite ou une clôture (règles 6 et
--            10). La colonne « sans dossier, vise un dossier » compte les
--            demandes déposées sur un dossier existant (Q4).
-- Tableaux : T25.
-- Résultat : 23 218 demandes en instance au 31/12/2025 avec la règle 6. Par
--            année de réception (en instance / sans dossier / dont visant un
--            dossier / dossier sans prestation / prestation pas notifiée /
--            dossier fermé) : 2015 et avant 14 643 / 13 149 / 1 / 18 / 1 476 /
--            1 465 ; 2016 à 2020 3 646 / 2 328 / 991 / 18 / 1 300 / 1 268 ;
--            2021 832 / 811 / 633 / 4 / 17 / 5 ; 2022 816 / 786 / 601 / 7 /
--            23 / 3 ; 2023 772 / 734 / 524 / 8 / 30 / 2 ; 2024 953 / 848 /
--            589 / 14 / 91 / 4 ; 2025 1 556 / 914 / 614 / 43 / 599 / 4.
--            RD_STATUT E pour 99 %.
-- Constat  : Le stock de la règle 6 est faux : 23 218 demandes, cinq fois les
--            dépôts d'une année, dont 79 % reçues avant 2021. Il mêle trois
--            choses qui ne sont pas des demandes en attente : les réceptions
--            anciennes sans dossier (13 149 avant 2016 : les vieux dossiers
--            n'ont pas de lien vers leur réception, P48) ; 2 751 dossiers
--            fermés dont la pension n'a pas de date de notification (P57) ;
--            depuis 2016, 3 952 réceptions sans dossier qui visent un dossier
--            existant (614 en 2025 : secondes demandes, ou lien noté du seul
--            côté de la réception, P52). Restent 180 à 300 demandes par an sans
--            dossier ni lien depuis 2021, et en 2025 599 pensions pas encore
--            notifiées : le vrai stock. Pour Q3 : sortir les dossiers fermés,
--            rattacher les réceptions par leur DOS_CODE et fixer une date de
--            départ du stock.
-- -----------------------------------------------------------------------------
WITH liq AS (
    SELECT d.DOS_CODE, d.RD_ID, d.DOS_STATUT, d.DOS_CATEGORIE,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))  AS date_liquidation,
           COUNT(pe.PE_ID)                      AS nb_prestations
    FROM USER_DWH.DOSSIER d
    LEFT JOIN USER_DWH.PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
    WHERE d.TDOS_CODE = 'V'
    GROUP BY d.DOS_CODE, d.RD_ID, d.DOS_STATUT, d.DOS_CATEGORIE
),
instance AS (
    SELECT EXTRACT(YEAR FROM r.RD_DATE_RECEPTION) AS annee_reception,
           r.RD_STATUT,
           r.DOS_CODE                             AS dossier_vise,
           l.DOS_CODE,
           l.DOS_STATUT,
           l.nb_prestations
    FROM USER_DWH.RECEPTION_DOSSIER r
    LEFT JOIN liq l ON l.RD_ID = r.RD_ID
    WHERE r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION < DATE '2026-01-01'
      AND NVL(l.DOS_CATEGORIE, 'ORDINAIRE') NOT IN ('COOR_AC', 'CNSS_AC')
      AND (l.date_liquidation IS NULL OR l.date_liquidation >= DATE '2026-01-01')
      AND NOT EXISTS (SELECT 1
                      FROM   USER_DWH.DOSSIER_REJETE x
                      WHERE  x.RD_ID = r.RD_ID
                        AND  x.DOSRE_DATE_REJET < DATE '2026-01-01')
)
SELECT CASE WHEN annee_reception <= 2015 THEN '2015 et avant'
            WHEN annee_reception <= 2020 THEN '2016 à 2020'
            ELSE TO_CHAR(annee_reception) END                                  AS annee_de_reception,
       COUNT(*)                                                                AS en_instance_31_12_2025,
       SUM(CASE WHEN DOS_CODE IS NULL THEN 1 ELSE 0 END)                       AS sans_dossier,
       SUM(CASE WHEN DOS_CODE IS NULL AND dossier_vise IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS sans_dossier_vise_un_dossier,
       SUM(CASE WHEN DOS_CODE IS NOT NULL AND nb_prestations = 0
                THEN 1 ELSE 0 END)                                             AS dossier_sans_prestation,
       SUM(CASE WHEN DOS_CODE IS NOT NULL AND nb_prestations > 0
                THEN 1 ELSE 0 END)                                             AS prestation_pas_notifiee,
       SUM(CASE WHEN DOS_STATUT = 'F' THEN 1 ELSE 0 END)                       AS dossier_ferme,
       SUM(CASE WHEN RD_STATUT = 'E' THEN 1 ELSE 0 END)                        AS rd_statut_e,
       SUM(CASE WHEN NVL(RD_STATUT, '-') <> 'E' THEN 1 ELSE 0 END)             AS rd_statut_autre
FROM instance
GROUP BY CASE WHEN annee_reception <= 2015 THEN '2015 et avant'
              WHEN annee_reception <= 2020 THEN '2016 à 2020'
              ELSE TO_CHAR(annee_reception) END
ORDER BY 1;


-- =============================================================================
-- Q4. LA PENSION DE LA VEUVE
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P51 — Pensions de survivant : nouveau dossier ou dossier du défunt ?
-- Question : Q4. La pension de la veuve est-elle ouverte dans un nouveau
--            dossier ou dans celui du défunt ?
-- Objectif : pour chaque code de survivant, compter les pensions rangées dans
--            un dossier qui porte aussi un droit propre de l'assuré (RPV,
--            RPRA, RPI, RAV), celles notifiées plus de 30 jours après la
--            première notification de leur dossier, et les dossiers de
--            survivants qui ont leur propre réception, déposée après le décès
--            de l'assuré.
-- Lecture  : « dans un dossier avec droit propre » et « notifiée après la 1re
--            notification » élevés → même dossier : la règle 3 prend ces
--            pensions pour des révisions et T25 ne les compte pas. Il faudra
--            compter les survivants par pension (règles 2, 3, 4 et 9). Ces
--            colonnes presque vides, avec une réception après le décès →
--            nouveau dossier : T25 les compte déjà bien.
--            « notifiées en 2025 hors 1re notification » = pensions de 2025
--            que la requête T25 actuelle perdrait.
-- Tableaux : T25, T28, T32 à T36.
-- Résultat : Pensions de survivant (pensions / dans un dossier avec droit
--            propre / notifiées plus de 30 j après la 1re notification du
--            dossier / dossier avec réception / réception après le décès /
--            notifiées en 2025 / dont hors 1re notification) : RPSV 25 541 /
--            13 104 / 5 266 / 2 576 / 888 / 800 / 675 ; RPSE 29 689 / 9 977 /
--            3 448 / 6 190 / 3 005 / 797 / 420 ; RPSF 424 / 183 / 116 / 253 /
--            138 / 40 / 22 ; RPA 81 / 7 / 3 / 44 / 32 / 0 / 0 ; RASO 7 580 /
--            25 / 3 / 5 537 / 3 740 / 494 / 0 ; RASC 1 469 / 12 / 0 / 1 013 /
--            663 / 91 / 0 ; RASA 327 / 0 / 0 / 217 / 122 / 11 / 0 ; RAS 864 /
--            1 / 0 / 0 / 0 / 0 / 0. Assuré avec plusieurs dossiers V : RPSE
--            994, RPSV 698.
-- Constat  : Les deux pratiques existent (Q4). Les pensions de veuve et
--            d'orphelin sont souvent dans le dossier du défunt (RPSV 51 %, RPSE
--            34 %) ; les allocations de survivants ont presque toujours leur
--            propre dossier, déposé après le décès. Explication probable, à
--            faire confirmer : décès d'un pensionné → la pension de réversion
--            s'ajoute à son dossier ; décès d'un assuré pas encore pensionné →
--            nouveau dossier de survivants. En 2025, 675 des 800 pensions de
--            veuve et 420 des 797 pensions d'orphelin notifiées ne sont pas la
--            première notification de leur dossier : la requête T25 proposée
--            (cas A) les perd. Il faut le cas B de t25_q4_deux_cas.sql. Peu de
--            ces dossiers ont une réception (RPSV : 10 %) : le cas B doit
--            prendre la date de demande de la pension quand la réception
--            manque.
-- -----------------------------------------------------------------------------
WITH dossier_v AS (
    SELECT DOS_CODE,
           MAX(CASE WHEN TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV')
                    THEN 1 ELSE 0 END)              AS a_droit_propre,
           TRUNC(MIN(PE_DATE_NOTIFICATION))         AS premiere_notif_dossier
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
assure_v AS (
    SELECT IND_ID, COUNT(*) AS nb_dossiers_v
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'V' AND IND_ID IS NOT NULL
    GROUP BY IND_ID
),
survivant AS (
    SELECT pe.TPE_CODE,
           TRUNC(pe.PE_DATE_NOTIFICATION)  AS date_notif,
           dv.a_droit_propre,
           dv.premiere_notif_dossier,
           av.nb_dossiers_v,
           d.RD_ID,
           r.RD_DATE_RECEPTION,
           i.IND_DATE_DECES
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN      USER_DWH.DOSSIER           d  ON d.DOS_CODE  = pe.DOS_CODE
    JOIN      dossier_v                  dv ON dv.DOS_CODE = pe.DOS_CODE
    LEFT JOIN assure_v                   av ON av.IND_ID   = d.IND_ID
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r  ON r.RD_ID     = d.RD_ID
    LEFT JOIN USER_DWH.INDIVIDU          i  ON i.IND_ID    = d.IND_ID
    WHERE d.TDOS_CODE = 'V'
      AND pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA', 'RAS', 'RASC', 'RASO', 'RASA')
)
SELECT TPE_CODE,
       COUNT(*)                                                                AS nb_pensions,
       SUM(a_droit_propre)                                                     AS dans_dossier_avec_droit_propre,
       SUM(CASE WHEN date_notif > premiere_notif_dossier + 30
                THEN 1 ELSE 0 END)                                             AS notifiee_apres_1re_notif,
       SUM(CASE WHEN nb_dossiers_v > 1 THEN 1 ELSE 0 END)                      AS assure_a_plusieurs_dossiers_v,
       SUM(CASE WHEN RD_ID IS NOT NULL THEN 1 ELSE 0 END)                      AS dossier_avec_reception,
       SUM(CASE WHEN IND_DATE_DECES IS NOT NULL THEN 1 ELSE 0 END)             AS deces_assure_connu,
       SUM(CASE WHEN RD_DATE_RECEPTION >= IND_DATE_DECES THEN 1 ELSE 0 END)    AS reception_apres_deces,
       SUM(CASE WHEN date_notif >= DATE '2025-01-01'
                 AND date_notif <  DATE '2026-01-01' THEN 1 ELSE 0 END)        AS notifiees_2025,
       SUM(CASE WHEN date_notif >= DATE '2025-01-01'
                 AND date_notif <  DATE '2026-01-01'
                 AND date_notif > premiere_notif_dossier + 30
                THEN 1 ELSE 0 END)                                             AS notifiees_2025_hors_1re_notif
FROM survivant
GROUP BY TPE_CODE
ORDER BY nb_pensions DESC;


-- -----------------------------------------------------------------------------
-- P52 — Une réception donne-t-elle un seul dossier ? Vise-t-elle un dossier
--       existant ?
-- Question : Q4, et la vérification BI « une demande = un seul dossier ».
-- Objectif : pour chaque type de réception, compter les réceptions de pension
--            sans dossier ouvert, avec un dossier, avec plusieurs. Pour celles
--            qui n'ont ouvert aucun dossier mais visent un dossier (DOS_CODE de
--            la réception), dire si ce dossier n'a pas de réception (le lien
--            n'est noté que du côté de la réception) ou s'il a été ouvert par
--            une autre réception (seconde demande : veuve, révision, reprise),
--            et s'il était déjà notifié avant la réception.
-- Lecture  : « plusieurs dossiers » > 0 → la règle 2 compterait ces dépôts
--            deux fois ; il faudra garder un seul dossier par réception.
--            « vise un dossier sans réception » → le lien est à lire aussi
--            par le DOS_CODE de la réception (cas A comme cas B).
--            « vise un dossier d'une autre réception » et « déjà notifié » →
--            secondes demandes sur un dossier ouvert : la requête T25 actuelle
--            les laisse sans ligne et en instance pour toujours ; le cas B les
--            rattache. Le type de réception dira lesquelles sont de nouvelles
--            demandes.
--            Première exécution le 30/10/2026 : ORA-00972 (nom de colonne de
--            plus de 30 caractères), noms raccourcis.
-- Tableaux : T25.
-- Résultat : Par type de réception (réceptions / sans dossier ouvert / un
--            dossier / plusieurs / visant un dossier / visant le dossier
--            ouvert / visant un dossier sans réception / visant le dossier
--            d'une autre réception / visant un dossier déjà notifié / en 2025 /
--            en 2025 sur un dossier existant) : NOUVEAU 51 417 / 15 818 /
--            35 588 / 11 / 34 997 / 34 976 / 1 / 0 / 0 / 4 055 / 0 ; ANCIEN
--            4 018 / 3 999 / 19 / 0 / 4 018 / 19 / 2 869 / 1 127 / 3 939 /
--            616 / 616 ; vide 17 / 0 / 17 / 0 / 17 / 17 / 0 / 0 / 0 / 0 / 0.
-- Constat  : Le type de réception tranche. NOUVEAU est une nouvelle demande,
--            qui ouvre son propre dossier : le lien est noté des deux côtés.
--            ANCIEN est une demande déposée sur un dossier existant, presque
--            toujours déjà notifié (98 %), et n'ouvre jamais de dossier. 616
--            réceptions ANCIEN en 2025, à rapprocher des 902 dossiers
--            renotifiés (P54) : ce sont surtout les réversions de Q4, et les
--            3 952 réceptions « qui visent un dossier » du stock de P50. Le cas
--            B les rattache bien par le DOS_CODE de la réception ; 2 869 visent
--            un vieux dossier sans réception. Onze réceptions ont ouvert deux
--            dossiers : garder un seul dossier par réception (règle 2). Reste à
--            savoir si une réception ANCIEN mène toujours à une nouvelle
--            pension (réversion) ou parfois à une simple révision : P58.
-- -----------------------------------------------------------------------------
WITH ouvert AS (
    SELECT RD_ID, COUNT(*) AS nb_dossiers, MIN(DOS_CODE) AS dossier_ouvert
    FROM USER_DWH.DOSSIER
    WHERE RD_ID IS NOT NULL
    GROUP BY RD_ID
),
notif AS (
    SELECT DOS_CODE, TRUNC(MIN(PE_DATE_NOTIFICATION)) AS premiere_notif
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
)
SELECT NVL(TO_CHAR(r.RD_TYPE_RECEPTION), '(vide)')                             AS type_reception,
       COUNT(*)                                                                AS receptions_v,
       SUM(CASE WHEN o.RD_ID IS NULL THEN 1 ELSE 0 END)                        AS sans_dossier_ouvert,
       SUM(CASE WHEN o.nb_dossiers = 1 THEN 1 ELSE 0 END)                      AS un_dossier,
       SUM(CASE WHEN o.nb_dossiers > 1 THEN 1 ELSE 0 END)                      AS plusieurs_dossiers,
       SUM(CASE WHEN r.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END)                 AS vise_un_dossier,
       SUM(CASE WHEN r.DOS_CODE = o.dossier_ouvert THEN 1 ELSE 0 END)          AS vise_le_dossier_ouvert,
       SUM(CASE WHEN o.RD_ID IS NULL AND v.DOS_CODE IS NOT NULL
                 AND v.RD_ID IS NULL THEN 1 ELSE 0 END)                        AS vise_dossier_sans_reception,
       SUM(CASE WHEN o.RD_ID IS NULL AND v.RD_ID IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS vise_dossier_autre_reception,
       SUM(CASE WHEN o.RD_ID IS NULL
                 AND n.premiere_notif < TRUNC(r.RD_DATE_RECEPTION)
                THEN 1 ELSE 0 END)                                             AS vise_dossier_deja_notifie,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS receptions_2025,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
                 AND r.DOS_CODE IS NOT NULL AND o.RD_ID IS NULL
                THEN 1 ELSE 0 END)                                             AS recep_2025_dossier_existant
FROM USER_DWH.RECEPTION_DOSSIER r
LEFT JOIN ouvert           o ON o.RD_ID    = r.RD_ID
LEFT JOIN USER_DWH.DOSSIER v ON v.DOS_CODE = r.DOS_CODE
LEFT JOIN notif            n ON n.DOS_CODE = r.DOS_CODE
WHERE r.TDOS_CODE = 'V'
GROUP BY NVL(TO_CHAR(r.RD_TYPE_RECEPTION), '(vide)')
ORDER BY receptions_v DESC;


-- -----------------------------------------------------------------------------
-- P53 — Qui dépose la demande de pension ? Le lien avec l'assuré
-- Question : Q4.
-- Objectif : regrouper les réceptions de pension par lien du déposant avec
--            l'assuré (lui-même, conjoint, enfant…), pour voir si les
--            demandes des veuves ouvrent un dossier ou visent celui du défunt.
--            Champ libre : seules les valeurs vues au moins 5 fois sortent.
-- Lecture  : les demandes des conjoints « sans dossier ouvert » et « sur un
--            dossier existant » sont celles que T25 perdrait (Q4, réponse
--            « même dossier »). Si elles ouvrent presque toutes un dossier,
--            la réponse « nouveau dossier » se confirme.
-- Tableaux : T25.
-- Résultat : Lien du déposant (réceptions / en 2025 / sans dossier ouvert / sur
--            un dossier existant) : L 53 648 / 4 531 / 18 407 / 3 351 ; V
--            1 384 / 121 / 1 153 / 620 ; T 267 / 5 / 163 / 17 ; A 86 / 2 / 58 /
--            1 ; F 62 / 7 / 32 / 18 ; AUTRE 5 / 5 / 4 / 4.
-- Constat  : Le lien est un code sans libellé : L sans doute l'assuré lui-même,
--            V le conjoint, T un tuteur, F un enfant, A un ascendant (à faire
--            confirmer par la DSI). Les demandes de conjoint n'ouvrent presque
--            jamais de dossier (83 %) et 620 visent un dossier existant : la
--            veuve dépose sur le dossier du défunt. Mais il n'y a que 121
--            réceptions V en 2025 pour 800 pensions de veuve notifiées : la
--            plupart des réversions n'ont pas de réception à ce titre, ou sont
--            saisies sous L. Correction après P58 : L domine aussi pour les
--            réversions, alors que l'assuré est décédé ; L est donc plutôt « le
--            demandeur lui-même » (la veuve qui dépose pour elle), et les
--            réversions ont bien une réception, de type ANCIEN.
-- -----------------------------------------------------------------------------
WITH ouvert AS (
    SELECT RD_ID, COUNT(*) AS nb_dossiers
    FROM USER_DWH.DOSSIER
    WHERE RD_ID IS NOT NULL
    GROUP BY RD_ID
)
SELECT *
FROM (
    SELECT NVL(SUBSTR(UPPER(TRIM(r.RD_LIEN_AVEC_ASS)), 1, 40), '(vide)')       AS lien_avec_assure,
           COUNT(*)                                                            AS receptions_v,
           SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                     AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
                    THEN 1 ELSE 0 END)                                         AS receptions_2025,
           SUM(CASE WHEN o.RD_ID IS NULL THEN 1 ELSE 0 END)                    AS sans_dossier_ouvert,
           SUM(CASE WHEN o.RD_ID IS NULL AND r.DOS_CODE IS NOT NULL
                    THEN 1 ELSE 0 END)                                         AS sur_un_dossier_existant
    FROM USER_DWH.RECEPTION_DOSSIER r
    LEFT JOIN ouvert o ON o.RD_ID = r.RD_ID
    WHERE r.TDOS_CODE = 'V'
    GROUP BY NVL(SUBSTR(UPPER(TRIM(r.RD_LIEN_AVEC_ASS)), 1, 40), '(vide)')
    HAVING COUNT(*) >= 5
    ORDER BY receptions_v DESC
)
WHERE ROWNUM <= 40;


-- -----------------------------------------------------------------------------
-- P54 — Dossiers de pension notifiés plusieurs fois
-- Question : Q4, et la vérification BI « combien de dossiers ont été notifiés
--            plusieurs fois ? ».
-- Objectif : répartir les dossiers de pension par nombre de dates de
--            notification de leurs prestations ; mesurer l'écart entre la
--            première et la dernière ; voir à laquelle correspond
--            DOS_DATE_NOTIFICATION, et combien de lettres de notification
--            (NOTIFICATION_PREST) chaque dossier a reçues.
-- Lecture  : la règle 3 garde la première notification. Les dossiers
--            « renotifiés en 2025, liquidés avant » sont des révisions, ou
--            des pensions de survivant ajoutées au dossier (Q4) : T25 ne les
--            compte pas en 2025. S'ils sont nombreux et que P51 montre des
--            survivants dans le dossier du défunt, la règle 3 doit changer.
-- Tableaux : T25.
-- Résultat : Dossiers V notifiés (1 date / 2 dates / 3 ou plus) : 54 744 /
--            5 968 / 158. Écart médian entre la première et la dernière
--            notification : 0 / 3 356 j / 665 j ; plus d'un an : 0 / 4 705 /
--            98. DOS_DATE_NOTIFICATION vide pour tous. Lettres de notification
--            par dossier : 1,4 / 2 / 3,3. Renotifiés en 2025 après une
--            liquidation antérieure : 859 + 43 = 902.
-- Constat  : Une seconde notification arrive en médiane 9 ans après la
--            première : ce sont des réversions (P51), pas des révisions. Les
--            902 dossiers renotifiés en 2025 ne comptent pas en 2025 avec la
--            règle 3 ; 3 901 (P48) + 902 ≈ 4 783, le chiffre de P09, qui
--            prenait la dernière notification. Le cas B (une demande par ligne)
--            les retrouve. DOS_DATE_NOTIFICATION est inutilisable pour les
--            pensions.
-- -----------------------------------------------------------------------------
WITH dates AS (
    SELECT pe.DOS_CODE,
           COUNT(DISTINCT TRUNC(pe.PE_DATE_NOTIFICATION))  AS nb_dates,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))             AS premiere,
           TRUNC(MAX(pe.PE_DATE_NOTIFICATION))             AS derniere
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'V'
      AND pe.PE_DATE_NOTIFICATION IS NOT NULL
    GROUP BY pe.DOS_CODE
),
lettres AS (
    SELECT DOS_CODE, COUNT(*) AS nb_lettres
    FROM USER_DWH.NOTIFICATION_PREST
    WHERE TDOS_CODE = 'V'
    GROUP BY DOS_CODE
)
SELECT CASE WHEN dt.nb_dates = 1 THEN '1 date'
            WHEN dt.nb_dates = 2 THEN '2 dates'
            ELSE '3 dates ou plus' END                                         AS notifications_du_dossier,
       COUNT(*)                                                                AS dossiers_v,
       ROUND(MEDIAN(dt.derniere - dt.premiere))                                AS med_ecart_1re_derniere_j,
       SUM(CASE WHEN dt.derniere - dt.premiere > 365 THEN 1 ELSE 0 END)        AS ecart_plus_d_un_an,
       SUM(CASE WHEN TRUNC(d.DOS_DATE_NOTIFICATION) = dt.premiere
                THEN 1 ELSE 0 END)                                             AS dos_date_notif_egale_1re,
       SUM(CASE WHEN TRUNC(d.DOS_DATE_NOTIFICATION) = dt.derniere
                 AND dt.derniere <> dt.premiere THEN 1 ELSE 0 END)             AS dos_date_notif_egale_derniere,
       SUM(CASE WHEN d.DOS_DATE_NOTIFICATION IS NULL THEN 1 ELSE 0 END)        AS dos_date_notif_vide,
       ROUND(AVG(l.nb_lettres), 1)                                             AS lettres_par_dossier,
       SUM(CASE WHEN dt.premiere <  DATE '2025-01-01'
                 AND dt.derniere >= DATE '2025-01-01'
                 AND dt.derniere <  DATE '2026-01-01' THEN 1 ELSE 0 END)       AS renotifies_2025_liquides_avant
FROM dates dt
JOIN      USER_DWH.DOSSIER d ON d.DOS_CODE = dt.DOS_CODE
LEFT JOIN lettres          l ON l.DOS_CODE = dt.DOS_CODE
GROUP BY CASE WHEN dt.nb_dates = 1 THEN '1 date'
              WHEN dt.nb_dates = 2 THEN '2 dates'
              ELSE '3 dates ou plus' END
ORDER BY 1;

-- -----------------------------------------------------------------------------
-- P58 — Réceptions « ANCIEN » : que deviennent-elles ?
-- Question : Q3 et Q4. Ajoutée après P52, qui a montré que le type de
--            réception ANCIEN marque une demande déposée sur un dossier
--            existant.
-- Objectif : pour ces demandes, par lien du déposant avec l'assuré, dire ce
--            qui suit la réception dans le dossier : une pension de survivant
--            notifiée après (réversion), une autre prestation notifiée après
--            (droit propre, complément, révision), une prestation sans date de
--            notification, ou rien.
-- Lecture  : si presque toutes mènent à une pension de survivant, une
--            réception ANCIEN est le dépôt d'une réversion : le cas B la compte
--            en dépôt sur la ligne 4. Celles suivies d'une autre notification
--            sont des révisions ou des compléments : faut-il les compter comme
--            dossiers déposés ? (question au métier). Celles sans suite restent
--            en instance : vrai stock, ou vieilles demandes à clore (Q3).
-- Tableaux : T25.
-- Résultat : Réceptions ANCIEN par lien (réceptions / pension de survivant
--            notifiée après / autre prestation notifiée après / prestation sans
--            date de notification / rien / en 2025 / dont survivant / dont
--            autre / dont sans suite) : L 3 342 / 3 172 / 3 / 117 / 50 / 529 /
--            478 / 0 / 51 ; V 635 / 568 / 0 / 52 / 15 / 76 / 53 / 0 / 23 ; F
--            18 / 14 / 0 / 4 / 0 / 4 / 2 / 0 / 2 ; T 18 / 15 / 0 / 2 / 1 / 3 /
--            3 / 0 / 0 ; AUTRE 4 / 2 / 0 / 1 / 1 / 4 / 2 / 0 / 2 ; A 1 / 0 /
--            0 / 0 / 1 / 0 / 0 / 0 / 0. Total : 4 018, dont 3 771 suivies d'une
--            pension de survivant (94 %), 3 d'une autre prestation, 176 d'une
--            prestation sans date de notification, 68 de rien. En 2025 : 616,
--            dont 538 déjà suivies d'une pension de survivant et 78 sans suite.
-- Constat  : Une réception ANCIEN est le dépôt d'une demande de réversion :
--            94 % sont suivies d'une pension de survivant dans le dossier visé,
--            presque jamais d'une révision. Le cas B la compte comme un dépôt
--            sur la ligne de la pension de survivant (ligne 4). Les 78 sans
--            suite de 2025 sont des demandes en cours : le vrai stock. Le lien
--            L domine aussi ici, alors que l'assuré est décédé : L veut sans
--            doute dire « le demandeur lui-même » (l'assuré pour sa pension, la
--            veuve pour sa réversion), pas « l'assuré » (correction de P53, à
--            faire confirmer par la DSI).
-- -----------------------------------------------------------------------------
WITH prest AS (
    SELECT DOS_CODE,
           CASE WHEN TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA',
                                  'RAS', 'RASC', 'RASO', 'RASA')
                THEN 1 ELSE 0 END                               AS survivant,
           TRUNC(PE_DATE_NOTIFICATION)                          AS date_notif
    FROM USER_DWH.PRESTATION_ESP
),
ancien AS (
    SELECT r.RD_ID,
           r.DOS_CODE,
           TRUNC(r.RD_DATE_RECEPTION)                           AS date_reception,
           CASE WHEN TRIM(r.RD_LIEN_AVEC_ASS) IN ('L', 'V', 'T', 'A', 'F')
                THEN TRIM(r.RD_LIEN_AVEC_ASS) ELSE 'AUTRE' END  AS lien
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE = 'V'
      AND r.RD_TYPE_RECEPTION = 'ANCIEN'
),
suite AS (
    SELECT a.RD_ID, a.lien, a.date_reception,
           MAX(CASE WHEN p.survivant = 1 AND p.date_notif >= a.date_reception
                    THEN 1 ELSE 0 END)                          AS survivant_apres,
           MAX(CASE WHEN p.survivant = 0 AND p.date_notif >= a.date_reception
                    THEN 1 ELSE 0 END)                          AS autre_apres,
           MAX(CASE WHEN p.DOS_CODE IS NOT NULL AND p.date_notif IS NULL
                    THEN 1 ELSE 0 END)                          AS sans_notif
    FROM ancien a
    LEFT JOIN prest p ON p.DOS_CODE = a.DOS_CODE
    GROUP BY a.RD_ID, a.lien, a.date_reception
)
SELECT lien,
       COUNT(*)                                                 AS receptions_ancien,
       SUM(survivant_apres)                                     AS survivant_notifie_apres,
       SUM(CASE WHEN survivant_apres = 0 AND autre_apres = 1
                THEN 1 ELSE 0 END)                              AS autre_notifiee_apres,
       SUM(CASE WHEN survivant_apres = 0 AND autre_apres = 0
                 AND sans_notif = 1 THEN 1 ELSE 0 END)          AS prestation_sans_notification,
       SUM(CASE WHEN survivant_apres = 0 AND autre_apres = 0
                 AND sans_notif = 0 THEN 1 ELSE 0 END)          AS rien_apres,
       SUM(CASE WHEN date_reception >= DATE '2025-01-01'
                 AND date_reception <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                              AS receptions_2025,
       SUM(CASE WHEN date_reception >= DATE '2025-01-01'
                 AND date_reception <  DATE '2026-01-01'
                 AND survivant_apres = 1 THEN 1 ELSE 0 END)     AS survivant_2025,
       SUM(CASE WHEN date_reception >= DATE '2025-01-01'
                 AND date_reception <  DATE '2026-01-01'
                 AND survivant_apres = 0 AND autre_apres = 1
                THEN 1 ELSE 0 END)                              AS autre_2025,
       SUM(CASE WHEN date_reception >= DATE '2025-01-01'
                 AND date_reception <  DATE '2026-01-01'
                 AND survivant_apres = 0 AND autre_apres = 0
                THEN 1 ELSE 0 END)                              AS sans_suite_2025
FROM suite
GROUP BY lien
ORDER BY receptions_ancien DESC;


-- =============================================================================
-- Q5. LES REFUS
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P55 — Statut de la réception face au dossier et au rejet
-- Question : Q5. Les refus sont-ils enregistrés ailleurs que dans le module
--            des rejets ?
-- Objectif : pour chaque valeur de RD_STATUT (E, I, L, R, A, vide), compter
--            les réceptions de pension avec un rejet, sans dossier ni rejet,
--            avec un dossier notifié, avec un dossier fermé sans notification.
-- Lecture  : si une valeur (R ?) va avec « ni dossier ni rejet », c'est
--            peut-être là que le guichet note les refus : la colonne
--            « rejetés » de T25 pourrait la lire (règle 7), et ces demandes
--            sortiraient du stock en instance (règle 6). À faire confirmer
--            par la DSI avant tout usage.
-- Tableaux : T25.
-- Résultat : RD_STATUT des réceptions V (réceptions / en 2025 / avec rejet / ni
--            dossier ni rejet / dossier notifié / dossier fermé sans
--            notification / dossier actif sans notification) : E 55 228 /
--            4 601 / 141 / 19 579 / 30 922 / 2 754 / 1 953 ; A 111 / 70 / 2 /
--            109 / 0 / 0 / 0, depuis le 26/05/2025 ; I 62 / 0 / 0 / 58 / 4 /
--            0 / 0, de 2005 à 2021 ; R 47 / 0 / 4 / 42 / 1 / 1 / 0, de 2005 à
--            2023 ; L 2 ; vide 2. Plus ancienne réception E affichée 27/01/21 :
--            une date fausse, antérieure à 2005.
-- Constat  : RD_STATUT ne note pas les refus : R ne concerne que 47 réceptions
--            anciennes. A est récent (depuis mai 2025) et toujours sans
--            dossier : « en attente » ou « annulée » ? (DSI). E est le cas
--            normal, la réception enregistrée.
-- -----------------------------------------------------------------------------
WITH dos AS (
    SELECT d.RD_ID,
           MAX(CASE WHEN pe.PE_DATE_NOTIFICATION IS NOT NULL THEN 1 ELSE 0 END)  AS notifie,
           MAX(CASE WHEN d.DOS_STATUT = 'F' THEN 1 ELSE 0 END)                   AS ferme
    FROM USER_DWH.DOSSIER d
    LEFT JOIN USER_DWH.PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
    WHERE d.RD_ID IS NOT NULL
    GROUP BY d.RD_ID
),
rej AS (
    SELECT DISTINCT RD_ID
    FROM USER_DWH.DOSSIER_REJETE
    WHERE RD_ID IS NOT NULL
)
SELECT NVL(TO_CHAR(r.RD_STATUT), '(vide)')                                     AS rd_statut,
       COUNT(*)                                                                AS receptions_v,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS receptions_2025,
       SUM(CASE WHEN x.RD_ID IS NOT NULL THEN 1 ELSE 0 END)                    AS avec_rejet,
       SUM(CASE WHEN o.RD_ID IS NULL AND x.RD_ID IS NULL THEN 1 ELSE 0 END)    AS ni_dossier_ni_rejet,
       SUM(CASE WHEN o.notifie = 1 THEN 1 ELSE 0 END)                          AS dossier_notifie,
       SUM(CASE WHEN o.notifie = 0 AND o.ferme = 1 THEN 1 ELSE 0 END)          AS dossier_ferme_sans_notif,
       SUM(CASE WHEN o.notifie = 0 AND o.ferme = 0 THEN 1 ELSE 0 END)          AS dossier_actif_sans_notif,
       MIN(r.RD_DATE_RECEPTION)                                                AS reception_min,
       MAX(r.RD_DATE_RECEPTION)                                                AS reception_max
FROM USER_DWH.RECEPTION_DOSSIER r
LEFT JOIN dos o ON o.RD_ID = r.RD_ID
LEFT JOIN rej x ON x.RD_ID = r.RD_ID
WHERE r.TDOS_CODE = 'V'
GROUP BY NVL(TO_CHAR(r.RD_STATUT), '(vide)')
ORDER BY receptions_v DESC;


-- -----------------------------------------------------------------------------
-- P56 — Motifs des rejets enregistrés
-- Question : Q5.
-- Objectif : voir ce que contient le module des rejets (486 lignes, P01) :
--            refus au guichet (pièce manquante) ou refus du droit (conditions
--            non remplies). Champ libre : seuls les motifs vus au moins 3
--            fois sortent.
-- Lecture  : si les motifs sont des pièces manquantes, le module sert au
--            guichet, pas aux refus de pension : la colonne « rejetés » de T25
--            devra chercher les refus du droit ailleurs (P55, P57).
-- Tableaux : T25 à T27.
-- Résultat : Motifs vus au moins 3 fois (rejets) : V : AUTRE 75, PRESCRIPTION
--            58, MANQUE_BENEFICIAIRE 10, PJ_NON_CONFORME 3 ; F : AUTRE 131,
--            PRESCRIPTION 110, PJ_NON_CONFORME 23 ; M : AUTRE 22, PRESCRIPTION
--            20, PJ_NON_CONFORME 6 ; A : AUTRE 19, PRESCRIPTION 3. Tous ont une
--            réception ; 112 des 146 rejets V ont un dossier. Rejets V du
--            06/10/2017 au 20/01/2026.
-- Constat  : Le motif est un code. Le module des rejets enregistre de vrais
--            refus du droit (prescription, pas de bénéficiaire), pas seulement
--            des pièces non conformes. Mais il sert peu : 146 rejets de pension
--            depuis 2017, une quinzaine par an (Q5). « AUTRE » domine : le
--            motif réel est sans doute dans la remarque, non lue ici.
-- -----------------------------------------------------------------------------
SELECT *
FROM (
    SELECT TDOS_CODE,
           NVL(SUBSTR(UPPER(TRIM(DOSRE_MOTIF)), 1, 60), '(vide)')              AS motif,
           COUNT(*)                                                            AS nb_rejets,
           SUM(CASE WHEN DOS_CODE IS NOT NULL THEN 1 ELSE 0 END)               AS avec_dossier,
           SUM(CASE WHEN RD_ID IS NOT NULL THEN 1 ELSE 0 END)                  AS avec_reception,
           MIN(DOSRE_DATE_REJET)                                               AS premier_rejet,
           MAX(DOSRE_DATE_REJET)                                               AS dernier_rejet
    FROM USER_DWH.DOSSIER_REJETE
    GROUP BY TDOS_CODE, NVL(SUBSTR(UPPER(TRIM(DOSRE_MOTIF)), 1, 60), '(vide)')
    HAVING COUNT(*) >= 3
    ORDER BY nb_rejets DESC
)
WHERE ROWNUM <= 30;


-- -----------------------------------------------------------------------------
-- P57 — Dossiers de pension fermés sans aucune notification
-- Question : Q5.
-- Objectif : par année d'ouverture, compter les dossiers de pension au statut
--            F (fermé) dont aucune prestation n'a été notifiée : sans
--            prestation, ou avec une prestation jamais notifiée. Un dossier
--            fermé sans décision notifiée est peut-être un refus ou un
--            abandon enregistré par la fermeture.
-- Lecture  : s'ils sont nombreux, ce sont des candidats pour la colonne
--            « rejetés » (règle 7), et ils doivent sortir du stock en
--            instance (règle 6). La DSI dira si la fermeture vaut refus.
-- Tableaux : T25.
-- Résultat : 13 951 dossiers V fermés, le total de P02. Par période d'ouverture
--            (fermés / avec notification / prestation jamais notifiée / avec
--            radiation sans notification) : avant 2010 6 145 / 3 107 / 2 968 /
--            3 024, dont 14 aux dates d'ouverture fausses (20, 198, 1010…) ;
--            2010 à 2017 5 738 / 662 / 5 060 / 5 076 ; 2018 à 2025 2 068 /
--            1 991 / 65 / 65. Sans prestation : 98. Avec rejet : 7. Détail par
--            année : resultats/lot3b_P57_dossiers_fermes_sans_notification.tsv.
-- Constat  : Fermé sans notification ne veut pas dire refusé : ces dossiers
--            datent d'avant 2018 et ont presque tous une date de radiation. Ce
--            sont des pensions servies puis terminées dont la date de
--            notification n'a pas été saisie (environ 8 000). Depuis 2018,
--            presque tous les dossiers fermés ont une notification. Pour T25 :
--            sortir ces dossiers du stock en instance (P50), ne pas les compter
--            comme rejetés (Q5). P02 disait que les dossiers V fermés avaient
--            tous été ouverts après 2020 : c'est faux, 85 % l'ont été avant
--            2018.
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DOS_CODE,
           COUNT(*)                   AS nb_prestations,
           MIN(PE_DATE_NOTIFICATION)  AS premiere_notif
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
rej AS (
    SELECT DISTINCT DOS_CODE
    FROM USER_DWH.DOSSIER_REJETE
    WHERE DOS_CODE IS NOT NULL
)
SELECT EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)                                 AS annee_ouverture,
       COUNT(*)                                                                AS dossiers_v_fermes,
       SUM(CASE WHEN n.premiere_notif IS NOT NULL THEN 1 ELSE 0 END)           AS avec_notification,
       SUM(CASE WHEN n.DOS_CODE IS NULL THEN 1 ELSE 0 END)                     AS sans_prestation,
       SUM(CASE WHEN n.DOS_CODE IS NOT NULL AND n.premiere_notif IS NULL
                THEN 1 ELSE 0 END)                                             AS prestation_jamais_notifiee,
       SUM(CASE WHEN n.premiere_notif IS NULL AND x.DOS_CODE IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS sans_notif_avec_rejet,
       SUM(CASE WHEN n.premiere_notif IS NULL AND d.DOS_DATE_RADIATION IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS sans_notif_avec_radiation
FROM USER_DWH.DOSSIER d
LEFT JOIN notif n ON n.DOS_CODE = d.DOS_CODE
LEFT JOIN rej   x ON x.DOS_CODE = d.DOS_CODE
WHERE d.TDOS_CODE  = 'V'
  AND d.DOS_STATUT = 'F'
GROUP BY EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)
ORDER BY 1;
