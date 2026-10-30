-- =============================================================================
-- LOT 1 — Inventaire des données de prestations (T25 à T57)
-- Rédigé le 26/10/2026, exécuté le 26/10/2026 (P19 abandonnée).
-- Partie A (P01 à P16) : sur la base source, schéma USER_DWH (celui que lit
--   l'ETL des prestations, malgré son nom). On lit les tables sources et leurs
--   nomenclatures, au cas où certaines tables du DWH seraient vides.
-- Partie B (P17 à P19) : sur le DWH, avec la connexion du DWH, pour comparer
--   les faits chargés à la source et voir ce que publie le DTM.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement
-- (aucune donnée nominative).
-- Pourquoi ce lot : avant de vérifier les requêtes DTM du domaine, il faut
-- connaître les codes (branches, types de prestation, statuts), les volumes
-- et la forme des données. Les requêtes vérifient aussi les hypothèses H1 à H14
-- de reference_cipres_T25_T57.md.
-- Numérotation : P01, P02… (P pour prestations), sans réutiliser un numéro.
-- =============================================================================


-- =============================================================================
-- PARTIE A — BASE SOURCE (USER_DWH)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P01 — Volumes des tables sources et date de la dernière saisie
-- Objectif : connaître la taille de chaque table et savoir jusqu'à quelle date
--            la base est alimentée (copie figée ou base vivante ?).
-- Tableaux : tous.
-- Résultat : Copie arrêtée au 02/02/2026 : dernière saisie le 02/02/26 pour 10
--            tables. DEBOURS 71 709 877 lignes, EFFET 8 711 190, ETAPE 1 705
--            341, INDIVIDU 1 543 286, AJUSTEMENT 1 355 405, LIEN 1 308 962,
--            PRESTATION_ESP 947 953, DOSSIER 367 947, NOTIFICATION_PREST 304
--            355, RECEPTION_DOSSIER 273 005, TSINISTRE 25 086 (dernier
--            accident le 30/12/24), DOSSIERS_CONTROLES 1 542 (dernière saisie
--            le 09/07/25), DOSSIER_REJETE 486 (20/01/26).
-- Constat  : Base figée au 02/02/2026, comme USER_GRH (18/02/2026). Plus aucun
--            contrôle a posteriori saisi depuis juillet 2025 ; TSINISTRE n'est
--            plus alimentée depuis fin 2024.
-- -----------------------------------------------------------------------------
SELECT 'RECEPTION_DOSSIER' AS table_source, COUNT(*) AS nb_lignes, MAX(RD_DATE_INSERT) AS derniere_saisie
FROM USER_DWH.RECEPTION_DOSSIER
UNION ALL
SELECT 'DOSSIER', COUNT(*), MAX(DOS_DATE_INSERT) FROM USER_DWH.DOSSIER
UNION ALL
SELECT 'DOSSIER_REJETE', COUNT(*), MAX(DOSRE_DATE_INSERT) FROM USER_DWH.DOSSIER_REJETE
UNION ALL
SELECT 'PRESTATION_ESP', COUNT(*), MAX(PE_DATE_INSERT) FROM USER_DWH.PRESTATION_ESP
UNION ALL
SELECT 'NOTIFICATION_PREST', COUNT(*), MAX(NOT_DATE_INSERT) FROM USER_DWH.NOTIFICATION_PREST
UNION ALL
SELECT 'ETAPE', COUNT(*), MAX(EA_DATE_INSERT) FROM USER_DWH.ETAPE
UNION ALL
SELECT 'DEBOURS', COUNT(*), MAX(DEB_DATE_INSERT) FROM USER_DWH.DEBOURS
UNION ALL
SELECT 'EFFET', COUNT(*), MAX(EFP_DATE_INSERT) FROM USER_DWH.EFFET
UNION ALL
SELECT 'AJUSTEMENT', COUNT(*), MAX(AJ_DATE_INSERT) FROM USER_DWH.AJUSTEMENT
UNION ALL
SELECT 'DOSSIERS_CONTROLES', COUNT(*), MAX(DC_DATE_INSERT) FROM USER_DWH.DOSSIERS_CONTROLES
UNION ALL
SELECT 'INDIVIDU', COUNT(*), MAX(IND_DATE_INSERT) FROM USER_DWH.INDIVIDU
UNION ALL
SELECT 'LIEN', COUNT(*), MAX(LN_DATE_INSERT) FROM USER_DWH.LIEN
UNION ALL
SELECT 'TSINISTRE', COUNT(*), MAX(DOS_DATE_ACCIDENT) FROM USER_DWH.TSINISTRE;


-- -----------------------------------------------------------------------------
-- P02 — Types de dossier, branches et statuts
-- Objectif : lire le libellé de chaque type de dossier (V, A, F, M…), sa
--            branche, et la répartition de DOS_STATUT (H4, H5).
-- Tableaux : T25 à T27, T56, T57.
-- Résultat : 4 types : A AT/MP (branche RP), F prestations familiales (PF), M
--            prestations de maternité (PF), V vieillesse et retraite (PE).
--            Statut A : A 31 544, F 156 245, M 9 905, V 61 632. Statut F : A 1
--            634, F 46 492, M 46 544, V 13 951. Les dossiers V au statut F ont
--            tous été ouverts après le 31/01/2020 (faux : 85 % l'ont été
--            avant 2018, voir P57).
-- Constat  : M = maternité, rangée dans la branche PF (H5 tranchée). 61 632
--            dossiers de vieillesse au statut A : A ne peut pas vouloir dire «
--            en instance ». A = dossier actif, F = dossier fermé (H4, à
--            confirmer par la DSI).
-- -----------------------------------------------------------------------------
SELECT d.TDOS_CODE, t.TDOS_DESCRIPTION, t.BR_CODE, b.BR_DESCRIPTION, d.DOS_STATUT,
       COUNT(*) AS nb_dossiers,
       MIN(d.DOS_DATE_OUVERTURE) AS ouverture_min,
       MAX(d.DOS_DATE_OUVERTURE) AS ouverture_max
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.TYPE_DOSSIER t ON t.TDOS_CODE = d.TDOS_CODE
LEFT JOIN USER_DWH.BRANCHE      b ON b.BR_CODE   = t.BR_CODE
GROUP BY d.TDOS_CODE, t.TDOS_DESCRIPTION, t.BR_CODE, b.BR_DESCRIPTION, d.DOS_STATUT
ORDER BY d.TDOS_CODE, d.DOS_STATUT;


-- -----------------------------------------------------------------------------
-- P03 — Catégories de dossier de pension
-- Objectif : voir si la catégorie du dossier distingue vieillesse, invalidité,
--            survivants, ascendants… (lignes de T25, T28 et T32 à T36).
-- Tableaux : T25, T28, T32 à T36, T42 à T44.
-- Résultat : La catégorie ne distingue pas le type de pension : ORDINAIRE,
--            COOR_CNSS (coordination payée par la CNSS), COOR_AC (coordination
--            payée par une autre caisse : 3 326 dossiers V), CNSS_AC
--            (transféré à une autre caisse : 158 dossiers V). Renseignée pour
--            44 809 des 75 583 dossiers V.
-- Constat  : Le type de pension vient du code de prestation (P04). Question :
--            les dossiers payés par une autre caisse ou transférés entrent-ils
--            dans les tableaux de la CNSS ?
-- -----------------------------------------------------------------------------
SELECT d.TDOS_CODE, d.DOS_CATEGORIE, c.CDP_DESCRIPTION, d.DOS_STATUT,
       COUNT(*) AS nb_dossiers
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.CATEGORIE_DOSSIER_PENSION c ON c.DOS_CATEGORIE = d.DOS_CATEGORIE
WHERE d.DOS_CATEGORIE IS NOT NULL
GROUP BY d.TDOS_CODE, d.DOS_CATEGORIE, c.CDP_DESCRIPTION, d.DOS_STATUT
ORDER BY d.TDOS_CODE, d.DOS_CATEGORIE, d.DOS_STATUT;


-- -----------------------------------------------------------------------------
-- P04 — Types de prestation en espèces
-- Objectif : liste complète des codes de prestation en espèces, avec leur
--            branche et leur fréquence de paiement, le volume de prestations
--            ouvertes et les paiements de 2025. Base de la table de
--            correspondance vers les lignes CIPRES.
-- Tableaux : T25 à T57.
-- Résultat : 28 codes. V : RPV pension de vieillesse (46 836 prestations ;
--            2025 : 377 938 débours, 41,9 Md), RPRA retraite anticipée, RPI
--            invalidité, RPSV et RPSF veuve et veuf, RPSE orphelin, RPA
--            ascendant, RAV allocation de vieillesse, RAS, RASC, RASO, RASA
--            allocation de survivants et ses parts, accessoires RBEC, RME,
--            RME-COL, RCCM, RRC. A : ARI rente viagère, ARSV, ARSF, ARSE, ARSA
--            rentes de survivants, AIJ indemnités journalières, AAI allocation
--            d'incapacité. F : FAF allocations familiales (614 811 prestations
--            ; 2025 : 3,2 M débours, 8,0 Md), FAP allocation prénatale. M :
--            MAP allocation prénatale, MIJ indemnités journalières. Paiement
--            trimestriel pour FAF et FAP, mensuel pour les autres.
-- Constat  : Chaque ligne CIPRES se rattache à des codes (correspondance
--            proposée dans le README). Deux codes d'allocation prénatale : FAP
--            (branche F) et MAP (branche M).
-- -----------------------------------------------------------------------------
WITH pe_agg AS (
    SELECT TPE_CODE, COUNT(*) AS nb_prestations,
           COUNT(DISTINCT DOS_CODE) AS nb_dossiers,
           COUNT(DISTINCT IND_ID_BENEF) AS nb_beneficiaires
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY TPE_CODE
),
deb_agg AS (
    SELECT TPE_CODE, COUNT(*) AS nb_debours_2025, SUM(DEB_MONTANT) AS montant_2025
    FROM USER_DWH.DEBOURS
    WHERE TPE_CODE IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
    GROUP BY TPE_CODE
),
codes AS (
    SELECT TPE_CODE FROM pe_agg
    UNION
    SELECT TPE_CODE FROM deb_agg
)
SELECT c.TPE_CODE, t.TPE_DESCRIPTION, t.TDOS_CODE, t.TPE_FREQ_PAIEMENT,
       p.nb_prestations, p.nb_dossiers, p.nb_beneficiaires,
       db.nb_debours_2025, db.montant_2025
FROM codes c
LEFT JOIN USER_DWH.TYPE_PRESTATION_ESP t ON t.TPE_CODE  = c.TPE_CODE
LEFT JOIN pe_agg                       p  ON p.TPE_CODE  = c.TPE_CODE
LEFT JOIN deb_agg                      db ON db.TPE_CODE = c.TPE_CODE
ORDER BY t.TDOS_CODE, c.TPE_CODE;


-- -----------------------------------------------------------------------------
-- P05 — Prestations en nature payées en 2025
-- Objectif : savoir ce que recouvrent les remboursements (prestations en
--            nature) et à quelle branche ils appartiennent, notamment la
--            branche M (maladie ou maternité ? — H5).
-- Tableaux : T27, T30, T56.
-- Résultat : Prestations en nature payées en 2025 : uniquement en AT/MP, 826
--            débours pour 23,9 M FCFA (pharmacie, enquête, consultation,
--            radio, examens…).
-- Constat  : Aucune prestation en nature en branche M : M n'est pas « maladie
--            » (H5).
-- -----------------------------------------------------------------------------
SELECT deb.TPN_CODE, n.TPN_DESCRIPTION, n.CPN_CODE, cp.CPN_DESCRIPTION,
       cp.TDOS_CODE AS tdos_categorie, n.CODE_BRANCHE AS branche_du_type,
       deb.CODE_BRANCHE AS branche_du_debours,
       COUNT(*) AS nb_debours, COUNT(DISTINCT deb.DOS_CODE) AS nb_dossiers,
       SUM(deb.DEB_MONTANT) AS montant
FROM USER_DWH.DEBOURS deb
LEFT JOIN USER_DWH.TYPE_PRESTATION_NAT      n  ON n.TPN_CODE  = deb.TPN_CODE
LEFT JOIN USER_DWH.CATEGORIE_PRESTATION_NAT cp ON cp.CPN_CODE = n.CPN_CODE
WHERE deb.TPN_CODE IS NOT NULL
  AND deb.DEB_DATE_EFFET >= DATE '2025-01-01' AND deb.DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY deb.TPN_CODE, n.TPN_DESCRIPTION, n.CPN_CODE, cp.CPN_DESCRIPTION,
         cp.TDOS_CODE, n.CODE_BRANCHE, deb.CODE_BRANCHE
ORDER BY nb_debours DESC;


-- -----------------------------------------------------------------------------
-- P06 — Valeurs des statuts et des drapeaux
-- Objectif : connaître les valeurs prises par les statuts des prestations,
--            débours, effets, réceptions et notifications, et par DEB_VERIFIE
--            (compté comme « contrôle a posteriori » dans DTM_PRESTATION).
-- Tableaux : T25 à T31.
-- Résultat : PE_STATUT : A, F, S. DEB_STATUT : A, D, I, N, P, S, V (D 57,97 M,
--            P 12,30 M). EFP_STATUT : A, D, I, N (et un « v »). NOT_STATUT :
--            A, I. RD_STATUT : E 272 455, I, L, R, A, vide. DEB_VERIFIE = O :
--            5 942 752 débours, dont 3 716 079 en V (19 % des débours de
--            vieillesse).
-- Constat  : Aucun statut n'a de libellé. DTM_PRESTATION ne filtre pas
--            DEB_STATUT : le sens des 7 valeurs est à obtenir. DEB_VERIFIE est
--            un drapeau de vérification de la saisie, pas un contrôle a
--            posteriori.
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


-- -----------------------------------------------------------------------------
-- P07 — Débours de 2025 par branche et par type
-- Objectif : voir les types de débours, leur signe, et ce que DTM_PRESTATION
--            compte comme payé : débours annulés (H7), sans effet de paiement,
--            non décaissés. Vérifier aussi que la branche du débours est celle
--            de son dossier.
-- Tableaux : T31, T32 à T57.
-- Résultat : 2025 : la branche du débours est toujours celle du dossier.
--            Paiements (PM) : V 913 540 débours, 50,46 Md ; F 3 196 959, 7,99
--            Md ; M 46 330, 2,95 Md ; A 24 604, 0,73 Md. Remboursements (RB) :
--            A 826. Aucun paiement négatif ni annulé ; 410 ajustements
--            annulés. Paiements sans effet : F 1 993 982 (62 %), V 208 966 (23
--            %), M 2 540, A 2 764. Paiements sans date de décaissement : F 2
--            549 446 (80 %), V 390 509 (43 %).
-- Constat  : H7 écartée (annulations négligeables). Mais DTM_PRESTATION prend
--            la personne payée, la DR et le service dans l'effet : 62 % des
--            paiements PF de 2025 n'y ont ni bénéficiaire, ni sexe, ni âge.
-- -----------------------------------------------------------------------------
SELECT deb.CODE_BRANCHE, d.TDOS_CODE AS branche_dossier, deb.DEB_TYPE, td.DESC_TYPE_DEB,
       CASE WHEN deb.TPE_CODE IS NOT NULL THEN 'ESPECES'
            WHEN deb.TPN_CODE IS NOT NULL THEN 'NATURE'
            ELSE 'SANS CODE' END                                        AS code_prestation,
       COUNT(*)                                                          AS nb_debours,
       SUM(deb.DEB_MONTANT)                                              AS montant,
       SUM(CASE WHEN deb.DEB_MONTANT < 0 THEN 1 ELSE 0 END)              AS nb_negatifs,
       SUM(CASE WHEN deb.DEB_DATE_ANNUL IS NOT NULL THEN 1 ELSE 0 END)   AS nb_annules,
       SUM(CASE WHEN deb.DEB_DATE_ANNUL IS NOT NULL
                THEN deb.DEB_MONTANT ELSE 0 END)                         AS montant_annule,
       SUM(CASE WHEN deb.EFP_ID IS NULL THEN 1 ELSE 0 END)               AS nb_sans_effet,
       SUM(CASE WHEN deb.DEB_DATE_DECAISSEMENT IS NULL THEN 1 ELSE 0 END) AS nb_non_decaisses
FROM USER_DWH.DEBOURS deb
LEFT JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE       = deb.DOS_CODE
LEFT JOIN USER_DWH.TYPE_DEBOURS td ON td.CODE_TYPE_DEB = deb.DEB_TYPE
WHERE deb.DEB_DATE_EFFET >= DATE '2025-01-01' AND deb.DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY deb.CODE_BRANCHE, d.TDOS_CODE, deb.DEB_TYPE, td.DESC_TYPE_DEB,
         CASE WHEN deb.TPE_CODE IS NOT NULL THEN 'ESPECES'
              WHEN deb.TPN_CODE IS NOT NULL THEN 'NATURE'
              ELSE 'SANS CODE' END
ORDER BY 1, 2, 3, 5;


-- -----------------------------------------------------------------------------
-- P08 — Net à payer des effets répété sur chaque débours (H6)
-- Objectif : DTM_PRESTATION joint l'effet de paiement à chacun de ses débours
--            et additionne NET_A_PAYER. Si un effet a plusieurs débours, son
--            net à payer est compté plusieurs fois. On compare le net à payer
--            réel des effets à celui que voit le DTM.
-- Tableaux : T32 à T41, T50 à T56.
-- Résultat : 520 197 effets payés en 2025, dont 321 674 avec plusieurs débours
--            (4,28 en moyenne, 72 au plus). Net à payer réel : 58,50 Md ; vu
--            par le DTM : 222,87 Md ; montant des débours : 55,05 Md.
-- Constat  : H6 confirmée : MONTANT_NET de DTM_PRESTATION est gonflé 3,8 fois.
-- -----------------------------------------------------------------------------
WITH deb AS (
    SELECT EFP_ID, COUNT(*) AS nb_deb, SUM(DEB_MONTANT) AS mnt_deb
    FROM USER_DWH.DEBOURS
    WHERE EFP_ID IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
    GROUP BY EFP_ID
)
SELECT COUNT(*)                                          AS nb_effets,
       SUM(CASE WHEN deb.nb_deb > 1 THEN 1 ELSE 0 END)   AS effets_plusieurs_debours,
       MAX(deb.nb_deb)                                   AS max_debours_par_effet,
       ROUND(AVG(deb.nb_deb), 2)                         AS moy_debours_par_effet,
       SUM(e.NET_A_PAYER)                                AS net_a_payer_reel,
       SUM(e.NET_A_PAYER * deb.nb_deb)                   AS net_a_payer_vu_par_dtm,
       SUM(deb.mnt_deb)                                  AS montant_des_debours
FROM deb
JOIN USER_DWH.EFFET e ON e.EFP_ID = deb.EFP_ID;


-- -----------------------------------------------------------------------------
-- P09 — Dossiers par année et par branche, à chaque étape (H1)
-- Objectif : comparer les dossiers reçus, ouverts, rejetés et notifiés à ceux
--            que garde DTM_DOSSIER (dossiers ouverts ayant au moins un débours).
-- Tableaux : T25 à T27.
-- Résultat : 2025 (reçus / ouverts / avec débours / rejetés / notifiés) : A 1
--            481 / 1 237 / 413 / 1 / 357 ; F 9 968 / 9 205 / 5 591 / 19 / 16
--            075 ; M 4 338 / 4 238 / 4 134 / 2 / 4 265 ; V 4 671 / 3 638 / 3
--            030 / 14 / 4 783. Rejets enregistrés : de 0 à 62 par an et par
--            branche.
-- Constat  : H1 confirmée : en 2025, DTM_DOSSIER ne garde que 33 % des
--            dossiers AT/MP ouverts, 61 % en PF, 83 % en vieillesse. Les
--            dossiers déposés sont les réceptions, plus nombreuses que les
--            dossiers ouverts (V : 1 033 réceptions de 2025 sans dossier ni
--            rejet). Les rejets ne sont presque pas saisis.
-- -----------------------------------------------------------------------------
SELECT annee, branche,
       SUM(CASE WHEN etape = 'RECUS'      THEN nb END) AS recus,
       SUM(CASE WHEN etape = 'OUVERTS'    THEN nb END) AS ouverts,
       SUM(CASE WHEN etape = 'AVEC_DEB'   THEN nb END) AS ouverts_avec_debours,
       SUM(CASE WHEN etape = 'REJETES'    THEN nb END) AS rejetes,
       SUM(CASE WHEN etape = 'REJ_SANS_D' THEN nb END) AS rejetes_sans_dossier,
       SUM(CASE WHEN etape = 'NOTIFIES'   THEN nb END) AS notifies
FROM (
    SELECT 'RECUS' AS etape, EXTRACT(YEAR FROM RD_DATE_RECEPTION) AS annee,
           TO_CHAR(TDOS_CODE) AS branche, COUNT(*) AS nb
    FROM USER_DWH.RECEPTION_DOSSIER
    WHERE RD_DATE_RECEPTION >= DATE '2019-01-01' AND RD_DATE_RECEPTION < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM RD_DATE_RECEPTION), TDOS_CODE
    UNION ALL
    SELECT 'OUVERTS', EXTRACT(YEAR FROM DOS_DATE_OUVERTURE), TO_CHAR(TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER
    WHERE DOS_DATE_OUVERTURE >= DATE '2019-01-01' AND DOS_DATE_OUVERTURE < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM DOS_DATE_OUVERTURE), TDOS_CODE
    UNION ALL
    SELECT 'AVEC_DEB', EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE), TO_CHAR(d.TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER d
    WHERE d.DOS_DATE_OUVERTURE >= DATE '2019-01-01' AND d.DOS_DATE_OUVERTURE < DATE '2026-01-01'
      AND d.DOS_CODE IN (SELECT DOS_CODE FROM USER_DWH.DEBOURS)
    GROUP BY EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE), d.TDOS_CODE
    UNION ALL
    SELECT 'REJETES', EXTRACT(YEAR FROM DOSRE_DATE_REJET), TO_CHAR(TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER_REJETE
    WHERE DOSRE_DATE_REJET >= DATE '2019-01-01' AND DOSRE_DATE_REJET < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM DOSRE_DATE_REJET), TDOS_CODE
    UNION ALL
    SELECT 'REJ_SANS_D', EXTRACT(YEAR FROM DOSRE_DATE_REJET), TO_CHAR(TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER_REJETE
    WHERE DOSRE_DATE_REJET >= DATE '2019-01-01' AND DOSRE_DATE_REJET < DATE '2026-01-01'
      AND DOS_CODE IS NULL
    GROUP BY EXTRACT(YEAR FROM DOSRE_DATE_REJET), TDOS_CODE
    UNION ALL
    SELECT 'NOTIFIES', EXTRACT(YEAR FROM n.dt_notif), TO_CHAR(d.TDOS_CODE), COUNT(*)
    FROM (SELECT DOS_CODE, MAX(PE_DATE_NOTIFICATION) AS dt_notif
          FROM USER_DWH.PRESTATION_ESP GROUP BY DOS_CODE) n
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = n.DOS_CODE
    WHERE n.dt_notif >= DATE '2019-01-01' AND n.dt_notif < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM n.dt_notif), d.TDOS_CODE
)
GROUP BY annee, branche
ORDER BY branche, annee;


-- -----------------------------------------------------------------------------
-- P10 — Durée de liquidation des dossiers ouverts en 2025 (H3)
-- Objectif : répartir le délai entre réception et notification, calculé comme
--            dans DTM_DOSSIER, et compter les dossiers que ses tranches
--            perdent (délai nul ou négatif, date manquante).
-- Tableaux : T25 à T27.
-- Résultat : Dossiers ouverts en 2025 : 18 008 au statut A, 310 au statut F.
--            Tous ont une réception. Sans notification : A 1 136, F 1 232, M
--            87, V 356. Délai nul : 6 ; négatif : 0. Notifiés, statut A (moins
--            de 15 j / 15 à 45 j / plus de 45 j ; médiane) : A 2 / 98 ; 156 j.
--            F 4 542 / 1 938 / 1 465 ; 13,5 j. M 1 716 / 1 711 / 486 ; 17 j. V
--            560 / 708 / 1 965 ; 62 j.
-- Constat  : H3 écartée (délai nul négligeable). Mais DTM_DOSSIER ne compte
--            comme liquidés que les dossiers au statut F : 310 pour 2025,
--            alors que 15 479 dossiers ouverts en 2025 sont notifiés. La durée
--            de liquidation est calculable : plus de 45 jours pour 61 % des
--            dossiers de vieillesse et 98 % des dossiers AT/MP.
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DOS_CODE, MAX(PE_DATE_NOTIFICATION) AS dt_notif
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
base AS (
    SELECT d.TDOS_CODE, d.DOS_STATUT, r.RD_DATE_RECEPTION, n.dt_notif,
           n.dt_notif - r.RD_DATE_RECEPTION AS delai
    FROM USER_DWH.DOSSIER d
    LEFT JOIN notif n ON n.DOS_CODE = d.DOS_CODE
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID = d.RD_ID
    WHERE d.DOS_DATE_OUVERTURE >= DATE '2025-01-01' AND d.DOS_DATE_OUVERTURE < DATE '2026-01-01'
)
SELECT TDOS_CODE, DOS_STATUT,
       COUNT(*)                                                         AS nb_dossiers,
       SUM(CASE WHEN RD_DATE_RECEPTION IS NULL THEN 1 ELSE 0 END)       AS sans_reception,
       SUM(CASE WHEN dt_notif IS NULL THEN 1 ELSE 0 END)                AS sans_notification,
       SUM(CASE WHEN delai < 0 THEN 1 ELSE 0 END)                       AS delai_negatif,
       SUM(CASE WHEN delai = 0 THEN 1 ELSE 0 END)                       AS delai_nul,
       SUM(CASE WHEN delai > 0 AND delai < 15 THEN 1 ELSE 0 END)        AS moins_15j,
       SUM(CASE WHEN delai BETWEEN 15 AND 45 THEN 1 ELSE 0 END)         AS de_15_a_45j,
       SUM(CASE WHEN delai > 45 THEN 1 ELSE 0 END)                      AS plus_45j,
       ROUND(AVG(CASE WHEN delai >= 0 THEN delai END), 1)               AS delai_moyen,
       ROUND(MEDIAN(CASE WHEN delai >= 0 THEN delai END), 1)            AS delai_median
FROM base
GROUP BY TDOS_CODE, DOS_STATUT
ORDER BY TDOS_CODE, DOS_STATUT;


-- -----------------------------------------------------------------------------
-- P11 — Types d'ajustement, statuts et montants (H12)
-- Objectif : lire le libellé de chaque type d'ajustement et de chaque statut,
--            le signe des montants et la branche du dossier, pour classer
--            correctement trop perçus et moins perçus.
-- Tableaux : T31.
-- Résultat : 12 types d'ajustement, statuts A, C, I, R, montants positifs.
--            Trop perçus : TP (A 6 879, C 5 601), TP-CONV trop-perçu sur
--            convention (937), REM-TP remboursement de trop-perçu (242). Moins
--            perçus : MP « moins-perçu ou rappel » (424 180), RC rappel sur
--            convention (6 272). Hors indus : AV avances (760 871, 77,4 Md),
--            MPG mois de pension gratuit (2010–2015), avance et don COVID, RR
--            rachat de rente, REV-AV reversement d'avance, OI opposition
--            interne. Branche M : 420 TP, 1 207 MP, 63 REM-TP. 2025 : TP V
--            718, TP-CONV 758.
-- Constat  : Le classement trop perçu / moins perçu du DTM suit les libellés,
--            sauf REM-TP : le remboursement d'un trop perçu n'est pas un
--            nouvel indu (H12). La branche M est exclue à tort. Sens des
--            statuts A, C, I, R à obtenir : C ne peut pas vouloir dire «
--            recouvré » pour un moins perçu.
-- -----------------------------------------------------------------------------
SELECT aj.TAJ_CODE, t.TAJ_DESCRIPTION, aj.AJ_STATUT, d.TDOS_CODE,
       COUNT(*)                                              AS nb_ajustements,
       SUM(aj.AJ_MONTANT)                                    AS montant,
       SUM(CASE WHEN aj.AJ_MONTANT < 0 THEN 1 ELSE 0 END)    AS nb_negatifs,
       MIN(aj.AJ_DATE_ETABLISSEMENT)                         AS date_min,
       MAX(aj.AJ_DATE_ETABLISSEMENT)                         AS date_max,
       SUM(CASE WHEN aj.AJ_DATE_ETABLISSEMENT >= DATE '2025-01-01'
                 AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                           AS nb_2025
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN USER_DWH.TYPE_AJUSTEMENT t ON t.TAJ_CODE = aj.TAJ_CODE
LEFT JOIN USER_DWH.DOSSIER         d ON d.DOS_CODE = aj.DOS_CODE
GROUP BY aj.TAJ_CODE, t.TAJ_DESCRIPTION, aj.AJ_STATUT, d.TDOS_CODE
ORDER BY aj.TAJ_CODE, aj.AJ_STATUT, d.TDOS_CODE;


-- -----------------------------------------------------------------------------
-- P12 — Types d'étape des prestations
-- Objectif : savoir si les étapes enregistrent la vie d'une pension
--            (liquidation, suspension, reprise, radiation, décès…). Elles
--            permettraient de compter les pensionnés présents au 31 décembre
--            (T32 à T41) et les nouveaux de l'année (T42 à T49).
-- Tableaux : T32 à T49, T57.
-- Résultat : 13 types d'étape : CIP calcul initial (947 441, une par
--            prestation ; 30 236 en 2025), RP révision (588 919), MAJ
--            majoration (167 810), retenues RP5 à RP25, RRR rabais pour rachat
--            de rente, correctifs CORN et CORP, PCCM. Date maximale du calcul
--            initial en 2100.
-- Constat  : Les étapes sont des étapes de calcul du montant, pas des
--            événements (suspension, radiation, décès). Elles ne permettent
--            pas de compter les pensionnés présents au 31/12 : il faudra les
--            dates de la prestation (suspension, radiation, réactivation) ou
--            les paiements de décembre (lot 2).
-- -----------------------------------------------------------------------------
SELECT e.TEA_CODE, t.TEA_DESCRIPTION,
       COUNT(*)                     AS nb_etapes,
       COUNT(DISTINCT e.PE_ID)      AS nb_prestations,
       MIN(e.EA_DATE_EFFET)         AS date_min,
       MAX(e.EA_DATE_EFFET)         AS date_max,
       SUM(CASE WHEN e.EA_DATE_EFFET >= DATE '2025-01-01'
                 AND e.EA_DATE_EFFET <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)  AS nb_2025
FROM USER_DWH.ETAPE e
LEFT JOIN USER_DWH.TYPE_ETAPE t ON t.TEA_CODE = e.TEA_CODE
GROUP BY e.TEA_CODE, t.TEA_DESCRIPTION
ORDER BY nb_etapes DESC;


-- -----------------------------------------------------------------------------
-- P13 — Contrôles a posteriori : sources possibles (H11)
-- Objectif : voir si DOSSIERS_CONTROLES est alimentée, par année et par
--            branche, et la comparer aux débours marqués « vérifiés » et au
--            nombre de dossiers payés (dénominateur du taux de contrôle).
-- Tableaux : T28 à T30.
-- Résultat : DOSSIERS_CONTROLES : de 1 à 212 contrôles par an et par branche ;
--            2025 : A 4, F 40, M 46, V 25 (115) ; années aberrantes 202, 203
--            et 2055. Débours vérifiés 2025 : V 165 039 sur 15 851 dossiers, F
--            70 317 sur 22 945. Dossiers payés en 2025 : A 2 134, F 100 113, M
--            5 227, V 47 732.
-- Constat  : H11 confirmée : taux de contrôle a posteriori 2025 = 0,07 % (115
--            / 155 206), pour une norme d'au moins 5 %, et plus aucune saisie
--            depuis juillet 2025. DEB_VERIFIE n'est pas un contrôle a
--            posteriori.
-- -----------------------------------------------------------------------------
SELECT 'a. Dossiers contrôlés (DOSSIERS_CONTROLES)' AS source,
       EXTRACT(YEAR FROM DC_DATE_CONTROLE) AS annee, TO_CHAR(TDOS_CODE) AS branche,
       COUNT(*) AS nb_lignes, COUNT(DISTINCT DOS_CODE) AS nb_dossiers
FROM USER_DWH.DOSSIERS_CONTROLES
GROUP BY EXTRACT(YEAR FROM DC_DATE_CONTROLE), TDOS_CODE
UNION ALL
SELECT 'b. Débours vérifiés (DEB_VERIFIE = O)',
       EXTRACT(YEAR FROM DEB_DATE_EFFET), TO_CHAR(CODE_BRANCHE),
       COUNT(*), COUNT(DISTINCT DOS_CODE)
FROM USER_DWH.DEBOURS
WHERE DEB_VERIFIE = 'O'
  AND DEB_DATE_EFFET >= DATE '2019-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM DEB_DATE_EFFET), CODE_BRANCHE
UNION ALL
SELECT 'c. Dossiers payés (au moins un débours)',
       EXTRACT(YEAR FROM DEB_DATE_EFFET), TO_CHAR(CODE_BRANCHE),
       COUNT(*), COUNT(DISTINCT DOS_CODE)
FROM USER_DWH.DEBOURS
WHERE DEB_DATE_EFFET >= DATE '2019-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM DEB_DATE_EFFET), CODE_BRANCHE
ORDER BY 1, 3, 2;


-- -----------------------------------------------------------------------------
-- P14 — Tranches d'âge de la base (H9)
-- Objectif : comparer la grille unique lue par le DTM (DIM_TRANCHE_AGE,
--            chargée depuis TRANCHE_AGE) aux 11 grilles de l'Annuaire.
-- Tableaux : T32 à T49.
-- Résultat : 21 tranches de 5 ans : 0-4, 5-9 … 95-99, 100+ (bornes incluses).
-- Constat  : H9 nuancée : toutes les bornes CIPRES sont des multiples de 5.
--            Les 11 grilles s'obtiennent donc en regroupant ces tranches
--            (table de regroupement côté BI). DIM_TRANCHE_AGE a 22 lignes
--            (P18) : une de plus, sans doute « non renseigné ».
-- -----------------------------------------------------------------------------
SELECT TAG_CODE, TRANCHE, INF, SUP
FROM USER_DWH.TRANCHE_AGE
ORDER BY INF;


-- -----------------------------------------------------------------------------
-- P15 — Qualité des individus : sexe, naissance, décès
-- Objectif : connaître les valeurs du sexe, les dates de naissance manquantes
--            ou aberrantes (âge impossible à calculer), et les décès par année
--            (T57 et NB_DECES).
-- Tableaux : T32 à T57.
-- Résultat : Sexe : 1 = 889 506, 2 = 653 780, aucun vide. Date de naissance
--            vide : 0 ; avant 1900 ou après 2025 : 58. Décès : 1 271 (2019), 1
--            361, 1 309, 1 165, 1 102, 1 109, 487 (2025) ; 14 171 avant 2019.
-- Constat  : Sexe et date de naissance fiables. Deux fois moins de décès en
--            2025 : saisie en retard, à confirmer.
-- -----------------------------------------------------------------------------
SELECT 'Sexe = ' || NVL(TO_CHAR(IND_SEXE), 'vide') AS rubrique, COUNT(*) AS nb
FROM USER_DWH.INDIVIDU GROUP BY IND_SEXE
UNION ALL
SELECT 'Date de naissance vide', COUNT(*)
FROM USER_DWH.INDIVIDU WHERE IND_DATE_NAISSANCE IS NULL
UNION ALL
SELECT 'Date de naissance avant 1900 ou après 2025', COUNT(*)
FROM USER_DWH.INDIVIDU
WHERE IND_DATE_NAISSANCE < DATE '1900-01-01' OR IND_DATE_NAISSANCE >= DATE '2026-01-01'
UNION ALL
SELECT 'Décès en ' || TO_CHAR(EXTRACT(YEAR FROM IND_DATE_DECES)), COUNT(*)
FROM USER_DWH.INDIVIDU
WHERE IND_DATE_DECES >= DATE '2019-01-01' AND IND_DATE_DECES < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM IND_DATE_DECES)
UNION ALL
SELECT 'Décès avant 2019', COUNT(*)
FROM USER_DWH.INDIVIDU WHERE IND_DATE_DECES < DATE '2019-01-01'
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- P16 — Qui est la personne comptée : assuré, ayant droit ou payé ? (H13)
-- Objectif : pour chaque type de prestation, comparer l'assuré du dossier,
--            la personne pour qui la prestation est ouverte (IND_ID_PREST) et
--            la personne payée (IND_ID_BENEF, celle que compte DTM_PRESTATION).
--            Pour une pension d'orphelin, CIPRES compte l'orphelin.
-- Tableaux : T32 à T49.
-- Résultat : FAF : la personne de la prestation (l'enfant) n'est jamais
--            l'assuré ; la personne payée l'est (599 700). Orphelins (RPSE 29
--            689, RASO 7 579, ARSE 1 121) et RBEC : la personne payée n'est
--            jamais l'ayant droit. Veuves (RPSV) : la personne payée est
--            l'ayant droit. AIJ : 9 162 prestations sur 13 201 sans personne
--            payée.
-- Constat  : H13 confirmée : pour les orphelins, le DTM compte le tuteur payé
--            (son âge, son sexe) au lieu de l'orphelin. Règle à proposer :
--            compter l'ayant droit (IND_ID_PREST), sauf pour les allocations
--            familiales, où l'allocataire est la personne payée.
-- -----------------------------------------------------------------------------
SELECT pe.TPE_CODE,
       COUNT(*)                                                          AS nb_prestations,
       SUM(CASE WHEN pe.IND_ID_BENEF IS NULL THEN 1 ELSE 0 END)          AS benef_vide,
       SUM(CASE WHEN pe.IND_ID_PREST IS NULL THEN 1 ELSE 0 END)          AS prest_vide,
       SUM(CASE WHEN pe.IND_ID_BENEF = d.IND_ID THEN 1 ELSE 0 END)       AS benef_est_assure,
       SUM(CASE WHEN pe.IND_ID_PREST = d.IND_ID THEN 1 ELSE 0 END)       AS prest_est_assure,
       SUM(CASE WHEN pe.IND_ID_BENEF <> pe.IND_ID_PREST THEN 1 ELSE 0 END) AS benef_autre_que_prest
FROM USER_DWH.PRESTATION_ESP pe
JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
GROUP BY pe.TPE_CODE
ORDER BY nb_prestations DESC;


-- =============================================================================
-- PARTIE B — DWH (connexion du DWH : schémas DWH et DTM)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P17 — Volumes et clichés des faits chargés dans le DWH
-- Objectif : comparer chaque fait à sa table source (P01) pour repérer les
--            faits vides ou incomplets, et vérifier qu'il n'y a qu'un cliché.
-- Tableaux : tous.
-- Résultat : 13 faits, chacun avec le même nombre de lignes que sa table
--            source ; un seul cliché, 092026.
-- Constat  : Le DWH est complet par rapport à la source (copie du 02/02/2026).
-- -----------------------------------------------------------------------------
SELECT 'FAIT_RECEPTION_DOSSIER' AS fait, COUNT(*) AS nb_lignes, COUNT(DISTINCT CLICHE) AS nb_cliches,
       MIN(TO_CHAR(CLICHE)) AS cliche_min, MAX(TO_CHAR(CLICHE)) AS cliche_max
FROM DWH.FAIT_RECEPTION_DOSSIER
UNION ALL
SELECT 'FAIT_DOSSIER', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_DOSSIER
UNION ALL
SELECT 'FAIT_DOSSIER_REJETE', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_DOSSIER_REJETE
UNION ALL
SELECT 'FAIT_PRESTATION_ESP', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_PRESTATION_ESP
UNION ALL
SELECT 'FAIT_NOTIFICATION_PREST', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_NOTIFICATION_PREST
UNION ALL
SELECT 'FAIT_ETAPE', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_ETAPE
UNION ALL
SELECT 'FAIT_DEBOURS', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_DEBOURS
UNION ALL
SELECT 'FAIT_EFFET', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_EFFET
UNION ALL
SELECT 'FAIT_AJUSTEMENT', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_AJUSTEMENT
UNION ALL
SELECT 'FAIT_DOSSIERS_CONTROLES', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_DOSSIERS_CONTROLES
UNION ALL
SELECT 'FAIT_INDIVIDU', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_INDIVIDU
UNION ALL
SELECT 'FAIT_LIEN', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_LIEN
UNION ALL
SELECT 'FAIT_TSINISTRE', COUNT(*), COUNT(DISTINCT CLICHE), MIN(TO_CHAR(CLICHE)), MAX(TO_CHAR(CLICHE)) FROM DWH.FAIT_TSINISTRE;


-- -----------------------------------------------------------------------------
-- P18 — Tables DTM du domaine : existence
-- Objectif : vérifier que les tables lues par les requêtes DTM existent, en
--            particulier DIM_TYPE_PRESTATION (lue par dossier.sql, absente de
--            la configuration des dimensions — H14).
-- Tableaux : tous.
-- Résultat : Toutes les tables existent. DIM_TYPE_PRESTATION : 79 lignes (28
--            codes en espèces + 51 en nature), analysée le 04/04/26 ; autres
--            dimensions le 01/09/26. Statistiques : DTM_PRESTATION 4 356 764
--            lignes, DTM_DOSSIER 424 088, DTM_PRESTATION_INDUE 146 620,
--            DTM_ACCIDENT_TRAVAIL 34 172, DTM_CONTROLE_PRESTATION 1 036,
--            DTM_INDICATEURS_PRESTATIONS 416.
-- Constat  : H14 écartée : DIM_TYPE_PRESTATION existe, faite à la main (codes
--            en espèces et en nature), hors configuration des dimensions.
--            Le contenu des tables DTM n'est pas analysé plus loin.
-- -----------------------------------------------------------------------------
SELECT table_name, num_rows, last_analyzed
FROM ALL_TABLES
WHERE owner = 'DTM'
  AND table_name IN ('DTM_PRESTATION', 'DTM_DOSSIER', 'DTM_CONTROLE_PRESTATION',
                     'DTM_PRESTATION_INDUE', 'DTM_INDICATEURS_PRESTATIONS',
                     'DTM_ACCIDENT_TRAVAIL', 'DIM_TYPE_PRESTATION',
                     'DIM_TYPE_PRESTATION_ESP', 'DIM_TYPE_PRESTATION_NAT',
                     'DIM_TYPE_DOSSIER', 'DIM_BRANCHE', 'DIM_TRANCHE_AGE',
                     'DIM_TYPE_AJUSTEMENT', 'DIM_TYPE_ETAPE', 'DIM_TYPE_DEBOURS',
                     'DIM_CATEGORIE_DOSSIER_PENSION', 'DIM_CATEGORIE_PRESTATION_NAT')
ORDER BY table_name;


-- -----------------------------------------------------------------------------
-- P19 — Tables DTM du domaine : clichés chargés et période couverte
-- Objectif : savoir quels clichés sont publiés et sur quelles années.
-- Tableaux : tous.
-- Résultat : abandonnée le 26/10/2026 : le contenu des tables DTM n'est pas
--            utilisé (considéré comme faux).
-- -----------------------------------------------------------------------------
SELECT 'DTM_PRESTATION' AS table_dtm, TO_CHAR(CLICHE) AS cliche, COUNT(*) AS nb_lignes,
       MIN(ID_TEMPS) AS periode_min, MAX(ID_TEMPS) AS periode_max
FROM DTM.DTM_PRESTATION GROUP BY CLICHE
UNION ALL
SELECT 'DTM_DOSSIER', TO_CHAR(CLICHE), COUNT(*), MIN(ID_TEMPS), MAX(ID_TEMPS) FROM DTM.DTM_DOSSIER GROUP BY CLICHE
UNION ALL
SELECT 'DTM_CONTROLE_PRESTATION', TO_CHAR(CLICHE), COUNT(*), MIN(ID_TEMPS), MAX(ID_TEMPS) FROM DTM.DTM_CONTROLE_PRESTATION GROUP BY CLICHE
UNION ALL
SELECT 'DTM_PRESTATION_INDUE', TO_CHAR(CLICHE), COUNT(*), MIN(ID_TEMPS), MAX(ID_TEMPS) FROM DTM.DTM_PRESTATION_INDUE GROUP BY CLICHE
UNION ALL
SELECT 'DTM_ACCIDENT_TRAVAIL', TO_CHAR(CLICHE), COUNT(*), MIN(ID_TEMPS), MAX(ID_TEMPS) FROM DTM.DTM_ACCIDENT_TRAVAIL GROUP BY CLICHE
UNION ALL
SELECT 'DTM_INDICATEURS_PRESTATIONS', TO_CHAR(CLICHE), COUNT(*), MIN(AN_ID), MAX(AN_ID) FROM DTM.DTM_INDICATEURS_PRESTATIONS GROUP BY CLICHE
ORDER BY 1, 2;
