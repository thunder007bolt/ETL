-- =============================================================================
-- LOT 3 — Partie A : les listes de codes (nomenclatures) du domaine Prestations
-- Rédigé le 29/10/2026, partie A exécutée le 29/10/2026.
-- Sur la base source, schéma USER_DWH, comme les lots 1 et 2.
-- Exécution : en mode script (F5) dans SQL Developer. Les listes de codes ne
-- contiennent aucune donnée nominative ; les comptages portent sur les tables
-- légères (DOSSIER, PRESTATION_ESP, ETAPE, AJUSTEMENT, EFFET), jamais sur
-- DEBOURS (71,7 millions de lignes).
-- Pourquoi ce lot : le schéma Excalidraw « tables_prestations » et l'artefact
-- « Tables des prestations CNSS » doivent lister tous les codes. Les lots 1 et 2
-- n'ont relevé que les codes rencontrés dans les données ; il manque des libellés
-- (accessoires de pension, étapes, ajustements COVID, modes de paiement) et des
-- listes entières (prestations en nature, effets, directions, lieux de paiement).
-- La partie B du lot 3 (points à vérifier de T25 et DTM_DOSSIER, indus,
-- allocataires) sera ajoutée à la suite, à partir de P48.
-- Numérotation : suite du lot 2, à partir de P31.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P31 — Types de dossier et leur branche
-- Objectif : libellés exacts des 4 types de dossier (V, A, F, M) et de leur
--            branche, avec le nombre de dossiers.
-- Tableaux : tous.
-- Résultat : A prestations d'accidents et maladies professionnelles, branche
--            RP (33 178 dossiers) ; F prestations familiales, branche PF
--            (202 737) ; M prestations de maternité, branche PF (56 449) ; V
--            prestations de vieillesse et retraite, branche PE (75 583). Total
--            367 947, tous les dossiers (P01).
-- Constat  : M est bien la maternité, rattachée à la branche PF (règle R1).
-- -----------------------------------------------------------------------------
SELECT t.TDOS_CODE, t.TDOS_DESCRIPTION, t.BR_CODE, b.BR_DESCRIPTION,
       (SELECT COUNT(*) FROM USER_DWH.DOSSIER d WHERE d.TDOS_CODE = t.TDOS_CODE) AS nb_dossiers
FROM USER_DWH.TYPE_DOSSIER t
LEFT JOIN USER_DWH.BRANCHE b ON b.BR_CODE = t.BR_CODE
ORDER BY t.TDOS_CODE;


-- -----------------------------------------------------------------------------
-- P32 — Branches
-- Objectif : liste complète des branches (y compris celles sans type de
--            dossier de prestations).
-- Tableaux : tous.
-- Résultat : 3 branches : PE branche des pensions, PF branche des prestations
--            familiales, RP branche des risques professionnels.
-- Constat  : Aucune branche sans type de dossier.
-- -----------------------------------------------------------------------------
SELECT BR_CODE, BR_DESCRIPTION
FROM USER_DWH.BRANCHE
ORDER BY BR_CODE;


-- -----------------------------------------------------------------------------
-- P33 — Catégories de dossier de pension
-- Objectif : libellés exacts de ORDINAIRE, COOR_CNSS, COOR_AC, CNSS_AC et des
--            catégories jamais utilisées.
-- Tableaux : T25, T28, T32 à T36.
-- Résultat : ORDINAIRE dossier ordinaire (240 472), COOR_CNSS dossier de
--            coordination payé par la CNSS (3 473), COOR_AC dossier de
--            coordination payé par une autre caisse (3 326), CNSS_AC dossier
--            CNSS transféré à une autre caisse (158), HORS_CONV dossier hors
--            convention (jamais utilisé).
-- Constat  : 247 429 dossiers ont une catégorie (toutes branches), 120 518
--            n'en ont pas. Les requêtes proposées lisent l'absence comme
--            ORDINAIRE (NVL), ce qui est cohérent.
-- -----------------------------------------------------------------------------
SELECT c.DOS_CATEGORIE, c.CDP_DESCRIPTION,
       (SELECT COUNT(*) FROM USER_DWH.DOSSIER d WHERE d.DOS_CATEGORIE = c.DOS_CATEGORIE) AS nb_dossiers
FROM USER_DWH.CATEGORIE_DOSSIER_PENSION c
ORDER BY c.DOS_CATEGORIE;


-- -----------------------------------------------------------------------------
-- P34 — Types de prestation en espèces : liste complète
-- Objectif : libellé exact de chacun des codes, en particulier les accessoires
--            de pension (RBEC, RME, RME-COL, RCCM, RRC) et les parts de
--            l'allocation de survivants (RASC, RASO, RASA), restés sans libellé
--            sûr après P04. Inclut les codes jamais utilisés.
-- Tableaux : T25 à T57.
-- Résultat : 28 codes, total 947 953 prestations (toutes, P01). V (17) :
--            RPV pension de vieillesse (46 836), RPRA pension de retraite
--            anticipée (176), RPI pension d'invalidité (231), RPSV veuve
--            (25 541), RPSF veuf (424), RPSE orphelin (29 689), RPA ascendant
--            (81), RAV allocation de vieillesse (12 833), RAS allocation de
--            survivants (864) et ses parts RASC conjoint (1 469), RASO orphelin
--            (7 580), RASA ascendant (327), RBEC bonification pour enfant à
--            charge (68 836), RME majoration pour enfant CARFO (762), RME-COL
--            majoration pour enfant collectivités (18), RCCM contributions aux
--            charges du ménage (70), RRC remboursement de cotisations (recours
--            gracieux) (17). A (7) : ARI rente viagère d'incapacité (2 349),
--            ARSV rente de veuve (388), ARSF veuf (4), ARSE orphelin (1 121),
--            ARSA ascendant (350), AIJ indemnités journalières (13 201), AAI
--            allocation d'incapacité (1 233). F (2) : FAF allocation familiale
--            (614 811), FAP allocation prénatale (9 600). M (2) : MAP allocation
--            prénatale (52 716), MIJ indemnités journalières (56 426).
--            Fréquence T pour FAF et FAP, M pour les autres.
-- Constat  : Tous les libellés sont maintenant sûrs. RPRA est bien la retraite
--            anticipée. RBEC, RME, RME-COL, RCCM et RRC sont des accessoires
--            versés en plus d'une pension (RBEC : 68 836 prestations) : à ne
--            pas compter comme des pensionnés (à confirmer). Aucun code IJ ni
--            RV, que cherche DTM_ACCIDENT_TRAVAIL (tâche 2 du TODO).
-- -----------------------------------------------------------------------------
SELECT t.TPE_CODE, t.TDOS_CODE, t.TPE_DESCRIPTION, t.TPE_FREQ_PAIEMENT,
       (SELECT COUNT(*) FROM USER_DWH.PRESTATION_ESP pe WHERE pe.TPE_CODE = t.TPE_CODE) AS nb_prestations
FROM USER_DWH.TYPE_PRESTATION_ESP t
ORDER BY t.TDOS_CODE, t.TPE_CODE;


-- -----------------------------------------------------------------------------
-- P35 — Prestations en nature et leurs catégories
-- Objectif : liste complète des types de prestation en nature (51 codes d'après
--            DIM_TYPE_PRESTATION, P18), avec leur catégorie et leur branche.
-- Tableaux : T26, T29, T56.
-- Résultat : 51 types en 24 catégories (liste complète dans
--            resultats/lot3_P35_prestations_nature.tsv). 23 catégories de la
--            branche A (44 types : analyse, appareillage, chirurgie, constat,
--            dentaire, divers, enquête, expertise, facturation, funéraire,
--            hôpital, laboratoire, médecine interne, pharmacie, prothèse,
--            radiologie, réadaptation, reclassement, rééducation, scanner,
--            séjour, transport, verres) ; 1 catégorie de la branche M, FM frais
--            de maternité (7 types : accouchement, consultation, examens,
--            hospitalisation, kit opératoire, pharmacie, radio). FRAIS_DIVERS
--            n'a pas de branche sur le type.
-- Constat  : La liste prévoit des frais de maternité, mais aucun n'a été payé
--            en 2025 (P05) : les soins payés sont ceux des AT/MP.
-- -----------------------------------------------------------------------------
SELECT c.CPN_CODE, c.CPN_DESCRIPTION, c.TDOS_CODE AS tdos_categorie,
       n.TPN_CODE, n.TPN_DESCRIPTION, n.CODE_BRANCHE
FROM USER_DWH.TYPE_PRESTATION_NAT n
LEFT JOIN USER_DWH.CATEGORIE_PRESTATION_NAT c ON c.CPN_CODE = n.CPN_CODE
ORDER BY c.CPN_CODE, n.TPN_CODE;


-- -----------------------------------------------------------------------------
-- P36 — Types de débours
-- Objectif : liste complète (PM et RB sont les seuls vus dans les données de
--            2025, P07).
-- Tableaux : T31 à T56.
-- Résultat : AJ ajustement, CP compensation, PM paiement, RB remboursement.
-- Constat  : AJ et CP ne sont pas vus dans les débours de 2025 (P07). Seul PM
--            compte pour les paiements (règle R3).
-- -----------------------------------------------------------------------------
SELECT CODE_TYPE_DEB, DESC_TYPE_DEB
FROM USER_DWH.TYPE_DEBOURS
ORDER BY CODE_TYPE_DEB;


-- -----------------------------------------------------------------------------
-- P37 — Types d'effet
-- Objectif : liste complète des types de titre de paiement, avec le nombre
--            d'effets (EFFET : 8,7 millions de lignes, lecture complète).
-- Tableaux : T32 à T56.
-- Résultat : 11 types, 8 711 190 effets : BVB virement bancaire (4 303 673),
--            BE espèces (4 242 131), BPEMP employeurs en convention (68 545),
--            BPM paiement mobile (53 167), BC chèque (23 889), BS structure
--            sanitaire et sociale (8 339), BMP mandat postal (7 435), BREV
--            reversement d'avance (3 806), BREMB remboursement de trop perçu
--            (205) ; RPC et RRC, relevés de coordination entre caisses, jamais
--            utilisés.
-- Constat  : Virement et espèces font 98,1 % des effets. Les relevés de
--            coordination n'existent pas dans la base.
-- -----------------------------------------------------------------------------
SELECT t.TEP_CODE, t.TEP_DESCRIPTION, NVL(e.nb_effets, 0) AS nb_effets
FROM USER_DWH.TYPE_EFFET t
LEFT JOIN (SELECT TEP_CODE, COUNT(*) AS nb_effets
           FROM USER_DWH.EFFET
           GROUP BY TEP_CODE) e ON e.TEP_CODE = t.TEP_CODE
ORDER BY t.TEP_CODE;


-- -----------------------------------------------------------------------------
-- P38 — Modes de paiement
-- Objectif : libellés des modes C, E, M, P, T, V vus sur les débours et les
--            effets (P23), question posée à la DSI.
-- Tableaux : T32 à T56.
-- Résultat : C chèque, E espèces, M mandat postal, P paiement aux employeurs
--            conventionnés, S paiement aux structures sanitaires et sociales,
--            T paiement mobile, V virement bancaire.
-- Constat  : Répond à la question posée à la DSI sur les modes de paiement.
--            S n'était pas vu dans les données (P23).
-- -----------------------------------------------------------------------------
SELECT TMP_CODE, TMP_DESCRIPTION
FROM USER_DWH.TYPE_MODE_PAIEMENT
ORDER BY TMP_CODE;


-- -----------------------------------------------------------------------------
-- P39 — Fréquences de paiement
-- Objectif : libellés des fréquences (TPE_FREQ_PAIEMENT, PE_FREQ_PAIEMENT) :
--            FAF et FAP trimestrielles, les autres mensuelles (P04).
-- Tableaux : T32 à T41.
-- Résultat : A annuel, BA bi-annuel, M mensuel, T trimestriel.
-- Constat  : Les types de prestation n'utilisent que M et T (P34).
-- -----------------------------------------------------------------------------
SELECT TFP_CODE, TFP_DESCRIPTION
FROM USER_DWH.TYPE_FREQUENCE_PAIEMENT
ORDER BY TFP_CODE;


-- -----------------------------------------------------------------------------
-- P40 — Types d'étape
-- Objectif : codes et libellés exacts des 13 étapes (dont les retenues RP5 à
--            RP25 et PCCM), avec le nombre d'étapes.
-- Tableaux : aucun directement (étapes de calcul, P12).
-- Résultat : 13 types, 1 705 341 étapes : CIP calcul initial basé sur les
--            paramètres (947 441), RP révision basée sur les paramètres
--            (588 919), MAJ majoration (167 810), RRR rabais pour rachat de
--            rente (472), RP5, RP10, RP15, RP20, RP25 retenues sur pension de
--            5 à 25 % (170, 140, 101, 61, 34), CORN correctif négatif (162),
--            CORP correctif positif (4), PCCM prélèvement contribution aux
--            charges du ménage (27), FS frais périodiques supplémentaires
--            (jamais utilisé).
-- Constat  : 947 441 calculs initiaux pour 947 953 prestations : 512
--            prestations n'ont pas de calcul initial (sans effet sur les
--            règles, qui ne lisent pas les étapes).
-- -----------------------------------------------------------------------------
SELECT t.TEA_CODE, t.TEA_DESCRIPTION,
       (SELECT COUNT(*) FROM USER_DWH.ETAPE e WHERE e.TEA_CODE = t.TEA_CODE) AS nb_etapes
FROM USER_DWH.TYPE_ETAPE t
ORDER BY t.TEA_CODE;


-- -----------------------------------------------------------------------------
-- P41 — Types d'ajustement
-- Objectif : codes et libellés exacts des 12 types (dont l'avance et le don
--            COVID), avec le nombre d'ajustements.
-- Tableaux : T31.
-- Résultat : 13 types, 1 355 405 ajustements : AV avance (760 871), MP
--            moins-perçu ou rappel (424 180), MPG mois de pension gratuit
--            (122 185), DON-COVID19 don kit sanitaire COVID 19 (22 591), TP
--            trop-perçu (12 480), RC rappel sur convention (6 272), REV-AV
--            reversement d'avance (3 857), AV-COVID19 avance COVID 19 (1 399),
--            TP-CONV trop-perçu sur convention (937), RR rachat de rente (379),
--            REM-TP remboursement de trop-perçu (242), OI opposition interne
--            (12), OE opposition externe (jamais utilisé).
-- Constat  : Les codes COVID sont AV-COVID19 et DON-COVID19. Ni l'un ni
--            l'autre n'est un indu (T31).
-- -----------------------------------------------------------------------------
SELECT t.TAJ_CODE, t.TAJ_DESCRIPTION,
       (SELECT COUNT(*) FROM USER_DWH.AJUSTEMENT a WHERE a.TAJ_CODE = t.TAJ_CODE) AS nb_ajustements
FROM USER_DWH.TYPE_AJUSTEMENT t
ORDER BY t.TAJ_CODE;


-- -----------------------------------------------------------------------------
-- P42 — Tranches d'âge
-- Objectif : codes des 21 tranches de 5 ans (P14), pour la table de
--            regroupement vers les 11 grilles CIPRES.
-- Tableaux : T32 à T49.
-- Résultat : 21 tranches, codes 1 (0-4) à 20 (95-99) et 21 (100+, bornes
--            100 et 200).
-- Constat  : Conforme à P14.
-- -----------------------------------------------------------------------------
SELECT TAG_CODE, TRANCHE, INF, SUP
FROM USER_DWH.TRANCHE_AGE
ORDER BY INF;


-- -----------------------------------------------------------------------------
-- P43 — Types d'accident
-- Objectif : libellés de TAT_CODE (DOSSIER et TSINISTRE).
-- Tableaux : statistiques AT/MP (DTM_ACCIDENT_TRAVAIL).
-- Résultat : AD accident de déplacement, ALOT accident au lieu occasionnel
--            de travail, ALT accident au lieu de travail, ATR accident de
--            trajet, MPR maladie professionnelle.
-- -----------------------------------------------------------------------------
SELECT TAT_CODE, TAT_DESCRIPTION
FROM USER_DWH.TYPE_ACCIDENT
ORDER BY TAT_CODE;


-- -----------------------------------------------------------------------------
-- P44 — Types de pièce
-- Objectif : libellés des pièces fournies à la réception (RECEPTION_DOS_PIECE).
-- Tableaux : aucun.
-- Résultat : 86 types de pièce (liste complète dans
--            resultats/lot3_P44_types_piece.tsv) : 80 actifs, 5 inactifs
--            (ANASCEND, ANENFANT, CG, CVENFANT, PHOTOID), RAPMED sans valeur.
-- -----------------------------------------------------------------------------
SELECT TPJ_CODE, TPJ_DESCRIPTION, TPJ_ACTIF
FROM USER_DWH.TYPE_PIECE
ORDER BY TPJ_CODE;


-- -----------------------------------------------------------------------------
-- P45 — Directions régionales
-- Objectif : liste des directions (DR_NO), qui servent d'axe dans les DTM.
-- Tableaux : tous (axe géographique).
-- Résultat : 0 SIEGE siège social (inactif), 10 DRO Ouagadougou, 20 DRB
--            Bobo-Dioulasso, 30 DRN Nord (Ouahigouya), 40 DRF Fada N'Gourma, 50
--            DRD Dédougou.
-- -----------------------------------------------------------------------------
SELECT DR_NO, DR_SIGLE, DR_DESC, DR_VILLE, DR_ACTIF
FROM USER_DWH.DIRECTION_REGIONALE
ORDER BY DR_NO;


-- -----------------------------------------------------------------------------
-- P46 — Services provinciaux
-- Objectif : liste des services (SP_NO) et de leur direction.
-- Tableaux : tous (axe géographique).
-- Résultat : 20 services (liste complète dans
--            resultats/lot3_P46_services_provinciaux.tsv). DRO : Kilwin,
--            Dassasgho, Tanghin, DRO siège, Koudougou ; DRB : Sarfalao, Sakaby,
--            DRB siège, Houndé, Banfora, Diébougou, Gaoua ; DRN : Dori, Kaya,
--            DRN siège ; DRF : DRF siège, Diapaga, Tenkodogo ; DRD : Tougan,
--            DRD siège. 4 inactifs (N) : Tanghin, Sakaby, Diapaga, Tougan ; les
--            autres sans valeur.
-- Constat  : Koudougou dépend de la DRO, Kaya de la DRN, Tenkodogo de la DRF.
-- -----------------------------------------------------------------------------
SELECT SP_NO, DR_NO, SP_SIGLE, SP_DESC, SP_VILLE, SP_ACTIVE
FROM USER_DWH.SERVICE_PROVINCIAL
ORDER BY DR_NO, SP_NO;


-- -----------------------------------------------------------------------------
-- P47 — Lieux de paiement
-- Objectif : liste des lieux (LP_NO), avec leur direction et leur service.
-- Tableaux : tous (axe géographique).
-- Résultat : 350 lieux (liste complète dans
--            resultats/lot3_P47_lieux_paiement.tsv) : siège 1, DRO 93, DRB 80,
--            DRN 78, DRF 56, DRD 42. DR_OU_SP vaut O pour 20 lieux, un par
--            service : le lieu où siège le service ; N pour 259 ; vide pour 71.
--            Lieux particuliers : « Pays d'Europe », « Pays d'Afrique » et
--            « CNSS-DCP » dans chaque direction, 5 républiques voisines à la
--            DRO.
-- Constat  : Le lieu 201 « caisse tenkodogo » est rattaché à la DRB, sans
--            service, alors que Tenkodogo dépend de la DRF. Le lieu 1110
--            SIEGE a la direction 0 mais le service 1090 (DRO). Les services
--            inactifs gardent leurs lieux (Diapaga 8, Tougan 11). Sens de
--            DR_OU_SP vide à demander à la DSI.
-- -----------------------------------------------------------------------------
SELECT LP_NO, DR_NO, SP_NO, DR_OU_SP, LP_DESC
FROM USER_DWH.LIEU_PAIEMENT
ORDER BY DR_NO, SP_NO, LP_NO;
