"""
Schéma « T50 à T57 : l'évolution sur cinq ans, les prestations servies par branche et les assurés
décédés, les règles en clair » (état au 08/11/2026).

Famille F5 des prestations « à l'image du GRH » : ce que demande le recueil des besoins v7, les
règles (R1 à R5, R9, R13, D4, A1, règles 6 et 7 de la fiche repris ; N1 à N11 nouvelles), les cas,
la comparaison avec l'annuaire statistique de la CNSS 2024, les réglages de la CTE parametres, les
questions ouvertes (Q1 à Q25), les points d'attention et ce qui reste à faire.

Le détail est dans analyses/prestations/regles_t50_t57_evolution.md : mêmes identifiants, mêmes
statuts, mêmes verdicts. Le travail existant (README, t50_t57_requete_proposee.sql, fiche
T50-T57, fiche de lecture, regles_prestations, points_a_valider) n'est pas modifié.

Usage :
    python3 excalidraw/sources/prestations/famille_t50_t57_evolution.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import main  # noqa: E402
from gabarit_famille import construire  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "T50 T57 evolution" / "regles_t50_t57_evolution.excalidraw"

RV7 = "Recueil v7"
REU = "Réunion"
KAYA = "Kaya 2017"
CIPRES = "Annuaire CIPRES"
CNSS = "Annuaire CNSS 2024"
MOD = "Modèles CNSS"
DON = "Données"
BI = "Proposition BI"

DATA = {
    "seed": 5057,
    "titre": "T50 à T57 : l'évolution sur cinq ans, les prestations servies et les décès",
    "sous_titre": (
        "Les règles en clair, état au 08/11/2026. T50 à T55 : six prestations suivies sur cinq "
        "ans (personnes et montants) ; T56 : les prestations servies par branche ; T57 : les "
        "assurés décédés par branche. Tout est proposé, rien n'est validé par la CNSS ; la requête "
        "t50_t57_requete_proposee.sql n'a jamais été exécutée (chiffres des lots 8 et 9, vus au "
        "02/02/2026). Le détail est dans analyses/prestations/regles_t50_t57_evolution.md."
    ),
    "en_bref": [
        "Ces tableaux comptent, sur cinq ans, les personnes et l'argent de six prestations (T50 à "
        "T55), l'argent versé par branche (T56) et les assurés décédés par branche (T57).",
        "Le recueil v7 change : le montant de T54 et T55 devient le « montant liquidé » (N1) ; T57 "
        "a un TOTAL au lieu de « Autres », un détail par type et un filtre « assuré ou ayant "
        "droit » (N2, N3) ; T51 compte les ascendants. T56 n'a aucune ligne dans le recueil.",
        "L'annuaire CNSS 2024 confirme : on compte la prestation liquidée, c'est-à-dire accordée "
        "et notifiée (831 allocataires de "
        "vieillesse pour 833 ; 3 890 mères indemnisées pour 3 893, sans départager la date). Il "
        "tranche : le rachat de rente "
        "entre dans T56 (N4), la ligne « Autres » est l'action sanitaire et sociale (N5).",
        "En écart : soins AT/MP (13 % dans la base), T56 AT/MP −28,9 %, IJ de maternité −4,2 % en "
        "montant, rentiers décédés 16 pour 24 ; retraités décédés 619 aujourd'hui pour 447, "
        "environ 444 vus fin mars 2025 (R13).",
        "La date d'un paiement : le rythme mensuel de l'annuaire suit la saisie ; sur l'année, les "
        "deux dates restent proches (AVS +1,3 % ou +1,8 %). À décider (Q1).",
        "Décisions attendues : Q1 (année d'un paiement), Q2 (arrêt fin mars ?), Q3 (montant "
        "liquidé), Q4 (l'assuré mort en activité dans T57), Q5 (T56 lu en comptabilité ?). Aucune "
        "ne bloque.",
        "Reste à faire : exécuter les trois requêtes, écrire P115 à P119, puis revoir T57 et T56 "
        "après le visa de la CNSS.",
    ],
    # ------------------------------------------------------------------ 0. les tableaux
    "tableaux": [
        ["T50", "Bénéficiaires d'allocation de vieillesse (versée en une fois), montants et nombre "
                "moyen de services passés, de N−4 à N ; HOMMES et FEMMES ; une ligne TOTAL",
         "p. 16, 46, 62, 66", "Requête 1 (pas exécutée) ; fiche, règles 1 à 5 ; lot 8 (P92, P94)"],
        ["T51", "Bénéficiaires d'allocation de survivants et montants ; HOMMES, FEMMES, TOTAL ; pas "
                "de ligne TOTAL. Le recueil ajoute les ascendants aux veufs et orphelins",
         "p. 16 et 46", "Requête 1 ; lot 8 (P92, P93)"],
        ["T52", "Bénéficiaires d'IJ des accidents du travail (AT/MP) et montants ; même forme que "
                "T51", "p. 16, 46, 64", "Requête 1 ; lot 8 (P92, P93)"],
        ["T53", "Bénéficiaires d'allocation d'incapacité (incapacité trop faible pour une rente) et "
                "montants ; même forme que T51", "p. 16 et 47", "Requête 1 ; lot 8 (P92)"],
        ["T54", "Bénéficiaires d'indemnités de maternité et montants ; colonne FEMMES ; Kaya 2017 "
                "ajoute une ligne TOTAL et veut les liquidées", "p. 16, 47, 64",
         "Requête 1 ; lot 8 (P92)"],
        ["T55", "Bénéficiaires d'allocations prénatales et montants ; même forme que T54 (Kaya : "
                "ligne TOTAL, liquidées)", "p. 16, 47, 62", "Requête 1 ; lot 8 (P92, P93)"],
        ["T56", "Prestations servies par branche sur cinq ans (montants) : PF, AT/MP, AVS, AUTRES, "
                "TOTAL ; soins compris (glossaire p. 65)", "p. 16, 48, 65",
         "Requête 2 ; fiche, règle 6 ; lots 8 et 9"],
        ["T57", "Décédés par branche sur cinq ans (personnes) : PF, AT/MP, AVS, AUTRES, sans total. "
                "Kaya 2017 : « assurés décédés déclarés »", "p. 16, 48, 63",
         "Requête 3 ; fiche, règles 7 et 8 ; lot 8 (P96 à P100)"],
    ],
    # ------------------------------------------------------------------ 1. recueil v7
    "recueil_note": (
        "Toutes les lignes du recueil v7 qui touchent T50 à T57 (numéros de besoins_v7.txt) ; la "
        "formule du ratio des ressources a été relue dans le document Word. T56 n'a aucune ligne ; "
        "T53 et T55 n'ont qu'un renvoi (« Idem »). Ne changent rien ici : l. 32 (pas de norme), "
        "virement (calculé hors famille ; N8 garde les paiements), taux de remplacement, ratios "
        "CIPRES respectés, colonnes « Priorité » vides. Six lignes qu'aucune autre famille ne "
        "classe sont rangées ici avec leur statut : satisfaction, SMI, CQP, nouvelles "
        "prestations, réclamations, document de référence."
    ),
    "recueil": [
        ("#28 (T50), l. 319", "« Service passé : total des périodes d'activités salarié en nombre "
         "de mois »", "Les services passés se comptent en mois de travail salarié",
         "R9 ; moyenne de la ligne TOTAL : N10", "pris en compte"),
        ("#28 (T50), l. 320", "« Périodicité : Annuelle »", "T50 une fois par an",
         "Requête 1, de N−4 à N", "pris en compte"),
        ("#29 (T51), l. 326", "« Allocation de survivants : veufs/veuves, orphelins et "
         "ascendants »", "Les ascendants s'ajoutent à l'Annuaire CIPRES",
         "R2 : RASC, RASO, RASA", "pris en compte"),
        ("#29 (T51), l. 327", "« Idem. Tab 52 » : la périodicité", "T51 une fois par an",
         "Requête 1", "pris en compte"),
        ("#16 (T52), l. 236", "« Périodicité : Annuelle »", "T52 une fois par an", "Requête 1",
         "pris en compte"),
        ("#17 (T53), l. 242", "« Idem. Tab 52 » : la périodicité", "T53 une fois par an",
         "Requête 1", "pris en compte"),
        ("#5 (T54), l. 159", "« Montant : Montant liquidé »", "Ce qui est dû pour les indemnités "
         "liquidées dans l'année, pas ce qui est payé dans l'année",
         "N1 ; montant_t54_t55 (Q3) ; remarque B15 du recueil", "à intégrer"),
        ("#5 (T54), l. 160", "« Périodicité : Annuelle »", "T54 une fois par an", "Requête 1",
         "pris en compte"),
        ("#6 (T55), l. 166, renvoi à l. 159", "« Idem Tab. 54 » : le montant liquidé",
         "Même règle pour les allocations prénatales", "N1 ; sens à confirmer (Q3)", "à intégrer"),
        ("#6 (T55), l. 166, renvoi à l. 160", "« Idem Tab. 54 » : la périodicité",
         "T55 une fois par an", "Requête 1", "pris en compte"),
        ("#7 (T57), l. 172", "« Assurés décédés »", "Kaya liste pension directe, rentier et "
         "sinistré, allocataire ; et l'assuré mort en activité ?", "Fiche 7, N2 (Q4)", "question"),
        ("#7 (T57), l. 173", "« Remplacer « Autres » par « TOTAL » »", "Colonnes PF, AT/MP, AVS, "
         "TOTAL", "N3 ; la requête 3 sort encore AUTRES (0)", "à intégrer"),
        ("#7 (T57), l. 174", "« Désagréger les branches par type de bénéficiaires »",
         "Retraité, veuve, rentier, sinistré, allocataire…", "N2", "à intégrer"),
        ("#7 (T57), l. 175", "« Ajouter un autre filtre : Assuré ou ayant droit »",
         "Montrer les assurés, les ayants droit ou les deux", "N2 ; qualite_deces ; PF (Q10)",
         "à intégrer"),
        ("#7 (T57), l. 176", "« Périodicité : Annuelle »", "T57 une fois par an", "Requête 3",
         "pris en compte"),
        ("Liste Prestation, l. 115 à 335", "Aucune ligne pour le tableau 56", "Rien sur T56 (ni "
         "sur T26, T39, T40) : Kaya ne les avait pas vus", "Requête 2 selon l'Annuaire CIPRES ; "
         "remarque B38 du recueil (Q21)", "question"),
        ("#4 (T41), l. 152", "« Allocataire : Titulaire du dossier (assuré) »",
         "L'allocataire est le titulaire du dossier", "Fiche 7, branche PF (FAF en cours)",
         "pris en compte"),
        ("#10 (T38), l. 194", "« Crédirentiers : rentiers »", "Le titulaire d'une rente de "
         "survivant est aussi un rentier", "Fiche 7 (AT/MP) ; N2 (ayant droit)", "pris en compte"),
        ("#25 (T42), l. 298", "Coordination : « prendre uniquement la partie CNSS »",
         "Seuls les mois cotisés à la CNSS ; B06 l'étend à T50", "R9 (moins PE_COT_AUTRE) ; sens "
         "du champ : T42-T49 Q10", "question"),
        ("#20 (T32), l. 263", "Coordination : les montants, « uniquement la partie CNSS »",
         "Seul ce que la CNSS paie", "T56 : paiements CNSS seuls (P78)", "pris en compte"),
        ("#3 (T31), l. 142 à 144", "PVID (branche pension) ; AT/MP (RP) ; PF",
         "Les noms des trois branches à la CNSS", "R1 ; écrire « AVS (PVID) », « AT/MP (RP) »",
         "à intégrer"),
        ("#1 (T27), l. 127", "Renommer « Autres prestations » en « Frais médicaux de maternité »",
         "Une prestation de la branche PF", "R1, Fiche 6 ; absente de la base (Q17)", "question"),
        ("Indicateurs PF, l. 1102", "« Allocations familiales et Indemnités Journalière de "
         "Maternité »", "Les IJ de maternité sont dans les PF", "R1", "pris en compte"),
        ("Ratio des ressources, l. 1129 à 1141 (note l. 1133), 1210 à 1214, 1287 à 1294",
         "Prestations ordonnancées ÷ cotisations ordonnancées × 100 ; ordonnancées = notifiées ; "
         "type, DR, agence", "Les prestations comptent à la notification", "N11 ; soins (Q20)",
         "à intégrer"),
        ("Ratio des ressources PF, l. 1137 à 1139", "« Trimestriel / Annuel »",
         "La branche PF se lit aussi par trimestre", "N8 (le mois dans le détail)",
         "à intégrer"),
        ("Indicateurs pensions, l. 1251", "Retraités = pensions directes (vieillesse, anticipée, "
         "invalidité)", "Les mêmes codes que l'AVS de T57", "Fiche 7 (RPV, RPRA, RPI)",
         "pris en compte"),
        ("Indicateurs ASS, l. 1318 à 1337", "Charges administratives de l'ASS ; couverture des "
         "dépenses ; DCG", "L'ASS a ses indicateurs", "N5 : même montant d'ASS (Q18)",
         "question"),
        ("#30 (T76), l. 333 et 334", "Ratio en %, 2 décimales ; annuel", "Tableau de l'ASS, hors "
         "famille", "N5 : même montant ; Kaya : Excel", "hors base"),
        ("Finance #1 (T66), l. 361 (372, 378)", "« Charges techniques : 601xxx, 602xxx, 603xxx, "
         "605xxx »", "Les prestations vues par la comptabilité", "N6 (Q5)", "question"),
        ("Finance #11 (T74), l. 445 à 449", "Rapport de charges ; norme strictement sous 100 %",
         "Prestations ÷ cotisations, côté finance", "Même numérateur que N11", "pris en compte"),
        ("Finance #12 (T75), l. 455, 461 et 464", "« Prestation de l'année N-1 (par type) / Reserve »",
         "Les prestations de N−1 par branche", "T56 les donne ; N6", "pris en compte"),
        ("Termes de référence, l. 4 à 10", "Cinq directions régionales", "« Par DR » = ces cinq",
         "N8", "à intégrer"),
        ("Indicateurs, l. 1065", "« Donner la possibilité de désagréger l'indicateur »",
         "Chaque chiffre se découpe", "N8", "à intégrer"),
        ("Attentes, l. 29", "Données « traquées jusqu'au niveau de granularité le plus "
         "élémentaires »", "Jusqu'à la prestation, au paiement, au décès", "N8", "à intégrer"),
        ("Attentes, l. 31 et 74", "Présentation multidimensionnelle ; dimension ajoutée valable "
         "partout", "Mêmes axes pour toute la famille", "N8", "à intégrer"),
        ("Attentes, l. 35", "Rapports « à des fréquences adaptées »", "Toute la famille est "
         "annuelle", "Requêtes annuelles", "pris en compte"),
        ("Attentes, l. 28, 34, 75", "Très peu d'intervention manuelle", "Pas de saisie à la main",
         "Requêtes ; source automatique (N5, N6)", "pris en compte"),
        ("Caractéristiques, l. 53 à 55 et 60", "Chargement de fichiers et de bases",
         "Un fichier Excel peut se charger", "Possible pour l'ASS (N5)", "pris en compte"),
        ("Caractéristiques, l. 58", "Règles de transcodage écrites et rejouables",
         "Codes et tableaux écrits", "R2", "pris en compte"),
        ("Caractéristiques, l. 72", "Accès aux données par profil", "Les décédés sont des "
         "données personnelles", "T57 ne lit pas la vue des décédés (Q23)", "pris en compte"),
        ("Attentes, l. 24 et 78", "Profilage de la qualité des données", "Mesurer les trous",
         "Lots 8 et 9 ; points d'attention", "pris en compte"),
        ("Général, l. 15 et 92 à 94", "La source : l'application des prestations",
         "Copie USER_DWH au 02/02/2026", "Toutes les règles", "pris en compte"),
        ("Indicateur 1, l. 1075 à 1084", "« La proportion des clients satisfaits » ; « Données "
         "non disponibles en base »", "Une enquête auprès des assurés, toutes branches",
         "Le recueil le dit : pas dans la base", "hors base"),
        ("Indicateur ASS, l. 1297 à 1304", "Clients pris en charge « selon les normes » ; par "
         "SMI", "Les soins des centres de santé maternelle et infantile", "Proposition BI : "
         "pas dans l'application des prestations", "hors base"),
        ("Indicateur ASS, l. 1308 à 1316", "« Le taux de réussite au CQP » ; « Source : Excel »",
         "Une formation, suivie dans un fichier Excel", "Source Excel, dit le recueil",
         "hors base"),
        ("Nouvelles prestations, l. 1340 à 1356", "« Proportion de nouvelles prestations "
         "promues » ; « A confirmer avec DAI et DPC »", "La CNSS doit d'abord le définir",
         "Rien à calculer en attendant (Q24)", "question"),
        ("Réclamations, l. 1358 à 1368", "« La variation du nombre de réclamations » ; "
         "« Source : Excel (les DR) »", "Réclamations de n+1 moins celles de n",
         "Fichiers Excel des DR, dit le recueil", "hors base"),
        ("Document de référence, l. 80 et 81", "« Document de référence des tableaux de la "
         "CNSS »", "Annoncé, ni nommé ni joint", "À obtenir (Q25)", "question"),
    ],
    # ------------------------------------------------------------------ 2. règles
    "regles": [
        {"id": "R1", "titre": "Les branches",
         "regle": "V = AVS (que la CNSS appelle PVID) ; A = AT/MP (ou RP) ; F et M = PF, "
                  "maternité comprise.",
         "exemple": "une IJ de maternité (MIJ) compte dans la ligne PF de T56 ; les frais "
                    "médicaux de maternité aussi.",
         "sources": [(RV7, "l. 1102, 127, 142 à 144"), (CNSS, "tableaux 40 et 106"),
                     (MOD, "feuilles 03 à 06 : cohérent"), (DON, "P02, P05, P95")],
         "etat": "confirmée par l'annuaire CNSS 2024"},
        {"id": "R2", "titre": "Les prestations de chaque tableau (fiche, règle 1)",
         "regle": "T50 RAV ; T51 RASC, RASO, RASA (RAS ne se liquide plus) ; T52 AIJ ; T53 AAI ; "
                  "T54 MIJ ; T55 MAP et FAP.",
         "exemple": "2024 : T51 = 81 parts de conjoint, 472 d'orphelin, 18 d'ascendant.",
         "sources": [(RV7, "l. 326 : les ascendants"), (CIPRES, "p. 16"), (DON, "P93")],
         "etat": "proposée ; ascendants tranchés par le recueil ; codes à viser (points_a_valider "
                 "n° 18)"},
        {"id": "D4", "titre": "Liquidé : le bénéficiaire de l'année (fiche, règle 2)",
         "regle": "Une personne compte l'année où une prestation du tableau lui est notifiée, "
                  "une fois dans l'année ; elle peut compter plusieurs années.",
         "exemple": "allocation notifiée le 10/03/2024 : bénéficiaire de 2024.",
         "sources": [(KAYA, "l. 130 et 140 : liquidées, pas payées"),
                     (CNSS, "tableaux 24 (831 pour 833) et 35 (3 890 pour 3 893)"),
                     (DON, "P92 ; fait B23 du schéma des règles")],
         "etat": "confirmée par l'annuaire pour T50 ; précisée pour T54 (tableau 35) ; T51 à T53 "
                 "et T55 par analogie (Q8) ; "
                 "même sens que D4 de T25-T27, appliqué à chaque prestation"},
        {"id": "R5", "titre": "La personne comptée (fiche, règle 3)",
         "regle": "L'ayant droit, une fois par tableau et par année, avec son sexe ; l'orphelin "
                  "et non son tuteur. T54 et T55 : des femmes.",
         "exemple": "trois IJ liquidées en 2024 : une victime (371 IJ pour 195 victimes).",
         "sources": [(CIPRES, "glossaire p. 62 : une personne"),
                     (CNSS, "tableau 20 : orphelins de 0 à 24 ans"), (DON, "P92, P93")],
         "etat": "proposée ; « une personne » tranché par le glossaire (n° 50, fiche Q5) ; orphelin "
                 "confirmé par l'annuaire"},
        {"id": "R3", "titre": "Ce qu'est un paiement",
         "regle": "Un débours PM au statut D (payé par un effet) ou P (payé sans effet) ; son "
                  "montant, jamais le net à payer de l'effet.",
         "exemple": "un débours validé jamais décaissé (statut V) ne compte pas.",
         "sources": [(DON, "P20, P95 ; fait B5 du schéma des règles"),
                     (CNSS, "tableau 106 : trois branches à moins de 0,6 %")],
         "etat": "confirmée par l'annuaire CNSS 2024"},
        {"id": "R4", "titre": "L'année d'un paiement",
         "regle": "Aujourd'hui, le mois payé. La CNSS semble compter un paiement à sa saisie (sa "
                  "préparation, quelques semaines avant le versement), rappels compris.",
         "exemple": "rappel de 15 mois saisi en mars 2024 : 12 mois en 2023 au mois payé ; tout "
                    "en 2024 à la saisie.",
         "sources": [(CNSS, "tableaux 28, 41, 90 : rythme de la saisie ; tableau 34 : +0,02 %"),
                     (DON, "P98 : saisie meilleure sur 11 lignes sur 13 ; P105")],
         "etat": "à revoir ; réglage date_montant ; posée au point du 02/11/2026 (Q1)"},
        {"id": "N1", "titre": "T54 et T55 : le montant liquidé",
         "regle": "Tous les paiements des prestations liquidées dans l'année, quelle que soit "
                  "leur date (ou le montant calculé à la liquidation).",
         "exemple": "IJ liquidée en décembre 2024, payée en décembre et en février 2025 : les deux "
                    "paiements dans T54 de 2024.",
         "sources": [(RV7, "l. 159 et 166"), (KAYA, "l. 130 et 140"),
                     (DON, "2024 : 2 745 737 842 payés, 2 865 630 056 publiés ; P116 à faire")],
         "etat": "nouvelle, imposée par le recueil ; sens à confirmer (Q3)"},
        {"id": "R9", "titre": "Les services passés de T50 (fiche, règle 5)",
         "regle": "Mois cotisés de la dernière allocation de l'année (1 à 600) ; en coordination, "
                  "la part CNSS ; moyenne par sexe.",
         "exemple": "2024 : 831 personnes, 95,5 mois (part CNSS), médiane 98.",
         "sources": [(RV7, "l. 319 ; l. 298 (B06)"), (CIPRES, "p. 46, glossaire p. 66"),
                     (DON, "P94 ; 489 mois au plus (P30)")],
         "etat": "proposée ; unité confirmée par le recueil ; mois nombreux à vérifier (Q9)"},
        {"id": "N10", "titre": "Les lignes TOTAL de T50 à T55",
         "regle": "T50 : la moyenne des services passés sur cinq ans, chaque année selon son "
                  "effectif. T54, T55 : une "
                  "ligne TOTAL. T51 à T53 : ajoutée, sans gêne.",
         "exemple": "2020 à 2024 : environ 90,8 mois (recalcul sur P94).",
         "sources": [(CIPRES, "maquette T50 p. 46"), (KAYA, "l. 129 et 139"), (DON, "P94")],
         "etat": "nouvelle (petite correction de la requête 1)"},
        {"id": "A1", "titre": "Les accessoires de pension dans T56",
         "regle": "Bonification et majorations pour enfant, charges du ménage : dans l'AVS.",
         "exemple": "2024 : 539 376 000 saisis, 537 343 000 publiés.",
         "sources": [(CNSS, "tableaux 28 et 34"), (DON, "P34, P98")],
         "etat": "confirmée par l'annuaire CNSS 2024"},
        {"id": "Fiche 6", "titre": "T56 aujourd'hui (règle 6 de la fiche)",
         "regle": "Paiements (PM) et remboursements de soins (RB) par branche, sans les "
                  "ajustements ; « Autres » vide ; un TOTAL.",
         "exemple": "2024 : AVS +1,3 %, PF −3,8 %, AT/MP −28,9 % ; les trois branches −0,2 %.",
         "sources": [(CIPRES, "p. 16, 48 ; glossaire p. 65"), (DON, "P95, P98, P104")],
         "etat": "à revoir : N4, N5, N6, frais de maternité (Q17), rappels (Q19)"},
        {"id": "N4", "titre": "T56 : les rachats de rente",
         "regle": "Le capital versé à la place d'une rente AT/MP est une prestation servie : on "
                  "ajoute les débours d'ajustement RR.",
         "exemple": "2024 : 21 519 361 dans la base, 25 366 909 publiés.",
         "sources": [(CNSS, "tableaux 90, 96, 97"), (KAYA, "l. 173 : achat de rente"),
                     (DON, "P104")],
         "etat": "nouvelle, tranchée par l'annuaire et Kaya (n° 52 réglé pour les rachats)"},
        {"id": "N5", "titre": "T56 : la ligne « Autres » = l'ASS",
         "regle": "La ligne « Autres » reçoit l'action sanitaire et sociale, hors de la base des "
                  "prestations ; même montant que les indicateurs de l'ASS et T76.",
         "exemple": "2024 : 650 700 099 au tableau 106 ; 1 571 655 315 au tableau 114.",
         "sources": [(CNSS, "tableau 106, fiche signalétique p. xx"),
                     (RV7, "l. 1318 à 1337 ; l. 28"), (KAYA, "l. 395 : T76 sous Excel")],
         "etat": "nouvelle ; contenu tranché par l'annuaire (n° 53) ; montant à choisir (Q18)"},
        {"id": "N6", "titre": "T56 : application ou comptabilité",
         "regle": "T56 peut se lire dans les charges techniques (comptes 601, 602, 603, 605), "
                  "par V_BI_BALANCE_COMPTABLE ; répartition par branche à demander à la DFC.",
         "exemple": "2024, AT/MP : 1 074 113 823 en charges ; 763 397 429 payés dans la base.",
         "sources": [(CNSS, "démarche p. xiv ; tableaux 103 à 105"), (RV7, "l. 361, 455, 464")],
         "etat": "nouvelle, option à décider (Q5) ; par défaut l'application"},
        {"id": "N11", "titre": "Le ratio des ressources allouées",
         "regle": "Prestations notifiées ÷ cotisations ordonnancées × 100. T56 le nourrit avec la "
                  "date de notification et peut-être sans les soins ; PF par trimestre.",
         "exemple": "repère 2024 recalculé (non publié), tableaux 103 à 105 : pensions 45,96 %, "
                    "PF 25,91 %, RP 9,82 %.",
         "sources": [(RV7, "l. 1129 à 1141, note l. 1133"), (KAYA, "l. 898"),
                     (CIPRES, "glossaire p. 65 : rapport de charges"),
                     (CNSS, "tableaux 17, 103 à 105")],
         "etat": "nouvelle, à valider avec la DAI et la DPC (Q20)"},
        {"id": "Fiche 7", "titre": "T57 aujourd'hui (règle 7 de la fiche)",
         "regle": "À la date du décès : AVS, pension directe en cours ; AT/MP, rente en cours et "
                  "victime morte dans l'année ; PF, allocation familiale en cours.",
         "exemple": "2024 : 622 pensions directes, 16 rentiers, 10 victimes, 194 allocataires.",
         "sources": [(KAYA, "l. 150 à 155"), (RV7, "l. 1251, 152, 194"),
                     (DON, "P96, P97, P100")],
         "etat": "à revoir (N2, N3) ; mélange assurés et ayants droit ; actifs absents (Q4)"},
        {"id": "N2", "titre": "T57 : qualité et type du décédé",
         "regle": "Assuré (retraité, rentier victime, sinistré, allocataire) ou ayant droit "
                  "(veuve, veuf, orphelin, ascendant) ; détail par type ; un filtre.",
         "exemple": "une veuve pensionnée qui meurt : comptée avec le filtre « ayant droit ».",
         "sources": [(RV7, "l. 174 et 175"), (KAYA, "l. 156"), (CNSS, "tableaux 26 et 81"),
                     (DON, "P97 : 622 ou 747")],
         "etat": "nouvelle, imposée par le recueil ; filtre « assuré » par défaut ; fiche Q4 et n° 54 "
                 "tranchés (les deux, au choix) ; restent Q4, Q10"},
        {"id": "N3", "titre": "T57 : colonnes et TOTAL",
         "regle": "Colonnes PF, AT/MP, AVS et TOTAL (« Autres » remplacé) ; une ligne TOTAL des "
                  "cinq années.",
         "exemple": "ligne 2024 : PF, AT/MP, AVS, puis leur total.",
         "sources": [(RV7, "l. 173"), (KAYA, "l. 151")],
         "etat": "nouvelle, imposée par le recueil ; une personne peut compter deux fois (Q11)"},
        {"id": "N9", "titre": "T57 : le sinistré",
         "regle": "La victime morte de son accident. La CNSS compte les décès immédiats ; la "
                  "requête 3, toute mort dans l'année qui suit l'accident.",
         "exemple": "2024 : 14 décès immédiats publiés ; 9 victimes mortes dans l'année.",
         "sources": [(CNSS, "tableau 49 : 13, 11, 13, 11, 14"), (KAYA, "l. 154"),
                     (DON, "P100 : champ rempli à 2 % en 2024")],
         "etat": "nouvelle, définition à décider (Q12)"},
        {"id": "R13", "titre": "Le cliché du calcul (fiche, règle 8)",
         "regle": "L'exercice N se calcule sur la copie de fin mars N+1 ; un décès compte à sa "
                  "date s'il est déjà saisi.",
         "exemple": "mort le 12/06/2024, saisi en février 2025 : dans T57 de 2024 ; saisi en mai : "
                    "non.",
         "sources": [(CNSS, "tableau 26 : 447"),
                     (DON, "P99 : environ 444 (pension), 404 (personne) ; pas de date de "
                           "déclaration")],
         "etat": "proposée, indice fort ; à confirmer par la DPC (Q2) ; répond aussi au n° 55 "
                 "(année du décès)"},
        {"id": "N7", "titre": "Les années passées",
         "regle": "Les années N−4 à N−1 reprennent les chiffres déjà produits ; une correction se "
                  "signale par une note.",
         "exemple": "IJ de maternité 2020 : 1,53 Md publiés, 2,19 Md recalculés.",
         "sources": [(CNSS, "tableau 114 (note) et 35 (2020, à vérifier)"), (DON, "P92")],
         "etat": "nouvelle, proposition (Q6)"},
        {"id": "N8", "titre": "Le détail : mois, DR, agence, code, sexe",
         "regle": "Garder ces axes pour chaque bénéficiaire, paiement et décès : descendre "
                  "jusqu'à la prestation, sortir un trimestre.",
         "exemple": "833 allocations de vieillesse en 2024, dont 507 à la DR de Ouagadougou.",
         "sources": [(RV7, "l. 4 à 10, 29, 31, 74, 1065, 1137"),
                     (CNSS, "tableaux 24, 29 à 46, 81 à 98"),
                     (DON, "A02 : proche, sauf les veuves")],
         "etat": "nouvelle ; les requêtes sortent des totaux annuels"},
    ],
    # ------------------------------------------------------------------ 3. cas
    "cas_note": (
        "Les 12 cas les plus parlants (18 dans le md, mêmes numéros). Chaque carte dit ce que "
        "devient la personne ou le paiement, et selon quelle règle."
    ),
    "cas": [
        ("Cas 2. Liquidée en décembre, payée en janvier", "Allocation de vieillesse liquidée le "
         "20/12/2024 ; débours saisi le 15/01/2025 pour le mois de décembre.",
         "RÉGLAGE date_montant : effectif 2024 ; montant 2024 (MOIS) ou 2025 (SAISIE)",
         "reglage"),
        ("Cas 3. L'assuré mort en activité", "Sa veuve et ses deux enfants reçoivent une part "
         "d'allocation de survivants en 2024.",
         "COMPTÉ : T51, trois bénéficiaires (R5) ; l'assuré absent de T57 (Q4)", "compte"),
        ("Cas 4. Trois IJ pour un blessé", "Arrêt prolongé deux fois : trois IJ d'accident "
         "liquidées en 2024.", "COMPTÉ UNE FOIS dans T52 (R5)", "compte"),
        ("Cas 5. L'IJ notifiée l'année suivante", "Arrêt en octobre 2024 ; IJ notifiée en mai "
         "2025 (7 mois d'attente, fait B24).", "PAS COMPTÉ EN 2024 : dans T52 de 2025 (D4)",
         "pas compte"),
        ("Cas 6. La maternité payée en deux fois", "IJ liquidée en décembre 2024, payée en "
         "décembre et en février 2025.", "RÉGLAGE montant_t54_t55 : un ou deux paiements (N1, Q3)",
         "reglage"),
        ("Cas 8. Le rachat d'une rente", "Capital versé en 2024 à la place de la rente, par un "
         "débours d'ajustement RR.", "PAS DANS T56 AUJOURD'HUI : à ajouter (N4)", "attention"),
        ("Cas 9. Les soins payés à une clinique", "Soins d'un accidenté : 18,8 M dans la base "
         "pour 139,7 M publiés en 2024.", "QUESTION Q16 : où sont payés les soins ?", "question"),
        ("Cas 11. Décès saisi en mai 2025", "Retraité mort le 10/11/2024 ; absent du calcul de "
         "fin mars 2025, comme de l'annuaire.", "À SURVEILLER : il reviendrait dans un recalcul "
         "(R13, N7, Q6)", "attention"),
        ("Cas 12. La veuve pensionnée qui meurt", "Pension de veuve (RPSV) ; l'annuaire compte "
         "103 veuves décédées en 2024.", "RÉGLAGE qualite_deces : comptée avec « ayant droit » "
         "(N2)", "reglage"),
        ("Cas 14. Morte cinq mois après l'accident", "Blessée en mars 2024, morte en août 2024, "
         "sans rente : pas un décès immédiat.", "QUESTION Q12 : sinistrée ou non ? (N9)",
         "question"),
        ("Cas 16. Retraité et allocataire", "Il touche une pension et des allocations "
         "familiales ; il meurt en 2024.", "QUESTION Q11 : compté deux fois dans le TOTAL (N3)",
         "question"),
        ("Cas 18. Mort sans prestation à son nom", "Assuré salarié mort en 2024 ; ses survivants "
         "touchent une pension.", "QUESTION Q4 : « assuré décédé » au sens du recueil ?",
         "question"),
    ],
    # ------------------------------------------------------------------ 4. annuaire
    "annuaire_note": (
        "L'annuaire ne publie pas les effectifs de T51, T52, T53 et T55, ni les services passés, "
        "ni le sexe, ni les décès d'allocataires. Il contrôle les effectifs de T50 et T54, les "
        "montants de 2024 par prestation, les montants AT/MP et par branche sur cinq ans, les "
        "décès de pensionnés et de rentiers."
    ),
    "annuaire": [
        ("Tableau 24 (p. 19)", "Allocations de vieillesse liquidées 2024 (T50)", "831 (832 "
         "allocations)", "833", "−0,24 %", "confirme", "D4 : « liquidées » ; garder mois et DR "
         "(N8)"),
        ("Tableaux 34 et 28", "Montant des allocations de vieillesse 2024", "497 818 548 "
         "(saisie)", "497 726 931", "+0,02 % ; +3,9 % au mois payé", "precise",
         "Appuie fortement SAISIE pour cette ligne (Q1)"),
        ("Tableaux 34 et 28", "Montant des allocations de survivants 2024 (T51)", "190 149 232 "
         "(saisie)", "197 906 991", "−3,9 % ; −5,5 % au mois payé", "precise",
         "4 % à expliquer ; effectif non publié"),
        ("Tableaux 28, 41, 90", "Rythme mensuel des montants 2024 (R4)", "corrélation 0,75 à "
         "0,92 (saisie)", "profils mensuels", "année : AVS +1,3 % ou +1,8 %", "precise",
         "Penche pour la saisie sans trancher ; P115"),
        ("Tableau 35 (p. 30)", "IJ de maternité, effectifs 2020 à 2024 (T54)", "3 438 … 3 890",
         "3 428 … 3 893", "−3,5 % à +0,5 %", "precise", "D4 retombe (seule 2022 hors de 1 %), "
         "sans départager la date, comme T25-T27"),
        ("Tableau 35 (p. 30)", "IJ de maternité, montants 2020 à 2024", "2 193 049 270 … "
         "2 749 474 700", "1 530 554 843 … 2 865 630 056", "+43,3 % … −4,1 %", "ecart",
         "2023-2024 sous le publié ; 2020 incomplet ; mesurer N1 (P116)"),
        ("Tableau 39 (p. 32)", "IJ de maternité 2024 par mois et DR", "3 890 sur l'année",
         "3 893", "−0,1 %", "precise", "Sans doute le mois de liquidation, à vérifier (N8)"),
        ("Tableau 40 (p. 32)", "Allocations prénatales 2024, montant (T55)", "52 149 000 "
         "(saisie)", "52 789 000", "−1,2 % ; −4,0 % au mois payé", "precise",
         "Effectif non publié (3 912 chez nous)"),
        ("Tableau 40 (p. 32)", "Total de la branche PF 2024 (T56)", "10 802 063 700 (saisie)",
         "11 180 191 425", "−3,4 % ; −3,8 % au mois payé", "ecart",
         "Allocations familiales 62 % de l'écart ; frais de maternité absents (Q17)"),
        ("Tableau 26 (p. 21)", "Retraités décédés 2024 : date d'arrêt (R13)", "619 ; environ "
         "444 (pension) ; 404 (personne)", "447", "+38,5 % ; −0,7 % ; −9,6 %", "precise",
         "Indice fort pour fin mars N+1 (Q2)"),
        ("Tableau 26 (p. 21)", "Pensionnés décédés : qui compter (N2)", "622 directs ; 747 tous",
         "451 directs ; 197 ayants droit", "+37,9 % ; +15,3 %", "precise",
         "La CNSS détaille par type ; 89 ascendants impossibles (Q15)"),
        ("Tableau 81 (p. 62)", "Rentiers décédés 2024", "16", "24", "−33 %", "ecart",
         "Condition « rente en cours » ? (P117, Q14)"),
        ("Tableau 49 (et 57, 64, 66, 69)", "Décès immédiats 2020 à 2024 (sinistrés)", "20, 17, "
         "24, 16, 9", "13, 11, 13, 11, 14", "+7 … −5", "precise", "Mort sur le coup seulement "
         "(N9, Q12, Q13)"),
        ("Tableau 97 (p. 74)", "IJ AT/MP, montants 2020 à 2024 (T52)", "51 859 178 en 2024 "
         "(saisie)", "65 420 082", "−20,7 % ; décaissement −7,1 %", "ecart",
         "La date retenue joue ; P115 sur cinq ans"),
        ("Tableau 97 (p. 74)", "Allocations d'incapacité 2020 à 2024 (T53)", "17 812 800 en "
         "2024", "20 034 111", "+1,3 % … −14,1 %", "ecart", "Écart plus grand en 2023 et 2024"),
        ("Tableaux 90, 96, 97", "Rachats de rente", "21 519 361 (2024)", "25 366 909",
         "−15,2 %", "tranche", "Les ajouter à T56 (N4)"),
        ("Tableaux 82, 88, 89", "Soins AT/MP 2020 à 2024", "18 787 945 (2024)", "139 668 695",
         "13 à 23 % couverts", "ecart", "Source à trouver (Q16) ou comptabilité (N6)"),
        ("Tableau 106 (p. 80)", "Prestations servies par branche 2020 à 2024 (T56)",
         "AVS 47,24 ; PF 10,75 ; AT/MP 0,76 Md (2024)", "46,63 ; 11,18 ; 1,07 Md",
         "+1,3 % ; −3,8 % ; −28,9 %", "ecart", "Total −0,2 % par compensation ; chaque écart a "
         "sa cause"),
        ("Tableau 106, fiche signalétique, tableau 114", "La ligne « Autres » : l'ASS", "vide",
         "650 700 099 ; 1 571 655 315", "ligne entière", "tranche", "Autres = ASS (N5) ; montant "
         "à choisir (Q18)"),
        ("Tableaux 103 à 105, démarche p. xiv", "Les charges techniques par branche",
         "paiements de la base", "= tableau 106", "celui du tableau 106", "precise",
         "Option comptable (N6, Q5)"),
        ("Tableaux 17 et 103 à 105", "Prestations ÷ cotisations 2024 (N11)", "pas calculé",
         "non publié ; recalculé : 45,96 % ; 25,91 % ; 9,82 %", "—", "precise", "Repère ; le recueil compte les "
         "notifiées"),
        ("Tableaux 114 et 35 (2020)", "Les années passées (N7)", "2 193 049 270", "1 530 554 843",
         "+43 %", "precise", "Repris tel quel ? À vérifier (Q6)"),
        ("Tableau 20 (p. 17)", "Orphelins par âge et sexe", "—", "9 478 de moins de 25 ans",
         "—", "confirme", "R5 : l'orphelin, pas le tuteur"),
        ("Tableaux 29 à 33", "Vieillesse servie par mois, une DR par tableau", "pas calculé",
         "DRO 32,17 Md … DRD 0,72 Md", "—", "precise", "Modèle de N8 ; tester R4 par DR"),
        ("Tableaux 91 à 95", "AT/MP en espèces par mois, une DR par tableau", "pas calculé",
         "DRO 621 802 712 … DRD 21 297 717", "—", "precise", "Où manquent les paiements AT/MP"),
        ("Tableaux 83 à 87", "Soins AT/MP par mois, une DR par tableau", "pas calculé",
         "DRO 96 737 361 … DRD 6 087 089", "—", "precise", "Soins absents par DR (Q16)"),
        ("Tableaux 78, 80 (et 75)", "Rentes en cours fin 2024", "pas calculé", "1 255 viagères ; 977 "
         "d'ayants droit ; 44 nouvelles viagères", "14 sorties non expliquées", "precise", "Le stock face aux décès de "
         "rentiers (Q14)"),
        ("Tableaux 82, 88, 89 (funéraires)", "Frais funéraires AT/MP 2020 à 2024", "pas calculé",
         "0 ; 600 000 ; 0 ; 700 000 ; 900 000", "—", "non comparable", "Indice des sinistrés ? "
         "À vérifier"),
        ("Aucun tableau", "Effectifs T51, T52, T53, T55 ; services passés ; sexe ; décès PF",
         "571 ; 195 ; 53 ; 3 912 ; 95,5 mois", "rien", "—", "non publie", "À valider avec la "
         "CNSS (fiche T50-T57)"),
    ],
    "annuaire_apres": (
        "Confirme : la prestation liquidée (D4, tableau 24), l'orphelin (R5) ; la maternité dans "
        "les PF (R1) et l'accessoire avec la pension (A1), cartes de T25-T27 et T28-T31. Tranche : les rachats dans T56 (N4), « Autres » = ASS "
        "(N5). Précise : les IJ de maternité liquidées (D4, tableau 35), la date d'un paiement (R4), l'arrêt fin mars (R13), qui compter dans T57 "
        "(N2, N9), la source comptable (N6). À expliquer : soins et IJ AT/MP, IJ de maternité "
        "2023-2024, allocations familiales, rentiers décédés, 89 ascendants."
    ),
    # ------------------------------------------------------------------ 5. réglages
    "reglages_note": (
        "Dans la CTE parametres en tête de chaque requête. « À créer » : le choix est écrit en dur, "
        "ou n'existe pas encore. Une décision attendue de la CNSS ne bloque rien : la valeur "
        "retenue s'applique en attendant."
    ),
    "reglages": [
        ("annee_n", "requêtes 1, 2, 3", "l'exercice, donné au lancement", "—",
         "Fixe les années N−4 à N", "la BI, à chaque lancement"),
        ("date_montant", "requête 1 (T50 à T53)", "MOIS (comme aujourd'hui)",
         "SAISIE (proposition BI)", "T50 2024 : 517 334 647 ou 497 818 548 (497 726 931 "
         "publiés) ; même réglage dans T31, T32-T41, T42-T49", "DPC, avec la DCP (Q1)"),
        ("date_montant", "requête 2 (T56)", "MOIS", "SAISIE", "AVS +1,3 % ou +1,8 % ; PF "
         "−3,8 % ou −3,4 %", "même décision (Q1)"),
        ("montant_t54_t55 (à créer)", "requête 1 (T54, T55)", "LIQUIDE (le recueil)",
         "PAYE (comme aujourd'hui)", "Chiffre à mesurer (P116)", "tranché par le recueil ; la "
         "DCP précise le sens (Q3)"),
        ("mois_cotises_max (à créer ; en dur : 600)", "requête 1 (T50)", "600",
         "le seuil d'une pension, à faire donner", "Les allocations aux carrières longues (489 "
         "mois au plus)", "DCP (Q9)"),
        ("qualite_deces (à créer)", "requête 3 (T57)", "ASSURE (titre, Kaya 2017)",
         "AYANT_DROIT ; TOUS ; ou une colonne QUALITE", "AVS 2024 : 622 ou 747",
         "l'utilisateur du rapport ; défaut avec la DPC"),
        ("actifs_decedes (à créer)", "requête 3 (T57)", "NON (Kaya ne les cite pas)", "OUI",
         "À mesurer (P117)", "DCP, avec la DPC (Q4)"),
        ("delai_sinistre_jours (à créer ; en dur : 366)", "requête 3 (T57)", "366", "0 (mort le "
         "jour de l'accident)", "20, 17, 24, 16, 9 contre 13, 11, 13, 11, 14 publiés",
         "DCP, service AT/MP (Q12)"),
        ("marge_radiation_jours (à créer ; en dur : 31)", "requête 3 (T57)", "31",
         "aucune condition", "Rentiers 2024 : 16 pour 24 publiés", "la BI, puis la DCP (Q14)"),
        ("types_debours_t56 (à créer ; en dur : PM, RB)", "requête 2 (T56)", "PM, RB et "
         "rachats RR", "sans rachats ; ou avec les rappels", "AT/MP +21 519 361 ; tous les "
         "ajustements : AVS +2,6 %", "rachats tranchés (N4) ; rappels : DCP (Q19)"),
        ("nature_t56 (à créer)", "requête 2 (T56)", "OUI (soins compris)", "NON (espèces "
         "seules, pour le ratio)", "18 787 945 de soins AT/MP en 2024", "DAI et DPC (Q20)"),
        ("ligne_autres_t56 (à créer)", "requête 2 (T56)", "vide", "l'ASS, hors de la base",
         "+650 700 099 (tableau 106) ou +1 571 655 315 (tableau 114)",
         "DPC, DPASS, DCG (Q18)"),
        ("source_t56 (à créer)", "requête 2 (T56)", "DEBOURS (l'application)", "COMPTA "
         "(comptes 601 à 605)", "COMPTA devrait retomber sur le tableau 106 ; DEBOURS : AVS "
         "+1,3 %, PF −3,8 %, AT/MP −28,9 %", "DPC et DFC ; DSI (Q5)"),
        ("cliché (au lancement, hors parametres)", "requêtes 1, 2, 3", "dernier cliché "
         "(092026, copie du 02/02/2026)", "fin mars N+1 (R13)", "Retraités décédés 2024 : 619 "
         "ou environ 444", "DPC (Q2), puis la BI"),
        ("années passées (façon de produire)", "requêtes 1, 2, 3", "recalculées",
         "reprises des résultats gardés (N7)", "IJ de maternité 2020 : 2,19 ou 1,53 Md",
         "DPC (Q6)"),
    ],
    # ------------------------------------------------------------------ 6. questions
    "questions_note": (
        "Une question par point, Q1 à Q5 d'abord. Chacune a une valeur par défaut : la réponse "
        "change un réglage, pas le travail. DCP : Direction centrale des prestations ; DPC : "
        "prospective, fait l'annuaire ; DSI : systèmes d'information ; DFC : financière et "
        "comptable ; DAI : audit interne ; DPASS : action sanitaire et sociale ; DCG : sigle du "
        "recueil, à faire préciser."
    ),
    "questions": [
        ("Q1", "Un paiement est-il rangé dans le mois qu'il paie, le mois où il est saisi "
               "(rappels compris), ou le mois où il est versé ?", "DPC, avec la DCP",
         "le mois payé (MOIS) ; proposition BI : SAISIE",
         "Déjà posée : point du 02/11 ; README R4 ; n° 1 ; fiche Q1 ; = T28-T31 Q24, T32-T41 Q2"),
        ("Q2", "Votre annuaire de l'année N est-il arrêté vers la fin mars N+1 ?", "DPC",
         "le dernier cliché ; lancer N fin mars N+1",
         "Déjà posée : README R13 ; points_a_valider n° 2 ; fiche Q2 ; = T32-T41 Q3"),
        ("Q3", "Le « montant liquidé » de T54 et T55 est-il la somme due pour les prestations "
               "liquidées dans l'année, même payée plus tard ?", "DCP",
         "oui (LIQUIDE), dès que la requête est revue", "Déjà posée : remarque B15 du recueil"),
        ("Q4", "Un assuré mort en activité, dont le décès ouvre une pension ou une allocation de "
               "survivants, compte-t-il dans T57 ?", "DCP, avec la DPC",
         "non (actifs_decedes = NON), comme Kaya 2017",
         "Déjà posée : README (assurés décédés qui ouvrent un droit aux survivants)"),
        ("Q5", "T56 se lit-il dans les paiements de l'application, ou dans les charges "
               "techniques de la comptabilité, comme votre tableau 106 ?", "DPC et DFC ; DSI",
         "les paiements (DEBOURS)", "Nouvelle"),
        ("Q6", "Dans un tableau sur cinq ans, les années passées reprennent-elles les chiffres "
               "déjà publiés ?", "DPC", "recalculées sur le cliché lu", "Nouvelle"),
        ("Q7", "Quel champ de la base garde le montant calculé à la liquidation ?", "DSI",
         "somme des paiements de la prestation, quelle que soit leur date", "Nouvelle"),
        ("Q8", "Pour T51 à T53, compte-t-on aussi les prestations liquidées dans l'année ?",
         "DCP", "oui (payées 2024 : T51 −26 %, T52 −21 %, T53 +6 %)",
         "Déjà posée : README ; points_a_valider n° 49"),
        ("Q9", "Au-delà de combien de mois cotisés a-t-on droit à une pension plutôt qu'à une "
               "allocation de vieillesse ?", "DCP", "1 à 600 mois dans la moyenne",
         "Déjà posée : README (489 mois) ; points_a_valider n° 62 ; fait B16"),
        ("Q10", "Avec le filtre « ayant droit », compte-t-on en PF les conjoints et enfants "
                "d'allocataires décédés ?", "DCP, avec la DPC", "PF = allocataires seulement",
         "Déjà posée : remarque B26 du recueil (n° 54 tranché, voir N2)"),
        ("Q11", "Une personne comptée dans deux branches compte-t-elle deux fois dans le TOTAL "
                "de T57 ?", "DPC", "oui : somme des branches", "Nouvelle"),
        ("Q12", "Un sinistré est-il la seule victime morte sur le coup, ou aussi celle qui meurt "
                "des mois après ?", "DCP (service AT/MP)", "mort dans l'année (366 jours)",
         "Déjà posée : points_a_valider n° 56 ; README"),
        ("Q13", "Où la base garde-t-elle les décès immédiats des accidents d'avant 2025 ?", "DSI",
         "la règle de l'année qui suit l'accident",
         "Déjà posée : points_a_valider n° 66 (en partie) ; README"),
        ("Q14", "Pour vos 24 rentiers décédés de 2024, la rente devait-elle être en cours ?",
         "DPC, avec la DCP", "rente en cours, radiée au plus tôt 31 jours avant", "Nouvelle"),
        ("Q15", "D'où viennent les 89 décès d'ascendants de 2024, pour 59 pensions d'ascendant "
                "en cours ?", "DPC", "sans effet : filtre « assuré »",
         "Déjà posée : README ; points_a_valider n° 57"),
        ("Q16", "Où sont payés les soins AT/MP (139,7 M en 2024) et la part des rentes et IJ "
                "absente de la base ?", "DSI, avec la DFC", "paiements et remboursements de la "
                "base", "Déjà posée : README ; n° 51 ; fiche Q3 ; = T32-T41 Q21, T42-T49 Q5"),
        ("Q17", "Où sont payés les frais médicaux de maternité (25,0 millions en 2024) ?",
         "DSI, avec la DFC et la DPC", "absents de la ligne PF de T56",
         "Déjà posée : T25-T27 Q15 (même réponse)"),
        ("Q18", "Pour la ligne « Autres » de T56 (l'ASS), quel montant prendre ?",
         "DPC, avec la DPASS et la DCG", "ligne vide",
         "Déjà posée : points_a_valider n° 53 (contenu tranché)"),
        ("Q19", "Les rappels de moins-perçus versés par ajustement sont-ils des prestations "
                "servies (T56) ?", "DCP, avec la DPC", "non (sauf les rachats, N4)",
         "Déjà posée : n° 52 ; README ; fiche Q3 (ajustements) ; = T28-T31 Q23"),
        ("Q20", "Le ratio des ressources compte-t-il aussi les soins, ou seulement les "
                "prestations en espèces ?", "DAI, avec la DPC",
         "soins compris ; datées à la notification (l. 1133)", "Nouvelle (remarque B37)"),
        ("Q21", "T56 n'a aucune ligne dans le recueil : reste-t-il à produire ?", "DPC",
         "oui, selon l'Annuaire CIPRES", "Déjà posée : remarque B38 du recueil"),
        ("Q22", "L'épouse non salariée reçoit-elle l'allocation prénatale sous le code FAP "
                "plutôt que MAP ?", "DCP", "T55 compte les deux codes", "Nouvelle"),
        ("Q23", "Peut-on retirer nom, adresse et téléphone de la vue des décédés préparée pour "
                "la BI ?", "DSI", "la BI ne lit pas cette vue",
         "Déjà posée : points_a_valider n° 66 ; README"),
        ("Q24", "Que compte l'indicateur « proportion de nouvelles prestations promues » ?",
         "DCP, avec la DAI et la DPC", "pas calculé",
         "Nouvelle (le recueil la laisse ouverte, l. 1340)"),
        ("Q25", "Le « document de référence des tableaux de la CNSS » (recueil l. 81) existe-t-il, "
                "et pouvez-vous nous le donner ?", "DPC (comité du projet)",
         "recueil, Kaya, Annuaire CIPRES, annuaire CNSS", "Nouvelle ; vaut pour toutes les familles"),
    ],
    # ------------------------------------------------------------------ 7. points d'attention
    "attention_note": "Les 19 points du md, regroupés en 9.",
    "attention": [
        ("Rien de publié pour plusieurs chiffres", "Effectifs de T51, T52, T53, T55, services "
         "passés, sexe, allocataires décédés, décès sur cinq ans : à valider avec la CNSS."),
        ("Deux règles de montant", "T50 à T53 et T56 : le payé ; T54 et T55 : le liquidé. Pas de "
         "montant moyen. date_montant vaut aussi pour T31 à T49 ; avec SAISIE, des années "
         "irrégulières (T55 2021 : 78,2 M contre 33,8 M)."),
        ("Un seul cliché, des années fragiles", "Lancer N fin mars N+1 et garder le résultat "
         "(R13, N7). 2020 publié semble incomplet ; IJ de maternité 2022 à −3,5 % ; arrondis de "
         "1 F."),
        ("T56 et la comptabilité", "La CNSS publie le compte de résultat : pas d'égalité branche "
         "par branche. Total −0,2 % par compensation. Écart AT/MP creusé depuis 2023 (−15,5 % "
         "puis −20,3 %)."),
        ("T57 aujourd'hui", "La requête mélange assurés et ayants droit ; le TOTAL compte des "
         "couples (personne, branche) ; « en cours » ne regarde que la radiation."),
        ("Les maquettes", "T50 : services passés sur la ligne TOTAL ; T51 à T53 sans TOTAL "
         "CIPRES ; T54, T55 : FEMMES. T51 garde les ascendants. Maquette d'origine de T57 : "
         "question T25-T27 Q22."),
        ("Données personnelles", "La vue des décédés de la DSI a nom, adresse, téléphone : ne "
         "pas la lire. Feuille 02 de la grille des ratios non lue (consigne)."),
        ("Les documents existants", "« Six mois exacts » : cinq. « 11 lignes sur 16 » : 11 sur "
         "13. points_a_valider n° 2 : mauvais champ. DOS_DECES_IMMEDIAT : 2 % en 2024. Fiche de "
         "lecture : rachats oubliés."),
        ("Les numéros", "Remarque Bnn du recueil et fait Bnn du schéma des règles sont deux "
         "séries. reference_cipres_T25_T57 a ses propres N1 à N9 : écrire « T50-T57 N3 »."),
    ],
    # ------------------------------------------------------------------ 8. reste et sources
    "reste": [
        ("Exécuter les trois requêtes sur le cliché, puis sur un cliché de fin mars N+1 ; "
         "comparer à l'annuaire", "toi, puis Claude", "à faire"),
        ("P115 : montants 2024 par mois, datés par l'approbation, la saisie, le décaissement "
         "(IJ AT/MP sur cinq ans)", "Claude, puis toi", "à faire"),
        ("P116 : montant liquidé 2020 à 2024 (MIJ, MAP, FAP et les autres codes)",
         "Claude, puis toi", "à faire"),
        ("P117 : T57 par qualité et type ; rentiers sans « en cours » ; assurés morts en "
         "activité", "Claude, puis toi", "à faire"),
        ("P118 : décès immédiats (TSINISTRE ; date du décès = date de l'accident)",
         "Claude, puis toi", "à faire"),
        ("P119 : T56 AT/MP avec les rachats de rente, 2020 à 2024", "Claude, puis toi",
         "à faire"),
        ("Mesurer les allocations de vieillesse aux mois cotisés nombreux (Q9)",
         "Claude, puis toi", "à faire"),
        ("Comparer par mois et par DR : tableaux 24, 39, 29 à 33, 91 à 95",
         "Claude, après exécution", "à faire"),
        ("Vérifier dans l'annuaire 2020 ou 2023 les chiffres de 2020 (N7)", "toi, puis Claude",
         "à faire"),
        ("Accès aux comptes 601 à 605 par branche (V_BI_BALANCE_COMPTABLE) et leur répartition "
         "(N6)", "toi", "à faire"),
        ("Poser Q1 à Q25, en commençant par Q1 à Q5", "CNSS, via toi", "à poser"),
        ("Revoir la requête 3 (T57) : TOTAL, qualité, type, réglages", "Claude",
         "après visa"),
        ("Revoir la requête 2 (T56) et la requête 1 (montant liquidé, TOTAL de T50, détail)",
         "Claude", "après visa"),
        ("Garder les résultats de chaque exercice, sans refonte du modèle (R13, N7)",
         "Claude, puis la DSI", "à faire"),
        ("Lire la feuille 02 de la grille des ratios (N11)", "Claude, sur accord",
         "non commencé"),
        ("Reporter ces règles et corrections dans README, TODO, fiche, fiche de lecture, "
         "regles_prestations, points_a_valider", "Claude, sur demande", "non commencé"),
    ],
    "sources": (
        "Sources : recueil des besoins v7 (besoins_v7.txt et formules du document Word) ; atelier "
        "de Kaya 2017, tableaux 1 et 2 ; Annuaire CIPRES p. 16, 46 à 48, 62 à 66 ; annuaire "
        "statistique de la CNSS 2024 ; classeur « Evaluation des Ratios CIPRES 2025 », feuilles "
        "03 à 06 ; lots 2, 5, 7, 8, 9 et comparaison A02 (copie de la base au 02/02/2026). "
        "Dossier de la famille et sa contre-vérification (style_grh/travail). Détail : "
        "analyses/prestations/regles_t50_t57_evolution.md."
    ),
}


def build():
    d = construire(DATA)
    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
