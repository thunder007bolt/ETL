"""
Schéma « T25 à T27 : les dossiers de prestations, les règles en clair » (état au 08/11/2026).

Famille pilote des prestations « à l'image du GRH » : ce que demande le recueil des besoins v7,
les règles (R1, R2, R13, D1 à D14, A1, H2 repris, N1 à N8 nouvelles), les cas, la comparaison
avec l'annuaire statistique de la CNSS 2024, les réglages de la CTE parametres, les questions
ouvertes (Q1 à Q22), les points d'attention et ce qui reste à faire.

Le détail est dans analyses/prestations/regles_t25_t27_dossiers.md : mêmes identifiants, mêmes
statuts, mêmes verdicts. Le travail existant (README, requêtes t25 à t27, fiches T25 à T27,
regles_prestations, points_a_valider) n'est pas modifié.

Usage :
    python3 excalidraw/sources/prestations/famille_t25_t27_dossiers.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import main  # noqa: E402
from gabarit_famille import construire  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "T25 T27 dossiers" / "regles_t25_t27_dossiers.excalidraw"

RV7 = "Recueil v7"
REU = "Réunion"
KAYA = "Kaya 2017"
CIPRES = "Annuaire CIPRES"
CNSS = "Annuaire CNSS 2024"
MOD = "Modèles CNSS"
DON = "Données"
BI = "Proposition BI"

DATA = {
    "seed": 2527,
    "titre": "T25 à T27 : les dossiers de prestations (déposés, liquidés, en instance)",
    "sous_titre": (
        "Les règles en clair, état au 08/11/2026. T25 pensions, T26 accidents du travail et "
        "maladies professionnelles (AT/MP), T27 prestations familiales et maternité : dossiers "
        "déposés, liquidés par durée, en instance au 31/12, rejetés ; et les indicateurs de délai "
        "et de restes à traiter. Liquidé = droits notifiés ; en instance = ni liquidé ni rejeté. "
        "Tout est proposé, rien n'est validé par la CNSS ; aucune requête n'est exécutée "
        "(chiffres de 2025 vus au 02/02/2026). Le détail est dans "
        "analyses/prestations/regles_t25_t27_dossiers.md."
    ),
    # ------------------------------------------------------------------ en bref
    "en_bref": [
        "Ce qu'on compte : les dossiers de l'année, déposés, liquidés (droits notifiés) en moins "
        "de 15 jours, de 15 à 45 ou plus de 45, en instance au 31/12, rejetés. T25 pensions, T26 "
        "AT/MP, T27 prestations familiales et maternité. Un dossier peut porter plusieurs "
        "demandes (D2).",
        "Le recueil v7 ajoute : la ligne « retraite anticipée » (R2), « Frais médicaux de "
        "maternité » dans T27 (N3), le mois et le trimestre (N1), délais et taux restant à "
        "traiter par DR et agence, avec normes (N2, N4, N5, N7), variation des déclarations "
        "AT/MP (N8). Il diffère sur la fin du délai PF (Q16) et la formule du taux (Q19).",
        "L'annuaire CNSS 2024 confirme : liquidé = droits notifiés (831 allocations de "
        "vieillesse pour 833 publiées, tableau 24) ; maternité dans les PF ; majoration avec la "
        "pension ; formule de N8. Il publie l'anticipée à part, comme le veut le recueil.",
        "Il contredit ou laisse en écart : frais médicaux de maternité, 25,0 millions FCFA payés "
        "en 2024, aucun dans la base ; soins AT/MP, 13,5 % du montant publié ; rentes viagères, "
        "39 pour 44 (−11,4 %). Dépôts, instances, rejets, durées : non publiés.",
        "Décisions attendues (Q1 à Q22, aucune ne bloque) : Q2 limite des demandes sans suite "
        "(pensions : 3 300 à 3 500 en instance, taux d'environ 70 % pour 15 %) ; Q1 dossiers "
        "des années d'avant ou non ; Q19 formule du taux ; Q13 enfant ajouté (8 246 ou environ "
        "16 400) ; Q15 où sont les frais de maternité.",
        "Reste : rien n'est exécuté (chiffres de 2025 vus au 02/02/2026). Exécuter t25 à t27, y "
        "ajouter mois, DR, agence et réglages, comparer 2024 au mois et par DR, poser Q1 à Q22.",
    ],
    # ------------------------------------------------------------------ 0. les tableaux
    "tableaux": [
        ["T25", "Les dossiers de pension (vieillesse, invalidité, décès) de l'exercice : déposés, "
                "liquidés en moins de 15 jours, de 15 à 45, en plus de 45, en instance, rejetés. "
                "Lignes : vieillesse, invalidité, survivant, allocations de vieillesse, allocation "
                "de survivant, autres, Total", "p. 11 et 34",
         "t25_requete_proposee.sql (pas exécutée) ; fiche T25 ; lot 3 B"],
        ["T26", "La même grille pour les AT/MP. Lignes : rentes viagères, rentes de survivants, "
                "indemnités journalières, allocation AT/MP, autres, Total", "p. 12 et 34",
         "t26_requete_proposee.sql (pas exécutée) ; fiche T26 ; lot 4"],
        ["T27", "La même grille pour les prestations familiales et la maternité. Lignes : "
                "allocations familiales, IJ de maternité, allocations prénatales, autres (à "
                "renommer « Frais médicaux de maternité »), Total", "p. 12 et 34",
         "t27_requete_proposee.sql (pas exécutée) ; fiche T27 ; lot 4"],
    ],
    # ------------------------------------------------------------------ 1. recueil v7
    "recueil_note": (
        "Toutes les lignes du recueil v7 qui touchent T25 à T27 (numéros de besoins_v7.txt). "
        "Les formules des indicateurs ont été relues dans le document Word. T26 n'a aucune ligne "
        "dans la liste des tableaux, mais ses durées et son taux viennent des indicateurs AT/MP. "
        "Ne changent rien ici : l. 72 (accès), l. 53 à 60 (fichiers Excel), virement, colonnes "
        "« Priorité » vides ; l. 80 et 81 (document de référence) et satisfaction sont rangées "
        "dans T50-T57."
    ),
    "recueil": [
        ("#1 (T27), l. 125", "« Ce tableau concerne les dossiers de l'exercice (déposés et "
         "liquidés) »", "Les dépôts et les liquidations de l'année. Deux lectures : chaque étape "
         "dans son année, ou les seuls dossiers déposés dans l'année",
         "H2, D3, D4 ; réglage lecture_periode (Q1)", "question"),
        ("#1 (T27), l. 126", "« Dossier en instance : dossier déposé, non liquidé et non rejeté "
         "au cours de la période »", "Stock au 31/12, ou dépôts de l'année encore en attente",
         "D6 ; debut_stock, sans_suite_mois (Q1, Q2)", "question"),
        ("#1 (T27), l. 127", "« Autres prestations de la branche : Renommer par “Frais médicaux "
         "de maternité” »", "La dernière ligne compte les dossiers de maternité avec des frais "
         "pris en charge", "N3 ; aucun paiement dans la base (Q15)", "à intégrer"),
        ("#1 (T27), l. 128", "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
         "T27 au mois, au trimestre, à l'année", "N1 (t27 est annuel)", "à intégrer"),
        ("#18 (T25), l. 248", "« Ajouter une ligne “Pension de retraite anticipée” »",
         "Une ligne à part pour le code RPRA", "R2 ; t25, ligne 2", "pris en compte"),
        ("#18 (T25), l. 249, renvoi à l. 125", "« Idem. Tab 27 » : les dossiers de l'exercice",
         "Même lecture pour les pensions", "H2, D3, D4 (Q1)", "question"),
        ("#18 (T25), l. 249, renvoi à l. 126", "« Idem. Tab 27 » : le dossier en instance",
         "Même stock ; pas de limite de 24 mois pour les pensions", "D6 (Q1, Q2)", "question"),
        ("#18 (T25), l. 249, renvoi à l. 128", "« Idem. Tab 27 » : la périodicité",
         "T25 au mois et au trimestre aussi", "N1", "à intégrer"),
        ("#18 (T25), l. 249, renvoi à l. 127", "« Idem. Tab 27 » : le renommage",
         "Ne peut viser que T27 : la maternité n'est pas une pension",
         "Lecture BI : T25 garde « Autres prestations » (A1) ; à signaler", "pris en compte"),
        ("Liste Prestation, l. 115 à 335", "Aucune ligne pour le tableau 26",
         "Rien sur la maquette de T26 ; ses durées et son taux suivent les indicateurs AT/MP",
         "t26 et fiche T26 suivent l'Annuaire CIPRES (Q12)", "question"),
        ("Indicateurs PF, l. 1093 à 1103", "Délai de liquidation d'un dossier de PF : moyenne ; "
         "trimestriel, annuel ; par DR et agence ; DES, DR", "Le nombre moyen de jours pour "
         "liquider un dossier de PF", "D5, N1, N2, N5", "à intégrer"),
        ("Indicateurs PF, l. 1097", "La durée va du dossier complet reçu au guichet à « la date "
         "de validation »", "Le recueil a changé la phrase de Kaya 2017 (notification)",
         "fin_duree_pf : notification en repli (Q16)", "question"),
        ("Indicateurs PF, l. 1102", "« Par type de prestations (Allocations familiales et "
         "Indemnités Journalière de Maternité) »", "L'allocation prénatale n'est pas dans "
         "l'indicateur de délai", "D14, N5 (Q14)", "question"),
        ("Indicateurs PF, l. 1104 à 1110", "Taux restant à traiter = en instance à la fin ÷ "
         "dossiers complets reçus dans l'exercice × 100 ; trimestriel, annuel ; type, DR, agence",
         "La part des dossiers de PF encore en attente", "N4 (formule_taux = RECUEIL ; Q19), N1, N2",
         "à intégrer"),
        ("Ratio des ressources, l. 1129 à 1141 (note l. 1133), 1210 à 1214, 1287 à 1294",
         "Les prestations ordonnancées sont "
         "« l'ensemble des prestations ayant fait l'objet d'une notification »",
         "Ordonnancé veut dire notifié", "D4 ; le ratio lui-même est hors famille",
         "pris en compte"),
        ("Indicateurs AT/MP, l. 1146 à 1156", "Taux de variation des déclarations = (n − n−1) ÷ "
         "n−1 × 100 ; annuel ; DR, agence ; DP, DR", "La hausse ou la baisse des déclarations "
         "d'une année sur l'autre", "N8", "à intégrer"),
        ("Indicateurs AT/MP, l. 1157 à 1169", "Délai des soins : du certificat du médecin "
         "traitant à la convocation ; moyenne ; trimestriel, annuel", "Deux dates pas encore "
         "trouvées dans la base (P63)", "Q17 ; t26 mesure jusqu'au 1er remboursement (D11)",
         "question"),
        ("Indicateurs AT/MP, l. 1170 à 1179", "Délai des « rentes de survivants » : du certificat "
         "de consolidation à la notification ; annuel", "Le certificat concerne la victime : vise "
         "sans doute la rente viagère", "depart_rente = GUICHET (Q8, Q9)", "question"),
        ("Indicateurs AT/MP, l. 1180 à 1188", "Taux restant à traiter, même formule ; annuel ; "
         "type, DR, agence ; DR, DCP", "La part des dossiers AT/MP en attente au 31/12",
         "N4 (Q19) ; dépend des déclarations sans suite (D12)", "à intégrer"),
        ("Indicateurs pensions, l. 1216 à 1238", "Délai : du dossier complet reçu au guichet à "
         "la notification des droits ; moyenne ; trimestriel, annuel ; DCP, DR", "Le nombre moyen "
         "de jours pour liquider une pension", "D3 à D5 ; N1, N2, N5", "à intégrer"),
        ("Indicateurs pensions, l. 1239 à 1245", "Taux restant à traiter, même formule ; annuel ; "
         "type, DR, agence", "La part des dossiers de pension en attente au 31/12",
         "N4 (Q19) ; stock gonflé (D6, Q2)", "à intégrer"),
        ("Indicateurs pensions, l. 1251", "Les retraités sont les bénéficiaires de pensions "
         "directes : vieillesse, anticipée, invalidité", "L'allocation de vieillesse n'est pas une "
         "pension directe", "Comparaison au tableau 27 de l'annuaire (Q21)", "pris en compte"),
        ("#3 (T31), l. 142 à 144", "PVID branche pension ; AT/MP (RP) ; PF prestation familiale",
         "Les trois branches ; la maternité n'y est pas citée", "R1 (maternité avec les PF)",
         "pris en compte"),
        ("Ratios CIPRES respectés, l. 1978 à 1989", "« Source : DAI. Données non disponibles en "
         "base »", "La part des ratios qui tiennent leur norme", "T25 à T27 lui fournissent deux "
         "ratios (N7)", "hors base"),
        ("Termes de référence, l. 4 à 10", "Cinq directions régionales : Ouagadougou, Bobo, "
         "Ouahigouya, Fada, Dédougou", "« Par DR » = ces cinq directions", "N2", "à intégrer"),
        ("Indicateurs, l. 1065", "« De manière globale, donner la possibilité de désagréger "
         "l'indicateur »", "Chaque indicateur se découpe", "N2", "à intégrer"),
        ("Attentes, l. 29", "Données « traquées jusqu'au niveau de granularité le plus "
         "élémentaires »", "Passage repris d'un cahier bancaire (remarque B41 du recueil)", "N6",
         "à intégrer"),
        ("Caractéristiques, l. 51 et 68", "Analyses sur l'intégralité des données, « y compris au "
         "niveau transactionnel » ; axes non définis d'avance", "Garder une ligne par demande",
         "N6, N2", "à intégrer"),
        ("Attentes, l. 31 et 74", "Présentation multidimensionnelle ; une dimension ajoutée vaut "
         "pour tous les indicateurs", "Les mêmes axes pour T25 à T27", "N2, N6", "à intégrer"),
        ("Attentes, l. 32", "« Emission d'alertes sur les indicateurs de performances »",
         "Prévenir quand un délai ou un taux sort de sa norme", "N7", "à intégrer"),
        ("Attentes, l. 35", "Rapports « à des fréquences adaptées »", "Chaque tableau à sa "
         "fréquence", "N1", "à intégrer"),
        ("Attentes, l. 28, 34, 75", "Peu d'intervention manuelle ; restitutions automatisées",
         "Les chiffres sortent de requêtes", "Requêtes sur la base ; N5, N6", "pris en compte"),
        ("Caractéristiques, l. 58", "Règles de transcodage écrites et rejouables",
         "La correspondance codes et lignes est écrite", "R2", "pris en compte"),
        ("Attentes, l. 24 et 78", "Profilage de la qualité des données", "Mesurer les trous de "
         "la base", "Lots 3 B et 4 ; points d'attention", "pris en compte"),
        ("Général, l. 15 et 92 à 94", "La source : l'application de gestion des prestations",
         "Copie USER_DWH au 02/02/2026", "Toutes les règles", "pris en compte"),
    ],
    # ------------------------------------------------------------------ 2. règles
    "regles": [
        {"id": "R1", "titre": "Les branches",
         "regle": "Pensions (V) dans T25 ; accidents du travail et maladies professionnelles (A) "
                  "dans T26 ; prestations familiales (F) et maternité (M) dans T27.",
         "exemple": "le dossier d'une grossesse (M) compte dans T27.",
         "sources": [(RV7, "l. 142 à 144 (la maternité n'y est pas)"),
                     (CNSS, "tableau 40 : la maternité est dans les PF"), (DON, "P02, P05")],
         "etat": "confirmée par l'annuaire CNSS 2024"},
        {"id": "D1", "titre": "Les dossiers concernés",
         "regle": "Tous les dossiers de la branche. On garde la coordination payée par une autre "
                  "caisse (COOR_AC) ; on retire seulement les dossiers transférés (CNSS_AC).",
         "exemple": "un assuré du Burkina et du Mali dépose à la CNSS, le Mali paie : compté.",
         "sources": [(DON, "P48 : 6 à 8 % de T25 en 2025"),
                     (CNSS, "pensions COOR_AC dans l'effectif (P78)")],
         "etat": "proposée ; Q4, réglage coordination (la dtm_dossier fait l'inverse)"},
        {"id": "D2", "titre": "On compte des demandes",
         "regle": "Une demande = un dossier × une ligne. Un dossier peut porter plusieurs "
                  "demandes : pension puis réversion, IJ puis rente, IJ et allocation prénatale.",
         "exemple": "pension liquidée en 2012, réversion de la veuve déposée et liquidée en 2025.",
         "sources": [(CIPRES, "p. 11 et 12"), (DON, "P51, P54, P59")],
         "etat": "prouvée par les données ; la question de la veuve (fiche T25 Q4) ne change "
                 "plus les chiffres"},
        {"id": "R2", "titre": "Les lignes",
         "regle": "La ligne vient du code de prestation. La retraite anticipée (RPRA) a sa ligne "
                  "dans T25 ; T27 gagne les frais médicaux de maternité (N3).",
         "exemple": "une retraite anticipée (RPRA) va sur sa ligne, pas en vieillesse ; "
                    "3 nouvelles en 2024 (annuaire, tableau 23).",
         "sources": [(RV7, "l. 248"), (KAYA, "tableau 1, l. 271"),
                     (CNSS, "tableaux 18, 22, 23 : anticipée à part")],
         "etat": "ligne anticipée confirmée ; liste des codes à viser (points_a_valider n° 18)"},
        {"id": "A1", "titre": "Les accessoires de pension",
         "regle": "Majoration pour enfant et charges du ménage suivent la pension : pas de "
                  "demande de plus. Un dossier qui n'a qu'un accessoire va en « Autres ».",
         "exemple": "pension de vieillesse + majoration : une seule demande.",
         "sources": [(CNSS, "tableaux 28 et 34"), (DON, "P34, P98")],
         "etat": "confirmée par l'annuaire CNSS 2024"},
        {"id": "H2", "titre": "Chaque étape dans sa période",
         "regle": "Dépôt à la réception, liquidation à la notification, rejet au rejet ; le "
                  "stock se lit à la fin de la période.",
         "exemple": "déposé le 20/11/2024, notifié le 05/02/2025 : en instance au 31/12/2024.",
         "sources": [(CIPRES, "p. 11"), (RV7, "l. 125"), (MOD, "feuille 04")],
         "etat": "proposée, appuyée par la feuille 04 ; lecture cohorte possible (Q1)"},
        {"id": "D3", "titre": "Déposé (remplace R10)",
         "regle": "Reçu au guichet dans la période. « NOUVEAU » ouvre le dossier ; « ANCIEN » "
                  "(dossier existant) dépose la réversion, la rente, l'allocation d'incapacité ou "
                  "la rente de survivant.",
         "exemple": "la veuve dépose le 03/03/2025 sur le dossier du mari : dépôt de mars.",
         "sources": [(CIPRES, "p. 11, 12, 64"), (RV7, "l. 1097 et 1224"),
                     (KAYA, "tableau 1, l. 86"), (DON, "P09, P49, P52")],
         "etat": "proposée ; t25 et t26 ne testent pas le type de réception"},
        {"id": "D4", "titre": "Liquidé (remplace R11)",
         "regle": "À la première notification des droits, au mois de cette notification. Le "
                  "statut du dossier ne sert pas. Exception PF : le recueil dit « validation ».",
         "exemple": "déposée le 20/11/2024, notifiée le 15/01/2025 : liquidée en janvier 2025.",
         "sources": [(CIPRES, "glossaire p. 64"), (RV7, "l. 1133 et 1224"), (MOD, "feuille 03"),
                     (CNSS, "tableau 24 : 831 pour 833")],
         "etat": "confirmée par l'annuaire CNSS 2024 (tableau 24)"},
        {"id": "D5", "titre": "La durée de liquidation",
         "regle": "Jours entre dépôt et première notification. Colonnes 0 à 14, 15 à 45, 46 et "
                  "plus ; part dans la norme = 45 jours au plus ; moyenne dans la norme si moins "
                  "de 45 jours.",
         "exemple": "reçu le 17/11/2025, notifié le 02/12/2025 : 15 jours.",
         "sources": [(CIPRES, "p. 11, 34, 64"), (RV7, "l. 1095, 1221"), (MOD, "feuille 03"),
                     (DON, "P10 : vieillesse 17 / 22 / 61 %")],
         "etat": "proposée ; la moyenne de 82 jours de la fiche T25 est à vérifier"},
        {"id": "D6", "titre": "En instance (remplace R12)",
         "regle": "Déposée, ni liquidée ni rejetée à la fin. On écarte les dossiers fermés jamais "
                  "notifiés, les réceptions d'avant 2016 sans dossier, et en AT/MP et PF ce qui "
                  "reste sans suite 24 mois.",
         "exemple": "reçue le 17/11/2025, notifiée le 22/01/2026 : en instance au 31/12/2025 ; "
                    "reçue en 2012 sans dossier : non.",
         "sources": [(RV7, "l. 126 et 249"), (MOD, "feuille 04"), (DON, "P50, P57, P68")],
         "etat": "exclusions = proposition BI ; pensions sans limite : 3 300 à 3 500 en instance "
                 "fin 2025 (estimation), taux d'environ 70 % (recueil)"},
        {"id": "D7", "titre": "Rejeté",
         "regle": "Un refus du module des rejets, compté l'année du rejet. Ni le statut de la "
                  "réception, ni la fermeture du dossier ne valent refus.",
         "exemple": "reçu le 02/06/2025, rejeté le 30/06/2025 : jamais en instance.",
         "sources": [(CIPRES, "p. 34"), (KAYA, "tableau 1, l. 87"), (DON, "P56, P09 : 14 en 2025")],
         "etat": "prouvée ; saisie sans doute incomplète (Q5)"},
        {"id": "D8", "titre": "Le Total",
         "regle": "Toutes les demandes, même sans ligne ; durée moyenne recalculée sur toutes "
                  "les demandes. N4 propose de remplacer sa formule du taux.",
         "exemple": "une déclaration sans suite compte dans le Total de T26, sur aucune ligne.",
         "sources": [(CIPRES, "p. 11 et 34")],
         "etat": "proposée"},
        {"id": "D9", "titre": "AT/MP : les dépôts",
         "regle": "La déclaration ouvre le dossier et dépose IJ et soins. Une demande plus tard "
                  "dépose la rente ou l'allocation (consolidation), ou les survivants (décès).",
         "exemple": "accident déclaré le 06/01/2024 : dépôt de l'IJ ; demande de rente le "
                    "10/04/2025 : dépôt de la rente.",
         "sources": [(CNSS, "tableaux 47 et 48"), (DON, "P59, P60, P71")],
         "etat": "proposée ; l'annuaire précise sans confirmer (réceptions +6 % sur les dossiers)"},
        {"id": "D10", "titre": "AT/MP : IJ et allocation d'incapacité",
         "regle": "Une IJ est déposée avec la déclaration, liquidée à sa première notification : "
                  "un arrêt prolongé ne fait qu'une demande. Durée depuis la déclaration.",
         "exemple": "déclaration le 06/01/2025, IJ notifiée le 15/07/2025 : 190 jours.",
         "sources": [(CIPRES, "glossaire p. 64"), (DON, "P61, P62 : 251 jours, P69")],
         "etat": "proposée ; Q10"},
        {"id": "D11", "titre": "AT/MP : les soins",
         "regle": "Ligne « Autres prestations » de T26 : une demande par dossier, liquidée à la "
                  "saisie du premier remboursement.",
         "exemple": "déclaration le 03/02/2025, 1er remboursement le 12/05/2025 : 98 jours.",
         "sources": [(RV7, "l. 1157 à 1169"), (CNSS, "tableaux 82 et 89"),
                     (DON, "P63 : 650 à 960 par an")],
         "etat": "proposée ; Q7, réglage soins_t26"},
        {"id": "D12", "titre": "AT/MP : déclarations sans suite",
         "regle": "Comptée dans les déposés (Total, sans ligne) ; en instance 24 mois au plus, "
                  "puis hors du stock.",
         "exemple": "déclaration du 15/09/2025 : hors du stock en septembre 2027.",
         "sources": [(CNSS, "tableau 47 : toutes les déclarations"), (DON, "P59, P68 : 52,9 %")],
         "etat": "question Q3 ; réglage sans_suite_mois"},
        {"id": "D13", "titre": "PF : l'enfant ajouté",
         "regle": "Réglage enfant_ajoute : à 1, chaque enfant ajouté est une demande de plus ; "
                  "à 0, seuls les nouveaux allocataires.",
         "exemple": "nouveau-né demandé le 03/03/2025, notifié le 14/03/2025 : 11 jours.",
         "sources": [(MOD, "feuille 03 : « 1ère demande »"), (DON, "P64, P65")],
         "etat": "question Q13"},
        {"id": "D14", "titre": "Maternité : deux lignes",
         "regle": "Une grossesse = l'IJ et l'allocation prénatale, déposées et liquidées "
                  "ensemble. Dans l'indicateur de délai des PF, une seule fois (l'IJ).",
         "exemple": "reçu le 05/05/2025, notifié le 21/05/2025 : 16 jours sur chaque ligne.",
         "sources": [(RV7, "l. 1102"), (KAYA, "tableau 1, l. 80 à 82"),
                     (DON, "P66 : prénatale notifiée ; P67")],
         "etat": "proposée ; Q14, réglage grossesse_deux_lignes"},
        {"id": "R13", "titre": "Le cliché du calcul",
         "regle": "L'exercice N se calcule sur la copie de fin mars N+1, quand la CNSS arrête "
                  "son annuaire.",
         "exemple": "2025 sur le cliché de fin mars 2026.",
         "sources": [(DON, "P99 : 444 décès pour 447 publiés")],
         "etat": "proposée ; le cliché 092026 porte la copie du 02/02/2026"},
        {"id": "N1", "titre": "Mois, trimestre, année",
         "regle": "Chaque événement compte à son mois ; trimestre et année additionnent. Le stock "
                  "se lit à la fin du dernier mois.",
         "exemple": "2e trimestre 2025 : avril à juin ; en instance au 30/06/2025.",
         "sources": [(RV7, "l. 128, 249, 1100, 1228"), (CNSS, "tableau 24 (au mois)")],
         "etat": "nouvelle, à intégrer"},
        {"id": "N2", "titre": "Par type, DR et agence",
         "regle": "DR du dossier, sinon de la réception ; agence = service du dossier (SP_NO), "
                  "sinon de la réception.",
         "exemple": "dépôt à l'agence de Koudougou (service 3310) : DR de Ouagadougou (DRO), "
                    "agence de Koudougou.",
         "sources": [(RV7, "l. 4 à 10, 1065, 1102"), (CNSS, "p. xvii à xix ; tableaux 73, 74"),
                     (DON, "P46 : 15 agences, 5 sièges")],
         "etat": "nouvelle ; la DSI confirme (Q20)"},
        {"id": "N3", "titre": "T27 : frais médicaux de maternité",
         "regle": "La dernière ligne de T27 compte les dossiers de maternité avec des frais pris "
                  "en charge : accouchement, examens, hospitalisation, pharmacie…",
         "exemple": "une césarienne prise en charge : ligne « Frais médicaux de maternité », "
                    "vide aujourd'hui.",
         "sources": [(RV7, "l. 127"), (CNSS, "tableaux 40, 41 : 25,0 millions"),
                     (DON, "P35 ; P95 : aucun paiement")],
         "etat": "nouvelle ; ligne vide tant que la source n'est pas trouvée (Q15)"},
        {"id": "N4", "titre": "Taux restant à traiter",
         "regle": "Par défaut, la formule du recueil : en instance à la fin ÷ reçus dans la "
                  "période. La feuille 04 en réglage : ÷ (en instance au début + reçus − "
                  "rejetés). Norme 15 %.",
         "exemple": "150 ÷ 400 = 37,5 % (recueil) ; 150 ÷ 490 = 30,6 % (feuille 04).",
         "sources": [(RV7, "l. 1104, 1180, 1239"), (KAYA, "tableau 2 : même formule"),
                     (MOD, "feuille 04")],
         "etat": "nouvelle ; réglage formule_taux, Q19"},
        {"id": "N5", "titre": "Les indicateurs de délai",
         "regle": "Moyenne, écart type, part en 45 jours au plus ; toutes les demandes par "
                  "défaut, la première demande en réglage. PF : allocations familiales et IJ.",
         "exemple": "11, 16 et 40 jours : moyenne 22,3 ; écart type 15,5.",
         "sources": [(RV7, "l. 1094 à 1103, 1217 à 1238"), (MOD, "feuille 03")],
         "etat": "nouvelle ; réglage premiere_demande, Q18"},
        {"id": "N6", "titre": "Retrouver chaque chiffre",
         "regle": "Une ligne par demande : branche, ligne, dates, durée, DR, agence. Chaque case "
                  "se retrouve dans cette liste.",
         "exemple": "les 833 allocations de vieillesse de 2024, une à une.",
         "sources": [(RV7, "l. 51, 68 (et l. 29)"), (MOD, "feuille 03 : liste des dossiers")],
         "etat": "nouvelle ; aucune requête ne la produit"},
        {"id": "N7", "titre": "Normes et alertes",
         "regle": "« Dans les normes » ou « hors normes » : délai moyen de moins de 45 jours ; "
                  "taux de 15 % au plus. Normes dans la CTE parametres.",
         "exemple": "vieillesse 2025 : 39 % en 45 jours au plus, médiane 62 jours.",
         "sources": [(RV7, "l. 32"), (CIPRES, "p. 11"), (MOD, "feuilles 03 et 04")],
         "etat": "nouvelle, à intégrer"},
        {"id": "N8", "titre": "Variation des déclarations AT/MP",
         "regle": "(n − n−1) ÷ n−1 × 100, deux décimales, par DR et agence. En attendant : "
                  "dossiers AT/MP ouverts.",
         "exemple": "2024 : −3,02 % dans la base, −2,58 % publié.",
         "sources": [(RV7, "l. 1146 à 1156"), (CNSS, "tableaux 38 et 47"), (DON, "P68")],
         "etat": "nouvelle, à intégrer"},
    ],
    # ------------------------------------------------------------------ 3. cas
    "cas_note": (
        "Les 12 cas les plus parlants (18 dans le md). Chaque carte dit ce que devient le dossier, "
        "et selon quelle règle."
    ),
    "cas": [
        ("Cas 1. La veuve d'un retraité", "Pension du mari depuis 2012 ; la veuve dépose le "
         "10/03/2025 sur son dossier ; droits notifiés le 09/05/2025.",
         "COMPTÉ : T25 2025, survivant, 60 jours (D2, D3, D4)", "compte"),
        ("Cas 2. Une veuve et trois orphelins", "Quatre pensions notifiées le même jour, dans le "
         "dossier du défunt. L'annuaire compte quatre personnes.",
         "COMPTÉ UNE FOIS : une demande (D2)", "compte"),
        ("Cas 3. Coordination avec le Mali", "Reçu et étudié par la CNSS ; la caisse du Mali paie "
         "(COOR_AC). La dtm_dossier l'écarte.", "RÉGLAGE coordination = GARDER (D1, Q4)",
         "reglage"),
        ("Cas 4. Une réception de 2012 sans dossier", "Jamais reliée à un dossier : le lien "
         "n'était pas saisi.", "DÉPOSÉ EN 2012, PAS EN INSTANCE : debut_stock (D6)", "reglage"),
        ("Cas 5. Une demande de pension de 2019 sans suite", "Dossier ouvert, ni notifié ni "
         "rejeté. Les pensions n'ont pas de limite de 24 mois.",
         "EN INSTANCE FIN 2025 : à surveiller (D6, Q2)", "attention"),
        ("Cas 7. À cheval sur deux années", "Reçu le 17/11/2025, notifié le 22/01/2026.",
         "COMPTÉ : déposé 2025, en instance au 31/12, liquidé 2026 (H2, Q1)", "compte"),
        ("Cas 11. Une rente trois ans après l'accident", "IJ en 2022 ; demande de rente le "
         "10/04/2025 avec la consolidation ; notifiée le 24/04/2025.",
         "RÉGLAGE depart_rente : 14 jours (guichet), 64 en médiane (certificat)", "reglage"),
        ("Cas 12. Une déclaration qui ne donne rien", "Accident du 15/09/2025 ; ni arrêt, ni "
         "soins, ni rente. 53 % des déclarations de 2024.",
         "RÉGLAGE sans_suite_mois : en instance 24 mois (D12, Q3)", "reglage"),
        ("Cas 14. Un dossier avec seulement des soins", "Premier remboursement saisi 98 jours "
         "après la déclaration. La base n'a que 13,5 % du montant publié.",
         "QUESTION Q7 : « Autres prestations » de T26 (D11)", "question"),
        ("Cas 16. Un enfant ajouté", "Allocataire depuis 2019 ; nouveau-né demandé le 03/03/2025, "
         "notifié le 14/03/2025, sans guichet.", "RÉGLAGE enfant_ajoute = 1 : 11 jours (D13, Q13)",
         "reglage"),
        ("Cas 17. Une grossesse", "Reçu le 05/05/2025 ; IJ et allocation prénatale notifiées le "
         "21/05/2025.", "QUESTION Q14 : deux lignes dans T27, une dans l'indicateur (D14)",
         "question"),
        ("Cas 18. Les frais d'une césarienne", "Le recueil veut la ligne. La base n'a aucun "
         "paiement de 2020 à 2025 ; l'annuaire publie 25,0 millions en 2024.",
         "NULLE PART AUJOURD'HUI : à surveiller (N3, Q15)", "attention"),
    ],
    # ------------------------------------------------------------------ 4. annuaire
    "annuaire_note": (
        "L'annuaire ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées. Il sert à "
        "trois contrôles directs (tableaux 24 ; 35 et 39 ; 47, 48, 72), à des contrôles indirects "
        "en personnes (23, 75, 77) et comme modèle de découpage par agence (73, 74)."
    ),
    "annuaire": [
        ("Tableaux 18 à 113", "Dépôts, dossiers en instance, rejets, durées", "—",
         "rien", "—", "non publie", "Ces colonnes ne se valident qu'avec la CNSS"),
        ("Tableau 24 (p. 19)", "Allocations de vieillesse liquidées en 2024", "831 (832 "
         "allocations)", "833", "−0,24 %", "confirme", "D4 : à la notification (la saisie "
         "donnerait 873) ; modèle de N1, N2"),
        ("Tableau 35 (p. 30)", "IJ de maternité 2020 à 2024", "3 438 … 3 890", "3 428 … 3 893",
         "+0,29 % à −3,50 %", "precise", "Une grossesse = un bénéficiaire ; la date ne se "
         "départage pas (2022, 2023)"),
        ("Tableau 39 (p. 32)", "IJ de maternité 2024, au mois et par DR", "3 890 sur l'année",
         "3 893", "−0,08 %", "precise", "Comparer mois par mois : le mois publié est-il celui de "
         "la notification ?"),
        ("Tableau 47 (p. 38)", "Accidents déclarés 2019 à 2024", "1 189 en 2024 (dossiers "
         "ouverts)", "1 171", "+1,4 à +3,0 %", "precise", "t26 comptera les réceptions, +6 % : "
         "écart à recalculer"),
        ("Tableau 47 (p. 38)", "Variation des déclarations 2024 (N8)", "−3,02 %", "−2,58 %",
         "0,44 point", "precise", "Même sens, même ordre de grandeur"),
        ("Tableau 38 (p. 31)", "Calcul d'un taux de variation", "(n − n−1) ÷ n−1",
         "« glissement », 2 décimales", "aucun", "confirme", "N8 suit la pratique de la CNSS"),
        ("Tableaux 48 et 72", "Déclarations 2024 par DR et par type", "pas encore par DR",
         "DRO 736 … DRD 31", "—", "precise", "Modèle du découpage par DR (N2, N8)"),
        ("Tableaux 73 et 74", "Déclarations 2024 par « lieu CNSS »", "pas calculé",
         "22 lieux, 1 171", "—", "precise", "Modèle publié du découpage par agence (N2)"),
        ("Pages xv à xix", "Directions et agences", "15 agences, 5 sièges (P46)",
         "16 agences", "à rapprocher", "precise", "Appuie agence = service du dossier (N2, Q20)"),
        ("Tableau 22 (p. 18)", "Pensionnés par DR (DR du dossier)", "A02", "62 231",
         "+0,1 à +4,0 % ; veuves −8 à −12 %", "precise", "La DR du dossier est plausible, rien "
         "de plus (orphelins hors comparaison)"),
        ("Tableau 23 (p. 19)", "Nouveaux retraités 2024 (vieillesse et anticipée)",
         "2 397 notifiés ; 2 463 saisis", "2 489", "−3,7 % ; −1,0 %", "precise",
         "Anticipée publiée à part (R2) ; des personnes, pas des demandes"),
        ("Tableau 27 (p. 21)", "« Départs à la retraite » 2024", "pas calculé", "3 729", "—",
         "non comparable", "Peut-être les dépôts de pension (Q21) ; 12,3 % au-dessus des "
         "entrées publiées (2 489 + 833 = 3 322)"),
        ("Tableau 75 (p. 60)", "Nouvelles rentes viagères 2024", "39", "44", "−11,4 %",
         "ecart", "Quelques révisions comptées comme nouvelles ?"),
        ("Tableau 77 (p. 61)", "Nouveaux ayants droit de rentes 2024", "63 personnes", "61",
         "+3,3 %", "precise", "Des personnes : la ligne de T26 sera bien plus basse (D2)"),
        ("Tableaux 28 et 34", "Majoration pour enfant 2024", "539 376 000", "537 343 000",
         "+0,4 %", "confirme", "A1 : l'accessoire suit la pension"),
        ("Tableaux 40 et 41", "Frais médicaux de maternité 2024", "0", "25 030 869 FCFA",
         "tout", "infirme", "Contredit la fiche T27 : payés hors de notre base (N3, Q15)"),
        ("Tableaux 82, 89, 98, 99", "Soins AT/MP 2024", "18 787 945", "139 668 695",
         "−86,5 %", "ecart", "Des dossiers de soins peuvent manquer (D11)"),
        ("Tableaux 90, 96, 97", "Rachats de rente 2024", "21 519 361 (11 rachats)",
         "25 366 909", "−15,2 %", "precise", "Prestation à part pour la CNSS (Q11)"),
        ("Tableau 118 (p. 88)", "Dossiers médicaux des centres de santé maternelle et infantile", "—", "4 555", "—",
         "non comparable", "Pas des dossiers de prestations"),
    ],
    "annuaire_apres": (
        "Confirme : la liquidation à la notification (D4), l'accessoire avec la pension (A1), la "
        "maternité dans les PF (R1), l'anticipée à part (R2), le calcul du taux de variation (N8). "
        "Contredit : la fiche T27 (« frais de maternité jamais payés »). À expliquer : les soins "
        "AT/MP et les rentes viagères. Pas publié : dépôts, instances, rejets, durées."
    ),
    # ------------------------------------------------------------------ 5. réglages
    "reglages_note": (
        "Dans la CTE parametres en tête de chaque requête. « À créer » : le choix est écrit en dur, "
        "ou n'existe pas encore. Une décision attendue de la CNSS ne bloque rien : la valeur "
        "retenue s'applique en attendant."
    ),
    "reglages": [
        ("debut_stock", "t25, t26, t27", "DATE '2016-01-01'", "une autre date ; 1900 pour tout "
         "garder", "Une réception d'avant, sans dossier, sort du stock (13 149 en pensions)",
         "DCP ; DP et DR ; DES et DR (Q2)"),
        ("sans_suite_mois", "t26, t27", "24", "12, 36 ; 999", "Une demande sans suite sort du "
         "stock après 24 mois", "DP et DR ; DES et DR (Q2, Q3)"),
        ("sans_suite_mois (à créer)", "t25", "aucune limite", "24", "Sans limite : 3 300 à 3 500 "
         "en instance fin 2025 (estimation)", "DCP (Q2)"),
        ("lecture_periode (à créer)", "t25, t26, t27", "STOCK", "COHORTE", "Cohorte : seuls les "
         "dépôts de l'année ; tableau additif", "DCP (Q1)"),
        ("coordination (à créer)", "t25 ; dtm_dossier", "GARDER", "EXCLURE", "238 dépôts et 292 "
         "liquidations de pension en 2025", "DCP (Q4)"),
        ("reception_statut_a (à créer)", "t25", "EN_INSTANCE", "ANNULEE", "111 réceptions sans "
         "dossier depuis mai 2025", "DSI (Q6)"),
        ("soins_t26 (à créer)", "t26", "OUI", "NON", "Environ 800 dossiers de soins par an en "
         "« Autres »", "DP et DR (Q7)"),
        ("depart_rente (à créer)", "t26", "GUICHET (exception provisoire)",
         "CERTIFICAT_SINON_GUICHET", "Médiane 14 jours ou 64 ; certificat absent pour 15 rentes "
         "sur 43", "DP et DR (Q8, Q9)"),
        ("rachat_rente_t26 (à créer)", "t26", "NON", "OUI", "11 rachats en 2024 dans la base",
         "DP et DR (Q11)"),
        ("enfant_ajoute", "t27", "1", "0", "Environ 16 400 liquidations d'allocations familiales "
         "en 2025, ou 8 246", "DES et DR (Q13)"),
        ("grossesse_deux_lignes (à créer)", "t27", "1", "0", "Le Total de T27 baisse d'environ "
         "4 200 avec 0", "DES et DR (Q14)"),
        ("fin_duree_pf (à créer)", "t27", "NOTIFICATION (repli)", "VALIDATION (le recueil)",
         "La fin de la durée des PF", "DES ; DSI (Q16)"),
        ("formule_taux (à créer)", "t25, t26, t27", "RECUEIL", "GRILLE ; ANCIENNE (D8)",
         "37,5 % ou 30,6 % dans l'exemple de N4", "auteur de la grille ; DCP (Q19)"),
        ("premiere_demande (à créer)", "indicateurs (N5)", "TOUTES", "PREMIERE", "Sans enfants "
         "ajoutés en PF ; rente seule en AT/MP", "DCP, DES, DP (Q18)"),
        ("norme_delai_jours, norme_taux (à créer)", "t25, t26, t27", "45 jours ; 15 %",
         "d'autres normes", "« Dans les normes » ou « hors normes » (N7)", "DCP, avec la DAI"),
        ("periode (à créer)", "t25, t26, t27", "MOIS", "ANNEE", "T25 et T27 au mois, trimestre, "
         "année (N1)", "tranché par le recueil (l. 128)"),
        ("agence (à créer)", "t25, t26, t27", "SERVICE", "LIEU_PAIEMENT", "Le découpage par "
         "agence (N2)", "DSI (Q20)"),
        ("cliché (au lancement, hors parametres)", "t25, t26, t27, dtm_dossier",
         "092026 (copie du 02/02/2026)", "fin mars N+1 (R13)", "Ce que la base sait des saisies "
         "tardives", "BI, avec la DSI (points_a_valider n° 2, 65)"),
    ],
    # ------------------------------------------------------------------ 6. questions
    "questions_note": (
        "Une question par point. Chacune a une valeur par défaut : la réponse change un réglage, "
        "pas le travail. DCP (Direction centrale des prestations) pour les pensions ; DP et "
        "directions régionales (DR) pour les AT/MP (DP : sigle à faire préciser) ; DES et DR "
        "pour les PF. DSI : systèmes d'information ; DPC : prospective, fait l'annuaire."
    ),
    "questions": [
        ("Q1", "Le tableau de 2025 compte-t-il aussi les dossiers déposés avant 2025, ou "
               "seulement ceux déposés en 2025 ?", "DCP, pour les trois tableaux",
         "chaque étape dans son année ; stock de tout ce qui attend (STOCK)",
         "Déjà posée : README ; points_a_valider n° 14"),
        ("Q2", "Au bout de combien de mois une demande sans suite sort-elle des dossiers en "
               "instance (ou jamais) ?", "DCP ; DP et DR ; DES et DR",
         "2016 partout ; 24 mois en AT/MP et PF ; pas de limite en pensions",
         "Déjà posée : fiches T25 Q3, T26 Q1, T27 Q3 ; points_a_valider n° 21 et 64"),
        ("Q3", "Une déclaration d'accident sans arrêt, ni soins, ni rente compte-t-elle comme "
               "un dossier déposé ?", "DP et DR", "dans les déposés ; en instance 24 mois au plus",
         "Déjà posée : fiche T26 Q1 ; points_a_valider n° 23"),
        ("Q4", "Un dossier de coordination payé par une autre caisse compte-t-il dans T25 à "
               "T27 ?", "DCP", "compté (GARDER)",
         "Déjà posée : fiche T25 Q1 ; points_a_valider n° 19"),
        ("Q5", "Tous les refus sont-ils saisis dans le module des rejets ?", "DCP, avec la DSI",
         "seuls les refus du module comptent", "Déjà posée : fiches T25 Q5, T27 Q4 ; "
         "points_a_valider n° 22"),
        ("Q6", "Une réception de pension au statut « A » (sens non écrit), sans dossier, est-elle "
               "en attente ou annulée ?", "DSI", "en instance", "Déjà posée : README ; points_a_valider n° 58"),
        ("Q7", "Les remboursements de soins des accidents entrent-ils dans T26 (« Autres "
               "prestations ») ?", "DP et DR", "oui, liquidés au premier remboursement",
         "Déjà posée : fiche T26 Q2 ; points_a_valider n° 24"),
        ("Q8", "La durée d'une rente viagère part-elle du certificat de consolidation ou de la "
               "demande au guichet ?", "DP et DR", "la demande au guichet",
         "Déjà posée : fiche T26 Q3 ; points_a_valider n° 25 ; remarque B16 du recueil"),
        ("Q9", "Le délai des « rentes de survivants » du recueil part du certificat de la "
               "victime : vise-t-il la rente viagère ?", "DP et DR", "survivant : depuis sa demande au guichet",
         "Déjà posée : remarque B16 du recueil"),
        ("Q10", "Quelle pièce ou étape attend une IJ d'accident entre la fin de l'arrêt et sa "
                "notification (211 jours) ?",
         "DP et DR", "durée depuis la déclaration, publiée telle quelle",
         "Déjà posée : fiche T26 Q4 ; points_a_valider n° 26"),
        ("Q11", "Un rachat de rente d'accident est-il un dossier liquidé de T26 ?", "DP et DR",
         "non compté", "Nouvelle (voisine de points_a_valider n° 52)"),
        ("Q12", "T26 n'a aucune ligne dans le recueil : le produit-on comme l'Annuaire CIPRES, "
                "avec les règles de T25 et T27 ?", "DP et DR, avec la DCP", "oui",
         "Déjà posée : remarque B38 du recueil"),
        ("Q13", "Un enfant déclaré par un allocataire est-il un dossier déposé et liquidé de "
                "plus dans T27 ?", "DES et DR", "oui (enfant_ajoute = 1)",
         "Déjà posée : fiche T27 Q1 ; points_a_valider n° 27"),
        ("Q14", "Une grossesse compte-t-elle sur les deux lignes de T27, ou une seule fois ?",
         "DES et DR", "deux lignes ; une seule (l'IJ) dans l'indicateur de délai",
         "Déjà posée : fiche T27 Q2 ; points_a_valider n° 28"),
        ("Q15", "Où sont enregistrés les frais médicaux de maternité payés (25,0 millions en "
                "2024) ?", "DSI, avec la DPC", "ligne vide",
         "Nouvelle (remarque B27 du recueil) ; reprise pour T56 : T50-T57 Q17"),
        ("Q16", "Quelle date de l'application est la « date de validation » d'un dossier de "
                "PF ?", "DSI, avec la DES", "la notification des droits",
         "Déjà posée : remarque B18 du recueil"),
        ("Q17", "Où sont la date du certificat du médecin traitant et la date de convocation "
                "(délai des soins) ?", "DSI, avec la DP",
         "pas d'indicateur ; soins de la déclaration au 1er remboursement",
         "Déjà posée : remarque B17 du recueil"),
        ("Q18", "Les indicateurs de délai portent-ils sur la seule première demande, comme la "
                "feuille 03 ?", "DCP, DES, DP", "toutes les demandes liquidées", "Nouvelle"),
        ("Q19", "Taux restant à traiter : formule du recueil (÷ reçus) ou de la feuille 04 (÷ en "
                "instance au début + reçus − rejetés) ?", "auteur de la grille, à identifier ; "
                "en attendant la DCP", "la formule du recueil, norme 15 %",
         "Déjà posée : fiche T25 règle 10 ; points_a_valider n° 15"),
        ("Q20", "L'agence d'un dossier est-elle bien son service dans l'application (SP_NO) ?",
         "DSI", "service du dossier, sinon de la réception",
         "Déjà posée : remarque B34 du recueil"),
        ("Q21", "Le tableau 27 « départs à la retraite » (3 729) compte-t-il les demandes de "
                "pension reçues ?", "DPC (Service études et statistique)",
         "pas de comparaison ; la BI compare quand même", "Nouvelle"),
        ("Q22", "Faut-il aussi garder la maquette CIPRES d'origine de T25 et T27 ?",
         "DCP, avec la DPC", "la version du recueil ; l'origine se refait en additionnant",
         "Nouvelle"),
    ],
    # ------------------------------------------------------------------ 7. points d'attention
    "attention_note": "Les 17 points du md, regroupés en 9.",
    "attention": [
        ("Rien de publié sur les dépôts", "Ni dépôts, ni instances, ni rejets, ni durées dans "
         "l'annuaire : ces colonnes ne se valident qu'avec la CNSS."),
        ("Personnes ou demandes", "L'annuaire compte des personnes, T25 à T27 des demandes : les "
         "lignes de survivants seront bien plus basses que 1 542 et 61. Ce n'est pas une erreur."),
        ("La dtm_dossier est en retard", "Anciennes règles : dossiers et non demandes, "
         "coordination écartée, soins à l'approbation, pas de limite de stock. Fusionner, puis "
         "accord de Jules (outil de restitution OBIEE)."),
        ("Rien n'est exécuté", "Chiffres 2025 vus au 02/02/2026 : il manque les saisies de "
         "février et mars 2026 (R13)."),
        ("Le stock des pensions", "3 300 à 3 500 demandes en instance fin 2025 avec les réglages "
         "actuels (estimation) : taux d'environ 70 % avec la formule du recueil, de l'ordre de "
         "40 % avec la feuille 04, pour une norme de 15 %."),
        ("T26 et les délais du recueil", "T26 n'a pas de ligne dans la liste, mais des "
         "indicateurs. Trois délais du recueil ne s'appliquent pas à la lettre : survivants, "
         "soins, validation PF."),
        ("La couverture de la base", "Soins AT/MP : 13,5 % du montant publié ; frais médicaux de "
         "maternité absents. Lignes « Autres » de T26 et T27 incomplètes."),
        ("Les modèles de la CNSS", "Feuille 04 : la ligne AT/MP oublie le stock du début (à "
         "signaler). Feuille 03 : modèle vide, 100 dossiers au plus par branche."),
        ("Les documents existants", "Fiche T25 : l'anticipée ne cite que Kaya. Fiche T27 : "
         "« jamais payés ». README : 183 jours depuis la dernière réception (251 depuis la "
         "déclaration)."),
    ],
    # ------------------------------------------------------------------ 8. reste et sources
    "reste": [
        ("Exécuter t25, t26, t27 sur 092026, puis sur un cliché de fin mars N+1", "toi, puis "
         "Claude", "à faire"),
        ("Mois, DR et agence dans t25 à t27, ou dtm_dossier remise aux règles du 30/10 (N1, N2)",
         "Claude, puis Jules", "à faire"),
        ("Ajouter les réglages « à créer » dans les CTE parametres", "Claude", "après visa"),
        ("Écart type, part en 45 jours, formule_taux, normes (N4, N5, N7)", "Claude", "à faire"),
        ("Renommer la ligne 4 de T27 et la nourrir (N3)", "Claude, après la DSI", "à faire"),
        ("Liste des demandes (N6)", "Claude", "à faire"),
        ("Comparer 2024 au mois et par DR : tableaux 24, 39, 47, 48, 72 à 74", "Claude, après "
         "exécution", "à faire"),
        ("Contrôle des IJ de maternité 2022", "Claude, puis toi", "à faire"),
        ("Réceptions de pension 2024 face au tableau 27, sans attendre la DPC", "Claude, puis toi",
         "à faire"),
        ("Taux de variation des déclarations par DR et agence (N8)", "Claude", "à faire"),
        ("Mesurer le stock de T25, avec et sans sans_suite_mois = 24", "toi, puis Claude",
         "à faire"),
        ("Compter les réceptions prises pour « ANCIEN » sans l'être (D3)", "Claude, puis toi",
         "à faire"),
        ("Poser Q1 à Q22 ; signaler l'oubli de la feuille 04", "toi, avec la CNSS", "à poser"),
        ("Reporter ces règles dans README, fiches, fiche de lecture, regles_prestations, "
         "points_a_valider", "Claude, sur demande", "non commencé"),
    ],
    "sources": (
        "Sources : recueil des besoins v7 (besoins_v7.txt et formules du document Word) ; atelier "
        "de Kaya 2017, tableaux 1 et 2 ; Annuaire CIPRES p. 11, 12, 34, 62, 64, 65 ; annuaire "
        "statistique de la CNSS 2024 ; classeur « Evaluation des Ratios CIPRES 2025 », feuilles "
        "03 et 04 ; lots 3 B, 4, 7, 8, 9 et comparaisons A01, A02 (copie de la base au "
        "02/02/2026). Dossier de la famille et sa contre-vérification (style_grh/travail). "
        "Détail : analyses/prestations/regles_t25_t27_dossiers.md."
    ),
}


def build():
    d = construire(DATA)
    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
