"""
Schéma « T28 à T31 : les contrôles a posteriori et les prestations indues, les règles en clair »
(état au 08/11/2026).

Famille F2 des prestations « à l'image du GRH » : ce que demande le recueil des besoins v7, les
règles (R1, R3, R4, R13, I1, règles 5 à 8 de la fiche T31, C1, A1, H11, H12 reprises ; N1 à N14
nouvelles), les cas, la comparaison avec l'annuaire statistique de la CNSS 2024, les réglages de
la CTE parametres, les questions ouvertes (Q1 à Q30), les points d'attention et ce qui reste à
faire.

Le détail est dans analyses/prestations/regles_t28_t31_controles_indus.md : mêmes identifiants,
mêmes statuts, mêmes verdicts. Le travail existant (README, requête t31, fiche T31, fiche de
lecture, regles_prestations, points_a_valider, vues cnss) n'est pas modifié.

Usage :
    python3 excalidraw/sources/prestations/famille_t28_t31_controles_indus.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import main  # noqa: E402
from gabarit_famille import construire  # noqa: E402

OUTPUT = (ROOT / "schemas" / "prestations" / "T28 T31 controles indus"
          / "regles_t28_t31_controles_indus.excalidraw")

RV7 = "Recueil v7"
REU = "Réunion"
KAYA = "Kaya 2017"
CIPRES = "Annuaire CIPRES"
CNSS = "Annuaire CNSS 2024"
MOD = "Modèles CNSS"
DON = "Données"
BI = "Proposition BI"

DATA = {
    "seed": 2831,
    "titre": "T28 à T31 : les contrôles a posteriori et les prestations indues",
    "sous_titre": (
        "Les règles en clair, état au 08/11/2026. T28 pensions, T29 accidents du travail et "
        "maladies professionnelles (AT/MP), T30 prestations familiales (PF) : dossiers payés et "
        "dossiers contrôlés après paiement, par trimestre (5 % au moins). T31 : payées, "
        "trop-perçus et moins-perçus par mois et par branche (2 % d'indus au plus). Tout est "
        "proposé, rien n'est validé par la CNSS ; T28 à T30 n'ont pas de requête (les contrôles "
        "manquent) ; t31 n'est pas exécutée (chiffres de 2024 vus au 02/02/2026). Le détail est "
        "dans analyses/prestations/regles_t28_t31_controles_indus.md."
    ),
    "en_bref": [
        "Ce que comptent ces tableaux : T28 à T30, les dossiers payés et ceux vérifiés après "
        "paiement (contrôles a posteriori), par trimestre (5 % au moins) ; T31, chaque mois et "
        "par branche, ce qui est payé, payé en trop (trop-perçus ou « indus »), payé en moins "
        "(2 % d'indus au plus).",
        "Le recueil v7 demande : une ligne « retraite anticipée » dans T28 ; un TOTAL = total "
        "des trimestres ; des dossiers payés ; T28 à T30 au trimestre, au semestre, à l'année, "
        "T31 au mois, au trimestre, à l'année ; les taux par direction régionale et agence ; "
        "les fichiers Excel de la DAI comme source de T28 et T31.",
        "L'annuaire CNSS 2024 confirme : la maternité avec les PF ; aucune prestation de "
        "solidarité (décision de Kaya 2017). Indice fort, sans preuve : le calcul sur la copie "
        "de la base de fin mars N+1 (444 décès de retraités pour 447).",
        "Écarts : rien de publié sur les indus ni les contrôles. Payées 2024 justes au total "
        "(−0,26 %) par compensation : pensions +1,3 %, PF −3,8 %, AT/MP −20,3 % (argent versé), "
        "−28,9 % (soins compris). Rachats de rente −15,2 %. L'annuaire compte les indemnités de "
        "maternité (3 893) et les allocations de vieillesse (833) l'année où elles sont "
        "liquidées, pas payées.",
        "Dans la base : indus 2024 = 0,39 % des sommes payées, sous la norme dans toutes les "
        "lectures ; contrôles 2025 = 0,07 % (115 pour 155 206 dossiers payés), loin des 5 %, "
        "plus rien depuis le 09/07/2025.",
        "Décisions attendues : 30 questions, aucune ne bloque. Les plus lourdes : où sont les "
        "contrôles et pouvez-vous les donner (Q1 à Q3) ; dénominateur du taux de contrôle (Q6) ; source des indus (Q14) ; dénominateur du "
        "taux d'indus (Q15) ; moins-perçus dans le taux (Q16).",
        "Reste à faire : obtenir les contrôles de la DAI ; taux par branche, direction et agence "
        "dans t31 ; tester la date des paiements mois par mois ; lot de contrôle P120 et "
        "suivantes.",
    ],
    # ------------------------------------------------------------------ 0. les tableaux
    "tableaux": [
        ["T28", "Les dossiers de pension (PVID) payés, puis contrôlés a posteriori au 1er, 2e, "
                "3e et 4e trimestre, et un TOTAL. Lignes : retraite, invalidité, veuf/veuve, "
                "orphelin, solidarité, ascendants ; allocations de retraite, de veuf/veuve, "
                "d'orphelin ; autres ; TOTAL", "p. 12, 35 ; glossaire p. 62, 64",
         "pas de requête (contrôles manquants) ; fiche de lecture ; P13"],
        ["T29", "La même grille pour les AT/MP. Lignes : indemnités journalières, rentes "
                "viagères, rentes de veuve/veuf, d'orphelins, d'ascendants, allocations, autres, "
                "TOTAL", "p. 12 et 35", "pas de requête ; fiche de lecture ; P13"],
        ["T30", "La même grille pour les PF, maternité comprise ; 1re colonne « bénéficiaires "
                "payés ». Lignes : allocations prénatales, IJ de maternité, allocations "
                "familiales, autres, TOTAL", "p. 13 et 36",
         "pas de requête ; fiche de lecture ; P13, P79"],
        ["T31", "Une ligne par mois et un TOTAL ; pour chaque branche (PVID, AT/MP, PF) puis au "
                "total : payées, trop perçus, moins perçus. Ratio des prestations indues",
         "p. 13, 36 ; glossaire p. 64 à 66",
         "t31_requete_proposee.sql (pas exécutée) ; fiche T31 ; lot 9"],
    ],
    # ------------------------------------------------------------------ 1. recueil v7
    "recueil_note": (
        "Toutes les lignes du recueil v7 qui touchent T28 à T31 (numéros de besoins_v7.txt). "
        "Les formules ont été lues dans le document Word (les mêmes que Kaya 2017). Les quatre "
        "tableaux ont leurs lignes ; T29 n'a qu'un renvoi, T29 et T30 n'ont pas de ligne "
        "« Source » ; la l. 127 (T27) sert à T30 par analogie. Ne changent rien ici : virement "
        "(l. 1085), ratio des ressources (l. 1129), inspection "
        "des employeurs (l. 1565 à 1590), rapports d'audit (l. 1991), colonnes « Priorité ». l. 80 "
        "et 81, satisfaction, nouvelles prestations : rangées dans T50-T57."
    ),
    "recueil": [
        ("#2 (T30), l. 134", "« TOTAL : total des trimestres »", "Le TOTAL additionne les "
         "contrôles des 4 trimestres, pas les dossiers payés", "N5 ; aucune requête T30",
         "à intégrer"),
        ("#2 (T30), l. 135", "« Nbre de bénéf. payés : nbre de dossiers payés »", "Des dossiers "
         "payés, pas des personnes", "N4 ; environ 104 600 en 2024 (P79) ; la CNSS compte "
         "autrement (Q7)", "à intégrer"),
        ("#2 (T30), l. 136", "« Périodicité : Trimestrielle, Semestrielle, Annuelle »",
         "Le semestre est nouveau", "N6 ; payes_depuis (Q8)", "à intégrer"),
        ("#8 (T29), l. 182, renvoi à l. 134", "« Idem. Tab 30 » : le TOTAL", "Même TOTAL pour "
         "les AT/MP (Kaya : total dans « nombre de dossiers »)", "N5", "à intégrer"),
        ("#8 (T29), l. 182, renvoi à l. 135", "« Idem. Tab 30 » : dossiers payés", "Remplace "
         "« prestations payées » de Kaya : lecture BI, à confirmer par la DCP", "N4",
         "à intégrer"),
        ("#8 (T29), l. 182, renvoi à l. 136", "« Idem. Tab 30 » : la périodicité",
         "Trimestre, semestre, année", "N6", "à intégrer"),
        ("#19 (T28), l. 255", "« Ajouter une ligne “Pension de retraite anticipée” »",
         "RPRA sur sa propre ligne", "N1 ; le README range RPRA avec la vieillesse",
         "à intégrer"),
        ("#19 (T28), l. 256", "« Source : Fichiers Excel (DAI) »", "Les contrôles sont tenus "
         "par la DAI, hors de l'application", "N11 ; vue V_BI_CONTROLE_POSTERIORI ; remarque "
         "B14 du recueil", "hors base"),
        ("#19 (T28), l. 257, renvoi à l. 134", "« Idem. Tab 30 » : le TOTAL", "Même TOTAL pour "
         "les pensions", "N5", "à intégrer"),
        ("#19 (T28), l. 257, renvoi à l. 135", "« Idem. Tab 30 » : dossiers payés",
         "Des dossiers payés pour T28", "N4", "à intégrer"),
        ("#19 (T28), l. 257, renvoi à l. 136", "« Idem. Tab 30 » : la périodicité",
         "Trimestre, semestre, année", "N6", "à intégrer"),
        ("#3 (T31), l. 142 à 144", "PVID branche pension ; AT/MP (RP) ; PF prestation "
         "familiale", "Les trois branches ; la maternité n'y est pas citée",
         "R1 (maternité avec les PF) ; t31 le fait", "pris en compte"),
        ("#3 (T31), l. 145", "« Source : Fichiers Excel (DAI) »", "La DAI ne compterait que "
         "les indus trouvés par ses contrôles (feuille 06)", "N10 : la base par défaut, le "
         "fichier pour comparer ; remarque B13", "question"),
        ("#3 (T31), l. 146", "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
         "T31 au mois, au trimestre, à l'année", "N6 ; t31 ne tourne que sur une année : "
         "periode à créer", "à intégrer"),
        ("#29 (T51), l. 326", "« Allocation de survivants : veufs/veuves, orphelins et "
         "ascendants »", "L'ascendant va avec les survivants", "N1 : RASA dans l'allocation "
         "de survivant de T28 ; aucune requête T28", "à intégrer"),
        ("#1 (T27), l. 127", "« Autres prestations de la branche : Renommer par “Frais "
         "médicaux de maternité” »", "Écrit pour T27 ; T30 a la même dernière ligne",
         "N3 par analogie ; ligne vide (pas de frais dans la base)", "question"),
        ("Indicateurs PF, l. 1111, 1116 à 1119", "Ratio de contrôle = contrôlés après "
         "paiement ÷ liquidés et payés × 100 ; mensuel, trimestriel, annuel ; type, DR, agence ; "
         "DAI, DR", "Le mois va plus loin que T30 (trimestriel)", "C1 ; N5, N6, N9 ; aucune "
         "donnée depuis juillet 2025", "hors base"),
        ("Indicateurs AT/MP, l. 1190, 1196 à 1198", "Même ratio ; annuel ; type, DR, agences ; "
         "DR, DCP", "Le taux de contrôle des AT/MP", "C1 ; contrôles de la DAI", "hors base"),
        ("Indicateurs pensions, l. 1267, 1274, 1275", "Même ratio ; annuel ; type, DR, agence",
         "Le taux de contrôle des pensions", "C1 ; contrôles de la DAI", "hors base"),
        ("Indicateurs PF, l. 1120, 1125 à 1128", "Ratio d'indus = indus ÷ prestations payées "
         "sur la période × 100 ; mensuel, trimestriel, annuel ; type, DR, agence ; DAI, DR",
         "Le taux d'indus des PF", "Fiche T31 r. 8 ; t31 ne donne que le taux total (PF "
         "0,045 % vient de P105) ; N9 ; type impossible (Q27)", "à intégrer"),
        ("Indicateurs AT/MP, l. 1199, 1207 à 1209", "Même ratio ; annuel ; DR, DCP",
         "Le taux d'indus des AT/MP", "0,47 % en 2024 (P105) ; payées trop basses (N7) ; N9",
         "à intégrer"),
        ("Indicateurs pensions, l. 1277, 1284, 1285", "Même ratio ; annuel ; type, DR, agence",
         "Le taux d'indus des pensions", "0,47 % en 2024 (P105) ; N9", "à intégrer"),
        ("Indicateurs, l. 1357, 1369, 1373 à 1375", "« Taux de prestations indues (CIPRES) » ; "
         "trimestriel, annuel ; DR et agence", "Le taux d'indus de toute la CNSS",
         "Taux total de t31 (0,39 %) ; N6, N9", "à intégrer"),
        ("Recouvrement, l. 1608 à 1614", "Le même taux ; annuel ; DCR, DR", "Déjà rangé sous "
         "le recouvrement par Kaya 2017", "Même calcul que l. 1369 (Q29)", "question"),
        ("Structures, l. 1119, 1128, 1198, 1209", "« DAI, DR » (PF) ; « DR, DCP » (AT/MP)",
         "Qui répond pour les deux ratios", "Destinataires des questions", "pris en compte"),
        ("Ratios CIPRES respectés, l. 1978 à 1989", "« Source : DAI. Données non disponibles en "
         "base »", "La part des normes CIPRES tenues", "T28 à T31 lui fournissent deux ratios "
         "(N12)", "hors base"),
        ("Termes de référence, l. 4 à 10", "Cinq directions régionales", "« Par DR » = ces "
         "cinq ; la base en a 5 et un siège inactif (P45)", "N9", "à intégrer"),
        ("Indicateurs, l. 1065", "« Donner la possibilité de désagréger l'indicateur »",
         "Chaque taux se découpe", "N9", "à intégrer"),
        ("Attentes et caractéristiques, l. 29, 51, 68", "Jusqu'au « niveau transactionnel »",
         "Descendre jusqu'à chaque trop-perçu, paiement, contrôle", "N13 (l. 29 : remarque B41)",
         "à intégrer"),
        ("Attentes, l. 31 et 74", "Présentation multidimensionnelle", "Les mêmes axes pour T28 "
         "à T31", "N9, N13", "à intégrer"),
        ("Attentes, l. 32", "« Emission d'alertes »", "Contrôle sous 5 %, indus au-dessus de "
         "2 %", "N12 ; alerte dans OBIEE", "à intégrer"),
        ("Attentes, l. 35", "Fréquences adaptées", "Chaque tableau à sa fréquence", "N6",
         "à intégrer"),
        ("Attentes, l. 28, 34, 75", "Peu de manuel ; automatisation", "Se heurte aux fichiers "
         "de la DAI", "N10 (base) ; N11 (fichier au modèle fixe)", "à intégrer"),
        ("Caractéristiques, l. 53 à 55, 60", "Charger des fichiers", "La BI peut prendre les "
         "fichiers de la DAI", "N11", "à intégrer"),
        ("Caractéristiques, l. 58", "Transcodage écrit et rejouable", "Codes et lignes "
         "écrits", "N1 à N3 ; README R2", "pris en compte"),
        ("Caractéristiques, l. 72", "Accès aux données par profil (confidentialité)",
         "Les résultats de contrôle sont sensibles", "N14 (Q10)", "à intégrer"),
        ("Attentes, l. 24 et 78", "Profilage de la qualité", "Mesurer les trous de la base",
         "Lots 1 et 9 ; points d'attention", "pris en compte"),
        ("Général, l. 15 et 92 à 94", "L'application des prestations", "T31 et les dossiers "
         "payés en viennent, pas les contrôles", "Copie USER_DWH au 02/02/2026",
         "pris en compte"),
        ("#30 (T76), l. 333", "« 2 chiffres après la virgule »", "Écrit pour le tableau 76 "
         "seulement", "N12 par analogie ; t31 arrondit à 3 (Q30)", "question"),
    ],
    # ------------------------------------------------------------------ 2. règles
    "regles": [
        {"id": "R1", "titre": "Les branches",
         "regle": "V = PVID ; A = AT/MP ; F et M (maternité) = PF. Un trop-perçu n'a pas de "
                  "prestation liée : sa branche se lit par le dossier.",
         "exemple": "un trop-perçu de maternité va en PF dans T31.",
         "sources": [(RV7, "l. 142 à 144"), (CNSS, "tableaux 40 et 41"),
                     (DON, "P02, P05, P102, P106")],
         "etat": "confirmée par l'annuaire CNSS 2024"},
        {"id": "R3", "titre": "Ce qu'est un paiement",
         "regle": "Débours de paiement (PM) au statut D ou P ; montant du débours, jamais le net "
                  "du titre de paiement.",
         "exemple": "un paiement au statut V (sens à confirmer par la DSI) ne compte pas.",
         "sources": [(KAYA, "tableau 1, l. 110"), (CIPRES, "glossaire p. 64"),
                     (DON, "P20, P21, P105")],
         "etat": "confirmée pour les pensions (+1,3 %) ; à préciser pour les PF (−3,8 %) ; non "
                 "confirmée pour les AT/MP (−20,3 % espèces, −28,9 % soins compris)"},
        {"id": "R4", "titre": "La date des payées de T31",
         "regle": "Mois payé (DEB_DATE_EFFET) aujourd'hui ; jour de saisie (DEB_DATE_INSERT) "
                  "en autre valeur. Réglage date_montant.",
         "exemple": "pension de janvier saisie le 5 février : janvier ou février.",
         "sources": [(CNSS, "tableaux 28, 41, 90"), (DON, "P98, P105")],
         "etat": "à décider (DCP) ; au trimestre la saisie suit la CNSS ; au mois, mieux pour PF "
                 "et AT/MP, pas pour les pensions"},
        {"id": "R13", "titre": "Le cliché du calcul",
         "regle": "L'exercice N se calcule sur la copie de fin mars N+1.",
         "exemple": "trop-perçu établi le 15/12/2024, saisi le 20/01/2025 : décembre 2024.",
         "sources": [(CNSS, "tableau 26 : 447 décès publiés"), (DON, "P99 : 444")],
         "etat": "proposée ; indice fort de l'annuaire CNSS 2024 (T32-T41 Q3)"},
        {"id": "I1", "titre": "Trop-perçus et moins-perçus",
         "regle": "Trop-perçus : TP, TP-CONV. Moins-perçus : MP, RC. Montrés tous deux ; seuls "
                  "les trop-perçus sont des indus et entrent dans le taux.",
         "exemple": "pension payée deux fois en mars, trop-perçu établi le 20/05 : mai, PVID.",
         "sources": [(CIPRES, "p. 13, 64 à 66"), (KAYA, "tableau 1, l. 107, 108"),
                     (DON, "P102 : 684 trop-perçus en 2024")],
         "etat": "proposée, à valider (DCP, DAI) ; Q16"},
        {"id": "Fiche T31 r. 5", "titre": "Ce qui n'est pas un indu",
         "regle": "Hors des indus : remboursement de trop-perçu, avances et leur reprise, mois "
                  "gratuits, dons COVID, rachats de rente, oppositions.",
         "exemple": "avance versée en avril, reprise en septembre : rien.",
         "sources": [(KAYA, "tableau 1"), (DON, "P41, P102, P11")],
         "etat": "proposée ; rachats et avances dans les payées ? (N7, Q22, Q23)"},
        {"id": "Fiche T31 r. 6", "titre": "Les statuts des ajustements",
         "regle": "Tous comptent, sauf le statut R (annulé). A en cours, C soldé.",
         "exemple": "un trop-perçu en cours de reprise compte au mois où il est établi.",
         "sources": [(DON, "P103 : plus de statut R depuis 2014")],
         "etat": "proposée ; sens des statuts à confirmer (Q25) ; sans effet sur 2015 à 2025"},
        {"id": "Fiche T31 r. 7", "titre": "Le mois d'un indu",
         "regle": "Une seule fois, le mois où il est établi : ni les mois payés en trop, ni les "
                  "mois où il est repris.",
         "exemple": "établi en novembre 2024, repris jusqu'en juin 2025 : novembre 2024.",
         "sources": [(DON, "P103 : date d'établissement toujours remplie")],
         "etat": "proposée ; Q17"},
        {"id": "Fiche T31 r. 8", "titre": "Le taux de prestations indues",
         "regle": "Trop-perçus ÷ payées × 100, par branche et sur l'année ; la norme (2 % au "
                  "plus) se lit sur l'année.",
         "exemple": "AT/MP avril 2024 : 4,08 % (un seul trop-perçu) ; l'année : 0,47 %.",
         "sources": [(RV7, "l. 1120, 1199, 1277, 1369"), (KAYA, "tableau 2"),
                     (DON, "P105 : 0,47 / 0,47 / 0,045 / 0,39 %")],
         "etat": "proposée, conforme au recueil ; t31 ne donne que le taux total : taux par "
                 "branche à ajouter"},
        {"id": "C1", "titre": "Ce qu'est un contrôle a posteriori",
         "regle": "Un contrôle enregistré par la DAI ; la coche « vérifié » n'en est pas un. "
                  "Taux = dossiers contrôlés après paiement ÷ dossiers payés ; 5 % au moins.",
         "exemple": "paiements tous cochés « vérifié », sans contrôle : pas contrôlé.",
         "sources": [(CIPRES, "p. 12, 13, 62"), (RV7, "l. 1111, 1190, 1267"),
                     (MOD, "feuille 05"), (DON, "P06, P13 : 0,07 % en 2025")],
         "etat": "ouverte : Q1, Q2 ; carte C1 étendue à la vue de la DAI (N11)"},
        {"id": "A1", "titre": "Les accessoires (étendue à T28)",
         "regle": "Bonification, majoration pour enfant et charges du ménage suivent la ligne "
                  "de leur pension : le dossier ne compte pas une fois de plus.",
         "exemple": "pension de vieillesse + bonification : une fois, ligne vieillesse.",
         "sources": [(CNSS, "tableaux 28 et 34"), (DON, "P34")],
         "etat": "confirmée pour les montants ; proposée pour T28 ; désaccord avec le README "
                 "(« autres prestations »)"},
        {"id": "H11", "titre": "Les DTM des contrôles sont faux",
         "regle": "DTM_CONTROLE_PRESTATION compte les payés parmi les seuls contrôlés ; "
                  "DTM_INDICATEURS_PRESTATIONS calcule le taux sur les ajustements.",
         "exemple": "le taux de contrôle ne mesure pas les contrôles de la DAI.",
         "sources": [(DON, "P06, P13 ; code lu, non modifié")],
         "etat": "confirmée ; à corriger après visa (TODO, tâche 10)"},
        {"id": "H12", "titre": "Le DTM des indus est faux",
         "regle": "DTM_PRESTATION_INDUE classe en moins-perçu tout ce qui n'est pas un "
                  "trop-perçu et exclut la maternité.",
         "exemple": "un remboursement de trop-perçu devient un indu de plus.",
         "sources": [(DON, "P11, P103 ; code lu")],
         "etat": "confirmée en partie ; à corriger après visa (TODO, tâche 9)"},
        {"id": "N1", "titre": "T28 : les lignes",
         "regle": "Vieillesse ; retraite anticipée (RPRA) à part ; invalidité ; survivant ; "
                  "ascendant ; allocation de vieillesse ; allocation de survivant (avec RASA) ; "
                  "autres (RRC) ; TOTAL. Plus de solidarité.",
         "exemple": "une retraite anticipée contrôlée va sur sa propre ligne.",
         "sources": [(RV7, "l. 255 et 326"), (KAYA, "tableau 1, l. 281 à 283"),
                     (CNSS, "tableaux 18, 22, 23, 28, 34"), (DON, "P34")],
         "etat": "nouvelle ; regroupements tranchés par Kaya 2017 ; README à reprendre"},
        {"id": "N2", "titre": "T29 : les lignes",
         "regle": "IJ ; rentes viagères ; rentes de survivants regroupées ; allocations "
                  "d'incapacité ; achat de rente (lu rachat, RR) ; autres (soins) ; TOTAL.",
         "exemple": "un rachat de rente de septembre 2024 : ligne « achat de rente ».",
         "sources": [(KAYA, "tableau 1, l. 173 et 175"), (CNSS, "tableaux 90, 96, 97"),
                     (DON, "P104 : 11 rachats")],
         "etat": "nouvelle ; « achat » = « rachat » ? (Q11)"},
        {"id": "N3", "titre": "T30 : les lignes",
         "regle": "Allocations prénatales (FAP, MAP) ; IJ de maternité ; allocations familiales ; "
                  "« frais médicaux de maternité » à la place de « autres » ; TOTAL.",
         "exemple": "une allocation prénatale de maternité (MAP) : ligne des prénatales.",
         "sources": [(RV7, "l. 127 (pour T27)"), (CNSS, "tableaux 40, 41"),
                     (DON, "P35, P95")],
         "etat": "nouvelle, par analogie (Q12) ; ligne vide aujourd'hui"},
        {"id": "N4", "titre": "T28 à T30 : le dossier payé",
         "regle": "Au moins un paiement dans la période : PM en général, RB pour les soins, "
                  "ajustement RR pour le rachat. Une fois par ligne, une fois au TOTAL. "
                  "Coordination payée ailleurs exclue.",
         "exemple": "IJ en mars et rente en octobre : une fois par ligne, une fois au TOTAL.",
         "sources": [(CIPRES, "glossaire p. 64"), (RV7, "l. 135, 182, 257"),
                     (CNSS, "tableaux 24, 35, 39"), (DON, "P13, P79, P93")],
         "etat": "nouvelle ; l'annuaire compte à la liquidation (Q7, dossier_paye)"},
        {"id": "N5", "titre": "Contrôle, trimestre et TOTAL",
         "regle": "Trimestre de la date du contrôle, ligne de la prestation contrôlée. TOTAL = "
                  "somme des trimestres ; le taux compte un dossier une fois. Dates impossibles "
                  "écartées.",
         "exemple": "contrôlé le 12/02 et le 20/10 : deux fois au TOTAL, une fois au taux.",
         "sources": [(CIPRES, "p. 35, 36, 62"), (RV7, "l. 134 ; formule l. 1111"),
                     (KAYA, "tableau 1 (T29, T30)"), (DON, "P13")],
         "etat": "nouvelle ; Q9, controle_compte"},
        {"id": "N6", "titre": "Les périodes",
         "regle": "T28 à T30 : trimestre, semestre, année ; T31 : mois, trimestre, année. Payés "
                  "depuis le 1er janvier. Un taux se recalcule sur les sommes.",
         "exemple": "T31 du 1er trimestre 2024 : trop-perçus ÷ payées de janvier à mars.",
         "sources": [(RV7, "l. 136, 146, 1116, 1125, 1373"), (CIPRES, "p. 12, 13")],
         "etat": "nouvelle ; Q8, periode, payes_depuis"},
        {"id": "N7", "titre": "Payées de T31 = prestations servies de T56",
         "regle": "Sur l'année, les payées d'une branche devraient égaler T56. Restent à "
                  "trancher : rachats, rappels (408,3 M), avances (373,2 M), soins hors débours, "
                  "frais médicaux de maternité.",
         "exemple": "AT/MP 2024 : 744,6 M payés pour 934,4 M publiés en espèces (−20,3 %).",
         "sources": [(BI, "cohérence entre T31 et T56"), (CNSS, "tableaux 90, 98, 99, 106"),
                     (DON, "P95, P104, P105")],
         "etat": "nouvelle ; Q22, Q23, payees_types"},
        {"id": "N8", "titre": "Les autres taux de T31",
         "regle": "À côté du taux d'indus : taux des moins-perçus, et les deux ensemble rapportés "
                  "à toutes les payées. Le taux de la grille (÷ dossiers contrôlés) ne se calcule "
                  "pas.",
         "exemple": "2024 : 0,39 % ; 0,71 % ; ensemble 1,10 %, sous 2 % partout.",
         "sources": [(MOD, "feuille 06"), (CIPRES, "p. 13"), (DON, "P105")],
         "etat": "nouvelle ; Q15, Q16"},
        {"id": "N9", "titre": "Branche, direction, agence, type",
         "regle": "Un taux par branche ; par direction régionale et agence du dossier, payées "
                  "comprises (pas la direction du titre de paiement). Type de prestation pour "
                  "T28 à T30 ; branche seule pour les indus.",
         "exemple": "taux d'indus de la DR de Bobo-Dioulasso = ses trop-perçus ÷ ses payées.",
         "sources": [(RV7, "l. 4 à 10, 1065, 1118, 1127, 1375"),
                     (CNSS, "tableaux 29 à 34, 40, 42 à 46, 91 à 96, 98"), (DON, "P07, P106")],
         "etat": "nouvelle ; Q27, Q28"},
        {"id": "N10", "titre": "T31 : la source des indus",
         "regle": "Par défaut, tous les ajustements de l'application (table AJUSTEMENT) ; le "
                  "fichier de la DAI sert à comparer.",
         "exemple": "comparer le total 2024 de la DAI aux 684 trop-perçus de la base.",
         "sources": [(RV7, "l. 145 ; l. 28, 34, 75"), (DON, "P102 à P106")],
         "etat": "nouvelle ; écart à une source plus forte, gardé en attendant la DAI (Q14)"},
        {"id": "N11", "titre": "T28 à T30 : la source des contrôles",
         "regle": "La vue V_BI_CONTROLE_POSTERIORI, alimentée par la DSI depuis la table ou le "
                  "fichier de la DAI. La table des contrôles est bien trop maigre pour servir "
                  "seule.",
         "exemple": "un contrôle de 2026 du fichier de la DAI arrive par la vue.",
         "sources": [(RV7, "l. 256 ; l. 53 à 60"), (KAYA, "tableau 1 (T28, T30)"),
                     (DON, "P01 : 1 542 lignes en tout ; P13 : 212 par an au plus, "
                                "pour environ 7 760 nécessaires en 2025")],
         "etat": "nouvelle ; Q1 à Q3"},
        {"id": "N12", "titre": "Normes, alertes, arrondi",
         "regle": "« Dans les normes » ou « hors normes » : contrôle 5 % au moins, indus 2 % au "
                  "plus sur l'année. Normes dans la CTE parametres. Taux à 3 chiffres après la "
                  "virgule.",
         "exemple": "2024 : 0,391 % d'indus, dans les normes ; 2025 : 0,07 % de contrôle, hors.",
         "sources": [(RV7, "l. 32, 1978 ; l. 333 (T76)"), (CIPRES, "p. 12, 13"),
                     (MOD, "feuilles 05, 06")],
         "etat": "nouvelle, à intégrer ; arrondi : Q30"},
        {"id": "N13", "titre": "Retrouver chaque chiffre",
         "regle": "Une ligne par trop-perçu, moins-perçu, paiement et contrôle, avec branche, "
                  "date, direction, agence.",
         "exemple": "les 684 trop-perçus de 2024, un à un.",
         "sources": [(RV7, "l. 51, 68 (et l. 29)")],
         "etat": "nouvelle ; t31 ne sort que des totaux"},
        {"id": "N14", "titre": "La confidentialité des contrôles",
         "regle": "Résultat et indu d'un dossier réservés aux profils autorisés. Textes libres "
                  "non lus ; ni nom ni numéro bancaire dans la vue.",
         "exemple": "une DR voit son taux, pas l'anomalie d'un dossier précis.",
         "sources": [(RV7, "l. 72")],
         "etat": "nouvelle ; Q10"},
    ],
    # ------------------------------------------------------------------ 3. cas
    "cas_note": (
        "Les 12 cas les plus parlants (17 dans le md). Chaque carte dit ce que devient la somme "
        "ou le dossier, et selon quelle règle."
    ),
    "cas": [
        ("Cas 1. Un retraité payé deux fois", "Mars 2024 payé deux fois ; trop-perçu établi le "
         "20/05/2024, repris par retenues.", "COMPTÉ : mai 2024, PVID, dans le taux (I1, "
         "fiche T31 r. 7)", "compte"),
        ("Cas 4. Une veuve sous-payée", "Avril à juin 2024 ; moins-perçu établi en juin, payé "
         "en juillet.", "SELON UN RÉGLAGE : moins perçus de juin, hors du taux (taux_numerateur, "
         "Q16)", "reglage"),
        ("Cas 5. Une avance sur pension", "Versée en avril 2025, reprise sur le rappel en "
         "septembre.", "PAS COMPTÉ dans les indus ; payées : selon payees_types (Q23)",
         "pas compte"),
        ("Cas 6. Un décès déclaré tard", "Décès en février 2025, déclaré en juin ; pension "
         "versée de mars à mai.", "QUESTION : trop-perçu seulement s'il est établi ; 7 en 2024 "
         "pour 175 décès tardifs (Q18)", "question"),
        ("Cas 7. Un trop-perçu remboursé le mois même", "AT/MP, avril 2024 : 2 306 520 FCFA, "
         "remboursé en avril.", "À SURVEILLER : avril à 4,08 %, l'année à 0,47 % ; la norme se "
         "lit sur l'année", "attention"),
        ("Cas 8. Un trop-perçu sur convention", "Pension de coordination entre caisses "
         "(TP-CONV), établi en 2025.", "À SURVEILLER : compté en PVID ; 758 en 2025 contre 15 "
         "en 2024 (Q21)", "attention"),
        ("Cas 9. Un rachat de rente", "Capital versé en septembre 2024 à la place de la rente, "
         "par un ajustement RR.", "SELON UN RÉGLAGE : pas un indu ; payées si la CNSS le dit ; "
         "T29 « achat de rente » (Q11, Q22)", "reglage"),
        ("Cas 10. Un contrôle sans anomalie", "Dossier de pension contrôlé par la DAI le "
         "12/02/2025, tout est conforme.", "COMPTÉ : T28, 1er trimestre, dans le taux (C1, N5)",
         "compte"),
        ("Cas 11. Un dossier contrôlé deux fois", "Contrôlé en mars, puis en octobre 2025.",
         "SELON UN RÉGLAGE : deux fois au TOTAL, une fois au taux (controle_compte, Q9)",
         "reglage"),
        ("Cas 12. Des paiements cochés « vérifié »", "Tous les débours ont la coche "
         "DEB_VERIFIE ; aucun contrôle enregistré.", "PAS COMPTÉ : la coche est faite à la "
         "saisie (C1, H11)", "pas compte"),
        ("Cas 13. Un contrôle d'août 2025", "Contrôle de prestations familiales ; la table n'a "
         "plus rien depuis le 09/07/2025.", "À SURVEILLER : invisible sans la DAI ; T30 2025 "
         "faussement bas (N11, Q1)", "attention"),
        ("Cas 15. Une IJ de maternité sur deux ans", "Liquidée en novembre 2024, payée en "
         "novembre 2024 et en janvier 2025.", "SELON UN RÉGLAGE : payée en 2024 et 2025 (N4) ; "
         "2024 seulement à la liquidation (Q7)", "reglage"),
    ],
    # ------------------------------------------------------------------ 4. annuaire
    "annuaire_note": (
        "L'annuaire ne publie ni contrôles a posteriori ni indus. Il sert à contrôler les payées "
        "(tableaux 28 à 34, 40 à 46, 82, 90 à 99, 106) et à voir comment la CNSS compte ses "
        "bénéficiaires (24, 35, 37, 39). Les tableaux 11, 100 à 102 et 107 à 113 parlent "
        "d'autres contrôles et d'autres paiements."
    ),
    "annuaire": [
        ("Liste des tableaux 1 à 126", "Trop-perçus, moins-perçus, indus, contrôles", "indus "
         "0,39 % ; contrôle 0,07 %", "rien", "—", "non publie", "Ne se vérifient qu'avec la "
         "DAI (feuilles 05 et 06)"),
        ("Tableaux 28 et 34", "Payées PVID 2024", "47 237 434 040 (mois payé) ; 47 486 010 786 "
         "(saisie)", "46 629 625 556", "+1,30 % ; +1,84 %", "precise", "R3 tient. Saisie : "
         "trimestres justes, rythme des mois (0,75), écart moyen non réduit (6,4 % / 6,2 %)"),
        ("Tableaux 28 et 34, zéros", "Solidarité, pension gratuite, remboursements",
         "aucun code (P34, P11)", "0", "aucun", "confirme", "Conforte Kaya 2017 : plus de "
         "pension de solidarité dans T28 (N1)"),
        ("Tableaux 18, 22, 23", "Retraite anticipée au 31/12/2024", "87 (A01)", "78 (66 à 78)",
         "+11,5 %", "precise", "Ligne à part dans les effectifs (N1) ; fondue dans la "
         "vieillesse pour les montants"),
        ("Tableaux 40 et 41", "Où va la maternité", "avec les PF (R1)", "dans les PF",
         "aucun", "confirme", "R1 confirmée pour T30 et T31"),
        ("Tableaux 40 et 41", "Payées PF 2024", "10 750 873 342 ; 10 802 063 700",
         "11 180 191 425", "−3,84 % ; −3,38 %", "precise", "AF −3,4 %, IJ −4,2 %, prénatales "
         "−4,0 % ; frais médicaux = 0,2 point (P95)"),
        ("Tableaux 90, 96, 97", "Payées AT/MP 2024 face aux espèces", "744 609 484 ; "
         "790 138 121", "934 445 128", "−20,3 % ; −15,4 %", "ecart", "Taux AT/MP : 0,47 % "
         "chez nous, 0,38 % sur les espèces publiées"),
        ("Tableaux 82, 98, 99, 106", "Paiements + soins remboursés face à l'ensemble",
         "763 397 429", "1 074 113 823", "−28,9 %", "ecart", "Taux AT/MP 0,33 % sur l'ensemble ; "
         "où sont payés les soins ? (N7, Q22)"),
        ("Tableaux 90, 96, 97", "Rachats de rente 2024", "21 519 361 (11)", "25 366 909",
         "−15,2 %", "ecart", "Prestation servie : ligne de T29 (N2) ; payées si confirmé (Q22)"),
        ("Tableau 106 (et 103 à 105)", "Total des payées 2024", "58 732 916 866",
         "58 883 930 804", "−0,26 %", "precise", "Juste par compensation (pensions +1,3 %, PF "
         "−3,8 %, AT/MP −30,7 %) ; taux 0,391 / 0,390 %"),
        ("Tableaux 35 et 39", "Bénéficiaires d'IJ de maternité 2024", "3 890 liquidées ; "
         "4 881 payées (P93)", "3 893", "−0,1 % ; +25 %", "precise", "Comptés à la "
         "liquidation : touche N4 (Q7)"),
        ("Tableau 24", "Allocations de vieillesse 2024", "832 liquidées ; 753 payées",
         "833", "−1 ; −80", "precise", "Même pratique (Q7)"),
        ("Tableaux 37 et 38", "Allocataires 2024 (T30)", "104 611 payés (N4) ; 98 048 au "
         "31/12 (P79)", "97 378", "+7,4 % ; +0,7 %", "precise", "La CNSS compte un stock ; "
         "avec N4, environ 5 230 contrôles pour 5 %"),
        ("Tableaux 18 et 22", "Pensions face aux dossiers payés", "47 732 dossiers (2025)",
         "62 231 pensions", "—", "non comparable", "Une pension n'est pas un dossier (Q4) ; "
         "2 901 de coordination non payées (P78)"),
        ("Tableaux 79, 80 (76, 78)", "Rentes AT/MP au 31/12/2024", "2 134 dossiers A (2025)",
         "2 232 rentes", "—", "non comparable", "Ordre de grandeur seulement des lignes de "
         "rentes de T29"),
        ("Tableaux 29 à 34, 40, 42 à 46, 91 à 96, 98", "Sommes servies par direction",
         "pas calculé", "publié", "—", "precise", "Pour contrôler les payées par direction "
         "(N9)"),
        ("Tableau 26", "Décès de pensionnés 2024", "444 retraités (P99)", "447 (648 en tout)",
         "−0,7 %", "precise", "Indice fort pour R13, comme T50-T57 ; repère pour Q18"),
        ("Tableaux 100 et 101", "Entreprises contrôlées 2024", "115 contrôles de dossiers "
         "(2025)", "178 entreprises", "sans objet", "non comparable", "Prévention des "
         "accidents : pas des contrôles de dossiers"),
        ("Tableaux 102 et 11", "Comités de sécurité ; employeurs contrôlés", "—", "16 + 23 ; "
         "8 794", "sans objet", "non comparable", "Hors famille (recouvrement)"),
        ("Tableaux 107 à 113 (et 8)", "Remise en banque des titres de paiement", "—", "38 413 "
         "titres", "sans objet", "non comparable", "Encaissement des chèques des cotisants ; "
         "pics des cotisations trimestrielles (tableau 8)"),
    ],
    "annuaire_apres": (
        "Confirme : la maternité dans les PF (R1), la fin de la pension de solidarité (N1, Kaya "
        "2017). Précise : le cliché de fin mars (R13, indice fort), la date des payées (R4), la ligne "
        "anticipée, le dossier payé (liquidation, stock), le total des payées (compensation), "
        "le découpage par direction. Contredit : rien dans nos règles. À expliquer : les payées "
        "AT/MP et les rachats. Pas publié : contrôles, trop-perçus, moins-perçus, taux."
    ),
    # ------------------------------------------------------------------ 5. réglages
    "reglages_note": (
        "Dans la CTE parametres en tête de chaque requête. « À créer » : le choix est écrit en "
        "dur, ou n'existe pas encore. La requête de T28 à T30 reste à écrire : ses réglages "
        "sont proposés dès maintenant. Une décision attendue de la CNSS ne bloque rien."
    ),
    "reglages": [
        ("annee, debut, fin", "t31 (tirés de l'exercice donné au lancement)", "l'année "
         "demandée, du 01/01 au 31/12", "—", "Bornent paiements et ajustements",
         "l'utilisateur, à chaque lancement"),
        ("periode (à créer)", "t31 ; requête T28 à T30", "MOIS pour T31, TRIMESTRE pour T28 à "
         "T30 ; taux recalculé sur les sommes", "période libre (début, fin)", "T31 au "
         "trimestre, T28 à T30 au semestre (N6)", "tranché par le recueil (l. 136, 146)"),
        ("Le cliché (choisi au lancement, pas un réglage de parametres)", "t31", "fin mars N+1 (R13) ; "
         "aujourd'hui, la copie du 02/02/2026", "le dernier cliché chargé", "Les ajustements "
         "saisis après coup comptent", "BI, avec la DSI"),
        ("date_montant", "t31 (et t32_t41, t42_t49, t50_t57)", "'MOIS' : mois payé",
         "'SAISIE' : jour de saisie, rappels compris", "2024 : 58,73 ou 59,08 Md payés ; taux "
         "0,39 % dans les deux cas", "DCP (R4, point du 02/11) ; la DPC renseigne (Q24)"),
        ("statut_ecarte (à créer)", "t31 (en dur : AJ_STATUT <> 'R')", "R écarté",
         "aucun écarté", "Aucun effet sur 2015 à 2025", "DSI (Q25)"),
        ("rem_tp_deduit (à créer)", "t31", "NON : trop-perçus bruts", "OUI : moins les "
         "remboursements", "2024 : 0,39 % brut, 0,37 % net (13 024 760 remboursés)",
         "DCP, avec la DAI (Q19)"),
        ("payees_types (à créer)", "t31 (en dur : PM)", "PM seulement", "+ RB (soins) ; + RR "
         "(rachats) ; + rappels et avances", "AT/MP 2024 : 744,6 ; 763,4 ; 784,9 M (taux 0,47, "
         "0,46, 0,45 %) ; rappels et avances : +781,5 M", "DCP, avec la DPC (Q22, Q23)"),
        ("taux_numerateur (à créer)", "t31 (N8)", "'TP' : trop-perçus seuls", "'TP+MP' : "
         "trop-perçus et moins-perçus", "2024 : 0,39 % ou 1,10 %", "DAI, avec la DCP (Q16)"),
        ("taux_denominateur (à créer)", "t31 (N8)", "'PAYEES' : toutes les payées (recueil)",
         "'CONTROLES' : montant des dossiers contrôlés (feuille 06)", "Pas calculable "
         "aujourd'hui : montant absent des sources", "DAI (Q15)"),
        ("source_indus (à créer)", "t31 (N10)", "'BASE' : table AJUSTEMENT", "'DAI' : fichier "
         "de la DAI", "Tous les trop-perçus, ou peut-être ceux des seuls contrôles",
         "DAI, avec la DCP (Q14)"),
        ("source_controles (à créer)", "requête T28 à T30 (N11)", "vue V_BI_CONTROLE_POSTERIORI",
         "table DOSSIERS_CONTROLES (incomplète)", "Sans la vue, presque aucun contrôle",
         "DSI et DAI (Q1 à Q3)"),
        ("denominateur_controle (à créer)", "requête T28 à T30 (C1)", "'PAYES' : dossiers payés",
         "'PORTEFEUILLE' : liquidés ou en portefeuille (feuille 05)", "À mesurer (suspendus, "
         "coordination)", "DAI (Q6)"),
        ("dossier_paye (à créer)", "requête T28 à T30 (N4)", "'PAIEMENT' : payé dans la période",
         "'LIQUIDATION' : liquidé dans la période (prestations ponctuelles)", "IJ de maternité "
         "2024 : 4 881 ou 3 890", "DAI, avec la DCP (Q7)"),
        ("payes_depuis (à créer)", "requête T28 à T30 (N6)", "CUMUL depuis le 1er janvier",
         "TRIMESTRE seul", "En cumul, le 4e trimestre donne le taux de l'année",
         "DAI, avec la DCP (Q8)"),
        ("controle_compte (à créer)", "requête T28 à T30 (N5)", "'DOSSIERS' : une fois dans le "
         "taux", "'CONTROLES' : chaque contrôle", "Change le taux des dossiers contrôlés "
         "plusieurs fois", "DAI (Q9)"),
        ("norme_controle, norme_indus (à créer)", "requête T28 à T30 ; t31 (N12)", "5 % au "
         "moins ; 2 % au plus", "d'autres normes", "« Dans » ou « hors normes »", "DAI"),
        ("decimales (à créer)", "t31 ; requête T28 à T30 (N12)", "3 (en dur aujourd'hui)",
         "2, comme le tableau 76", "PF 2024 : 0,045 % ou 0,04 %", "DAI (Q30)"),
        ("agence (à créer)", "t31 ; requête T28 à T30 (N9)", "SERVICE du dossier",
         "LIEU_PAIEMENT", "Le découpage par agence", "DSI (Q28)"),
    ],
    # ------------------------------------------------------------------ 6. questions
    "questions_note": (
        "Une question par point, avec ce que fait la requête en attendant. Destinataires du "
        "recueil : DAI et DR (PF), DR et DCP (AT/MP) ; pensions sans structure : DAI avec la DCP. "
        "DAI audit interne ; DCP prestations ; DSI informatique ; DPC annuaire ; DCR recouvrement. "
        "Les questions de Kaya 2017 sur les regroupements de T28 et T29 sont tranchées (N1, N2)."
    ),
    "questions": [
        ("Q1", "Depuis juillet 2025, où sont enregistrés les contrôles a posteriori ?",
         "DAI, avec la DSI", "T28 à T30 pas produits",
         "Déjà posée : README ; points_a_valider n° 29 ; vues cnss 02, q. 1 ; C1"),
        ("Q2", "La table des contrôles de l'application a-t-elle un jour contenu tous vos "
               "contrôles ?", "DAI", "la table ne sert pas, même pour le passé", "Nouvelle"),
        ("Q3", "Pouvez-vous donner vos contrôles des cinq derniers exercices, un par ligne "
               "(dossier, prestation, date, résultat, indu trouvé) ?", "DAI, avec la DSI",
         "rien : la vue est seulement proposée", "Déjà posée : vues cnss 02 (étendue à 5 ans)"),
        ("Q4", "Un contrôle porte-t-il sur tout le dossier, ou sur une seule prestation ?",
         "DAI", "la ligne de la prestation contrôlée", "Déjà posée : vues cnss 02, q. 2"),
        ("Q5", "Quels résultats peut avoir un contrôle (conforme, anomalie, en cours…) ?",
         "DAI", "tout contrôle compte", "Déjà posée : vues cnss 02, q. 3"),
        ("Q6", "Le taux de contrôle divise-t-il par les dossiers payés, ou par les dossiers "
               "« liquidés ou en portefeuille » de votre grille ?", "DAI, avec la DCP et les DR",
         "les dossiers payés", "Nouvelle"),
        ("Q7", "Pour les prestations versées une fois ou sur quelques mois, le dossier payé "
               "est-il celui payé ou celui liquidé dans la période ?", "DAI, avec la DCP",
         "payé dans la période", "Nouvelle"),
        ("Q8", "Au trimestre ou au semestre, compte-t-on les dossiers payés depuis le 1er "
               "janvier ?", "DAI, avec la DCP", "oui ; les contrôles par trimestre",
         "Nouvelle"),
        ("Q9", "Dans le taux de contrôle, un dossier contrôlé deux fois dans l'année "
               "compte-t-il deux fois ?", "DAI", "une fois au taux, deux fois au TOTAL",
         "Nouvelle"),
        ("Q10", "Qui peut voir, dans la BI, le résultat des contrôles et l'indu trouvé pour "
                "chaque dossier ?", "DAI, avec la DSI", "seuls les taux sont diffusés",
         "Nouvelle"),
        ("Q11", "L'« achat de rente » de Kaya pour T29 est-il le rachat de rente (un capital à "
                "la place de la rente) ?", "DCP, avec les DR", "oui", "Nouvelle (voisine de "
         "T25-T27 Q11)"),
        ("Q12", "Dans T30, « Autres prestations de la branche » devient-elle « Frais médicaux "
                "de maternité », comme dans T27 ?", "DCP, avec la DES", "oui ; ligne vide",
         "Nouvelle pour T30 (T27 : recueil l. 127)"),
        ("Q13", "Pour la CIPRES, faut-il aussi garder la maquette d'origine de T28 à T30 ?",
         "DCP, avec la DPC", "la version de Kaya et du recueil",
         "Déjà posée : T25-T27 Q22"),
        ("Q14", "T31 prend-il tous les trop-perçus de l'application, ou seulement ceux trouvés "
                "par les contrôles de la DAI ?", "DAI, avec la DCP",
         "tous ceux de l'application", "Déjà posée : vues cnss 02, q. 4 ; remarque B13"),
        ("Q15", "Le taux d'indus divise-t-il par toutes les prestations payées, ou par le seul "
                "montant des dossiers contrôlés ?", "DAI", "toutes les payées (recueil)",
         "Nouvelle"),
        ("Q16", "Les moins-perçus entrent-ils dans le taux d'indus ?", "DAI, avec la DCP",
         "non : montrés à part (0,39 % sans, 1,10 % avec)",
         "Déjà posée : fiche T31 Q1 ; README ; points_a_valider n° 17"),
        ("Q17", "Un trop-perçu se compte-t-il le mois où il est établi ?", "DCP",
         "oui", "Déjà posée : fiche T31 Q2 ; README"),
        ("Q18", "Une pension versée après un décès déclaré en retard devient-elle un "
                "trop-perçu ?", "DCP", "seuls les trop-perçus enregistrés",
         "Déjà posée : fiche T31 Q3 ; points_a_valider n° 30"),
        ("Q19", "Comment voit-on qu'un trop-perçu a été remboursé ?", "DCP, avec la DSI",
         "le remboursement n'entre pas dans T31",
         "Déjà posée : fiche T31 Q5 ; points_a_valider n° 31"),
        ("Q20", "Pourquoi les moins-perçus des PF ont-ils presque disparu (56 362 en 2023, "
                "1 384 en 2025) ?", "DCP", "comptés tels quels",
         "Déjà posée : README ; points_a_valider n° 32"),
        ("Q21", "Pourquoi les trop-perçus sur convention passent-ils de 15 en 2024 à 758 en "
                "2025 ?", "DCP", "comptés en PVID", "Nouvelle (constat du README, lot 9)"),
        ("Q22", "Les payées de T31 doivent-elles égaler vos prestations servies (soins, "
                "rachats, frais médicaux de maternité compris) ?", "DCP, avec la DPC",
         "les débours de paiement seulement", "Déjà posée : points_a_valider n° 52"),
        ("Q23", "Les rappels et les avances versés par ajustement sont-ils des payées de "
                "T31 ?", "DCP, avec la DAI", "non",
         "Déjà posée : fiche T31 Q4 ; points_a_valider n° 33"),
        ("Q24", "Dans vos tableaux mois par mois, un paiement va-t-il au mois qu'il paie, au "
                "mois de sa saisie, ou au mois de son versement ?", "DPC (études et "
                "statistique)", "le mois payé",
         "Déjà posée : n° 1 ; README R4 ; aussi T32-T41 Q2, T50-T57 Q1"),
        ("Q25", "Le statut R d'un ajustement veut-il dire « annulé » ?", "DSI",
         "écarté ; sans effet depuis 2015",
         "Déjà posée : fiche T31 Q5 ; points_a_valider n° 17 et 58"),
        ("Q26", "Parmi les paiements liés à un trop-perçu, lesquels sont des retenues, "
                "lesquels des versements ?", "DSI", "T31 lit le montant établi",
         "Déjà posée : README ; points_a_valider n° 61"),
        ("Q27", "Un trop-perçu n'a pas de prestation liée : la branche suffit-elle pour les "
                "taux d'indus ?", "DCP, avec la DSI", "par branche seulement", "Nouvelle"),
        ("Q28", "L'agence d'un dossier est-elle bien son service dans l'application ?",
         "DSI, avec la DPC", "service du dossier",
         "Déjà posée : T25-T27 Q20 ; remarque B34"),
        ("Q29", "Le taux d'indus rangé aussi sous le recouvrement (l. 1608) est-il suivi par la "
                "DCR ou par la DAI ?", "DCR, avec la DAI", "un seul calcul (l. 1369)",
         "Nouvelle"),
        ("Q30", "Les taux s'affichent-ils avec 2 chiffres après la virgule, comme le "
                "tableau 76 ?", "DAI", "3 chiffres, comme t31", "Nouvelle (remarque B35)"),
    ],
    # ------------------------------------------------------------------ 7. points d'attention
    "attention_note": "Les 17 points du md, regroupés en 9.",
    "attention": [
        ("Rien de publié sur les indus et les contrôles", "Ces chiffres ne se valident qu'avec "
         "la DAI. L'annuaire contrôle les payées et montre comment la CNSS compte ses "
         "bénéficiaires."),
        ("Trois lectures du taux d'indus", "Grille : ÷ dossiers contrôlés, moins-perçus "
         "compris. Recueil : ÷ toutes les payées. Kaya : sans les moins-perçus. À trancher par "
         "la DAI (Q15, Q16)."),
        ("La DAI comme source", "Le recueil la cite pour T28 et T31 ; T31 lit la base, écart "
         "gardé en attendant (N10). La table des contrôles est bien trop maigre (1 542 lignes "
         "en tout, environ 7 760 contrôles nécessaires en 2025) : T28 à T30 restent vides."),
        ("0,07 % à recompter", "Les 155 206 dossiers comptent tout débours ; 115 contrôles, "
         "lignes ou dossiers ? 2025 s'arrête au 9 juillet. Le taux restera très loin des 5 %."),
        ("Payées justes par compensation", "Total −0,26 %, mais AT/MP −20,3 % (espèces) et "
         "−28,9 % (soins) : taux AT/MP gonflé (0,47 % contre 0,38 et 0,33 %). T31 et T56 ne "
         "prennent pas les mêmes paiements."),
        ("La date des payées", "Mois par mois, aucune date ne retombe (écart moyen de 6 % "
         "pour les pensions). Une date de saisie n'est pas une date de versement. Ne pas "
         "trancher R4 sur l'année seule."),
        ("La norme et les mois", "2 % tenu en 2024 dans toutes les lectures calculables "
         "(0,39 % ; 1,10 %). Un seul trop-perçu peut porter un mois à 4,08 %. Trop-perçus sur "
         "convention : 15, puis 758 (Q21)."),
        ("Dossier payé ou liquidé", "L'annuaire compte à la liquidation (IJ de maternité, "
         "allocations de vieillesse) et en stock (allocataires). Avec N4, les IJ de T30 "
         "seraient 25 % plus hautes (Q7)."),
        ("DTM et documents existants", "DTM faux (H11, H12, statut F pris pour « payé », "
         "coche « vérifié », direction du titre de paiement). Fiche de lecture et README à "
         "reprendre après validation (solidarité, RPRA, accessoires)."),
    ],
    # ------------------------------------------------------------------ 8. reste et sources
    "reste": [
        ("Où sont les contrôles depuis juillet 2025 ; fournir ceux des 5 derniers exercices "
         "dans la vue (Q1 à Q3)", "DAI et DSI, via toi", "à poser"),
        ("Présenter les lectures des deux taux et recueillir la décision (Q6, Q14 à Q16)",
         "toi, avec la DAI et la DCP", "à faire"),
        ("t31 : taux par branche, puis direction et agence du dossier, payées comprises (N9)",
         "Claude", "à faire"),
        ("t31 : période libre, autres taux, normes, réglages « à créer » (N6, N8, N12)",
         "Claude", "après visa"),
        ("Lot P120 et suivantes : code de prestation des contrôles, dossiers distincts, "
         "dénominateur 2025 recompté, trop-perçus après un contrôle", "Claude, puis toi",
         "à faire"),
        ("Tester la date des payées mois par mois (date de l'effet), tableaux 28, 41, 90",
         "Claude, puis toi", "à faire"),
        ("Comparer les payées par direction (tableaux 29 à 34, 40, 42 à 46, 91 à 96, 98)",
         "Claude, après exécution", "à faire"),
        ("Fichier des indus 2024 de la DAI face aux 684 trop-perçus de la base (N10)",
         "DAI, via toi", "à poser"),
        ("Le mois où l'annuaire range un paiement (Q24)", "DPC, via toi", "à poser"),
        ("Requête T28 à T30, puis corrections de DTM_CONTROLE_PRESTATION et "
         "DTM_INDICATEURS_PRESTATIONS (TODO, tâche 10)", "Claude", "en attente des contrôles"),
        ("Requête DTM_PRESTATION_INDUE corrigée (TODO, tâche 9)", "Claude", "après visa"),
        ("Poser Q1 à Q30", "CNSS, via toi", "à poser"),
        ("Reporter ces règles dans README, fiche T31, fiche de lecture, regles_prestations, "
         "points_a_valider", "Claude, sur demande", "non commencé"),
    ],
    "sources": (
        "Sources : recueil des besoins v7 (besoins_v7.txt et formules du document Word) ; atelier "
        "de Kaya 2017, tableaux 1 et 2 ; Annuaire CIPRES p. 12, 13, 35, 36, 62, 64 à 66 ; "
        "annuaire statistique de la CNSS 2024 ; classeur « Evaluation des Ratios CIPRES 2025 », "
        "feuilles 05 et 06 ; lots 1, 3, 5, 6, 8, 9 et comparaison A01 (copie de la base au "
        "02/02/2026). Dossier de la famille et sa contre-vérification (style_grh/travail). "
        "Détail : analyses/prestations/regles_t28_t31_controles_indus.md."
    ),
}


def build():
    d = construire(DATA)
    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
