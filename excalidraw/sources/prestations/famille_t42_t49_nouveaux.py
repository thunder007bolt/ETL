"""
Schéma « T42 à T49 : les nouveaux bénéficiaires de l'année, les règles en clair » (état au
08/11/2026).

Famille F4 des prestations « à l'image du GRH » : ce que demande le recueil des besoins v7,
les règles (R2, R3, R4, R5, R7, R8, R9, R13, A1 reprises, N1 à N8 nouvelles), les cas, la
comparaison avec l'annuaire statistique de la CNSS 2024, les réglages de la CTE parametres,
les questions ouvertes (Q1 à Q21), les points d'attention et ce qui reste à faire.

Le détail est dans analyses/prestations/regles_t42_t49_nouveaux.md : mêmes identifiants, mêmes
statuts, mêmes verdicts. Le travail existant (README, t42_t49_requete_proposee.sql, fiche T42,
regles_prestations, points_a_valider) n'est pas modifié.

Usage :
    python3 excalidraw/sources/prestations/famille_t42_t49_nouveaux.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import main  # noqa: E402
from gabarit_famille import construire  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "T42 T49 nouveaux" / "regles_t42_t49_nouveaux.excalidraw"

RV7 = "Recueil v7"
REU = "Réunion"
KAYA = "Kaya 2017"
CIPRES = "Annuaire CIPRES"
CNSS = "Annuaire CNSS 2024"
MOD = "Modèles CNSS"
DON = "Données"
BI = "Proposition BI"

DATA = {
    "seed": 4249,
    "titre": "T42 à T49 : les nouveaux bénéficiaires de l'année (pensionnés et rentiers)",
    "sous_titre": (
        "Les règles en clair, état au 08/11/2026. T42 à T44 : nouveaux retraités, veufs et "
        "veuves, orphelins de la branche des pensions ; T45 à T49 : nouveaux rentiers des "
        "accidents du travail et maladies professionnelles (AT/MP) : tous, orphelins, parents, "
        "victimes (avec leur taux d'incapacité), veufs et veuves. Par tranche d'âge et par "
        "sexe, avec ce qui leur est payé. Tout est proposé, rien n'est validé par la CNSS ; la "
        "requête n'est pas exécutée (chiffres vus au 02/02/2026). Le détail est dans "
        "analyses/prestations/regles_t42_t49_nouveaux.md."
    ),
    "en_bref": [
        "Ces tableaux comptent les personnes qui touchent pour la première fois, dans l'année, "
        "une pension (T42 retraités, T43 veufs et veuves, T44 orphelins) ou une rente AT/MP "
        "(T45 toutes, T46 orphelins, T47 parents, T48 victimes, T49 veufs et veuves) : âge, "
        "sexe, montant ; mois de travail (T42) et taux d'incapacité (T48).",
        "Le recueil v7 demande : chaque tableau par mois, trimestre, année, et une période libre "
        "pour T43 à T49 ; les mois de travail sans l'autre caisse ; le taux d'incapacité (IPP) en moyenne. Sa phrase "
        "« Idem. Tab 41 » ferait compter le défunt : on la lit « même périodicité » (N4, Q1).",
        "L'annuaire CNSS 2024 tranche, faute de source plus forte : on compte à la saisie, même "
        "quand une autre caisse paie (coordination). Retraités 2 463 pour 2 489 (−1,0 %), veufs "
        "et veuves +0,7 %, orphelins +0,5 %. Il confirme : la première fois seulement, orphelins "
        "et veuves de rente exacts (44 et 4) ; âge par tranche de 5 ans juste de 55 à 69 ans.",
        "En écart : rentes de victimes 39 pour 44 (−11,4 %) ; parents de rente 15 pour 13 "
        "(+15,4 %) ; 641 femmes parmi les retraités saisis pour 527 (+21,6 %) ; rentes payées 12 à 25 % sous "
        "l'annuaire : montants de T45 à T49 trop bas.",
        "Décisions attendues : qui compte pour un survivant (Q1) ; saisie ou notification (Q2) ; "
        "le rappel dans le montant (Q3) ; où sont les rentes qui manquent (Q5) ; les 44 rentes "
        "de victimes (Q6, après un dernier test).",
        "Reste à faire : Claude, période libre (N1 à N3), colonne du rappel (N8), DR et agence "
        "(N5), lot 10 (une requête de contrôle) à rédiger ; toi, exécuter la requête et le "
        "lot 10, poser Q1, Q2, Q3, Q5.",
    ],
    # ------------------------------------------------------------------ 0. les tableaux
    "tableaux": [
        ["T42", "Nouveaux pensionnés de vieillesse : effectif, montant, total des services passés. "
                "Moins de 50 ans … 100 ans et plus ; hommes et femmes, sans colonne Total",
         "p. 15 et 42", "t42_t49_requete_proposee.sql (pas exécutée) ; fiche T42 ; lot 7"],
        ["T43", "Nouveaux pensionnés veufs et veuves : effectif et montant. Moins de 20 ans … "
                "60 ans et plus ; hommes, femmes, total", "p. 15 et 42", "même requête ; lot 7"],
        ["T44", "Nouveaux pensionnés orphelins. Moins de 5 ans … 20 ans et plus",
         "p. 15 et 43", "même requête ; lot 7"],
        ["T45", "Nouveaux crédirentiers AT/MP, toute la branche. Moins de 15 ans … 100 ans et "
                "plus (la première tranche suppose des orphelins)", "p. 15 et 43",
         "même requête ; lot 7"],
        ["T46", "Nouveaux crédirentiers orphelins (rentes AT/MP). Moins de 5 ans … 20 ans et plus",
         "p. 15 et 44", "même requête ; lot 7"],
        ["T47", "Nouveaux crédirentiers ascendants : père ou mère d'une victime décédée. Moins "
                "de 50 ans … 100 ans et plus", "p. 15 et 44", "même requête ; lot 7"],
        ["T48", "Nouveaux bénéficiaires de rente viagère (la victime) : effectif, montant, degré "
                "moyen d'IPP. Moins de 20 ans … 65 ans et plus ; sans colonne Total",
         "p. 15 et 45", "même requête ; lot 7"],
        ["T49", "Nouveaux crédirentiers veufs et veuves (rentes AT/MP). Moins de 20 ans … 65 ans "
                "et plus", "p. 16 et 45", "même requête ; lot 7"],
    ],
    # ------------------------------------------------------------------ 1. recueil v7
    "recueil_note": (
        "Toutes les lignes du recueil v7 qui touchent T42 à T49 (numéros de besoins_v7.txt). "
        "Aucun tableau n'est sans ligne, mais T46, T47 et T49 n'ont qu'un renvoi (« Idem. Tab "
        "45 ») et T42 n'a ni période libre ni « Idem. Tab 41 ». Ne changent rien ici : l. 72 "
        "(accès), l. 53 à 60 (fichiers Excel), l. 32 (alertes), virement, ratios CIPRES "
        "respectés ; l. 80 et 81 et satisfaction sont rangées dans T50-T57."
    ),
    "recueil": [
        ("#25 (T42), l. 296", "« Service passé : total des périodes d'activités salarié en "
         "nombre de mois »", "Le total des mois de travail des nouveaux retraités",
         "R9 ; fiche T42, règle 8", "pris en compte"),
        ("#25 (T42), l. 297", "« total services passés » à estimer en nombre de mois",
         "En mois, pas en années ; même phrase que Kaya 2017", "R9", "pris en compte"),
        ("#25 (T42), l. 298", "« Pour les pensions de coordination, prendre uniquement la "
         "partie CNSS »", "Ne dit pas si c'est pour les mois, le montant ou les deux",
         "R9 (mois) ; N6 (montant)", "question"),
        ("#25 (T42), l. 299", "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
         "T42 par mois, trimestre, année ; le recueil l'emporte sur « fin d'exercice »",
         "N1 ; requête annuelle", "à intégrer"),
        ("#11, #26, #27 (T45, T43, T44), l. 203, 305, 312 ; T46 à T49 par renvoi",
         "« Indiquer la période (début, fin) en paramètre »", "Une date de début, une date de "
         "fin choisies par l'utilisateur", "N1, N2, N3 ; debut et fin à rendre libres",
         "à intégrer"),
        ("#11 (T45), l. 202 ; T46 à T49 par renvoi", "« Nouveaux crédirentiers : rentiers »",
         "La victime ou sa famille qui touche une rente AT/MP : T45 = toute la branche", "R2",
         "pris en compte"),
        ("l. 204, 306, 313 et renvois ; T41, l. 153", "« Idem. Tab 41 » : périodicité "
         "mensuelle, trimestrielle, annuelle", "T43 à T49 par mois, trimestre, année", "N1",
         "à intégrer"),
        ("l. 204, 306, 313 et renvois ; T41, l. 152", "« Idem. Tab 41 » : « Allocataire : "
         "Titulaire du dossier (assuré) »", "À la lettre, on compterait le défunt à la place de "
         "sa famille", "N4 : l'ayant droit (R5) ; remarque B05 du recueil", "question"),
        ("#14 (T48), l. 222", "« Dégré IPP : moyenne des taux IPP »", "Somme des taux de la "
         "tranche ÷ nombre de nouveaux rentiers", "N7 ; quel taux si la rente change (Q15)",
         "pris en compte"),
        ("#14 (T48), l. 223", "« Chaque rentier possède un taux dans l'application »",
         "Le taux est toujours saisi", "Vérifié : de 15 à 100, jamais vide (P87)",
         "pris en compte"),
        ("#20 (T32), l. 263", "Coordination : « les montants concernent uniquement la partie "
         "CNSS »", "Écrit pour le stock ; vaut par analogie pour T43 et T44", "N6",
         "pris en compte"),
        ("#18, #19 (T25, T28), l. 248, 255", "« Ajouter une ligne « Pension de retraite "
         "anticipée » »", "À part dans T25 et T28 ; rien pour T42",
         "R2 : dans T42, proposition BI", "question"),
        ("Indicateurs, l. 1247 à 1252, 1256 à 1262", "Retraités = « pensions directes » : vieillesse, "
         "anticipée, invalidité", "Pour le rapport démographique, pas pour T42",
         "Pas repris pour T42 (R2)", "pris en compte"),
        ("#23 (T35), l. 283", "« Supprimer la tranche d'âge « 25 ans et plus » »",
         "Pour le stock ; 2 nouveaux orphelins de 25 à 29 ans en 2024", "orphelin_age_max "
         "(Q14)", "question"),
        ("#21 (T33), l. 270", "Un module pour convertir l'invalidité en pension normale à "
         "l'âge de la retraite", "Un développement de l'application", "Effet sur T42 : Q18",
         "hors BI"),
        ("#28 (T50), l. 319 et 320", "Services passés en mois ; annuel", "T50, les allocations "
         "de vieillesse : frontière de T42", "R2", "pris en compte"),
        ("#29 (T51), l. 326", "« Allocation de survivants : veufs/veuves, orphelins et "
         "ascendants »", "Versée une seule fois : frontière de T43 et T44", "R2",
         "pris en compte"),
        ("Indicateurs pensions, l. 1217 à 1238", "Délai de liquidation, du dossier complet à "
         "la notification des droits", "Mêmes premières pensions, datées à la notification",
         "Traité dans T25-T27 (même statut) ; contre-indice pour R7 (Q2)", "à intégrer"),
        ("Indicateurs AT/MP, l. 1170 à 1179", "Délai des rentes de survivants, jusqu'à la "
         "notification", "Même population que T45 à T49 : la première rente",
         "Traité dans T25-T27 (même statut : T25-T27 Q8, Q9) ; R7", "question"),
        ("Indicateurs, l. 1133", "Ordonnancé = « ayant fait l'objet d'une notification »",
         "Pour un ratio, pas pour un nouveau pensionné", "R7 garde la saisie",
         "pris en compte"),
        ("Taux de remplacement, l. 1256 à 1265", "Pension ÷ salaire moyen des 5 meilleures "
         "années × 100 ; pensions directes ; par DR, agence", "Naturel sur les nouveaux "
         "retraités, mais pas dit", "Pas fait ; PE_SAL_MOYEN à étudier", "question"),
        ("Termes de référence, l. 4 à 10", "Cinq directions régionales", "« Par DR » = ces "
         "cinq directions", "N5", "à intégrer"),
        ("Indicateurs, l. 1065 (et l. 1080 à 1265)", "« donner la possibilité de désagréger "
         "l'indicateur »", "Par DR et par agence", "N5 ; remarque B34", "à intégrer"),
        ("Attentes, l. 29", "Données « traquées jusqu'au niveau de granularité le plus "
         "élémentaires »", "Jusqu'à la liste des personnes", "Liste de R7 à rendre "
         "consultable", "à intégrer"),
        ("Caractéristiques, l. 51 et 68", "Analyses « au niveau transactionnel »",
         "Une ligne par personne", "Liste de R7", "à intégrer"),
        ("Attentes, l. 31 et 74", "Présentation multidimensionnelle", "Mêmes axes : tableau, "
         "âge, sexe, mois, DR, agence", "N1, N5", "à intégrer"),
        ("Attentes, l. 35", "Rapports « à des fréquences adaptées »", "Chaque tableau à sa "
         "fréquence", "N1", "à intégrer"),
        ("Attentes, l. 28, 34, 75", "Peu d'intervention manuelle", "Les chiffres sortent d'une "
         "requête", "t42_t49_requete_proposee.sql", "pris en compte"),
        ("Caractéristiques, l. 58", "Règles de transcodage écrites", "Codes et grilles "
         "d'âge écrits", "R2, R8", "pris en compte"),
        ("Attentes, l. 24 et 78", "Profilage de la qualité", "Ex. : 1900, date de naissance "
         "par défaut", "R8 (âge inconnu) ; lot 7", "pris en compte"),
        ("Général, l. 15 et 92 à 94", "La source : l'application des prestations",
         "Copie USER_DWH au 02/02/2026", "Toutes les règles", "pris en compte"),
    ],
    # ------------------------------------------------------------------ 2. règles
    "regles": [
        {"id": "R2", "titre": "Le périmètre de chaque tableau",
         "regle": "T42 RPV (+ RPRA, proposé) ; T43 RPSV, RPSF ; T44 RPSE ; T45 toutes les rentes "
                  "AT/MP ; T46 ARSE ; T47 ARSA ; T48 ARI ; T49 ARSV, ARSF. Hors de ces "
                  "tableaux : allocations versées une fois (T50, T51), AAI (T53), RPI, RPA.",
         "exemple": "une veuve qui reçoit une allocation de survivants ne compte pas dans T43 "
                    "(elle va dans T51).",
         "sources": [(CIPRES, "p. 15, 16, 42 à 45, 62, 65"), (RV7, "l. 202 ; l. 326"),
                     (KAYA, "T45 à T49 avec les AT/MP"), (CNSS, "tableaux 24, 28, 77")],
         "etat": "proposée ; T45 appuyé par le recueil, pas par l'annuaire ; T47 en écart (Q7) ; "
                 "anticipée : Q13"},
        {"id": "R5", "titre": "La personne comptée",
         "regle": "L'ayant droit (IND_ID_PREST), une fois par tableau : la veuve, l'orphelin "
                  "lui-même (pas son tuteur), le parent, la victime. On n'additionne jamais "
                  "deux tableaux.",
         "exemple": "orphelin de 8 ans payé à sa mère : T44, « 5 – 9 ans », colonne hommes.",
         "sources": [(CIPRES, "p. 65 ; grilles de T44, T46"),
                     (CNSS, "tableau 20 ; tableau 77"), (DON, "P16, P82, P77")],
         "etat": "confirmée par l'annuaire CNSS 2024 ; voir N4 et Q1"},
        {"id": "N4", "titre": "Les survivants malgré « Idem. Tab 41 »",
         "regle": "Pour T43 à T49, « Idem. Tab 41 » = même périodicité. La définition de "
                  "l'allocataire (titulaire du dossier) ne change pas la personne comptée.",
         "exemple": "trois orphelins d'un ouvrier mort au travail : trois nouveaux de T46 ; "
                    "l'ouvrier n'est pas compté.",
         "sources": [(CIPRES, "p. 62 : allocataire « en matière de PF »"),
                     (KAYA, "T41, l. 120"), (RV7, "l. 202"), (CNSS, "tableau 77"),
                     (DON, "P51 : moitié des veuves dans le dossier du mari")],
         "etat": "nouvelle, à confirmer (Q1), une seule fois pour T32 à T49"},
        {"id": "R7", "titre": "Le nouveau bénéficiaire",
         "regle": "Toute première prestation du tableau saisie dans la période (PE_DATE_INSERT). "
                  "Une révision n'est pas une entrée. Sortis avant le 31/12 et coordination "
                  "payée ailleurs comptés.",
         "exemple": "saisie le 18/12/2024, notifiée le 10/01/2025 : nouvelle de 2024.",
         "sources": [(CIPRES, "p. 15 : loi des entrées"),
                     (CNSS, "tableau 23 : 4 090 saisies, 4 037 publiés ; 75 et 77"),
                     (DON, "P82, A03")],
         "etat": "proposée ; pratique de l'annuaire ; contre-indice de la feuille 03 (Q2)"},
        {"id": "N1", "titre": "Période choisie et périodicité",
         "regle": "Un mois, un trimestre, une année ou deux dates libres. Nouveau = première "
                  "prestation du tableau saisie entre les deux dates. Effectifs additifs, "
                  "montants non.",
         "exemple": "saisie le 15/03/2025 : mars, 1er trimestre et année 2025, jamais un autre "
                    "mois.",
         "sources": [(RV7, "l. 299 ; l. 203, 305, 312 ; l. 153"), (CIPRES, "p. 15 : « fin "
                     "d'exercice », plus faible")],
         "etat": "nouvelle, demandée par le recueil ; à intégrer"},
        {"id": "R8", "titre": "L'âge et le sexe",
         "regle": "Âge = année de l'exercice − année de naissance, dans la grille du tableau ; "
                  "négatif, vide ou 120 ans et plus : âge inconnu. Sexe de la fiche (1 homme, "
                  "2 femme).",
         "exemple": "né en 1965, retraité en 2025 : 60 ans, « 60 – 64 ans ».",
         "sources": [(CIPRES, "p. 62 ; p. 42 à 45"), (KAYA, "l. 203, 213 : saisie correcte"),
                     (CNSS, "tableau 25"), (DON, "P83, P84")],
         "etat": "par tranche, confirmée de 55 à 69 ans (1,6 % au plus) ; âge par âge et sexe : "
                 "Q8, Q9"},
        {"id": "N2", "titre": "L'âge sur une période choisie",
         "regle": "Âge = année de l'entrée (la saisie) − année de naissance. Sur une année "
                  "civile, c'est R8.",
         "exemple": "période de juillet 2024 à juin 2025, entrée en octobre 2024 : âge de 2024.",
         "sources": [(CIPRES, "p. 62 : année de constatation"), (RV7, "période libre")],
         "etat": "nouvelle, proposition BI (Q16)"},
        {"id": "R3", "titre": "Le montant",
         "regle": "Ce que la CNSS a payé : débours PM (paiement) au statut D ou P, pour les "
                  "prestations du tableau saisies dans la période. COOR_AC sans montant. Ni "
                  "accessoires, ni rachat de rente.",
         "exemple": "personnes saisies dans T42 en 2024 : 2,86 Md ; 321 sans paiement "
                    "(révisions comprises).",
         "sources": [(CIPRES, "p. 15 : montants servis"), (CNSS, "tableaux 19, 28 ; 90"),
                     (DON, "P85, P98, P104")],
         "etat": "pensions à 0,7 à 2,5 % de l'annuaire ; rentes 12 à 25 % en dessous (Q5)"},
        {"id": "A1", "titre": "Les accessoires de pension",
         "regle": "Majoration pour enfant et charges du ménage : pas un pensionné de plus, et "
                  "hors du montant de T42.",
         "exemple": "un nouveau retraité avec bonification pour enfant compte une fois.",
         "sources": [(CNSS, "tableaux 19 et 28 : majoration à part"), (DON, "P34, P98")],
         "etat": "prouvée pour les personnes ; montant : précision nouvelle, à valider"},
        {"id": "R4", "titre": "La date d'un paiement",
         "regle": "Réglage date_montant. 'MOIS' (retenu) : paiements des mois de la période. "
                  "'SAISIE' : paiements saisis dans la période, rappel et janvier suivant "
                  "compris.",
         "exemple": "T42 2024 : 2,86 Md ('MOIS') ou 3,22 Md ('SAISIE').",
         "sources": [(CNSS, "profil mensuel des tableaux 28, 41, 90 : la saisie"),
                     (DON, "P85, P98, P105")],
         "etat": "en attente de la CNSS (point du 02/11 ; Q3) ; l'annuaire penche vers "
                 "'SAISIE'"},
        {"id": "N8", "titre": "Le rappel, montré à part",
         "regle": "Une colonne « rappel » : ce qui est payé aux nouveaux pour des mois d'avant "
                  "la période. La requête ne la sort pas aujourd'hui.",
         "exemple": "saisi en février 2024, dû depuis octobre 2023 : octobre à décembre 2023 "
                    "en rappel.",
         "sources": [(DON, "P85 : 8 % (T42), 25 % (T43), 39 % (T44), 198 % (T46)"),
                     (BI, "fiche T42, règle 7")],
         "etat": "nouvelle, proposition BI ; à ajouter"},
        {"id": "N3", "titre": "Le montant sur une période choisie",
         "regle": "Ce qui est payé pour les mois de la période. Les montants des mois ne "
                  "s'additionnent pas pour faire l'année.",
         "exemple": "100 000 FCFA par mois dès mars 2025 : 100 000 en mars, 1 000 000 sur "
                    "l'année.",
         "sources": [(CIPRES, "p. 15"), (RV7, "l. 299 ; l. 159 « montant liquidé » (T54)")],
         "etat": "nouvelle, proposition BI (Q4)"},
        {"id": "N6", "titre": "Le montant de la coordination",
         "regle": "Pour T42 à T44, la part CNSS = ce que la CNSS a payé : zéro quand l'autre "
                  "caisse paie (COOR_AC).",
         "exemple": "retraité payé par la caisse d'un pays voisin : dans T42, montant nul.",
         "sources": [(RV7, "l. 298 (T42) ; l. 263 par analogie"), (KAYA, "l. 292, 349"),
                     (BI, "décision du 31/10 pour T32 à T36, étendue")],
         "etat": "proposée ; Q11 (mois, montant ou les deux), Q12 (COOR_CNSS)"},
        {"id": "R9", "titre": "Les services passés (T42)",
         "regle": "Total des mois cotisés (PE_MOIS_COTISATION) de la dernière pension, de 1 à "
                  "600. Coordination : moins les mois de l'autre caisse (PE_COT_AUTRE).",
         "exemple": "300 mois dont 120 dans l'autre caisse : 180 mois.",
         "sources": [(RV7, "l. 296 à 298"), (KAYA, "l. 348, 349"), (CIPRES, "p. 66"),
                     (DON, "P86, P89, P90")],
         "etat": "proposée ; PE_COT_AUTRE plus petit en moyenne seulement ; à limiter à la "
                 "coordination ; Q10"},
        {"id": "N7", "titre": "Le degré moyen d'IPP (T48)",
         "regle": "Somme des taux d'incapacité (PE_TAUX_INCAPACITE) ÷ nombre de nouveaux "
                  "rentiers, par tranche et par sexe ; taux de la dernière rente de la période.",
         "exemple": "20 %, 30 % et 40 % : 30 %.",
         "sources": [(RV7, "l. 222, 223"), (CIPRES, "p. 15, 45"), (DON, "P87 : 15 à 100")],
         "etat": "moyenne du recueil ; « dernière rente » = choix BI (Q15) ; au plus 1 cas en "
                 "2024"},
        {"id": "N5", "titre": "Direction régionale et agence",
         "regle": "DR du dossier ; agence = service du dossier (SP_NO), comme T25-T27 N2. "
                  "Découpage des huit tableaux.",
         "exemple": "tableau 23 : 1 760 des 2 486 nouveaux retraités de 2024 à la DRO (70,8 %).",
         "sources": [(RV7, "l. 4 à 10, 29, 1065"), (CNSS, "tableaux 23, 75, 77, 22, 34, 96"),
                     (DON, "A02, P46")],
         "etat": "nouvelle, proposition BI ; à tester (lot 10)"},
        {"id": "R13", "titre": "Le cliché du calcul",
         "regle": "L'exercice N sur la copie de fin mars N+1. Touche le montant, presque pas "
                  "l'effectif.",
         "exemple": "5 % du montant 2024 de T42 saisi en 2025 ou après.",
         "sources": [(DON, "P99 : 444 décès pour 447 publiés ; P85")],
         "etat": "proposée ; le cliché 092026 porte la copie du 02/02/2026"},
    ],
    # ------------------------------------------------------------------ 3. cas
    "cas_note": (
        "Les 12 cas les plus parlants (19 dans le md, mêmes numéros). Chaque carte dit ce que "
        "devient la personne, et selon quelle règle."
    ),
    "cas": [
        ("Cas 1. Une veuve dans le dossier du mari", "Pension de veuve saisie en mars 2024 dans "
         "le dossier de son mari retraité décédé.", "COMPTÉE : T43 2024, à son âge (R7, R5)",
         "compte"),
        ("Cas 2. Un orphelin payé à sa mère", "Garçon de 8 ans ; la pension va à sa mère, sa "
         "tutrice.", "COMPTÉ : T44, « 5 – 9 ans », hommes ; la mère non (R5)", "compte"),
        ("Cas 3. Trois orphelins d'un ouvrier", "Ouvrier mort d'un accident du travail ; "
         "rentes des orphelins saisies en 2024.", "COMPTÉS : trois nouveaux de T46 et T45 ; "
         "l'ouvrier non (N4, Q1)", "compte"),
        ("Cas 4. Saisie en décembre, notifiée en janvier", "Saisie le 18/12/2024, notifiée le "
         "10/01/2025.", "QUESTION Q2 : T42 2024 à la saisie ; 2025 à la notification (R7)",
         "question"),
        ("Cas 5. Un rappel de 2023", "Effet au 01/10/2023, saisie en février 2024, rappel "
         "d'octobre 2023 à janvier 2024.", "RÉGLAGE date_montant : rappel à part avec 'MOIS' "
         "(R4, N8, Q3)", "reglage"),
        ("Cas 6. Un retraité de 2018 recalculé", "Une nouvelle pension est saisie en 2025 pour "
         "corriger l'ancienne. 220 cas en 2025.", "PAS COMPTÉ : ce n'est pas une entrée (R7, "
         "Q17)", "pas compte"),
        ("Cas 8. Coordination payée ailleurs", "Cotisé au Burkina et dans un pays voisin ; "
         "l'autre caisse paie (COOR_AC). 234 cas en 2024.", "COMPTÉ SANS MONTANT : mois CNSS "
         "seuls (N6, R9)", "compte"),
        ("Cas 10. Une allocation de survivants", "Une veuve reçoit une allocation versée une "
         "seule fois (RASC).", "PAS COMPTÉE dans T43 : elle va dans T51 (R2)", "pas compte"),
        ("Cas 11. Une retraite anticipée", "Pension RPRA saisie en 2024, à 53 ans. L'annuaire "
         "la montre à part (3 en 2024).", "QUESTION Q13 : dans T42, « 50 – 54 ans » (R2)",
         "question"),
        ("Cas 13. Une rente recalculée la même année", "Rente viagère saisie en mars 2024 à "
         "20 %, recalculée en octobre à 30 %.", "RÉGLAGE taux_ipp : une fois dans T48, à 30 % "
         "(N7, Q15)", "reglage"),
        ("Cas 16. Un orphelin de 26 ans", "Pension d'orphelin saisie en 2024 ; 2 cas. Au 31/12, "
         "pas d'orphelin de 25 ans ou plus (R6).", "À SURVEILLER : « 20 ans et plus » de T44 "
         "(Q14)", "attention"),
        ("Cas 17. Orphelin de rente, puis victime", "Rente d'orphelin en 2015 ; accident du "
         "travail et rente viagère en 2024.", "À SURVEILLER : nouveau dans T48, pas dans T45 "
         "(R7)", "attention"),
    ],
    # ------------------------------------------------------------------ 4. annuaire
    "annuaire_note": (
        "L'annuaire publie les nouveaux par type et par DR (tableau 23), l'âge et le sexe des "
        "nouveaux retraités (25) et des nouvelles rentes viagères (75), les nouveaux ayants "
        "droit de rentes (77). Aucun montant de nouveaux, ni services passés, ni IPP. Il tranche "
        "la date d'entrée et la « première fois », que les sources plus fortes ne disent pas."
    ),
    "annuaire": [
        ("Tableau 23 (p. 19)", "Nouveaux pensionnés 2024 : la date d'entrée", "Saisie : 2 463, "
         "771, 773 ; notifiées 3 888 (A03)", "2 489, 766, 769 ; 4 037", "−1,0 % ; +0,7 % ; "
         "+0,5 % ; notification −3,7 %", "tranche", "La saisie (R7) ; Q2 pour confirmer"),
        ("Tableau 23, vieillesse", "Coordination payée ailleurs", "2 463 ; sans COOR_AC 2 229",
         "2 489", "−1,0 % ; −10,4 %", "tranche", "On garde la coordination (R7, N6)"),
        ("Tableau 23, vieillesse", "Sortis avant le 31/12 ; révisions", "sans sortis 2 428 ; "
         "avec révisions 2 510", "2 489", "−2,5 % ; +0,8 %", "precise", "Ne départage pas ; "
         "sortis gardés (une entrée reste une entrée)"),
        ("Tableau 25 (p. 20)", "Âges des nouveaux retraités, par tranche", "1 450 ; 941 ; 89 "
         "(révisions comprises)", "1 429 ; 956 ; 89", "+1,5 % ; −1,6 % ; 0", "confirme",
         "R8 par tranche ; âge par âge non (Q9)"),
        ("Tableau 25 (p. 20)", "Sexe des nouveaux retraités", "641 femmes", "527 femmes",
         "+21,6 %", "ecart", "Propre au tableau 25 ; sexe de la fiche gardé (Q8)"),
        ("Tableau 75 (p. 60)", "Nouvelles rentes viagères (T48)", "39 ; notifiées 57 ; "
         "saisies 68", "44", "−11,4 % ; +29,5 % ; +54,5 %", "ecart", "Deux variantes à tester "
         "(lot 10), puis Q6"),
        ("Tableau 77 (p. 61)", "Orphelins (T46), veuves et veufs (T49) de rente", "44 ; 4",
         "44 ; 4", "0 ; 0", "confirme", "R2, R5, R7"),
        ("Tableau 77 (p. 61)", "Ascendants de rente (T47)", "15", "13", "+15,4 %", "ecart",
         "La date n'y change rien (Q7)"),
        ("Tableau 77, lecture du recueil", "La personne comptée face à « Idem. Tab 41 »",
         "ayants droit", "« selon la nature du bénéficiaire »", "pas chiffrable", "precise",
         "La CNSS compte le survivant ; la décision reste à elle (N4, Q1)"),
        ("Tableaux 75 et 77", "Nouveaux rentiers (T45) et recalculs", "102 ; avec recalculs "
         "131", "105 (notre somme)", "−2,9 % ; +24,8 %", "confirme", "La première fois (R7), "
         "pas le périmètre de T45"),
        ("Tableaux 18, 23, 26", "Stock + entrées − décès", "non calculé", "veufs, invalidité "
         "justes", "vieillesse −351 ; orphelins +24 ; ascendants +87", "non comparable",
         "Ne prouve rien : faux pour orphelins et ascendants (Q20)"),
        ("Tableau 20 (p. 17)", "Orphelins de pension au 31/12 par âge", "—", "9 478 de moins "
         "de 25 ans, dont 3 812 de 20 à 24", "—", "confirme", "R5 et R6 ; des sorties à 25 ans "
         "chaque année (Q20)"),
        ("Tableaux 80, 81, 75, 77", "Stock des rentes + entrées − décès", "T46 : 44",
         "orphelins 379 puis 485", "orphelins +62", "ecart", "Sans effet sur T46 ; à suivre "
         "pour T39 (Q20)"),
        ("Tableaux 19 et 28", "Montant d'un pensionné", "38 496 066 498", "38 235 880 033",
         "+0,7 %", "precise", "Pension seule, majoration à part (A1)"),
        ("Tableau 90 (p. 71)", "Montants 2024 des rentes", "399,4 M ; 275,9 M (mois payé)",
         "455,0 M ; 368,6 M", "−12,2 % ; −25,1 %", "ecart", "Montants de T45 à T49 trop bas "
         "(R3, Q5) ; rachat hors montant"),
        ("Tableaux 96 et 97", "Rentes par DR ; de 2020 à 2024", "pas calculé", "DRO 285,5 M "
         "(viagères)", "—", "precise", "Contrôles futurs de N5 et du montant"),
        ("Tableaux 28, 41, 90 (mois)", "Date des paiements publiés", "corrélation 0,75 et "
         "0,84 à la saisie", "pics trimestriels", "—", "precise", "Appuie 'SAISIE' (R4, Q3)"),
        ("Tableau 24 (p. 19)", "Allocations de vieillesse 2024", "831 notifiées ; 873 saisies",
         "833", "−0,2 % ; +4,8 %", "confirme", "Hors T42 (T50) ; comptées à la notification"),
        ("Tableau 28 (p. 22)", "Allocations de survivants 2024", "hors T43, T44", "197 906 991 "
         "FCFA à part", "—", "confirme", "Hors T43 et T44 (T51) (R2)"),
        ("Tableau 27 (p. 21)", "« Départs à la retraite »", "pas comparable", "3 729",
         "+12,3 % face aux tableaux 23 et 24", "non comparable", "Ce n'est pas T42"),
        ("Tableau 76 (p. 60)", "Stock des rentes viagères par âge", "pas calculé", "1 255",
         "—", "precise", "Libellés « 20 – 24 ans » ; contrôle par âge à venir"),
        ("Tableau 49 (p. 39)", "Accidents mortels 2020 à 2024", "—", "13, 11, 13, 11, 14",
         "—", "non comparable", "Ordre de grandeur des rentes de survivants"),
        ("Tableaux 22 et 34", "Stock et montants par DR", "A02", "62 231", "0 à 4 %",
         "precise", "DR du dossier plausible ; à tester sur les nouveaux (N5)"),
        ("Tableaux 23, 25, 75, 77", "Montants, services passés, IPP ; âge de T43 à T47, T49",
         "—", "rien", "—", "non publie", "Ne se valident qu'avec la CNSS"),
    ],
    "annuaire_apres": (
        "Confirme ou tranche : la saisie (R7), la première fois, la coordination comptée, "
        "l'orphelin à son âge, T46 et T49 exacts, les allocations hors des tableaux, l'âge par "
        "tranche. Précise : la personne comptée (N4), le montant sans accessoire (A1), la date "
        "des paiements (R4). Contredit : rien de nos règles. À expliquer : rentes viagères, "
        "ascendants, sexe, montants des rentes, stocks d'orphelins. Pas publié : montants, "
        "services passés, IPP."
    ),
    # ------------------------------------------------------------------ 5. réglages
    "reglages_note": (
        "Dans la CTE parametres de t42_t49_requete_proposee.sql. « À créer » : le choix est écrit "
        "en dur, ou n'existe pas encore. Une décision attendue de la CNSS ne bloque rien : la "
        "valeur retenue s'applique en attendant."
    ),
    "reglages": [
        ("date_montant", "t42_t49", "'MOIS'", "'SAISIE'", "T42 2024 : 2,86 Md ou 3,22 Md ; "
         "effectifs inchangés", "DCP (Q3 ; points_a_valider n° 1, 45)"),
        ("debut et fin", "t42_t49 (tirés de l'exercice)", "1er janvier, 31 décembre",
         "deux dates libres", "Qui est nouveau, quels paiements comptent (N1, N3)",
         "tranché par le recueil ; l'utilisateur choisit"),
        ("annee", "t42_t49", "l'exercice", "l'année de l'entrée (N2)", "Même âge sur une "
         "année civile", "DPC, avec la DCP (Q16)"),
        ("date_entree (à créer)", "t42_t49 (en dur : saisie)", "'SAISIE'", "'NOTIFICATION'",
         "T42 : 2 397 au lieu de 2 463, −66 (−2,7 %)", "DCP (Q2 ; points_a_valider n° 4)"),
        ("retraite_anticipee (à créer)", "t42_t49 (en dur : dans T42)", "DANS_T42", "HORS_T42",
         "Environ 4 retraités de moins par an", "DCP (Q13 ; n° 18, 34)"),
        ("orphelin_age_max (à créer)", "t42_t49", "aucune limite", "25 (comme R6)",
         "T44 : 2 orphelins de moins en 2024", "DCP (Q14 ; n° 10)"),
        ("taux_ipp (à créer)", "t42_t49 (en dur : dernière rente)", "DERNIER", "ENTREE",
         "Au plus 1 personne en 2024", "DP et DR (Q15)"),
        ("agence (à créer)", "t42_t49 (N5)", "SERVICE", "LIEU_PAIEMENT", "Le découpage par "
         "agence", "DSI (T25-T27 Q20)"),
        ("cliché (au lancement, hors parametres)", "t42_t49", "092026 (copie du 02/02/2026)",
         "fin mars N+1 (R13)", "Le montant surtout (5 % saisi après)",
         "BI, avec la DSI (n° 2)"),
    ],
    # ------------------------------------------------------------------ 6. questions
    "questions_note": (
        "Une question par point. Chacune a une valeur retenue en attendant : la réponse change un "
        "réglage ou une règle, pas le travail fait. DCP (Direction centrale des prestations) pour "
        "les pensions ; DP et directions régionales (DR) pour les AT/MP (DP : sigle à faire "
        "préciser) ; DES : sigle du recueil ; DSI : systèmes d'information ; DPC : prospective, "
        "fait l'annuaire."
    ),
    "questions": [
        ("Q1", "Pour un orphelin, une veuve ou un parent, compte-t-on l'ayant droit lui-même, "
               "et non l'assuré décédé titulaire du dossier ?", "DCP, avec les auteurs du "
               "recueil", "l'ayant droit ; « Idem. Tab 41 » = même périodicité",
         "Nouvelle ; une seule fois pour T32 à T49 ; remarque B05 du recueil"),
        ("Q2", "Un nouveau compte-t-il l'année où sa prestation est saisie, ou l'année où elle "
               "est notifiée ?", "DCP", "la saisie, comme l'annuaire",
         "Déjà posée : points_a_valider n° 4 ; fiche T42 Q1"),
        ("Q3", "Le montant d'un nouveau comprend-il le rappel des mois d'avant l'année ?",
         "DCP", "les mois de l'année ; le rappel à part (N8)",
         "Déjà posée : n° 1 et 45 ; fiche T42 Q4 ; point du 02/11"),
        ("Q4", "Dans un tableau mensuel, quel montant : le payé de la période, ou le montant "
               "mensuel de la pension ?", "DCP", "le payé de la période (N3)", "Nouvelle"),
        ("Q5", "Où sont les paiements de rentes qui manquent dans la base (12 à 25 % en "
               "2024) ?", "DSI, avec la DP", "ce que la base contient, mention « incomplet »",
         "Déjà posée : README (DSI) ; même question que T32-T41 Q21"),
        ("Q6", "Comment comptez-vous vos 44 nouvelles rentes viagères de 2024 ?",
         "DP et DR, avec la DPC", "la première rente (39)",
         "Déjà posée : points_a_valider n° 48 ; à reposer après le lot 10"),
        ("Q7", "Votre tableau 77 compte 13 nouveaux ascendants de rente, la base 15 : comment "
               "les comptez-vous ?", "DP et DR, avec la DPC", "les 15 comptés", "Nouvelle"),
        ("Q8", "Comment votre tableau 25 obtient-il le sexe des nouveaux retraités (527 femmes, "
               "641 dans la base) ?", "DPC (Service études et statistique)",
         "le sexe de la fiche", "Déjà posée : n° 44 ; fiche T42 Q3"),
        ("Q9", "À quelle date calculez-vous l'âge des nouveaux retraités du tableau 25 ?",
         "DPC (Service études et statistique)", "année de l'exercice − année de naissance",
         "Déjà posée : n° 44 ; fiche T42 Q3 ; même question que T32-T41 Q10"),
        ("Q10", "PE_COT_AUTRE donne-t-il les mois cotisés dans l'autre caisse, compris dans le "
                "total ?", "DCP, à défaut la DSI", "total − PE_COT_AUTRE (coordination)",
         "Déjà posée : n° 11 ; fiche T42 Q5"),
        ("Q11", "Pour T42, la « partie CNSS » vaut-elle pour les mois, le montant, ou les "
                "deux ?", "DCP", "les deux (R9, N6)", "Nouvelle"),
        ("Q12", "Quand la CNSS paie une pension de coordination, son paiement comprend-il la "
                "part de l'autre caisse ?", "DCP", "ce que la CNSS a payé", "Nouvelle"),
        ("Q13", "La retraite anticipée compte-t-elle dans T42, avec la vieillesse ?", "DCP",
         "oui (DANS_T42)", "Déjà posée : n° 18 et 34 ; fiche T32 Q2"),
        ("Q14", "Un orphelin de 25 ans ou plus peut-il recevoir une nouvelle pension "
                "d'orphelin ?", "DCP", "compté dans « 20 ans et plus »",
         "Liée à points_a_valider n° 10"),
        ("Q15", "Degré d'IPP de T48 : taux à l'entrée, ou dernier taux de l'année ?",
         "DP et DR", "le dernier taux (DERNIER)", "Nouvelle"),
        ("Q16", "Sur une période à cheval sur deux années, l'âge se compte-t-il avec l'année de "
                "l'entrée ?", "DPC, avec la DCP", "année de la saisie (N2)", "Nouvelle"),
        ("Q17", "Pourquoi 220 retraités qui avaient une pension en ont-ils reçu une nouvelle en "
                "2025 ?", "DCP, avec la DSI", "pas comptés comme nouveaux (R7)",
         "Déjà posée sous une autre forme : fiche T42 Q2"),
        ("Q18", "Une pension d'invalidité convertie à l'âge de la retraite fera-t-elle un "
                "nouveau retraité de T42 ?", "DCP et DSI", "sans objet : le module n'existe "
                "pas", "Nouvelle (remarque B31 du recueil)"),
        ("Q19", "Le taux de remplacement se calcule-t-il sur les nouveaux retraités de "
                "l'année ?", "DCP, avec la DES", "pas calculé",
         "Même question que T32-T41 Q18 (remarque B37 du recueil)"),
        ("Q20", "Comment calculez-vous le stock d'orphelins de 2024 (+791 pensions, +106 "
                "rentes) ?", "DPC (Service études et statistique)", "sans effet sur T44, T46",
         "Nouvelle"),
        ("Q21", "Pour T43, garder « 60 ans et plus » ou détailler (60 – 64, 65 – 69…) ?",
         "DCP, avec la DPC", "la maquette telle quelle", "Nouvelle (facultative)"),
    ],
    # ------------------------------------------------------------------ 7. points d'attention
    "attention_note": "Les 18 points du md, regroupés en 9.",
    "attention": [
        ("Qui tranche quoi", "Le recueil : période, périodicité, part CNSS, IPP, « crédirentier "
         "= rentier ». L'annuaire CNSS : la saisie, la première fois, la coordination."),
        ("Effectif et montant", "Une personne n'entre qu'une fois : les mois s'additionnent pour "
         "l'effectif, pas pour le montant. 'SAISIE' ajoute aussi la pension de janvier suivant."),
        ("Deux dates à la CNSS", "La saisie pour les pensions (tableau 23), la notification pour "
         "les allocations de vieillesse (tableau 24)."),
        ("Les montants des rentes", "12 à 25 % sous l'annuaire : T45 à T49 trop bas (Q5). Le "
         "rachat de rente, payé par ajustement, reste hors du montant."),
        ("T45 n'est pas toujours la somme", "T45 = T46 + T47 + T48 + T49 si personne n'entre "
         "dans deux tableaux et si personne n'avait déjà une rente. Vrai en 2024 : 102."),
        ("Révisions comprises", "P83 à P86 portent sur 2 510 personnes saisies, pas sur les 2 463 "
         "nouveaux : âge, sexe, montant et mois des premières fois à recalculer (lot 10)."),
        ("Coordination", "5 COOR_CNSS sans PE_COT_AUTRE ; la requête le retire aussi dans 3 "
         "dossiers hors coordination (à corriger)."),
        ("Les grilles", "T42 et T48 sans colonne Total (à masquer) ; T43 : 53 % dans « 60 ans "
         "et plus » (Q21). RPI et RPA n'ont pas de tableau des nouveaux."),
        ("Les documents existants", "TODO, prestation_esp, regles_prestations : « nouveau = "
         "notifié », périmé. Fiche T42 : « 4 % » (c'est −2,7 %), rappel « à part » (pas dans la "
         "requête)."),
    ],
    # ------------------------------------------------------------------ 8. reste et sources
    "reste": [
        ("Rendre debut et fin libres ; âge sur l'année de l'entrée (N1, N2, N3)",
         "Claude, après accord", "à faire"),
        ("Ajouter la colonne du rappel (N8)", "Claude", "à faire"),
        ("Ne retirer PE_COT_AUTRE que pour la coordination (R9)", "Claude", "à faire"),
        ("Ajouter les réglages « à créer » dans la CTE parametres", "Claude", "après visa"),
        ("DR du dossier et agence comme découpage (N5), après le lot 10 (b)", "Claude",
         "à faire"),
        ("Liste des nouveaux consultable, accès par profil (l. 29, 51, 68)", "Claude",
         "à faire"),
        ("Exécuter t42_t49 et retrouver le lot 7 (2 463, 771, 773, 102, 44, 15, 39, 4)",
         "toi, puis Claude", "à faire"),
        ("Lot 10 (P107 à P114) : rentes viagères, DR, âge à la date d'effet, premières fois, mois "
         "nets, PE_SAL_MOYEN, première notifiée, 15 ascendants", "Claude rédige, toi exécutes",
         "à faire"),
        ("Poser Q1 à Q21 (d'abord Q1, Q2, Q3, Q5 ; Q6 après le lot 10)", "toi, avec la CNSS",
         "à poser"),
        ("Reporter ces règles dans README, fiche T42, fiche de lecture, regles_prestations, "
         "points_a_valider ; corriger les textes périmés", "Claude, sur demande",
         "non commencé"),
    ],
    "sources": (
        "Sources : recueil des besoins v7 (besoins_v7.txt) ; atelier de Kaya 2017, tableau 1 ; "
        "Annuaire CIPRES p. 14 à 16, 42 à 45, 62, 65, 66 ; annuaire statistique de la CNSS 2024 "
        "(tableaux 18 à 28, 34, 49, 75 à 81, 90, 96, 97) ; classeur « Evaluation des Ratios "
        "CIPRES 2025 », feuille 03 ; lots 3 B, 7, 8, 9 et comparaisons A01 à A03 (copie de la "
        "base au 02/02/2026). Dossier de la famille et sa contre-vérification (style_grh/travail). "
        "Détail : analyses/prestations/regles_t42_t49_nouveaux.md."
    ),
}


def build():
    d = construire(DATA)
    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
