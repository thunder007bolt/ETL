"""
Schéma « T32 à T41 : les bénéficiaires au 31 décembre, les règles en clair » (état au 08/11/2026).

Famille F3 des prestations « à l'image du GRH » : ce que demande le recueil des besoins v7, les
règles (R2, R3, R4, R5, R6, R6 pour T41, R8, R13, A1 repris, N1 à N13 nouvelles), les cas, la
comparaison avec l'annuaire statistique de la CNSS 2024, les réglages de la CTE parametres, les
questions ouvertes (Q1 à Q23), les points d'attention et ce qui reste à faire.

Le détail est dans analyses/prestations/regles_t32_t41_beneficiaires.md : mêmes identifiants,
mêmes statuts, mêmes verdicts. Le travail existant (README, requête t32_t41, fiches T32 et T41,
fiche de lecture, regles_prestations, points_a_valider) n'est pas modifié.

Usage :
    python3 excalidraw/sources/prestations/famille_t32_t41_beneficiaires.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import main  # noqa: E402
from gabarit_famille import construire  # noqa: E402

OUTPUT = (ROOT / "schemas" / "prestations" / "T32 T41 beneficiaires"
          / "regles_t32_t41_beneficiaires.excalidraw")

RV7 = "Recueil v7"
REU = "Réunion"
KAYA = "Kaya 2017"
CIPRES = "Annuaire CIPRES"
CNSS = "Annuaire CNSS 2024"
MOD = "Modèles CNSS"
DON = "Données"
BI = "Proposition BI"

DATA = {
    "seed": 3241,
    "titre": "T32 à T41 : les bénéficiaires au 31 décembre (pensionnés, rentiers, allocataires)",
    "sous_titre": (
        "Les règles en clair, état au 08/11/2026. Les personnes qui touchent une prestation le "
        "31 décembre, par tranche d'âge et par sexe, avec le montant payé dans l'année : T32 à T36 "
        "pensionnés (vieillesse, invalidité, veuves et veufs, orphelins, ascendants), T37 à T40 "
        "rentiers des accidents du travail, T41 allocataires des prestations familiales ; et deux "
        "indicateurs : rapport démographique, taux de remplacement. Tout est proposé, rien n'est "
        "validé par la CNSS ; la requête n'est pas exécutée (chiffres des lots, vus au "
        "02/02/2026). Le détail est dans analyses/prestations/regles_t32_t41_beneficiaires.md."
    ),
    "en_bref": [
        "Ces tableaux comptent les personnes qui touchent une pension, une rente d'accident du "
        "travail ou les allocations familiales le 31 décembre, par âge et par sexe, avec le "
        "montant payé dans l'année.",
        "Le recueil v7 ajoute : sortie au mois et au trimestre (N1) ; l'allocataire de T41 est le "
        "titulaire du dossier, compté une fois (N2) ; T35 sans « 25 ans et plus » ; « rentiers » "
        "(N9) ; coordination : seule la part CNSS ; rapport démographique et taux de remplacement "
        "(N7) ; module des invalides : hors BI (N5). Rien pour T39 et T40.",
        "L'annuaire CNSS 2024 confirme le bénéficiaire (R6) : T32 31 093 pour 30 808 (+0,9 %), "
        "T36 exact, T37 −1,5 %, T41 +0,7 %. Il tranche : retraite anticipée à part (N4), dates "
        "« 1900 » dans « 100 ans et plus » (R8), accessoires hors du montant de T32 (N13).",
        "Restent en écart en 2024 : veuves −1 079 (−5,0 %), orphelins −1 508 (−15,9 %), rentes "
        "d'orphelins −6,4 % ; montants des rentes −12 à −25 %, allocations familiales −3 à −7 % ; "
        "T41 +5 à +11 % de 2020 à 2023 ; plus de jeunes retraités que le tableau 19.",
        "Décisions attendues : Q1 quel montant ; Q2 le mois d'un paiement (posée le 02/11) ; Q3 "
        "annuaire arrêté fin mars ; Q4 les veuves qui manquent ; Q12 âge limite des rentes "
        "d'orphelins. Aucune ne bloque : chaque réglage a une valeur par défaut.",
        "À faire : exécuter t32_t41 et les comparaisons A06 à A08 ; un lot de contrôle (âge des "
        "allocataires, montants mois par mois pour Q2, veuves, enfants par âge) ; réglages après "
        "visa ; table de l'entrepôt à choisir avec Jules.",
    ],
    # ------------------------------------------------------------------ 0. les tableaux
    "tableaux": [
        ["T32", "Pensionnés de vieillesse et montants, par tranche de 5 ans et par sexe, fin "
                "d'exercice. Moins de 55 ans … 100 ans et plus ; effectif et montant des hommes, "
                "des femmes, total ; pas de moyenne, pas d'« âge inconnu »", "p. 13 et 37",
         "t32_t41_requete_proposee.sql (pas exécutée) ; fiche T32 ; lots 5, 6"],
        ["T33", "Pensionnés d'invalidité. Moins de 25 ans … 60 ans et +", "p. 13 et 37",
         "même requête ; lot 5"],
        ["T34", "Veuves et veufs pensionnés. Moins de 15 ans … 100 ans et plus", "p. 13 et 38",
         "même requête ; lots 5, 6"],
        ["T35", "Orphelins pensionnés. Moins de 5 ans … 20-24 ans ; « 25 ans et plus » supprimée "
                "(Kaya 2017, recueil v7)", "p. 14 et 38", "même requête ; lot 5"],
        ["T36", "Ascendants pensionnés (père ou mère d'un assuré décédé). Moins de 50 ans … 100 "
                "ans et plus", "p. 14 et 39", "même requête ; lot 5"],
        ["T37", "Victimes d'accident du travail avec une rente à vie (rente viagère). Moins de 20 "
                "ans … 100 ans et plus", "p. 14 et 39", "même requête ; lot 5"],
        ["T38", "Veuves et veufs rentiers (« crédirentiers »). Même grille que T37",
         "p. 14 et 40", "même requête ; lot 5"],
        ["T39", "Orphelins rentiers. Moins de 5 ans … 25 ans et plus", "p. 14 et 40",
         "même requête ; lot 5"],
        ["T40", "Ascendants rentiers. Moins de 50 ans … 80 ans et +", "p. 14 et 41",
         "même requête ; lot 5"],
        ["T41", "Allocataires des prestations familiales et montants. Moins de 20 ans … 80 ans et "
                "plus", "p. 14 et 41", "même requête ; fiche T41 ; lot 6 (P79)"],
    ],
    # ------------------------------------------------------------------ 1. recueil v7
    "recueil_note": (
        "Toutes les lignes du recueil v7 qui touchent T32 à T41 (numéros de besoins_v7.txt). Les "
        "formules des indicateurs ont été relues dans le document Word. T39 et T40 n'ont aucune "
        "ligne (question Q13). Ne changent rien ici : l. 32 (alertes : aucune norme), l. 53 à 60 "
        "(Excel), ratios CIPRES respectés, colonnes « Priorité » vides ; l. 80 et 81 et "
        "satisfaction sont rangées dans T50-T57 ; l. 202 et 298 relèvent de T42 à T49."
    ),
    "recueil": [
        ("#4 (T41), l. 152", "« Allocataire : Titulaire du dossier (assuré) »", "L'assuré au nom "
         "de qui est le dossier, une fois. Le glossaire CIPRES (p. 62) est plus large",
         "N2 ; allocataire = TITULAIRE (−6 sans la condition de paiement) ; titulaire décédé : Q8",
         "à intégrer"),
        ("#4 (T41), l. 153", "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
         "T41 à chaque fin de mois et de trimestre", "N1, N10 ; periodicite, t41_paiement",
         "à intégrer"),
        ("#9 (T37), l. 188", "« Idem. Tab 41 »", "Périodicité de T41 ; le titulaire est la "
         "victime : la définition ne change rien (à vérifier)", "N1 ; R5", "à intégrer"),
        ("#10 (T38), l. 194", "« Crédirentiers : rentiers »", "Dire « rentiers »",
         "N9 ; T39 et T40 (Q13)", "à intégrer"),
        ("#10 (T38), l. 195", "« Le cumul sans préciser un exercice »", "Stock à la date "
         "choisie (lecture BI), ou montant cumulé ; T45 (l. 203) demande une période",
         "R6, N1 ; remarque B28 (Q14)", "question"),
        ("#10 (T38), l. 196", "« Idem. Tab 41 »", "Périodicité ; on compte la veuve, pas le "
         "titulaire (victime décédée)", "N1 ; R5 (lecture BI)", "à intégrer"),
        ("#20 (T32), l. 263", "« Pour les pensions de coordination, les montants concernent "
         "uniquement la partie CNSS »", "Seul ce que verse la CNSS ; Kaya 2017 l. 292",
         "R3 ; payées ailleurs comptées sans montant (P78) ; moyenne (Q16)", "pris en compte"),
        ("#20 (T32), l. 264", "« Idem. Tab 41 »", "Périodicité ; titulaire = retraité", "N1",
         "à intégrer"),
        ("#21 (T33), l. 270", "Un module pour convertir la pension d'invalidité en pension "
         "normale à l'âge de la retraite", "Travail sur l'application, pas sur la BI",
         "N5 ; Q15", "hors BI"),
        ("#21 (T33), l. 271", "« Idem. Tab 41 »", "Périodicité ; titulaire = invalide", "N1",
         "à intégrer"),
        ("#22 (T34), l. 277", "« Idem. Tab 41 »", "Périodicité ; on compte la veuve ou le veuf",
         "N1 ; R5 (lecture BI)", "à intégrer"),
        ("#23 (T35), l. 283", "« Supprimer la tranche d'âge « 25 ans et plus » »", "Orphelin "
         "compté jusqu'à 24 ans ; Kaya 2017 l. 322", "R6 ; age_max_orphelin ; annuaire tableau 20",
         "pris en compte"),
        ("#23 (T35), l. 284", "« Idem. Tab 41 »", "Périodicité ; l'orphelin, pas le tuteur",
         "N1 ; R5", "à intégrer"),
        ("#24 (T36), l. 290", "« Idem. Tab 41 »", "Périodicité ; l'ascendant, pas l'assuré",
         "N1 ; R5", "à intégrer"),
        ("T32 à T38, l. 188 à 290 ; renvoi à T41, l. 152", "« Idem. Tab 41 » : « Allocataire : "
         "Titulaire du dossier (assuré) »", "À la lettre, T34, T35, T36, T38 compteraient le "
         "défunt à la place de sa famille", "R5 : l'ayant droit ; une seule question pour T32 à "
         "T49 : T42-T49 Q1", "question"),
        ("Liste Prestation, l. 115 à 335", "Aucune ligne pour T39 et T40", "Ni le recueil ni "
         "Kaya 2017 n'en parlent", "Règles de T38 : « rentiers », au mois, au trimestre, à "
         "l'année (Q13)", "question"),
        ("Rapport démographique, l. 1247 à 1254", "Cotisants actifs ÷ retraités ; retraités = "
         "pensions directes : vieillesse, anticipée, invalidité (l. 1251) ; DR et agence",
         "T32 (anticipée comprise) + T33, une fois chacun ; cotisants = « dossiers actifs »",
         "N7, N3 ; coordination (Q17)", "à intégrer"),
        ("Taux de remplacement, l. 1256 à 1265", "Pension ÷ dernier revenu × 100 ; salaire moyen "
         "des cinq meilleures années ; pensions directes ; DR et agence", "La part du salaire "
         "que la pension remplace", "Pas calculé ; PE_SAL_MOYEN (Q19) ; stock ou nouvelles (Q18)",
         "question"),
        ("#3 (T31), l. 142 à 144", "PVID branche pension ; AT/MP (RP) ; PF", "T32 à T36 PVID ; "
         "T37 à T40 RP ; T41 PF", "R1 du README ; R2", "pris en compte"),
        ("Termes de référence, l. 4 à 10", "Cinq DR : Ouagadougou, Bobo, Ouahigouya, Fada, "
         "Dédougou", "Ouahigouya = DRN de l'annuaire", "N3", "à intégrer"),
        ("Indicateurs, l. 1065", "« Donner la possibilité de désagréger l'indicateur »",
         "DR, agence, type, sexe, âge", "N3, N4", "à intégrer"),
        ("Attentes l. 29 ; caractéristiques l. 51, 68", "Jusqu'à la granularité la plus fine ; "
         "« niveau transactionnel »", "Déplier chaque chiffre jusqu'au bénéficiaire (l. 29 : "
         "cahier bancaire, B41)", "N12", "à intégrer"),
        ("Caractéristiques, l. 72", "« Contrôle par profil d'utilisateurs des conditions d'accès "
         "aux données »", "La liste des bénéficiaires est personnelle", "N12 : sans nom ; accès "
         "par profil", "à intégrer"),
        ("Attentes, l. 31 et 74", "Présentation multidimensionnelle ; une dimension ajoutée vaut "
         "partout", "Mêmes axes pour les dix tableaux", "N3, N4, N12", "à intégrer"),
        ("Attentes, l. 35", "Rapports « à des fréquences adaptées »", "Chaque tableau à sa "
         "fréquence", "N1", "à intégrer"),
        ("Attentes, l. 28, 34, 75", "Peu d'intervention manuelle ; restitutions automatisées",
         "Les chiffres sortent d'une requête", "Requête t32_t41", "pris en compte"),
        ("Caractéristiques, l. 58", "Règles de transcodage écrites et rejouables", "Codes et "
         "bornes des tranches écrits", "R2, R8 ; listes perimetre et grille", "pris en compte"),
        ("Attentes, l. 24 et 78", "Profilage de la qualité des données", "Mesurer les trous de "
         "la base", "Lots 5 et 6 ; dates « 1900 », orphelins non radiés", "pris en compte"),
        ("Général, l. 15 et 92 à 94", "La source : l'application de gestion des prestations",
         "Copie de la base au 02/02/2026", "Toutes les règles", "pris en compte"),
        ("Indicateur 2, l. 1085 à 1091", "Part des prestations payées par virement ; mois, "
         "trimestre, an ; type, DR, agence, mode", "Mêmes paiements que les montants de T32 à "
         "T41", "Hors famille ; mode absent pour 62 % des paiements PF 2025", "à intégrer"),
        ("Ratio des ressources, l. 1129 à 1141 (note l. 1133)", "Ordonnancé = « ayant fait "
         "l'objet d'une notification »", "Seule définition d'un montant dans le recueil, pour un "
         "autre indicateur", "Versée à Q1 et Q2 ; ne les tranche pas", "pris en compte"),
    ],
    # ------------------------------------------------------------------ 2. règles
    "regles": [
        {"id": "R2", "titre": "Les tableaux et leurs codes",
         "regle": "T32 RPV (et RPRA à part, N4) ; T33 RPI ; T34 RPSV, RPSF ; T35 RPSE ; T36 RPA ; "
                  "T37 ARI ; T38 ARSV, ARSF ; T39 ARSE ; T40 ARSA ; T41 FAF.",
         "exemple": "pension de veuve (RPSV) dans T34 ; rente de veuve d'accident (ARSV) dans "
                    "T38 ; allocation d'incapacité (AAI) : pas dans T37.",
         "sources": [(CIPRES, "p. 13, 14, 65"), (RV7, "l. 142 à 144"),
                     (CNSS, "tableaux 18 et 79"), (DON, "P34, P74, P87")],
         "etat": "confirmée par l'annuaire CNSS 2024 ; codes à viser (points_a_valider n° 18)"},
        {"id": "R5", "titre": "La personne comptée",
         "regle": "L'ayant droit (IND_ID_PREST), une fois par tableau. L'orphelin, pas le tuteur. "
                  "On n'additionne jamais deux tableaux pour compter des personnes.",
         "exemple": "une veuve retraitée compte dans T34 et dans T32.",
         "sources": [(CNSS, "tableau 20 : orphelins à leur âge"),
                     (BI, "lecture du « Idem. Tab 41 »"),
                     (DON, "P74 : 31 093 personnes, 31 267 pensions ; P77, P16")],
         "etat": "confirmée par l'annuaire CNSS 2024 ; 174 personnes à deux pensions de T32 en "
                 "2024 (points_a_valider n° 8)"},
        {"id": "R6", "titre": "Le bénéficiaire au 31/12 (pensions, rentes)",
         "regle": "Prestation en cours le 31/12 (ni radiée ni suspendue), personne en vie, payée "
                  "ou non. Orphelins (T35, T39) : moins de 25 ans.",
         "exemple": "plus payée depuis octobre : comptée ; suspendue le 31/10 : non.",
         "sources": [(CIPRES, "p. 13 et 14"), (RV7, "l. 283"), (KAYA, "l. 322"),
                     (CNSS, "tableaux 18, 20, 76 à 80"), (DON, "P74, P81")],
         "etat": "confirmée par l'annuaire CNSS 2024 (T32 +0,9 %) ; payée dans l'année : −10,2 % ; "
                 "écart 2024 à vérifier par A06 (points_a_valider n° 3)"},
        {"id": "R6 (T41)", "titre": "L'enfant et l'allocataire",
         "regle": "Enfant en cours, 21 ans au plus, en vie, payé dans l'année ; allocataire = au "
                  "moins un enfant compté. Allocations familiales (FAF) seules.",
         "exemple": "un dossier dont le seul enfant n'a rien touché en 2024 : pas compté.",
         "sources": [(KAYA, "l. 120"), (CNSS, "tableaux 36 à 38"),
                     (DON, "P79 : 98 048 pour 97 378")],
         "etat": "confirmée pour 2024 seulement (+5 à +11 % de 2020 à 2023, non expliqué) ; "
                 "paiement : Q5 ; 21 ans et prénatale seule : tranchés"},
        {"id": "N2", "titre": "L'allocataire = le titulaire",
         "regle": "Le titulaire du dossier d'allocations familiales (DOSSIER.IND_ID), l'assuré, "
                  "une fois même avec deux dossiers.",
         "exemple": "un assuré avec deux dossiers : un allocataire.",
         "sources": [(RV7, "l. 152"), (KAYA, "l. 120"), (CIPRES, "glossaire p. 62, plus large"),
                     (DON, "P74 : −6, sans la condition de paiement")],
         "etat": "nouvelle ; tranchée par le recueil v7 ; à remesurer ; titulaire décédé : Q8"},
        {"id": "R8", "titre": "Âge, tranches et sexe",
         "regle": "Année du calcul moins année de naissance ; grille de chaque maquette ; sexe 1 "
                  "hommes, 2 femmes. Proposé : 120 ans et plus dans « 100 ans et plus ».",
         "exemple": "né en 1960 : 64 ans en 2024 ; veuve « née en 1900 » : 100 ans et plus.",
         "sources": [(CIPRES, "glossaire p. 62 ; p. 37 à 41"), (CNSS, "tableaux 19, 20, 21, 76"),
                     (DON, "P75, P76, P80, P83")],
         "etat": "grilles et sexe confirmés ; ligne « 1900 » tranchée par l'annuaire ; date de "
                 "l'âge à vérifier (Q10)"},
        {"id": "R3", "titre": "Le montant",
         "regle": "Paiements faits (débours D ou P) dans l'année, aux comptés par défaut. Avec "
                  "TOUT : ligne « sortis dans l'année ». Coordination : part CNSS seule.",
         "exemple": "12 fois 100 000 FCFA : 1 200 000 ; pension payée par l'autre caisse : 0.",
         "sources": [(CIPRES, "p. 13 et 14"), (RV7, "l. 263"), (KAYA, "l. 292"),
                     (DON, "P75 : 37,67 ou 38,63 Md (38,24 publiés) ; P78")],
         "etat": "proposée ; l'annuaire ne tranche pas (Q1, réglage montant)"},
        {"id": "A1", "titre": "Les accessoires de pension",
         "regle": "Bonification, majorations pour enfant, charges du ménage s'ajoutent à une "
                  "pension : pas de pensionné de plus (sens de la carte A1, gardé).",
         "exemple": "pension + bonification : compté une fois dans T32.",
         "sources": [(CNSS, "tableau 18 : 7 types de pension"),
                     (DON, "P34 ; carte A1 de regles_prestations, README")],
         "etat": "confirmée ; RRC sans doute pas un accessoire (à vérifier)"},
        {"id": "N13", "titre": "Montant de T32 sans les accessoires",
         "regle": "Le montant de T32 ne prend que la pension ; l'argent des accessoires reste "
                  "à part, comme la majoration de l'annuaire.",
         "exemple": "100 000 de pension + 5 000 de bonification par mois : 1 200 000 dans T32.",
         "sources": [(CNSS, "tableaux 19, 28, 34 : pension seule"),
                     (DON, "P98 : +0,4 % saisie, +3,1 % mois payé")],
         "etat": "nouvelle ; tranchée par l'annuaire ; va contre la carte A1, le README et "
                 "points_a_valider n° 12 (laissés tels quels)"},
        {"id": "R4", "titre": "L'année d'un paiement",
         "regle": "L'année du mois payé (date_montant = MOIS) ; autre lecture : l'année de la "
                  "saisie, rappels compris (SAISIE).",
         "exemple": "rappel de février 2025 pour fin 2024 : 2024 (MOIS) ou 2025 (SAISIE).",
         "sources": [(CNSS, "démarche ; tableaux 28, 41, 90 au mois"),
                     (DON, "P98 : saisie meilleure sur 4 groupes sur 6")],
         "etat": "en attente (Q2, posée au point du 02/11/2026) ; −3,5 % à +29 % selon le "
                 "tableau ; change aussi T41"},
        {"id": "R13", "titre": "La date du calcul",
         "regle": "L'année N sur la copie de fin mars N+1, date d'arrêt de l'annuaire.",
         "exemple": "veuve décédée en novembre 2024, saisie en juin 2025 : comptée fin 2024.",
         "sources": [(CNSS, "démarche ; tableaux 20, 22, 26"),
                     (DON, "P99 : 444 (indice), 404, pour 447")],
         "etat": "proposée ; à valider (Q3, points_a_valider n° 2)"},
        {"id": "N1", "titre": "Fin de mois, de trimestre, d'année",
         "regle": "Stock à la fin de chaque période ; montant de la période par défaut (l'année = "
                  "somme des mois). Le stock ne s'additionne pas.",
         "exemple": "T32 au 31/03/2025 : en cours ce jour-là ; montant de mars.",
         "sources": [(RV7, "l. 153 et « Idem. Tab 41 » ; l. 35"),
                     (CNSS, "tableaux 28, 41, 90")],
         "etat": "nouvelle, à intégrer ; montant mensuel : Q7"},
        {"id": "N3", "titre": "Direction régionale et agence",
         "regle": "DR du dossier (DR_NO) : DRO, DRB, DRN Ouahigouya, DRF, DRD. Agence = service "
                  "du dossier (SP_NO), comme T25 à T27.",
         "exemple": "pensions de vieillesse DRO fin 2024 : 20 352 ; 20 067 publiés.",
         "sources": [(RV7, "l. 4 à 10, 1065, 1254, 1265"), (CNSS, "tableaux 22, 37, 76, 79"),
                     (DON, "A02 ; P46, P47")],
         "etat": "nouvelle ; DR précisée par l'annuaire (tableau 22) ; agence : Q20 (= T25-T27 Q20) ; pour les "
                 "tableaux, proposition BI (le recueil vise les indicateurs)"},
        {"id": "N4", "titre": "Retraite anticipée à part",
         "regle": "Le code reste visible. RPRA sur une ligne à part sous T32, hors des tranches, "
                  "comme le tableau 19. Maquette CIPRES : on additionne.",
         "exemple": "fin 2024 : 31 180 pensions de vieillesse, 87 anticipées ; publié 30 730 et "
                    "78.",
         "sources": [(CNSS, "tableaux 18, 19, 22, 23"), (RV7, "l. 248, 255, 1251"),
                     (KAYA, "l. 271"), (DON, "P76 : 16 de moins de 55 ans")],
         "etat": "nouvelle ; tranchée par l'annuaire CNSS 2024 ; personnes à mesurer"},
        {"id": "N5", "titre": "Invalidité non convertie",
         "regle": "Une pension d'invalidité reste dans T33 après l'âge de la retraite, tant que "
                  "l'application ne la convertit pas.",
         "exemple": "invalide de 68 ans : T33, « 60 ans et + ».",
         "sources": [(KAYA, "l. 300 à 302"), (RV7, "l. 270"),
                     (CNSS, "tableau 21 : 70 sur 119"), (DON, "P76 : 72 sur 125")],
         "etat": "nouvelle (pratique CNSS), à valider (Q15)"},
        {"id": "N6", "titre": "Le tableau des enfants",
         "regle": "À côté de T41 : enfants comptés par tranche (moins de 5 ans … 20-21 ans), par "
                  "sexe, avec le montant.",
         "exemple": "2024 : 271 577 chez nous, 268 481 publiés.",
         "sources": [(KAYA, "l. 53"), (CNSS, "tableau 36"), (DON, "P79, P76")],
         "etat": "nouvelle ; tranchée par Kaya 2017 ; forme par âge : Q11"},
        {"id": "N7", "titre": "Retraités du rapport démographique",
         "regle": "Pensions directes : T32 (avec l'anticipée) et T33 à la même date, une fois "
                  "chacun, par DR et agence. Coordination comprise par défaut.",
         "exemple": "31/12/2024 : 31 093 dans T32 et 125 dans T33.",
         "sources": [(RV7, "l. 1247 à 1254"), (DON, "P74, P77, P78")],
         "etat": "nouvelle, à intégrer ; coordination : Q17"},
        {"id": "N8", "titre": "Contrôle des entrées et sorties",
         "regle": "Fin = début + nouveaux (T42 à T49 ; à part pour T33, T36, T41) − décès − "
                  "autres sorties.",
         "exemple": "orphelins publiés : 8 687 + 769 − 2 = 9 454 pour 9 478, sans sortie ; la "
                    "base en voit sortir 900 à 1 900 par an.",
         "sources": [(CNSS, "tableaux 18, 23, 26, 75, 77, 80, 81"), (BI, "comme T60 en GRH")],
         "etat": "nouvelle (proposition BI)"},
        {"id": "N9", "titre": "Rentiers, pas crédirentiers",
         "regle": "Les libellés de T38 (et T45 à T49) disent « rentiers ».",
         "exemple": "T38 : répartition des rentiers veufs et veuves.",
         "sources": [(RV7, "l. 194 et 202")],
         "etat": "nouvelle, à intégrer ; T39 et T40 : Q13"},
        {"id": "N10", "titre": "T41 en cours d'année",
         "regle": "À une fin de mois ou de trimestre, un enfant compte s'il a été payé dans les 12 "
                  "mois avant ; au 31/12, l'année civile.",
         "exemple": "au 30/06/2025, payé en novembre 2024 : compté.",
         "sources": [(RV7, "l. 153"), (DON, "P79"), (BI, "la fenêtre de 12 mois")],
         "etat": "nouvelle (proposition BI), à valider (Q6)"},
        {"id": "N11", "titre": "Dossiers transférés (CNSS_AC)",
         "regle": "Une pension d'un dossier transféré à une autre caisse reste comptée ; un "
                  "réglage permet de la retirer, comme T25-T27 D1.",
         "exemple": "fin 2025 : 34 dans T32 (2 payées), 79 dans T34.",
         "sources": [(DON, "P78"), (BI, "réglage comme T25-T27 D1")],
         "etat": "nouvelle (proposition BI) ; Q23"},
        {"id": "N12", "titre": "Retrouver chaque bénéficiaire",
         "regle": "Une ligne par personne, tableau et date : code, âge, sexe, DR, agence, "
                  "montant ; sans nom ni numéro ; accès par profil.",
         "exemple": "les 125 invalides de fin 2024, un à un, sans nom.",
         "sources": [(RV7, "l. 29, 51, 68, 72")],
         "etat": "nouvelle ; aucune requête ne la produit"},
    ],
    # ------------------------------------------------------------------ 3. cas
    "cas_note": (
        "Les 12 cas les plus parlants (19 dans le md). Chaque carte dit ce que devient la "
        "personne, et selon quelle règle."
    ),
    "cas": [
        ("Cas 2. Plus payé depuis octobre", "Pension en cours, ni suspendue ni radiée ; certificat "
         "de vie attendu. 3 423 retraités comptés sans paiement CNSS en 2024.",
         "COMPTÉ : payé ou non, comme l'annuaire (R6)", "compte"),
        ("Cas 3. Coordination payée ailleurs", "L'autre caisse verse la pension (COOR_AC). La "
         "moyenne est 8,8 % plus basse avec elles.",
         "RÉGLAGE moyenne_coordination : compté, 0 FCFA (R3, Q16)", "reglage"),
        ("Cas 6. Une retraite anticipée", "Code RPRA, 54 ans en 2024. L'annuaire la publie à part "
         "(78) et la sort du tableau 19.",
         "RÉGLAGE retraite_anticipee : ligne à part sous T32 (N4)", "reglage"),
        ("Cas 7. Une veuve retraitée", "Pension de veuve dans le dossier du mari, et sa propre "
         "retraite.", "COMPTÉE dans T34 et dans T32, à son âge (R5)", "compte"),
        ("Cas 8. Une veuve « née en 1900 »", "Date par défaut : 124 ans ; 499 veuves dans ce cas.",
         "RÉGLAGE ages_impossibles : « 100 ans et plus » (R8)", "reglage"),
        ("Cas 9. Un orphelin de 25 ans", "Pension jamais radiée, plus payée ; 5 774 dans ce cas "
         "fin 2024.", "PAS COMPTÉ : moins de 25 ans (R6, recueil l. 283)", "pas compte"),
        ("Cas 10. Un invalide de 68 ans", "Pension d'invalidité jamais convertie en vieillesse.",
         "À SURVEILLER : reste dans T33 (N5, Q15)", "attention"),
        ("Cas 11. Un orphelin rentier de 23 ans", "Rente d'orphelin d'accident (ARSE). Annuaire "
         "485 ; nous 454 (moins de 25 ans) ou 531.", "QUESTION Q12 : la limite d'âge", "question"),
        ("Cas 13. Un enfant jamais payé en 2024", "Allocation en cours, certificat attendu ; seul "
         "enfant du dossier.", "PAS COMPTÉ, ni l'allocataire : +0,7 % (R6 T41, Q5)", "pas compte"),
        ("Cas 15. Un titulaire décédé", "Ses enfants touchent encore, par un tuteur. Nombre de cas "
         "pas mesuré.", "QUESTION Q8 : qui est l'allocataire ?", "question"),
        ("Cas 16. Un décès déclaré tard", "Veuve décédée en novembre 2024, décès saisi en juin "
         "2025.", "À SURVEILLER : comptée seulement sur la copie de fin mars (R13)", "attention"),
        ("Cas 17. Un rappel de février 2025", "Paie octobre à décembre 2024.",
         "RÉGLAGE date_montant : 2024 (MOIS) ou 2025 (SAISIE) (R4, Q2)", "reglage"),
    ],
    # ------------------------------------------------------------------ 4. annuaire
    "annuaire_note": (
        "L'annuaire publie presque tous ces effectifs, sauf l'âge des allocataires. Seule 2024 a "
        "été publiée sur une base proche de la nôtre ; quand 2020 à 2023 s'écartent, c'est dit. "
        "Les modèles de ratios de la CNSS (feuilles 03 à 06) n'apportent rien de direct ici."
    ),
    "annuaire": [
        ("Tableau 18 (p. 16)", "Vieillesse et anticipée 2020 à 2024 (T32)", "25 273 … 31 093",
         "25 230 … 30 808", "+0,2 à +0,9 %", "confirme", "R6 ; payé dans l'année : −10,2 % ; "
         "écart 2024 à vérifier (A06)"),
        ("Tableau 18 (p. 16)", "Invalidité 2020 à 2024 (T33)", "111 … 125", "114 … 119",
         "−2,6 à +5,0 %", "confirme", "Quelques unités"),
        ("Tableaux 18 et 22", "Ascendants 2024 (T36)", "59", "59", "0", "confirme",
         "Exact dans les 5 DR"),
        ("Tableaux 18 et 80", "Ascendants 2020 à 2023 (T36, T40)", "T36 58 … 59 ; T40 225 … 214",
         "T36 48 … 54 ; T40 185 … 196", "+9 à +22 %", "ecart", "Seule 2024 tombe juste ; à "
         "expliquer, pas bloquant"),
        ("Tableau 18 (p. 16)", "Veuves et veufs (T34)", "20 715 (20 359 f, 356 h)",
         "21 767 (21 438, 329)", "veuves −5,0 % ; veufs +8,2 %", "ecart", "Décès ou radiations "
         "saisis tard (R13 ; A06, A07 ; Q4)"),
        ("Tableau 20 (p. 17)", "Orphelins de moins de 25 ans (T35)", "à leur âge",
         "9 478, tous < 25 ans", "aucun", "confirme", "Limite de 25 ans et orphelin compté : "
         "pratique CNSS et recueil"),
        ("Tableaux 18, 20, 23, 26", "Orphelins 2020 à 2024 (T35)", "9 686 … 7 970",
         "8 982 … 9 478", "+7,8 % … −15,9 %", "ecart", "2024 incohérent avec ses entrées : on ne "
         "juge pas la règle sur lui (A07, A08)"),
        ("Tableaux 18, 23, 26", "Entrées et sorties 2024 (N8)", "635 sorties de retraités",
         "vieillesse 31 081 pour 30 730", "CNSS : 351 sorties hors décès ; orphelins +24 ; "
         "ascendants −28", "precise",
         "Contrôle N8 à côté de chaque tableau"),
        ("Tableau 19 (p. 16)", "Grille et sexe (T32)", "24 814 h ; 6 279 f",
         "24 627 h ; 6 103 f", "+0,8 % ; +2,9 %", "confirme", "Grille et sexe ; femmes de 55 à 64 "
         "ans en trop (n° 44)"),
        ("Tableaux 19 et 25", "Forme par âge (T32, R8)", "55-59 ans 3 778", "3 399",
         "+11,2 % ; 65-89 −261", "ecart", "Âge à une autre date ? (P83, Q10)"),
        ("Tableaux 19 et 34", "Montant de T32", "37,67 Md (comptés) ; 38,63 Md (tout)",
         "38,24 Md", "−1,5 % ; +1,0 %", "precise", "Entre nos deux montants ; le plus proche "
         "varie selon le groupe (Q1, Q2)"),
        ("Tableaux 28 et 34", "Majoration pour enfant (A1, N13)", "539,4 M (saisie) ; 554,3 M "
         "(mois)", "537,3 M à part", "+0,4 % ; +3,1 %", "confirme", "Pas un pensionné de plus "
         "(A1) ; montant hors de T32 (N13)"),
        ("Tableau 20 (p. 17)", "100 ans et plus (R8)", "635, dont 500 « nés en 1900 »", "679",
         "−44 ; −544 sans", "tranche", "Dates par défaut dans « 100 ans et plus »"),
        ("Tableau 20 (p. 17)", "Veuves, veufs, ascendants par âge", "20 777", "21 826",
         "−4,8 % (55 ans et plus)", "precise", "Pas de remariages ni de suspensions (Q4)"),
        ("Tableaux 20, 21, 36", "Montants survivants, invalidité, allocations familiales",
         "comptés : 7,16 Md ; 112,1 M ; 7,69 Md", "7,05 Md ; 113,6 M ; 8,24 Md",
         "survivants +1,6 à +2,5 % ; invalidité −1,4 à +2,2 % ; AF −2,9 à −6,7 %", "precise",
         "Entre nos montants (invalidité), sous (survivants), au-dessus (AF) ; Q1, Q2, Q9"),
        ("Tableau 21 (p. 18)", "Invalides par âge (T33)", "125 ; 72 de 60 ans et +",
         "119 ; 70", "+5,0 %", "confirme", "Grille ; invalides gardés après 60 ans (N5)"),
        ("Tableau 22 (p. 18)", "Pensionnés par DR (N3)", "vieillesse DRO 20 352 (pensions)",
         "20 067", "+0,1 à +4,0 %", "precise", "DR du dossier plausible ; veuves DRB −8,1 %, DRD "
         "−11,5 % (comme T25-T27, T42-T49)"),
        ("Tableau 36 (p. 30)", "Enfants par âge (N6)", "271 577", "268 481",
         "+1,2 % ; 20-21 ans −32,1 %", "ecart", "Enfants sortis à 21 ans comptés ? (Q11)"),
        ("Tableaux 37 et 38", "Allocataires 2024 (T41)", "98 048", "97 378", "+0,7 %",
         "confirme", "Seulement avec « payé dans l'année » (sinon +5,4 %)"),
        ("Tableau 38 (p. 31)", "Allocataires 2020 à 2023 (T41)", "93 972 … 98 128",
         "84 401 … 93 233", "+11,3 à +5,3 %", "ecart", "Non expliqué (P79 écarte la saisie "
         "tardive)"),
        ("Tableau 76 (p. 60)", "Rentiers viagers (T37)", "1 236", "1 255", "−1,5 %", "confirme",
         "Grille CNSS jusqu'à 90 ans et + : regrouper"),
        ("Tableaux 78 à 80", "Rentes 2024 (T37, T38, T40)", "1 236 ; 288 ; 214",
         "1 255 ; 285 ; 207", "−1,5 ; +1,1 ; +3,4 %", "confirme", "R6 vaut pour les rentes ; "
         "« ensemble au 31-12 » (Q14)"),
        ("Tableaux 80, 77, 81", "Rentes d'orphelins (T39)", "454 (< 25 ans) ; 531",
         "485", "−6,4 % ; +9,5 %", "ecart", "+106 en 2024 pour 44 entrées ; limite (Q12)"),
        ("Tableaux 90, 96, 97", "Montants des rentes (T37 à T40)", "viagères 399 M ; survivants "
         "276 M", "455 M ; 369 M", "−12 à −25 %", "ecart", "Paiements absents de la base (Q21)"),
        ("Tableaux 28 à 33", "Vieillesse mois par mois et par DR", "pas comparé",
         "survivants 396 à 773 M par mois", "—", "precise", "Peut trancher R4 sans attendre"),
        ("Tableaux 90 à 95", "Rentes mois par mois et par DR", "pas comparé",
         "viagères 22 à 51 M par mois", "—", "precise", "Même test de R4 (T37 à T40)"),
        ("Tableaux 40 à 46", "Allocations familiales par DR et mois", "pas comparé",
         "DRO 4,97 Md …", "—", "precise", "Montant de T41 par DR et par mois"),
        ("Tableaux 34 et 96", "Montants par DR", "pas comparé", "retraite DRO 26,98 Md …", "—",
         "precise", "Contrôle des montants par DR (N3)"),
        ("Tableau 25 (p. 20)", "Nouveaux retraités par âge", "60 ans : 377", "519",
         "femmes +114", "precise", "Même décalage que T32 (Q10)"),
        ("Tableaux 106 et 97", "Montants 2020 à 2024", "pas comparé", "35,2 à 46,6 Md",
         "—", "precise", "Tester les montants sur 5 ans"),
        ("Démarche, p. xiii, xiv, xx", "Comment la CNSS fait l'annuaire", "—", "base + rapports "
         "+ collecte ; juin 2025", "—", "precise", "Jamais à l'unité ; arrêt fin mars (R13)"),
        ("Tableaux 18 à 80", "Ce qui n'est pas publié", "—", "âge des allocataires ; T34, T36, "
         "T38 à T40 par âge", "—", "non publie", "Ne se valide qu'avec la CNSS"),
    ],
    "annuaire_apres": (
        "Confirme : le bénéficiaire payé ou non (R6), l'orphelin à son âge, T41 en 2024, les "
        "grilles et le sexe (R8), l'accessoire pas compté à part (A1), "
        "l'invalidité gardée (N5). Tranche : l'anticipée à part (N4), « 1900 » dans « 100 ans et "
        "plus » (R8), 21 ans pour les enfants, accessoires hors du montant (N13). Précise : la DR "
        "du dossier (N3), le montant, les séries mensuelles. À expliquer : veuves, orphelins, rentes "
        "d'orphelins, historiques de T36, T40, T41, forme par âge, montants des rentes. Pas "
        "publié : âge des allocataires, T34, T36, T38 à T40 par âge."
    ),
    # ------------------------------------------------------------------ 5. réglages
    "reglages_note": (
        "Dans la CTE parametres de la requête t32_t41. « À créer » : le choix est écrit en dur, ou "
        "n'existe pas encore. Une décision attendue de la CNSS ne bloque rien : la valeur retenue "
        "s'applique en attendant. Un réglage suit le recueil ; s'il se tait, l'annuaire CNSS."
    ),
    "reglages": [
        ("annee, debut, fin", "t32_t41 (exercice donné au lancement)", "calculés dans "
         "parametres", "l'exercice au lancement", "Dates d'arrêté et période des montants", "BI"),
        ("date_montant", "t32_t41", "MOIS", "SAISIE", "−3,5 % à +29 % selon le tableau ; change "
         "aussi l'effectif de T41", "DCP, avec la DPC (Q2)"),
        ("montant (à créer)", "t32_t41", "COMPTES", "TOUT, avec une ligne « sortis »",
         "T32 2024 : 37,67 ou 38,63 Md", "DCP ; DES et DR ; DP et DR (Q1)"),
        ("montant_periode (à créer)", "t32_t41", "PERIODE", "CUMUL depuis janvier",
         "Le montant d'un tableau mensuel", "DCP, DES, DP (Q7)"),
        ("periodicite (à créer)", "t32_t41", "MENSUELLE", "ANNUELLE ; TRIMESTRIELLE",
         "Effectif en « dernière valeur » dans OBIEE", "tranché par le recueil (l. 153)"),
        ("retraite_anticipee (à créer)", "t32_t41", "LIGNE_A_PART", "DANS_T32 ; HORS_T32",
         "87 pensions de 2024 changent de ligne", "tranché par l'annuaire (tableau 19)"),
        ("ages_impossibles (à créer)", "t32_t41", "DERNIERE_TRANCHE", "LIGNE_A_PART",
         "504 personnes de 120 ans et plus changent de ligne", "tranché par l'annuaire (tableau "
         "20)"),
        ("age_max_orphelin (à créer)", "t32_t41", "25", "21 pour T39", "T39 2024 : 454, ou 531 "
         "sans limite (485 publiés)", "tranché pour T35 ; DP et DR pour T39 (Q12)"),
        ("age_max_enfant (à créer)", "t32_t41", "21", "une autre limite", "Enfants de 22 ans et "
         "plus exclus", "tranché par l'annuaire (tableau 36)"),
        ("t41_paiement (à créer)", "t32_t41", "12_MOIS (année civile au 31/12)", "SANS",
         "98 048 allocataires, ou 102 643 sans condition", "DES et DR (Q5, Q6)"),
        ("allocataire (à créer)", "t32_t41", "TITULAIRE", "DOSSIER", "−6 sans la condition de "
         "paiement ; à remesurer", "tranché par le recueil (l. 152)"),
        ("moyenne_coordination (à créer)", "t32_t41", "TOUTES", "PAYEES_CNSS", "Moyenne 9,7 % "
         "plus haute sans elles (fin 2025) ; T32, T34, T35", "DCP (Q16)"),
        ("invalides_age_retraite (à créer)", "t32_t41", "DANS_T33", "A_PART", "72 des 125 "
         "invalides de 2024 ont 60 ans et plus", "DCP (Q15)"),
        ("coordination_retraites (à créer)", "rapport démographique", "AVEC", "SANS", "2 901 "
         "pensions payées ailleurs fin 2025", "DCP (Q17)"),
        ("remplacement_population (à créer)", "taux de remplacement", "STOCK", "NOUVELLES",
         "La population de l'indicateur", "DCP (Q18)"),
        ("dossier_transfere (à créer)", "t32_t41", "GARDER", "EXCLURE (comme T25-T27 D1)",
         "34 pensions de T32, 79 de T34 (fin 2025)", "DCP, avec la DSI (Q23)"),
        ("agence (à créer)", "t32_t41", "SERVICE", "LIEU_PAIEMENT", "Le découpage par agence",
         "DSI (Q20)"),
        ("perimetre et grille (listes)", "t32_t41", "codes de R2, grilles CIPRES", "—",
         "Changer un code ou une borne sans réécrire", "DCP ; Annuaire CIPRES et recueil"),
        ("cliché (au lancement, hors parametres)", "t32_t41", "copie du 02/02/2026",
         "fin mars N+1 (R13)", "Environ 1 050 veuves et 1 500 orphelins de moins en 2024",
         "DPC (Q3), puis BI"),
    ],
    # ------------------------------------------------------------------ 6. questions
    "questions_note": (
        "Une question par point. Chacune a une valeur par défaut : la réponse change un réglage, "
        "pas le travail. DCP (Direction centrale des prestations) pour les pensions ; DP et "
        "directions régionales (DR) pour les rentes d'accident ; DES et DR pour les PF. DSI : "
        "systèmes d'information ; DPC : prospective, fait l'annuaire. Tranchées, plus posées : "
        "anticipée (N4), dates « 1900 » (R8), orphelins de T35, allocataire (N2), 21 ans, "
        "prénatale seule, tableau des enfants (N6), accessoires dans le montant (N13)."
    ),
    "questions": [
        ("Q1", "Le montant est-il ce qui est payé aux personnes comptées au 31/12, ou tout le payé "
               "de l'année, sortis compris ?", "DCP ; DES et DR ; DP et DR",
         "payé aux comptés (COMPTES)", "Déjà posée : points_a_valider n° 35 ; fiches T32 Q3, "
         "T41 Q3"),
        ("Q2", "Un paiement est-il rangé dans le mois qu'il paie, le mois où il est saisi "
               "(rappels compris), ou le mois où il est versé ?",
         "DPC, avec la DCP", "le mois payé (MOIS)",
         "Déjà posée : point du 02/11/2026 ; n° 1 ; même question que T28-T31 Q24"),
        ("Q3", "Votre annuaire de l'année N est-il fait avec la base de fin mars N+1 ?",
         "DPC (Service études et statistique)", "R13 proposée ; cliché du 02/02/2026",
         "Déjà posée : points_a_valider n° 2"),
        ("Q4", "Les décès des veuves, ou l'arrêt de leur pension, sont-ils souvent enregistrés "
               "des mois après ?", "DCP, avec les DR de Bobo et de Ouagadougou",
         "R6 inchangée ; R13 proposée", "Déjà posée : points_a_valider n° 37 ; A06, A07"),
        ("Q5", "Un enfant dont l'allocation court au 31/12, mais qui n'a rien touché dans l'année, "
               "compte-t-il dans T41 ?", "DES et DR", "non (payé dans l'année)",
         "Déjà posée : points_a_valider n° 38 ; fiche T41 Q1"),
        ("Q6", "Pour T41 à une fin de mois, un enfant compte-t-il s'il a été payé dans les 12 mois "
               "avant ?", "DES et DR", "oui (N10)", "Nouvelle"),
        ("Q7", "Pour un tableau mensuel, le montant est-il celui du mois seul, ou le cumul depuis "
               "janvier ?", "DCP, DES, DP", "le mois seul (PERIODE)", "Nouvelle"),
        ("Q8", "Si le titulaire du dossier PF est décédé, qui est l'allocataire : lui, ou la "
               "personne qui touche pour les enfants ?", "DES et DR",
         "le titulaire, à l'âge qu'il aurait", "Nouvelle"),
        ("Q9", "Votre montant des allocations familiales (8,24 Md) comprend-il des allocations "
               "validées mais pas versées ?", "DPC, avec la DES",
         "les allocations payées (D ou P)", "Déjà posée : points_a_valider n° 40 ; fiche T41 Q3"),
        ("Q10", "Dans vos tableaux par âge (19, 25, 36), l'âge est-il l'année de l'exercice moins "
                "l'année de naissance ?", "DPC", "oui, comme le glossaire CIPRES",
         "Nouvelle (liée à points_a_valider n° 44)"),
        ("Q11", "Votre tableau 36 compte-t-il aussi les enfants sortis dans l'année à 21 ans ?",
         "DPC, avec la DES", "enfants en cours au 31/12 et payés",
         "Nouvelle (liée à points_a_valider n° 42)"),
        ("Q12", "Jusqu'à quel âge une rente d'orphelin d'accident du travail est-elle versée ?",
         "DP et DR", "moins de 25 ans", "Nouvelle"),
        ("Q13", "T39 et T40, sans ligne dans le recueil, se font-ils comme T38 (« rentiers », au "
                "mois, au trimestre, à l'année) ?", "DP et DR", "oui (N1, N9)",
         "Déjà posée : remarque B38 du recueil ; comme T25-T27 Q12"),
        ("Q14", "Pour T38, « le cumul sans préciser un exercice » veut-il dire tous les rentiers "
                "en cours à la date choisie ?", "DP et DR", "oui, un stock (R6)",
         "Déjà posée : remarque B28 du recueil"),
        ("Q15", "En attendant le module, gardez-vous dans T33 les invalides qui ont passé l'âge de "
                "la retraite ?", "DCP", "gardés dans T33 (DANS_T33)", "Nouvelle"),
        ("Q16", "Faut-il garder dans la pension moyenne les pensions de coordination payées par "
                "l'autre caisse ?", "DCP", "oui (TOUTES), pour T32, T34, T35",
         "Déjà posée : points_a_valider n° 36 ; fiche T32 Q5"),
        ("Q17", "Pour le rapport démographique, une pension payée par l'autre caisse compte-t-elle "
                "comme un retraité ?", "DCP", "oui (AVEC)", "Nouvelle"),
        ("Q18", "Le taux de remplacement porte-t-il sur toutes les pensions directes en cours, ou "
                "sur les nouvelles de l'année ?", "DCP", "toutes en cours (STOCK)",
         "Nouvelle (relevé du recueil)"),
        ("Q19", "Le salaire moyen gardé avec chaque pension (PE_SAL_MOYEN) est-il celui des cinq "
                "meilleures années ?", "DSI, avec la DCP", "pas d'indicateur",
         "Nouvelle (remarque B37 du recueil)"),
        ("Q20", "L'agence d'un bénéficiaire est-elle le service de son dossier (SP_NO) ?", "DSI",
         "service du dossier (SERVICE)", "Déjà posée : T25-T27 Q20 ; remarque B34 du recueil"),
        ("Q21", "Où sont enregistrés les paiements de rentes d'accident qui manquent dans la "
                "base ?", "DSI, avec la direction financière",
         "paiements de la base ; montants avec réserve",
         "Déjà posée : points_a_valider n° 51 ; README"),
        ("Q22", "Un contrôle d'existence est-il prévu pour les pensionnés très âgés et les "
                "pensions de titulaires décédés ?", "DCP",
         "comptés tant que la pension court", "Déjà posée : points_a_valider n° 43 ; fiche T32 Q4"),
        ("Q23", "Une pension d'un dossier transféré à une autre caisse compte-t-elle encore parmi "
                "vos bénéficiaires ?", "DCP, avec la DSI", "comptée (GARDER)", "Nouvelle"),
    ],
    # ------------------------------------------------------------------ 7. points d'attention
    "attention_note": "Les 18 points du md, regroupés en 9.",
    "attention": [
        ("« Idem. Tab 41 »", "On garde la périodicité. La définition de l'allocataire est sans "
         "effet pour T32, T33, T37 et ne s'applique pas à T34, T35, T36, T38. T39 et T40 n'ont pas "
         "d'« Idem »."),
        ("Jamais à l'unité", "Copie du 02/02/2026 contre base du printemps 2025, rapports, "
         "collecte en DR et montants alignés sur la comptabilité. Les chiffres publiés ne tiennent "
         "pas toujours entre eux (orphelins, ascendants)."),
        ("2024 seule ne suffit pas", "T32 tombe juste sur 5 ans ; T41 (+5 à +11 %), T36 et T40 "
         "(+9 à +22 %) non, de 2020 à 2023, sans explication prouvée."),
        ("Personnes, pas pensions", "31 093 personnes pour 31 267 pensions dans T32 en 2024 ; A01 "
         "et A02 comptent des pensions. Ne jamais additionner deux tableaux. Un stock ne "
         "s'additionne pas d'un mois à l'autre (OBIEE)."),
        ("Requête et DTM", "Requête jamais exécutée, exercice encore donné au lancement. "
         "DTM_PRESTATION ne convient pas (H8, H9, H13) : table à choisir avec Jules. Si les faits "
         "gardent un seul cliché, produire fin mars (R13)."),
        ("Qualité des données", "503 dates « 1900 », 215 pensions de titulaires décédés, 5 774 "
         "pensions d'orphelins de 25 ans et plus jamais radiées : à signaler à la DCP, pas à "
         "corriger dans la BI."),
        ("Les montants", "Rentes d'accident −12 à −25 % : réserve (Q21). T41 −3 à −7 % avec un "
         "effectif juste. date_montant change aussi l'effectif de T41 : à mesurer."),
        ("Grilles et âges", "Grilles CNSS pas toutes CIPRES (tableaux 20, 76) : on regroupe. Âge "
         "par année, pas en années pleines comme la GRH. T35 : la ligne « 25 ans et plus » ne sort "
         "jamais ; T39 la garde, vide."),
        ("Documents existants", "N13 sort les accessoires du montant (la carte A1 les faisait "
         "suivre la pension) ; la requête et la fiche T32 mettent l'anticipée dans T32 ; README : allocataire par dossier, « âge inconnu ». Rien n'est "
         "modifié. Données personnelles : aucun nom lu."),
    ],
    # ------------------------------------------------------------------ 8. reste et sources
    "reste": [
        ("Exécuter t32_t41 (2024) : attendu T32 31 093 et 37,67 Md, T34 20 715, T35 7 970, T37 "
         "1 236, T41 98 048", "toi, puis Claude", "à faire"),
        ("Exécuter A06, A07, A08 (base à la publication, radiations, orphelins par âge)", "toi",
         "à faire"),
        ("Lot de contrôle : décès des veuves saisis après mars 2025, par âge et par DR", "Claude, "
         "puis toi", "à faire"),
        ("Lot : montants mois par mois face aux tableaux 28 à 33, 41 à 46, 90 à 95 (R4) ; par DR "
         "(34, 40, 96) ; 2020 à 2023 (106, 97)", "Claude, puis toi", "à faire"),
        ("Lot : âge et sexe des allocataires ; titulaires avec la condition de paiement ; effet de "
         "date_montant sur T41", "Claude, puis toi", "à faire"),
        ("Lot : enfants par tranche avec le montant ; allocataires par DR ; 2020 à 2023 avec la "
         "saisie avant le 30/06", "Claude, puis toi", "à faire"),
        ("Lot : montant AF par statut et par date (Q9)", "Claude, puis toi", "à faire"),
        ("Lot : T32 vieillesse seule par âge (âge 2024 et 2025) ; anticipées en personnes",
         "Claude, puis toi", "à faire"),
        ("Lot : rentes d'orphelins par âge (21, 25 ans) ; orphelins par DR", "Claude, puis toi",
         "à faire"),
        ("Lot : titulaires décédés (Q8) ; coordination et dossiers transférés en 2024", "Claude, "
         "puis toi", "à faire"),
        ("Réglages dans parametres, sans la valeur :2 ; DR, agence, code ; ligne anticipée ; N6, "
         "N8, N12", "Claude", "après visa"),
        ("Table du DTM et « dernière valeur » dans OBIEE ; un seul cliché ?", "Claude, avec Jules",
         "à faire"),
        ("Poser Q1 à Q23 ; signaler les données à corriger", "toi, avec la CNSS", "à poser"),
        ("Reporter dans README, requête, fiches T32 et T41, fiche de lecture, regles_prestations, "
         "points_a_valider", "Claude, sur demande", "non commencé"),
    ],
    "sources": (
        "Sources : recueil des besoins v7 (besoins_v7.txt et formules du document Word) ; atelier "
        "de Kaya 2017 ; Annuaire CIPRES p. 13, 14, 37 à 41, 62, 65 ; annuaire statistique de la "
        "CNSS 2024 ; classeur « Evaluation des Ratios CIPRES 2025 », feuilles 03 à 06 (titres, "
        "colonnes et normes seulement) ; lots 1, 2, 3, 5, 6, 7, 8 et comparaisons A01, A02 (copie "
        "de la base au 02/02/2026). Dossier de la famille et sa contre-vérification "
        "(style_grh/travail). Détail : analyses/prestations/regles_t32_t41_beneficiaires.md."
    ),
}


def build():
    d = construire(DATA)
    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
