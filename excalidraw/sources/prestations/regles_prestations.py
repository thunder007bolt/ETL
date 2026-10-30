"""
Les règles du domaine Prestations (T25 à T57) trouvées dans les données, avec leurs preuves
(état au 30/10/2026).

Une feuille pour s'approprier tout le travail fait du 26 au 30/10/2026 : ce que chaque lot de
requêtes a cherché, ce que la base nous a appris, les règles de calcul proposées, ce qui est faux
dans les requêtes DTM actuelles, les questions encore ouvertes, et l'index des 58 requêtes. Chaque
carte renvoie aux requêtes qui la prouvent (P01 à P58), rangées dans analyses/prestations.

Sources : analyses/prestations (README, TODO, lots 1, 2, 3 A et 3 B, requêtes proposées),
reference_cipres_T25_T57.md (hypothèses H1 à H14), fiche T25.

Usage :
    python3 excalidraw/sources/prestations/regles_prestations.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402
from fiche_cipres import BADGE_H, WIDTH, badge, badge_width, height_of, wrap  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "regles_prestations.excalidraw"
INNER = WIDTH - 50

# état d'une règle : (étiquette, fond, bord, ce qu'il veut dire)
STATES = {
    "prouve": ("Prouvé par les données", C["vert_moyen"], C["vert"],
               "les requêtes le montrent ; un libellé reste parfois à confirmer"),
    "propose": ("Proposition BI, à valider", C["jaune"], C["jaune_bord"],
                "notre choix, cohérent avec les données : la direction des prestations doit le viser"),
    "ouvert": ("Question ouverte", C["orange_moyen"], C["orange"],
               "on ne peut pas trancher sans la DSI ou le métier"),
}
# verdict d'une hypothèse sur les requêtes DTM actuelles
VERDICTS = {
    "Confirmée": (C["rouge_moyen"], C["rouge"]),
    "Confirmée en partie": (C["rouge_clair"], C["rouge"]),
    "Nuancée": (C["jaune"], C["jaune_bord"]),
    "Écartée": (C["gris"], C["gris_bord"]),
    "Conception": (C["gris"], C["gris_bord"]),
    "Tranchée": (C["vert_moyen"], C["vert"]),
}

HOW_TO = ("Chaque carte est une règle ou un constat. Elle dit la règle en clair, la preuve (le "
          "chiffre qui la fonde), les requêtes qui la prouvent, les tableaux CIPRES touchés et "
          "son état.\n"
          "Une requête Pnn se retrouve dans son fichier (partie 6) : chercher « -- P48 — ». Son "
          "en-tête dit l'objectif, le résultat et le constat.\n"
          "Ordre de lecture : l'essentiel (en bref), le chemin parcouru (1), ce que la base nous "
          "a appris (2), les règles de calcul (3), ce qui est faux dans les DTM (4), les "
          "questions ouvertes (5).")

KEY_POINTS = [
    "Les requêtes DTM actuelles du domaine se trompent sur des points graves : elles ne gardent "
    "que les dossiers payés, lisent « fermé » comme « liquidé », gonflent les montants 3,8 fois "
    "et comptent le tuteur au lieu de l'orphelin (partie 4). Tout est recalculé depuis la source.",
    "Un paiement est un débours de type PM au statut D ou P ; son montant est celui du débours, "
    "jamais le net à payer de l'effet (R3).",
    "T25 compte des demandes, pas des dossiers : la réversion d'une veuve est une demande à part, "
    "même ajoutée au dossier du défunt (D2). Sans cette règle, environ 900 réversions de 2025 "
    "disparaissent.",
    "La base est une copie figée au 02/02/2026. Beaucoup de statuts et de codes n'ont pas de "
    "libellé : nous les avons déduits des données, la DSI doit les confirmer (partie 5).",
    "Les cartes jaunes sont nos choix : elles attendent l'avis de la direction des prestations. "
    "La fiche T25 (dossier « tableaux cipres ») sert à le recueillir, tableau par tableau.",
]

LOTS = [
    ("Lot 1", "26/10/2026", "lot1_inventaire_prestations.sql", "P01 à P19",
     "Faire l'inventaire de la base (volumes, codes, statuts) et tester 14 hypothèses d'erreur "
     "sur les requêtes DTM.",
     "Base figée au 02/02/2026 ; H1, H4, H6, H11, H13 confirmées ; M = maternité."),
    ("Lot 2", "26/10/2026", "lot2_statuts_stocks_nouveaux.sql", "P20 à P30",
     "Savoir ce qu'est un paiement, qui est pensionné au 31/12, qui est un nouveau pensionné, et "
     "si les mois cotisés sont fiables.",
     "Paiement = statut D ou P ; règles R1 à R12 proposées."),
    ("Lot 3 A", "29/10/2026", "lot3_listes_de_codes.sql", "P31 à P47",
     "Lire toutes les listes de codes du domaine avec leurs libellés.",
     "Libellés sûrs des 28 codes ; accessoires de pension ; géographie."),
    ("Lot 3 B", "30/10/2026", "lot3b_controles_t25.sql", "P48 à P58",
     "Répondre dans les données aux cinq questions de la fiche T25.",
     "Réversion dans le dossier du défunt ; stock en instance faussé ; T25 par demande."),
]

PRODUCED = [
    ("Requêtes proposées", "t25_requete_proposee.sql : T25, par demande (revue le 30/10)\n"
                           "dtm_dossier_requete_proposee.sql : DTM_DOSSIER (à aligner)\n"
                           "t25_q4_deux_cas.sql : Q4 sous ses deux réponses\n"
                           "Aucune n'est encore exécutée."),
    ("Schémas Excalidraw", "fiche T25 (tableaux cipres) : règles, SQL, questions\n"
                           "tables_prestations : tables, liens, codes\n"
                           "vie_dossier et modele_conceptuel_dossier :\n"
                           "parcours d'un dossier, mis à jour le 30/10"),
    ("Docs pour la CNSS", "Prestations sociales (T25–T57) : constats, règles, questions\n"
                          "Règles de calcul des tableaux des prestations\n"
                          "Guide du métier · artefact Tables des prestations"),
    ("Suivi", "analyses/prestations/README.md : index des lots, constats, règles\n"
              "analyses/prestations/TODO.md : les 12 tâches et leur état\n"
              "resultats/ : les listes longues en TSV"),
]

FACTS = [
    dict(n="B1", titre="Une copie figée de la base",
         regle="La base des prestations est une copie arrêtée au 02/02/2026. L'entrepôt (DWH) en "
               "est la copie complète.",
         preuve="dernière saisie le 02/02/2026 pour 10 tables ; volumes du DWH égaux à ceux de "
                "la source",
         p=["P01", "P17"], tableaux="tous", etat="prouve"),
    dict(n="B2", titre="Les quatre branches",
         regle="V = pensions (PVID), A = accidents du travail et maladies professionnelles, F = "
               "prestations familiales, M = maternité, rattachée aux prestations familiales.",
         preuve="aucune prestation en nature en M : M n'est pas « maladie », comme le dit le DTM",
         p=["P02", "P05", "P31"], tableaux="tous", etat="prouve"),
    dict(n="B3", titre="Le statut d'un dossier",
         regle="A veut dire actif, F fermé. Aucun ne veut dire « en instance » ni « liquidé ».",
         preuve="61 632 dossiers de pension sont actifs ; en 2025, 310 dossiers au statut F pour "
                "15 479 notifiés",
         p=["P02", "P10"], tableaux="T25 à T27", etat="prouve"),
    dict(n="B4", titre="Le statut d'une prestation",
         regle="A active, F fermée (avec une date de radiation), S suspendue (avec une date de "
               "suspension).",
         preuve="statut et dates concordent pour tous les codes",
         p=["P24"], tableaux="T32 à T49", etat="prouve"),
    dict(n="B5", titre="Le statut d'un paiement",
         regle="D = payé par un effet décaissé ; P = payé sans effet, par un autre circuit ; V = "
               "validé mais pas décaissé ; N, A, S = jamais approuvés.",
         preuve="2025 : D 53,72 Md, P 7,63 Md ; seul D va avec un effet décaissé ; le circuit P "
                "existe depuis 2019",
         p=["P20", "P21", "P22", "P23"], tableaux="T31, T50 à T56", etat="prouve"),
    dict(n="B6", titre="Le type de pension",
         regle="Il vient du code de la prestation (28 codes), pas de la catégorie du dossier, qui "
               "dit seulement ordinaire, coordination ou transfert.",
         preuve="28 codes au libellé sûr ; 120 518 dossiers sans catégorie, lus comme ordinaires",
         p=["P03", "P04", "P33", "P34"], tableaux="tous", etat="prouve"),
    dict(n="B7", titre="Les étapes d'une prestation",
         regle="Ce sont les calculs du montant (calcul initial, révision, majoration), pas les "
               "événements de la vie (suspension, décès).",
         preuve="13 types d'étape ; un calcul initial par prestation",
         p=["P12", "P40"], tableaux="T32 à T49", etat="prouve"),
    dict(n="B8", titre="Deux sortes de demandes au guichet",
         regle="NOUVEAU : une nouvelle demande, qui ouvre son dossier. ANCIEN : une demande "
               "déposée sur un dossier existant, presque toujours une réversion.",
         preuve="51 417 NOUVEAU, 4 018 ANCIEN (616 en 2025) ; 94 % des ANCIEN donnent une pension "
                "de survivant",
         p=["P52", "P58"], tableaux="T25 à T27", etat="prouve"),
    dict(n="B9", titre="La réversion",
         regle="Quand un pensionné décède, la pension de sa veuve et de ses orphelins est "
               "ajoutée à son dossier. Un assuré mort avant sa retraite laisse sans doute un "
               "nouveau dossier de survivants.",
         preuve="13 104 des 25 541 pensions de veuve sont dans le dossier du défunt ; une seconde "
                "notification arrive en médiane 9 ans après la première",
         p=["P51", "P54", "P58"], tableaux="T25, T32 à T36", etat="prouve"),
    dict(n="B10", titre="Le dossier est complet au guichet",
         regle="Les pièces sont saisies le jour de la réception : la date de réception est celle "
               "du dossier complet.",
         preuve="96 % des réceptions de 2025 ; un rendez-vous est fixé 45 jours après, en médiane",
         p=["P49"], tableaux="T25 à T27", etat="prouve"),
    dict(n="B11", titre="Les trous des anciennes données",
         regle="Les anciens dossiers n'ont pas de lien vers leur réception. Environ 8 000 vieilles "
               "pensions radiées n'ont pas de date de notification. La date de notification du "
               "dossier est toujours vide.",
         preuve="30 774 dossiers sans catégorie pour 35 réceptions ; 13 149 réceptions d'avant "
                "2016 sans dossier",
         p=["P48", "P50", "P54", "P57"], tableaux="T25 à T27", etat="prouve"),
    dict(n="B12", titre="Les refus",
         regle="Ils ne sont notés que dans le module des rejets, avec un motif codé, et rarement. "
               "Ni le statut de la réception ni la fermeture du dossier ne valent refus.",
         preuve="146 rejets de pension depuis 2017 : prescription, pas de bénéficiaire, pièce non "
                "conforme, autre",
         p=["P09", "P55", "P56", "P57"], tableaux="T25 à T27", etat="prouve"),
    dict(n="B13", titre="Les contrôles a posteriori",
         regle="Presque absents, et plus saisis depuis juillet 2025. Le drapeau DEB_VERIFIE "
               "vérifie la saisie : ce n'est pas un contrôle.",
         preuve="115 contrôles en 2025 pour 155 206 dossiers payés : 0,07 %, pour une norme d'au "
                "moins 5 %",
         p=["P06", "P13"], tableaux="T28 à T30", etat="prouve"),
    dict(n="B14", titre="Les personnes",
         regle="Sexe et date de naissance sont fiables, les décès saisis en retard. Pour un "
               "orphelin, la personne payée est le tuteur, jamais l'orphelin.",
         preuve="deux fois moins de décès saisis en 2025 ; 29 689 pensions d'orphelin, toutes "
                "payées à un tiers",
         p=["P15", "P16"], tableaux="T32 à T49", etat="prouve"),
    dict(n="B15", titre="Les paiements sans effet",
         regle="62 % des paiements de prestations familiales de 2025 n'ont pas d'effet de "
               "paiement : la personne se lit sur la prestation, pas sur l'effet.",
         preuve="tous les paiements sont reliés à leur prestation",
         p=["P07", "P20", "P23"], tableaux="T41, T54 à T56", etat="prouve"),
    dict(n="B16", titre="Les mois cotisés",
         regle="Ce sont ceux de l'assuré, recopiés sur chaque prestation du dossier. 999 est une "
               "valeur par défaut ; le détail obligatoire / volontaire ne s'additionne pas.",
         preuve="des allocations de vieillesse vont jusqu'à 489 mois (à vérifier)",
         p=["P29", "P30"], tableaux="T42, T50", etat="prouve"),
    dict(n="B17", titre="Les tranches d'âge",
         regle="La base a 21 tranches de 5 ans. Les 11 grilles de l'Annuaire s'obtiennent en les "
               "regroupant.",
         preuve="toutes les bornes CIPRES sont des multiples de 5",
         p=["P14", "P42"], tableaux="T32 à T49", etat="prouve"),
]

RULE_GROUPS = [
    ("3.1 Les dossiers (T25 à T27) : établies pour T25, à étendre à T26 et T27", [
        dict(n="D1", titre="Les dossiers concernés",
             regle="Les dossiers de la branche, y compris ceux de coordination payés par une "
                   "autre caisse, que la CNSS reçoit et instruit. Sans les dossiers transférés à "
                   "une autre caisse.",
             preuve="coordination payée ailleurs : 76 % reçus au guichet de la CNSS, 238 dépôts et "
                    "292 liquidations en 2025 ; transférés : rien en 2025",
             p=["P03", "P48"], tableaux="T25 à T27", etat="propose",
             note="à valider : question Q1 de la fiche T25"),
        dict(n="D2", titre="On compte des demandes, pas des dossiers",
             regle="Une demande = un dossier × une ligne du tableau. Le dossier d'un pensionné "
                   "décédé porte deux demandes : sa pension et la réversion de sa veuve et de "
                   "ses orphelins.",
             preuve="en 2025, 675 des 800 pensions de veuve notifiées ne sont pas la première "
                    "notification de leur dossier",
             p=["P51", "P52", "P54", "P58"], tableaux="T25 à T27", etat="prouve",
             note="à confirmer : question Q4 de la fiche T25"),
        dict(n="D3", titre="Déposé",
             regle="Une réception au guichet = un dépôt. NOUVEAU va à la demande du dossier "
                   "qu'elle ouvre, ANCIEN à la réversion du dossier visé. Une réversion non "
                   "enregistrée au guichet prend la date de demande de sa pension.",
             preuve="4 671 réceptions de pension en 2025 : 4 055 NOUVEAU, 616 ANCIEN ; une "
                    "réception n'ouvre qu'un dossier, sauf 11",
             p=["P09", "P52", "P58"], tableaux="T25 à T27", etat="propose",
             note="remplace R10"),
        dict(n="D4", titre="Liquidé",
             regle="Une demande est liquidée à la première notification de ses droits, l'année de "
                   "cette notification. Jamais d'après le statut du dossier.",
             preuve="2025 : 3 901 dossiers + environ 900 réversions ≈ 4 800 liquidations ; le "
                    "statut F n'en trouvait que 310",
             p=["P10", "P27", "P48", "P54"], tableaux="T25 à T27", etat="prouve",
             note="remplace R11"),
        dict(n="D5", titre="La durée de liquidation",
             regle="Jours entre le dépôt et la première notification (négatif = 0). Colonnes : "
                   "moins de 15 jours, 15 à 45, plus de 45. Moyenne comparée à la norme de 45 "
                   "jours, et médiane.",
             preuve="2025 : médiane 62 j, moyenne 82 j ; 18 %, 22 % et 60 % des dossiers",
             p=["P10", "P49"], tableaux="T25 à T27", etat="propose",
             note="colonnes et norme : Annuaire p. 11, 34, 64"),
        dict(n="D6", titre="En instance au 31/12",
             regle="Déposé, ni liquidé ni rejeté au 31/12. Sans les dossiers fermés jamais "
                   "notifiés, ni les réceptions d'avant 2016 jamais reliées à un dossier.",
             preuve="sans ces exclusions : 23 218 demandes en instance fin 2025, cinq fois les "
                    "dépôts d'une année",
             p=["P50", "P57", "P58"], tableaux="T25 à T27", etat="propose",
             note="remplace R12 ; date de 2016 : question Q3 de la fiche T25"),
        dict(n="D7", titre="Rejeté",
             regle="Les refus enregistrés dans le module des rejets, comptés l'année du rejet.",
             preuve="14 rejets de pension en 2025 ; aucun refus noté ailleurs",
             p=["P09", "P55", "P56"], tableaux="T25 à T27", etat="prouve",
             note="la saisie est-elle complète ? question Q5 de la fiche T25"),
        dict(n="D8", titre="Le Total et le taux restant à traiter",
             regle="Le Total recalcule la durée sur toutes les demandes. Taux = en instance au "
                   "31/12 ÷ (en instance au 1er janvier + déposés de l'année).",
             preuve="une moyenne des moyennes des lignes serait fausse ; l'atelier de Kaya cite "
                    "le taux sans formule",
             p=[], tableaux="T25 à T27", etat="propose"),
    ]),
    ("3.2 Les paiements et les montants (T31, T50 à T56)", [
        dict(n="R3", titre="Ce qu'est un paiement",
             regle="Un paiement = un débours de type PM au statut D ou P. Son montant est celui "
                   "du débours, jamais le net à payer de l'effet.",
             preuve="61,35 Md payés en 2025 ; le net à payer répété sur chaque débours gonfle le "
                    "DTM 3,8 fois (222,9 Md pour 58,5 Md)",
             p=["P07", "P08", "P20", "P21", "P36"], tableaux="T31, T32 à T56", etat="prouve",
             note="sens des statuts D et P à confirmer par la DSI"),
        dict(n="R4", titre="Le mois d'un paiement",
             regle="Le mois de DEB_DATE_EFFET : le mois payé, pas la date du paiement.",
             preuve="débours préparés environ 3 semaines avant l'échéance ; 9 % sont des rappels "
                    "saisis 2 mois ou plus après",
             p=["P20"], tableaux="T31, T50 à T56", etat="propose"),
    ]),
    ("3.3 Les personnes et les pensionnés (T32 à T49)", [
        dict(n="R5", titre="La personne comptée",
             regle="L'ayant droit de la prestation. Pour les allocations familiales, "
                   "l'allocataire, c'est-à-dire la personne payée.",
             preuve="orphelins : la personne payée est toujours le tuteur, que le DTM compte à tort",
             p=["P16"], tableaux="T32 à T49", etat="propose"),
        dict(n="R6", titre="Le pensionné au 31 décembre",
             regle="Pension active au 31/12 (effet avant, ni radiée ni suspendue) et payée au 4e "
                   "trimestre. Une personne comptée une fois.",
             preuve="vieillesse : 32 974 pensions actives, 29 643 payées ; orphelins : 8 447 "
                    "actives sans aucun paiement",
             p=["P24", "P25", "P26"], tableaux="T32 à T41", etat="propose"),
        dict(n="R7", titre="Le nouveau pensionné",
             regle="Pension notifiée dans l'exercice. Une personne comptée une fois.",
             preuve="notification, saisie, calcul initial et premier paiement donnent le même "
                    "nombre ; un tiers des pensions ont un effet antérieur à l'année",
             p=["P27", "P28"], tableaux="T42 à T49", etat="propose"),
        dict(n="R8", titre="L'âge",
             regle="Année de l'exercice moins année de naissance ; tranches de 5 ans regroupées "
                   "selon la grille de chaque tableau.",
             preuve="naissance fiable ; 21 tranches de 5 ans dans la base",
             p=["P14", "P15", "P42"], tableaux="T32 à T49", etat="propose"),
        dict(n="R9", titre="Les services passés",
             regle="Mois cotisés de la dernière prestation de la personne ; valeurs au-dessus de "
                   "600 écartées ; total seulement.",
             preuve="les mois sont recopiés sur chaque prestation : les additionner les gonfle",
             p=["P29", "P30"], tableaux="T42, T50", etat="propose"),
        dict(n="A1", titre="Les accessoires de pension",
             regle="Bonification ou majoration pour enfant, contribution aux charges du ménage : "
                   "elles s'ajoutent à une pension. Elles ne font pas un pensionné de plus et "
                   "suivent la ligne de la pension.",
             preuve="bonification pour enfant (RBEC) : 68 836 prestations",
             p=["P34"], tableaux="T25, T32 à T49", etat="propose"),
    ]),
    ("3.4 Les branches et les lignes CIPRES (tous les tableaux)", [
        dict(n="R1", titre="Les branches",
             regle="V → PVID ; A → AT/MP ; F et M → prestations familiales.",
             preuve="M = maternité : aucune prestation en nature en M",
             p=["P02", "P05", "P31"], tableaux="tous", etat="prouve"),
        dict(n="R2", titre="La ligne CIPRES",
             regle="D'après le code de la prestation, selon la correspondance ci-dessous.",
             preuve="libellés sûrs des 28 codes ; retraite anticipée et accessoires à viser",
             p=["P04", "P34"], tableaux="tous", etat="propose"),
    ]),
    ("3.5 Les indus (T31) et les contrôles (T28 à T30)", [
        dict(n="I1", titre="Les indus",
             regle="Trop perçus et moins perçus d'après le type d'ajustement. Un remboursement de "
                   "trop perçu n'est pas un nouvel indu ; avances, mois gratuits et dons COVID "
                   "sont écartés ; la branche maternité est incluse.",
             preuve="le DTM compte le remboursement de trop perçu et oublie la maternité",
             p=["P11", "P41"], tableaux="T31", etat="propose",
             note="norme CIPRES : 2 % d'indus au plus"),
        dict(n="C1", titre="Les contrôles a posteriori",
             regle="Un contrôle se lit dans la table des dossiers contrôlés, jamais dans le "
                   "drapeau DEB_VERIFIE. Taux = dossiers contrôlés ÷ dossiers payés.",
             preuve="0,07 % en 2025, et plus aucune saisie depuis juillet 2025",
             p=["P06", "P13"], tableaux="T28 à T30", etat="ouvert",
             note="où sont saisis les contrôles depuis juillet 2025 ? (DSI)"),
    ]),
]

CORRESPONDANCE = [
    ("T25, T28, T32", "Pension de vieillesse", "RPV ; RPRA, retraite anticipée (à confirmer)"),
    ("T25, T28, T33", "Pension d'invalidité", "RPI"),
    ("T25, T28, T34", "Pension de veuf / veuve", "RPSV, RPSF"),
    ("T25, T28, T35", "Pension d'orphelin", "RPSE"),
    ("T28, T36", "Pension d'ascendant", "RPA"),
    ("T25, T28, T50", "Allocation de vieillesse", "RAV"),
    ("T25, T28, T51", "Allocation de survivants", "RAS, RASC, RASO, RASA"),
    ("T25, T28", "Autres prestations de la branche",
     "accessoires RBEC, RME, RME-COL, RCCM ; remboursement de cotisations RRC"),
    ("T26, T29, T37, T48", "Rente viagère", "ARI"),
    ("T26, T29, T38", "Rente de veuf / veuve", "ARSV, ARSF"),
    ("T26, T29, T39", "Rente d'orphelin", "ARSE"),
    ("T26, T29, T40", "Rente d'ascendant", "ARSA"),
    ("T26, T29, T52", "Indemnités journalières AT/MP", "AIJ"),
    ("T26, T29, T53", "Allocation d'incapacité", "AAI"),
    ("T26, T29", "Autres prestations de la branche", "prestations en nature (frais médicaux)"),
    ("T27, T30, T41", "Allocations familiales", "FAF"),
    ("T27, T30, T54", "Indemnités journalières de maternité", "MIJ"),
    ("T27, T30, T55", "Allocations prénatales", "FAP, MAP"),
]

HYPOTHESES = [
    ("H1", "DTM_DOSSIER ne garde que les dossiers payés", "Confirmée",
     "2025 : 33 % des dossiers AT/MP ouverts, 61 % en PF, 83 % en pensions", ["P09"]),
    ("H2", "Un dossier compte l'année de son ouverture, pas de sa liquidation", "Tranchée",
     "chaque événement compte à sa date (D3, D4, D6)", []),
    ("H3", "Un délai nul ou négatif sort des tranches", "Écartée",
     "6 délais nuls, aucun négatif en 2025", ["P10"]),
    ("H4", "Le statut F est lu comme « liquidé »", "Confirmée",
     "F = fermé : 310 « liquidés » pour 15 479 notifiés", ["P02", "P10"]),
    ("H5", "La branche M serait « maladie »", "Tranchée",
     "M = maternité, branche PF", ["P02", "P05"]),
    ("H6", "Le net à payer est répété sur chaque débours", "Confirmée",
     "montants gonflés 3,8 fois", ["P08"]),
    ("H7", "Les débours annulés comptent comme payés", "Écartée",
     "aucun paiement annulé ; mais le statut n'est pas filtré", ["P07"]),
    ("H8", "Des bénéficiaires distincts par mois ne s'additionnent pas", "Conception",
     "il faut un effectif au 31/12 (R6)", []),
    ("H9", "Une seule grille de tranches d'âge", "Nuancée",
     "21 tranches de 5 ans à regrouper (R8)", ["P14"]),
    ("H10", "Mois cotisés additionnés à chaque paiement", "Confirmée en partie",
     "mois recopiés par prestation (R9)", ["P29", "P30"]),
    ("H11", "Le taux de contrôle ne se calcule pas", "Confirmée",
     "0,07 % en 2025 ; DEB_VERIFIE n'est pas un contrôle", ["P13"]),
    ("H12", "Tout ajustement non trop perçu = moins perçu", "Confirmée en partie",
     "sauf REM-TP ; maternité exclue à tort", ["P11"]),
    ("H13", "La personne comptée est celle qui est payée", "Confirmée",
     "orphelins : le tuteur est compté", ["P16"]),
    ("H14", "DIM_TYPE_PRESTATION n'est pas chargée", "Écartée",
     "elle existe, faite à la main", ["P18"]),
]

QUESTIONS_METIER = [
    "Fiche T25, Q1 à Q5 : coordination, date du dépôt, vieilles demandes, réversion, refus",
    "Pensionné au 31/12 : pension active, ou payée en décembre ? (R6 · P25)",
    "Nouveau pensionné : date de notification ? (R7 · P27)",
    "Orphelins : compter l'orphelin et non le tuteur payé ? (R5 · P16)",
    "Retraite anticipée et accessoires : dans quelle ligne ? (R2, A1 · P34)",
    "T57 « décédés » : bénéficiaires décédés, ou assurés décédés ? (atelier de Kaya)",
    "Indus : le remboursement de trop perçu, les avances, les mois gratuits ? (I1 · P11)",
    "Un paiement compte-t-il au mois payé ou à la date du paiement ? (R4 · P20)",
    "8 447 pensions d'orphelin actives sans paiement : radier ? (P25)",
    "69 pensionnés payés de 100 ans ou plus, 215 actifs décédés : contrôle d'existence ? (P26)",
]
QUESTIONS_DSI = [
    "Le sens des statuts sans libellé : dossier, prestation, débours, effet, réception, "
    "notification, ajustement (B3 à B5 · P06)",
    "Le lien du déposant L, V, T, F, A et le statut A des réceptions depuis mai 2025 (P53, P55)",
    "Pourquoi 62 % des paiements PF sont-ils sans effet ? Quel est le circuit P ? (B15 · P23)",
    "Où sont saisis les contrôles a posteriori depuis juillet 2025 ? (C1 · P13)",
    "Que veulent dire les drapeaux *_VERIFIE ? (P06)",
    "La valeur 999 des mois cotisés (B16 · P30)",
    "TSINISTRE n'est plus alimentée depuis 12/2024 ; quand la base sera-t-elle rafraîchie ? (P01)",
    "Lieux de paiement : DR_OU_SP vide, lieu 201 « caisse tenkodogo », CNSS-DCP (P47)",
    "120 518 dossiers sans catégorie : tous ordinaires ? (B6 · P33)",
]

INDEX = [
    ("Lot 1 · 26/10/2026", "lot1_inventaire_prestations.sql", [
        ("P01", "Volumes des tables → base figée au 02/02/2026"),
        ("P02", "Types de dossier, statuts → M = maternité ; A actif, F fermé"),
        ("P03", "Catégories de dossier → ne donnent pas le type de pension"),
        ("P04", "Codes en espèces → correspondance code → ligne CIPRES"),
        ("P05", "Prestations en nature 2025 → aucune en M : M n'est pas « maladie »"),
        ("P06", "Valeurs des statuts → aucun libellé ; DEB_VERIFIE n'est pas un contrôle"),
        ("P07", "Débours 2025 par branche → 62 % des paiements PF sans effet"),
        ("P08", "Net à payer répété → montants du DTM gonflés 3,8 fois"),
        ("P09", "Dossiers à chaque étape → déposé = réception ; le DTM perd des dossiers"),
        ("P10", "Durée de liquidation 2025 → calculable ; médiane 62 j en pensions"),
        ("P11", "Ajustements → REM-TP n'est pas un indu ; maternité oubliée"),
        ("P12", "Types d'étape → calculs du montant, pas événements"),
        ("P13", "Contrôles a posteriori → 0,07 % en 2025, plus saisis depuis 07/2025"),
        ("P14", "Tranches d'âge → 21 tranches de 5 ans à regrouper"),
        ("P15", "Qualité des individus → sexe et naissance fiables ; décès en retard"),
        ("P16", "Personne comptée → orphelins : le DTM compte le tuteur"),
        ("P17", "Faits du DWH → copie complète de la source"),
        ("P18", "Tables DTM → DIM_TYPE_PRESTATION faite à la main"),
        ("P19", "Clichés des DTM → abandonnée : contenu DTM non utilisé"),
    ]),
    ("Lot 2 · 26/10/2026", "lot2_statuts_stocks_nouveaux.sql", [
        ("P20", "Débours 2025 par statut → D et P = vrais paiements, 61,35 Md"),
        ("P21", "Débours et effet → seul D va avec un effet décaissé"),
        ("P22", "Statuts 2019–2025 → P est un circuit stable depuis 2019"),
        ("P23", "Paiements avec et sans effet → sans effet = circuit P"),
        ("P24", "État des prestations → statut fiable : A, F, S"),
        ("P25", "Pensionnés au 31/12/2025 → dates et paiements concordent, sauf orphelins"),
        ("P26", "Esquisse de T32 → calculable depuis la source"),
        ("P27", "Nouveaux de 2025 selon la date → la notification marque l'entrée"),
        ("P28", "Délais d'une pension → payée 4 j après la notification"),
        ("P29", "Mois cotisés par dossier → ceux de l'assuré, recopiés"),
        ("P30", "Qualité des mois cotisés → 999 par défaut ; total seulement"),
    ]),
    ("Lot 3 A · 29/10/2026", "lot3_listes_de_codes.sql", [
        ("P31", "Types de dossier → M rattachée aux PF"),
        ("P32", "Branches → toutes ont un type de dossier"),
        ("P33", "Catégories de dossier → 120 518 dossiers sans catégorie"),
        ("P34", "Codes en espèces (28) → libellés sûrs ; accessoires"),
        ("P35", "Prestations en nature → 51 types ; maternité jamais payée"),
        ("P36", "Types de débours → seul PM est un paiement"),
        ("P37", "Types d'effet → virement et espèces : 98 %"),
        ("P38", "Modes de paiement → 7 modes, libellés connus"),
        ("P39", "Fréquences → mensuelle et trimestrielle"),
        ("P40", "Types d'étape → 512 prestations sans calcul initial"),
        ("P41", "Types d'ajustement → codes COVID, pas des indus"),
        ("P42", "Tranches d'âge → conforme à P14"),
        ("P43", "Types d'accident → liste"),
        ("P44", "Types de pièce → 86 types (resultats/)"),
        ("P45", "Directions régionales → 6, le siège inactif"),
        ("P46", "Services provinciaux → 20 (resultats/)"),
        ("P47", "Lieux de paiement → 350 ; lieu 201 mal rattaché"),
    ]),
    ("Lot 3 B · 30/10/2026", "lot3b_controles_t25.sql", [
        ("P48", "Coordination (Q1) → la CNSS l'instruit : à garder"),
        ("P49", "Date du dépôt (Q2) → 96 % des pièces le jour même"),
        ("P50", "Stock en instance (Q3) → 23 218, faussé par de vieilles réceptions"),
        ("P51", "Pensions de survivant (Q4) → la moitié dans le dossier du défunt"),
        ("P52", "Réception et dossier (Q4) → NOUVEAU ouvre, ANCIEN vise un dossier"),
        ("P53", "Lien du déposant (Q4) → codes L, V, T, F, A"),
        ("P54", "Notifications multiples (Q4) → la seconde est une réversion"),
        ("P55", "Statut de la réception (Q5) → ne note pas les refus"),
        ("P56", "Motifs de rejet (Q5) → refus codés, une quinzaine par an"),
        ("P57", "Dossiers fermés sans notification (Q5) → pensions radiées"),
        ("P58", "Suite des demandes ANCIEN (Q3, Q4) → 94 % : pension de survivant"),
    ]),
]


def tag(d, x, y, label, bg, stroke, size=16, groups=()):
    """Étiquette arrondie avec son texte ; renvoie sa largeur."""
    w = text_size(label, size)[0] + 24
    d.box(x, y, w, 30, label, size, bg=bg, stroke=stroke, groups=groups)
    return w


def p_chips(d, x, y, w, refs, groups=()):
    """Les requêtes d'une carte, en pastilles grises sur une ou plusieurs lignes ; renvoie le bas."""
    lx = x
    if not refs:
        d.text(x, y + 5, "aucune requête : choix de conception", 16, C["doux"], groups=groups)
        return y + 30
    for ref in refs:
        cw = text_size(ref, 16)[0] + 20
        if lx + cw > x + w:
            lx, y = x, y + 38
        d.box(lx, y, cw, 30, ref, 16, bg=C["gris"], stroke=C["gris_bord"], groups=groups)
        lx += cw + 8
    return y + 30


def _chips_height(w, refs):
    if not refs:
        return 30
    lx, rows = 0, 1
    for ref in refs:
        cw = text_size(ref, 16)[0] + 20
        if lx + cw > w:
            lx, rows = 0, rows + 1
        lx += cw + 8
    return rows * 30 + (rows - 1) * 8


def _layout(w, c):
    """Positions relatives des blocs d'une carte, et sa hauteur."""
    inner = w - 36
    bw = badge_width(c["n"])
    title = wrap(c["titre"], 22, inner - bw - 14)
    y = 14 + max(BADGE_H, height_of(title, 22) + 10) + 12
    rows = {"title": title, "meta": y}
    y += 30 + 14
    rows["regle"] = (y, wrap(c["regle"], 18, inner))
    y += height_of(rows["regle"][1], 18) + 12
    rows["preuve"] = (y, wrap("Preuve : " + c["preuve"], 16, inner))
    y += height_of(rows["preuve"][1], 16) + 12
    label_w = text_size("Requêtes :", 16)[0] + 10
    rows["p"] = (y, label_w)
    y += _chips_height(inner - label_w, c["p"]) + 12
    if c.get("note"):
        rows["note"] = (y, wrap("→ " + c["note"], 16, inner))
        y += height_of(rows["note"][1], 16) + 12
    return rows, y + 6


def card(d, x, y, w, h, c):
    rows, _ = _layout(w, c)
    g = d.group()
    label, bg, stroke, _ = STATES[c["etat"]]
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=stroke, stroke_width=3, groups=g)
    badge(d, x + 18, y + 14, c["n"], groups=g)
    d.text(x + 18 + badge_width(c["n"]) + 14, y + 22, rows["title"], 22, groups=g, inside=frame)
    my = y + rows["meta"]
    tw = tag(d, x + 18, my, label, bg, stroke, groups=g)
    d.text(x + 18 + tw + 14, my + 5, "Tableaux : " + c["tableaux"], 16, C["doux"], groups=g,
           inside=frame)
    ry, text = rows["regle"]
    d.text(x + 18, y + ry, text, 18, groups=g, inside=frame)
    py, text = rows["preuve"]
    d.text(x + 18, y + py, text, 16, C["doux"], groups=g, inside=frame)
    qy, label_w = rows["p"]
    d.text(x + 18, y + qy + 5, "Requêtes :", 16, C["doux"], groups=g, inside=frame)
    p_chips(d, x + 18 + label_w, y + qy, w - 36 - label_w, c["p"], groups=g)
    if "note" in rows:
        ny, text = rows["note"]
        d.text(x + 18, y + ny, text, 16, C["jaune_bord"] if c["etat"] != "prouve" else C["vert"],
               groups=g, inside=frame)


def cards(d, x, y, w, items, per_row=3, gap=24):
    """Cartes rangées par lignes, chaque ligne à la hauteur de sa plus haute carte ; renvoie le bas."""
    cw = (w - (per_row - 1) * gap) / per_row
    for start in range(0, len(items), per_row):
        row = items[start:start + per_row]
        h = max(_layout(cw, c)[1] for c in row)
        for i, c in enumerate(row):
            card(d, x + i * (cw + gap), y, cw, h, c)
        y += h + gap
    return y - gap


def banner(d, y):
    g = d.group()
    frame = d.box(25, y, INNER, 300, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(45, y + 16, "Comment lire cette feuille", 22, groups=g, inside=frame)
    d.text(45, y + 54, wrap(HOW_TO, 18, 1240), 18, groups=g, inside=frame)
    d.text(1360, y + 16, "L'état de chaque règle (couleur du cadre)", 22, groups=g, inside=frame)
    cy = y + 56
    for label, bg, stroke, meaning in STATES.values():
        tw = tag(d, 1360, cy, label, bg, stroke, groups=g)
        d.text(1360 + tw + 12, cy + 5, wrap(meaning, 16, 1170 - tw - 12), 16, groups=g,
               inside=frame)
        cy += 56
    d.text(1360, cy + 4, "Les pastilles grises P01 à P58 sont les requêtes qui prouvent la règle.",
           16, C["doux"], groups=g, inside=frame)
    return y + 300


def key_points(d, y):
    g = d.group()
    lines = [wrap(f"{i}. {p}", 18, INNER - 60) for i, p in enumerate(KEY_POINTS, 1)]
    h = 60 + sum(height_of(t, 18) + 10 for t in lines) + 10
    frame = d.box(25, y, INNER, h, bg=C["blanc"], stroke=C["encre"], stroke_width=3, groups=g)
    d.text(45, y + 16, "En bref : ce qu'il faut retenir", 26, groups=g, inside=frame)
    ty = y + 60
    for t in lines:
        d.text(45, ty, t, 18, groups=g, inside=frame)
        ty += height_of(t, 18) + 10
    return y + h


def journey(d, y):
    """Partie 1 : les quatre lots, puis ce qu'ils ont produit."""
    gap = 60
    w = (INNER - 3 * gap) / 4
    texts = [wrap(purpose, 18, w - 36) for *_, purpose, _ in LOTS]
    finds = [wrap("Trouvé : " + found, 16, w - 36) for *_, found in LOTS]
    h = 150 + max(height_of(t, 18) for t in texts) + 16 + max(height_of(t, 16) for t in finds) + 20
    boxes = []
    for i, ((name, day, path, refs, _, _), text, found) in enumerate(zip(LOTS, texts, finds)):
        x = 25 + i * (w + gap)
        g = d.group()
        frame = d.box(x, y, w, h, bg=C["bleu_clair"], stroke=C["bleu"], stroke_width=3, groups=g)
        d.text(x + 18, y + 14, f"{name} · {day}", 24, C["bleu"], groups=g, inside=frame)
        d.text(x + 18, y + 54, path, 16, groups=g, inside=frame, mono=True)
        d.text(x + 18, y + 84, refs, 20, groups=g, inside=frame)
        d.text(x + 18, y + 122, text, 18, groups=g, inside=frame)
        d.text(x + 18, y + 138 + height_of(text, 18), found, 16, C["doux"], groups=g, inside=frame)
        boxes.append(frame)
    for a, b in zip(boxes, boxes[1:]):
        d.arrow(a["x"] + a["width"] + 6, y + h / 2, b["x"] - 6, y + h / 2, start=a, end=b,
                color=C["bleu"])
    d.text(25, y + h + 24, "Tous les fichiers sont dans analyses/prestations. Ce que ces lots ont "
                           "produit :", 20, C["doux"])
    y += h + 70
    w2 = (INNER - 3 * 24) / 4
    bodies = [wrap(body, 16, w2 - 36) for _, body in PRODUCED]
    h2 = 60 + max(height_of(t, 16) for t in bodies) + 20
    for i, ((title, _), body) in enumerate(zip(PRODUCED, bodies)):
        x = 25 + i * (w2 + 24)
        g = d.group()
        frame = d.box(x, y, w2, h2, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
        d.text(x + 18, y + 14, title, 22, groups=g, inside=frame)
        d.text(x + 18, y + 54, body, 16, groups=g, inside=frame)
    return y + h2


def correspondance(d, y):
    """Table code de prestation → ligne CIPRES (règle R2)."""
    cols = [("Tableaux", 330), ("Ligne CIPRES", 620), ("Codes de prestation", INNER - 950)]
    g = d.group()
    rows = [(t, l, wrap(c, 18, cols[2][1] - 30)) for t, l, c in CORRESPONDANCE]
    heights = [max(44, height_of(c, 18) + 18) for _, _, c in rows]
    total = 50 + sum(heights)
    frame = d.box(25, y, INNER, total, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    x = 25
    for name, cw in cols:
        d.text(x + 15, y + 14, name, 18, C["doux"], groups=g, inside=frame)
        x += cw
    ry = y + 50
    for (t, l, c), rh in zip(rows, heights):
        d.arrow(25, ry, 25 + INNER, ry, color=C["gris_bord"], stroke_width=1, head=None)
        x = 25
        for value, (_, cw) in zip((t, l, c), cols):
            d.text(x + 15, ry + 9, value, 18, groups=g, inside=frame)
            x += cw
        ry += rh
    return y + total


def hypotheses(d, y):
    per_row, gap = 7, 16
    w = (INNER - (per_row - 1) * gap) / per_row
    items = []
    for n, text, verdict, detail, refs in HYPOTHESES:
        body = wrap(text, 16, w - 30)
        det = wrap(detail, 16, w - 30)
        items.append((n, body, verdict, det, refs))
    h = max(52 + height_of(b, 16) + 12 + 30 + 10 + height_of(t, 16) + 12 + _chips_height(w - 30, r)
            for _, b, _, t, r in items) + 16
    for i, (n, body, verdict, det, refs) in enumerate(items):
        x = 25 + (i % per_row) * (w + gap)
        top = y + (i // per_row) * (h + gap)
        g = d.group()
        bg, stroke = VERDICTS[verdict]
        frame = d.box(x, top, w, h, bg=C["blanc"], stroke=stroke, stroke_width=2, groups=g)
        d.text(x + 15, top + 12, n, 24, groups=g, inside=frame)
        d.text(x + 15, top + 52, body, 16, groups=g, inside=frame)
        ty = top + 52 + height_of(body, 16) + 12
        tag(d, x + 15, ty, verdict, bg, stroke, groups=g)
        ty += 40
        d.text(x + 15, ty, det, 16, C["doux"], groups=g, inside=frame)
        ty += height_of(det, 16) + 12
        p_chips(d, x + 15, ty, w - 30, refs, groups=g)
    rows = (len(items) + per_row - 1) // per_row
    return y + rows * h + (rows - 1) * gap


def questions(d, y):
    w = (INNER - 24) / 2
    blocks = []
    for title, items in [("À la direction des prestations", QUESTIONS_METIER),
                         ("À la DSI", QUESTIONS_DSI)]:
        lines = [wrap("• " + q, 18, w - 50) for q in items]
        blocks.append((title, lines))
    h = 70 + max(sum(height_of(t, 18) + 10 for t in lines) for _, lines in blocks) + 10
    for i, (title, lines) in enumerate(blocks):
        x = 25 + i * (w + 24)
        g = d.group()
        frame = d.box(x, y, w, h, bg=C["orange_clair"], stroke=C["orange"], groups=g)
        d.text(x + 20, y + 16, title, 24, C["orange"], groups=g, inside=frame)
        ty = y + 64
        for t in lines:
            d.text(x + 20, ty, t, 18, groups=g, inside=frame)
            ty += height_of(t, 18) + 10
    return y + h


def index(d, y):
    gap = 24
    w = (INNER - 3 * gap) / 4
    blocks = []
    for title, path, entries in INDEX:
        lines = [(ref, wrap(text, 16, w - 36 - 64)) for ref, text in entries]
        blocks.append((title, path, lines))
    h = 100 + max(sum(max(30, height_of(t, 16)) + 10 for _, t in lines)
                  for _, _, lines in blocks) + 10
    for i, (title, path, lines) in enumerate(blocks):
        x = 25 + i * (w + gap)
        g = d.group()
        frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["bleu"], groups=g)
        d.text(x + 18, y + 14, title, 22, C["bleu"], groups=g, inside=frame)
        d.text(x + 18, y + 52, path, 16, groups=g, inside=frame, mono=True)
        ty = y + 96
        for ref, text in lines:
            d.box(x + 18, ty, 54, 30, ref, 16, bg=C["gris"], stroke=C["gris_bord"], groups=g)
            d.text(x + 18 + 64, ty + 5, text, 16, groups=g, inside=frame)
            ty += max(30, height_of(text, 16)) + 10
    return y + h


def section(d, y, title, subtitle, build_part):
    sec = d.section(0, y, WIDTH, 0, title)
    top = y + 70
    if subtitle:
        text = wrap(subtitle, 18, INNER)
        d.text(25, top, text, 18, C["doux"])
        top += height_of(text, 18) + 20
    bottom = build_part(top)
    sec["height"] = bottom + 30 - y
    return bottom + 30 + 40


def build():
    d = Diagram(seed=58)
    d.text(0, 0, "Prestations (T25 à T57) : les règles trouvées dans les données, et leurs preuves",
           48)
    d.text(0, 66, "Tout ce que l'équipe BI a établi du 26 au 30/10/2026 : 4 lots, 58 requêtes, les "
                  "règles qui en sortent et ce qui reste à trancher · état au 30/10/2026", 22,
           C["doux"])
    y = banner(d, 120) + 30
    y = key_points(d, y) + 40

    y = section(d, y, "1. Le chemin parcouru : quatre lots de requêtes",
                "Chaque lot répond aux questions laissées par le précédent. Les requêtes sont en "
                "lecture seule et ne sortent que des comptages, jamais de données nominatives.",
                lambda top: journey(d, top))

    y = section(d, y, "2. Ce que la base nous a appris",
                "Des faits établis dans les données : ils fondent les règles de la partie 3. Les "
                "libellés déduits des données restent à confirmer par la DSI (partie 5).",
                lambda top: cards(d, 25, top, INNER, FACTS))

    def rules(top):
        for title, items in RULE_GROUPS:
            d.text(25, top, title, 26, C["bleu"])
            top = cards(d, 25, top + 50, INNER, items) + 40
            if title.startswith("3.4"):
                d.text(25, top - 10, "La correspondance proposée (règle R2), à faire viser par la "
                                     "direction des prestations :", 18, C["doux"])
                top = correspondance(d, top + 24) + 40
        return top - 40

    y = section(d, y, "3. Les règles de calcul proposées",
                "Les règles R1 à R12 viennent des lots 1 et 2 ; les règles D1 à D8 viennent de la "
                "fiche T25 et du lot 3 B et remplacent R10 à R12 pour les dossiers.", rules)

    y = section(d, y, "4. Les requêtes DTM actuelles : les 14 hypothèses d'erreur vérifiées",
                "Avant les règles, nous avons relu le code des requêtes DTM du domaine et listé 14 "
                "erreurs possibles (reference_cipres_T25_T57.md). Le contenu des tables DTM n'est "
                "pas analysé : il est jugé faux.", lambda top: hypotheses(d, top))

    y = section(d, y, "5. Les questions encore ouvertes",
                "Entre parenthèses : la règle concernée et la requête qui a soulevé la question.",
                lambda top: questions(d, top))

    section(d, y, "6. L'index des 58 requêtes : ce que chacune a montré",
            "Pour lire une requête : ouvrir son fichier dans analyses/prestations et chercher "
            "« -- Pnn — ». Les listes longues sont dans analyses/prestations/resultats.",
            lambda top: index(d, top))
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
