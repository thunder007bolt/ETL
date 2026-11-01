"""
Les règles du domaine GRH (T58 à T65) trouvées dans les données, avec leurs preuves (état au
01/11/2026).

Une feuille pour s'approprier le contrôle de la GRH par l'annuaire de la CNSS 2024 : ce que
chaque lot de requêtes a cherché, ce que l'annuaire publie face à ce que la base donne, les règles
de calcul proposées (G1 à G11), les questions à la DRH (QG1 à QG7), et l'index des requêtes Q97 à
Q117. Chaque carte renvoie aux requêtes qui la prouvent, rangées dans analyses/grh.

Sources : analyses/grh (README, TODO, lots 17 à 20), annuaire de la CNSS 2024 (partie IX,
tableaux 119 à 124), atelier de Kaya (2017).

Usage :
    python3 excalidraw/sources/grh/regles_grh.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402
from fiche_cipres import BADGE_H, WIDTH, badge, badge_width, height_of, wrap  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "regles_grh.excalidraw"
INNER = WIDTH - 50

# état d'une règle : (étiquette, fond, bord, ce qu'il veut dire)
STATES = {
    "prouve": ("Retrouvé dans l'annuaire", C["vert_moyen"], C["vert"],
               "la base redonne les chiffres publiés par la CNSS, à quelques agents près"),
    "propose": ("Proposition BI, à valider", C["jaune"], C["jaune_bord"],
                "notre choix, cohérent avec les données, mais l'annuaire ne le confirme pas : la "
                "DRH doit le viser"),
    "ouvert": ("Question ouverte", C["orange_moyen"], C["orange"],
               "la base ne permet pas de trancher : réponse de la DRH attendue"),
}
# verdict d'une ligne de l'annuaire face à la base
VERDICTS = {
    "Retrouvé": (C["vert_moyen"], C["vert"]),
    "Proche": (C["jaune"], C["jaune_bord"]),
    "Ne retombe pas": (C["rouge_moyen"], C["rouge"]),
}

HOW_TO = ("Chaque carte est une règle de calcul. Elle dit la règle en clair, la preuve (le chiffre "
          "de l'annuaire de la CNSS 2024 qu'elle retrouve), les requêtes qui la prouvent, les "
          "tableaux CIPRES touchés et son état.\n"
          "Une requête Qnn se retrouve dans son fichier (partie 5) : chercher « -- Q99 — ». Son "
          "en-tête dit l'objectif, le résultat et le constat.\n"
          "Ordre de lecture : l'essentiel (en bref), le chemin parcouru (1), l'annuaire face à la "
          "base (2), les règles (3), les questions à la DRH (4).")

KEY_POINTS = [
    "L'annuaire de la CNSS 2024 (partie IX, p. 89 à 92) publie T58, T60, T61, T62 et T65 pour le "
    "personnel permanent au 31/12/2024. Les lots 17 à 20 les ont refaits depuis la base GRH.",
    "La base retrouve l'effectif à 2 % près (742 permanents pour 758), et presque tout le reste : "
    "âge, ancienneté, catégorie, encadrement, recrutements, retraites, contractuels (partie 2).",
    "La catégorie CIPRES est celle de la grille de salaire de la situation (01-02 employés, 03 "
    "maîtrise, 04-05 cadres moyens, 06 cadres supérieurs), pas le niveau de la qualification que "
    "lisent les requêtes DTM (G3).",
    "84 agents engagés en 2019 n'ont aucune date de départ : la base les perd dès 2019, "
    "l'annuaire les garde jusqu'en 2023. Sans effet sur les exercices calculés à partir de 2025 "
    "(G11).",
    "Les formés de 2024 (T62) ne retombent pas : 409 agents dans la base pour 480 publiés (G10).",
    "Les cartes jaunes et orange attendent la DRH : sept questions (partie 4), chacune avec un "
    "réglage par défaut dans les requêtes. Elles ne bloquent rien.",
]

LOTS = [
    ("Lots 1 à 16", "21 au 26/10", "lot1 à lot16_*.sql", "Q01 à Q96",
     "Comprendre la base GRH : mouvements, décisions, effectif, formation, types d'agents, "
     "domaines ; corriger les requêtes DTM.",
     "Départs dans les décisions ; 817 départs sans date ; requêtes DTM corrigées (885 agents "
     "en 2025)."),
    ("Lot 17", "01/11/2026", "lot17_comparaison_annuaire_cnss_2024.sql", "Q97 à Q102",
     "Refaire depuis la base les tableaux 119 à 124 de l'annuaire de la CNSS 2024.",
     "742 permanents pour 758 ; âge, ancienneté, recrutements retrouvés ; catégorie fausse ; "
     "les 84 engagés de 2019."),
    ("Lot 18", "01/11/2026", "lot18_ecarts_annuaire_cnss_2024.sql", "Q103 à Q109",
     "Expliquer les écarts : catégorie, 84 engagés de 2019, contractuels, date d'état, formés.",
     "La catégorie est celle de la situation ; la date d'état ne date rien ; T62 ne retombe "
     "pas."),
    ("Lot 19", "01/11/2026", "lot19_categorie_cnss.sql", "Q110 à Q116",
     "Refaire T58, T62 et T65 avec la catégorie de la CNSS ; chercher les contractuels et les 16 "
     "manquants.",
     "T58 à 4 agents près, T65 à 0,3 point ; 46 contractuels = engagés de l'année ; retraite à "
     "la date de la décision."),
    ("Lot 20", "01/11/2026", "lot20_mouvements_t60.sql", "Q117",
     "Trouver les décès, disponibilités, remises en activité et la sortie des 84.",
     "Disponibilités = décision 07, remises = 18 ; statuts de fin de carrière presque jamais "
     "saisis."),
]

# (tableau de l'annuaire, ce qui est compté, publié, base, verdict, règles)
ANNUAIRE = [
    ("119", "Permanents au 31/12/2024", "758", "742", "Proche", "G1, G2"),
    ("119", "Contractuels au 31/12/2024", "46", "46", "Retrouvé", "G7"),
    ("120", "Permanents par catégorie : employés, maîtrise, cadres moyens, cadres supérieurs",
     "177 · 208 · 311 · 62", "173 · 207 · 304 · 58", "Retrouvé", "G3"),
    ("120", "Permanents par sexe : hommes, femmes", "412 · 346", "400 · 342", "Retrouvé", "G1"),
    ("120", "Permanents par âge : 20-34 ans, 35-54 ans, 55-59 ans", "110 · 569 · 79",
     "110 · 560 · 72", "Proche", "G4"),
    ("122", "Permanents par ancienneté : moins d'un an, 10-14 ans", "103 · 232", "101 · 224",
     "Retrouvé", "G5"),
    ("121", "Recrutements de 2024", "104", "104", "Retrouvé", "G8"),
    ("121", "Retraites de 2020 à 2024", "43 · 31 · 26 · 32 · 23", "39 · 31 · 29 · 30 · 25",
     "Proche", "G9"),
    ("121", "Démissions de 2023 et 2024 ; licenciements de 2022 à 2024", "1 · 2 ; 1 · 0 · 1",
     "1 · 2 ; 2 · 0 · 1", "Retrouvé", "G9"),
    ("121", "Décès de 2020 à 2024 (base : liquidations des droits)", "17", "19", "Proche", "G9"),
    ("121", "Disponibilités et remises en activité de 2020 à 2024", "21 · 19", "18 · 22",
     "Proche", "G8, G9"),
    ("121", "« Autres » : entrées de 2019, sorties de 2024", "84 · 84", "84 recrutés · 0",
     "Ne retombe pas", "G11"),
    ("123", "Permanents formés en 2024", "480", "409 agents · 524 inscriptions",
     "Ne retombe pas", "G10"),
    ("124", "Taux d'encadrement 2024 ; de 2020 à 2023", "49,2 % ; 41,2 à 41,4 %",
     "48,8 % ; 40,9 à 41,3 %", "Retrouvé", "G6"),
]

RULES = [
    dict(n="G1", titre="Le personnel permanent",
         regle="Agent CNSS (qualité A) dont la dernière situation commencée au 31/12 est de nature "
               "PERMANENT. Les agents de la fonction publique (B) et affectés (C) n'en sont pas.",
         preuve="742 permanents au 31/12/2024 pour 758 publiés ; 400 hommes et 342 femmes pour "
                "412 et 346",
         p=["Q97", "Q98", "Q100"], tableaux="T58, T59, T61, T65", etat="prouve"),
    dict(n="G2", titre="Présent au 31/12",
         regle="Entré au plus tard le 31/12 (embauche, à défaut prise de service), et fiche ACTIVE "
               "ou sorti après le 31/12. Sortie : première décision 02, 34, 35, 36, 52 ou 54 non "
               "annulée par une 40, à défaut la date de décès.",
         preuve="effectif 2024 à 2 % ; de 2020 à 2023 à 1 ou 2 % une fois les 84 engagés de 2019 "
                "remis ; les 16 manquants de 2024 ne forment aucun groupe",
         p=["Q99", "Q102", "Q106", "Q115"], tableaux="T58 à T61, T65", etat="prouve",
         note="écart de 2 % accepté"),
    dict(n="G3", titre="La catégorie",
         regle="La catégorie de la dernière situation au 31/12 (GRH_SITUATION.CATEGORIE, grille de "
               "la CNSS) : 01 et 02 employés, 03 agents de maîtrise, 04 et 05 cadres moyens, 06 "
               "cadres supérieurs. Plus le niveau de la qualification.",
         preuve="173, 207, 304, 58 pour 177, 208, 311, 62 ; chaque case catégorie et sexe à 4 "
                "agents près",
         p=["Q103", "Q110", "Q111"], tableaux="T58, T62, T65", etat="prouve",
         note="Kaya (2017) rangeait 05 en cadres supérieurs : question QG1, réglage categorie_05"),
    dict(n="G4", titre="L'âge",
         regle="Années pleines au 31/12. Tranches de l'Annuaire : moins de 20, 20-24… 55-59, 60 et "
               "plus. Les prestations comptent autrement (année moins année de naissance, R8).",
         preuve="identique jusqu'à 34 ans, à 2 près jusqu'à 54 ans ; 72 pour 79 à 55-59 ans",
         p=["Q98", "Q111"], tableaux="T58", etat="prouve"),
    dict(n="G5", titre="L'ancienneté",
         regle="Années pleines au 31/12 depuis l'embauche, à défaut la prise de service. Tranches : "
               "moins de 1, 1-4, 5-9… 35-39, 40 et plus.",
         preuve="chaque tranche à 8 près, la plupart à 2 près ; moins d'un an 101 pour 103",
         p=["Q100"], tableaux="T61", etat="prouve"),
    dict(n="G6", titre="Le taux d'encadrement",
         regle="Cadres (catégories 04, 05 et 06) sur les permanents présents au 31/12.",
         preuve="48,8 % pour 49,2 % en 2024 ; à 0,3 point près de 2020 à 2023 en comptant les 84 "
                "engagés de 2019",
         p=["Q102", "Q112"], tableaux="T65", etat="prouve"),
    dict(n="G7", titre="Les contractuels",
         regle="Agent CNSS de nature TEMPORAIRE au 31/12, engagé (décision 50 non annulée) dans "
               "l'année ou au second semestre de l'année d'avant, sans sortie au 31/12. Une fin de "
               "contrat (35) n'est pas une sortie si un nouveau contrat suit.",
         preuve="46 pour 46 publiés ; par catégorie, seuls les employés retombent (18 pour 18)",
         p=["Q97", "Q106", "Q114"], tableaux="T58 (temporaires, Kaya 2017)", etat="propose",
         note="durée du contrat absente de la base : question QG5, réglage duree_contrat (18 mois)"),
    dict(n="G8", titre="Les entrées (T60)",
         regle="Recrutement : entrée (embauche) dans l'année d'un agent permanent au 31/12. Remise "
               "en activité : décision 18 de l'année.",
         preuve="104 recrutements pour 104 en 2024, aucun de 2020 à 2023 ; remises à 1 à 3 près "
                "par an",
         p=["Q99", "Q117"], tableaux="T60", etat="prouve",
         note="détachements (2 et 1 publiés) introuvables : question QG7"),
    dict(n="G9", titre="Les sorties (T60)",
         regle="Première sortie de l'année : retraite = décision 02 à sa date ; démission = 36 ; "
               "licenciement = 54 ; décès = date de décès, à défaut liquidation des droits (34) ; "
               "autres = 35, 52. Mise en disponibilité : décision 07.",
         preuve="retraites à 4 près par an, démissions exactes, licenciements à 1 près ; "
                "disponibilités à 1 à 3 près ; décès 19 pour 17 sur cinq ans",
         p=["Q99", "Q109", "Q116", "Q117"], tableaux="T60", etat="prouve",
         note="décès : plus de date depuis 2010, la liquidation vient 8 mois après (QG3) ; "
              "disponibles dans l'effectif ? (QG6)"),
    dict(n="G10", titre="Les formés",
         regle="Agents distincts inscrits à une formation commencée dans l'année (Kaya 2017), par "
               "catégorie (G3) et sexe. Finalité : PERF perfectionnement, DIPLO diplômante. « Avec "
               "promotion » : classement changé après la formation.",
         preuve="ne retombe pas : 409 agents permanents et 524 inscriptions pour 480 publiés, deux "
                "fois plus de cadres moyens formés",
         p=["Q101", "Q108", "Q113"], tableaux="T62", etat="propose",
         note="d'où viennent les 480 ? question QG4"),
    dict(n="G11", titre="Les années passées",
         regle="Les 84 agents engagés en 2019 (décision 51), inactifs sans sortie datée, ne sont "
               "jamais comptés présents par la base. L'annuaire les compte de 2019 à 2023 et les "
               "sort en 2024 (« autres »).",
         preuve="en 2019, 84 recrutés et 84 de moins en fin d'année ; 10 radiés dès 2019, les 74 "
                "autres sans trace de départ",
         p=["Q99", "Q105", "Q117"], tableaux="T58 à T61, T65 (2019 à 2023)", etat="ouvert",
         note="sans effet à partir de 2025 ; question QG2"),
]

# (n°, question, en attendant)
QUESTIONS = [
    ("QG1", "La catégorie 05 est-elle « cadre moyen » (annuaire 2024) ou « cadre supérieur » "
            "(Kaya 2017, liste des qualifications) ?", "cadre moyen (réglage categorie_05)"),
    ("QG2", "Que sont devenus les 84 agents engagés en 2019, et à quelle date sont-ils partis ?",
     "années 2019 à 2023 telles que la base les donne"),
    ("QG3", "Les décès ne sont plus datés depuis 2010 : peut-on saisir la date de décès, ou celle "
            "du communiqué nécrologique (Kaya 2017) ?", "liquidation des droits (34)"),
    ("QG4", "D'où viennent les 480 formés de 2024 publiés (tableau 123) ?",
     "agents distincts de la base"),
    ("QG5", "Quelle est la durée d'un contrat temporaire ?",
     "18 mois après le dernier engagement"),
    ("QG6", "Un agent en disponibilité au 31/12 fait-il partie de l'effectif ? L'annuaire le "
            "sort (tableau 121).", "compté, comme aujourd'hui (réglage disponibilite)"),
    ("QG7", "Où sont saisis les détachements et les fins de détachement ?", "non comptés"),
]

NEXT = [
    "Requêtes proposées le 01/11 dans analyses/grh : t58_t65_effectif (T58, T59, T61, T65), "
    "t60 et t62_requete_proposee.sql, avec les réglages categorie_05, duree_contrat et "
    "disponibilite.",
    "Toi : les exécuter sur le cliché 102026, exercice 2024, et comparer aux contrôles attendus "
    "de leur en-tête.",
    "Fiches de validation faites (dossier grh/tableaux cipres) : T58 (pilote de T58, T59, T61, "
    "T65), T60, T62. À faire remplir par la DRH. T63-T64 attendent la paie et la comptabilité.",
    "Après le visa de la DRH : reporter les règles dans les requêtes DTM, sans changer leurs "
    "colonnes ; puis les fiches de maîtrise « 4 gestion administrative ».",
]

INDEX = [
    ("Lot 17 · 01/11/2026", "lot17_comparaison_annuaire_cnss_2024.sql", [
        ("Q97", "Type d'agent et catégorie → 742 permanents pour 758 ; catégorie fausse"),
        ("Q98", "Âge et sexe → âge en années pleines retrouvé"),
        ("Q99", "Mouvements 2019-2024 → 104 recrutements ; les 84 de 2019"),
        ("Q100", "Ancienneté → retrouvée à 8 près"),
        ("Q101", "Formés en 2024 → 410 agents pour 480"),
        ("Q102", "Encadrement → 92 à 99 permanents de moins de 2020 à 2023"),
    ]),
    ("Lot 18 · 01/11/2026", "lot18_ecarts_annuaire_cnss_2024.sql", [
        ("Q103", "Champs de classement → la catégorie de la situation"),
        ("Q104", "Tables et listes de codes → libellés à lire"),
        ("Q105", "Les 84 engagés de 2019 → un seul lot, sans date"),
        ("Q106", "Agents non comptés → candidats aux 16 manquants"),
        ("Q107", "Date d'état → ne date pas les départs"),
        ("Q108", "Formés : agents ou inscriptions → ni l'un ni l'autre"),
        ("Q109", "Sorties par code → décès dans la 34"),
    ]),
    ("Lot 19 · 01/11/2026", "lot19_categorie_cnss.sql", [
        ("Q110", "Libellés → 1ère à 6ième catégorie"),
        ("Q111", "T58 par catégorie → à 4 agents près"),
        ("Q112", "T65 de 2020 à 2024 → à 0,3 point près"),
        ("Q113", "T62 par catégorie → ne retombe pas"),
        ("Q114", "Contractuels → 46 engagés de l'année"),
        ("Q115", "Les 16 manquants → aucun groupe"),
        ("Q116", "Retraites mois par mois → date de la décision"),
    ]),
    ("Lot 20 · 01/11/2026", "lot20_mouvements_t60.sql", [
        ("Q117", "Décisions et statuts → disponibilités 07, remises 18"),
    ]),
]


def tag(d, x, y, label, bg, stroke, size=16, groups=()):
    """Étiquette arrondie avec son texte ; renvoie sa largeur."""
    w = text_size(label, size)[0] + 24
    d.box(x, y, w, 30, label, size, bg=bg, stroke=stroke, groups=groups)
    return w


def q_chips(d, x, y, w, refs, groups=()):
    """Les requêtes d'une carte, en pastilles grises sur une ou plusieurs lignes ; renvoie le bas."""
    lx = x
    for ref in refs:
        cw = text_size(ref, 16)[0] + 20
        if lx + cw > x + w:
            lx, y = x, y + 38
        d.box(lx, y, cw, 30, ref, 16, bg=C["gris"], stroke=C["gris_bord"], groups=groups)
        lx += cw + 8
    return y + 30


def _chips_height(w, refs):
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
    q_chips(d, x + 18 + label_w, y + qy, w - 36 - label_w, c["p"], groups=g)
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
    d.text(1360, cy + 4, "Les pastilles grises Q97 à Q117 sont les requêtes qui prouvent la règle.",
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
    """Partie 1 : les lots, de gauche à droite."""
    gap, per_row = 50, len(LOTS)
    w = (INNER - (per_row - 1) * gap) / per_row
    texts = [wrap(purpose, 18, w - 36) for *_, purpose, _ in LOTS]
    finds = [wrap("Trouvé : " + found, 16, w - 36) for *_, found in LOTS]
    paths = [wrap(path, 16, w - 36) for _, _, path, *_ in LOTS]
    top_h = 54 + max(height_of(t, 16) for t in paths) + 12
    h = (top_h + 38 + max(height_of(t, 18) for t in texts) + 16
         + max(height_of(t, 16) for t in finds) + 20)
    boxes = []
    for i, ((name, day, _, refs, _, _), path, text, found) in enumerate(
            zip(LOTS, paths, texts, finds)):
        x = 25 + i * (w + gap)
        g = d.group()
        frame = d.box(x, y, w, h, bg=C["bleu_clair"], stroke=C["bleu"], stroke_width=3, groups=g)
        d.text(x + 18, y + 14, f"{name} · {day}", 24, C["bleu"], groups=g, inside=frame)
        d.text(x + 18, y + 54, path, 16, groups=g, inside=frame, mono=True)
        d.text(x + 18, y + top_h, refs, 20, groups=g, inside=frame)
        d.text(x + 18, y + top_h + 38, text, 18, groups=g, inside=frame)
        d.text(x + 18, y + top_h + 54 + height_of(text, 18), found, 16, C["doux"], groups=g,
               inside=frame)
        boxes.append(frame)
    for a, b in zip(boxes, boxes[1:]):
        d.arrow(a["x"] + a["width"] + 6, a["y"] + h / 2, b["x"] - 6, b["y"] + h / 2,
                start=a, end=b, color=C["bleu"])
    return y + h


def annuaire(d, y):
    """Partie 2 : chaque chiffre publié face à celui de la base."""
    cols = [("Tableau", 130), ("Ce qui est compté", 900), ("Annuaire CNSS 2024", 420),
            ("Base GRH", 480), ("Verdict", 340), ("Règle", INNER - 2270)]
    g = d.group()
    rows = []
    for t, what, pub, base, verdict, rule in ANNUAIRE:
        cells = [t, wrap(what, 18, cols[1][1] - 30), wrap(pub, 18, cols[2][1] - 30),
                 wrap(base, 18, cols[3][1] - 30), verdict, rule]
        rows.append(cells)
    heights = [max(48, max(height_of(c, 18) for c in cells[:4]) + 20) for cells in rows]
    total = 50 + sum(heights)
    frame = d.box(25, y, INNER, total, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    x = 25
    for name, cw in cols:
        d.text(x + 15, y + 14, name, 18, C["doux"], groups=g, inside=frame)
        x += cw
    ry = y + 50
    for cells, rh in zip(rows, heights):
        d.arrow(25, ry, 25 + INNER, ry, color=C["gris_bord"], stroke_width=1, head=None)
        x = 25
        for i, (value, (_, cw)) in enumerate(zip(cells, cols)):
            if i == 4:
                bg, stroke = VERDICTS[value]
                tag(d, x + 15, ry + 9, value, bg, stroke, groups=g)
            else:
                d.text(x + 15, ry + 10, value, 18, groups=g, inside=frame)
            x += cw
        ry += rh
    return y + total


def questions(d, y):
    """Partie 4 : les questions à la DRH, puis la suite du travail."""
    w = (INNER - 24) * 0.62
    w2 = INNER - 24 - w
    q_lines = [(n, wrap(q, 18, w - 160), wrap("En attendant : " + wait, 16, w - 160))
               for n, q, wait in QUESTIONS]
    h = 70 + sum(height_of(q, 18) + 6 + height_of(a, 16) + 18 for _, q, a in q_lines)
    g = d.group()
    frame = d.box(25, y, w, h, bg=C["orange_clair"], stroke=C["orange"], groups=g)
    d.text(45, y + 16, "À la DRH : sept questions, aucune bloquante", 24, C["orange"], groups=g,
           inside=frame)
    ty = y + 66
    for n, q, a in q_lines:
        d.box(45, ty, 80, 30, n, 16, bg=C["orange_moyen"], stroke=C["orange"], groups=g)
        d.text(145, ty + 2, q, 18, groups=g, inside=frame)
        d.text(145, ty + 8 + height_of(q, 18), a, 16, C["doux"], groups=g, inside=frame)
        ty += height_of(q, 18) + 6 + height_of(a, 16) + 18
    x2 = 25 + w + 24
    n_lines = [wrap(f"{i}. {t}", 18, w2 - 40) for i, t in enumerate(NEXT, 1)]
    g2 = d.group()
    frame2 = d.box(x2, y, w2, h, bg=C["bleu_clair"], stroke=C["bleu"], groups=g2)
    d.text(x2 + 20, y + 16, "La suite du travail", 24, C["bleu"], groups=g2, inside=frame2)
    ty = y + 66
    for t in n_lines:
        d.text(x2 + 20, ty, t, 18, groups=g2, inside=frame2)
        ty += height_of(t, 18) + 14
    return y + h


def index(d, y):
    """Les requêtes de chaque lot, quatre lots par rangée ; renvoie le bas."""
    gap, per_row = 24, 4
    w = (INNER - (per_row - 1) * gap) / per_row
    blocks = []
    for title, path, entries in INDEX:
        lines = [(ref, wrap(text, 16, w - 36 - 74)) for ref, text in entries]
        blocks.append((title, wrap(path, 16, w - 36), lines))
    for start in range(0, len(blocks), per_row):
        row = blocks[start:start + per_row]
        head = 52 + max(height_of(p, 16) for _, p, _ in row) + 20
        h = head + max(sum(max(30, height_of(t, 16)) + 10 for _, t in lines)
                       for _, _, lines in row) + 10
        for i, (title, path, lines) in enumerate(row):
            x = 25 + i * (w + gap)
            g = d.group()
            frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["bleu"], groups=g)
            d.text(x + 18, y + 14, title, 22, C["bleu"], groups=g, inside=frame)
            d.text(x + 18, y + 52, path, 16, groups=g, inside=frame, mono=True)
            ty = y + head
            for ref, text in lines:
                d.box(x + 18, ty, 64, 30, ref, 16, bg=C["gris"], stroke=C["gris_bord"],
                      groups=g)
                d.text(x + 18 + 74, ty + 5, text, 16, groups=g, inside=frame)
                ty += max(30, height_of(text, 16)) + 10
        y += h + gap
    return y - gap


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
    d = Diagram(seed=65)
    d.text(0, 0, "GRH (T58 à T65) : les règles trouvées dans les données, et leurs preuves", 48)
    d.text(0, 66, "Le contrôle par l'annuaire de la CNSS 2024 : lots 17 à 20, requêtes Q97 à Q117, "
                  "règles G1 à G11 et questions à la DRH · état au 01/11/2026", 22, C["doux"])
    y = banner(d, 120) + 30
    y = key_points(d, y) + 40

    y = section(d, y, "1. Le chemin parcouru",
                "Les lots 1 à 16 ont appris à lire la base GRH ; les lots 17 à 20 l'ont confrontée "
                "à l'annuaire de la CNSS 2024. Les requêtes sont en lecture seule et ne sortent que "
                "des comptages, jamais de données nominatives.",
                lambda top: journey(d, top))

    y = section(d, y, "2. L'annuaire de la CNSS 2024 face à la base",
                "Annuaire de la CNSS 2024, partie IX (gestion administrative), tableaux 119 à 124 : "
                "le personnel au 31/12/2024 et ses mouvements. La base est la copie de USER_GRH "
                "figée au 18/02/2026. « Proche » : quelques agents d'écart par an.",
                lambda top: annuaire(d, top))

    y = section(d, y, "3. Les règles de calcul proposées",
                "Écrites le 01/11/2026 après les lots 17 à 20 (analyses/grh/README.md). Elles "
                "remplacent, là où elles diffèrent, les règles des requêtes DTM corrigées du "
                "22/10 : surtout le périmètre (permanents) et la catégorie.",
                lambda top: cards(d, 25, top, INNER, RULES))

    y = section(d, y, "4. Les questions à la DRH, et la suite",
                "Chaque question a une réponse par défaut, réglée dans les requêtes : la réponse "
                "de la DRH change le réglage, pas le travail.",
                lambda top: questions(d, top))

    section(d, y, "5. L'index des requêtes Q97 à Q117 : ce que chacune a montré",
            "Pour lire une requête : ouvrir son fichier dans analyses/grh et chercher « -- Qnn — ».",
            lambda top: index(d, top))
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
