"""
Schéma « La requête du mouvement (T60), pas à pas » (état au 08/11/2026).

Explique comment analyses/grh/t60_requete_proposee.sql retrouve les entrées et les
sorties de chaque année avec le seul cliché d'aujourd'hui : 1. l'idée (deux photos de
l'effectif) ; 2. la requête partie par partie, avec ses numéros de ligne, lus dans le
fichier SQL ; 3. six agents fictifs, d'un 31/12 à l'autre ; 4. ce qui en sort, et ce que
la base ne permet pas de rejouer.

Même modèle que « La requête de l'effectif, pas à pas » (requete_effectif_pas_a_pas.py).
Les lignes de 2024 de la partie 4 sont celles que la requête donne pour les six agents,
avec les réglages actuels (disponibilite = HORS_EFFECTIF depuis le 08/11/2026,
passages_annee = NON).

Usage :
    python3 excalidraw/sources/grh/requete_mouvement_pas_a_pas.py [--force]
"""

import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "T60 mouvements" / "requete_mouvement_pas_a_pas.excalidraw"
SQL = ROOT.parent / "analyses" / "grh" / "t60_requete_proposee.sql"

WIDTH = 2600
PHOTOS = list(range(2020, 2026))          # les 31/12 photographiés
EXERCICES = list(range(2021, 2026))       # les mouvements : entre deux photos
DEBUT_AXE = date(2020, 1, 1)
X0, PX = 560, 320                         # le 01/01/2020 sur l'axe, et la largeur d'une année
CELL_W, CELL_H = 170, 72
PILL_W, PILL_H = 340, 50
ROW_H = 300

PRESENT = (C["vert_clair"], C["vert"])
DISPO = (C["jaune_clair"], C["jaune_bord"])
ABSENT = (C["gris"], C["gris_bord"])
DOUTE = (C["rouge_clair"], C["rouge"])
ENTREE = (C["vert_moyen"], C["vert"])
SORTIE = (C["orange_moyen"], C["orange"])
REGLAGE = (C["jaune"], C["jaune_bord"])
TEMPORAIRE = (C["bleu_moyen"], C["bleu"])


def plages():
    """Les lignes de chaque partie de la requête : de son en-tête « -- ── N. » à la dernière
    ligne non vide avant la partie suivante."""
    lignes = SQL.read_text(encoding="utf-8").splitlines()
    debuts = [(int(m.group(1)), i) for i, ligne in enumerate(lignes, 1)
              if (m := re.match(r"-- ── (\d+)\.", ligne))]
    res = {}
    for k, (n, debut) in enumerate(debuts):
        fin = debuts[k + 1][1] - 1 if k + 1 < len(debuts) else len(lignes)
        while fin > debut and not lignes[fin - 1].strip():
            fin -= 1
        res[n] = (debut, fin)
    return res


def x_de(jour):
    return X0 + (jour - DEBUT_AXE).days / 365.25 * PX


def fin_annee(annee):
    return date(annee, 12, 31)


def milieu(annee):
    """Le milieu de l'exercice : entre la photo d'avant et celle de l'année."""
    return (x_de(fin_annee(annee - 1)) + x_de(fin_annee(annee))) / 2


# ---------------------------------------------------------------- les six agents
# photos : {année: (texte, couleurs)} ; mouvements : {année: [(texte, couleurs), …]}
FIN_DESSIN = date(2026, 1, 1)
AGENTS = [
    dict(recit="AGENT A : PERMANENT, RETRAITÉ\nprésent depuis 1995\nadmis à la retraite (02)\n"
               "le 30/06/2024\nfiche aujourd'hui : INACTIF",
         couleurs=(C["blanc"], C["encre"]),
         barres=[(DEBUT_AXE, date(2024, 6, 30), ENTREE, "solid")],
         evenements=[(date(2024, 6, 30), "02 : retraite\n30/06/2024", C["rouge"])],
         photos={**{a: ("présent", PRESENT) for a in range(2020, 2024)},
                 2024: ("absent", ABSENT), 2025: ("absent", ABSENT)},
         mouvements={2024: [("SORTIE · RET\ndécision 02", SORTIE)]}),
    dict(recit="AGENT B : PERMANENT, RECRUTÉ\nembauché le 01/03/2022\n"
               "(date d'embauche de sa fiche)\nfiche aujourd'hui : ACTIF",
         couleurs=(C["blanc"], C["encre"]),
         barres=[(date(2022, 3, 1), FIN_DESSIN, ENTREE, "solid")],
         evenements=[(date(2022, 3, 1), "embauche\n01/03/2022", C["vert"])],
         photos={2020: ("absent", ABSENT), 2021: ("absent", ABSENT),
                 **{a: ("présent", PRESENT) for a in range(2022, 2026)}},
         mouvements={2022: [("ENTRÉE · RECRU\nembauche dans l'année", ENTREE)]}),
    dict(recit="AGENT C : TEMPORAIRE, PUIS TITULARISÉ\nengagé (50) le 01/06/2022\n"
               "nouvelle situation PERMANENT\nle 01/07/2024\nfiche aujourd'hui : ACTIF",
         couleurs=(C["blanc"], C["encre"]),
         barres=[(date(2022, 6, 1), date(2024, 7, 1), TEMPORAIRE, "solid"),
                 (date(2024, 7, 1), FIN_DESSIN, ENTREE, "solid")],
         evenements=[(date(2022, 6, 1), "50 : engagé\n01/06/2022", C["bleu"]),
                     (date(2024, 7, 1), "permanent\n01/07/2024", C["vert"])],
         photos={2020: ("absent", ABSENT), 2021: ("absent", ABSENT),
                 2022: ("présent\ntemporaire", PRESENT), 2023: ("présent\ntemporaire", PRESENT),
                 2024: ("présent\npermanent", PRESENT), 2025: ("présent\npermanent", PRESENT)},
         mouvements={2022: [("ENTRÉE · RECRU\ntableau des temporaires", ENTREE)],
                     2024: [("SORTIE · AUTRE_SORT\ndevient permanent (temporaires)", SORTIE),
                            ("ENTRÉE · AUTRE_ENT\nétait temporaire (permanents)", ENTREE)]}),
    dict(recit="AGENT D : PERMANENT EN DISPONIBILITÉ\nmis en disponibilité (07)\n"
               "le 01/03/2023\nde retour (18) le 01/06/2025\nhors de l'effectif entre les deux",
         couleurs=(C["blanc"], C["encre"]),
         barres=[(DEBUT_AXE, FIN_DESSIN, ENTREE, "solid"),
                 (date(2023, 3, 1), date(2025, 6, 1), REGLAGE, "solid")],
         evenements=[(date(2023, 3, 1), "07 : dispo\n01/03/2023", C["jaune_bord"]),
                     (date(2025, 6, 1), "18 : retour\n01/06/2025", C["jaune_bord"])],
         photos={**{a: ("présent", PRESENT) for a in (2020, 2021, 2022, 2025)},
                 2023: ("absent\nen dispo", DISPO), 2024: ("absent\nen dispo", DISPO)},
         mouvements={2023: [("SORTIE · DISP\ndécision 07", SORTIE)],
                     2025: [("ENTRÉE · REM\ndécision 18", ENTREE)]}),
    dict(recit="AGENT E : PERMANENT, DÉCÉDÉ\ndécédé le 15/10/2023,\ndate jamais saisie\n"
               "liquidation des droits (34)\nle 01/07/2024",
         couleurs=(C["blanc"], C["encre"]),
         barres=[(DEBUT_AXE, date(2023, 10, 15), ENTREE, "solid"),
                 (date(2023, 10, 15), date(2024, 7, 1), ABSENT, "dashed")],
         evenements=[(date(2023, 10, 15), "décès\nnon saisi", C["gris_bord"]),
                     (date(2024, 7, 1), "34 : liquidation\n01/07/2024", C["rouge"])],
         photos={**{a: ("présent", PRESENT) for a in range(2020, 2023)},
                 2023: ("présent\npour la base", DOUTE),
                 2024: ("absent", ABSENT), 2025: ("absent", ABSENT)},
         mouvements={2024: [("SORTIE · DEC\ndécision 34 : en 2024, pas 2023", SORTIE)]}),
    dict(recit="AGENT F : ENTRÉ ET SORTI LA MÊME ANNÉE\nembauché le 01/02/2024\n"
               "démission (36) le 01/10/2024\nfiche aujourd'hui : INACTIF",
         couleurs=(C["blanc"], C["encre"]),
         barres=[(date(2024, 2, 1), date(2024, 10, 1), ENTREE, "solid")],
         evenements=[(date(2024, 2, 1), "embauche\n01/02/2024", C["vert"]),
                     (date(2024, 10, 1), "36 : démission\n01/10/2024", C["rouge"])],
         photos={a: ("absent", ABSENT) for a in PHOTOS},
         mouvements={2024: [("passages_annee = NON : rien\nOUI : RECRU, puis DEM", REGLAGE)]}),
]

# les lignes de 2024 que la requête donne pour ces six agents (réglages actuels)
LIGNES_2024 = [
    ("DEBUT", "PERMANENT", "", "3", "A, B, E"),
    ("DEBUT", "TEMPORAIRE", "", "1", "C"),
    ("AUTRE_ENT", "PERMANENT", "ETAIT_TEMPORAIRE", "1", "C"),
    ("RET", "PERMANENT", "DECISION_02", "1", "A"),
    ("DEC", "PERMANENT", "DECISION_34", "1", "E"),
    ("AUTRE_SORT", "TEMPORAIRE", "DEVIENT_PERMANENT", "1", "C"),
]


def build():
    d = Diagram(seed=60)
    p = plages()

    def lignes(n):
        debut, fin = p[n]
        return f"l. {debut} à {fin}"

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2, style="solid"):
        """Cadre avec un texte aligné à gauche (une liste se lit mieux ainsi)."""
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, style=style,
                  groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b)
        return b

    def trait(x1, y1, x2, y2, couleur=C["encre"], sw=2, style="solid"):
        return d.arrow(x1, y1, x2, y2, color=couleur, stroke_width=sw, style=style, head=None)

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "La requête du mouvement (T60), pas à pas", 48)
    d.text(0, 66, "Comment t60_requete_proposee.sql retrouve les entrées et les sorties de chaque "
                  "année, avec la seule photo d'aujourd'hui\n"
                  "Faits du DWH, cliché 102026 · règles proposées, en attente du visa de la DRH · "
                  "agents fictifs · état au 08/11/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. l'idée
    s1 = 160
    d.section(0, s1, WIDTH, 440, "1. L'idée : comparer deux photos de l'effectif")
    table = d.box(40, s1 + 75, 780, 230,
                  "LA TABLE DES MOUVEMENTS NE SUFFIT PAS\nGRH_MOUVEMENT : affectations,\n"
                  "mutations, nominations ;\nni recrutement, ni retraite, ni décès\n"
                  "(lot 1 ; réunion du 03/11, M06)", 22, bg=C["gris"], stroke=C["gris_bord"])
    effectif = d.box(910, s1 + 75, 780, 230,
                     "MAIS L'EFFECTIF SE REJOUE À CHAQUE 31/12\nla requête de l'effectif (T58) "
                     "dit\nqui était présent à n'importe quel 31/12 :\nentrées, départs, contrats "
                     "et\ndisponibilités sont datés", 22,
                     bg=C["jaune"], stroke=C["jaune_bord"], stroke_width=3)
    compare = d.box(1780, s1 + 75, 780, 230,
                    "ALORS T60 COMPARE DEUX PHOTOS\nle 31/12 d'avant et le 31/12 de l'année\n"
                    "présent à la fin, pas au début : ENTRÉE\nprésent au début, plus à la fin : "
                    "SORTIE\nla fiche ou une décision dit pourquoi", 22,
                    bg=C["vert_clair"], stroke=C["vert"], stroke_width=3)
    d.harrow(table, effectif, stroke_width=3)
    d.harrow(effectif, compare, stroke_width=3)
    bloc(40, s1 + 330, 2520, 80,
         "Toujours : DÉBUT + ENTRÉES − SORTIES = FIN, et la fin est l'effectif de T58 au 31/12. "
         "Chaque agent est comparé dans sa nature (permanent, temporaire) :\nle filtre de nature "
         "d'OBIEE donne un tableau des permanents et un tableau des temporaires, chacun juste.",
         20, couleurs=(C["vert_clair"], C["vert"]))

    # ------------------------------------------------------------------ 2. les parties
    s2 = s1 + 440 + 50
    sec2 = d.section(0, s2, WIDTH, 1700, "2. La requête, partie par partie (numéros du fichier "
                                         "et lignes)")
    g_bande = d.group()
    bande = d.box(40, s2 + 80, 2520, 330, bg=C["section"], stroke=C["gris_bord"], style="dashed",
                  groups=g_bande)
    d.text(70, s2 + 96, "PARTIES 1 À 7 : LES MÊMES QUE LA REQUÊTE DE L'EFFECTIF (voir « la "
                        "requête de l'effectif, pas à pas », dans le dossier T58 effectif)", 20,
           groups=g_bande, inside=bande)
    communes = [
        (1, "1. LES 31/12\nde chaque année, sans trou\nexercice : d0 = 31/12\nd'avant, "
            "d1 = 31/12"),
        (2, "2. LES AGENTS\nentrée = embauche,\nsinon prise de service\nqualité A, B ou C"),
        (3, "3. LES DÉPARTS\n02, 34, 35, 36, 52, 54,\nou date de décès ; chacun\navec son type "
            "de sortie"),
        (4, "4. LES CONTRATS\nchaque engagement 50 :\nun début et une fin"),
        (5, "5. LES DISPONIBILITÉS\ndernière 07 et\ndernière 18 au 31/12"),
        (6, "6. LA SITUATION\nla dernière au 31/12 :\nnature, qualification,\nunité"),
        (7, "7. LES PRÉSENTS\nchaque agent à chaque\n31/12, règles de T58\n(G2, G7)"),
    ]
    wc = (2520 - 40 - 6 * 20) / 7
    for i, (n, txt) in enumerate(communes):
        d.box(60 + i * (wc + 20), s2 + 145, wc, 245, f"{txt}\n\n{lignes(n)}", 18,
              bg=C["blanc"], stroke=C["encre"])

    # partie 8 : les deux photos
    y8 = s2 + 470
    photos = bloc(40, y8, 2520, 130,
                  f"8. LES DEUX PHOTOS DE CHAQUE EXERCICE (G14) · {lignes(8)}\n"
                  "au_debut : les présents au 31/12 d'avant · a_la_fin : les présents au 31/12 de "
                  "l'année · comparés agent par agent, dans leur nature\n"
                  "à côté : le dernier départ de l'année (pour les temporaires) et les agents "
                  "classés comme détachés (37)", 20, couleurs=(C["vert_clair"], C["vert"]), sw=3)
    d.arrow(1300, s2 + 410 + 6, 1300, y8 - 6, bande, photos, stroke_width=3)

    # parties 9 à 12
    y9 = y8 + 200
    w9 = (2520 - 3 * 30) / 4
    parties = [
        (f"9. LES ENTRÉES (G8) · {lignes(9)}\nprésent à la fin, absent au début\n"
         "(dans la même nature) ; le code :\n· DETAC : décision 37\n· RECRU : embauche dans "
         "l'année ;\n  temporaire : aucun contrat au début\n· REM : retour de dispo (réglage)\n"
         "· AUTRE_ENT : autre nature au début\n  (ETAIT_TEMPORAIRE…), sinon SANS_MOTIF",
         (C["vert_clair"], C["vert"])),
        (f"10. LES SORTIES (G9) · {lignes(10)}\nprésent au début, absent à la fin ;\n"
         "le départ qui l'a fait sortir :\n· 02 RET · 34 ou décès DEC · 36 DEM\n"
         "· 54 LIC · 35 et 52 FIN_CTRAT\n· DISP : en dispo à la fin (réglage)\n"
         "· AUTRE_SORT : autre nature à la fin\n  (DEVIENT_PERMANENT…), sinon\n  SANS_MOTIF",
         (C["orange_clair"], C["orange"])),
        (f"11. ENTRÉ ET SORTI DANS L'ANNÉE (G16)\n{lignes(11)}\nabsent des deux photos,\n"
         "présent entre les deux\nréglage passages_annee :\n· NON : pas compté (actuel)\n"
         "· OUI : une entrée RECRU\n  et une sortie (son départ)",
         (C["jaune_clair"], C["jaune_bord"])),
        (f"12. LES MOUVEMENTS INTERNES (G17)\n{lignes(12)}\nlus dans GRH_MOUVEMENT, comme "
         "avant :\naffectations, mutations,\nnominations (validées O) ;\nhors T60\n"
         "ses DIS, DET, REM ne sont plus lus\nlit : FAIT_GRH_MOUVEMENT",
         (C["blanc"], C["encre"])),
    ]
    boites = []
    for i, (txt, couleurs) in enumerate(parties):
        boites.append(bloc(40 + i * (w9 + 30), y9, w9, 330, txt, 19, couleurs=couleurs))
    for b in boites[:3]:
        cx = b["x"] + b["width"] / 2
        d.arrow(cx, y8 + 130 + 6, cx, y9 - 6, photos, b, stroke_width=2)

    # parties 13 et 14, puis OBIEE
    y13 = y9 + 400
    comptage = bloc(40, y13, 1220, 250,
                    f"13. TOUT ENSEMBLE · 14. LE COMPTAGE · l. {p[13][0]} à {p[14][1]}\n"
                    "une ligne par agent, exercice et code, puis comptées par\n"
                    "exercice × code × qualification × sexe × qualité\n"
                    "× nature d'emploi × unité × motif\n"
                    "la nature (ENTREES, SORTIES…) et le sens viennent de\n"
                    "DIM_GRH_TYPE_MOUVEMENT ; mesures : NB_MOUVEMENTS, NB_AGENTS", 20)
    obiee = d.box(1340, y13, 1220, 250,
                  "PUIS OBIEE : LE TABLEAU T60\nfiltres : qualité A, nature d'emploi (global), "
                  "5 ans\ndébut = DEBUT · recrutement = RECRU\nautres entrées = ENTREES sauf "
                  "RECRU\nretraite = RET · décès = DEC\nautres sorties = SORTIES sauf RET et "
                  "DEC\nfin = début + entrées − sorties", 20, bg=C["violet"], stroke=C["encre"])
    for b in boites:
        cx = b["x"] + b["width"] / 2
        tx = min(max(cx, 80), 1220)
        d.arrow(cx, y9 + 330 + 6, tx, y13 - 6, b, comptage, stroke_width=1, color=C["doux"])
    d.harrow(comptage, obiee, stroke_width=3)

    bloc(40, y13 + 290, 2520, 120,
         f"0. LES RÉGLAGES (CTE parametres, {lignes(0)}) : ils changent le résultat sans "
         "réécrire la requête ; valeur actuelle en premier\n"
         "disponibilite : HORS_EFFECTIF ou PRESENT (même valeur que T58)  ·  "
         "fin_contrat_inconnue : PRESENT ou FICHE  ·  passages_annee : NON ou OUI  ·  "
         "mouvements_internes : COMME_AVANT ou AUCUN\n"
         "decisions_depart : le type de sortie de chaque décision (02 RET, 34 DEC, 35 et 52 "
         "FIN_CTRAT, 36 DEM, 54 LIC)", 19, couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec2["height"] = y13 + 290 + 120 + 40 - s2

    # ------------------------------------------------------------------ 3. six agents
    s3 = sec2["y"] + sec2["height"] + 50
    sec3 = d.section(0, s3, WIDTH, 2200, "3. Concrètement : six agents fictifs, d'un 31/12 à "
                                        "l'autre")
    legende = [("présent au 31/12", PRESENT), ("absent", ABSENT), ("entrée", ENTREE),
               ("sortie", SORTIE), ("selon un réglage", REGLAGE)]
    for i, (txt, (bg, bord)) in enumerate(legende):
        d.box(1160 + i * 280, s3 + 22, 260, 40, txt, 17, bg=bg, stroke=bord)

    haut = s3 + 150
    rangs = [haut + i * (ROW_H + 24) for i in range(len(AGENTS))]
    bas = rangs[-1] + ROW_H
    for annee in PHOTOS:
        x = x_de(fin_annee(annee))
        d.text(x, s3 + 84, f"photo du\n31/12/{annee}", 17, align="center")
        trait(x, haut - 16, x, bas, C["gris_bord"], 1, "dashed")
    for annee in EXERCICES:
        d.text(milieu(annee), s3 + 100, f"mouvement {annee}", 17, C["doux"], align="center")

    def rang(top, agent):
        bloc(40, top, 480, ROW_H, agent["recit"], 17, couleurs=agent["couleurs"])
        axe = top + 72
        d.arrow(X0, axe, x_de(FIN_DESSIN) + 30, axe, color=C["doux"], stroke_width=1)
        for debut, fin, (bg, bord), style in agent["barres"]:
            d.box(x_de(debut), axe - 8, x_de(fin) - x_de(debut), 16, bg=bg, stroke=bord,
                  stroke_width=1, style=style)
        for annee in PHOTOS:
            x = x_de(fin_annee(annee))
            trait(x, axe - 12, x, axe + 12)
        for jour, txt, couleur in agent["evenements"]:
            x = x_de(jour)
            d.text(x, top + 8, txt, 15, align="center")
            trait(x, top + 46, x, axe - 10, couleur, 1, "dotted")
            d.ellipse(x - 8, axe - 8, 16, 16, bg=couleur, stroke=couleur)
        for annee, (txt, (bg, bord)) in agent["photos"].items():
            x = x_de(fin_annee(annee))
            d.box(x - CELL_W / 2, top + 98, CELL_W, CELL_H, txt, 15, bg=bg, stroke=bord)
        for annee, pastilles in agent["mouvements"].items():
            x = milieu(annee)
            for k, (txt, (bg, bord)) in enumerate(pastilles):
                d.box(x - PILL_W / 2, top + 186 + k * (PILL_H + 6), PILL_W, PILL_H, txt, 15,
                      bg=bg, stroke=bord, stroke_width=2)

    for top, agent in zip(rangs, AGENTS):
        rang(top, agent)

    bloc(40, bas + 30, 2520, 120,
         "À RETENIR : chaque année, la requête regarde l'agent à deux dates, le 31/12 d'avant et "
         "le 31/12 de l'année. S'il change de case (absent, puis présent, ou l'inverse),\n"
         "c'est un mouvement, et la fiche ou une décision en donne le code. Ce qui se passe entre "
         "les deux photos sans les changer ne compte pas (agent F), et un fait\n"
         "sans date n'est vu qu'à sa première trace datée (agent E). Un temporaire titularisé "
         "sort des temporaires et entre chez les permanents la même année (agent C).", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec3["height"] = bas + 30 + 120 + 40 - s3

    # ------------------------------------------------------------------ 4. résultat et limites
    s4 = s3 + sec3["height"] + 50
    sec4 = d.section(0, s4, WIDTH, 900, "4. Ce qui en sort, et ce que la base ne permet pas de "
                                        "rejouer")
    g = d.group()
    cadre = d.box(40, s4 + 80, 1220, 430, bg=C["blanc"], stroke=C["encre"], groups=g)
    d.text(62, s4 + 96, "LES LIGNES DE 2024 POUR CES AGENTS (ID_TEMPS = 20241231)", 20, groups=g,
           inside=cadre)
    colonnes = [("CODE_TYPE_MVT", 62), ("NATURE_EMPLOI", 300), ("MOTIF_MVT", 540),
                ("NB_AGENTS", 820), ("qui", 980)]
    for titre, x in colonnes:
        d.text(x, s4 + 140, titre, 17, C["doux"], groups=g, inside=cadre)
    for i, ligne in enumerate(LIGNES_2024):
        y = s4 + 176 + i * 36
        for (_, x), val in zip(colonnes, ligne):
            d.text(x, y, val or "(vide)", 17, groups=g, inside=cadre)
    d.text(62, s4 + 176 + len(LIGNES_2024) * 36 + 10,
           "B ne bouge pas : présent aux deux photos. D non plus : en disponibilité,\n"
           "il est absent des deux (sorti en 2023, de retour en 2025). F n'a aucune\n"
           "ligne : passages_annee = NON. Les lignes DEBUT sont additionnées ici.", 17,
           C["doux"], groups=g, inside=cadre)

    bloc(40, s4 + 530, 1220, 130,
         "LA LIGNE 2024 DE T60 DANS OBIEE (permanents)\n"
         "début 3 · recrutement 0 · autres entrées 1 · retraite 1 · décès 1 · autres sorties 0\n"
         "fin 2 (B, C) = l'effectif de T58 au 31/12/2024", 20,
         couleurs=(C["violet"], C["encre"]))
    bloc(40, s4 + 680, 1220, 170,
         "ATTENDU AU CONTRÔLE (lots 17 à 25 ; la requête reste à exécuter)\n"
         "2024, permanents : environ 672 au début, 104 recrutements, 4 remises,\n"
         "25 retraites, 5 décès, 9 autres sorties, 741 à la fin\n"
         "annuaire CNSS 2024 : 767, 104, 23 retraites, 3 décès, 94 autres sorties\n"
         "(dont les 84 engagés de 2019), 758 à la fin", 20, couleurs=(C["vert_clair"], C["vert"]))
    bloc(1340, s4 + 80, 1220, 770,
         "CE QUE LA BASE D'AUJOURD'HUI NE PERMET PAS DE REJOUER\n\n"
         "· un départ sans date : l'agent n'est présent à aucune photo,\n"
         "  donc ni entrée ni sortie. C'est le cas des 84 engagés de 2019,\n"
         "  que l'annuaire fait entrer en 2019 et sortir en 2024 (G11, QG2)\n\n"
         "· un décès sans date : l'agent sort à la liquidation des droits (34),\n"
         "  environ 8 mois plus tard, souvent l'année suivante (agent E ;\n"
         "  question QG3 à la DRH)\n\n"
         "· les détachements et fins de détachement de l'annuaire :\n"
         "  aucune décision 37 chez les permanents (QG7)\n\n"
         "· une mutation d'une unité à l'autre n'est pas un mouvement :\n"
         "  par direction ou agence, la fin se lit dans T58, et l'écart\n"
         "  donne les mutations nettes de l'unité\n\n"
         "· ce qui se passe entre deux photos sans les changer : un agent\n"
         "  entré et sorti dans l'année (agent F, réglage passages_annee),\n"
         "  un temporaire parti puis réengagé avant le 31/12\n\n"
         "Tout le reste se rejoue, car chaque fait a sa date.", 20,
         couleurs=(C["rouge_clair"], C["rouge"]))
    sec4["height"] = s4 + 850 + 40 - s4
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
