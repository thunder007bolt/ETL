"""
Fiche de définitions, pour retrouver un mot oublié sans relire les autres schémas.

Une carte = le mot, une phrase, un exemple. Rien d'autre.

    python3 excalidraw/sources/lexique/definitions.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, LINE_HEIGHT, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "lexique" / "définitions.excalidraw"

COLS = 3
CARD_W = 760
GAP = 22
MARGIN = 40
PAGE_W = MARGIN * 2 + COLS * CARD_W + (COLS - 1) * GAP


def wrap(text, size, width):
    lines = []
    for paragraph in text.split("\n"):
        line = ""
        for word in paragraph.split(" "):
            candidate = f"{line} {word}" if line else word
            if line and text_size(candidate, size)[0] > width:
                lines.append(line)
                line = word
            else:
                line = candidate
        lines.append(line)
    return "\n".join(lines)


def hauteur(text, size):
    return (text.count("\n") + 1) * size * LINE_HEIGHT


# (mot, définition, exemple, fond, bord)
GROUPES = [
    ("Les allocations", C["vert_clair"], C["vert"], [
        ("Allocation familiale",
         "L'argent que la CNSS verse chaque mois pour un enfant.",
         "Un père a deux enfants : la CNSS lui verse une allocation pour chacun."),
        ("Allocataire",
         "Le parent qui reçoit cet argent. On le compte une fois, pas une fois par enfant.",
         "Une mère touche les allocations de ses trois enfants : un seul allocataire."),
        ("Enfant",
         "Celui pour qui l'allocation est versée. Ce n'est pas lui qui reçoit l'argent.",
         "L'enfant de 8 ans dont l'allocation est encore ouverte au 31 décembre."),
        ("Allocation prénatale",
         "L'argent versé à une future mère pendant sa grossesse.",
         "Une femme qui ne touche que ça n'est pas une allocataire des allocations familiales."),
    ]),
    ("Les personnes", C["bleu_clair"], C["bleu"], [
        ("Assuré",
         "Le travailleur immatriculé à la CNSS, celui qui cotise.",
         "Le salarié qui part à la retraite."),
        ("Ayant droit",
         "La personne à qui la prestation est due.",
         "L'orphelin, pour sa pension d'orphelin."),
        ("Pensionné",
         "La personne qui touche une pension.",
         "Un retraité de vieillesse, au 31 décembre."),
        ("Survivants",
         "Les proches d'un assuré décédé : la veuve ou le veuf, les orphelins, les parents.",
         "La veuve et les deux orphelins d'un pensionné."),
    ]),
    ("Ce que la CNSS verse", C["orange_clair"], C["orange"], [
        ("Pension",
         "Un versement chaque mois : vieillesse, invalidité, ou après un décès.",
         "La pension de vieillesse d'un retraité."),
        ("Retraite anticipée",
         "Une pension de vieillesse prise avant l'âge normal.",
         "Dans la base, son code est RPRA. La vieillesse ordinaire est RPV."),
        ("Rente",
         "L'argent versé après un accident du travail, une fois la blessure stabilisée.",
         "La rente de la victime, ou celle de sa veuve."),
        ("Indemnité journalière",
         "L'argent versé pendant un arrêt, à la place du salaire.",
         "AIJ pour un accident du travail, MIJ pour un congé de maternité."),
    ]),
    ("L'argent", C["jaune_clair"], C["jaune_bord"], [
        ("Débours",
         "Une ligne de paiement : une prestation, pour un mois.",
         "La pension de mars de M. A est un débours."),
        ("Indu",
         "Une somme versée à tort. Trop-perçu : versé en trop. Moins-perçu : versé en moins.",
         "Une pension encore payée après le décès : un trop-perçu."),
        ("Rappel",
         "Un paiement d'un mois déjà passé, enregistré plus tard.",
         "La pension de janvier, versée seulement en mars."),
    ]),
    ("Le dossier", C["gris"], C["gris_bord"], [
        ("Dossier",
         "Le dossier ouvert pour une personne. Il porte ses prestations.",
         "Le dossier de pension d'un retraité, puis celui de sa veuve après son décès."),
        ("Prestation",
         "Un droit ouvert dans un dossier : une pension, une rente, une allocation.",
         "Dans un dossier d'accident : des indemnités, puis une rente des années après."),
        ("Liquidation",
         "Le calcul des droits. Il se termine quand la CNSS notifie sa décision.",
         "Reçu le 10 janvier, notifié le 13 mars : liquidé le 13 mars."),
        ("En instance",
         "Une demande déposée, pas encore acceptée ni refusée au 31 décembre.",
         "Reçue en novembre, notifiée en janvier de l'année suivante."),
    ]),
]


def _contenu(mot, definition, exemple, largeur):
    inner = largeur - 40
    titre = wrap(mot, 26, inner)
    defin = wrap(definition, 18, inner)
    exemple_txt = wrap("Exemple. " + exemple, 16, inner)
    h = (20 + hauteur(titre, 26) + 12 + hauteur(defin, 18) + 10
         + hauteur(exemple_txt, 16) + 20)
    return titre, defin, exemple_txt, h


def carte(d, x, y, w, h, titre, defin, exemple_txt, fond, bord):
    g = d.group()
    box = d.box(x, y, w, h, bg=fond, stroke=bord, groups=g)
    yy = y + 18
    d.text(x + 20, yy, titre, 26, groups=g, inside=box)
    yy += hauteur(titre, 26) + 12
    d.text(x + 20, yy, defin, 18, groups=g, inside=box)
    yy += hauteur(defin, 18) + 10
    d.text(x + 20, yy, exemple_txt, 16, C["doux"], groups=g, inside=box)
    return box


def build():
    d = Diagram(seed=14)
    d.text(MARGIN, 28, "Définitions à relire", 40)
    d.text(MARGIN, 84,
           "Une carte, un mot. La phrase dit ce que c'est. La ligne « Exemple » le montre.",
           20, C["doux"])
    d.text(MARGIN, 116, "État au 14/11/2026", 16, C["doux"])

    y = 170
    for titre_groupe, fond, bord, mots in GROUPES:
        rangees = []
        for i in range(0, len(mots), COLS):
            ligne = mots[i:i + COLS]
            prepares = [_contenu(m, defin, ex, CARD_W) for m, defin, ex in ligne]
            h = max(item[3] for item in prepares)
            rangees.append((prepares, h))
        bloc_h = 70 + sum(h + GAP for _, h in rangees) + 8
        cadre = d.section(MARGIN - 8, y, PAGE_W - 2 * MARGIN + 16, bloc_h, titre_groupe, 28)
        cy = y + 70
        for prepares, h in rangees:
            for col, (titre, defin, exemple_txt, _) in enumerate(prepares):
                x = MARGIN + col * (CARD_W + GAP)
                carte(d, x, cy, CARD_W, h, titre, defin, exemple_txt, fond, bord)
            cy += h + GAP
        cadre["height"] = cy - y + 8
        y = cy + 36
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
