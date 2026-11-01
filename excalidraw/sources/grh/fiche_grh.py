"""
Le gabarit des fiches de validation (sources/prestations/fiche_cipres.py), réglé pour la GRH.

Les fiches des tableaux T58 à T65 se lisent comme celles des prestations ; seules changent la
légende des sources (la base GRH, copie du 18/02/2026) et l'encadré des premiers chiffres, qui met
l'annuaire de la CNSS 2024 en face de la base, ligne par ligne.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

import fiche_cipres  # noqa: E402
from excalidraw_builder import C  # noqa: E402
from fiche_cipres import chip, height_of, wrap  # noqa: E402

# la légende des sources, lue par banner() et chip() au moment du dessin
fiche_cipres.SOURCES = {
    **fiche_cipres.SOURCES,
    "Base": (C["gris"], C["gris_bord"], "les libellés de la base GRH (listes de codes)"),
    "Données": (C["gris"], C["gris_bord"], "nos requêtes sur la base GRH (copie du 18/02/2026)"),
}

OUTPUT_DIR = ROOT / "schemas" / "grh" / "tableaux cipres"

HOW_TO_DEBUT = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix "
                "dans une case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la "
                "carte si besoin. Enregistre avec Cmd+S : chaque version est gardée dans "
                "l'historique.\n")

# sort d'un agent fictif dans le calendrier
SORTIES = {
    "compte": (C["vert_moyen"], C["vert"], "compté"),
    "exclu": (C["gris"], C["gris_bord"], "pas compté"),
    "question": (C["jaune"], C["jaune_bord"], "selon la réponse à une question"),
}


def figures(d, x, y, w, title, intro, legend, pairs, source, unit=""):
    """Encadré des premiers chiffres : chaque ligne de l'annuaire de la CNSS face à la base, en
    deux barres. pairs : [(libellé, annuaire, base)]. Renvoie le bas."""
    text = wrap(intro, 18, w - 40)
    h = 54 + height_of(text, 18) + 16 + 30 + 36 + len(pairs) * 56 + 56
    g = d.group()
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, title, 22, groups=g, inside=frame)
    d.text(x + 20, y + 54, text, 18, groups=g, inside=frame)
    ly = y + 54 + height_of(text, 18) + 16
    d.text(x + 20, ly, legend, 16, C["doux"], groups=g, inside=frame)
    lx = x + 20
    ly += 30
    d.box(lx, ly + 2, 18, 14, bg=C["bleu_moyen"], stroke=C["bleu"], groups=g)
    d.text(lx + 26, ly, "annuaire de la CNSS 2024", 16, groups=g, inside=frame)
    d.box(lx + 250, ly + 2, 18, 14, bg=C["vert_moyen"], stroke=C["vert"], groups=g)
    d.text(lx + 276, ly, "la base GRH", 16, groups=g, inside=frame)
    by = ly + 36
    label_w = 150
    biggest = max(max(a, b) for _, a, b in pairs)
    scale = (w - label_w - 120) / biggest
    for label, cnss, base in pairs:
        d.text(x + 20, by + 13, label, 16, groups=g, inside=frame)
        for k, (n, bg, stroke) in enumerate([(cnss, C["bleu_moyen"], C["bleu"]),
                                             (base, C["vert_moyen"], C["vert"])]):
            top = by + k * 24
            d.box(x + label_w, top, max(n * scale, 4), 20, bg=bg, stroke=stroke, groups=g)
            d.text(x + label_w + 10 + max(n * scale, 4), top, f"{n:,}{unit}".replace(",", " "), 15,
                   groups=g, inside=frame)
        by += 56
    w_chip = chip(d, x + 20, by + 10, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 15, source, 16, groups=g, inside=frame)
    return y + h
