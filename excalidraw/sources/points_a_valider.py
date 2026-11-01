"""
Les points à valider de chaque domaine, en schéma : la version Excalidraw de
analyses/<domaine>/points_a_valider.md (qui existe aussi en Word, points_a_valider.docx).

Le fichier .md est la seule source : chaque point devient une carte avec son numéro, son texte,
les cases D'accord / Pas d'accord / À discuter et la place pour la réponse. Les points à
trancher en priorité ont un cadre orange.

Usage :
    python3 excalidraw/sources/points_a_valider.py [--force]
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, text_size  # noqa: E402
from fiche_cipres import ANSWERS, BADGE_H, WIDTH, badge, badge_width, height_of, wrap  # noqa: E402

ETL = ROOT.parent
DOMAINES = {
    "prestations": (ETL / "analyses" / "prestations" / "points_a_valider.md",
                    ROOT / "schemas" / "prestations" / "points_a_valider.excalidraw"),
    "grh": (ETL / "analyses" / "grh" / "points_a_valider.md",
            ROOT / "schemas" / "grh" / "points_a_valider.excalidraw"),
}
INNER = WIDTH - 50
CONSIGNE = ("Dans chaque carte : coche D'accord, Pas d'accord ou À discuter, et note la réponse en "
            "dessous. Cmd+S pour enregistrer, chaque version est gardée dans l'historique.")


def lire(md):
    """Titre, paragraphes d'introduction et sections du fichier .md. Une section est une liste
    de groupes (sous-titre ou None, points) ; un point est {n, texte, reponse}."""
    titre, intro, sections = None, [], []
    for ligne in md.read_text(encoding="utf-8").splitlines():
        ligne = ligne.rstrip()
        if not ligne.strip():
            continue
        if ligne.startswith("# "):
            titre = ligne[2:]
        elif ligne.startswith("## "):
            sections.append({"titre": ligne[3:], "groupes": [[None, []]]})
        elif ligne.strip().startswith("Réponse :"):
            sections[-1]["groupes"][-1][1][-1]["reponse"] = True
        elif re.match(r"\d+\. ", ligne):
            n, texte = ligne.split(". ", 1)
            sections[-1]["groupes"][-1][1].append({"n": n, "texte": texte, "reponse": False})
        elif ligne.startswith("- "):
            # la liste des tableaux, un par un
            sections[-1].setdefault("liste", []).append(ligne[2:])
        elif sections:
            sections[-1]["groupes"].append([ligne, []])
        else:
            intro.append(ligne)
    return titre, intro, sections


def _carte(w, point):
    """Le texte coupé d'une carte, la position de ses cases et de sa réponse, et sa hauteur."""
    texte = wrap(point["texte"], 18, w - 28 - badge_width(point["n"]) - 14)
    cases = 14 + max(BADGE_H, height_of(texte, 18) + 6) + 14
    reponse = cases + 30 + 12
    return texte, cases, reponse, reponse + 20 + (110 if point["reponse"] else 50)


def carte(d, x, y, w, h, point):
    texte, cases, reponse, _ = _carte(w, point)
    g = d.group()
    bord = C["orange"] if point["reponse"] else C["gris_bord"]
    cadre = d.box(x, y, w, h, bg=C["blanc"], stroke=bord, groups=g)
    badge(d, x + 14, y + 12, point["n"], groups=g)
    d.text(x + 14 + badge_width(point["n"]) + 14, y + 16, texte, 18, groups=g, inside=cadre)
    ax = x + 14
    for label in ANSWERS:
        d.box(ax, y + cases + 2, 22, 22, bg=C["blanc"], stroke=C["encre"], groups=g)
        d.text(ax + 30, y + cases + 2, label, 16, groups=g, inside=cadre)
        ax += 30 + text_size(label, 16)[0] + 26
    d.text(x + 14, y + reponse, "Réponse :" if point["reponse"] else "Remarque :", 16, C["doux"],
           groups=g, inside=cadre)


def cartes(d, y, points, par_ligne=3, gap=24):
    """Les cartes par lignes de par_ligne, chaque ligne à la hauteur de sa plus haute carte ;
    renvoie le bas."""
    w = (INNER - (par_ligne - 1) * gap) / par_ligne
    for debut in range(0, len(points), par_ligne):
        ligne = points[debut:debut + par_ligne]
        h = max(_carte(w, p)[3] for p in ligne)
        for i, p in enumerate(ligne):
            carte(d, 25 + i * (w + gap), y, w, h, p)
        y += h + gap
    return y - gap


def tableaux(d, y, lignes, par_ligne=3, gap=20):
    """Un encadré par tableau CIPRES : son numéro en pastille, son contenu et les points qui le
    concernent ; renvoie le bas."""
    w = (INNER - (par_ligne - 1) * gap) / par_ligne
    items = []
    for ligne in lignes:
        code, texte = ligne.split(", ", 1)
        texte = wrap(texte[0].upper() + texte[1:], 16, w - 28 - badge_width(code) - 14)
        items.append((code, texte, 12 + max(BADGE_H, height_of(texte, 16) + 8) + 12))
    for debut in range(0, len(items), par_ligne):
        rangee = items[debut:debut + par_ligne]
        h = max(hauteur for *_, hauteur in rangee)
        for i, (code, texte, _) in enumerate(rangee):
            x = 25 + i * (w + gap)
            g = d.group()
            cadre = d.box(x, y, w, h, bg=C["blanc"], stroke=C["bleu"], groups=g)
            badge(d, x + 14, y + 12, code, groups=g)
            d.text(x + 14 + badge_width(code) + 14, y + 16, texte, 16, groups=g, inside=cadre)
        y += h + gap
    return y - gap


def build(md):
    titre, intro, sections = lire(md)
    d = Diagram(seed=11)
    d.text(0, 0, titre, 48)
    d.text(0, 66, intro[0], 22, C["doux"])

    g = d.group()
    texte = wrap("\n".join(intro[1:] + [CONSIGNE]), 18, INNER - 40)
    h = 24 + height_of(texte, 18) + 24
    cadre = d.box(25, 120, INNER, h, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(45, 144, texte, 18, groups=g, inside=cadre)

    y = 120 + h + 40
    for s in sections:
        sec = d.section(0, y, WIDTH, 0, s["titre"])
        top = y + 70
        for sous_titre, points in s["groupes"]:
            if not points:
                continue
            if sous_titre:
                d.text(25, top, sous_titre, 24, C["bleu"])
                top += 44
            top = cartes(d, top, points) + 30
        if s.get("liste"):
            top = tableaux(d, top, s["liste"]) + 30
        sec["height"] = top - y
        y = top + 40
    return d


def main():
    parser = argparse.ArgumentParser(description="Génère les schémas des points à valider.")
    parser.add_argument("--force", action="store_true", help="écraser les schémas existants")
    args = parser.parse_args()
    for md, sortie in DOMAINES.values():
        d = build(md)
        try:
            chemin = d.save(sortie, force=args.force)
        except FileExistsError as e:
            print("IGNORÉ", e)
            continue
        print(f"{len(d.elements)} éléments écrits dans {chemin}")
        for warning in d.warnings:
            print("ATTENTION", warning)


if __name__ == "__main__":
    main()
