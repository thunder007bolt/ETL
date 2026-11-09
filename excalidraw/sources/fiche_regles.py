"""
La fiche des règles des tableaux CIPRES, en schéma : la version Excalidraw de
analyses/regles_cipres/fiche_regles.md (qui existe aussi en Word, fiche_regles.docx).

Le fichier .md est la seule source. Chaque domaine (## …) devient un cadre, chaque groupe de
tableaux (### …) une carte : son titre en bleu, ses règles numérotées, et les lignes « À faire »
en orange. Les cartes vont trois par rangée, dans l'ordre du fichier ; un domaine sans groupe
(« Pour tous les tableaux ») prend toute la largeur.

Usage :
    python3 excalidraw/sources/fiche_regles.py [--force]
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram  # noqa: E402
from fiche_cipres import WIDTH, height_of, wrap  # noqa: E402

ETL = ROOT.parent
SOURCE = ETL / "analyses" / "regles_cipres" / "fiche_regles.md"
OUTPUT = ROOT / "schemas" / "regles cipres" / "fiche_regles.excalidraw"
INNER = WIDTH - 50
TAILLE, TITRE, ECART = 18, 22, 10      # taille d'une règle, d'un titre de carte, écart entre lignes
NUM_W = 46                             # place du numéro, devant le texte de la règle


def lire(md):
    """Titre, introduction et domaines. Un domaine a des groupes {titre, lignes} ; une ligne est
    (numéro, texte), avec numéro None pour une ligne « À faire »."""
    titre, intro, domaines = None, [], []
    for ligne in md.read_text(encoding="utf-8").splitlines():
        ligne = ligne.rstrip()
        if not ligne.strip():
            continue
        if ligne.startswith("# "):
            titre = ligne[2:]
        elif ligne.startswith("## "):
            domaines.append({"titre": ligne[3:], "groupes": []})
        elif ligne.startswith("### "):
            domaines[-1]["groupes"].append({"titre": ligne[4:], "lignes": []})
        elif re.match(r"^\d+\. ", ligne) or ligne.startswith("- "):
            if not domaines[-1]["groupes"]:
                domaines[-1]["groupes"].append({"titre": None, "lignes": []})
            m = re.match(r"^(\d+)\. (.*)$", ligne)
            domaines[-1]["groupes"][-1]["lignes"].append(
                (m.group(1), m.group(2)) if m else (None, ligne[2:]))
        else:
            intro.append(ligne)
    return titre, intro, domaines


def _mise_en_page(w, groupe):
    """Le titre et les lignes coupés d'une carte, leur position, et la hauteur de la carte."""
    y, titre = 18, None
    if groupe["titre"]:
        titre = wrap(groupe["titre"], TITRE, w - 36)
        y += height_of(titre, TITRE) + 14
    lignes = []
    for numero, texte in groupe["lignes"]:
        retrait = NUM_W if numero else 0
        corps = wrap(texte, TAILLE, w - 36 - retrait)
        lignes.append((y, numero, corps, retrait))
        y += height_of(corps, TAILLE) + ECART
    return titre, lignes, y + 10


def carte(d, x, y, w, h, groupe):
    titre, lignes, _ = _mise_en_page(w, groupe)
    g = d.group()
    cadre = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    if titre:
        d.text(x + 18, y + 18, titre, TITRE, C["bleu"], groups=g, inside=cadre)
    for ly, numero, corps, retrait in lignes:
        if numero:
            d.text(x + 18, y + ly, f"{numero}.", TAILLE, C["bleu"], groups=g, inside=cadre)
        d.text(x + 18 + retrait, y + ly, corps, TAILLE, C["encre"] if numero else C["orange"],
               groups=g, inside=cadre)


def cartes(d, y, groupes, par_ligne=3, gap=30):
    """Les cartes par rangées de par_ligne, chaque rangée à la hauteur de sa plus haute carte ;
    un groupe sans titre prend toute la largeur. Renvoie le bas."""
    rangees, rangee = [], []
    for g in groupes:
        if not g["titre"]:
            if rangee:
                rangees.append(rangee)
            rangees.append([g])
            rangee = []
        else:
            rangee.append(g)
            if len(rangee) == par_ligne:
                rangees.append(rangee)
                rangee = []
    if rangee:
        rangees.append(rangee)
    for rangee in rangees:
        n = 1 if not rangee[0]["titre"] else par_ligne
        w = (INNER - (n - 1) * gap) / n
        h = max(_mise_en_page(w, g)[2] for g in rangee)
        for i, g in enumerate(rangee):
            carte(d, 25 + i * (w + gap), y, w, h, g)
        y += h + gap
    return y - gap


def build(md):
    titre, intro, domaines = lire(md)
    d = Diagram(seed=13)
    d.text(0, 0, titre, 48)
    d.text(0, 66, intro[0], 22, C["doux"])

    g = d.group()
    texte = wrap("\n".join(intro[1:]), 20, INNER - 40)
    h = 24 + height_of(texte, 20) + 24
    cadre = d.box(25, 120, INNER, h, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(45, 144, texte, 20, groups=g, inside=cadre)

    y = 120 + h + 40
    for dom in domaines:
        sec = d.section(0, y, WIDTH, 0, dom["titre"])
        bas = cartes(d, y + 70, dom["groupes"])
        sec["height"] = bas + 30 - y
        y = bas + 70
    return d


def main():
    parser = argparse.ArgumentParser(description="Génère le schéma de la fiche des règles.")
    parser.add_argument("--force", action="store_true", help="écraser le schéma existant")
    args = parser.parse_args()
    d = build(SOURCE)
    try:
        chemin = d.save(OUTPUT, force=args.force)
    except FileExistsError as e:
        print("IGNORÉ", e)
        return
    print(f"inchangé : {chemin}" if d.inchange else f"{len(d.elements)} éléments écrits dans {chemin}")
    for warning in d.warnings:
        print("ATTENTION", warning)


if __name__ == "__main__":
    main()
