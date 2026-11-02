"""
La fiche de lecture des tableaux CIPRES, une par domaine, à lire à côté de l'Annuaire statistique
CIPRES. Pour chaque tableau, chaque terme de l'Annuaire (déposé, liquidé, en instance…) a une
rangée de trois colonnes :
    ce que j'en ai compris : ce que dit l'Annuaire, ma compréhension, un exemple, la question à
    valider, les cases D'accord / Pas d'accord / À discuter et la place pour la réponse ;
    où je le prends dans la base : tables, champs, codes ;
    la requête qui permet de vérifier le chiffre.

Le contenu de chaque domaine est dans sources/<domaine>/lecture_<domaine>.py : TITRE, SOUS_TITRE,
INTRO, TABLES (les tables de la base, une fois pour toutes), SUITE et TABLEAUX.

Usage :
    python3 excalidraw/sources/fiche_de_lecture.py [--force]
"""

import argparse
import importlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, text_size  # noqa: E402
from fiche_cipres import ANSWERS, BADGE_H, WIDTH, badge, badge_width, height_of, wrap  # noqa: E402

DOMAINES = {
    "prestations": ("prestations.lecture_prestations",
                    ROOT / "schemas" / "prestations" / "fiche_de_lecture.excalidraw"),
    "grh": ("grh.lecture_grh", ROOT / "schemas" / "grh" / "fiche_de_lecture.excalidraw"),
}
GAP = 20
INNER = WIDTH - 50
COLONNES = ["Ce que j'ai compris, et ta réponse", "Où je le prends dans la base",
            "La requête pour vérifier"]
CONSIGNE = ("Pour chaque terme : à gauche, ce que dit l'Annuaire et ce que j'en ai compris, puis "
            "la question ; au milieu, où je le prends dans la base ; à droite, la requête qui "
            "permet de refaire le chiffre. Coche D'accord, Pas d'accord ou À discuter et note la "
            "réponse en dessous. Cmd+S pour enregistrer, chaque version est gardée dans "
            "l'historique.")


def renvoi(sql, decalage=None):
    """Les requêtes des fiches de validation parlent de « règle n » : ici on dit « terme n »,
    avec le numéro du terme quand il diffère (decalage : règle, terme)."""
    decalage = decalage or {}

    def termes(m):
        nums = [str(decalage.get(int(x), int(x))) for x in re.findall(r"\d+", m.group(0))]
        return ("termes " + " et ".join(nums)) if len(nums) > 1 else "terme " + nums[0]
    return re.sub(r"règles? \d+(?: et \d+)?", termes, sql)


def _gauche(w, n, t):
    """Positions de la colonne « ce que j'ai compris » et sa hauteur."""
    inner = w - 28
    ops = [("badge", 14, 12, n)]
    titre = wrap(t["terme"], 22, inner - badge_width(n) - 12)
    ops.append(("text", 14 + badge_width(n) + 12, 22, titre, 22, C["encre"]))
    y = 12 + max(BADGE_H, height_of(titre, 22) + 10) + 12
    if t.get("annuaire"):
        texte = wrap(t["annuaire"], 16, inner - 24)
        h = 10 + height_of(texte, 16) + 10
        ops.append(("annuaire", 14, y, texte, h))
        y += h + 14
    ops.append(("text", 14, y, "Ce que j'ai compris", 16, C["doux"]))
    y += 24
    compris = wrap(t["compris"], 18, inner)
    ops.append(("text", 14, y, compris, 18, C["encre"]))
    y += height_of(compris, 18) + 12
    if t.get("exemple"):
        exemple = wrap("Exemple : " + t["exemple"], 16, inner)
        ops.append(("text", 14, y, exemple, 16, C["doux"]))
        y += height_of(exemple, 16) + 12
    question = wrap(t["question"], 18, inner)
    ops.append(("text", 14, y + 4, question, 18, C["bleu"]))
    y += 4 + height_of(question, 18) + 14
    ops.append(("answers", 14, y))
    y += 30 + 10
    ops.append(("text", 14, y, "Réponse :", 16, C["doux"]))
    return ops, y + 20 + 50


def _milieu(w, t):
    """Les puces « où je le prends dans la base » et leur hauteur."""
    puces, y = [], 14 + 24 + 6
    for ligne in t["base"]:
        texte = wrap(ligne, 16, w - 28 - 22)
        puces.append((y, texte))
        y += height_of(texte, 16) + 10
    return puces, y + 6


def rangee(d, y, largeurs, n, t, annee):
    """Les trois colonnes d'un terme, à la hauteur de la plus haute ; renvoie le bas."""
    wg, wm, wd = largeurs
    ops, hg = _gauche(wg, n, t)
    puces, hm = _milieu(wm, t)
    hd = 44 + height_of(t["sql"], 16) + 20
    h = max(hg, hm, hd)

    x = 25
    g = d.group()
    cadre = d.box(x, y, wg, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    for op in ops:
        kind, dx, dy = op[:3]
        if kind == "badge":
            badge(d, x + dx, y + dy, op[3], groups=g)
        elif kind == "text":
            d.text(x + dx, y + dy, op[3], op[4], op[5], groups=g, inside=cadre)
        elif kind == "annuaire":
            panneau = d.box(x + dx, y + dy, wg - 28, op[4], bg=C["rouge_clair"], stroke=C["rouge"],
                            groups=g)
            d.text(x + dx + 12, y + dy + 10, op[3], 16, C["encre"], groups=g, inside=panneau)
        elif kind == "answers":
            ax = x + dx
            for label in ANSWERS:
                d.box(ax, y + dy + 2, 22, 22, bg=C["blanc"], stroke=C["encre"], groups=g)
                d.text(ax + 30, y + dy + 2, label, 16, groups=g, inside=cadre)
                ax += 30 + text_size(label, 16)[0] + 26

    x += wg + GAP
    g = d.group()
    cadre = d.box(x, y, wm, h, bg=C["bleu_clair"], stroke=C["bleu_moyen"], groups=g)
    d.text(x + 14, y + 14, "Dans la base", 16, C["doux"], groups=g, inside=cadre)
    for py, texte in puces:
        d.text(x + 14, y + py, "•", 16, groups=g, inside=cadre)
        d.text(x + 36, y + py, texte, 16, groups=g, inside=cadre)

    x += wm + GAP
    g = d.group()
    cadre = d.box(x, y, wd, h, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(x + 18, y + 14, f"Pour vérifier ({annee})", 16, C["doux"], groups=g, inside=cadre)
    d.text(x + 18, y + 44, t["sql"], 16, groups=g, inside=cadre, mono=True)
    return y + h


def encadre(d, y, titre, lignes, bg, bord, puces=False):
    """Un encadré pleine largeur : un titre et des paragraphes (ou des puces) ; renvoie le bas."""
    corps = [wrap(("• " if puces else "") + ligne, 18, INNER - 60) for ligne in lignes]
    h = 20 + 34 + sum(height_of(c, 18) + 10 for c in corps) + 14
    g = d.group()
    cadre = d.box(25, y, INNER, h, bg=bg, stroke=bord, groups=g)
    d.text(45, y + 20, titre, 22, groups=g, inside=cadre)
    ty = y + 20 + 34
    for c in corps:
        d.text(45, ty, c, 18, groups=g, inside=cadre)
        ty += height_of(c, 18) + 10
    return y + h


def tableau(d, y, tab):
    """La section d'un tableau : son titre, ses pages dans l'Annuaire, ce qu'il montre, puis une
    rangée par terme ; renvoie le bas."""
    sec = d.section(0, y, WIDTH, 0, f"{tab['code']}   {tab['titre']}")
    top = y + 76
    d.text(25, top, tab["pages"], 18, C["rouge"])
    top += 34
    resume = wrap(tab["resume"], 20, INNER)
    d.text(25, top, resume, 20)
    top += height_of(resume, 20) + 30

    sql_w = max(text_size(t["sql"], 16, mono=True)[0] for t in tab["termes"]) + 36
    reste = INNER - 2 * GAP - sql_w
    largeurs = (round(reste * 0.56), round(reste * 0.44), sql_w)
    x = 25
    for titre, w in zip(COLONNES, largeurs):
        d.text(x, top, titre, 20, C["doux"])
        x += w + GAP
    top += 40
    for n, t in enumerate(tab["termes"], 1):
        top = rangee(d, top, largeurs, str(n), t, tab["annee"]) + GAP
    sec["height"] = top + 20 - y
    return top + 20


def build(contenu):
    d = Diagram(seed=13)
    d.text(0, 0, contenu.TITRE, 48)
    d.text(0, 66, contenu.SOUS_TITRE, 22, C["doux"])
    y = encadre(d, 120, "Comment lire cette fiche", contenu.INTRO + [CONSIGNE], C["jaune_clair"],
                C["jaune_bord"]) + 24
    y = encadre(d, y, contenu.TITRE_TABLES, contenu.TABLES, C["blanc"], C["gris_bord"],
                puces=True) + 24
    y = encadre(d, y, "Les tableaux de cette fiche", contenu.SUITE, C["blanc"], C["gris_bord"]) + 40
    for tab in contenu.TABLEAUX:
        y = tableau(d, y, tab) + 40
    return d


def main():
    parser = argparse.ArgumentParser(description="Génère les fiches de lecture des tableaux CIPRES.")
    parser.add_argument("--force", action="store_true", help="écraser les schémas existants")
    args = parser.parse_args()
    sys.path.insert(0, str(ROOT / "sources"))
    for module, sortie in DOMAINES.values():
        d = build(importlib.import_module(module))
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
