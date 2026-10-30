"""
Générateur des fiches de maîtrise : de quoi réviser et s'approprier tout le travail fait sur le
projet BI de la CNSS, domaine par domaine.

Les fiches sont rangées dans schemas/maitrise/, un sous-dossier par domaine de l'Annuaire CIPRES
(plus « 0 projet » pour ce qui est commun à tous les domaines) :

    maitrise/00 commencer ici.excalidraw        le plan de révision
    maitrise/0 projet/…                          le contexte, la méthode, les documents
    maitrise/3 prestations/…                     le domaine des prestations (T25 à T57)
    maitrise/4 gestion administrative/…          plus tard, de même pour les autres domaines

Une fiche n'est pas un grand cadre unique : c'est une suite de parties, et chaque partie une
grille de blocs indépendants, chacun groupé pour se déplacer d'un bloc. Un bloc porte un titre,
une pastille (R6, B12, P25…), un état s'il y a lieu, puis des paragraphes titrés (« Ce que
c'est », « Pourquoi », « Si on te demande »…).

Le contenu est dans un module par domaine (projet.py, prestations.py…), qui définit une liste
FICHES de Fiche. Ajouter un domaine : écrire son module et l'ajouter à DOMAINES dans generer.py.
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, LINE_HEIGHT, Diagram, text_size  # noqa: E402

FOLDER = ROOT / "schemas" / "maitrise"
WIDTH = 2600
MARGIN = 25
GAP = 50          # espace entre deux blocs : ils restent bien séparés
PAD = 20          # marge intérieure d'un bloc

# genre d'un bloc : (fond de l'en-tête, bord, nom montré dans la légende)
GENRES = {
    "notion": (C["bleu_clair"], C["bleu"], "une notion à connaître"),
    "donnee": (C["gris"], C["gris_bord"], "une table ou un code de la base"),
    "etape": (C["violet"], C["encre"], "une étape du travail"),
    "constat": (C["vert_clair"], C["vert"], "un fait établi dans les données"),
    "regle": (C["jaune_clair"], C["jaune_bord"], "une règle de calcul"),
    "erreur": (C["rouge_clair"], C["rouge"], "une erreur des requêtes actuelles"),
    "question": (C["orange_clair"], C["orange"], "une question encore ouverte"),
    "reponse": (C["bleu_clair"], C["bleu"], "une question qu'on peut te poser"),
    "suite": (C["gris"], C["gris_bord"], "ce qui reste à faire"),
}
# état d'une règle ou d'un constat : (fond, bord)
ETATS = {
    "prouvé": (C["vert_moyen"], C["vert"]),
    "proposé": (C["jaune"], C["jaune_bord"]),
    "ouvert": (C["orange_moyen"], C["orange"]),
    "fait": (C["vert_moyen"], C["vert"]),
    "en cours": (C["jaune"], C["jaune_bord"]),
    "à faire": (C["gris"], C["gris_bord"]),
    # verdict d'une erreur supposée des requêtes DTM actuelles
    "confirmée": (C["rouge_moyen"], C["rouge"]),
    "en partie": (C["rouge_clair"], C["rouge"]),
    "écartée": (C["gris"], C["gris_bord"]),
    "tranchée": (C["vert_moyen"], C["vert"]),
}


@dataclass
class Bloc:
    """Un bloc : titre, pastille facultative, état facultatif, paragraphes titrés, pied."""
    titre: str
    champs: list                 # [(intitulé, texte)] ; intitulé vide : paragraphe sans titre
    ref: str = ""                # pastille : R6, B12, Lot 2…
    genre: str = "notion"
    etat: str = ""               # une clé de ETATS
    pied: str = ""               # sources, requêtes, fichiers


@dataclass
class Partie:
    titre: str
    blocs: list
    intro: str = ""
    colonnes: int = 3
    flux: bool = False           # flèches d'un bloc au suivant, dans une même rangée


@dataclass
class Fiche:
    fichier: str                 # chemin dans schemas/maitrise
    titre: str
    sous_titre: str
    objectif: str                # ce que tu sauras à la fin de la fiche
    parties: list = field(default_factory=list)


# ---------------------------------------------------------------- mise en forme du texte

def wrap(text, size, width):
    """Coupe le texte en lignes qui tiennent dans width (les retours à la ligne sont gardés)."""
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


def height_of(text, size):
    return (text.count("\n") + 1) * size * LINE_HEIGHT


# ---------------------------------------------------------------- un bloc

def _tag_width(label, size=16):
    return text_size(label, size)[0] + 24


def _layout(bloc, w):
    """Positions relatives du contenu d'un bloc, et sa hauteur."""
    inner = w - 2 * PAD
    left = _tag_width(bloc.ref, 18) + 10 if bloc.ref else 0
    right = _tag_width(bloc.etat) + 10 if bloc.etat else 0
    title = wrap(bloc.titre, 22, inner - left - right - 10)
    head_h = max(56, height_of(title, 22) + 26)
    ops = [("title", title, left, head_h)]
    y = head_h + 16
    for label, text in bloc.champs:
        if label:
            ops.append(("label", label, y))
            y += 24
        body = wrap(text, 18, inner - 10)
        ops.append(("body", body, y))
        y += height_of(body, 18) + 16
    if bloc.pied:
        foot = wrap(bloc.pied, 16, inner - 10)
        ops.append(("foot", foot, y))
        y += height_of(foot, 16) + 12
    return ops, y + 6


def bloc_height(bloc, w):
    return _layout(bloc, w)[1]


def draw_bloc(d, x, y, w, h, bloc):
    bg, stroke, _ = GENRES[bloc.genre]
    g = d.group()
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=stroke, stroke_width=2, groups=g)
    ops, _ = _layout(bloc, w)
    for op in ops:
        kind = op[0]
        if kind == "title":
            _, title, left, head_h = op
            head = d.box(x, y, w, head_h, bg=bg, stroke=stroke, groups=g)
            if bloc.ref:
                tw = _tag_width(bloc.ref, 18)
                d.box(x + PAD - 6, y + (head_h - 34) / 2, tw, 34, bloc.ref, 18, bg=C["blanc"],
                      stroke=stroke, groups=g)
            d.text(x + PAD + left, y + (head_h - height_of(title, 22)) / 2, title, 22,
                   groups=g, inside=head)
            if bloc.etat:
                ebg, estroke = ETATS[bloc.etat]
                ew = _tag_width(bloc.etat)
                d.box(x + w - PAD - ew + 6, y + (head_h - 30) / 2, ew, 30, bloc.etat, 16,
                      bg=ebg, stroke=estroke, groups=g)
        elif kind == "label":
            d.text(x + PAD, y + op[2], op[1], 16, stroke, groups=g, inside=frame)
        elif kind == "body":
            d.text(x + PAD, y + op[2], op[1], 18, groups=g, inside=frame)
        elif kind == "foot":
            d.text(x + PAD, y + op[2], op[1], 16, C["doux"], groups=g, inside=frame)
    return frame


# ---------------------------------------------------------------- une partie, une fiche

def draw_partie(d, y, n, partie):
    """Titre de partie (sans cadre autour), puis la grille de blocs. Renvoie le bas."""
    d.text(MARGIN, y, f"{n}. {partie.titre}", 32)
    d.arrow(MARGIN, y + 52, WIDTH - MARGIN, y + 52, color=C["section_bord"], stroke_width=1,
            head=None)
    y += 68
    if partie.intro:
        intro = wrap(partie.intro, 20, WIDTH - 2 * MARGIN)
        d.text(MARGIN, y, intro, 20, C["doux"])
        y += height_of(intro, 20) + 24
    cols = partie.colonnes
    w = (WIDTH - 2 * MARGIN - (cols - 1) * GAP) / cols
    for start in range(0, len(partie.blocs), cols):
        row = partie.blocs[start:start + cols]
        h = max(bloc_height(b, w) for b in row)
        frames = [draw_bloc(d, MARGIN + i * (w + GAP), y, w, h, b) for i, b in enumerate(row)]
        if partie.flux:
            for a, b in zip(frames, frames[1:]):
                d.arrow(a["x"] + a["width"] + 6, y + h / 2, b["x"] - 6, y + h / 2, start=a, end=b,
                        color=C["doux"])
        y += h + GAP
    return y - GAP


def legend(d, x, y, w, genres):
    """Bloc « Comment lire cette fiche » : les couleurs des blocs utilisés, et des états."""
    lines = [(GENRES[g][0], GENRES[g][1], GENRES[g][2]) for g in genres]
    h = 60 + len(lines) * 36 + 10
    g = d.group()
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + PAD, y + 16, "Comment lire cette fiche", 22, groups=g, inside=frame)
    ly = y + 60
    for bg, stroke, label in lines:
        d.box(x + PAD, ly, 46, 26, bg=bg, stroke=stroke, groups=g)
        d.text(x + PAD + 60, ly + 2, label, 18, groups=g, inside=frame)
        ly += 36
    return h


def render(fiche):
    d = Diagram(seed=sum(map(ord, fiche.fichier)))
    d.text(0, 0, fiche.titre, 44)
    sub = wrap(fiche.sous_titre, 22, WIDTH)
    d.text(0, 62, sub, 22, C["doux"])
    y = 62 + height_of(sub, 22) + 34

    # en tête : ce que tu sauras à la fin, et la légende des couleurs
    genres = []
    for partie in fiche.parties:
        for b in partie.blocs:
            if b.genre not in genres:
                genres.append(b.genre)
    lw = 760
    ow = WIDTH - 2 * MARGIN - lw - GAP
    goal = wrap(fiche.objectif, 20, ow - 2 * PAD)
    oh = 60 + height_of(goal, 20) + 20
    lh = 60 + len(genres) * 36 + 10
    h = max(oh, lh)
    g = d.group()
    frame = d.box(MARGIN, y, ow, h, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(MARGIN + PAD, y + 16, "À la fin de cette fiche, tu sauras", 22, groups=g, inside=frame)
    d.text(MARGIN + PAD, y + 60, goal, 20, groups=g, inside=frame)
    legend(d, MARGIN + ow + GAP, y, lw, genres)
    y += h + 80

    for n, partie in enumerate(fiche.parties, 1):
        y = draw_partie(d, y, n, partie) + 100
    return d


def generate(fiches, force=False, seulement=None):
    """Écrit chaque fiche dans schemas/maitrise. Refuse d'écraser une fiche existante sans force
    (elle a peut-être été annotée dans la visionneuse ; l'état écrasé reste dans l'historique)."""
    for fiche in fiches:
        if seulement and seulement not in fiche.fichier:
            continue
        d = render(fiche)
        path = FOLDER / fiche.fichier
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            d.save(path, force=force)
        except FileExistsError as e:
            print("IGNORÉE", e)
            continue
        print(f"{len(d.elements)} éléments écrits dans {path.relative_to(ROOT)}")
        for warning in d.warnings:
            print("ATTENTION", warning)
