"""
Construit des schémas Excalidraw (.excalidraw) en Python.

Sert à produire le premier jet d'un schéma. Une fois le schéma retouché dans la
visionneuse, c'est le fichier .excalidraw qui fait foi : save() refuse donc
d'écraser un fichier existant, sauf avec force=True (option --force de main()).
L'état écrasé reste alors dans historique/ (voir historique.py), restaurable depuis la visionneuse.
Si le schéma a été déplacé dans la visionneuse, save() refuse aussi d'en recréer un à l'ancienne
place et indique la nouvelle (option --output).

Exemple :
    from excalidraw_builder import C, Diagram

    d = Diagram()
    a = d.box(0, 0, 200, 80, "Dépôt", bg=C["bleu_clair"], stroke=C["bleu"])
    b = d.box(300, 0, 200, 80, "Liquidation", bg=C["bleu_clair"], stroke=C["bleu"])
    d.harrow(a, b)
    d.save("excalidraw/schemas/prestations/exemple.excalidraw")
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path

from historique import moved_to, write_with_history

FONT = 5            # Excalifont, la police manuscrite par défaut d'Excalidraw
CHAR_WIDTH = 0.63   # largeur d'un caractère hors des classes ci-dessous, en fraction de la taille
# Largeurs mesurées dans Excalifont, plus 5 % de marge : les majuscules et les « _ »
# des noms de tables et de colonnes sont bien plus larges que les minuscules.
_CHAR_WIDTHS = [
    ("T", 0.90), ("DMOQW", 0.82), ("mw", 0.72),  # les plus larges, mesurées à part
    ("02", 0.74), (str.isupper, 0.74), ("_", 0.72), (str.isdigit, 0.65), (str.islower, 0.56),
    (" ", 0.42), ("()-", 0.46), (".,:;'’·", 0.38),
]
MONO = 8            # Comic Shanns, la police « code » d'Excalidraw, à chasse fixe
MONO_WIDTH = 0.58   # largeur d'un caractère : 0,55 mesuré, plus 5 % de marge
LINE_HEIGHT = 1.25
BOUND_PADDING = 5   # marge d'Excalidraw autour d'un texte lié à une forme
_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

# Palette commune à tous les schémas (voir README)
C = {
    "encre": "#1e1e1e", "doux": "#495057", "blanc": "#ffffff",
    "gris": "#e9ecef", "gris_bord": "#868e96",
    "section": "#f8f9fa", "section_bord": "#adb5bd",
    # PVID
    "bleu": "#1971c2", "bleu_moyen": "#a5d8ff", "bleu_clair": "#e7f5ff",
    # AT/MP
    "orange": "#e8590c", "orange_moyen": "#ffd8a8", "orange_clair": "#fff4e6",
    # PF, étape faite
    "vert": "#2f9e44", "vert_moyen": "#b2f2bb", "vert_clair": "#ebfbee",
    # normes, problèmes
    "rouge": "#c92a2a", "rouge_moyen": "#ffc9c9", "rouge_clair": "#fff5f5",
    # notes, points à valider
    "jaune_bord": "#f08c00", "jaune": "#ffec99", "jaune_clair": "#fff9db",
    # CNSS
    "violet": "#d0bfff",
}


def _char_width(ch):
    for test, width in _CHAR_WIDTHS:
        if (test(ch) if callable(test) else ch in test):
            return width
    return CHAR_WIDTH


def text_size(txt, size, mono=False):
    """Largeur et hauteur estimées d'un texte. Excalidraw recalcule celles d'un texte lié à
    une forme, mais garde la largeur d'un texte libre : trop courte, elle le coupe.
    mono : texte en police de code (MONO)."""
    lines = txt.split("\n")
    if mono:
        width = max(len(line) for line in lines) * MONO_WIDTH
    else:
        width = max(sum(_char_width(ch) for ch in line) for line in lines)
    return width * size, len(lines) * size * LINE_HEIGHT


class Diagram:
    """Un schéma : une liste d'éléments Excalidraw, dans l'ordre d'affichage (le dernier au-dessus)."""

    def __init__(self, seed=1):
        self.elements = []
        self.warnings = []
        self._rng = random.Random(seed)
        self._now = int(time.time() * 1000)

    def _id(self):
        return "".join(self._rng.choice(_ALPHABET) for _ in range(20))

    def group(self):
        """Identifiant de groupe : les éléments qui le partagent se déplacent ensemble."""
        return [self._id()]

    def _base(self, kind, x, y, w, h, stroke, bg="transparent", stroke_width=2, style="solid",
              groups=(), roundness=None):
        return {
            "id": self._id(), "type": kind, "x": x, "y": y, "width": w, "height": h, "angle": 0,
            "strokeColor": stroke, "backgroundColor": bg, "fillStyle": "solid",
            "strokeWidth": stroke_width, "strokeStyle": style, "roughness": 1, "opacity": 100,
            "groupIds": list(groups), "frameId": None, "roundness": roundness,
            "seed": self._rng.randint(1, 2**31 - 1), "version": 1,
            "versionNonce": self._rng.randint(1, 2**31 - 1), "isDeleted": False,
            "boundElements": [], "updated": self._now, "link": None, "locked": False,
        }

    def _check_inside(self, label, x, y, w, h, rect, pad):
        if (x < rect["x"] + pad - 0.5 or y < rect["y"] + pad - 0.5
                or x + w > rect["x"] + rect["width"] - pad + 0.5
                or y + h > rect["y"] + rect["height"] - pad + 0.5):
            self.warnings.append(
                f"débordement estimé : {label!r} ({w:.0f}x{h:.0f}) dans {rect['width']}x{rect['height']}")

    def text(self, x, y, txt, size=20, color=C["encre"], align="left", groups=(), inside=None,
             mono=False):
        """Texte libre. Avec align="center", x est le centre. inside : forme qui doit le contenir.
        mono : police de code, pour du SQL."""
        w, h = text_size(txt, size, mono)
        if align == "center":
            x -= w / 2
        e = self._base("text", x, y, w, h, stroke=color, groups=groups)
        e.update(text=txt, fontSize=size, fontFamily=MONO if mono else FONT, textAlign=align,
                 verticalAlign="top",
                 containerId=None, originalText=txt, autoResize=True, lineHeight=LINE_HEIGHT)
        self.elements.append(e)
        if inside:
            self._check_inside(txt.split("\n")[0], x, y, w, h, inside, pad=8)
        return e

    def box(self, x, y, w, h, txt=None, size=20, bg="transparent", stroke=C["encre"],
            color=C["encre"], style="solid", stroke_width=2, groups=()):
        """Rectangle arrondi, avec un texte centré lié (il suit la forme quand on la déplace)."""
        r = self._base("rectangle", x, y, w, h, stroke=stroke, bg=bg, stroke_width=stroke_width,
                       style=style, groups=groups, roundness={"type": 3})
        self.elements.append(r)
        if txt:
            tw, th = text_size(txt, size)
            t = self._base("text", x + (w - tw) / 2, y + (h - th) / 2, tw, th, stroke=color,
                           groups=groups)
            t.update(text=txt, fontSize=size, fontFamily=FONT, textAlign="center",
                     verticalAlign="middle", containerId=r["id"], originalText=txt,
                     autoResize=True, lineHeight=LINE_HEIGHT)
            r["boundElements"].append({"type": "text", "id": t["id"]})
            self.elements.append(t)
            self._check_inside(txt.split("\n")[0], t["x"], t["y"], tw, th, r, pad=BOUND_PADDING)
        return r

    def ellipse(self, x, y, w, h, txt=None, size=18, bg="transparent", stroke=C["encre"],
                color=C["encre"], stroke_width=2, groups=()):
        """Ovale avec un texte centré lié. Excalidraw place ce texte dans le rectangle inscrit,
        environ 0,7 fois la largeur et la hauteur de l'ovale."""
        r = self._base("ellipse", x, y, w, h, stroke=stroke, bg=bg, stroke_width=stroke_width,
                       groups=groups)
        self.elements.append(r)
        if txt:
            tw, th = text_size(txt, size)
            t = self._base("text", x + (w - tw) / 2, y + (h - th) / 2, tw, th, stroke=color,
                           groups=groups)
            t.update(text=txt, fontSize=size, fontFamily=FONT, textAlign="center",
                     verticalAlign="middle", containerId=r["id"], originalText=txt,
                     autoResize=True, lineHeight=LINE_HEIGHT)
            r["boundElements"].append({"type": "text", "id": t["id"]})
            self.elements.append(t)
            inner_w, inner_h = w * 0.707, h * 0.707
            inner = {"x": x + (w - inner_w) / 2, "y": y + (h - inner_h) / 2,
                     "width": inner_w, "height": inner_h}
            self._check_inside(txt.split("\n")[0], t["x"], t["y"], tw, th, inner, pad=BOUND_PADDING)
        return r

    def arrow(self, x1, y1, x2, y2, start=None, end=None, color=C["encre"], stroke_width=2,
              style="solid", head="arrow"):
        """Flèche droite de (x1, y1) à (x2, y2), accrochée aux formes start et end si données.
        head=None donne un simple trait (lien d'un modèle conceptuel)."""
        e = self._base("arrow", x1, y1, abs(x2 - x1), abs(y2 - y1), stroke=color,
                       stroke_width=stroke_width, style=style, roundness={"type": 2})
        e.update(points=[[0, 0], [x2 - x1, y2 - y1]], lastCommittedPoint=None,
                 startArrowhead=None, endArrowhead=head, elbowed=False,
                 startBinding={"elementId": start["id"], "focus": 0, "gap": 6} if start else None,
                 endBinding={"elementId": end["id"], "focus": 0, "gap": 6} if end else None)
        for shape in (start, end):
            if shape:
                shape["boundElements"].append({"type": "arrow", "id": e["id"]})
        self.elements.append(e)
        return e

    def harrow(self, a, b, **kw):
        """Flèche horizontale du bord droit de a au bord gauche de b."""
        y = a["y"] + a["height"] / 2
        return self.arrow(a["x"] + a["width"] + 6, y, b["x"] - 6, y, a, b, **kw)

    def varrow(self, a, b, **kw):
        """Flèche du bas de a vers le haut de b."""
        return self.arrow(a["x"] + a["width"] / 2, a["y"] + a["height"] + 6,
                          b["x"] + b["width"] / 2, b["y"] - 6, a, b, **kw)

    def section(self, x, y, w, h, title, size=30):
        """Cadre de fond pointillé avec un titre en haut à gauche. Renvoie le cadre, dont on peut
        fixer la hauteur une fois le contenu placé."""
        frame = self.box(x, y, w, h, bg=C["section"], stroke=C["section_bord"], style="dashed")
        self.text(x + 25, y + 20, title, size)
        return frame

    def to_dict(self):
        return {
            "type": "excalidraw",
            "version": 2,
            "source": "https://excalidraw.com",
            "elements": self.elements,
            "appState": {"gridSize": None, "viewBackgroundColor": C["blanc"]},
            "files": {},
        }

    def save(self, path, force=False):
        path = Path(path)
        target = None if path.exists() else moved_to(path)
        if target:
            raise FileExistsError(
                f"{path} a été déplacé dans la visionneuse vers {target}. Pour le régénérer à sa "
                f"nouvelle place : --output {target} --force (et mettre à jour OUTPUT dans le script).")
        if path.exists() and not force:
            raise FileExistsError(
                f"{path} existe déjà et a peut-être été retouché dans la visionneuse. "
                "Relancer avec --force pour l'écraser : l'état actuel restera dans l'historique.")
        write_with_history(path, json.dumps(self.to_dict(), ensure_ascii=False, indent=2), "script")
        return path


def main(build, default_output):
    """Point d'entrée des scripts de sources/ : construit le schéma et l'enregistre."""
    parser = argparse.ArgumentParser(description="Génère un schéma Excalidraw.")
    parser.add_argument("--output", type=Path, default=default_output, help="fichier à écrire")
    parser.add_argument("--force", action="store_true", help="écraser le fichier s'il existe")
    args = parser.parse_args()

    diagram = build()
    try:
        path = diagram.save(args.output, force=args.force)
    except FileExistsError as e:
        sys.exit(str(e))
    print(f"{len(diagram.elements)} éléments écrits dans {path}")
    for warning in diagram.warnings:
        print("ATTENTION", warning)
