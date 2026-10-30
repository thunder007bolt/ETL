"""
Génère les fiches de maîtrise dans schemas/maitrise.

Usage :
    python3 excalidraw/sources/maitrise/generer.py [--force] [--seulement <texte>]

--seulement : ne génère que les fiches dont le chemin contient ce texte (par exemple
« 3 prestations » ou « 05 »). Sans --force, une fiche qui existe déjà est laissée telle quelle :
elle a peut-être été annotée dans la visionneuse.

Ajouter un domaine : écrire son module (sur le modèle de prestations.py) et l'ajouter à DOMAINES.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import prestations  # noqa: E402
import projet  # noqa: E402
from fiches import generate  # noqa: E402

DOMAINES = [projet, prestations]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère les fiches de maîtrise.")
    parser.add_argument("--force", action="store_true", help="écraser les fiches existantes")
    parser.add_argument("--seulement", help="ne générer que les fiches dont le chemin contient "
                                            "ce texte")
    args = parser.parse_args()
    for module in DOMAINES:
        generate(module.FICHES, force=args.force, seulement=args.seulement)
