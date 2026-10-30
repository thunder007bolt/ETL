"""
Historique des versions des schémas Excalidraw.

Toute écriture d'un schéma (visionneuse, restauration, script du builder) passe par
write_with_history() :
    1. l'état trouvé sur le disque est gardé s'il n'est pas déjà la dernière version
       (schéma antérieur à l'historique, fichier retouché hors de la visionneuse) ;
    2. le nouveau contenu est écrit ;
    3. il est gardé à son tour, sauf s'il est identique à la dernière version.
On peut donc revenir à tout état enregistré, même après un --force du builder.

Rangement : historique/<dossier>/<nom>/<AAAAMMJJ-HHMMSS-micro>__<source>.excalidraw
Le dossier historique/ n'est pas suivi par git : git garde les étapes, l'historique le reste.

Quand la visionneuse déplace un schéma ou un dossier, move_history() déplace les versions de
chaque schéma avec lui, et record_move() note l'ancien et le nouveau chemin dans historique/deplacements.json :
le builder y lit qu'un schéma a changé de place (moved_to) au lieu d'en recréer un à l'ancienne.
"""

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = ROOT / "schemas"
HISTORY_DIR = ROOT / "historique"
# d'où vient une version : visionneuse, restauration et script écrivent ; disque est un état
# trouvé sur le disque avant une écriture, qui n'était pas encore dans l'historique
SOURCES = ("visionneuse", "restauration", "script", "disque")
MOVES_FILE = HISTORY_DIR / "deplacements.json"
_VERSION = re.compile(r"^(\d{8}-\d{6}-\d{6})__(" + "|".join(SOURCES) + r")$")


def write_atomic(path, content):
    """Écrit dans un fichier temporaire puis le renomme : jamais de schéma à moitié écrit."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=path.suffix)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def _folder(path):
    """Dossier d'historique d'un schéma, ou None s'il est hors de schemas/ (pas d'historique)."""
    try:
        relative = Path(path).resolve().relative_to(SCHEMAS_DIR.resolve())
    except ValueError:
        return None
    return HISTORY_DIR / relative.with_suffix("")


def _entries(folder):
    """Fichiers de versions, du plus récent au plus ancien (le nom commence par la date)."""
    if not folder.is_dir():
        return []
    files = [f for f in folder.glob("*.excalidraw") if _VERSION.match(f.stem)]
    return sorted(files, key=lambda f: f.name, reverse=True)


def _latest_content(folder):
    entries = _entries(folder)
    return entries[0].read_text(encoding="utf-8") if entries else None


def _keep(folder, content, source):
    folder.mkdir(parents=True, exist_ok=True)
    while True:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        target = folder / f"{stamp}__{source}.excalidraw"
        if not target.exists():
            break
    write_atomic(target, content)


def write_with_history(path, content, source):
    """Écrit un schéma en gardant l'état précédent et le nouveau dans l'historique."""
    if source not in SOURCES:
        raise ValueError(f"source inconnue : {source}")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    folder = _folder(path)
    if folder is None:
        write_atomic(path, content)
        return
    latest = _latest_content(folder)
    if path.exists():
        current = path.read_text(encoding="utf-8")
        if current != latest:
            _keep(folder, current, "disque")
            latest = current
    write_atomic(path, content)
    if content != latest:
        _keep(folder, content, source)


def list_versions(path):
    """Versions d'un schéma, de la plus récente à la plus ancienne.

    « actuelle » nomme la version identique au fichier sur le disque, ou vaut None si le
    fichier a changé depuis la dernière version (il sera gardé à la prochaine écriture).
    """
    folder = _folder(path)
    entries = _entries(folder) if folder else []
    versions = []
    for f in entries:
        stamp, source = _VERSION.match(f.stem).groups()
        date = datetime.strptime(stamp, "%Y%m%d-%H%M%S-%f")
        versions.append({
            "version": f.stem,
            "date": date.isoformat(timespec="seconds"),
            "source": source,
            "taille": f.stat().st_size,
        })
    current = None
    if entries and Path(path).is_file():
        if entries[0].read_text(encoding="utf-8") == Path(path).read_text(encoding="utf-8"):
            current = entries[0].stem
    return {"versions": versions, "actuelle": current}


def read_version(path, version):
    """Contenu d'une version, ou None si elle n'existe pas (nom invalide compris)."""
    folder = _folder(path)
    if folder is None or not _VERSION.match(version):
        return None
    target = folder / f"{version}.excalidraw"
    return target.read_text(encoding="utf-8") if target.is_file() else None


def _relative(path):
    return Path(path).resolve().relative_to(SCHEMAS_DIR.resolve())


def move_history(old, new):
    """Déplace les versions du schéma old vers new, deux chemins de schémas dans schemas/.

    Seuls les fichiers de versions bougent, un par un : le dossier d'historique d'un schéma x
    contient aussi ceux des schémas d'un dossier x voisin (x.excalidraw et x/…). Déplacer le
    dossier entier emporterait leurs versions, ou le mettrait dans son propre sous-dossier quand
    x.excalidraw entre dans le dossier x.
    """
    source, target = _folder(old), _folder(new)
    if source is None or target is None:
        return
    for entry in _entries(source):
        destination = target / entry.name
        if not destination.exists():
            target.mkdir(parents=True, exist_ok=True)
            os.replace(entry, destination)
    # les dossiers d'historique laissés vides par le déplacement sont retirés
    for folder in (source, *source.parents):
        if folder == HISTORY_DIR or not folder.is_dir() or any(folder.iterdir()):
            break
        folder.rmdir()


def _read_moves():
    try:
        return json.loads(MOVES_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def record_move(old, new):
    """Note qu'un schéma est passé de old à new, en mettant à jour les déplacements plus anciens
    (a vers old devient a vers new)."""
    old_rel, new_rel = _relative(old).as_posix(), _relative(new).as_posix()
    moves = {k: (new_rel if v == old_rel else v) for k, v in _read_moves().items()}
    moves[old_rel] = new_rel
    moves.pop(new_rel, None)  # un schéma vit de nouveau à ce chemin
    MOVES_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(MOVES_FILE, json.dumps(moves, ensure_ascii=False, indent=2, sort_keys=True))


def moved_to(path):
    """Nouvelle place d'un schéma déplacé dans la visionneuse, ou None (jamais déplacé, ou
    disparu depuis)."""
    try:
        target = _read_moves().get(_relative(path).as_posix())
    except ValueError:
        return None
    if target and (SCHEMAS_DIR / target).is_file():
        return SCHEMAS_DIR / target
    return None
