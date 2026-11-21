"""
Visionneuse locale des schémas Excalidraw du projet.

Usage :
    python3 excalidraw/server.py              # http://localhost:8765 (et accessible par IP)
    python3 excalidraw/server.py --port 8800
    python3 excalidraw/server.py --host 127.0.0.1   # local seulement

Sert la visionneuse (viewer/) et une petite API sur les fichiers de schemas/ :
    GET  /api/arbre              dossiers, schémas, espaces et vues
    GET  /api/schemas            liste des schémas
    GET  /api/schemas/<chemin>   contenu d'un schéma et date de modification
    PUT  /api/schemas/<chemin>   enregistre un schéma
                                 ?create=1 : refuse si le schéma existe déjà
                                 en-tête X-Expected-Mtime : refuse si le fichier a changé
                                 depuis son ouverture, sauf avec ?force=1
                                 ?source=restauration : l'écriture restaure une version
                                 ?source=auto : enregistrement automatique (une version
                                 gardée par tranche de 10 minutes)
    GET  /api/historique/<chemin>                versions gardées d'un schéma
    GET  /api/historique/<chemin>?version=<id>   contenu d'une version
    POST /api/dossiers           crée un dossier : {"chemin": "prestations/archives"}
    DELETE /api/dossiers/<chemin>  supprime un dossier vide
    POST /api/deplacer           déplace un schéma ou un dossier dans un autre dossier :
                                 {"de": "prestations/x.excalidraw", "vers": "grh"} ("" : la racine)
    GET  /api/vues               les vues thématiques (aussi dans /api/arbre)
    PUT  /api/vues               remplace la liste : {"vues": [{"id", "nom", "chemins"}]}
    GET  /api/espaces            les espaces physiques (aussi dans /api/arbre)
    POST /api/espaces            crée un espace : {"nom": "…"} → dossier schemas/espaces/<id>/
    PUT  /api/espaces/<id>       renomme : {"nom": "…"}
    DELETE /api/espaces/<id>     supprime un espace vide ; ?force=1 retire aussi son contenu

Un espace est un contexte isolé : sa racine sur le disque est schemas/espaces/<id>/.
Une vue est un filtre thématique (nom + liste de chemins) : rien n'est déplacé ; la liste
est dans vues.json. Un déplacement ou une suppression met les vues à jour.
Chaque enregistrement garde l'état précédent et le nouveau dans historique/ (lib/historique.py) ;
un déplacement emmène les versions du schéma avec lui.
Par défaut écoute sur 0.0.0.0 : accessible depuis un autre ordinateur du réseau
via l'IP de cette machine (http://<ip>:8765). Passer --host 127.0.0.1 pour
rester en local seulement.
"""

import argparse
import json
import os
import re
import shutil
import socket
import sys
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "lib"))

from historique import (list_versions, move_history, read_version, record_move,  # noqa: E402
                        write_atomic, write_with_history)

SCHEMAS_DIR = ROOT / "schemas"
VIEWER_DIR = ROOT / "viewer"
SPACES_ROOT_NAME = "espaces"
SPACES_DIR = SCHEMAS_DIR / SPACES_ROOT_NAME
API = "/api/schemas"
TREE_API = "/api/arbre"
FOLDERS_API = "/api/dossiers"
MOVE_API = "/api/deplacer"
HISTORY_API = "/api/historique"
SPACES_API = "/api/espaces"
VIEWS_API = "/api/vues"
SPACES_FILE = ROOT / "espaces.json"
VIEWS_FILE = ROOT / "vues.json"
SPACE_ID = re.compile(r"[a-z0-9][a-z0-9\-]{0,39}")
# nom d'un nouveau dossier : lettres (accents compris), chiffres, espaces, - et _
FOLDER_NAME = re.compile(r"\w[\w\- ]{0,79}")
WRITE_SOURCES = ("visionneuse", "auto", "restauration")
MAX_SIZE = 50 * 1024 * 1024


def mtime(path):
    return str(path.stat().st_mtime_ns)


def _hidden(path):
    """Fichier caché ou temporaire (.tmp-…, .DS_Store), ou rangé dans un dossier caché."""
    return any(part.startswith(".") for part in path.relative_to(SCHEMAS_DIR).parts)


def relative(path):
    return path.resolve().relative_to(SCHEMAS_DIR.resolve()).as_posix()


def inside(relative_path):
    """Chemin disque d'un élément de schemas/ ("" : la racine), ou None s'il en sort."""
    root = SCHEMAS_DIR.resolve()
    path = (root / relative_path.strip("/")).resolve()
    if path != root and (root not in path.parents or _hidden(path)):
        return None
    return path


def space_root(ident):
    """Racine disque d'un espace, ou None si l'identifiant est invalide."""
    if not isinstance(ident, str) or not SPACE_ID.fullmatch(ident):
        return None
    return SPACES_DIR / ident


def list_folders():
    return sorted(relative(p) for p in SCHEMAS_DIR.rglob("*") if p.is_dir() and not _hidden(p))


def list_schemas():
    items = []
    for path in sorted(SCHEMAS_DIR.rglob("*.excalidraw")):
        if _hidden(path):
            continue
        folder = path.parent.relative_to(SCHEMAS_DIR).as_posix()
        items.append({
            "chemin": path.relative_to(SCHEMAS_DIR).as_posix(),
            "dossier": "" if folder == "." else folder,
            "modifie": mtime(path),
        })
    return items


def clean_views(data):
    """La liste de vues, vérifiée et normalisée, ou None si elle est invalide."""
    if not isinstance(data, list) or len(data) > 50:
        return None
    root, seen, views = SCHEMAS_DIR.resolve(), set(), []
    for item in data:
        if not isinstance(item, dict):
            return None
        ident, name, paths = item.get("id"), item.get("nom"), item.get("chemins", [])
        if not isinstance(ident, str) or not SPACE_ID.fullmatch(ident) or ident in seen:
            return None
        if not isinstance(name, str) or not 0 < len(name.strip()) <= 60:
            return None
        if not isinstance(paths, list) or len(paths) > 500:
            return None
        kept = []
        for raw in paths:
            path = inside(raw) if isinstance(raw, str) else None
            if path is None or path == root:
                return None
            if relative(path) not in kept:
                kept.append(relative(path))
        seen.add(ident)
        views.append({"id": ident, "nom": name.strip(), "chemins": kept})
    return views


def clean_spaces(data):
    """La liste d'espaces physiques ({id, nom}), ou None si elle est invalide."""
    if not isinstance(data, list) or len(data) > 50:
        return None
    seen, spaces = set(), []
    for item in data:
        if not isinstance(item, dict):
            return None
        ident, name = item.get("id"), item.get("nom")
        if not isinstance(ident, str) or not SPACE_ID.fullmatch(ident) or ident in seen:
            return None
        if not isinstance(name, str) or not 0 < len(name.strip()) <= 60:
            return None
        # un ancien format (vues) ne doit pas passer pour des espaces
        if "chemins" in item:
            return None
        seen.add(ident)
        spaces.append({"id": ident, "nom": name.strip()})
    return spaces


def read_views():
    try:
        return clean_views(json.loads(VIEWS_FILE.read_text(encoding="utf-8")).get("vues")) or []
    except (OSError, ValueError, AttributeError):
        return []


def write_views(views):
    write_atomic(VIEWS_FILE, json.dumps({"vues": views}, ensure_ascii=False, indent=2) + "\n")


def read_spaces():
    try:
        return clean_spaces(json.loads(SPACES_FILE.read_text(encoding="utf-8")).get("espaces")) or []
    except (OSError, ValueError, AttributeError):
        return []


def write_spaces(spaces):
    write_atomic(SPACES_FILE, json.dumps({"espaces": spaces}, ensure_ascii=False, indent=2) + "\n")


def follow_in_vues(old, new):
    """Un schéma ou un dossier est passé de old à new (None : supprimé) : les vues suivent."""
    views, changed = read_views(), False
    for view in views:
        paths = []
        for path in view["chemins"]:
            if path == old or path.startswith(f"{old}/"):
                changed = True
                if new is None:
                    continue
                path = new + path[len(old):]
            if path not in paths:
                paths.append(path)
        view["chemins"] = paths
    if changed:
        write_views(views)


# compatibilité pour les scripts qui appelaient encore follow_in_spaces
follow_in_spaces = follow_in_vues


def migrate_views_and_spaces():
    """Si vues.json manque et que espaces.json contient encore des chemins : migrer une fois."""
    SPACES_DIR.mkdir(parents=True, exist_ok=True)
    if VIEWS_FILE.exists():
        return
    try:
        raw = json.loads(SPACES_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        write_views([])
        write_spaces([])
        return
    items = raw.get("espaces") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        write_views([])
        return
    if items and isinstance(items[0], dict) and "chemins" in items[0]:
        views = clean_views(items)
        write_views(views if views is not None else [])
        write_spaces([])
        return
    write_views([])


def space_is_empty(path):
    """True si le dossier n'a rien d'utile (un .DS_Store ou .gitkeep ne compte pas)."""
    if not path.is_dir():
        return True
    skip = {".DS_Store", ".gitkeep"}
    return not any(child.name not in skip for child in path.iterdir())


def remove_space_tree(path):
    """Retire le contenu d'un espace (schémas + historique associé) puis le dossier."""
    if path.is_dir():
        for schema in [p for p in path.rglob("*.excalidraw") if not _hidden(p)]:
            follow_in_vues(relative(schema), None)
        shutil.rmtree(path)
    history_root = ROOT / "historique" / SPACES_ROOT_NAME / path.name
    if history_root.exists():
        shutil.rmtree(history_root, ignore_errors=True)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(VIEWER_DIR), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, fmt, *args):
        # la visionneuse relit la liste toutes les 3 secondes : on ne journalise pas ces lectures
        if self.command == "GET" and self.path.startswith((API, TREE_API, SPACES_API, VIEWS_API)):
            return
        super().log_message(fmt, *args)

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message, code=None):
        self._send_json(status, {"erreur": message, "code": code})

    def _same_origin(self):
        """Refuse les écritures venues d'une autre page que la visionneuse."""
        origin = self.headers.get("Origin")
        return origin is None or origin == f"http://{self.headers.get('Host', '')}"

    def _schema_path(self, url_path, prefix=API):
        """Chemin disque d'un schéma, ou None s'il sort de schemas/ ou n'est pas un .excalidraw."""
        path = (SCHEMAS_DIR / unquote(url_path[len(prefix) + 1:])).resolve()
        if path.suffix != ".excalidraw" or SCHEMAS_DIR.resolve() not in path.parents:
            return None
        return path

    def _read_json_body(self, max_size):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            return json.loads(self.rfile.read(length)) if 0 < length <= max_size else None
        except ValueError:
            return None

    def _space_id_from_path(self, url_path):
        """Identifiant dans /api/espaces/<id>, ou None."""
        if not url_path.startswith(SPACES_API + "/"):
            return None
        ident = unquote(url_path[len(SPACES_API) + 1:]).strip("/")
        return ident if SPACE_ID.fullmatch(ident) and "/" not in ident else None

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == API:
            return self._send_json(HTTPStatus.OK, list_schemas())
        if url.path == TREE_API:
            return self._send_json(HTTPStatus.OK, {
                "dossiers": list_folders(),
                "schemas": list_schemas(),
                "espaces": read_spaces(),
                "vues": read_views(),
            })
        if url.path == SPACES_API:
            return self._send_json(HTTPStatus.OK, {"espaces": read_spaces()})
        if url.path == VIEWS_API:
            return self._send_json(HTTPStatus.OK, {"vues": read_views()})
        if url.path.startswith(API + "/"):
            path = self._schema_path(url.path)
            if path is None or not path.is_file():
                return self._error(HTTPStatus.NOT_FOUND, "Schéma introuvable")
            try:
                content = json.loads(path.read_text(encoding="utf-8"))
            except ValueError:
                return self._error(HTTPStatus.UNPROCESSABLE_ENTITY, "Le fichier n'est pas du JSON valide")
            return self._send_json(HTTPStatus.OK, {"contenu": content, "modifie": mtime(path)})
        if url.path.startswith(HISTORY_API + "/"):
            return self._get_history(url)
        return super().do_GET()

    def _get_history(self, url):
        path = self._schema_path(url.path, HISTORY_API)
        if path is None:
            return self._error(HTTPStatus.NOT_FOUND, "Schéma introuvable")
        version = parse_qs(url.query).get("version", [None])[0]
        if version is None:
            return self._send_json(HTTPStatus.OK, list_versions(path))
        content = read_version(path, version)
        if content is None:
            return self._error(HTTPStatus.NOT_FOUND, "Version introuvable")
        try:
            return self._send_json(HTTPStatus.OK, {"contenu": json.loads(content), "version": version})
        except ValueError:
            return self._error(HTTPStatus.UNPROCESSABLE_ENTITY, "La version n'est pas du JSON valide")

    def _put_views(self):
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        body = self._read_json_body(256 * 1024)
        views = clean_views(body.get("vues")) if isinstance(body, dict) else None
        if views is None:
            return self._error(HTTPStatus.BAD_REQUEST, "Liste de vues invalide")
        write_views(views)
        return self._send_json(HTTPStatus.OK, {"vues": views})

    def _create_space(self):
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        body = self._read_json_body(64 * 1024)
        if not isinstance(body, dict):
            return self._error(HTTPStatus.BAD_REQUEST, "JSON invalide")
        name = body.get("nom")
        if not isinstance(name, str) or not 0 < len(name.strip()) <= 60:
            return self._error(HTTPStatus.BAD_REQUEST, "Nom d'espace invalide")
        name = name.strip()
        wanted = body.get("id")
        if wanted is None:
            wanted = _slug(name)
        if not isinstance(wanted, str) or not SPACE_ID.fullmatch(wanted):
            return self._error(HTTPStatus.BAD_REQUEST, "Identifiant d'espace invalide")
        spaces = read_spaces()
        taken = {e["id"] for e in spaces}
        ident = wanted
        n = 2
        while ident in taken or (SPACES_DIR / ident).exists():
            ident = f"{wanted[:28]}-{n}"
            n += 1
            if not SPACE_ID.fullmatch(ident):
                return self._error(HTTPStatus.BAD_REQUEST, "Identifiant d'espace invalide")
        root = space_root(ident)
        root.mkdir(parents=True, exist_ok=False)
        spaces.append({"id": ident, "nom": name})
        write_spaces(spaces)
        return self._send_json(HTTPStatus.CREATED, {"espace": {"id": ident, "nom": name}})

    def _rename_space(self, ident):
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        body = self._read_json_body(64 * 1024)
        if not isinstance(body, dict):
            return self._error(HTTPStatus.BAD_REQUEST, "JSON invalide")
        name = body.get("nom")
        if not isinstance(name, str) or not 0 < len(name.strip()) <= 60:
            return self._error(HTTPStatus.BAD_REQUEST, "Nom d'espace invalide")
        spaces = read_spaces()
        space = next((e for e in spaces if e["id"] == ident), None)
        if space is None:
            return self._error(HTTPStatus.NOT_FOUND, "Espace introuvable")
        space["nom"] = name.strip()
        write_spaces(spaces)
        return self._send_json(HTTPStatus.OK, {"espace": space})

    def _delete_space(self, ident, force):
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        spaces = read_spaces()
        space = next((e for e in spaces if e["id"] == ident), None)
        if space is None:
            return self._error(HTTPStatus.NOT_FOUND, "Espace introuvable")
        root = space_root(ident)
        if root is None:
            return self._error(HTTPStatus.BAD_REQUEST, "Identifiant d'espace invalide")
        if root.exists() and not space_is_empty(root) and not force:
            return self._error(HTTPStatus.CONFLICT, "L'espace n'est pas vide", "non_vide")
        if root.exists():
            if force:
                remove_space_tree(root)
            else:
                (root / ".DS_Store").unlink(missing_ok=True)
                (root / ".gitkeep").unlink(missing_ok=True)
                if root.exists():
                    root.rmdir()
        follow_in_vues(f"{SPACES_ROOT_NAME}/{ident}", None)
        write_spaces([e for e in spaces if e["id"] != ident])
        return self._send_json(HTTPStatus.OK, {"espace": space})

    def do_PUT(self):
        url = urlparse(self.path)
        if url.path == VIEWS_API:
            return self._put_views()
        space_id = self._space_id_from_path(url.path)
        if space_id is not None:
            return self._rename_space(space_id)
        if not url.path.startswith(API + "/"):
            return self._error(HTTPStatus.NOT_FOUND, "Adresse inconnue")
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        path = self._schema_path(url.path)
        if path is None:
            return self._error(HTTPStatus.BAD_REQUEST,
                               "Le chemin doit finir par .excalidraw et rester dans schemas/")
        length = int(self.headers.get("Content-Length") or 0)
        if not 0 < length <= MAX_SIZE:
            return self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Taille du schéma invalide")
        try:
            doc = json.loads(self.rfile.read(length))
        except ValueError:
            return self._error(HTTPStatus.BAD_REQUEST, "JSON invalide")
        if not isinstance(doc, dict) or doc.get("type") != "excalidraw":
            return self._error(HTTPStatus.BAD_REQUEST, "Ce n'est pas un schéma Excalidraw")

        query = parse_qs(url.query)
        source = query.get("source", ["visionneuse"])[0]
        if source not in WRITE_SOURCES:
            return self._error(HTTPStatus.BAD_REQUEST, f"Source inconnue : {source}")
        if path.exists():
            if "create" in query:
                return self._error(HTTPStatus.CONFLICT, "Un schéma porte déjà ce nom", "existe")
            expected = self.headers.get("X-Expected-Mtime")
            if expected and expected != mtime(path) and "force" not in query:
                return self._error(HTTPStatus.CONFLICT,
                                   "Le schéma a été modifié sur le disque depuis son ouverture",
                                   "conflit")
        write_with_history(path, json.dumps(doc, ensure_ascii=False, indent=2), source)
        return self._send_json(HTTPStatus.OK, {
            "chemin": path.relative_to(SCHEMAS_DIR.resolve()).as_posix(),
            "modifie": mtime(path),
        })

    def do_POST(self):
        url = urlparse(self.path)
        if url.path == SPACES_API:
            return self._create_space()
        if url.path not in (FOLDERS_API, MOVE_API):
            return self._error(HTTPStatus.NOT_FOUND, "Adresse inconnue")
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        body = self._read_json_body(64 * 1024)
        if not isinstance(body, dict):
            return self._error(HTTPStatus.BAD_REQUEST, "JSON invalide")
        if url.path == FOLDERS_API:
            return self._create_folder(str(body.get("chemin", "")))
        return self._move(str(body.get("de", "")), str(body.get("vers", "")))

    def _create_folder(self, relative_path):
        parts = relative_path.strip("/").split("/")
        if not all(FOLDER_NAME.fullmatch(part) and part == part.strip() for part in parts):
            return self._error(HTTPStatus.BAD_REQUEST, "Nom de dossier invalide : lettres, chiffres, "
                               "espaces, - et _ seulement", "nom")
        if parts == [SPACES_ROOT_NAME] or (len(parts) == 1 and parts[0] == SPACES_ROOT_NAME):
            return self._error(HTTPStatus.BAD_REQUEST,
                               "Le dossier « espaces » est réservé aux espaces de travail",
                               "reserve")
        # créer un sous-dossier direct de schemas/espaces/ hors API espaces : refusé
        if len(parts) == 2 and parts[0] == SPACES_ROOT_NAME:
            return self._error(HTTPStatus.BAD_REQUEST,
                               "Créez un espace depuis le hub Espaces, pas un dossier à cet endroit",
                               "reserve")
        path = inside("/".join(parts))
        if path is None:
            return self._error(HTTPStatus.BAD_REQUEST, "Le dossier doit rester dans schemas/")
        if path.exists():
            return self._error(HTTPStatus.CONFLICT, "Ce dossier existe déjà", "existe")
        path.mkdir(parents=True)
        return self._send_json(HTTPStatus.CREATED, {"chemin": relative(path)})

    def _move(self, source, destination):
        src, dest = inside(source), inside(destination)
        if (src is None or src == SCHEMAS_DIR.resolve() or not src.exists()
                or (src.is_file() and src.suffix != ".excalidraw")):
            return self._error(HTTPStatus.NOT_FOUND, "Schéma ou dossier introuvable")
        if dest is None or not dest.is_dir():
            return self._error(HTTPStatus.NOT_FOUND, "Dossier de destination introuvable")
        # le dossier réservé schemas/espaces et chaque racine d'espace ne se déplacent pas
        if src == SPACES_DIR.resolve() or src.parent == SPACES_DIR.resolve():
            return self._error(HTTPStatus.BAD_REQUEST,
                               "Un espace ne se déplace pas comme un dossier ordinaire", "reserve")
        if dest == SPACES_DIR.resolve():
            return self._error(HTTPStatus.BAD_REQUEST,
                               "Déposez dans un espace précis, pas dans le dossier réservé", "reserve")
        if src.is_dir() and (dest == src or src in dest.parents):
            return self._error(HTTPStatus.BAD_REQUEST, "Un dossier ne peut pas aller dans lui-même",
                               "boucle")
        target = dest / src.name
        if target == src:
            return self._send_json(HTTPStatus.OK, {"chemin": relative(src)})
        if target.exists():
            return self._error(HTTPStatus.CONFLICT,
                               "Le dossier de destination contient déjà un élément de ce nom", "existe")
        schemas = [src] if src.is_file() else [p for p in src.rglob("*.excalidraw") if not _hidden(p)]
        moves = [(p, target / p.relative_to(src) if src.is_dir() else target) for p in schemas]
        old = relative(src)
        os.rename(src, target)
        result = {"chemin": relative(target)}
        try:
            follow_in_vues(old, result["chemin"])
            for old_path, new_path in moves:
                move_history(old_path, new_path)
                record_move(old_path, new_path)
        except OSError as e:
            result["avertissement"] = f"Versions non déplacées : {e}"
        return self._send_json(HTTPStatus.OK, result)

    def do_DELETE(self):
        url = urlparse(self.path)
        space_id = self._space_id_from_path(url.path)
        if space_id is not None:
            force = "force" in parse_qs(url.query)
            return self._delete_space(space_id, force)
        if not url.path.startswith(FOLDERS_API + "/"):
            return self._error(HTTPStatus.NOT_FOUND, "Adresse inconnue")
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        path = inside(unquote(url.path[len(FOLDERS_API) + 1:]))
        if path is None or path == SCHEMAS_DIR.resolve() or not path.is_dir():
            return self._error(HTTPStatus.NOT_FOUND, "Dossier introuvable")
        if path == SPACES_DIR.resolve() or path.parent == SPACES_DIR.resolve():
            return self._error(HTTPStatus.BAD_REQUEST,
                               "Supprimez un espace depuis le hub Espaces", "reserve")
        # seul un dossier vide se supprime ; .DS_Store est le fichier que le Finder y laisse
        if any(child.name != ".DS_Store" for child in path.iterdir()):
            return self._error(HTTPStatus.CONFLICT, "Le dossier n'est pas vide", "non_vide")
        (path / ".DS_Store").unlink(missing_ok=True)
        gone = relative(path)
        path.rmdir()
        try:
            follow_in_vues(gone, None)
        except OSError:
            pass
        return self._send_json(HTTPStatus.OK, {"chemin": gone})


def _slug(name):
    """Identifiant d'espace tiré du nom : « À valider (CNSS) » → a-valider-cnss."""
    import unicodedata
    folded = unicodedata.normalize("NFD", name).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")[:32]
    return slug or "espace"


def local_ips():
    """Adresses IPv4 de cette machine, hors loopback, pour l'accès distant."""
    ips = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127.") and ip not in ips:
                ips.append(ip)
    except OSError:
        pass
    if not ips:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
                if not ip.startswith("127."):
                    ips.append(ip)
        except OSError:
            pass
    return ips


def main():
    parser = argparse.ArgumentParser(description="Visionneuse des schémas Excalidraw.")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="0.0.0.0",
                        help="adresse d'écoute (défaut 0.0.0.0 = réseau local ; "
                             "127.0.0.1 = cette machine seulement)")
    args = parser.parse_args()

    SCHEMAS_DIR.mkdir(exist_ok=True)
    migrate_views_and_spaces()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Visionneuse Excalidraw : http://localhost:{args.port}  (schémas : {SCHEMAS_DIR})")
    if args.host in ("0.0.0.0", ""):
        for ip in local_ips():
            print(f"  Accès distant    : http://{ip}:{args.port}")
    else:
        print(f"  Écoute           : {args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
