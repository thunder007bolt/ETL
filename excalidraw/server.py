"""
Visionneuse locale des schémas Excalidraw du projet.

Usage :
    python3 excalidraw/server.py              # http://localhost:8765
    python3 excalidraw/server.py --port 8800

Sert la visionneuse (viewer/) et une petite API sur les fichiers de schemas/ :
    GET  /api/arbre              dossiers (même vides) et schémas, pour l'arborescence
    GET  /api/schemas            liste des schémas
    GET  /api/schemas/<chemin>   contenu d'un schéma et date de modification
    PUT  /api/schemas/<chemin>   enregistre un schéma
                                 ?create=1 : refuse si le schéma existe déjà
                                 en-tête X-Expected-Mtime : refuse si le fichier a changé
                                 depuis son ouverture, sauf avec ?force=1
                                 ?source=restauration : l'écriture restaure une version
    GET  /api/historique/<chemin>                versions gardées d'un schéma
    GET  /api/historique/<chemin>?version=<id>   contenu d'une version
    POST /api/dossiers           crée un dossier : {"chemin": "prestations/archives"}
    DELETE /api/dossiers/<chemin>  supprime un dossier vide
    POST /api/deplacer           déplace un schéma ou un dossier dans un autre dossier :
                                 {"de": "prestations/x.excalidraw", "vers": "grh"} ("" : la racine)
Chaque enregistrement garde l'état précédent et le nouveau dans historique/ (lib/historique.py) ;
un déplacement emmène les versions du schéma avec lui.
N'écoute que sur 127.0.0.1.
"""

import argparse
import json
import os
import re
import sys
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "lib"))

from historique import (list_versions, move_history, read_version, record_move,  # noqa: E402
                        write_with_history)

SCHEMAS_DIR = ROOT / "schemas"
VIEWER_DIR = ROOT / "viewer"
API = "/api/schemas"
TREE_API = "/api/arbre"
FOLDERS_API = "/api/dossiers"
MOVE_API = "/api/deplacer"
HISTORY_API = "/api/historique"
# nom d'un nouveau dossier : lettres (accents compris), chiffres, espaces, - et _
FOLDER_NAME = re.compile(r"\w[\w\- ]{0,79}")
WRITE_SOURCES = ("visionneuse", "restauration")
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


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(VIEWER_DIR), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, fmt, *args):
        # la visionneuse relit la liste toutes les 3 secondes : on ne journalise pas ces lectures
        if self.command == "GET" and self.path.startswith((API, TREE_API)):
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

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == API:
            return self._send_json(HTTPStatus.OK, list_schemas())
        if url.path == TREE_API:
            return self._send_json(HTTPStatus.OK, {"dossiers": list_folders(), "schemas": list_schemas()})
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

    def do_PUT(self):
        url = urlparse(self.path)
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
        if url.path not in (FOLDERS_API, MOVE_API):
            return self._error(HTTPStatus.NOT_FOUND, "Adresse inconnue")
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length)) if 0 < length <= 64 * 1024 else None
        except ValueError:
            body = None
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
        os.rename(src, target)
        result = {"chemin": relative(target)}
        try:
            for old, new in moves:
                move_history(old, new)
                record_move(old, new)
        except OSError as e:
            result["avertissement"] = f"Versions non déplacées : {e}"
        return self._send_json(HTTPStatus.OK, result)

    def do_DELETE(self):
        url = urlparse(self.path)
        if not url.path.startswith(FOLDERS_API + "/"):
            return self._error(HTTPStatus.NOT_FOUND, "Adresse inconnue")
        if not self._same_origin():
            return self._error(HTTPStatus.FORBIDDEN, "Origine refusée")
        path = inside(unquote(url.path[len(FOLDERS_API) + 1:]))
        if path is None or path == SCHEMAS_DIR.resolve() or not path.is_dir():
            return self._error(HTTPStatus.NOT_FOUND, "Dossier introuvable")
        # seul un dossier vide se supprime ; .DS_Store est le fichier que le Finder y laisse
        if any(child.name != ".DS_Store" for child in path.iterdir()):
            return self._error(HTTPStatus.CONFLICT, "Le dossier n'est pas vide", "non_vide")
        (path / ".DS_Store").unlink(missing_ok=True)
        path.rmdir()
        return self._send_json(HTTPStatus.OK, {"chemin": relative(path)})


def main():
    parser = argparse.ArgumentParser(description="Visionneuse locale des schémas Excalidraw.")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    SCHEMAS_DIR.mkdir(exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"Visionneuse Excalidraw : http://localhost:{args.port}  (schémas : {SCHEMAS_DIR})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
