// Visionneuse des schémas Excalidraw : arborescence des dossiers et des schémas (glisser-déposer
// pour les ranger), espaces de travail (des vues par thème sur ces dossiers), édition et
// enregistrement sur le disque, à la main ou automatique.
import React, { useCallback, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import htm from "htm";
import { Excalidraw, serializeAsJSON } from "@excalidraw/excalidraw";

const html = htm.bind(React.createElement);
const API = "/api/schemas";
const TREE_API = "/api/arbre";
const FOLDERS_API = "/api/dossiers";
const MOVE_API = "/api/deplacer";
const HISTORY_API = "/api/historique";
const SPACES_API = "/api/espaces";
const POLL_MS = 3000;
const CLOSED_KEY = "schemas.dossiers-fermes";
// espace de travail affiché ("" : tous les schémas), retenu dans ce navigateur
const SPACE_KEY = "schemas.espace";
const NEW_SPACE = "\u0000nouveau";
// largeur de la liste, réglée en glissant son bord droit et retenue dans ce navigateur
const BAR_KEY = "schemas.largeur-liste";
const BAR_DEFAULT = 270;
const BAR_MIN = 200;
const BAR_MAX = 720;
// un serveur lancé avant les espaces de travail ne les connaît pas : il faut le relancer
const OLD_SERVER = "Relancez la visionneuse (server.py) : son serveur a été lancé avant les espaces de travail.";
const DRAG_TYPE = "application/x-visionneuse-schemas";
const OPEN_ON_HOVER_MS = 700;
// enregistrement automatique : toutes les N secondes si le schéma ouvert a changé (0 : désactivé) ;
// la fréquence choisie est retenue dans ce navigateur
const AUTO_KEY = "schemas.enregistrement-auto";
const AUTO_DEFAULT = 30;
const AUTO_CHOICES = [0, 10, 15, 30, 60, 120, 300, 600];
const AUTO_MIN = 5;
const AUTO_MAX = 3600;
const AUTO_LABEL = "Enregistrement auto";
// marqueur de lecture : une position par schéma, retenue dans ce navigateur (pas dans le fichier)
const MARKER_KEY = "schemas.marqueurs-lecture";
const UI_OPTIONS = { canvasActions: { loadScene: false, saveToActiveFile: false } };
const SOURCE_LABELS = {
  visionneuse: "Enregistrée dans la visionneuse",
  auto: "Enregistrement automatique",
  restauration: "Restauration d'une version",
  script: "Générée par un script",
  disque: "Trouvée sur le disque",
};

const encodePath = (path) => path.split("/").map(encodeURIComponent).join("/");
const label = (path) => path.split("/").pop().replace(/\.excalidraw$/, "").replace(/_/g, " ");
const formatDate = (iso) => new Date(iso).toLocaleString("fr-FR", { dateStyle: "short", timeStyle: "medium" });
const formatTime = (date) => date.toLocaleTimeString("fr-FR");
// 30 → « 30 s », 120 → « 2 min », 90 → « 1 min 30 s »
const formatDelay = (seconds) => {
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  if (!minutes) return `${rest} s`;
  return rest ? `${minutes} min ${rest} s` : `${minutes} min`;
};
// Excalidraw incrémente la version d'un élément à chaque modification
const versionSum = (elements) => elements.reduce((sum, e) => sum + e.version, 0);
const parentOf = (path) => (path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "");
const isWithin = (path, folder) => path === folder || path.startsWith(`${folder}/`);

// identifiant d'un espace de travail, tiré de son nom : « À valider (CNSS) » → a-valider-cnss
const slug = (name) => name.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
  .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 32) || "espace";
// un espace contient un schéma ou un dossier s'il le liste, ou s'il liste un dossier au-dessus
const inSpace = (space, path) => space.chemins.some((c) => isWithin(path, c));

async function request(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw Object.assign(new Error(payload.erreur || response.statusText), { code: payload.code });
  }
  return payload;
}

const postJson = (url, body) => request(url, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});
const readTree = () => request(TREE_API);
const readSchema = (path) => request(`${API}/${encodePath(path)}`);
const writeSchema = (path, body, { mtime, create, force, source } = {}) => {
  const query = new URLSearchParams();
  if (create) query.set("create", "1");
  else if (force) query.set("force", "1");
  if (source) query.set("source", source);
  const suffix = query.toString() ? `?${query}` : "";
  return request(`${API}/${encodePath(path)}${suffix}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json", ...(mtime ? { "X-Expected-Mtime": mtime } : {}) },
    body,
  });
};
const listVersions = (path) => request(`${HISTORY_API}/${encodePath(path)}`);
const readVersion = (path, version) =>
  request(`${HISTORY_API}/${encodePath(path)}?version=${encodeURIComponent(version)}`);

const emptyScene = () => JSON.stringify({
  type: "excalidraw",
  version: 2,
  source: "visionneuse",
  elements: [],
  appState: { viewBackgroundColor: "#ffffff", gridSize: null },
  files: {},
});

// cadre tout le schéma ; ne zoome que pour réduire, jamais pour agrandir un petit schéma
function fitView(api, elements) {
  const visible = elements.filter((e) => !e.isDeleted);
  if (!visible.length) return;
  const { width, height } = api.getAppState();
  const spanX = Math.max(...visible.map((e) => e.x + e.width)) - Math.min(...visible.map((e) => e.x));
  const spanY = Math.max(...visible.map((e) => e.y + e.height)) - Math.min(...visible.map((e) => e.y));
  const tooBig = spanX > width * 0.95 || spanY > height * 0.95;
  api.scrollToContent(visible, tooBig ? { fitToViewport: true, viewportZoomFactor: 0.95 } : undefined);
}

// arbre des dossiers : { nom, chemin, dossiers (Map), schemas } ; la racine a le chemin ""
function buildTree(folders, schemas) {
  const root = { nom: "", chemin: "", dossiers: new Map(), schemas: [] };
  const node = (path) => {
    let current = root;
    let acc = "";
    for (const part of path ? path.split("/") : []) {
      acc = acc ? `${acc}/${part}` : part;
      if (!current.dossiers.has(part)) {
        current.dossiers.set(part, { nom: part, chemin: acc, dossiers: new Map(), schemas: [] });
      }
      current = current.dossiers.get(part);
    }
    return current;
  };
  folders.forEach(node);
  schemas.forEach((s) => node(s.dossier).schemas.push(s));
  return root;
}
const findNode = (root, path) => {
  let current = root;
  for (const part of path.split("/")) {
    current = current.dossiers.get(part);
    if (!current) return null;
  }
  return current;
};
const countSchemas = (node) =>
  node.schemas.length + [...node.dossiers.values()].reduce((sum, f) => sum + countSchemas(f), 0);
const sortedFolders = (node) => [...node.dossiers.values()].sort((a, b) => a.nom.localeCompare(b.nom, "fr"));

// dossiers repliés, gardés d'une visite à l'autre dans ce navigateur
function loadClosed() {
  try {
    return new Set(JSON.parse(localStorage.getItem(CLOSED_KEY) || "[]"));
  } catch {
    return new Set();
  }
}
function saveClosed(closed) {
  try {
    localStorage.setItem(CLOSED_KEY, JSON.stringify([...closed]));
  } catch {
    // stockage indisponible (navigation privée) : le repli ne sera pas retenu
  }
}

function loadSpace() {
  try {
    return localStorage.getItem(SPACE_KEY) || "";
  } catch {
    return "";
  }
}
function storeSpace(id) {
  try {
    localStorage.setItem(SPACE_KEY, id);
  } catch {
    // stockage indisponible : la visionneuse rouvrira sur tous les schémas
  }
}

function loadBarWidth() {
  try {
    const width = Number(localStorage.getItem(BAR_KEY));
    return width >= BAR_MIN && width <= BAR_MAX ? width : BAR_DEFAULT;
  } catch {
    return BAR_DEFAULT;
  }
}
function storeBarWidth(width) {
  try {
    localStorage.setItem(BAR_KEY, String(width));
  } catch {
    // stockage indisponible : la liste reprendra sa largeur d'origine
  }
}

function loadAuto() {
  try {
    const stored = localStorage.getItem(AUTO_KEY);
    const seconds = stored === null ? AUTO_DEFAULT : Number(stored);
    return seconds === 0 || (seconds >= AUTO_MIN && seconds <= AUTO_MAX) ? seconds : AUTO_DEFAULT;
  } catch {
    return AUTO_DEFAULT;
  }
}
function storeAuto(seconds) {
  try {
    localStorage.setItem(AUTO_KEY, String(seconds));
  } catch {
    // stockage indisponible : la fréquence reviendra à 30 s au prochain chargement
  }
}

function loadMarkers() {
  try {
    const data = JSON.parse(localStorage.getItem(MARKER_KEY) || "{}");
    return data && typeof data === "object" ? data : {};
  } catch {
    return {};
  }
}
function storeMarkers(markers) {
  try {
    localStorage.setItem(MARKER_KEY, JSON.stringify(markers));
  } catch {
    // stockage indisponible : le marqueur ne sera pas retenu
  }
}
function markerFor(path) {
  const entry = loadMarkers()[path];
  if (!entry || !Number.isFinite(entry.x) || !Number.isFinite(entry.y)) return null;
  return { x: entry.x, y: entry.y };
}
function saveMarker(path, point) {
  const markers = loadMarkers();
  if (point) markers[path] = { x: point.x, y: point.y };
  else delete markers[path];
  storeMarkers(markers);
}
// coordonnées du schéma ↔ pixels de l'écran (formules d'Excalidraw)
function sceneToScreen(sceneX, sceneY, view) {
  const zoom = view.zoom || 1;
  return {
    x: (sceneX + view.scrollX) * zoom + (view.offsetLeft || 0),
    y: (sceneY + view.scrollY) * zoom + (view.offsetTop || 0),
  };
}
function screenToScene(clientX, clientY, view) {
  const zoom = view.zoom || 1;
  return {
    x: (clientX - (view.offsetLeft || 0)) / zoom - view.scrollX,
    y: (clientY - (view.offsetTop || 0)) / zoom - view.scrollY,
  };
}

const ICONS = {
  chevron: html`<path d="m9 18 6-6-6-6" />`,
  dossier: html`<path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z" />`,
  dossierPlus: html`<g><path d="M12 10v6" /><path d="M9 13h6" /><path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z" /></g>`,
  schema: html`<g><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z" /><path d="M14 2v4a2 2 0 0 0 2 2h4" /></g>`,
  schemaPlus: html`<g><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z" /><path d="M14 2v4a2 2 0 0 0 2 2h4" /><path d="M9 15h6" /><path d="M12 18v-6" /></g>`,
  corbeille: html`<g><path d="M3 6h18" /><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" /><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2" /></g>`,
  marqueur: html`<g><path d="M12 21s-7-5.3-7-11a7 7 0 0 1 14 0c0 5.7-7 11-7 11Z" /><circle cx="12" cy="10" r="2.5" /></g>`,
  espace: html`<g><path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z" /><path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65" /><path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65" /></g>`,
  espacePlus: html`<g><path d="m19 21-7-4-7 4V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16z" /><path d="M12 7v6" /><path d="M15 10H9" /></g>`,
  retirer: html`<g><path d="M18 6 6 18" /><path d="m6 6 12 12" /></g>`,
  coche: html`<path d="M20 6 9 17l-5-5" />`,
  bas: html`<path d="m6 9 6 6 6-6" />`,
};
const Icon = ({ name }) => html`<svg class="icone" viewBox="0 0 24 24" aria-hidden="true">${ICONS[name]}</svg>`;

// Fenêtre de question dans la page, à la place de confirm() et prompt() que certains navigateurs
// (dont celui de l'app Claude) bloquent. Répond la valeur du choix, le texte saisi, ou null.
function Dialog({ dialog, onClose }) {
  const [value, setValue] = useState(dialog.value ?? "");
  const focusRef = useRef(null);
  useEffect(() => {
    const el = focusRef.current;
    if (!el) return;
    el.focus();
    if (el.setSelectionRange) el.setSelectionRange(el.value.length, el.value.length);
  }, []);
  const main = dialog.choices.find((c) => c.fort) || dialog.choices[dialog.choices.length - 1];
  const answer = (choice) => onClose(dialog.texte ? value : choice.value);
  return html`
    <div class="voile" onMouseDown=${(e) => e.target === e.currentTarget && onClose(null)}>
      <form class="dialogue" role="dialog" aria-modal="true" aria-labelledby="dialogue-titre"
        onSubmit=${(e) => { e.preventDefault(); answer(main); }}
        onKeyDown=${(e) => { if (e.key === "Escape") { e.stopPropagation(); onClose(null); } }}>
        <h2 id="dialogue-titre">${dialog.title}</h2>
        ${dialog.message ? html`<p>${dialog.message}</p>` : null}
        ${dialog.texte ? html`<input ref=${focusRef} value=${value} spellCheck="false"
          onInput=${(e) => setValue(e.target.value)} />` : null}
        <div class=${dialog.colonne ? "dialogue-boutons colonne" : "dialogue-boutons"}>
          <button type="button" class="bouton annuler" onClick=${() => onClose(null)}>Annuler</button>
          ${dialog.choices.map((c) => html`
            <button key=${c.label} type=${c === main ? "submit" : "button"} class=${c.fort ? "bouton fort" : "bouton"}
              ref=${!dialog.texte && c === main ? focusRef : null}
              onClick=${c === main ? undefined : () => answer(c)}>${c.label}</button>`)}
        </div>
      </form>
    </div>`;
}

function App() {
  const [schemas, setSchemas] = useState([]);
  const [folders, setFolders] = useState([]);
  const [closed, setClosed] = useState(loadClosed);
  const [spaces, setSpaces] = useState([]); // espaces de travail : [{ id, nom, chemins }]
  const [spaceId, setSpaceId] = useState(loadSpace); // espace affiché ("" : tous les schémas)
  const [spaceMenu, setSpaceMenu] = useState(false);
  const [oldServer, setOldServer] = useState(false); // le serveur ne connaît pas les espaces
  const [dragging, setDragging] = useState(false);
  const [dropTarget, setDropTarget] = useState(null); // dossier survolé pendant un glisser ("" : la racine)
  const [scene, setScene] = useState(null); // { path, key, initial }
  const [dirty, setDirty] = useState(false);
  const [diskChanged, setDiskChanged] = useState(false);
  const [notice, setNotice] = useState(null); // { text, kind }
  const [sidebar, setSidebar] = useState(true);
  const [barWidth, setBarWidth] = useState(loadBarWidth); // largeur de la liste, en pixels
  const [resizing, setResizing] = useState(false); // on glisse le bord de la liste
  const [versionsOpen, setVersionsOpen] = useState(false);
  const [versions, setVersions] = useState(null); // { versions, actuelle } du schéma ouvert
  const [preview, setPreview] = useState(null); // version affichée en lecture seule
  const [autoSeconds, setAutoSeconds] = useState(loadAuto); // fréquence de l'enregistrement auto
  const [lastSave, setLastSave] = useState(null); // { at, auto } : dernier enregistrement du schéma
  const [marker, setMarker] = useState(null); // { x, y } dans le schéma ouvert, ou null
  const [view, setView] = useState(null); // cadrage Excalidraw pour afficher le marqueur

  const apiRef = useRef(null);
  const canevasRef = useRef(null);
  const markerRef = useRef(null);
  const lastCtxRef = useRef({ clientX: 0, clientY: 0 }); // dernier clic droit sur le canevas
  // bandeau marqueur en DOM (pas de setState : un re-render refermerait le menu natif)
  const markerDockRef = useRef(null);
  const placeMarkerAtRef = useRef(null);
  const goToMarkerRef = useRef(null);
  const clearMarkerRef = useRef(null);
  const previewRef = useRef(null);
  const versionsOpenRef = useRef(false);
  const pathRef = useRef(null);
  const mtimeRef = useRef(null);
  const baselineRef = useRef(null);
  // les changements faits par Excalidraw seul (mesure des textes au chargement des polices)
  // ne comptent pas comme des modifications tant que la personne n'a pas touché au schéma
  const interactedRef = useRef(false);
  const dirtyRef = useRef(false);
  const diskChangedRef = useRef(false);
  const savingRef = useRef(false);
  const fitPendingRef = useRef(false);
  const listJsonRef = useRef("");
  const loadCountRef = useRef(0);
  const dragRef = useRef(null); // élément glissé : { type: "schema" | "dossier", chemin }
  const hoverRef = useRef({ folder: null, timer: null }); // dossier replié qui s'ouvre au survol
  const [dialog, setDialog] = useState(null); // fenêtre de question ouverte
  const dialogRef = useRef(null); // sa fonction de réponse
  const saveRef = useRef(null);
  const spacesRef = useRef([]);
  const spaceIdRef = useRef(spaceId);
  const spaceMenuRef = useRef(null);
  const oldServerRef = useRef(false);

  const markDirty = (value) => {
    dirtyRef.current = value;
    setDirty(value);
  };
  const markDiskChanged = (value) => {
    diskChangedRef.current = value;
    setDiskChanged(value);
  };

  // pose une question dans la page ; choices : [{ label, value, fort }], « Annuler » répond null
  const ask = useCallback((options) => new Promise((resolve) => {
    dialogRef.current?.(null); // une seule question à la fois
    dialogRef.current = resolve;
    setDialog({ ...options, key: Date.now() });
  }), []);
  const askText = useCallback((options) =>
    ask({ ...options, texte: true, choices: [{ label: options.confirmLabel, fort: true }] }), [ask]);
  const closeDialog = (result) => {
    const resolve = dialogRef.current;
    dialogRef.current = null;
    setDialog(null);
    resolve?.(result);
  };

  // avant de quitter un schéma modifié : l'enregistrer, abandonner les modifications, ou rester
  const leaveDirty = useCallback(async (action) => {
    if (!dirtyRef.current) return true;
    const choice = await ask({
      title: "Modifications non enregistrées",
      message: `Le schéma ouvert a des modifications non enregistrées. Que faire avant de ${action} ?`,
      choices: [
        { label: "Abandonner les modifications", value: "abandon" },
        { label: "Enregistrer d'abord", value: "enregistrer", fort: true },
      ],
    });
    if (choice === "enregistrer") return saveRef.current();
    return choice === "abandon";
  }, [ask]);

  const flash = useCallback((text, kind = "ok") => {
    setNotice({ text, kind });
    if (kind === "ok") setTimeout(() => setNotice((n) => (n && n.text === text ? null : n)), 2500);
  }, []);

  const setShownVersion = (version) => {
    previewRef.current = version;
    setPreview(version);
  };

  const refreshVersions = useCallback(async () => {
    const path = pathRef.current;
    if (!path || !versionsOpenRef.current) return;
    try {
      const result = await listVersions(path);
      if (path === pathRef.current) setVersions(result);
    } catch (e) {
      flash(`Historique illisible : ${e.message}`, "erreur");
    }
  }, [flash]);

  const refreshList = useCallback(async () => {
    const tree = await readTree();
    const json = JSON.stringify(tree);
    if (json !== listJsonRef.current) {
      listJsonRef.current = json;
      setSchemas(tree.schemas);
      setFolders(tree.dossiers);
      spacesRef.current = tree.espaces || [];
      setSpaces(spacesRef.current);
      oldServerRef.current = tree.espaces === undefined;
      setOldServer(oldServerRef.current);
    }
    return tree;
  }, []);

  const updateClosed = (change) => setClosed((prev) => {
    const next = change(new Set(prev));
    saveClosed(next);
    return next;
  });
  // ouvre les dossiers qui mènent à path, pour qu'il soit visible dans l'arbre
  const reveal = (path) => updateClosed((set) => new Set([...set].filter((f) => !path.startsWith(`${f}/`))));
  const toggleFolder = (folder) => updateClosed((set) => {
    if (set.has(folder)) set.delete(folder);
    else set.add(folder);
    return set;
  });

  const open = useCallback(async (path, { force = false } = {}) => {
    if (!force && !(await leaveDirty("ouvrir un autre schéma"))) return;
    try {
      const { contenu, modifie } = await readSchema(path);
      // en rechargeant le même schéma, on garde le cadrage en cours
      const api = apiRef.current;
      const view = path === pathRef.current && api
        ? (({ scrollX, scrollY, zoom }) => ({ scrollX, scrollY, zoom }))(api.getAppState())
        : null;
      pathRef.current = path;
      mtimeRef.current = modifie;
      baselineRef.current = null;
      interactedRef.current = false;
      fitPendingRef.current = !view;
      markDirty(false);
      markDiskChanged(false);
      setShownVersion(null);
      markerDockRef.current?.remove();
      markerDockRef.current = null;
      setMarker(markerFor(path));
      if (!view) setLastSave(null);
      if (!view) setVersions(null); // autre schéma : l'historique affiché n'est plus le sien
      loadCountRef.current += 1;
      setScene({
        path,
        key: `${path}#${loadCountRef.current}`,
        initial: {
          elements: contenu.elements || [],
          files: contenu.files || {},
          appState: { ...(contenu.appState || {}), ...(view || {}) },
        },
      });
      history.replaceState(null, "", `#${encodePath(path)}`);
      document.title = `${label(path)} · Schémas`;
      reveal(path);
    } catch (e) {
      flash(`Ouverture impossible : ${e.message}`, "erreur");
    }
  }, [flash, leaveDirty]);

  // enregistre le schéma ouvert ; répond true si c'est fait. auto : enregistrement automatique,
  // sans message ni question (un conflit avec le disque le suspend jusqu'au rechargement)
  const save = useCallback(async ({ force = false, auto = false } = {}) => {
    const api = apiRef.current;
    const path = pathRef.current;
    // une version affichée de l'historique est en lecture seule : on la restaure, on ne l'enregistre pas
    if (!api || !path || savingRef.current || previewRef.current) return false;
    savingRef.current = true;
    // Excalidraw modifie les éléments sur place : la somme se prend avant l'envoi
    const sentSum = versionSum(api.getSceneElementsIncludingDeleted());
    const body = serializeAsJSON(api.getSceneElements(), api.getAppState(), api.getFiles(), "local");
    try {
      const result = await writeSchema(path, body,
        { mtime: mtimeRef.current, force, source: auto ? "auto" : undefined });
      mtimeRef.current = result.modifie;
      baselineRef.current = sentSum;
      // une retouche faite pendant l'envoi reste à enregistrer
      const stillChanged = apiRef.current === api && pathRef.current === path
        && versionSum(api.getSceneElementsIncludingDeleted()) !== sentSum;
      markDirty(stillChanged);
      markDiskChanged(false);
      setLastSave({ at: new Date(), auto });
      if (auto) setNotice((n) => (n?.text.startsWith(AUTO_LABEL) ? null : n));
      else flash("Enregistré");
      refreshList();
      refreshVersions();
      return true;
    } catch (e) {
      savingRef.current = false;
      if (auto) {
        if (e.code === "conflit") markDiskChanged(true);
        // en cas de conflit, « Modifié sur le disque » propose de recharger ; « Enregistrer » d'écraser
        flash(e.code === "conflit" ? `${AUTO_LABEL} suspendu` : `${AUTO_LABEL} impossible : ${e.message}`,
          "erreur");
        return false;
      }
      if (e.code === "conflit" && await ask({
        title: "Le schéma a changé sur le disque",
        message: "Il a été modifié depuis son ouverture (par Claude ou dans un autre onglet). "
          + "L'écraser avec votre version ? L'état actuel restera dans l'historique.",
        choices: [{ label: "Écraser avec ma version", value: true, fort: true }],
      })) {
        return await save({ force: true });
      }
      if (e.code !== "conflit") flash(`Enregistrement impossible : ${e.message}`, "erreur");
      return false;
    } finally {
      savingRef.current = false;
    }
  }, [ask, flash, refreshList, refreshVersions]);
  saveRef.current = save;

  // affiche une version de l'historique à la place du schéma, en lecture seule, au même cadrage
  const showVersion = useCallback(async (entry) => {
    const path = pathRef.current;
    const api = apiRef.current;
    if (!path || !(await leaveDirty("afficher cette version"))) return;
    try {
      const { contenu } = await readVersion(path, entry.version);
      const view = api ? (({ scrollX, scrollY, zoom }) => ({ scrollX, scrollY, zoom }))(api.getAppState()) : {};
      baselineRef.current = null;
      interactedRef.current = false;
      markDirty(false);
      setShownVersion(entry);
      loadCountRef.current += 1;
      setScene({
        path,
        key: `${path}#${loadCountRef.current}`,
        readOnly: true,
        initial: {
          elements: contenu.elements || [],
          files: contenu.files || {},
          appState: { ...(contenu.appState || {}), ...view },
        },
      });
    } catch (e) {
      flash(`Lecture de la version impossible : ${e.message}`, "erreur");
    }
  }, [flash, leaveDirty]);

  const restore = useCallback(async (entry) => {
    const path = pathRef.current;
    if (!path || !(await leaveDirty("restaurer cette version"))) return;
    if (!(await ask({
      title: `Restaurer la version du ${formatDate(entry.date)} ?`,
      message: "L'état actuel reste dans l'historique : on peut y revenir en le restaurant à son tour.",
      choices: [{ label: "Restaurer", value: true, fort: true }],
    }))) return;
    savingRef.current = true; // le suivi du disque ne doit pas recharger pendant la restauration
    try {
      const { contenu } = await readVersion(path, entry.version);
      await writeSchema(path, JSON.stringify(contenu), { mtime: mtimeRef.current, source: "restauration" });
      await open(path, { force: true });
      await refreshList();
      refreshVersions();
      flash("Version restaurée");
    } catch (e) {
      flash(e.code === "conflit"
        ? "Le schéma a changé sur le disque : rechargez-le, puis restaurez la version."
        : `Restauration impossible : ${e.message}`, "erreur");
    } finally {
      savingRef.current = false;
    }
  }, [ask, flash, leaveDirty, open, refreshList, refreshVersions]);

  const chooseAuto = useCallback(async (value) => {
    let seconds = Number(value);
    if (value === "autre") {
      const answer = await askText({
        title: "Enregistrement automatique",
        message: `Toutes les combien de secondes ? Entre ${AUTO_MIN} et ${AUTO_MAX} (une heure).`,
        value: String(autoSeconds || AUTO_DEFAULT),
        confirmLabel: "Valider",
      });
      if (answer === null || !answer.trim()) return;
      seconds = Math.round(Number(answer.trim().replace(",", ".")));
      if (!Number.isFinite(seconds) || seconds < AUTO_MIN || seconds > AUTO_MAX) {
        flash(`Fréquence refusée (${AUTO_MIN} à ${AUTO_MAX} s)`, "erreur");
        return;
      }
    }
    setAutoSeconds(seconds);
    storeAuto(seconds);
    flash(seconds ? `${AUTO_LABEL} : ${formatDelay(seconds)}` : `${AUTO_LABEL} désactivé`);
  }, [askText, autoSeconds, flash]);

  const toggleVersions = () => {
    versionsOpenRef.current = !versionsOpenRef.current;
    setVersionsOpen(versionsOpenRef.current);
  };

  // --- espaces de travail : des vues par thème ; rien n'est déplacé sur le disque
  const chooseSpace = (id) => {
    spaceIdRef.current = id;
    setSpaceId(id);
    storeSpace(id);
    setSpaceMenu(false);
  };
  const saveSpaces = useCallback(async (next) => {
    if (oldServerRef.current) {
      flash(OLD_SERVER, "erreur");
      return false;
    }
    try {
      const result = await request(SPACES_API, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ espaces: next }),
      });
      spacesRef.current = result.espaces;
      setSpaces(result.espaces);
      refreshList();
      return true;
    } catch (e) {
      flash(e.message === "Adresse inconnue" ? OLD_SERVER : `Espaces non enregistrés : ${e.message}`,
        "erreur");
      return false;
    }
  }, [flash, refreshList]);
  // crée un espace (avec, au besoin, un premier dossier ou schéma) ; répond son identifiant
  const createSpace = useCallback(async (first) => {
    setSpaceMenu(false);
    if (oldServerRef.current) {
      flash(OLD_SERVER, "erreur");
      return null;
    }
    const answer = await askText({
      title: "Nouvel espace de travail",
      message: "Un espace est une vue sur vos dossiers, par thème : « Réunion CNSS », « GRH »… "
        + "Rien n'est déplacé sur le disque.",
      confirmLabel: "Créer",
    });
    const name = answer && answer.trim().slice(0, 60);
    if (!name) return null;
    const taken = new Set(spacesRef.current.map((e) => e.id));
    let id = slug(name);
    for (let n = 2; taken.has(id); n += 1) id = `${slug(name).slice(0, 28)}-${n}`;
    const ok = await saveSpaces([...spacesRef.current, { id, nom: name, chemins: first ? [first] : [] }]);
    return ok ? id : null;
  }, [askText, saveSpaces]);
  const renameSpace = useCallback(async (space) => {
    setSpaceMenu(false);
    const answer = await askText({ title: "Renommer l'espace", value: space.nom, confirmLabel: "Renommer" });
    const name = answer && answer.trim().slice(0, 60);
    if (!name || name === space.nom) return;
    if (await saveSpaces(spacesRef.current.map((e) => (e.id === space.id ? { ...e, nom: name } : e)))) {
      flash("Espace renommé");
    }
  }, [askText, flash, saveSpaces]);
  const deleteSpace = useCallback(async (space) => {
    setSpaceMenu(false);
    if (!(await ask({
      title: `Supprimer l'espace « ${space.nom} » ?`,
      message: "Seule la vue disparaît : les dossiers et les schémas restent à leur place.",
      choices: [{ label: "Supprimer l'espace", value: true, fort: true }],
    }))) return;
    if (await saveSpaces(spacesRef.current.filter((e) => e.id !== space.id))) {
      if (spaceIdRef.current === space.id) chooseSpace("");
      flash("Espace supprimé");
    }
  }, [ask, flash, saveSpaces]);
  const addToSpace = useCallback(async (path) => {
    if (oldServerRef.current) {
      flash(OLD_SERVER, "erreur");
      return;
    }
    const all = spacesRef.current;
    const picked = all.length === 0 ? NEW_SPACE : await ask({
      title: "Ajouter à un espace de travail",
      message: `« ${label(path)} » apparaîtra dans l'espace choisi. Il reste à sa place sur le disque.`,
      colonne: true,
      choices: [
        ...all.map((e) => ({ label: inSpace(e, path) ? `${e.nom} (déjà dedans)` : e.nom, value: e.id })),
        { label: "+ Nouvel espace…", value: NEW_SPACE, fort: true },
      ],
    });
    if (!picked) return;
    if (picked === NEW_SPACE) {
      if (await createSpace(path)) flash("Espace créé");
      return;
    }
    const space = all.find((e) => e.id === picked);
    if (!space || inSpace(space, path)) return;
    // un dossier ajouté remplace ce qu'il contient déjà dans l'espace
    const kept = space.chemins.filter((c) => !isWithin(c, path));
    if (await saveSpaces(all.map((e) => (e.id === picked ? { ...e, chemins: [...kept, path] } : e)))) {
      flash(`Ajouté à « ${space.nom} »`);
    }
  }, [ask, createSpace, flash, saveSpaces]);
  // la croix est vite cliquée par mégarde : on demande avant de retirer
  const removeFromSpace = useCallback(async (path) => {
    const id = spaceIdRef.current;
    const space = spacesRef.current.find((e) => e.id === id);
    if (!space) return;
    if (!(await ask({
      title: `Retirer « ${label(path)} » de l'espace « ${space.nom} » ?`,
      message: "Il disparaît seulement de cet espace. Il reste à sa place sur le disque, et dans "
        + "« Tous les schémas ».",
      choices: [{ label: "Retirer de l'espace", value: true, fort: true }],
    }))) return;
    const next = spacesRef.current.map((e) =>
      (e.id === id ? { ...e, chemins: e.chemins.filter((c) => c !== path) } : e));
    if (await saveSpaces(next)) flash("Retiré de l'espace");
  }, [ask, flash, saveSpaces]);
  // ce qu'on crée depuis un espace y apparaît, même rangé hors de ses dossiers
  const showInSpace = useCallback(async (path) => {
    const space = spacesRef.current.find((e) => e.id === spaceIdRef.current);
    if (!space || inSpace(space, path)) return;
    await saveSpaces(spacesRef.current.map((e) =>
      (e.id === space.id ? { ...e, chemins: [...e.chemins, path] } : e)));
  }, [saveSpaces]);
  // dossier proposé pour un nouveau schéma : celui de l'espace, s'il n'en a qu'un
  const spaceFolder = () => {
    const space = spacesRef.current.find((e) => e.id === spaceIdRef.current);
    const own = space ? space.chemins.filter((c) => !c.endsWith(".excalidraw")) : [];
    return own.length === 1 ? own[0] : "";
  };

  const createSchema = useCallback(async (folder = "") => {
    const base = folder || spaceFolder();
    const answer = await askText({
      title: "Nouveau schéma",
      message: "Son nom, précédé de son dossier : par exemple grh/effectif.",
      value: base ? `${base}/` : "",
      confirmLabel: "Créer",
    });
    if (!answer || !answer.trim()) return;
    const path = `${answer.trim().replace(/^\/+/, "").replace(/\.excalidraw$/, "")}.excalidraw`;
    try {
      await writeSchema(path, emptyScene(), { create: true });
      await showInSpace(path);
      await refreshList();
      open(path);
    } catch (e) {
      flash(e.code === "existe" ? "Un schéma porte déjà ce nom." : `Création impossible : ${e.message}`,
        "erreur");
    }
  }, [askText, flash, open, refreshList, showInSpace]);

  const createFolder = useCallback(async (parent = "") => {
    const base = parent || spaceFolder();
    const answer = await askText({
      title: "Nouveau dossier",
      message: "Son nom, précédé de son dossier parent : par exemple prestations/archives.",
      value: base ? `${base}/` : "",
      confirmLabel: "Créer",
    });
    if (!answer || !answer.trim()) return;
    const path = answer.trim().replace(/^\/+|\/+$/g, "");
    try {
      await postJson(FOLDERS_API, { chemin: path });
      await showInSpace(path);
      reveal(path);
      await refreshList();
      flash("Dossier créé");
    } catch (e) {
      flash(e.code === "existe" ? "Ce dossier existe déjà." : `Création impossible : ${e.message}`, "erreur");
    }
  }, [askText, flash, refreshList, showInSpace]);

  const removeFolder = useCallback(async (folder) => {
    try {
      await request(`${FOLDERS_API}/${encodePath(folder)}`, { method: "DELETE" });
      await refreshList();
      flash("Dossier supprimé");
    } catch (e) {
      flash(e.code === "non_vide" ? "Le dossier n'est pas vide." : `Suppression impossible : ${e.message}`,
        "erreur");
    }
  }, [flash, refreshList]);

  // range un schéma ou un dossier dans folder ; le schéma ouvert suit, même non enregistré
  const move = useCallback(async (item, folder) => {
    try {
      const result = await postJson(MOVE_API, { de: item.chemin, vers: folder });
      const moved = (path) => result.chemin + path.slice(item.chemin.length);
      const opened = pathRef.current;
      if (opened && isWithin(opened, item.chemin)) {
        const path = moved(opened);
        pathRef.current = path;
        setScene((s) => (s ? { ...s, path } : s));
        history.replaceState(null, "", `#${encodePath(path)}`);
      }
      if (item.type === "dossier") {
        updateClosed((set) => new Set([...set].map((f) => (isWithin(f, item.chemin) ? moved(f) : f))));
      }
      reveal(result.chemin);
      await refreshList();
      refreshVersions();
      if (result.avertissement) flash(result.avertissement, "erreur");
      else flash(`Rangé dans ${folder || "la racine"}`);
    } catch (e) {
      flash(e.code === "existe" ? "Le dossier contient déjà un élément de ce nom."
        : `Déplacement impossible : ${e.message}`, "erreur");
    }
  }, [flash, refreshList, refreshVersions]);

  // glisser-déposer dans l'arbre : un schéma ou un dossier se dépose sur un dossier, sur un schéma
  // (il rejoint son dossier) ou sur la zone « racine »
  const canDrop = (folder) => {
    const item = dragRef.current;
    if (folder === "" && spaceIdRef.current) return false; // pas de racine dans un espace
    return !!item && parentOf(item.chemin) !== folder
      && !(item.type === "dossier" && isWithin(folder, item.chemin));
  };
  const armOpen = (folder) => {
    const hover = hoverRef.current;
    if (hover.folder === folder) return;
    clearTimeout(hover.timer);
    hoverRef.current = {
      folder,
      timer: folder && setTimeout(() => updateClosed((set) => {
        set.delete(folder);
        return set;
      }), OPEN_ON_HOVER_MS),
    };
  };
  const startDrag = (e, item) => {
    e.stopPropagation();
    dragRef.current = item;
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData(DRAG_TYPE, item.chemin);
    setTimeout(() => setDragging(true), 0); // changer la page pendant dragstart annulerait le glisser
  };
  const endDrag = () => {
    dragRef.current = null;
    armOpen(null);
    setDragging(false);
    setDropTarget(null);
  };
  const dragOver = (e, folder, { openable = false } = {}) => {
    if (!dragRef.current) return; // un fichier venu d'ailleurs : pas pour l'arbre
    e.stopPropagation();
    armOpen(openable && closed.has(folder) ? folder : null);
    if (!canDrop(folder)) {
      setDropTarget(null);
      return;
    }
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    setDropTarget(folder);
  };
  const dropOn = (e, folder) => {
    const item = dragRef.current;
    if (!item) return;
    e.preventDefault();
    e.stopPropagation();
    const allowed = canDrop(folder);
    endDrag();
    if (allowed) move(item, folder);
  };
  const leaveTree = (e) => {
    if (!e.currentTarget.contains(e.relatedTarget)) setDropTarget(null);
  };

  const onChange = useCallback((elements, appState) => {
    if (fitPendingRef.current && apiRef.current) {
      fitPendingRef.current = false;
      fitView(apiRef.current, elements);
    }
    if (appState) {
      const next = {
        scrollX: appState.scrollX,
        scrollY: appState.scrollY,
        zoom: appState.zoom?.value ?? 1,
        offsetLeft: appState.offsetLeft || 0,
        offsetTop: appState.offsetTop || 0,
        width: appState.width,
        height: appState.height,
      };
      setView((prev) => (prev
        && prev.scrollX === next.scrollX && prev.scrollY === next.scrollY
        && prev.zoom === next.zoom && prev.offsetLeft === next.offsetLeft
        && prev.offsetTop === next.offsetTop
        && prev.width === next.width && prev.height === next.height
        ? prev : next));
    }
    const sum = versionSum(elements);
    if (baselineRef.current === null || !interactedRef.current) {
      baselineRef.current = sum;
      return;
    }
    const changed = sum !== baselineRef.current;
    if (changed !== dirtyRef.current) markDirty(changed);
  }, []);

  const onApi = useCallback((api) => { apiRef.current = api; }, []);
  const markInteraction = () => { interactedRef.current = true; };

  markerRef.current = marker;

  const closeNativeContextMenu = () => {
    markerDockRef.current?.remove();
    markerDockRef.current = null;
    // Excalidraw ferme son menu au Escape
    dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
  };

  const placeMarkerAt = useCallback((clientX, clientY) => {
    const path = pathRef.current;
    const api = apiRef.current;
    if (!path || !api) return;
    const state = api.getAppState();
    const point = screenToScene(clientX, clientY, {
      scrollX: state.scrollX,
      scrollY: state.scrollY,
      zoom: state.zoom?.value ?? 1,
      offsetLeft: state.offsetLeft || 0,
      offsetTop: state.offsetTop || 0,
    });
    saveMarker(path, point);
    setMarker(point);
    closeNativeContextMenu();
    flash("Marqueur posé");
  }, [flash]);

  const goToMarker = useCallback(() => {
    const api = apiRef.current;
    const point = markerRef.current;
    if (!api || !point) return;
    const state = api.getAppState();
    const zoom = state.zoom?.value ?? 1;
    api.updateScene({
      appState: {
        scrollX: (state.width / 2) / zoom - point.x,
        scrollY: (state.height / 2) / zoom - point.y,
      },
    });
    closeNativeContextMenu();
  }, []);

  const clearMarker = useCallback(() => {
    const path = pathRef.current;
    if (!path) return;
    saveMarker(path, null);
    setMarker(null);
    closeNativeContextMenu();
    flash("Marqueur retiré");
  }, [flash]);

  placeMarkerAtRef.current = placeMarkerAt;
  goToMarkerRef.current = goToMarker;
  clearMarkerRef.current = clearMarker;

  // retient la position du clic droit sans bloquer le menu natif Excalidraw
  const noteContextCoords = useCallback((e) => {
    lastCtxRef.current = { clientX: e.clientX, clientY: e.clientY };
  }, []);

  // colle les options marqueur au-dessus du menu natif, en DOM pur (évite de refermer le menu)
  useEffect(() => {
    const root = canevasRef.current;
    if (!root) return undefined;

    const makeBtn = (label, onClick) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.setAttribute("role", "menuitem");
      btn.textContent = label;
      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        onClick();
      });
      return btn;
    };

    const removeDock = () => {
      markerDockRef.current?.remove();
      markerDockRef.current = null;
    };

    const syncDock = () => {
      const menu = root.querySelector(".context-menu");
      const rect = root.getBoundingClientRect();
      if (!menu || !pathRef.current || previewRef.current) {
        removeDock();
        return;
      }
      const box = menu.getBoundingClientRect();
      let dock = markerDockRef.current;
      if (!dock) {
        dock = document.createElement("div");
        dock.className = "menu-marqueur";
        dock.setAttribute("role", "menu");
        dock.setAttribute("aria-label", "Marqueur de lecture");
        dock.addEventListener("pointerdown", (e) => e.stopPropagation());
        root.appendChild(dock);
        markerDockRef.current = dock;
      }
      dock.style.left = `${box.left - rect.left}px`;
      dock.style.top = `${box.top - rect.top}px`;
      dock.style.width = `${Math.max(box.width, 200)}px`;

      const has = !!markerRef.current;
      const wanted = has
        ? ["Déplacer le marqueur ici", "Aller au marqueur", "Retirer le marqueur"]
        : ["Poser le marqueur ici"];
      const current = [...dock.querySelectorAll("button")].map((b) => b.textContent);
      if (current.join("|") === wanted.join("|")) return;

      dock.replaceChildren();
      dock.appendChild(makeBtn(wanted[0], () => {
        const { clientX, clientY } = lastCtxRef.current;
        placeMarkerAtRef.current?.(clientX, clientY);
      }));
      if (has) {
        dock.appendChild(makeBtn("Aller au marqueur", () => goToMarkerRef.current?.()));
        dock.appendChild(makeBtn("Retirer le marqueur", () => clearMarkerRef.current?.()));
      }
    };

    const obs = new MutationObserver(syncDock);
    obs.observe(root, { childList: true, subtree: true });
    syncDock();
    return () => {
      obs.disconnect();
      removeDock();
    };
  }, []);

  // premier chargement : le schéma de l'adresse (#dossier/nom.excalidraw) ou le premier de la liste
  useEffect(() => {
    refreshList()
      .then((tree) => {
        const wanted = decodeURIComponent(location.hash.slice(1));
        const space = (tree.espaces || []).find((e) => e.id === spaceIdRef.current);
        // l'espace retenu n'existe plus, ou le schéma demandé par l'adresse n'y est pas : tout afficher
        const asked = tree.schemas.find((s) => s.chemin === wanted);
        if (spaceIdRef.current && (!space || (asked && !inSpace(space, asked.chemin)))) chooseSpace("");
        const visible = space && !asked ? tree.schemas.filter((s) => inSpace(space, s.chemin)) : tree.schemas;
        const first = asked || visible[0] || tree.schemas[0];
        if (first) open(first.chemin, { force: true });
      })
      .catch((e) => flash(`Serveur injoignable : ${e.message}`, "erreur"));
  }, []);

  useEffect(() => {
    const onHash = () => {
      const wanted = decodeURIComponent(location.hash.slice(1));
      if (wanted && wanted !== pathRef.current) open(wanted);
    };
    addEventListener("hashchange", onHash);
    return () => removeEventListener("hashchange", onHash);
  }, [open]);

  // suit les changements sur le disque : nouveaux schémas, schéma ouvert régénéré par Claude
  useEffect(() => {
    const timer = setInterval(async () => {
      let list;
      try {
        list = (await refreshList()).schemas;
      } catch {
        return;
      }
      const path = pathRef.current;
      const entry = path && list.find((s) => s.chemin === path);
      if (!entry || savingRef.current || entry.modifie === mtimeRef.current) return;
      if (dirtyRef.current || previewRef.current) markDiskChanged(true);
      else open(path, { force: true });
    }, POLL_MS);
    return () => clearInterval(timer);
  }, [open, refreshList]);

  // enregistrement automatique : à chaque tour, le schéma ouvert est enregistré s'il a changé,
  // sauf pendant une question, une version affichée en lecture seule ou un conflit avec le disque
  useEffect(() => {
    if (!autoSeconds) return undefined;
    const timer = setInterval(() => {
      if (!dirtyRef.current || savingRef.current || previewRef.current || dialogRef.current
        || diskChangedRef.current) return;
      saveRef.current({ auto: true });
    }, autoSeconds * 1000);
    return () => clearInterval(timer);
  }, [autoSeconds]);

  useEffect(() => {
    if (versionsOpen) refreshVersions();
  }, [versionsOpen, scene?.key, refreshVersions]);

  // le menu des espaces se ferme au clic ailleurs et à Échap
  useEffect(() => {
    if (!spaceMenu) return undefined;
    const onDown = (e) => {
      if (!spaceMenuRef.current?.contains(e.target)) setSpaceMenu(false);
    };
    const onKey = (e) => {
      if (e.key === "Escape") setSpaceMenu(false);
    };
    addEventListener("mousedown", onDown);
    addEventListener("keydown", onKey);
    return () => {
      removeEventListener("mousedown", onDown);
      removeEventListener("keydown", onKey);
    };
  }, [spaceMenu]);

  useEffect(() => {
    const onKey = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        e.stopPropagation();
        save();
      }
    };
    const onUnload = (e) => {
      if (dirtyRef.current) {
        e.preventDefault();
        e.returnValue = "";
      }
    };
    addEventListener("keydown", onKey, true);
    addEventListener("beforeunload", onUnload);
    return () => {
      removeEventListener("keydown", onKey, true);
      removeEventListener("beforeunload", onUnload);
    };
  }, [save]);

  // la liste s'étire en glissant son bord droit ; elle laisse toujours de la place au schéma
  const clampBar = (width) => Math.round(Math.max(BAR_MIN, Math.min(width, BAR_MAX, innerWidth - 320)));
  const startResize = (e) => {
    e.preventDefault();
    e.currentTarget.setPointerCapture(e.pointerId);
    setResizing(true);
  };
  const moveResize = (e) => {
    if (resizing) setBarWidth(clampBar(e.clientX));
  };
  const endResize = (e) => {
    if (!resizing) return;
    setResizing(false);
    const width = clampBar(e.clientX);
    setBarWidth(width);
    storeBarWidth(width);
  };
  const keyResize = (e) => {
    const step = { ArrowLeft: -16, ArrowRight: 16 }[e.key];
    if (!step) return;
    e.preventDefault();
    const width = clampBar(barWidth + step);
    setBarWidth(width);
    storeBarWidth(width);
  };
  const resetBar = () => {
    setBarWidth(BAR_DEFAULT);
    storeBarWidth(BAR_DEFAULT);
  };

  const current = scene?.path;
  const indent = (depth) => ({ paddingLeft: `${8 + depth * 16}px` });

  // pinned : le dossier ou le schéma est listé par l'espace affiché (il en est une entrée)
  const spaceActions = (path, pinned) => html`
    <button class="icone-bouton" title="Ajouter à un espace de travail"
      onClick=${() => addToSpace(path)}><${Icon} name="espacePlus" /></button>
    ${pinned ? html`<button class="icone-bouton" title="Retirer de cet espace (rien n'est supprimé)"
      onClick=${() => removeFromSpace(path)}><${Icon} name="retirer" /></button>` : null}`;

  const renderFolder = (node, depth, pinned = false) => {
    const isClosed = closed.has(node.chemin);
    const isEmpty = node.dossiers.size === 0 && node.schemas.length === 0;
    return html`
      <li key=${`dossier:${node.chemin}`}>
        <div class=${dropTarget === node.chemin ? "ligne dossier cible" : "ligne dossier"} style=${indent(depth)}
          role="button" tabIndex="0" draggable="true" title=${node.chemin}
          aria-expanded=${!isClosed} onClick=${() => toggleFolder(node.chemin)}
          onKeyDown=${(e) => e.key === "Enter" && toggleFolder(node.chemin)}
          onDragStart=${(e) => startDrag(e, { type: "dossier", chemin: node.chemin })} onDragEnd=${endDrag}
          onDragOver=${(e) => dragOver(e, node.chemin, { openable: true })}
          onDrop=${(e) => dropOn(e, node.chemin)}>
          <span class=${isClosed ? "chevron" : "chevron ouvert"}><${Icon} name="chevron" /></span>
          <${Icon} name="dossier" />
          <span class="nom">${node.nom}</span>
          ${pinned && parentOf(node.chemin)
            ? html`<span class="origine" title=${node.chemin}>${parentOf(node.chemin)}</span>` : null}
          ${isClosed ? html`<span class="compte">${countSchemas(node)}</span>` : null}
          <span class="actions" onClick=${(e) => e.stopPropagation()}>
            ${spaceActions(node.chemin, pinned)}
            <button class="icone-bouton" title="Nouveau schéma dans ce dossier"
              onClick=${() => createSchema(node.chemin)}><${Icon} name="schemaPlus" /></button>
            <button class="icone-bouton" title="Nouveau sous-dossier"
              onClick=${() => createFolder(node.chemin)}><${Icon} name="dossierPlus" /></button>
            ${isEmpty ? html`<button class="icone-bouton" title="Supprimer ce dossier vide"
              onClick=${() => removeFolder(node.chemin)}><${Icon} name="corbeille" /></button>` : null}
          </span>
        </div>
        ${isClosed ? null : html`<ul>${renderChildren(node, depth + 1)}</ul>`}
      </li>`;
  };

  const renderSchema = (s, depth, pinned = false) => html`
    <li key=${s.chemin}>
      <div class=${s.chemin === current ? "ligne schema actif" : "ligne schema"} style=${indent(depth + 1)}
        role="button" tabIndex="0" draggable="true" title=${s.chemin}
        onClick=${() => open(s.chemin)} onKeyDown=${(e) => e.key === "Enter" && open(s.chemin)}
        onDragStart=${(e) => startDrag(e, { type: "schema", chemin: s.chemin })} onDragEnd=${endDrag}
        onDragOver=${(e) => dragOver(e, s.dossier)} onDrop=${(e) => dropOn(e, s.dossier)}>
        <${Icon} name="schema" />
        <span class="nom">${label(s.chemin)}</span>
        ${pinned && s.dossier ? html`<span class="origine" title=${s.chemin}>${s.dossier}</span>` : null}
        <span class="actions" onClick=${(e) => e.stopPropagation()}>${spaceActions(s.chemin, pinned)}</span>
      </div>
    </li>`;

  const renderChildren = (node, depth) => [
    ...sortedFolders(node).map((f) => renderFolder(f, depth)),
    ...node.schemas.map((s) => renderSchema(s, depth)),
  ];

  const tree = buildTree(folders, schemas);
  const space = spaces.find((e) => e.id === spaceId) || null;
  // les entrées de l'espace affiché : ses dossiers puis ses schémas, ceux qui existent encore
  const spaceItems = () => {
    const nodes = space.chemins.filter((c) => !c.endsWith(".excalidraw"))
      .map((c) => findNode(tree, c)).filter(Boolean)
      .sort((a, b) => a.nom.localeCompare(b.nom, "fr") || a.chemin.localeCompare(b.chemin, "fr"));
    const files = space.chemins.filter((c) => c.endsWith(".excalidraw"))
      .map((c) => schemas.find((s) => s.chemin === c)).filter(Boolean)
      .sort((a, b) => label(a.chemin).localeCompare(label(b.chemin), "fr"));
    return [...nodes.map((n) => renderFolder(n, 0, true)), ...files.map((f) => renderSchema(f, -1, true))];
  };
  const items = space ? spaceItems() : null;
  const countIn = (e) => schemas.filter((sch) => inSpace(e, sch.chemin)).length;
  const autoOptions = [...new Set([...AUTO_CHOICES, autoSeconds])].sort((a, b) => a - b);
  const savedLabel = lastSave?.auto ? `Enregistré (auto, ${formatTime(lastSave.at)})` : "Enregistré";
  return html`
    <div class=${sidebar ? "app" : "app sans-barre"}>
      <aside class="barre" style=${{ width: `${barWidth}px` }}>
        <div class="barre-tete">
          <div class="espaces" ref=${spaceMenuRef}>
            <button class=${spaceMenu ? "espace-bouton ouvert" : "espace-bouton"} aria-haspopup="menu"
              aria-expanded=${spaceMenu} title="Changer d'espace de travail"
              onClick=${() => setSpaceMenu((v) => !v)}>
              <${Icon} name="espace" />
              <span class="nom">${space ? space.nom : "Tous les schémas"}</span>
              <${Icon} name="bas" />
            </button>
            ${spaceMenu ? html`
              <div class="espace-menu" role="menu">
                <button role="menuitem" class=${space ? "" : "actif"} onClick=${() => chooseSpace("")}>
                  <span class="marque">${space ? null : html`<${Icon} name="coche" />`}</span>
                  <span class="nom">Tous les schémas</span>
                  <span class="compte">${schemas.length}</span>
                </button>
                ${spaces.length ? html`<div class="espace-titre">Espaces de travail</div>` : null}
                ${spaces.map((e) => html`
                  <button key=${e.id} role="menuitem" class=${e.id === spaceId ? "actif" : ""}
                    onClick=${() => chooseSpace(e.id)}>
                    <span class="marque">${e.id === spaceId ? html`<${Icon} name="coche" />` : null}</span>
                    <span class="nom">${e.nom}</span>
                    <span class="compte">${countIn(e)}</span>
                  </button>`)}
                <div class="espace-trait"></div>
                ${oldServer ? html`<p class="espace-alerte">${OLD_SERVER}</p>` : null}
                <button role="menuitem" onClick=${async () => {
                  const id = await createSpace();
                  if (id) chooseSpace(id);
                }}><span class="marque">+</span><span class="nom">Nouvel espace…</span></button>
                ${space ? html`
                  <button role="menuitem" onClick=${() => renameSpace(space)}>
                    <span class="marque"></span><span class="nom">Renommer « ${space.nom} »…</span></button>
                  <button role="menuitem" onClick=${() => deleteSpace(space)}>
                    <span class="marque"></span><span class="nom">Supprimer « ${space.nom} »…</span></button>` : null}
              </div>` : null}
          </div>
          <span class="boutons">
            <button class="bouton petit" title="Nouveau schéma" onClick=${() => createSchema()}>+ Schéma</button>
            <button class="bouton petit" title="Nouveau dossier" onClick=${() => createFolder()}>+ Dossier</button>
          </span>
        </div>
        <nav class=${dropTarget === "" ? "arbre cible" : "arbre"} aria-label="Dossiers et schémas"
          onDragOver=${(e) => dragOver(e, "")} onDrop=${(e) => dropOn(e, "")} onDragLeave=${leaveTree}>
          ${schemas.length === 0 && folders.length === 0
            ? html`<p class="vide-liste">Aucun schéma pour l'instant.</p>` : null}
          ${space && items.length === 0 ? html`<p class="vide-liste">Cet espace est vide. Passez sur
            « Tous les schémas », survolez un dossier ou un schéma, puis cliquez sur le signet pour
            l'ajouter ici.</p>` : null}
          <ul>${space ? items : renderChildren(tree, 0)}</ul>
          ${dragging && !space ? html`
            <div class=${dropTarget === "" ? "zone-racine cible" : "zone-racine"}>
              Déposer ici pour ranger à la racine</div>` : null}
        </nav>
        <p class="aide-arbre">${space
          ? "Un espace est une vue : y ajouter ou en retirer un dossier ne déplace rien sur le disque."
          : "Glissez un schéma ou un dossier sur un dossier pour l'y ranger. Le signet l'ajoute à un espace de travail."}</p>
      </aside>
      ${sidebar ? html`<div class=${resizing ? "poignee active" : "poignee"} role="separator"
        aria-orientation="vertical" aria-label="Largeur de la liste" tabIndex="0"
        title="Glisser pour élargir ou rétrécir la liste (double-clic : largeur d'origine)"
        onPointerDown=${startResize} onPointerMove=${moveResize} onPointerUp=${endResize}
        onPointerCancel=${endResize} onDoubleClick=${resetBar} onKeyDown=${keyResize}></div>` : null}
      <main class="zone">
        <header class="entete">
          <button class="bouton" title="Afficher ou masquer la liste des schémas"
            onClick=${() => setSidebar((v) => !v)}>Liste</button>
          <span class="titre">${current ? label(current) : "Aucun schéma ouvert"}</span>
          ${preview ? html`<span class="apercu">Ancienne version</span>`
            : current ? html`<span class=${dirty ? "etat modifie" : "etat"}>
                ${dirty ? "Modifications non enregistrées" : savedLabel}</span>` : null}
          <span class="espace"></span>
          ${diskChanged ? html`<span class="alerte">Modifié sur le disque
            <button class="bouton" onClick=${() => open(current, { force: true })}>Recharger</button></span>` : null}
          ${notice ? html`<span class=${`avis ${notice.kind}`}>${notice.text}</span>` : null}
          <button class=${versionsOpen ? "bouton actif" : "bouton"} disabled=${!current}
            title="Afficher ou masquer l'historique des versions" onClick=${toggleVersions}>Versions</button>
          <label class="auto" title="Enregistrement automatique : le schéma ouvert est enregistré à cette fréquence s'il a changé">
            Auto
            <select value=${autoSeconds} onChange=${(e) => chooseAuto(e.target.value)}>
              ${autoOptions.map((sec) => html`
                <option key=${sec} value=${sec}>${sec ? formatDelay(sec) : "désactivé"}</option>`)}
              <option value="autre">Autre…</option>
            </select>
          </label>
          <button class="bouton fort" title="Enregistrer (Cmd+S)" disabled=${!current || !dirty || !!preview}
            onClick=${() => save()}>Enregistrer</button>
        </header>
        ${preview ? html`
          <div class="bandeau-apercu">
            <span>Version du ${formatDate(preview.date)} (${(SOURCE_LABELS[preview.source] || preview.source)
              .toLowerCase()}), en lecture seule.</span>
            <span class="boutons">
              <button class="bouton fort" onClick=${() => restore(preview)}>Restaurer cette version</button>
              <button class="bouton" onClick=${() => open(current, { force: true })}>Revenir à l'actuelle</button>
            </span>
          </div>` : null}
        <div class="corps">
          <div class="canevas" ref=${canevasRef}
            onPointerDownCapture=${markInteraction} onKeyDownCapture=${markInteraction}
            onDropCapture=${markInteraction} onContextMenuCapture=${noteContextCoords}>
            ${scene
              ? html`<div class="cadre"><${Excalidraw} key=${scene.key} initialData=${scene.initial}
                  excalidrawAPI=${onApi} onChange=${onChange} langCode="fr-FR" UIOptions=${UI_OPTIONS}
                  viewModeEnabled=${!!scene.readOnly} /></div>`
              : html`<div class="vide">Choisissez un schéma dans la liste, ou créez-en un.</div>`}
            ${marker && view ? (() => {
              const screen = sceneToScreen(marker.x, marker.y, view);
              const rect = canevasRef.current?.getBoundingClientRect();
              if (!rect) return null;
              const left = screen.x - rect.left;
              const top = screen.y - rect.top;
              return html`
                <div class="curseur-lecture" style=${{ left: `${left}px`, top: `${top}px` }}
                  title="Marqueur de lecture — où vous en étiez" aria-label="Marqueur de lecture">
                  <${Icon} name="marqueur" />
                  <span>Ici</span>
                </div>`;
            })() : null}
          </div>
          ${versionsOpen && current ? html`
            <aside class="versions" aria-label="Historique des versions">
              <div class="versions-tete">
                <strong>Versions</strong>
                <button class="bouton" onClick=${toggleVersions}>Fermer</button>
              </div>
              <p class="versions-aide">Une version est gardée à chaque enregistrement, à chaque restauration
                et à chaque régénération par un script. L'enregistrement automatique en garde une par
                tranche de 10 minutes.</p>
              ${versions === null ? html`<p class="versions-aide">Chargement…</p>`
                : versions.versions.length === 0
                  ? html`<p class="versions-aide">Aucune version gardée pour l'instant. L'état actuel le sera
                      au premier enregistrement.</p>`
                  : html`
                    ${versions.actuelle ? null : html`<p class="versions-aide alerte-douce">Le fichier a changé
                      depuis la dernière version : il sera gardé au prochain enregistrement.</p>`}
                    <ol class="versions-liste">
                      ${versions.versions.map((v) => html`
                        <li key=${v.version} class=${preview?.version === v.version ? "version vue" : "version"}>
                          <div class="version-ligne">
                            <span class="version-date">${formatDate(v.date)}</span>
                            ${v.version === versions.actuelle ? html`<span class="pastille">actuelle</span>` : null}
                          </div>
                          <div class="version-source">${SOURCE_LABELS[v.source] || v.source}</div>
                          <div class="version-actions">
                            <button class="bouton petit" onClick=${() => showVersion(v)}>Voir</button>
                            ${v.version === versions.actuelle ? null : html`
                              <button class="bouton petit" onClick=${() => restore(v)}>Restaurer</button>`}
                          </div>
                        </li>`)}
                    </ol>`}
            </aside>` : null}
        </div>
      </main>
      ${dialog ? html`<${Dialog} key=${dialog.key} dialog=${dialog} onClose=${closeDialog} />` : null}
    </div>`;
}

createRoot(document.getElementById("app")).render(html`<${App} />`);
