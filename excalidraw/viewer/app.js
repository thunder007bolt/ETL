// Visionneuse des schémas Excalidraw : arborescence des dossiers et des schémas (glisser-déposer
// pour les ranger), édition et enregistrement sur le disque.
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
const POLL_MS = 3000;
const CLOSED_KEY = "schemas.dossiers-fermes";
const DRAG_TYPE = "application/x-visionneuse-schemas";
const OPEN_ON_HOVER_MS = 700;
const UI_OPTIONS = { canvasActions: { loadScene: false, saveToActiveFile: false } };
const SOURCE_LABELS = {
  visionneuse: "Enregistrée dans la visionneuse",
  restauration: "Restauration d'une version",
  script: "Générée par un script",
  disque: "Trouvée sur le disque",
};

const encodePath = (path) => path.split("/").map(encodeURIComponent).join("/");
const label = (path) => path.split("/").pop().replace(/\.excalidraw$/, "").replace(/_/g, " ");
const formatDate = (iso) => new Date(iso).toLocaleString("fr-FR", { dateStyle: "short", timeStyle: "medium" });
// Excalidraw incrémente la version d'un élément à chaque modification
const versionSum = (elements) => elements.reduce((sum, e) => sum + e.version, 0);
const parentOf = (path) => (path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "");
const isWithin = (path, folder) => path === folder || path.startsWith(`${folder}/`);

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

const ICONS = {
  chevron: html`<path d="m9 18 6-6-6-6" />`,
  dossier: html`<path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z" />`,
  dossierPlus: html`<g><path d="M12 10v6" /><path d="M9 13h6" /><path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z" /></g>`,
  schema: html`<g><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z" /><path d="M14 2v4a2 2 0 0 0 2 2h4" /></g>`,
  schemaPlus: html`<g><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z" /><path d="M14 2v4a2 2 0 0 0 2 2h4" /><path d="M9 15h6" /><path d="M12 18v-6" /></g>`,
  corbeille: html`<g><path d="M3 6h18" /><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" /><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2" /></g>`,
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
        <div class="dialogue-boutons">
          <button type="button" class="bouton" onClick=${() => onClose(null)}>Annuler</button>
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
  const [dragging, setDragging] = useState(false);
  const [dropTarget, setDropTarget] = useState(null); // dossier survolé pendant un glisser ("" : la racine)
  const [scene, setScene] = useState(null); // { path, key, initial }
  const [dirty, setDirty] = useState(false);
  const [diskChanged, setDiskChanged] = useState(false);
  const [notice, setNotice] = useState(null); // { text, kind }
  const [sidebar, setSidebar] = useState(true);
  const [versionsOpen, setVersionsOpen] = useState(false);
  const [versions, setVersions] = useState(null); // { versions, actuelle } du schéma ouvert
  const [preview, setPreview] = useState(null); // version affichée en lecture seule

  const apiRef = useRef(null);
  const previewRef = useRef(null);
  const versionsOpenRef = useRef(false);
  const pathRef = useRef(null);
  const mtimeRef = useRef(null);
  const baselineRef = useRef(null);
  // les changements faits par Excalidraw seul (mesure des textes au chargement des polices)
  // ne comptent pas comme des modifications tant que la personne n'a pas touché au schéma
  const interactedRef = useRef(false);
  const dirtyRef = useRef(false);
  const savingRef = useRef(false);
  const fitPendingRef = useRef(false);
  const listJsonRef = useRef("");
  const loadCountRef = useRef(0);
  const dragRef = useRef(null); // élément glissé : { type: "schema" | "dossier", chemin }
  const hoverRef = useRef({ folder: null, timer: null }); // dossier replié qui s'ouvre au survol
  const [dialog, setDialog] = useState(null); // fenêtre de question ouverte
  const dialogRef = useRef(null); // sa fonction de réponse
  const saveRef = useRef(null);

  const markDirty = (value) => {
    dirtyRef.current = value;
    setDirty(value);
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
    }
    return tree.schemas;
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
      setDiskChanged(false);
      setShownVersion(null);
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

  // enregistre le schéma ouvert ; répond true si c'est fait
  const save = useCallback(async ({ force = false } = {}) => {
    const api = apiRef.current;
    const path = pathRef.current;
    // une version affichée de l'historique est en lecture seule : on la restaure, on ne l'enregistre pas
    if (!api || !path || savingRef.current || previewRef.current) return false;
    savingRef.current = true;
    const elements = api.getSceneElementsIncludingDeleted();
    const body = serializeAsJSON(api.getSceneElements(), api.getAppState(), api.getFiles(), "local");
    try {
      const result = await writeSchema(path, body, { mtime: mtimeRef.current, force });
      mtimeRef.current = result.modifie;
      baselineRef.current = versionSum(elements);
      markDirty(false);
      setDiskChanged(false);
      flash("Enregistré");
      refreshList();
      refreshVersions();
      return true;
    } catch (e) {
      savingRef.current = false;
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

  const toggleVersions = () => {
    versionsOpenRef.current = !versionsOpenRef.current;
    setVersionsOpen(versionsOpenRef.current);
  };

  const createSchema = useCallback(async (folder = "") => {
    const answer = await askText({
      title: "Nouveau schéma",
      message: "Son nom, précédé de son dossier : par exemple grh/effectif.",
      value: folder ? `${folder}/` : "",
      confirmLabel: "Créer",
    });
    if (!answer || !answer.trim()) return;
    const path = `${answer.trim().replace(/^\/+/, "").replace(/\.excalidraw$/, "")}.excalidraw`;
    try {
      await writeSchema(path, emptyScene(), { create: true });
      await refreshList();
      open(path);
    } catch (e) {
      flash(e.code === "existe" ? "Un schéma porte déjà ce nom." : `Création impossible : ${e.message}`,
        "erreur");
    }
  }, [askText, flash, open, refreshList]);

  const createFolder = useCallback(async (parent = "") => {
    const answer = await askText({
      title: "Nouveau dossier",
      message: "Son nom, précédé de son dossier parent : par exemple prestations/archives.",
      value: parent ? `${parent}/` : "",
      confirmLabel: "Créer",
    });
    if (!answer || !answer.trim()) return;
    const path = answer.trim().replace(/^\/+|\/+$/g, "");
    try {
      await postJson(FOLDERS_API, { chemin: path });
      reveal(path);
      await refreshList();
      flash("Dossier créé");
    } catch (e) {
      flash(e.code === "existe" ? "Ce dossier existe déjà." : `Création impossible : ${e.message}`, "erreur");
    }
  }, [askText, flash, refreshList]);

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

  const onChange = useCallback((elements) => {
    if (fitPendingRef.current && apiRef.current) {
      fitPendingRef.current = false;
      fitView(apiRef.current, elements);
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

  // premier chargement : le schéma de l'adresse (#dossier/nom.excalidraw) ou le premier de la liste
  useEffect(() => {
    refreshList()
      .then((list) => {
        const wanted = decodeURIComponent(location.hash.slice(1));
        const first = list.find((s) => s.chemin === wanted) || list[0];
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
        list = await refreshList();
      } catch {
        return;
      }
      const path = pathRef.current;
      const entry = path && list.find((s) => s.chemin === path);
      if (!entry || savingRef.current || entry.modifie === mtimeRef.current) return;
      if (dirtyRef.current || previewRef.current) setDiskChanged(true);
      else open(path, { force: true });
    }, POLL_MS);
    return () => clearInterval(timer);
  }, [open, refreshList]);

  useEffect(() => {
    if (versionsOpen) refreshVersions();
  }, [versionsOpen, scene?.key, refreshVersions]);

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

  const current = scene?.path;
  const indent = (depth) => ({ paddingLeft: `${8 + depth * 16}px` });

  const renderFolder = (node, depth) => {
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
          ${isClosed ? html`<span class="compte">${countSchemas(node)}</span>` : null}
          <span class="actions" onClick=${(e) => e.stopPropagation()}>
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

  const renderChildren = (node, depth) => [
    ...sortedFolders(node).map((f) => renderFolder(f, depth)),
    ...node.schemas.map((s) => html`
      <li key=${s.chemin}>
        <div class=${s.chemin === current ? "ligne schema actif" : "ligne schema"} style=${indent(depth + 1)}
          role="button" tabIndex="0" draggable="true" title=${s.chemin}
          onClick=${() => open(s.chemin)} onKeyDown=${(e) => e.key === "Enter" && open(s.chemin)}
          onDragStart=${(e) => startDrag(e, { type: "schema", chemin: s.chemin })} onDragEnd=${endDrag}
          onDragOver=${(e) => dragOver(e, s.dossier)} onDrop=${(e) => dropOn(e, s.dossier)}>
          <${Icon} name="schema" />
          <span class="nom">${label(s.chemin)}</span>
        </div>
      </li>`),
  ];

  const tree = buildTree(folders, schemas);
  return html`
    <div class=${sidebar ? "app" : "app sans-barre"}>
      <aside class="barre">
        <div class="barre-tete">
          <strong>Schémas</strong>
          <span class="boutons">
            <button class="bouton petit" title="Nouveau schéma" onClick=${() => createSchema()}>+ Schéma</button>
            <button class="bouton petit" title="Nouveau dossier" onClick=${() => createFolder()}>+ Dossier</button>
          </span>
        </div>
        <nav class=${dropTarget === "" ? "arbre cible" : "arbre"} aria-label="Dossiers et schémas"
          onDragOver=${(e) => dragOver(e, "")} onDrop=${(e) => dropOn(e, "")} onDragLeave=${leaveTree}>
          ${schemas.length === 0 && folders.length === 0
            ? html`<p class="vide-liste">Aucun schéma pour l'instant.</p>` : null}
          <ul>${renderChildren(tree, 0)}</ul>
          ${dragging ? html`
            <div class=${dropTarget === "" ? "zone-racine cible" : "zone-racine"}>
              Déposer ici pour ranger à la racine</div>` : null}
        </nav>
        <p class="aide-arbre">Glissez un schéma ou un dossier sur un dossier pour l'y ranger.</p>
      </aside>
      <main class="zone">
        <header class="entete">
          <button class="bouton" title="Afficher ou masquer la liste des schémas"
            onClick=${() => setSidebar((v) => !v)}>Liste</button>
          <span class="titre">${current ? label(current) : "Aucun schéma ouvert"}</span>
          ${preview ? html`<span class="apercu">Ancienne version</span>`
            : current ? html`<span class=${dirty ? "etat modifie" : "etat"}>
                ${dirty ? "Modifications non enregistrées" : "Enregistré"}</span>` : null}
          <span class="espace"></span>
          ${diskChanged ? html`<span class="alerte">Modifié sur le disque
            <button class="bouton" onClick=${() => open(current, { force: true })}>Recharger</button></span>` : null}
          ${notice ? html`<span class=${`avis ${notice.kind}`}>${notice.text}</span>` : null}
          <button class=${versionsOpen ? "bouton actif" : "bouton"} disabled=${!current}
            title="Afficher ou masquer l'historique des versions" onClick=${toggleVersions}>Versions</button>
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
          <div class="canevas" onPointerDownCapture=${markInteraction} onKeyDownCapture=${markInteraction}
            onDropCapture=${markInteraction}>
            ${scene
              ? html`<div class="cadre"><${Excalidraw} key=${scene.key} initialData=${scene.initial}
                  excalidrawAPI=${onApi} onChange=${onChange} langCode="fr-FR" UIOptions=${UI_OPTIONS}
                  viewModeEnabled=${!!scene.readOnly} /></div>`
              : html`<div class="vide">Choisissez un schéma dans la liste, ou créez-en un.</div>`}
          </div>
          ${versionsOpen && current ? html`
            <aside class="versions" aria-label="Historique des versions">
              <div class="versions-tete">
                <strong>Versions</strong>
                <button class="bouton" onClick=${toggleVersions}>Fermer</button>
              </div>
              <p class="versions-aide">Une version est gardée à chaque enregistrement, à chaque restauration
                et à chaque régénération par un script.</p>
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
