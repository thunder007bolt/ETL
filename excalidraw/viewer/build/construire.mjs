// Construit viewer/vendor/ pour que la visionneuse marche sans Internet :
//   vendor/js/        app.js regroupé avec React, htm et Excalidraw (découpé en morceaux chargés à
//                     la demande : langues, découpage des polices à l'export) ;
//   vendor/excalidraw.css  la feuille de style d'Excalidraw ;
//   vendor/fonts/     les polices d'Excalidraw, sauf Xiaolai (12 Mo, pour le chinois et le
//                     japonais seulement).
// Il faut Internet une seule fois, pour « npm install ». À relancer après toute modification de
// viewer/app.js :
//   cd excalidraw/viewer/build && npm install && npm run build

import { cp, mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const ici = path.dirname(fileURLToPath(import.meta.url));
const viewer = path.resolve(ici, "..");
const vendor = path.join(viewer, "vendor");
const excalidraw = path.join(ici, "node_modules", "@excalidraw", "excalidraw", "dist", "prod");

await rm(vendor, { recursive: true, force: true });
await mkdir(vendor, { recursive: true });

await build({
  entryPoints: [path.join(viewer, "app.js")],
  bundle: true,
  splitting: true,
  format: "esm",
  minify: true,
  legalComments: "none",
  outdir: path.join(vendor, "js"),
  nodePaths: [path.join(ici, "node_modules")],
  define: { "process.env.NODE_ENV": '"production"' },
  logLevel: "info",
});

await cp(path.join(excalidraw, "index.css"), path.join(vendor, "excalidraw.css"));
await cp(path.join(excalidraw, "fonts"), path.join(vendor, "fonts"), {
  recursive: true,
  filter: (source) => !source.split(path.sep).includes("Xiaolai"),
});

console.log(`Visionneuse construite dans ${path.relative(path.resolve(viewer, "..", ".."), vendor)}`);
