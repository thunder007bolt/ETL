/*
 * La version Word des points à valider d'un domaine : analyses/<domaine>/points_a_valider.docx,
 * faite à partir de analyses/<domaine>/points_a_valider.md, la seule source (comme le schéma
 * Excalidraw de points_a_valider.py).
 *
 * Mise en page de la version du 01/11 :
 * - A4, marges de 2,5 cm, Calibri 11 ;
 * - titres bleus, un point par paragraphe numéroté (une liste Word par point, qui commence à
 *   son numéro : les points gardent leur numéro d'une version à l'autre) ;
 * - libellés en gras, ligne « Réponse » en pointillés, numéro de page en bas.
 * Conventions de Word en français : apostrophes courbes, espaces insécables avant « : ; ? ! % »,
 * dans les guillemets et dans les nombres ; auteur Paulin Lendi.
 *
 * Usage (la bibliothèque docx n'est pas dans le projet) :
 *   npm install docx@9.5.1 --prefix /tmp/docxgen
 *   NODE_PATH=/tmp/docxgen/node_modules node excalidraw/sources/points_a_valider_word.js prestations
 * Pour un essai : ... points_a_valider_word.js prestations <fichier.md> <sortie.docx>
 */

const fs = require("fs");
const path = require("path");
const {
  AlignmentType, Document, Footer, HeadingLevel, LeaderType, LevelFormat, Packer, PageNumber,
  Paragraph, TabStopType, TextRun,
} = require("docx");

const ETL = path.resolve(__dirname, "..", "..");
const AUTEUR = "Paulin Lendi";
const NBSP = " ";

function fr(texte) {
  let t = texte.replace(/'/g, "’");
  t = t.replace(/ ([:;?!%»])/g, NBSP + "$1").replace(/« /g, "«" + NBSP);
  t = t.replace(/(\d) (Md|M|FCFA|ans?|mois|jours?)\b/g, "$1" + NBSP + "$2");
  let avant;
  do {
    avant = t;
    t = t.replace(/(\d) (\d{3})(?!\d)/g, "$1" + NBSP + "$2");
  } while (t !== avant);
  return t;
}

/* Même lecture que points_a_valider.py : titre, introduction, sections, groupes, points, listes. */
function lire(fichier) {
  let titre = null;
  const intro = [];
  const sections = [];
  for (const brute of fs.readFileSync(fichier, "utf8").split("\n")) {
    const ligne = brute.replace(/\s+$/, "");
    if (!ligne.trim()) continue;
    if (ligne.startsWith("# ")) titre = ligne.slice(2);
    else if (ligne.startsWith("## ")) {
      sections.push({ titre: ligne.slice(3), groupes: [[null, []]], liste: [] });
    } else if (ligne[0] === " ") {
      if (ligne.trim().startsWith("Réponse :")) continue;
      const m = ligne.trim().match(/^([^:]{1,40}) : (.*)$/);
      const groupe = sections[sections.length - 1].groupes;
      const points = groupe[groupe.length - 1][1];
      points[points.length - 1].details.push(m ? [m[1], m[2]] : [null, ligne.trim()]);
    } else if (/^\d+\. /.test(ligne)) {
      const i = ligne.indexOf(". ");
      const groupe = sections[sections.length - 1].groupes;
      groupe[groupe.length - 1][1].push({ n: ligne.slice(0, i), titre: ligne.slice(i + 2), details: [] });
    } else if (ligne.startsWith("- ")) {
      sections[sections.length - 1].liste.push(ligne.slice(2));
    } else if (sections.length) {
      sections[sections.length - 1].groupes.push([ligne, []]);
    } else {
      intro.push(ligne);
    }
  }
  return { titre, intro, sections };
}

/* La ligne en pointillés jusqu'à la marge de droite : un taquet à droite avec des points de
   conduite, comme la version du 01/11 (largeur utile de l'A4 : 11906 - 2 x 1418 = 9070). */
const POINTILLES = [{ type: TabStopType.RIGHT, position: 9070, leader: LeaderType.DOT }];

function paragraphesPoint(p) {
  const out = [new Paragraph({
    keepNext: true,
    spacing: { before: 200, after: 60 },
    numbering: { reference: `point-${p.n}`, level: 0 },
    children: [new TextRun({ text: fr(p.titre), bold: true })],
  })];
  for (const [label, texte] of p.details) {
    const enfants = label
      ? [new TextRun({ text: fr(`${label} :`) + " ", bold: true }), new TextRun(fr(texte))]
      : [new TextRun(fr(texte))];
    out.push(new Paragraph({ keepNext: true, spacing: { after: 40 }, indent: { left: 360 }, children: enfants }));
  }
  out.push(new Paragraph({
    keepNext: true,
    spacing: { before: 60, after: 0, line: 360 },
    indent: { left: 360 },
    tabStops: POINTILLES,
    children: [new TextRun({ text: fr("Réponse :") + " ", bold: true }), new TextRun("\t")],
  }));
  out.push(new Paragraph({
    spacing: { after: 120, line: 360 },
    indent: { left: 360 },
    tabStops: POINTILLES,
    children: [new TextRun("\t")],
  }));
  return out;
}

function document(md) {
  const { titre, intro, sections } = lire(md);
  const numeros = sections.flatMap((s) => s.groupes.flatMap(([, points]) => points.map((p) => p.n)));
  const enfants = [new Paragraph({ heading: HeadingLevel.TITLE, children: [new TextRun(fr(titre))] })];
  for (const p of intro) enfants.push(new Paragraph({ spacing: { after: 160 }, children: [new TextRun(fr(p))] }));
  for (const s of sections) {
    enfants.push(new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(fr(s.titre))] }));
    for (const [sousTitre, points] of s.groupes) {
      if (!points.length) continue;
      if (sousTitre) enfants.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(fr(sousTitre))] }));
      for (const p of points) enfants.push(...paragraphesPoint(p));
    }
    for (const ligne of s.liste) {
      enfants.push(new Paragraph({
        numbering: { reference: "tirets", level: 0 },
        spacing: { after: 60 },
        children: [new TextRun(fr(ligne))],
      }));
    }
  }
  const titreStyle = (taille, couleur, avant, apres) => ({
    run: { font: "Calibri", size: taille, bold: true, color: couleur },
    paragraph: { spacing: { before: avant, after: apres }, keepNext: true },
  });
  return new Document({
    creator: AUTEUR,
    lastModifiedBy: AUTEUR,
    title: titre,
    styles: {
      default: {
        document: { run: { font: "Calibri", size: 22 }, paragraph: { spacing: { after: 120, line: 264 } } },
        title: titreStyle(32, "000000", 0, 160),
        heading1: titreStyle(26, "2F5496", 280, 120),
        heading2: titreStyle(22, "1F3763", 200, 80),
      },
    },
    numbering: {
      config: [{
        reference: "tirets",
        levels: [{
          level: 0, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 360, hanging: 360 } } },
        }],
      }, ...numeros.map((n) => ({
        /* une liste par point, qui commence à son numéro : le numéro ne change jamais */
        reference: `point-${n}`,
        levels: [{
          level: 0, format: LevelFormat.DECIMAL, text: "%1.", start: Number(n),
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 360, hanging: 360 } } },
        }],
      }))],
    },
    sections: [{
      properties: {
        page: {
          size: { width: 11906, height: 16838 },
          margin: { top: 1418, right: 1418, bottom: 1418, left: 1418 },
        },
      },
      footers: {
        default: new Footer({
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [new TextRun({ children: [PageNumber.CURRENT], color: "7F7F7F", size: 18 })],
          })],
        }),
      },
      children: enfants,
    }],
  });
}

async function main() {
  const domaine = process.argv[2] || "prestations";
  const md = process.argv[3] || path.join(ETL, "analyses", domaine, "points_a_valider.md");
  const sortie = process.argv[4] || path.join(ETL, "analyses", domaine, "points_a_valider.docx");
  const tampon = await Packer.toBuffer(document(md));
  fs.writeFileSync(sortie, tampon);
  console.log(`écrit : ${sortie} (${tampon.length} octets)`);
}

main().catch((e) => { console.error(e); process.exit(1); });
