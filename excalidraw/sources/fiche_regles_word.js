/*
 * La version Word de la fiche des règles : analyses/regles_cipres/fiche_regles.docx, faite à
 * partir de analyses/regles_cipres/fiche_regles.md, la seule source (comme le schéma Excalidraw
 * de fiche_regles.py).
 *
 * Même mise en page que la fiche des points à valider (points_a_valider_word.js) :
 * - A4, marges de 2,5 cm, Calibri 11, titres bleus, numéro de page en bas ;
 * - une règle par paragraphe numéroté (une liste Word par règle, qui commence à son numéro :
 *   les règles gardent leur numéro d'une version à l'autre) ; l'origine de la règle, entre
 *   parenthèses en fin de ligne, en petit et en gris ;
 * - les lignes « À faire » en retrait, le libellé en gras et en orange.
 * Conventions de Word en français : apostrophes courbes, espaces insécables avant « : ; ? ! % »,
 * dans les guillemets et dans les nombres ; auteur Paulin Lendi.
 *
 * Usage (la bibliothèque docx n'est pas dans le projet) :
 *   npm install docx@9.5.1 --prefix /tmp/docxgen
 *   NODE_PATH=/tmp/docxgen/node_modules node excalidraw/sources/fiche_regles_word.js
 * Pour un essai : ... fiche_regles_word.js <fichier.md> <sortie.docx>
 */

const fs = require("fs");
const path = require("path");
const {
  AlignmentType, Document, Footer, HeadingLevel, LevelFormat, Packer, PageNumber, Paragraph,
  TextRun,
} = require("docx");

const ETL = path.resolve(__dirname, "..", "..");
const AUTEUR = "Paulin Lendi";
const NBSP = " ";
const ORANGE = "C55A11";

function fr(texte) {
  let t = texte.replace(/'/g, "’");
  t = t.replace(/ ([:;?!%»])/g, NBSP + "$1").replace(/« /g, "«" + NBSP);
  t = t.replace(/(\d) (Md|M|FCFA|ans?|mois|jours?|unités|décimales)\b/g, "$1" + NBSP + "$2");
  let avant;
  do {
    avant = t;
    t = t.replace(/(\d) (\d{3})(?!\d)/g, "$1" + NBSP + "$2");
  } while (t !== avant);
  return t;
}

/* Même lecture que fiche_regles.py : titre, introduction, domaines, groupes, règles, « À faire ». */
function lire(fichier) {
  let titre = null;
  const intro = [];
  const domaines = [];
  for (const brute of fs.readFileSync(fichier, "utf8").split("\n")) {
    const ligne = brute.replace(/\s+$/, "");
    if (!ligne.trim()) continue;
    if (ligne.startsWith("# ")) titre = ligne.slice(2);
    else if (ligne.startsWith("## ")) domaines.push({ titre: ligne.slice(3), groupes: [] });
    else if (ligne.startsWith("### ")) {
      domaines[domaines.length - 1].groupes.push({ titre: ligne.slice(4), lignes: [] });
    } else if (/^\d+\. /.test(ligne) || ligne.startsWith("- ")) {
      const groupes = domaines[domaines.length - 1].groupes;
      if (!groupes.length) groupes.push({ titre: null, lignes: [] });
      const m = ligne.match(/^(\d+)\. (.*)$/);
      groupes[groupes.length - 1].lignes.push(m ? { n: m[1], texte: m[2] } : { n: null, texte: ligne.slice(2) });
    } else {
      intro.push(ligne);
    }
  }
  return { titre, intro, domaines };
}

/* Le texte d'une règle et son origine, la parenthèse de fin de ligne. */
function couper(texte) {
  const m = texte.match(/^(.*) (\([^()]*\))$/);
  return m ? [m[1], m[2]] : [texte, null];
}

function paragrapheRegle({ n, texte }) {
  const [corps, origine] = couper(texte);
  const enfants = [new TextRun(fr(corps))];
  if (origine) enfants.push(new TextRun({ text: " " + fr(origine), color: "7F7F7F", size: 18 }));
  return new Paragraph({
    spacing: { after: 80 },
    numbering: { reference: `regle-${n}`, level: 0 },
    children: enfants,
  });
}

function paragrapheAFaire({ texte }) {
  const [corps, origine] = couper(texte);
  const m = corps.match(/^(À faire(?: \([^()]*\))?) : (.*)$/);
  const enfants = m
    ? [new TextRun({ text: fr(`${m[1]} :`) + " ", bold: true, color: ORANGE }), new TextRun(fr(m[2]))]
    : [new TextRun(fr(corps))];
  if (origine) enfants.push(new TextRun({ text: " " + fr(origine), color: "7F7F7F", size: 18 }));
  return new Paragraph({ spacing: { before: 40, after: 80 }, indent: { left: 360 }, children: enfants });
}

function document(md) {
  const { titre, intro, domaines } = lire(md);
  const numeros = domaines.flatMap((d) => d.groupes.flatMap((g) => g.lignes.filter((l) => l.n).map((l) => l.n)));
  const enfants = [new Paragraph({ heading: HeadingLevel.TITLE, children: [new TextRun(fr(titre))] })];
  intro.forEach((p, i) => enfants.push(new Paragraph({
    spacing: { after: i === 0 ? 200 : 100 },
    children: [new TextRun(i === 0 ? { text: fr(p), color: "595959" } : fr(p))],
  })));
  for (const dom of domaines) {
    enfants.push(new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(fr(dom.titre))] }));
    for (const g of dom.groupes) {
      if (g.titre) enfants.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(fr(g.titre))] }));
      for (const l of g.lignes) enfants.push(l.n ? paragrapheRegle(l) : paragrapheAFaire(l));
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
        title: titreStyle(32, "000000", 0, 120),
        heading1: titreStyle(26, "2F5496", 280, 120),
        heading2: titreStyle(22, "1F3763", 200, 80),
      },
    },
    numbering: {
      config: numeros.map((n) => ({
        /* une liste par règle, qui commence à son numéro : le numéro ne change jamais */
        reference: `regle-${n}`,
        levels: [{
          level: 0, format: LevelFormat.DECIMAL, text: "%1.", start: Number(n),
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 360, hanging: 360 } } },
        }],
      })),
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
  const md = process.argv[2] || path.join(ETL, "analyses", "regles_cipres", "fiche_regles.md");
  const sortie = process.argv[3] || path.join(ETL, "analyses", "regles_cipres", "fiche_regles.docx");
  const tampon = await Packer.toBuffer(document(md));
  fs.writeFileSync(sortie, tampon);
  console.log(`écrit : ${sortie} (${tampon.length} octets)`);
}

main().catch((e) => { console.error(e); process.exit(1); });
