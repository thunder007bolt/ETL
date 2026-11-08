"""
Ce qui est commun aux schémas des vues à demander à la CNSS (07/11/2026).

La CNSS crée ces vues dans la base source ; la BI les lit comme les autres
tables de la base source. Ce module garde :
- les outils de mise en page (cadres, tableaux, lignes d'exemple) ;
- la définition de la vue commune de la comptabilité, V_BI_BALANCE_COMPTABLE,
  reprise par schemas/vues cnss/01 et par schemas/grh/T63 T64 finances/t63_t64_donnees_a_fournir,
  pour que les deux schémas disent toujours la même chose.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, text_size  # noqa: E402

WIDTH = 2400


# ---------------------------------------------------------------- mise en page
def couper(txt, size, largeur, mono=False):
    """Coupe un texte en lignes qui tiennent dans la largeur (mot par mot).
    Les lignes qui commencent par des espaces gardent leur retrait."""
    lignes = []
    for para in txt.split("\n"):
        retrait = para[:len(para) - len(para.lstrip(" "))]
        courante = ""
        for mot in para.split(" "):
            if not mot and not courante:
                continue
            essai = f"{courante} {mot}" if courante else retrait + mot
            if courante and text_size(essai, size, mono)[0] > largeur:
                lignes.append(courante)
                courante = (retrait or "  " if para.lstrip().startswith("·") else retrait) + mot
            else:
                courante = essai
        lignes.append(courante)
    return "\n".join(lignes)


def entete(d, titre, sous_titre):
    """Titre et sous-titre ; renvoie le haut de la première section."""
    d.text(0, 0, titre, 48)
    sous = couper(sous_titre, 22, WIDTH)
    d.text(0, 66, sous, 22, C["doux"])
    return 66 + text_size(sous, 22)[1] + 40


def bloc(d, x, y, w, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2, h=None):
    """Cadre avec un texte aligné à gauche ; la hauteur suit le texte, sauf h donné."""
    t = couper(txt, size, w - 44)
    h = h or text_size(t, size)[1] + 34
    g = d.group()
    b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
    d.text(x + 22, y + 16, t, size, groups=g, inside=b)
    return b


def rangee(d, top, textes, size=20, gap=40, x=40, largeur=WIDTH - 80):
    """Des cadres côte à côte, de même hauteur. textes : liste de (texte, couleurs, sw).
    Renvoie le bas de la rangée."""
    n = len(textes)
    w = (largeur - (n - 1) * gap) / n
    h = max(text_size(couper(t, size, w - 44), size)[1] for t, _, _ in textes) + 34
    for i, (t, couleurs, sw) in enumerate(textes):
        bloc(d, x + i * (w + gap), top, w, t, size, couleurs, sw, h=h)
    return top + h


def tableau(d, top, colonnes, lignes, x=40, gap=20):
    """Un tableau à lignes de hauteur variable.
    colonnes : liste de (titre, largeur, options) ; options : size, mono, couleur
    (une couleur, ou une fonction de la valeur). Renvoie le bas du tableau."""
    xs, cx = [], x
    for _, w, _ in colonnes:
        xs.append(cx)
        cx += w + gap
    total = cx - gap - x
    for (titre, _, _), xc in zip(colonnes, xs):
        d.text(xc + 4, top, titre, 21, C["doux"])
    y = top + 44
    for i, ligne in enumerate(lignes):
        cellules = []
        for (_, w, opt), val in zip(colonnes, ligne):
            size, mono = opt.get("size", 19), opt.get("mono", False)
            couleur = opt.get("couleur", C["encre"])
            if callable(couleur):
                couleur = couleur(val)
            cellules.append((couper(val, size, w - 32, mono), size, mono, couleur))
        h = max(64, max(text_size(t, s, m)[1] for t, s, m, _ in cellules) + 30)
        g = d.group()
        cadre = d.box(x, y, total, h, bg=C["blanc"] if i % 2 == 0 else C["section"],
                      stroke=C["gris_bord"], stroke_width=1, groups=g)
        for (t, s, m, couleur), xc in zip(cellules, xs):
            d.text(xc + 16, y + 15, t, s, couleur, groups=g, inside=cadre, mono=m)
        y += h + 8
    return y - 8


def tableau_colonnes(d, top, colonnes):
    """Le tableau d'une structure : Colonne | Type | Obligatoire | Contenu."""
    return tableau(d, top, [
        ("Colonne", 400, {"size": 20, "mono": True}),
        ("Type", 280, {"size": 19, "mono": True, "couleur": C["doux"]}),
        ("Obligatoire", 190, {"size": 20,
                              "couleur": lambda v: C["vert"] if v == "oui" else C["doux"]}),
        ("Contenu", 1390, {"size": 19}),
    ], colonnes)


def exemple(d, top, noms, largeurs, lignes, x=40, gap=10):
    """Des lignes d'exemple, sous les noms des colonnes. Renvoie le bas."""
    xs, cx = [], x
    for w in largeurs:
        xs.append(cx)
        cx += w + gap
    for nom, xc, w in zip(noms, xs, largeurs):
        d.box(xc, top, w, 50, nom, 13, bg=C["gris"], stroke=C["gris_bord"], stroke_width=1)
    y = top + 60
    for ligne in lignes:
        for val, xc, w in zip(ligne, xs, largeurs):
            d.box(xc, y, w, 48, val or " ", 16, bg=C["blanc"], stroke=C["gris_bord"],
                  stroke_width=1)
        y += 56
    return y - 8


# ---------------------------------------------------------------- la vue commune
BALANCE_NOM = "V_BI_BALANCE_COMPTABLE"
BALANCE_LIGNE = ("un exercice × un mois × une gestion × une branche × un compte × une section "
                 "analytique × une nature de montant")

# (colonne, type, obligatoire, contenu)
BALANCE_COLONNES = [
    ("EXERCICE", "NUMBER(4)", "oui", "Année de l'exercice comptable (exemple : 2024)"),
    ("MOIS", "NUMBER(2)", "oui",
     "1 à 12 : les mouvements du mois. 0 : le solde au 1er janvier des comptes de bilan "
     "(à-nouveaux). 13 : les écritures de clôture, s'il y en a"),
    ("CODE_GESTION", "VARCHAR2(5)", "oui",
     "Gestion d'Oracle GL, telle qu'elle y est notée : G ou 7 (administrative), S ou 5 (action "
     "sanitaire et sociale), I ou 6 (immeubles de rapport), T ou 8, E ou 9"),
    ("BRANCHE", "VARCHAR2(10)", "oui",
     "PF, ATMP ou PVID quand le montant est rattaché à une branche ; COMMUN sinon"),
    ("COMPTE", "VARCHAR2(20)", "oui", "Compte de comptabilité générale, au niveau le plus fin"),
    ("LIBELLE_COMPTE", "VARCHAR2(240)", "non", "Intitulé du compte dans le plan comptable"),
    ("SECTION_ANALYTIQUE", "VARCHAR2(30)", "oui",
     "Centre de coût, « - » s'il n'y en a pas. Il sert à décliner les tableaux par direction, "
     "agence et bureau"),
    ("NATURE_MONTANT", "VARCHAR2(10)", "oui",
     "REALISE : écritures comptabilisées. BUDGET : budget définitif (voté et modifié). "
     "ENGAGE : engagements, si Oracle GL les suit"),
    ("MONTANT_DEBIT", "NUMBER(18,2)", "oui",
     "Total des débits, en francs CFA (budget : le montant prévu d'une charge ou d'un "
     "investissement)"),
    ("MONTANT_CREDIT", "NUMBER(18,2)", "oui",
     "Total des crédits, en francs CFA (budget : le montant prévu d'un produit)"),
    ("PERIODE_CLOSE", "CHAR(1)", "oui", "O si le mois est clos dans Oracle GL, N sinon"),
    ("DATE_MISE_A_JOUR", "DATE", "oui", "Date à laquelle la ligne a été alimentée"),
]

BALANCE_EXEMPLE_NOMS = [c[0] for c in BALANCE_COLONNES]
BALANCE_EXEMPLE_LARGEURS = [120, 80, 150, 120, 130, 310, 230, 190, 220, 220, 150, 190]

BALANCE_REGLES = [
    ("CE QU'ON MET DEDANS\n"
     "· toute la balance générale : tous les comptes mouvementés, de toutes les gestions\n"
     "· le réalisé mois par mois : les mouvements du mois, sans cumul\n"
     "· le mois 0 : le solde au 1er janvier des comptes de bilan\n"
     "· le budget définitif, par mois s'il est mensualisé, sinon en entier sur le mois 12\n"
     "· les 5 derniers exercices clos et l'exercice en cours"),
    ("CE QU'ON N'Y MET PAS\n"
     "· aucune écriture une à une : des totaux par compte\n"
     "· ni nom, ni tiers, ni numéro bancaire\n\n"
     "MISE À JOUR\n"
     "· après chaque clôture mensuelle ; un mois remis à jour remplace ses lignes\n"
     "· en francs CFA : une charge au débit, un produit au crédit, pour le réalisé comme pour "
     "le budget"),
]
