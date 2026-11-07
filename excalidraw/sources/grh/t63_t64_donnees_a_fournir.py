"""
Schéma « T63 et T64 : les données à fournir par la CNSS » (état au 07/11/2026).

La structure que l'équipe BI demande à la DSI et à la DFC pour T63 et T64 :
pourquoi, les colonnes (nom, type, obligatoire, contenu), une ligne
d'exemple fictive, les règles, et ce qu'il faut savoir de la DFC. Pas de SQL :
la DSI crée elle-même la table ou la vue et l'alimente depuis Oracle GL.

Usage :
    python3 excalidraw/sources/grh/t63_t64_donnees_a_fournir.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "t63_t64_donnees_a_fournir.excalidraw"

WIDTH = 2400

# (colonne, type, obligatoire, contenu)
COLONNES = [
    ("EXERCICE", "NUMBER(4)", "oui", "Année de l'exercice comptable (exemple : 2024)"),
    ("CODE_GESTION", "VARCHAR2(5)", "oui",
     "Gestion d'Oracle GL, telle qu'elle y est notée : G ou 7, S ou 5, I ou 6, T ou 8, E ou 9"),
    ("COMPTE", "VARCHAR2(20)", "oui", "Compte de comptabilité générale, au niveau le plus fin"),
    ("LIBELLE_COMPTE", "VARCHAR2(240)", "non", "Intitulé du compte dans le plan comptable"),
    ("SECTION_ANALYTIQUE", "VARCHAR2(30)", "oui",
     "Centre de coût de la dépense, « - » s'il n'y en a pas. Il sert à décliner T63 et T64 "
     "par direction, agence et bureau"),
    ("CODE_UNITE", "VARCHAR2(12)", "non",
     "Unité administrative de la GRH qui correspond à la section, si la DSI la connaît"),
    ("NATURE_MONTANT", "VARCHAR2(10)", "oui",
     "REALISE : dépenses comptabilisées ; BUDGET : budget définitif de l'exercice"),
    ("MONTANT", "NUMBER(18,2)", "oui",
     "Montant en francs CFA, cumulé sur l'exercice : débit moins crédit"),
    ("EXERCICE_CLOS", "CHAR(1)", "oui", "O si l'exercice est clôturé dans Oracle GL, N sinon"),
    ("DATE_MISE_A_JOUR", "DATE", "oui", "Date à laquelle la ligne a été alimentée"),
]

EXEMPLE = [
    ("2024", "G", "651100", "Salaires et appointements", "DG01", "", "REALISE", "1 250 000 000",
     "O", "15/01/2026"),
    ("2024", "S", "652000", "Primes et gratifications", "DPASS", "", "REALISE", "84 000 000",
     "O", "15/01/2026"),
    ("2024", "G", "638600", "Frais de formation", "-", "", "REALISE", "12 500 000", "O",
     "15/01/2026"),
    ("2024", "G", "651100", "Salaires et appointements", "DG01", "", "BUDGET", "1 300 000 000",
     "O", "15/01/2026"),
]


def build():
    d = Diagram(seed=63)

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2):
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b)
        return b

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "T63 et T64 : les données à fournir par la CNSS", 48)
    d.text(0, 66, "Structure demandée par l'équipe BI à la DSI et à la DFC · la DSI choisit "
                  "d'en faire une table ou une vue, et l'alimente depuis Oracle GL · "
                  "état au 07/11/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. pourquoi
    s1 = 140
    d.section(0, s1, WIDTH, 330, "1. Pourquoi ces données")
    bloc(40, s1 + 80, 740, 210,
         "T63 : FRAIS DE PERSONNEL\nsur CHARGES DE FONCTIONNEMENT\n\n5 dernières années, "
         "fin d'exercice\nnorme : 85 % au plus", 22, couleurs=(C["bleu_clair"], C["bleu"]))
    bloc(820, s1 + 80, 740, 210,
         "T64 : DÉPENSES DE FORMATION\nsur MASSE SALARIALE, et sur TOTAL BUDGET\n\n"
         "5 dernières années, fin d'exercice\nnorme : de 1 à 5 % (formation / masse salariale)",
         22, couleurs=(C["bleu_clair"], C["bleu"]))
    bloc(1600, s1 + 80, 760, 210,
         "CES MONTANTS SONT DANS ORACLE GL\nla paie n'est pas dans la base de la GRH\n\n"
         "recueil des besoins v7 (T63, T64) :\n« Voir la DFC » · « Source : Oracle GL »\n"
         "« Déclinaison : Direction, Agence, Bureau »", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))

    # ------------------------------------------------------------------ 2. structure
    s2 = s1 + 330 + 50
    sec2 = d.section(0, s2, WIDTH, 1000, "2. La structure demandée")
    bloc(40, s2 + 80, 2320, 100,
         "NOM PROPOSÉ : BI_GRH_SOLDE_COMPTABLE (table ou vue, au choix de la DSI)\n"
         "UNE LIGNE = un exercice × une gestion × un compte × une section analytique × une "
         "nature de montant (réalisé ou budget)", 22, couleurs=(C["vert_clair"], C["vert"]), sw=3)
    widths = [400, 280, 190, 1390]
    xs = [40]
    for w in widths[:-1]:
        xs.append(xs[-1] + w + 20)
    top = s2 + 210
    for titre, x in zip(["Colonne", "Type", "Obligatoire", "Contenu"], xs):
        d.text(x + 4, top, titre, 21, C["doux"])
    y = top + 42
    for i, (col, typ, obl, contenu) in enumerate(COLONNES):
        h = 70
        fond = C["blanc"] if i % 2 == 0 else C["section"]
        g = d.group()
        ligne = d.box(40, y, 2320, h, bg=fond, stroke=C["gris_bord"], stroke_width=1, groups=g)
        d.text(xs[0] + 16, y + 20, col, 20, groups=g, inside=ligne, mono=True)
        d.text(xs[1] + 16, y + 21, typ, 19, C["doux"], groups=g, inside=ligne, mono=True)
        d.text(xs[2] + 16, y + 21, obl, 20, C["vert"] if obl == "oui" else C["doux"], groups=g,
               inside=ligne)
        d.text(xs[3] + 16, y + 21, contenu, 19, groups=g, inside=ligne)
        y += h + 8
    sec2["height"] = y + 32 - s2

    # ------------------------------------------------------------------ 3. exemple
    s3 = y + 32 + 50
    sec3 = d.section(0, s3, WIDTH, 400, "3. À quoi ça ressemble (lignes fictives)")
    ew = [150, 190, 150, 360, 260, 170, 230, 260, 180, 230]
    ex = [40]
    for w in ew[:-1]:
        ex.append(ex[-1] + w + 12)
    top = s3 + 80
    noms = ["EXERCICE", "CODE_GESTION", "COMPTE", "LIBELLE_COMPTE", "SECTION_ANALYTIQUE",
            "CODE_UNITE", "NATURE_MONTANT", "MONTANT", "EXERCICE_CLOS", "DATE_MISE_A_JOUR"]
    for nom, x, w in zip(noms, ex, ew):
        d.box(x, top, w, 50, nom, 13, bg=C["gris"], stroke=C["gris_bord"], stroke_width=1)
    y = top + 60
    for ligne in EXEMPLE:
        for val, x, w in zip(ligne, ex, ew):
            d.box(x, y, w, 48, val if val else " ", 16, bg=C["blanc"], stroke=C["gris_bord"],
                  stroke_width=1)
        y += 56
    sec3["height"] = y + 30 - s3

    # ------------------------------------------------------------------ 4. règles
    s4 = y + 30 + 50
    d.section(0, s4, WIDTH, 470, "4. Les règles d'alimentation")
    bloc(40, s4 + 80, 1140, 350,
         "CE QU'ON MET DEDANS\n"
         "· le réalisé : les comptes de charges (classes 6 et 8),\n"
         "  de toutes les gestions\n"
         "· le budget : tous les comptes budgétés de l'exercice\n"
         "  (fonctionnement et investissement), budget définitif\n"
         "· les 5 derniers exercices clos et l'exercice en cours\n"
         "· des montants en francs CFA, cumulés sur l'exercice", 20)
    bloc(1220, s4 + 80, 1140, 350,
         "CE QU'ON N'Y MET PAS\n"
         "· aucune écriture une à une : des totaux par compte\n"
         "· aucun nom, aucun tiers, aucun numéro bancaire\n\n"
         "MISE À JOUR\n"
         "· au moins après chaque clôture d'exercice ;\n"
         "  si possible après chaque clôture mensuelle\n"
         "· un exercice remis à jour remplace toutes ses lignes", 20)

    # ------------------------------------------------------------------ 5. BI et DFC
    s5 = s4 + 470 + 50
    d.section(0, s5, WIDTH, 470, "5. Ce que la BI en fera, et ce qu'il faut savoir de la DFC")
    bloc(40, s5 + 80, 1140, 350,
         "LA BI REGROUPE LES COMPTES (recueil des besoins v7)\n"
         "· frais de personnel : comptes 65, gestions G et S\n"
         "· masse salariale : comptes 651 et 652\n"
         "· dépenses de formation : « 63, 86 et 87 », gestions G et S\n"
         "· charges de fonctionnement : comptes 61 à 66, gestion G\n"
         "· total budget : les lignes BUDGET\n"
         "Puis elle calcule T63 et T64, par année et par unité.\n"
         "Un compte changé ne demande aucune nouvelle extraction.", 20,
         couleurs=(C["vert_clair"], C["vert"]))
    bloc(1220, s5 + 80, 1140, 350,
         "LES QUESTIONS À LA DFC\n"
         "1. Charges de fonctionnement : comptes 61 à 66, ou 61 à 68 ?\n"
         "   gestion G seule, ou G et S ?\n"
         "2. Formation : que veut dire « 63, 86 et 87 » ?\n"
         "   6386 et 6387 ? ou 63812, 6386 et 6577 ?\n"
         "3. Masse salariale : 651 et 652, ou 651 seul ?\n"
         "4. Total budget : quelles gestions ? avec l'investissement ?\n"
         "5. Les charges de personnel ont-elles une section analytique ?", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
