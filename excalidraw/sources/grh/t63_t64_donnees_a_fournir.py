"""
Schéma « T63 et T64 : les données à fournir par la CNSS » (état au 07/11/2026).

Ce que l'équipe BI demande à la DSI et à la DFC pour T63 et T64 : pourquoi, la
structure (nom, colonnes, types, grain), des lignes d'exemple fictives, les
règles, et ce qu'il faut savoir de la DFC. Pas de SQL : la CNSS crée la vue
dans la base source et l'alimente depuis Oracle GL.

Depuis le 07/11/2026 au soir, la structure est la vue commune de la
comptabilité, V_BI_BALANCE_COMPTABLE, qui sert aussi à la gestion financière
(schemas/vues cnss/01 balance comptable). Ses colonnes et ses règles viennent
de sources/vues_cnss/commun.py : les deux schémas disent la même chose.

Usage :
    python3 excalidraw/sources/grh/t63_t64_donnees_a_fournir.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "vues_cnss"))

from excalidraw_builder import C, Diagram, main  # noqa: E402
from commun import (BALANCE_COLONNES, BALANCE_EXEMPLE_LARGEURS,  # noqa: E402
                    BALANCE_EXEMPLE_NOMS, BALANCE_LIGNE, BALANCE_NOM, BALANCE_REGLES,
                    WIDTH, bloc, entete, exemple, rangee, tableau_colonnes)

OUTPUT = ROOT / "schemas" / "grh" / "T63 T64 finances" / "t63_t64_donnees_a_fournir.excalidraw"

BLEU = (C["bleu_clair"], C["bleu"])
JAUNE = (C["jaune_clair"], C["jaune_bord"])
VERT = (C["vert_clair"], C["vert"])
BLANC = (C["blanc"], C["encre"])

# lignes fictives, colonnes de BALANCE_EXEMPLE_NOMS
EXEMPLE = [
    ("2024", "3", "G", "COMMUN", "651100", "Salaires et appointements", "DG01", "REALISE",
     "104 000 000", "0", "O", "15/04/2024"),
    ("2024", "3", "S", "COMMUN", "652000", "Primes et gratifications", "DPASS", "REALISE",
     "7 000 000", "0", "O", "15/04/2024"),
    ("2024", "5", "G", "COMMUN", "638600", "Frais de formation", "-", "REALISE", "2 500 000",
     "0", "O", "15/06/2024"),
    ("2024", "12", "G", "COMMUN", "651100", "Salaires et appointements", "DG01", "BUDGET",
     "1 300 000 000", "0", "O", "15/01/2025"),
]


def build():
    d = Diagram(seed=63)

    def section(top, titre):
        return d.section(0, top, WIDTH, 300, titre)

    def fermer(sec, bas):
        sec["height"] = bas + 40 - sec["y"]
        return bas + 40 + 50

    s = entete(d, "T63 et T64 : les données à fournir par la CNSS",
               f"Ces montants viendront de la vue commune de la comptabilité, {BALANCE_NOM}, que "
               "la CNSS crée dans la base source et alimente depuis Oracle GL. Elle sert aussi "
               "à la gestion financière (schéma « vues cnss / 01 balance comptable ») · état au "
               "07/11/2026")

    # ------------------------------------------------------------------ 1. pourquoi
    sec = section(s, "1. Pourquoi ces données")
    bas = rangee(d, s + 80, [
        ("T63 : FRAIS DE PERSONNEL sur CHARGES DE FONCTIONNEMENT\n5 dernières années, fin "
         "d'exercice\nnorme : 85 % au plus", BLEU, 2),
        ("T64 : DÉPENSES DE FORMATION sur MASSE SALARIALE, et sur TOTAL BUDGET\n5 dernières "
         "années, fin d'exercice\nnorme : de 1 à 5 % (formation / masse salariale)", BLEU, 2),
        ("CES MONTANTS SONT DANS ORACLE GL\nla paie n'est pas dans la base de la GRH\n"
         "recueil des besoins v7 (T63, T64) : « Voir la DFC » · « Source : Oracle GL » · "
         "« Déclinaison : Direction, Agence, Bureau »", JAUNE, 2),
    ], size=21)
    s = fermer(sec, bas)

    # ------------------------------------------------------------------ 2. structure
    sec = section(s, "2. La structure demandée")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             f"NOM PROPOSÉ : {BALANCE_NOM} (une vue dans la base source, alimentée depuis "
             f"Oracle GL)\nUNE LIGNE = {BALANCE_LIGNE}", 22, VERT, sw=3)
    bas = tableau_colonnes(d, b["y"] + b["height"] + 30, BALANCE_COLONNES)
    s = fermer(sec, bas)

    # ------------------------------------------------------------------ 3. exemple
    sec = section(s, "3. À quoi ça ressemble (comptes et montants fictifs)")
    bas = exemple(d, s + 80, BALANCE_EXEMPLE_NOMS, BALANCE_EXEMPLE_LARGEURS, EXEMPLE)
    s = fermer(sec, bas)

    # ------------------------------------------------------------------ 4. règles
    sec = section(s, "4. Les règles d'alimentation")
    bas = rangee(d, s + 80, [(t, BLANC, 2) for t in BALANCE_REGLES], size=20)
    s = fermer(sec, bas)

    # ------------------------------------------------------------------ 5. BI et DFC
    sec = section(s, "5. Ce que la BI en fera, et ce qu'il faut savoir de la DFC")
    bas = rangee(d, s + 80, [
        ("LA BI REGROUPE LES COMPTES (recueil des besoins v7)\n"
         "· frais de personnel : comptes 65, gestions G et S\n"
         "· masse salariale : comptes 651 et 652\n"
         "· dépenses de formation : « 63, 86 et 87 », gestions G et S\n"
         "· charges de fonctionnement : comptes 61 à 66, gestion G\n"
         "· total budget : les lignes BUDGET\n"
         "Puis elle calcule T63 et T64, par année et par unité. Un compte changé ne demande "
         "aucune nouvelle extraction.", VERT, 2),
        ("LES QUESTIONS À LA DFC\n"
         "1. Charges de fonctionnement : comptes 61 à 66, ou 61 à 68 ? gestion G seule, ou G "
         "et S ?\n"
         "2. Formation : que veut dire « 63, 86 et 87 » ? 6386 et 6387 ? ou 63812, 6386 et "
         "6577 ?\n"
         "3. Masse salariale : 651 et 652, ou 651 seul ?\n"
         "4. Total budget : quelles gestions ? avec l'investissement ?\n"
         "5. Les charges de personnel ont-elles une section analytique ?", JAUNE, 2),
    ], size=20)
    fermer(sec, bas)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
