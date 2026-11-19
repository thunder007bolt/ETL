"""
Les règles de chaque tableau CIPRES des prestations, en un tableau compact par tableau :
« # | Règle | Dans la base | Votre remarque ». À montrer à la CNSS : elle voit d'un coup d'œil
les règles appliquées et écrit ses remarques dans la dernière colonne.

Les règles viennent de la fiche des règles (analyses/regles_cipres/fiche_regles.md, numéros
entre parenthèses) et des documents de famille (regles_t*_*.md) ; les champs, des requêtes
proposées. Une règle encore en question porte le numéro de son point à valider
(analyses/prestations/points_a_valider.md), en orange.

Pilote : T32 (15/11/2026), validé par l'utilisateur. Les autres tableaux sont dans le paquet
regles_tableaux_donnees, un module par famille (t25_t27, t28_t31, t33_t41, t42_t49, t50_t57),
chacun avec son dictionnaire TABLEAUX au même format que T32.
GRH (16/11/2026) : modules grh_effectif (T58, T59, T61, T65) et grh_autres (T60, T62, T63, T64),
sortie dans schemas/grh/1 pour la cnss/regles par tableau.

Usage :
    python3 excalidraw/sources/prestations/regles_par_tableau.py [--tableau T32 | --tous] [--force]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, text_size  # noqa: E402
from fiche_cipres import height_of, wrap  # noqa: E402

DOSSIER = ROOT / "schemas" / "prestations" / "1 pour la cnss" / "regles par tableau"
DOSSIER_GRH = ROOT / "schemas" / "grh" / "1 pour la cnss" / "regles par tableau"


def dossier_de(cle):
    """T58 à T65 : GRH ; sinon prestations."""
    return DOSSIER_GRH if int(cle[1:]) >= 58 else DOSSIER

# Colonnes du tableau : numéro, règle, champ de la base, remarque de la CNSS
COL_NUM, COL_REGLE, COL_BASE, COL_REM = 70, 1050, 820, 560
LARGEUR = COL_NUM + COL_REGLE + COL_BASE + COL_REM
TAILLE, PAD = 20, 16

# Une règle : (texte, champs de la base, point à valider ou None)
TABLEAUX = {
    "T32": {
        "titre": "T32 — Les pensionnés de vieillesse par âge et par sexe : les règles de la BI",
        "chapeau": (
            "T32 compte, à une date d'arrêté, les personnes qui ont une pension de vieillesse en cours, "
            "par tranche d'âge de 5 ans et par sexe, avec le montant payé et le montant moyen.\n"
            "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
            "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche des pensions.", "DOSSIER.TDOS_CODE = V", None),
                ("La pension de vieillesse. La retraite anticipée va sur une ligne à part, "
                 "hors des tranches d'âge.",
                 "PRESTATION_ESP.TPE_CODE = RPV ;\nretraite anticipée : RPRA", "34"),
                ("La personne qui a droit, une seule fois, même avec deux pensions de vieillesse.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None),
                ("Les accessoires (bonification, majorations pour enfant, charges du ménage) "
                 "ne font pas un pensionné de plus.",
                 "TPE_CODE = RBEC, RME, RME-COL, RCCM : écartés", None),
            ]),
            ("Qui est en cours à la date d'arrêté", [
                ("La pension a commencé avant ou le jour de la date d'arrêté.",
                 "PRESTATION_ESP.PE_DATE_EFFET ≤ date d'arrêté", None),
                ("Elle n'est pas radiée.", "PE_DATE_RADIATION vide ou après la date", None),
                ("Elle n'est pas suspendue, sauf si elle a repris avant la date.",
                 "PE_DATE_SUSPENSION vide ou après la date,\nou PE_DATE_REACTIVATION avant la date", None),
                ("La personne est en vie.", "INDIVIDU.IND_DATE_DECES vide ou après la date", None),
                ("Payée ou non : une pension en cours compte même sans paiement à la fin de la période "
                 "(certificat de vie attendu, par exemple).", "aucun paiement exigé", None),
            ]),
            ("La date d'arrêté", [
                ("Chaque fin de mois, le 31 décembre compris.", "paramètre de la requête", None),
                ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi "
                 "plus tard n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None),
            ]),
            ("L'âge et le sexe", [
                ("L'âge = l'année du calcul moins l'année de naissance, sans regarder le jour. "
                 "Les tranches sont celles de la maquette CIPRES.", "INDIVIDU.IND_DATE_NAISSANCE", None),
                ("Une date de naissance par défaut (année 1900) va dans « 100 ans et plus ».",
                 "IND_DATE_NAISSANCE en 1900", None),
                ("Le sexe : 1 hommes, 2 femmes.", "INDIVIDU.IND_SEXE", None),
            ]),
            ("Le montant", [
                ("Ce qui a été payé dans la période aux personnes comptées.",
                 "DEBOURS.DEB_MONTANT, relié par PE_ID", "35"),
                ("Un paiement est un débours de paiement payé, par titre de paiement (D) ou sans titre (P). "
                 "Jamais le net à payer du titre.", "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", None),
                ("Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi.",
                 "DEBOURS.DEB_DATE_EFFET", None),
                ("La pension seule : l'argent des accessoires n'entre pas dans le montant.",
                 "seulement les débours des pensions RPV (et RPRA)", None),
                ("L'année = la somme des mois. Montant moyen = montant ÷ effectif.", "calcul", None),
            ]),
            ("Les cas particuliers", [
                ("Pension de coordination payée par l'autre caisse : comptée, sans montant. Seule la part "
                 "payée par la CNSS entre dans le montant.", "DOSSIER.DOS_CATEGORIE = COOR_AC", "36"),
                ("Dossier transféré à une autre caisse : compté aujourd'hui.",
                 "DOSSIER.DOS_CATEGORIE = CNSS_AC", "101"),
            ]),
            ("Le découpage et le contrôle", [
                ("Chaque chiffre se découpe par direction régionale et par agence, celles du dossier.",
                 "DOSSIER.DR_NO, DOSSIER.SP_NO", None),
                ("Contrôle : stock de fin = stock d'avant + nouveaux retraités − décès − autres sorties.",
                 "T32 de l'année d'avant, T42, T50", None),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 31 093 pensionnés (24 814 hommes, 6 279 femmes), pour 30 808 "
            "dans l'annuaire CNSS 2024 (30 730 en vieillesse et 78 en retraite anticipée) : +0,9 %. "
            "Montant payé aux pensionnés comptés : 37,67 Md, pour 38,24 Md publiés."
        ),
    },
}


from regles_tableaux_donnees import (  # noqa: E402
    grh_autres, grh_effectif, t25_t27, t28_t31, t33_t41, t42_t49, t50_t57)

for _famille in (t25_t27, t28_t31, t33_t41, t42_t49, t50_t57, grh_effectif, grh_autres):
    TABLEAUX.update(_famille.TABLEAUX)
TABLEAUX = dict(sorted(TABLEAUX.items()))


def _cellule(d, x, y, w, h, bg, stroke=C["gris_bord"]):
    return d.box(x, y, w, h, bg=bg, stroke=stroke, stroke_width=1)


def build(cle):
    t = TABLEAUX[cle]
    d = Diagram(seed=32)
    x0, y = 0, 0

    d.text(x0, y, t["titre"], 36, color=C["bleu"])
    y += 64
    chapeau = wrap(t["chapeau"], TAILLE, LARGEUR)
    d.text(x0, y, chapeau, TAILLE, color=C["doux"])
    y += height_of(chapeau, TAILLE) + 28

    # en-tête
    entetes = [("#", COL_NUM), ("Règle", COL_REGLE), ("Dans la base", COL_BASE),
               ("Votre remarque", COL_REM)]
    x = x0
    for libelle, w in entetes:
        cell = _cellule(d, x, y, w, 52, C["bleu_moyen"], C["bleu"])
        d.text(x + w / 2, y + 13, libelle, 22, align="center", inside=cell)
        x += w
    y += 52

    num = 0
    for titre_groupe, regles in t["groupes"]:
        cell = _cellule(d, x0, y, LARGEUR, 44, C["gris"])
        d.text(x0 + PAD, y + 10, titre_groupe, 21, color=C["bleu"], inside=cell)
        y += 44
        for texte, base, point in regles:
            num += 1
            regle = wrap(texte, TAILLE, COL_REGLE - 2 * PAD)
            champ = wrap(base, TAILLE - 2, COL_BASE - 2 * PAD)
            h_regle = height_of(regle, TAILLE) + (TAILLE * 1.6 if point else 0)
            h = max(h_regle, height_of(champ, TAILLE - 2)) + 2 * PAD
            x = x0
            c = _cellule(d, x, y, COL_NUM, h, C["blanc"])
            d.text(x + COL_NUM / 2, y + PAD, str(num), TAILLE, align="center", inside=c)
            x += COL_NUM
            c = _cellule(d, x, y, COL_REGLE, h, C["blanc"])
            d.text(x + PAD, y + PAD, regle, TAILLE, inside=c)
            if point:
                d.text(x + PAD, y + PAD + height_of(regle, TAILLE) + 6,
                       f"À valider : point {point}", TAILLE - 2, color=C["orange"], inside=c)
            x += COL_REGLE
            c = _cellule(d, x, y, COL_BASE, h, C["section"])
            d.text(x + PAD, y + PAD, champ, TAILLE - 2, color=C["doux"], inside=c)
            x += COL_BASE
            _cellule(d, x, y, COL_REM, h, C["jaune_clair"], C["jaune_bord"])
            y += h

    if not t.get("chiffres"):
        return d
    y += 30
    chiffres = wrap(t["chiffres"], TAILLE, LARGEUR - 2 * PAD)
    h = height_of(chiffres, TAILLE) + 2 * PAD
    cell = d.box(x0, y, LARGEUR, h, bg=C["vert_clair"], stroke=C["vert"])
    d.text(x0 + PAD, y + PAD, chiffres, TAILLE, inside=cell)
    return d


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Règles d'un tableau CIPRES, en Excalidraw.")
    parser.add_argument("--tableau", default="T32", choices=sorted(TABLEAUX))
    parser.add_argument("--tous", action="store_true", help="tous les tableaux")
    parser.add_argument("--grh", action="store_true", help="les tableaux GRH (T58 à T65)")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    erreurs = 0
    cles = (list(TABLEAUX) if args.tous else [c for c in TABLEAUX if int(c[1:]) >= 58]
            if args.grh else [args.tableau])
    for cle in cles:
        diagram = build(cle)
        sortie = dossier_de(cle) / f"{cle} regles.excalidraw"
        try:
            path = diagram.save(sortie, force=args.force)
        except FileExistsError as e:
            print(cle, ":", e)
            erreurs += 1
            continue
        print(cle, ":", "inchangé" if diagram.inchange
              else f"{len(diagram.elements)} éléments écrits dans {path}")
        for w in diagram.warnings:
            print("ATTENTION", cle, w)
    sys.exit(1 if erreurs else 0)
