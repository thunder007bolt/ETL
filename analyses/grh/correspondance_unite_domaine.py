"""
Fichier Excel de correspondance « unité administrative → domaine d'activité » (T59).

Décision de la réunion du 03/11/2026 (terme 8 de la fiche de lecture GRH) : le domaine d'un agent
est celui de son unité administrative (GRH_SITUATION.UA_CODE), lu dans un fichier tenu par la CNSS.
Ce fichier sert aussi à enrichir la dimension des unités administratives : il contient donc toutes
les unités de la CNSS, en vigueur ou fermées, qu'elles aient des agents ou non.

Entrée  : unites_cnss.tsv, la liste des unités de la table GRH_UNITE_ADMINISTRATIVE sans les
          organismes de formation (lot 23, Q130). Pour la mettre à jour, relancer Q130 et
          remplacer le fichier (mêmes colonnes).
Reprise : --reprendre <fichier rempli par la CNSS> garde les domaines déjà retenus ; une unité
          apparue depuis est marquée « Nouvelle unité ».
Sortie  : correspondance_unite_domaine.xlsx.

La proposition de domaine vient du nom de l'unité (règles plus bas) ; une unité dont le nom ne dit
rien prend la proposition de son unité de rattachement ; sinon, « À décider ».

Usage (openpyxl requis) :
    python3 analyses/grh/correspondance_unite_domaine.py [--reprendre FICHIER.xlsx]
"""

import argparse
import csv
import re
import unicodedata
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation

DOSSIER = Path(__file__).parent
ENTREE = DOSSIER / "unites_cnss.tsv"
SORTIE = DOSSIER / "correspondance_unite_domaine.xlsx"

DOMAINES = [
    "Recouvrement",
    "Prestations",
    "Prévention des risques professionnels",
    "Action sanitaire et sociale",
    "Gestion administrative et financière",
]
REC, PRE, PRV, ASS, GAF = DOMAINES
A_DECIDER = "À décider"

REGIONS = {"000": "Siège", "100": "DRO (Ouagadougou)", "500": "DRB (Bobo-Dioulasso)",
           "300": "DRN (Nord)", "200": "DRF (Fada N'Gourma)", "400": "DRD (Dédougou)"}
ORDRE_REGIONS = ["000", "100", "500", "300", "200", "400"]

# Mots du nom de l'unité (sans accents, en majuscules) qui donnent le domaine, testés dans cet
# ordre : l'audit des prestations est de la gestion, un service médico-social n'est pas le
# service social du personnel, etc.
MOTS = [
    (GAF, ["AUDIT"]),
    (ASS, ["MEDICO", "SMI", "S.M.I", "SANTE MATERNELLE", "PROMOTION DES ASSURES", "S.P.A.S",
           "SPAS"]),
    (PRV, ["PREVENTION"]),
    (REC, ["RECOUVREMENT", "COTISATION", "IMMATRICULATION", "CONTROLE EMPLOYEUR",
           "ASSURANCE VOLONTAIRE"]),
    (PRE, ["PRESTATION", "VIEILLESSE", "VIELLESSE", "ACCIDENT", "AT/MP", "FAMILIALE", r"\bAV\b",
           r"\bPF\b", r"\bAT\b"]),
    (GAF, ["GESTION", "BUDGET", "PLANIFICATION", "FINANCI", "COMPTAB", "TRESOR",
           "RESSOURCES HUMAINES", r"\bRH\b", "PERSONNEL", "PAIE", "FORMATION", "STAGES",
           "SOCIAL", "COMMUNICATION", "QUALITE", "ARCHIVES", "DOCUMENT", "COURRIER", "MARCHES",
           "SECURITE", "INFORMATI", "SYSTEME", "D.B.A", "ETUDE", "STATISTIQUE", "PROSPECTIVE",
           "COOPERATION", "INGENIERIE", "EQUIPEMENT", "APPROVISIONNEMENT", "LOGISTIQUE",
           "IMMEUBLES", "PATRIMOINE", "IMMOBILI", "ENGAGEMENT", "JURIDIQUE", "CONSEILLER",
           "GENERAL", "PROTOCOLE", "EXTERIEUR", "ADMINISTRATI"]),
]


def sans_accents(texte):
    texte = unicodedata.normalize("NFD", texte)
    return "".join(c for c in texte if unicodedata.category(c) != "Mn").upper()


def proposer(u, unites, cache):
    """Proposition de domaine d'une unité, et pourquoi elle est « À décider » le cas échéant."""
    if u["UA_CODE"] in cache:
        return cache[u["UA_CODE"]]
    t = sans_accents(f"{u['LIBELLE']} {u['SIGLE']}")
    lib = sans_accents(u["LIBELLE"])
    parent = unites.get(u["UA_CODE_SUP"])

    def du_parent(motif_si_rien):
        if parent:
            prop, motif = proposer(parent, unites, cache)
            return prop, (f"suit {parent['SIGLE'] or parent['LIBELLE']} : {motif}"
                          if prop == A_DECIDER else "")
        return A_DECIDER, motif_si_rien

    if u["CATEGORIE"] == "R" and (lib.startswith(("AGENCE", "BUREAU", "GUICHET"))
                                  or "PROVINCIAL" in lib):
        res = (A_DECIDER, "agence ou bureau : recouvrement et prestations")
    elif u["NATURE"] == "Direction" and "DIRECTION REGIONALE" in lib:
        res = (A_DECIDER, "agents rattachés à la direction régionale, sans service")
    elif "PREVENTION" in t and ("SANITAIRE" in t or u["NATURE"] == "Direction"):
        res = (A_DECIDER, "direction de la prévention et de l'action sanitaire et sociale")
    elif lib.startswith("SECRETARIAT") and u["NATURE"] != "Direction":
        res = du_parent("secrétariat sans direction")
    else:
        res = None
        for domaine, mots in MOTS:
            if any(re.search(m, t) if m.startswith(r"\b") else m in t for m in mots):
                res = (domaine, "")
                break
        if res is None:
            res = du_parent("le nom ne dit pas le domaine")
    cache[u["UA_CODE"]] = res
    return res


def lire_unites():
    with open(ENTREE, encoding="utf-8") as f:
        return {r["UA_CODE"]: r for r in csv.DictReader(f, delimiter="\t")}


def lire_reprise(chemin):
    """Les domaines déjà retenus par la CNSS dans une version précédente du fichier."""
    ws = load_workbook(chemin, read_only=True)["Correspondance"]
    lignes = ws.iter_rows(values_only=True)
    entetes = next(lignes)
    i_code = entetes.index(ENTETES[0])
    i_dom = entetes.index("Domaine retenu par la CNSS")
    i_com = entetes.index("Commentaire de la CNSS")
    return {str(r[i_code]): (r[i_dom], r[i_com]) for r in lignes if r[i_code]}


ENTETES = ["Code unité (UA_CODE)", "Sigle", "Nom de l'unité", "Nature", "Direction régionale",
           "Rattachée à", "Statut", "Agents actifs (18/02/2026)", "Proposition BI",
           "Pourquoi « À décider »", "Domaine retenu par la CNSS", "Commentaire de la CNSS",
           "Ligne"]
LARGEURS = [20, 26, 52, 11, 22, 26, 12, 14, 36, 50, 36, 40, 16]

FOND_ENTETE = PatternFill("solid", fgColor="1F4E79")
FOND_DECIDER = PatternFill("solid", fgColor="FCE4D6")
FOND_CNSS = PatternFill("solid", fgColor="FFF2CC")
FOND_FERMEE = PatternFill("solid", fgColor="EDEDED")
FOND_NOUVELLE = PatternFill("solid", fgColor="DDEBF7")

MODE_EMPLOI = [
    ("Correspondance unité → domaine d'activité (tableau CIPRES T59)", True),
    ("", False),
    ("À quoi sert ce fichier", True),
    ("Le domaine d'activité d'un agent est celui de son unité administrative (décision de la "
     "réunion du 03/11/2026). Ce fichier dit, pour chaque unité de la CNSS, à quel domaine elle "
     "appartient. La BI le charge dans l'entrepôt de données : il complète la dimension des unités "
     "administratives et sert au tableau T59, à toute date, passée ou future.", False),
    ("", False),
    ("Ce qu'il contient", True),
    ("Toutes les unités de la table des unités administratives de l'application GRH, sauf les "
     "organismes de formation : les unités en vigueur (codes à 8 chiffres) et les unités fermées "
     "des organigrammes précédents (2012, codes à 6 chiffres ; 1993, codes à 4 chiffres), qu'elles "
     "aient des agents ou non. Les unités fermées restent utiles pour les années passées.", False),
    ("", False),
    ("Comment le remplir", True),
    ("1. Pour chaque ligne, choisir le domaine dans la colonne « Domaine retenu par la CNSS » "
     "(liste déroulante des 5 domaines du recueil des besoins).", False),
    ("2. La colonne « Proposition BI » est une aide, tirée du nom de l'unité. Elle peut être "
     "acceptée ou corrigée.", False),
    ("3. Les lignes en orange (« À décider ») sont les cas où le nom ne suffit pas : agences, "
     "bureaux et guichets (recouvrement et prestations), agents rattachés directement à une "
     "direction régionale, direction de la prévention et de l'action sanitaire et sociale.", False),
    ("4. Les lignes grises sont des unités fermées ; celles qui ont encore des agents actifs sont à "
     "corriger dans l'application GRH si possible.", False),
    ("", False),
    ("Quand l'organigramme change", True),
    ("Une unité créée dans l'application GRH apparaît à la mise à jour suivante du fichier, en "
     "bleu, marquée « Nouvelle unité » : il suffit de lui donner un domaine. Les domaines déjà "
     "retenus sont gardés.", False),
    ("", False),
    ("Règle appliquée par la BI", True),
    ("Un agent dont l'unité n'a pas de domaine retenu est compté en « Domaine non renseigné ». La "
     "BI ne choisit pas de domaine à la place de la CNSS.", False),
    ("", False),
    ("Source", True),
    ("Table GRH_UNITE_ADMINISTRATIVE de l'application GRH, copie de la base du 18/02/2026 (lot 23, "
     "Q130). Agents actifs : agents de la CNSS, fiche active, unité de leur dernière situation, à "
     "titre d'information.", False),
]


def ordre(u, unites):
    """Siège puis régions ; en vigueur avant fermées ; la direction avant ses services."""
    direction = u["UA_CODE_SUP"] or u["UA_CODE"]
    return (u["STATUT"] != "O", ORDRE_REGIONS.index(u["REGION"]) if u["REGION"] in ORDRE_REGIONS
            else 9, unites[direction]["LIBELLE"] if direction in unites else "",
            u["NATURE"] != "Direction", u["LIBELLE"])


def construire(reprise=None):
    unites = lire_unites()
    deja = lire_reprise(reprise) if reprise else {}
    cache = {}

    wb = Workbook()
    ws = wb.active
    ws.title = "Correspondance"
    ws.append(ENTETES)
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = FOND_ENTETE
        cell.alignment = Alignment(wrap_text=True, vertical="center")

    for u in sorted(unites.values(), key=lambda u: ordre(u, unites)):
        prop, motif = proposer(u, unites, cache)
        parent = unites.get(u["UA_CODE_SUP"])
        retenu, commentaire = deja.get(u["UA_CODE"], (None, None))
        nouvelle = bool(deja) and u["UA_CODE"] not in deja
        ws.append([u["UA_CODE"], u["SIGLE"], u["LIBELLE"], u["NATURE"],
                   REGIONS.get(u["REGION"], u["REGION"]),
                   (parent["SIGLE"] or parent["LIBELLE"]) if parent else "",
                   "En vigueur" if u["STATUT"] == "O" else "Fermée", int(u["AGENTS_ACTIFS"]),
                   prop, motif, retenu, commentaire, "Nouvelle unité" if nouvelle else ""])
        row = ws.max_row
        fill = (FOND_NOUVELLE if nouvelle else FOND_FERMEE if u["STATUT"] != "O"
                else FOND_DECIDER if prop == A_DECIDER else None)
        if fill:
            for cell in ws[row][:10]:
                cell.fill = fill
        if u["STATUT"] != "O":
            for cell in ws[row][:10]:
                cell.font = Font(italic=True, color="595959")
            if prop == A_DECIDER:
                ws.cell(row, 9).fill = FOND_DECIDER
        ws.cell(row, 11).fill = FOND_CNSS
        ws.cell(row, 12).fill = FOND_CNSS

    for i, w in enumerate(LARGEURS, 1):
        ws.column_dimensions[ws.cell(1, i).column_letter].width = w
    ws.row_dimensions[1].height = 32
    ws.freeze_panes = "D2"
    ws.auto_filter.ref = f"A1:M{ws.max_row}"

    dom = wb.create_sheet("Domaines")
    dom.append(["Domaines d'activité (recueil des besoins CNSS v7, partie RH, ligne 2)"])
    dom["A1"].font = Font(bold=True)
    for d in DOMAINES:
        dom.append([d])
    dom.column_dimensions["A"].width = 70

    dv = DataValidation(type="list", formula1=f"=Domaines!$A$2:$A${len(DOMAINES) + 1}",
                        allow_blank=True)
    dv.error = "Choisir un des 5 domaines de la liste."
    dv.errorTitle = "Domaine"
    ws.add_data_validation(dv)
    dv.add(f"K2:K{ws.max_row}")

    me = wb.create_sheet("Mode d'emploi", 0)
    for texte, titre in MODE_EMPLOI:
        me.append([texte])
        cell = me.cell(me.max_row, 1)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        if titre:
            cell.font = Font(bold=True, size=13 if me.max_row == 1 else 11)
    me.column_dimensions["A"].width = 110

    wb.active = 1
    wb.save(SORTIE)
    return unites, cache


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--reprendre", type=Path, help="fichier déjà rempli par la CNSS")
    args = parser.parse_args()
    unites, cache = construire(args.reprendre)
    en_vigueur = sum(u["STATUT"] == "O" for u in unites.values())
    agents = sum(int(u["AGENTS_ACTIFS"]) for u in unites.values())
    print(f"{SORTIE} · {len(unites)} unités ({en_vigueur} en vigueur) · {agents} agents actifs")
