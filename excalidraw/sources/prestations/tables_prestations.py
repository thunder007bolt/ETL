"""
Schéma « Les tables du domaine Prestations » (état au 29/10/2026).

Sources : analyses/prestations (lots 1 et 2 : volumes P01, codes P02 à P12, faits P17,
tables DTM P18 ; lot 3 : listes de codes P31 à P47, les plus longues relues dans
analyses/prestations/resultats), facts/sql et dtm/sql du domaine prestation_recouvrement.

Usage :
    python3 excalidraw/sources/prestations/tables_prestations.py [--force]
"""

import csv
import re
import sys
from itertools import groupby
from math import ceil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, LINE_HEIGHT, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "tables_prestations.excalidraw"
RESULTS = ROOT.parent / "analyses" / "prestations" / "resultats"

# Listes courtes du lot 3 : (code, libellé, nombre de lignes ; None si jamais utilisé)
TPE_V = [("RPV", "pension de vieillesse", 46836), ("RPRA", "pension de retraite anticipée", 176),
         ("RPI", "pension d'invalidité", 231), ("RPSV", "pension de veuve", 25541),
         ("RPSF", "pension de veuf", 424), ("RPSE", "pension d'orphelin", 29689),
         ("RPA", "pension d'ascendant", 81), ("RAV", "allocation de vieillesse", 12833),
         ("RAS", "allocation de survivants", 864),
         ("RASC", "allocation de survivants : part conjoint", 1469),
         ("RASO", "allocation de survivants : part orphelin", 7580),
         ("RASA", "allocation de survivants : part ascendant", 327),
         ("RBEC", "bonification pour enfant à charge", 68836),
         ("RME", "majoration pour enfant CARFO", 762),
         ("RME-COL", "majoration pour enfant collectivités", 18),
         ("RCCM", "contributions aux charges du ménage", 70),
         ("RRC", "remboursement de cotisations (recours gracieux)", 17)]
TPE_A = [("ARI", "rente viagère d'incapacité", 2349), ("ARSV", "rente de veuve", 388),
         ("ARSF", "rente de veuf", 4), ("ARSE", "rente d'orphelin", 1121),
         ("ARSA", "rente d'ascendant", 350), ("AIJ", "indemnités journalières", 13201),
         ("AAI", "allocation d'incapacité", 1233)]
TPE_F = [("FAF", "allocation familiale", 614811), ("FAP", "allocation prénatale", 9600)]
TPE_M = [("MAP", "allocation prénatale", 52716), ("MIJ", "indemnités journalières", 56426)]
ETAPES = [("CIP", "calcul initial basé sur les paramètres", 947441),
          ("RP", "révision basée sur les paramètres", 588919), ("MAJ", "majoration", 167810),
          ("RRR", "rabais pour rachat de rente", 472), ("RP5", "retenue sur pension 5 %", 170),
          ("RP10", "retenue sur pension 10 %", 140), ("RP15", "retenue sur pension 15 %", 101),
          ("RP20", "retenue sur pension 20 %", 61), ("RP25", "retenue sur pension 25 %", 34),
          ("CORN", "correctif de type négatif", 162), ("CORP", "correctif de type positif", 4),
          ("PCCM", "prélèvement contribution aux charges du ménage", 27),
          ("FS", "frais périodiques supplémentaires", None)]
AJUSTEMENTS = [("AV", "avance", 760871), ("MP", "moins-perçu ou rappel", 424180),
               ("MPG", "mois de pension gratuit", 122185),
               ("DON-COVID19", "don kit sanitaire COVID 19", 22591), ("TP", "trop-perçu", 12480),
               ("RC", "rappel sur convention", 6272), ("REV-AV", "reversement d'avance", 3857),
               ("AV-COVID19", "avance COVID 19", 1399),
               ("TP-CONV", "trop-perçu sur convention", 937), ("RR", "rachat de rente", 379),
               ("REM-TP", "remboursement de trop-perçu", 242), ("OI", "opposition interne", 12),
               ("OE", "opposition externe", None)]
EFFETS = [("BVB", "bon de paiement par virement bancaire", 4303673),
          ("BE", "bon de paiement en espèces", 4242131),
          ("BPEMP", "bon de paiement aux employeurs en convention", 68545),
          ("BPM", "bon de paiement mobile", 53167), ("BC", "bon de paiement par chèque", 23889),
          ("BS", "bon de paiement structure sanitaire et sociale", 8339),
          ("BMP", "bon de paiement par mandat postal", 7435),
          ("BREV", "bon de reversement avance", 3806),
          ("BREMB", "bon de remboursement de TP", 205),
          ("RPC", "relevé de paiement, coordination entre caisses", None),
          ("RRC", "relevé de réclamation, coordination entre caisses", None)]
DIRECTIONS = [("10", "DRO", "Ouagadougou"), ("20", "DRB", "Bobo-Dioulasso"),
              ("30", "DRN", "Nord, Ouahigouya"), ("40", "DRF", "Fada N'Gourma"),
              ("50", "DRD", "Dédougou")]
ACRONYMS = ("CNSS", "CARFO", "AT", "PF", "AV")

# Grille de la partie 2 : 6 colonnes, dans l'ordre de la vie d'un dossier
COL_W, COL_GAP, X0 = 340, 100, 30
COLS = [X0 + i * (COL_W + COL_GAP) for i in range(6)]
TOP_Y, TOP_H = 600, 170
SPINE_Y, SPINE_H = 820, 210
BOT_Y, BOT_H = 1080, 180


def read_tsv(name):
    """Une liste longue sortie par le lot 3, gardée dans analyses/prestations/resultats."""
    with open(RESULTS / name, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def number(n):
    return f"{n:,}".replace(",", " ")


def plural(n, word):
    return f"{n} {word}{'x' if n > 1 else ''}"


def coded(rows):
    """Une ligne par code : code · libellé · nombre de lignes."""
    return "\n".join(f"{code} · {label} · {number(n) if n is not None else 'jamais utilisé'}"
                     for code, label, n in rows)


def lower1(label):
    return label[:1].lower() + label[1:]


def piece_label(label):
    """Libellé d'une pièce, écrit en capitales dans la base, remis en minuscules."""
    label = label.lower()
    for acronym in ACRONYMS:
        label = re.sub(rf"\b{acronym.lower()}\b", acronym, label)
    return re.sub(r"\d{4}-\d+/[a-z/]+", lambda m: m.group().upper(), label)


def place(name):
    """Nom de lieu de la base remis en minuscules : FADA N'GOURMA devient Fada N'Gourma."""
    if name == "CNSS-DCP":
        return name
    words = []
    for i, word in enumerate(name.split(" ")):
        if i and word.upper() in ("DU", "DE", "DES", "LA", "LE"):
            words.append(word.lower())
            continue
        parts = []
        for part in word.split("-"):
            lead = "(" if part.startswith("(") else ""
            part = part.lstrip("(")
            if "'" in part:
                head, tail = part.split("'", 1)
                head = head.lower() if head.upper() in ("D", "L") else head.capitalize()
                parts.append(f"{lead}{head}'{tail.capitalize()}")
            else:
                parts.append(lead + part.capitalize())
        words.append("-".join(parts))
    return " ".join(words)


def wrap(items, size, width, sep=", "):
    """Met les éléments bout à bout et passe à la ligne avant de dépasser width."""
    lines, line = [], ""
    for item in items:
        candidate = f"{line}{sep}{item}" if line else item
        if line and text_size(candidate + sep.strip(), size)[0] > width:
            lines.append(line + sep.strip())
            line = item
        else:
            line = candidate
    lines.append(line)
    return "\n".join(lines)


def up_arrow(d, a, b, label):
    """Flèche verticale du haut de a (ligne principale) vers le bas de b (ligne du dessus)."""
    cx = a["x"] + a["width"] / 2
    d.arrow(cx, a["y"] - 6, cx, b["y"] + b["height"] + 6, a, b, color=C["doux"])
    d.text(cx + 10, (a["y"] + b["y"] + b["height"]) / 2 - 10, label, 16, C["doux"])


def down_arrow(d, a, b, label):
    """Flèche verticale du bas de a vers le haut de b, avec la colonne de liaison."""
    d.varrow(a, b, color=C["doux"])
    cx = a["x"] + a["width"] / 2
    d.text(cx + 10, (a["y"] + a["height"] + b["y"]) / 2 - 10, label, 16, C["doux"])


def right_arrow(d, a, b, label, **kw):
    """Flèche horizontale de a vers b, avec la colonne de liaison au-dessus."""
    d.harrow(a, b, **kw)
    d.text((a["x"] + a["width"] + b["x"]) / 2, a["y"] + a["height"] / 2 - 30, label, 16,
           C["doux"], align="center")


def build():
    d = Diagram(seed=29)

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "Les tables du domaine Prestations", 48)
    d.text(0, 66, "CNSS Burkina Faso · de la base source aux tableaux CIPRES T25 à T57 · "
                  "état au 29/10/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. trois couches
    d.section(0, 120, 2600, 300, "1. Le trajet des données : 3 couches, puis les tableaux")
    layers = [
        ("BASE SOURCE\nschéma USER_DWH\n\nles tables de l'application\nde gestion des prestations\n"
         "copie figée au 02/02/2026", C["violet"], C["encre"]),
        ("DWH\nfaits DWH.FAIT_*\n\nune copie 1 pour 1\nde chaque table source\nun seul cliché (092026)",
         C["gris"], C["gris_bord"]),
        ("DTM\n6 tables DTM_*\n\ncomptent et regroupent\npour les tableaux\nrequêtes à corriger",
         C["gris"], C["gris_bord"]),
        ("TABLEAUX CIPRES\nT25 à T57\n\nl'Annuaire statistique\nremis chaque année",
         C["rouge_moyen"], C["rouge"]),
    ]
    prev = None
    for i, (txt, bg, stroke) in enumerate(layers):
        r = d.box(40 + i * 520, 185, 430, 200, txt, 20, bg=bg, stroke=stroke)
        if prev:
            d.harrow(prev, r, stroke_width=3)
        prev = r
    d.box(2120, 185, 450, 200,
          "Les analyses partent de la\nbase source : le contenu des\ntables DTM est jugé faux.\n"
          "On ne relit leur code que\npour le corriger.", 20, bg=C["jaune_clair"],
          stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 2. base source
    d.section(0, 470, 2600, 1010,
              "2. La base source : la vie d'un dossier, de la demande au paiement")
    d.text(25, 532, "Cadres épais : la colonne vertébrale. Flèche : ce qui suit dans l'histoire ; "
                    "texte sur la flèche : la colonne qui relie les deux tables. "
                    "Le détail de chaque étape : schéma vie_dossier.", 20, C["doux"])

    spine_txt = [
        "RECEPTION_DOSSIER\n273 005 lignes\n\nune demande déposée\nau guichet (date, déposant)\n"
        "clé RD_ID",
        "DOSSIER\n367 947 lignes\n\nle dossier d'un assuré\ndans une branche V, A, F, M\n"
        "statut A actif · F fermé\nclé DOS_CODE",
        "PRESTATION_ESP\n947 953 lignes\n\nun droit ouvert (ex. RPV)\nayant droit IND_ID_PREST\n"
        "personne payée IND_ID_BENEF\nstatut A · S · F\nclé PE_ID",
        "DEBOURS\n71,7 millions de lignes\n\nune somme due pour un mois\nmontant DEB_MONTANT\nmois payé DEB_DATE_EFFET\n"
        "statut D, P, V, I, N, A, S\nclé DEB_ID",
        "EFFET\n8,7 millions de lignes\n\nle titre de paiement\n(chèque, virement…)\n"
        "qui regroupe des débours\npersonne payée, NET_A_PAYER\nclé EFP_ID",
        "BORDEREAU_PREST\nvolume non mesuré\n\nun lot d'effets remis\nà une banque ou une caisse\n"
        "(nombre, net, décaissement)\nclé BO_ID",
    ]
    captions = ["la demande", "le dossier", "le droit", "la somme due", "le paiement", "le lot payé"]
    spine = []
    for x, txt, cap in zip(COLS, spine_txt, captions):
        d.text(x + 10, SPINE_Y - 28, cap.upper(), 16, C["doux"])
        spine.append(d.box(x, SPINE_Y, COL_W, SPINE_H, txt, 18, bg=C["blanc"], stroke=C["encre"],
                           stroke_width=3))
    for a, b, key in zip(spine, spine[1:], ["RD_ID", "DOS_CODE", "PE_ID", "EFP_ID", "BO_ID"]):
        right_arrow(d, a, b, key, stroke_width=3)
    reception, dossier, prestation, debours, effet, _bordereau = spine

    def table(col, y, h, txt, **kw):
        style = dict(bg=C["blanc"], stroke=C["doux"])
        style.update(kw)
        return d.box(COLS[col], y, COL_W, h, txt, 18, **style)

    # ligne du dessus : détails et corrections
    piece = table(0, TOP_Y, TOP_H, "RECEPTION_DOS_PIECE\nvolume non mesuré\n\nles pièces fournies\n"
                                   "à la réception")
    up_arrow(d, reception, piece, "RD_ID")
    sinistre = table(1, TOP_Y, TOP_H, "TSINISTRE\n25 086 lignes\n\nles accidents du travail\n"
                                      "(lésion, incapacité…)\nplus alimentée depuis 12/2024")
    up_arrow(d, dossier, sinistre, "DOS_CODE")
    pe_aj = table(2, TOP_Y, TOP_H, "PE_AJ\nvolume non mesuré\n\nrelie une prestation\n"
                                   "à ses ajustements")
    up_arrow(d, prestation, pe_aj, "PE_ID")
    ajustement = table(3, TOP_Y, TOP_H, "AJUSTEMENT\n1,36 million de lignes\n\n"
                                        "une correction : trop perçu,\nmoins perçu, rappel, avance\n"
                                        "statut A · C · I · R\nclé AJ_ID")
    right_arrow(d, pe_aj, ajustement, "AJ_ID")
    down_arrow(d, ajustement, debours, "AJ_ID")
    compte = table(4, TOP_Y, TOP_H, "IND_COMPTE_BANCAIRE\nvolume non mesuré\n\n"
                                    "le compte bancaire\nd'une personne (banque,\nagence, numéro)")
    down_arrow(d, compte, effet, "compte du payé")
    d.box(COLS[5], TOP_Y, COL_W, TOP_H,
          "Toutes ces tables ont aussi\nun lien direct vers le dossier\n(DOS_CODE), sauf PE_AJ,\n"
          "ETAPE et les pièces.", 18, bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ligne du dessous : décisions, calculs, contrôles
    rejet = table(0, BOT_Y, BOT_H, "DOSSIER_REJETE\n486 lignes\n\nune demande refusée\n"
                                   "(date, motif)\npresque jamais saisie")
    down_arrow(d, reception, rejet, "RD_ID")
    notif = table(1, BOT_Y, BOT_H, "NOTIFICATION_PREST\n304 355 lignes\n\nla décision envoyée :\n"
                                   "montant, date d'effet,\ndate de signature")
    down_arrow(d, dossier, notif, "DOS_CODE + TPE_CODE")
    etape = table(2, BOT_Y, BOT_H, "ETAPE\n1,7 million de lignes\n\nles calculs du montant :\n"
                                   "CIP calcul initial,\nRP révision, MAJ majoration")
    down_arrow(d, prestation, etape, "PE_ID")
    table(3, BOT_Y, BOT_H, "Un vrai paiement = un\ndébours PM au statut\nD (payé par un effet)\n"
                           "ou P (payé sans effet)\nMontant : DEB_MONTANT\n"
                           "Mois : DEB_DATE_EFFET\nrègles R3 et R4, à confirmer",
          bg=C["jaune_clair"], stroke=C["jaune_bord"])
    table(4, BOT_Y, BOT_H, "Pièges\nNET_A_PAYER est répété\nsur chacun de ses débours :\n"
                           "additionné, montant × 3,8\n62 % des paiements PF\nde 2025 n'ont pas d'effet",
          bg=C["rouge_clair"], stroke=C["rouge"], color=C["rouge"])
    table(5, BOT_Y, BOT_H, "DOSSIERS_CONTROLES\n1 542 lignes\n\nles contrôles a posteriori\n"
                           "(après paiement), lien\nDOS_CODE · plus saisie\ndepuis juillet 2025")

    # socle : les personnes
    d.box(COLS[0], 1310, 1660, 140,
          "INDIVIDU · 1,54 million de lignes · clé IND_ID — toutes les personnes : assurés, "
          "conjoints, enfants, tuteurs\n(sexe 1 ou 2, date de naissance, date de décès). "
          "Relié par DOSSIER.IND_ID (l'assuré), PRESTATION_ESP.IND_ID_PREST\n(l'ayant droit, "
          "celui qu'on compte) et IND_ID_BENEF (la personne payée), EFFET.IND_ID_BENEF, "
          "AJUSTEMENT.IND_ID", 18, bg=C["gris"], stroke=C["gris_bord"])
    d.box(COLS[4], 1310, COL_W * 2 + COL_GAP, 140,
          "LIEN · 1,31 million de lignes\nles liens de famille entre deux personnes\n"
          "(IND_ID_1, IND_ID_2) : type, mariage, à charge", 18, bg=C["gris"],
          stroke=C["gris_bord"])

    # ------------------------------------------------------------------ 3. listes de codes
    # Listes lues dans la base par le lot 3 (lot3_listes_de_codes.sql, P31 à P47) ; les plus
    # longues sont relues dans analyses/prestations/resultats.
    def code_box(x, y, w, h, title, columns, bg=C["blanc"], stroke=C["gris_bord"],
                 title_color=C["encre"]):
        """Cadre avec un titre et une ou plusieurs colonnes de codes, texte aligné à gauche."""
        g = d.group()
        frame = d.box(x, y, w, h, bg=bg, stroke=stroke, groups=g)
        d.text(x + 18, y + 14, title, 20, title_color, groups=g, inside=frame)
        for col_x, body in columns:
            d.text(x + col_x, y + 50, body, 18, groups=g, inside=frame)

    def open_box(x, y, w, title=None, bg=C["blanc"]):
        """Cadre dont la hauteur est fixée après coup (fit_box), une fois le contenu placé."""
        g = d.group()
        frame = d.box(x, y, w, 10000, bg=bg, stroke=C["gris_bord"], groups=g)
        if title:
            d.text(x + 18, y + 14, title, 20, groups=g, inside=frame)
        return frame, g

    def fit_box(frame, bottom, pad=16):
        frame["height"] = bottom + pad - frame["y"]
        return frame["y"] + frame["height"]

    def flow(frame, g, x, y, col_w, n_cols, blocks, size=18, gap=10):
        """Range des blocs (un titre en couleur, puis ses lignes) en n_cols colonnes de hauteurs
        voisines, sans couper un bloc. Renvoie le bas du contenu."""
        line_h = size * LINE_HEIGHT
        heights = [(1 + len(lines)) * line_h + gap for _, _, lines in blocks]
        cap = sum(heights) / n_cols
        col, cy, bottom = 0, y, y
        for (head, color, lines), h in zip(blocks, heights):
            if cy > y and cy - y + h > cap + line_h and col < n_cols - 1:
                col, cy = col + 1, y
            cx = x + col * col_w
            d.text(cx, cy, head, size, color, groups=g, inside=frame)
            d.text(cx + 24, cy + line_h, "\n".join(lines), size, groups=g, inside=frame)
            cy += h
            bottom = max(bottom, cy - gap)
        return bottom

    sec3 = d.section(0, 1530, 2600, 0, "3. Les listes de codes (nomenclatures)")
    d.text(25, 1590, "Toutes les listes, lues dans la base par le lot 3 (P31 à P47). "
                     "Le nombre après un libellé : les lignes qui portent ce code.", 20, C["doux"])

    code_box(25, 1630, 760, 200, "TYPE_DOSSIER et BRANCHE (TDOS_CODE, BR_CODE)", [(18,
             "V · vieillesse et retraite · branche PE · 75 583\n"
             "A · accidents et maladies professionnelles · RP · 33 178\n"
             "F · prestations familiales · branche PF · 202 737\n"
             "M · prestations de maternité · branche PF · 56 449\n"
             "PE pensions · PF prestations familiales\n"
             "RP risques professionnels")])
    code_box(797, 1630, 700, 200, "CATEGORIE_DOSSIER_PENSION (DOS_CATEGORIE)", [(18,
             "ORDINAIRE · dossier ordinaire · 240 472\n"
             "COOR_CNSS · coordination payée par la CNSS · 3 473\n"
             "COOR_AC · coordination payée par une autre caisse · 3 326\n"
             "CNSS_AC · transféré à une autre caisse · 158\n"
             "HORS_CONV · hors convention · jamais utilisé\n"
             "sans catégorie : 120 518 dossiers")])
    code_box(1509, 1630, 380, 200, "TYPE_DEBOURS (DEB_TYPE)", [(18,
             "AJ · ajustement\nCP · compensation\nPM · paiement\nRB · remboursement")])
    ages = [f"{i + 1} = {a}" for i, a in enumerate(
        [f"{5 * k}-{5 * k + 4}" for k in range(20)] + ["100+ (bornes 100 et 200)"])]
    code_box(1901, 1630, 674, 200, "TRANCHE_AGE · 21 tranches (TAG_CODE)", [(18,
             "\n".join(" · ".join(ages[i:i + 4]) for i in range(0, 21, 4)))])

    d.text(25, 1858, "TYPE_PRESTATION_ESP · les 28 codes de prestation en espèces (TPE_CODE) · "
                     "nombre de prestations ouvertes", 24)
    code_box(25, 1900, 900, 460, "Branche V · PVID · 17 codes", [(18, coded(TPE_V))],
             bg=C["bleu_clair"], stroke=C["bleu"])
    code_box(945, 1900, 640, 230, "Branche A · AT/MP · 7 codes", [(18, coded(TPE_A))],
             bg=C["orange_clair"], stroke=C["orange"])
    code_box(1605, 1900, 475, 130, "Branche F · PF · 2 codes", [(18, coded(TPE_F))],
             bg=C["vert_clair"], stroke=C["vert"])
    code_box(2100, 1900, 475, 130, "Branche M · maternité (PF) · 2 codes", [(18, coded(TPE_M))],
             bg=C["vert_clair"], stroke=C["vert"])
    d.box(945, 2150, 640, 210,
          "Accessoires de pension :\nRBEC, RME, RME-COL, RCCM, RRC.\nVersés en plus d'une pension,\n"
          "ils ne font pas un pensionné\nde plus (à confirmer).", 20, bg=C["jaune_clair"],
          stroke=C["jaune_bord"])
    d.box(1605, 2050, 970, 310,
          "Paiement mensuel, sauf FAF et FAP (trimestriel).\n\n"
          "Deux codes d'allocation prénatale :\nFAP (branche F) et MAP (branche M).\n\n"
          "Aucun code IJ ni RV : DTM_ACCIDENT_TRAVAIL\nles cherche à tort (AIJ et ARI).", 20,
          bg=C["jaune_clair"], stroke=C["jaune_bord"])

    blocks = []
    for cat, rows in groupby(read_tsv("lot3_P35_prestations_nature.tsv"), lambda r: r["CPN_CODE"]):
        rows = list(rows)
        color = C["vert"] if rows[0]["TDOS_CATEGORIE"] == "M" else C["orange"]
        lines = [f"{r['TPN_CODE']} · {lower1(r['TPN_DESCRIPTION'])}"
                 + ("" if r["CODE_BRANCHE"] else " · sans branche") for r in rows]
        blocks.append((f"{cat} · {lower1(rows[0]['CPN_DESCRIPTION'])}", color, lines))
    frame, g = open_box(25, 2390, 2550,
                        "TYPE_PRESTATION_NAT · 51 prestations en nature (TPN_CODE), dans 24 "
                        "catégories (CPN_CODE) · orange : AT/MP · vert : maternité, "
                        "rien de payé en 2025")
    y = fit_box(frame, flow(frame, g, 43, 2440, 845, 3, blocks)) + 20

    code_box(25, y, 836, 360, "TYPE_ETAPE · 13 étapes de calcul (TEA_CODE)", [(18, coded(ETAPES))])
    code_box(882, y, 836, 360, "TYPE_AJUSTEMENT · 13 types (TAJ_CODE)", [(18, coded(AJUSTEMENTS))])
    code_box(1739, y, 836, 360, "TYPE_EFFET · 11 titres de paiement (TEP_CODE)",
             [(18, coded(EFFETS))])
    y += 380

    code_box(25, y, 836, 225, "TYPE_MODE_PAIEMENT · 7 modes (TMP_CODE)", [(18,
             "C · chèque\nE · espèces\nM · mandat postal\nP · paiement aux employeurs conventionnés\n"
             "S · paiement aux structures sanitaires et sociales\nT · paiement mobile\n"
             "V · virement bancaire")])
    code_box(882, y, 836, 225, "TYPE_FREQUENCE_PAIEMENT (TFP_CODE)", [(18,
             "A · annuel\nBA · bi-annuel\nM · mensuel : toutes les prestations, sauf FAF et FAP\n"
             "T · trimestriel : FAF et FAP")])
    code_box(1739, y, 836, 225, "TYPE_ACCIDENT (TAT_CODE)", [(18,
             "AD · accident de déplacement\nALOT · accident au lieu occasionnel de travail\n"
             "ALT · accident au lieu de travail\nATR · accident de trajet\n"
             "MPR · maladie professionnelle")])
    y += 245

    pieces = [f"{p['TPJ_CODE']} · {piece_label(p['TPJ_DESCRIPTION'])}"
              + (" · inactif" if p["TPJ_ACTIF"] == "N" else "")
              for p in read_tsv("lot3_P44_types_piece.tsv")]
    per_col = ceil(len(pieces) / 3)
    piece_h = ceil(50 + per_col * 18 * LINE_HEIGHT + 18)
    code_box(25, y, 2550, piece_h,
             f"TYPE_PIECE · {len(pieces)} pièces demandées à la réception d'un dossier (TPJ_CODE)",
             [(18 + i * 845, "\n".join(pieces[i * per_col:(i + 1) * per_col])) for i in range(3)])
    y += piece_h + 20

    code_box(25, y, 1560, 230,
             "Statuts : aucune liste dans la base, sens lu dans les données (lots 1 et 2), "
             "à confirmer par la DSI", [(18,
             "DOS_STATUT · A actif, F fermé\n"
             "PE_STATUT · A active, S suspendue, F fermée (radiée)\n"
             "DEB_STATUT · D payé par un effet, P payé sans effet, V validé jamais décaissé, "
             "N, A, S non approuvés, I inconnu\n"
             "EFP_STATUT · A, D décaissé, I, N\n"
             "AJ_STATUT · A, C, I, R (sens inconnu)\n"
             "RD_STATUT · E, I, L, R, A (sens inconnu)\n"
             "NOT_STATUT · A, I (sens inconnu)")],
             bg=C["rouge_clair"], stroke=C["rouge"], title_color=C["rouge"])
    code_box(1605, y, 970, 230, "Ce que les listes ont appris (lot 3)", [(18,
             "RPRA : retraite anticipée ; RBEC, RME… : accessoires\n"
             "120 518 dossiers sans catégorie\n"
             "frais de maternité prévus, aucun payé en 2025\n"
             "aucun code IJ ni RV (DTM_ACCIDENT_TRAVAIL)\n"
             "virement et espèces : 98,1 % des effets\n"
             "jamais utilisés : HORS_CONV, FS, OE, RPC, RRC")],
             bg=C["jaune_clair"], stroke=C["jaune_bord"])
    sec3["height"] = y + 230 + 30 - sec3["y"]

    # ------------------------------------------------------------------ 4. géographie
    yg = sec3["y"] + sec3["height"] + 50
    sec4 = d.section(0, yg, 2600, 0, "4. La géographie : directions, services, lieux de paiement")
    d.text(25, yg + 62, "Une direction régionale (DR_NO) regroupe des services provinciaux "
                        "(SP_NO), qui regroupent des lieux de paiement (LP_NO) : 6 directions, "
                        "20 services, 350 lieux (P45 à P47).", 20, C["doux"])
    services = read_tsv("lot3_P46_services_provinciaux.tsv")
    lieux = read_tsv("lot3_P47_lieux_paiement.tsv")
    col_w, y = 494, yg + 110
    bottom = y
    for i, (dr, sigle, ville) in enumerate(DIRECTIONS):
        x = 25 + i * (col_w + 20)
        sps = [s for s in services if s["DR_NO"] == dr]
        lieux_dr = [lp for lp in lieux if lp["DR_NO"] == dr]
        d.box(x, y, col_w, 90, f"{dr} · {sigle} · {ville}\n{len(sps)} services · "
                               f"{len(lieux_dr)} lieux de paiement", 20, bg=C["violet"])
        frame, g = open_box(x, y + 100, col_w)
        cy = y + 114
        for s in sps:
            ls = [lp for lp in lieux_dr if lp["SP_NO"] == s["SP_NO"]]
            head = (f"{s['SP_NO']} · {s['SP_SIGLE']} · {plural(len(ls), 'lieu')}"
                    + (" · inactif" if s["SP_ACTIVE"] == "N" else ""))
            d.text(x + 15, cy, head, 18, groups=g, inside=frame)
            body = wrap([f"{lp['LP_NO']} {place(lp['LP_DESC'])}" for lp in ls], 16, col_w - 45)
            d.text(x + 30, cy + 24, body, 16, C["doux"], groups=g, inside=frame)
            cy += 24 + (body.count("\n") + 1) * 16 * LINE_HEIGHT + 12
        for lp in (lp for lp in lieux_dr if not lp["SP_NO"]):
            d.text(x + 15, cy, f"sans service : {lp['LP_NO']} {place(lp['LP_DESC'])}", 18,
                   C["rouge"], groups=g, inside=frame)
            cy += 18 * LINE_HEIGHT + 12
        bottom = max(bottom, fit_box(frame, cy - 12))
    y = bottom + 20
    d.box(25, y, 760, 150, "0 · SIEGE · siège social, Ouagadougou\ndirection inactive\n"
                           "un seul lieu : 1110 Siege,\nrattaché au service 1090 (DRO)", 20,
          bg=C["gris"], stroke=C["gris_bord"])
    code_box(805, y, 1770, 150, "À savoir sur les lieux de paiement", [(18,
             "DR_OU_SP vaut O pour 20 lieux, un par service : la ville où siège le service ; "
             "N pour 259 ; vide pour 71 (sens à demander)\n"
             "Dans chaque direction : Pays d'Europe, Pays d'Afrique et CNSS-DCP ; "
             "à la DRO, 5 républiques voisines (9001 à 9005)\n"
             "201 Caisse Tenkodogo : rattaché à la DRB, sans service, alors que Tenkodogo "
             "dépend de la DRF ; 1110 Siege : direction 0, service 1090\n"
             "Services inactifs : Tanghin, Sakaby, Diapaga, Tougan ; Diapaga (8 lieux) et "
             "Tougan (11) gardent leurs lieux")],
             bg=C["jaune_clair"], stroke=C["jaune_bord"])
    sec4["height"] = y + 150 + 30 - sec4["y"]

    # ------------------------------------------------------------------ 5. DTM
    y4 = sec4["y"] + sec4["height"] + 50
    d.section(0, y4, 2600, 760, "5. Les tables DTM : ce qu'elles lisent, à quoi elles servent")
    d.box(25, y4 + 65, 2550, 70,
          "Dans le DWH, chaque table source devient DWH.FAIT_<nom> (FAIT_DOSSIER, FAIT_DEBOURS…), "
          "rechargée en entier avec la colonne CLICHE.\nFAIT_DEBOURS, trop lourde, tourne dans "
          "un job à part. Les DTM lisent ces faits, plus DIM_TRANCHE_AGE et DIM_TYPE_PRESTATION.",
          18, bg=C["gris"], stroke=C["gris_bord"])
    dtm = [
        ("DTM_DOSSIER\n424 088 lignes",
         "lit FAIT_RECEPTION_DOSSIER,\nFAIT_DOSSIER,\nFAIT_DOSSIER_REJETE,\n"
         "FAIT_PRESTATION_ESP,\nFAIT_DEBOURS, FAIT_INDIVIDU",
         "T25, T26, T27\ndurée de liquidation",
         "ne garde que les dossiers payés,\nlit F comme « liquidé »\n"
         "(nouvelle requête proposée)"),
        ("DTM_CONTROLE_PRESTATION\n1 036 lignes",
         "lit FAIT_DOSSIERS_CONTROLES,\nFAIT_DOSSIER,\nFAIT_PRESTATION_ESP",
         "T28, T29, T30\ncontrôles a posteriori",
         "source plus saisie depuis 07/2025\n(0,07 % des dossiers payés\ncontrôlés en 2025)"),
        ("DTM_PRESTATION_INDUE\n146 620 lignes",
         "lit FAIT_AJUSTEMENT,\nFAIT_DOSSIER,\nFAIT_INDIVIDU",
         "T31\ntrop perçus, moins perçus",
         "compte REM-TP comme un indu,\nexclut la branche M"),
        ("DTM_PRESTATION\n4 356 764 lignes",
         "lit FAIT_DEBOURS, FAIT_EFFET,\nFAIT_PRESTATION_ESP,\nFAIT_DOSSIER, FAIT_INDIVIDU",
         "T32 à T57\nbénéficiaires et montants",
         "montants × 3,8 (net de l'effet),\ncompte la personne payée\n(le tuteur, pas l'orphelin)"),
        ("DTM_ACCIDENT_TRAVAIL\n34 172 lignes",
         "lit FAIT_TSINISTRE,\nFAIT_PRESTATION_ESP,\nFAIT_DEBOURS",
         "statistiques AT/MP\n(hors T25 à T57)",
         "cherche les codes IJ et RV,\nabsents des 28 codes (AIJ, ARI…) ;\nTSINISTRE arrêtée fin 2024"),
        ("DTM_INDICATEURS_PRESTATIONS\n416 lignes",
         "lit DTM_DOSSIER,\nDTM_PRESTATION_INDUE,\nDTM_PRESTATION, FAIT_DOSSIER,\nFAIT_AJUSTEMENT",
         "indicateurs CIPRES\npar année et par branche",
         "hérite des erreurs des\n3 tables DTM qu'il lit"),
    ]
    for i, (head, reads, serves, issue) in enumerate(dtm):
        x = 25 + i * 428
        g = d.group()
        d.box(x, y4 + 165, 410, 80, head, 20, bg=C["gris"], stroke=C["encre"], stroke_width=3,
              groups=g)
        body = d.box(x, y4 + 245, 410, 170, bg=C["blanc"], stroke=C["encre"], groups=g)
        d.text(x + 15, y4 + 260, reads, 18, groups=g, inside=body)
        d.box(x, y4 + 425, 410, 120, "sert à\n" + serves, 20, bg=C["rouge_moyen"],
              stroke=C["rouge"], groups=g)
        d.box(x, y4 + 555, 410, 115, issue, 18, bg=C["rouge_clair"], stroke=C["rouge"],
              color=C["rouge"], groups=g)
    d.text(25, y4 + 695, "Volumes des tables DTM d'après les statistiques Oracle (P18) ; "
                         "leur contenu n'est pas analysé.", 18, C["doux"])

    return d


if __name__ == "__main__":
    main(build, OUTPUT)
