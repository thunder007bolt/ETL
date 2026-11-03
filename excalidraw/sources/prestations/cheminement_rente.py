"""
Schéma « Le cheminement d'une rente AT/MP dans la base » (état au 03/11/2026).

Suit une rente d'accident du travail de la déclaration au paiement, puis aux rentes de
survivants : ce que chaque étape écrit, dans quelle table, par quelle clé.

Sources : analyses/prestations, lot 4 (P60 réceptions et pièces, P61 prestations par code, P62
durée de liquidation), lot 7 (P87 taux d'incapacité), P20, P40 et P41 (débours, révisions,
ajustements) ; règles R3 à R7 ; t32_t41 et t42_t49_requete_proposee.sql pour les codes des
tableaux ; schémas tables_prestations et vie_dossier.

Usage :
    python3 excalidraw/sources/prestations/cheminement_rente.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, LINE_HEIGHT, Diagram, main  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "vie_dossier" / "cheminement_rente.excalidraw"

# Partie 2 : une bande par étape ; colonnes du cadre (décalage, largeur)
STEP_W = 300
FRAME_X, FRAME_W = 345, 2230
CELLS = [(18, 470), (508, 650), (1178, 470), (1668, 544)]
HEADERS = ["Ce qui se passe", "Tables et colonnes", "Chiffres", "À savoir"]

STEPS = [
    ("0. L'ACCIDENT\ndéclaré, le\ndossier ouvert", [
        "L'employeur déclare l'accident au\nguichet. La CNSS ouvre le dossier\n"
        "de la victime. Ce dépôt suffit pour\npayer les IJ et rembourser les\nsoins.",
        "RECEPTION_DOSSIER · clé RD_ID\nRD_TYPE_RECEPTION = NOUVEAU\n"
        "TDOS_CODE = 'A' · RD_DATE_RECEPTION\nRECEPTION_DOS_PIECE : la déclaration\n"
        "DOSSIER · clé DOS_CODE · lien RD_ID\nIND_ID : la victime\n"
        "TSINISTRE · lien DOS_CODE : l'accident\nPRESTATION_ESP : AIJ, les IJ",
        "Réceptions NOUVEAU de 2019 à\n2025 : 9 496, dont 9 334\ndéclarations (1 313 en 2025, P60)\n"
        "IJ : 13 201 sur 6 963 dossiers,\n1,9 IJ par dossier (P61)",
        "Aucune réception pour une IJ :\nelle est demandée avec la\ndéclaration (P60).\n"
        "TSINISTRE n'est plus alimentée\ndepuis 12/2024.",
    ]),
    ("1. DEMANDE\nde la rente,\nau guichet", [
        "L'état de la victime est stabilisé\n(la consolidation). Elle revient au\n"
        "guichet avec son certificat de\nconsolidation. Pas de nouveau\n"
        "dossier : la demande vise celui\nde l'accident.",
        "RECEPTION_DOSSIER\nRD_TYPE_RECEPTION = ANCIEN\nDOS_CODE = le dossier de l'accident\n"
        "RD_DATE_RECEPTION : la date du dépôt\nRECEPTION_DOS_PIECE : le certificat\n"
        "de consolidation\nDOSSIER.DOS_DATE_REC_CM_RENTE :\nle certificat médical pour la rente\n"
        "DOSSIER.DOS_DATE_REC_AVIS_MC :\nl'avis du médecin conseil",
        "Réceptions ANCIEN de 2019 à\n2025 : 1 097 (136 en 2025),\ndont 233 avec un certificat de\n"
        "consolidation (P60)\nCertificat pour la rente rempli :\n65 % des rentes, 85 % des\n"
        "allocations (P62)",
        "Une rente sans réception au\nguichet : on prend la date du\ncertificat pour la rente, sinon\n"
        "celle de la déclaration.",
    ]),
    ("2. CALCUL\ndu droit", [
        "La CNSS fixe le taux d'incapacité\net crée une ligne pour la rente,\n"
        "sur le dossier de l'accident.\nEn dessous de 15 %, ce n'est pas\n"
        "une rente mais une allocation\nd'incapacité, payée une seule fois.",
        "PRESTATION_ESP · clé PE_ID · lien DOS_CODE\nTPE_CODE = 'ARI' : la rente viagère\n"
        "ou 'AAI' : l'allocation d'incapacité\nPE_TAUX_INCAPACITE : de 15 à 100\n"
        "IND_ID_PREST : l'ayant droit, compté\nIND_ID_BENEF : la personne payée\n"
        "PE_DATE_EFFET : le début du droit\nPE_DATE_INSERT : la saisie (R7)\n"
        "ETAPE · lien PE_ID : CIP, calcul initial",
        "ARI : 2 349 rentes viagères\nsur 1 970 dossiers\nAAI : 1 233 allocations\n"
        "sur 1 233 dossiers (P61)\nTaux médian des nouvelles\nrentes de 2024 : 24 (P87)",
        "PE_DATE_DEMANDE précède la\nnotification de 2 ans en\nmédiane : elle ne vaut pas\n"
        "date de dépôt (P61).",
    ]),
    ("3. NOTIFICATION\nla rente est\nliquidée", [
        "La décision est signée et envoyée\nà la victime : la rente est\nliquidée.",
        "PRESTATION_ESP.PE_DATE_NOTIFICATION\nNOTIFICATION_PREST · lien DOS_CODE\n"
        "et TPE_CODE : la décision imprimée",
        "ARI : 1 853 avec une date de\nnotification, 1 244 décisions\n"
        "imprimées, 96 notifiées en\n2025 (P61)",
        "Durée de liquidation d'une\nrente en 2025 : 14 j en médiane\ndepuis la demande au guichet,\n"
        "64 j depuis le certificat pour\nla rente (P62).",
    ]),
    ("4. PAIEMENT\nchaque mois", [
        "Chaque mois, une somme due est\npréparée pour la rente, puis\n"
        "réunie dans un titre de paiement\n(l'effet) et dans un lot remis à\nla banque.",
        "DEBOURS · lien PE_ID · DEB_TYPE = 'PM'\nDEB_DATE_EFFET : le mois payé\n"
        "DEB_MONTANT : la somme\nDEB_STATUT D ou P : vraiment versé (R3)\n"
        "EFFET · clé EFP_ID : le titre de paiement\nBORDEREAU_PREST · clé BO_ID : le lot",
        "Les mêmes tables que pour\nles pensions (P20, P21)",
        "NET_A_PAYER est répété sur\nchaque débours de l'effet : ne\njamais l'additionner. Le montant\n"
        "payé est la somme de\nDEB_MONTANT.",
    ]),
    ("5. VIE\nde la rente,\njusqu'à sa fin", [
        "La rente peut être suspendue,\nreprise, révisée ou majorée, puis\n"
        "elle prend fin (la radiation).",
        "PRESTATION_ESP.PE_STATUT : A active,\nS suspendue, F fermée\n"
        "PE_DATE_SUSPENSION, PE_DATE_REACTIVATION,\nPE_DATE_RADIATION\n"
        "ETAPE : RP révision, MAJ majoration\nAJUSTEMENT, par PE_AJ : trop-perçu,\nrappel",
        "Toutes branches : 588 919\nrévisions, 167 810 majorations,\n1,36 million d'ajustements\n"
        "(P40, P41)",
        "Rentier au 31/12 : rente active\net payée au 4e trimestre (R6).",
    ]),
    ("6. DÉCÈS\nde la victime :\nles rentes de\nsurvivants", [
        "Si la victime meurt de l'accident,\nsa famille dépose le certificat de\n"
        "décès. Chaque survivant reçoit sa\nrente, sur le dossier de\nl'accident.",
        "RECEPTION_DOSSIER : ANCIEN, même\nDOS_CODE ; pièce : le certificat de décès\n"
        "PRESTATION_ESP : une ligne par survivant\nTPE_CODE : ARSV veuve, ARSF veuf,\n"
        "ARSE orphelin, ARSA ascendant\nIND_ID_PREST : le survivant\n"
        "IND_ID_BENEF : la personne payée\nLIEN (IND_ID_1, IND_ID_2) : le lien\n"
        "de famille avec la victime",
        "Réceptions ANCIEN avec un\ncertificat de décès : 53 (P60)\n"
        "ARSE 1 121 · ARSV 388\nARSA 350 · ARSF 4 (P61)",
        "Un orphelin est compté\n(IND_ID_PREST) mais peut être\npayé à sa mère ou à son\n"
        "tuteur (IND_ID_BENEF) : on\ncompte l'ayant droit (R5).",
    ]),
]


def build():
    d = Diagram(seed=41)
    line_h = 18 * LINE_HEIGHT

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "Le cheminement d'une rente AT/MP dans la base", 48)
    d.text(0, 66, "De la déclaration de l'accident au paiement, puis aux rentes de survivants : "
                  "ce que chaque étape écrit, et dans quelle table · état au 03/11/2026", 22,
           C["doux"])

    # ------------------------------------------------------------------ 1. parcours
    d.section(0, 120, 2600, 540, "1. Le parcours en 7 étapes")
    timeline = [
        "0. L'ACCIDENT\ndéclaré, le dossier\nouvert\n\nRECEPTION_DOSSIER\nDOSSIER",
        "1. DEMANDE\nde la rente,\nau guichet\n\nRECEPTION_DOSSIER\nANCIEN",
        "2. CALCUL\ndu droit\n\n\nPRESTATION_ESP\nARI, ETAPE",
        "3. NOTIFICATION\nla rente est\nliquidée\n\nNOTIFICATION_PREST\nPE_DATE_NOTIFICATION",
        "4. PAIEMENT\nchaque mois\n\n\nDEBOURS, EFFET\nBORDEREAU_PREST",
        "5. VIE DE LA RENTE\nsuspension, révision,\nfin\n\nPRESTATION_ESP\nETAPE, AJUSTEMENT",
        "6. DÉCÈS\nles rentes de\nsurvivants\n\nPRESTATION_ESP\nARSV, ARSF, ARSE, ARSA",
    ]
    boxes, prev = [], None
    for i, txt in enumerate(timeline):
        box = d.box(25 + i * 370, 330, 330, 180, txt, 18, bg=C["blanc"], stroke=C["encre"],
                    stroke_width=3)
        if prev:
            d.harrow(prev, box, stroke_width=3)
        boxes.append(box)
        prev = box

    def span(first, last, txt, **kw):
        """Bandeau au-dessus des étapes first à last (incluses)."""
        a, b = boxes[first], boxes[last]
        return d.box(a["x"], 195, b["x"] + b["width"] - a["x"], 110, txt, 18, **kw)

    span(0, 0, "Avant la rente :\nles IJ et les soins", bg=C["gris"], stroke=C["gris_bord"])
    span(1, 5, "La rente : sur le dossier de l'accident, sans en ouvrir un nouveau\n"
               "durée de liquidation : du dépôt au guichet à la notification, 14 j en médiane "
               "en 2025 (P62)", bg=C["bleu_clair"], stroke=C["bleu"], color=C["bleu"])
    span(6, 6, "Si la victime meurt\nde l'accident : sur le\nmême dossier", bg=C["gris"],
         stroke=C["gris_bord"], style="dashed")
    d.box(25, 540, 2550, 90,
          "Le fil qui relie tout est le dossier de l'accident (DOS_CODE) : la demande de rente, "
          "la rente, ses paiements et les rentes de survivants s'y rattachent.\n"
          "Chaque étape écrit une ligne dans sa table, reliée à la précédente par une clé : "
          "RD_ID, DOS_CODE, PE_ID, EFP_ID, BO_ID (partie 3).", 20,
          bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 2. étape par étape
    sec2 = d.section(0, 710, 2600, 0, "2. Étape par étape : ce que la base enregistre")
    for (dx, _), head in zip(CELLS, HEADERS):
        d.text(FRAME_X + dx, 770, head, 22, C["doux"])
    y = 815
    for title, cells in STEPS:
        lines = max(cell.count("\n") + 1 for cell in cells)
        h = lines * line_h + 30
        g = d.group()
        d.box(25, y, STEP_W, h, title, 20, bg=C["gris"], stroke=C["encre"], stroke_width=2,
              groups=g)
        frame = d.box(FRAME_X, y, FRAME_W, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
        for (dx, width), cell in zip(CELLS, cells):
            color = C["rouge"] if cell is cells[3] else C["encre"]
            t = d.text(FRAME_X + dx, y + 15, cell, 18, color, groups=g, inside=frame)
            if t["width"] > width - 16:
                d.warnings.append(f"cellule trop large : {cell.splitlines()[0]!r} "
                                  f"({t['width']:.0f} pour {width})")
        y += h + 14
    sec2["height"] = y + 16 - sec2["y"]

    # ------------------------------------------------------------------ 3. les liens
    y3 = sec2["y"] + sec2["height"] + 50
    d.section(0, y3, 2600, 560, "3. Les clés pour suivre une rente d'un bout à l'autre")
    chain = [
        ("RECEPTION_DOSSIER\nle dépôt", None),
        ("DOSSIER\nle dossier de l'accident", "DOS_CODE"),
        ("PRESTATION_ESP\nla rente (ARI, ARS…)", "DOS_CODE"),
        ("DEBOURS\nla somme d'un mois", "PE_ID"),
        ("EFFET\nle titre de paiement", "EFP_ID"),
        ("BORDEREAU_PREST\nle lot remis à la banque", "BO_ID"),
    ]
    tables, prev = [], None
    for i, (txt, key) in enumerate(chain):
        box = d.box(25 + i * 430, y3 + 110, 300, 110, txt, 20, bg=C["bleu_clair"],
                    stroke=C["bleu"])
        if prev:
            d.harrow(prev, box, color=C["bleu"], stroke_width=2)
            d.text(prev["x"] + prev["width"] + 65, y3 + 132, key, 18, C["bleu"], align="center")
        tables.append(box)
        prev = box
    individu = d.box(455, y3 + 330, 760, 110,
                     "INDIVIDU · les personnes\nDOSSIER.IND_ID : la victime · PRESTATION_ESP."
                     "IND_ID_PREST :\nl'ayant droit · PRESTATION_ESP.IND_ID_BENEF : la personne "
                     "payée", 18, bg=C["gris"], stroke=C["gris_bord"])
    for table in tables[1:3]:
        x = table["x"] + table["width"] / 2
        d.arrow(x, table["y"] + table["height"] + 6, x, individu["y"] - 6, table, individu,
                color=C["gris_bord"])
    lien = d.box(1345, y3 + 330, 560, 110,
                 "LIEN · les liens de famille\nIND_ID_1, IND_ID_2 : la victime et\nle survivant "
                 "(veuve, orphelin…)", 18, bg=C["gris"], stroke=C["gris_bord"])
    d.harrow(individu, lien, color=C["gris_bord"])
    d.text(25, y3 + 480, "Pour une rente de survivant, IND_ID_PREST est le survivant et "
                         "DOSSIER.IND_ID reste la victime : LIEN dit qui il est pour elle.",
           20, C["doux"])

    # ------------------------------------------------------------------ 4. exemple
    y4 = y3 + 610
    d.section(0, y4, 2600, 440, "4. Un exemple suivi de bout en bout (personne fictive)")
    d.text(25, y4 + 62, "M. B, ouvrier, se blesse à la main au travail. Les délais sont les "
                        "médianes de 2025 pour les rentes (P62).", 20, C["doux"])
    example = [
        "05/03/2023 · accident\n\nl'employeur le déclare\n(NOUVEAU) ; un dossier\nA est ouvert "
        "pour M. B\n\nRECEPTION_DOSSIER\nDOSSIER",
        "Mars 2023 et après\n· l'arrêt\n\nM. B touche des IJ\n(AIJ), ses soins sont\nremboursés\n\n"
        "PRESTATION_ESP, DEBOURS",
        "10/06/2025 · demande\n\nconsolidé, M. B dépose\nson certificat (ANCIEN)\nsur le même "
        "dossier\n\nRECEPTION_DOSSIER",
        "24/06/2025 · décision\n\ntaux de 30 % : une rente\nviagère ARI, notifiée\n14 jours "
        "après le dépôt\n\nPRESTATION_ESP\nNOTIFICATION_PREST",
        "Ensuite, chaque mois\n\nun débours PM, payé\n(statut D), dans un\neffet et un lot remis\n"
        "à la banque\n\nDEBOURS, EFFET",
        "S'il meurt de l'accident\n\nsa veuve dépose sur le\nmême dossier : une rente\nARSV ; "
        "ses enfants :\ndes rentes ARSE\n\nPRESTATION_ESP, LIEN",
    ]
    prev = None
    for i, txt in enumerate(example):
        box = d.box(25 + i * 431, y4 + 110, 395, 290, txt, 18, bg=C["bleu_clair"],
                    stroke=C["bleu"])
        if prev:
            d.harrow(prev, box, color=C["bleu"], stroke_width=2)
        prev = box

    # ------------------------------------------------------------------ 5. tableaux CIPRES
    y5 = y4 + 490
    d.section(0, y5, 2600, 360, "5. Où la rente compte dans les tableaux CIPRES")
    cards = [
        "T26 · dossiers AT/MP\n\ndéposé : la réception\nANCIEN de la demande ;\nliquidé : la 1re "
        "notification\nde la rente ; durée : du\ndépôt à la notification",
        "T37 à T40 · crédirentiers\nau 31/12\n\nT37 rente viagère, ARI\nT38 veufs et veuves, "
        "ARSV, ARSF\nT39 orphelins, ARSE\nT40 ascendants, ARSA\nactive et payée au 4e trimestre",
        "T45 à T49 · nouveaux\ncrédirentiers\n\nrente saisie dans l'année\n(PE_DATE_INSERT, R7)\n"
        "T45 toutes · T46 ARSE\nT47 ARSA · T48 ARI\nT49 ARSV, ARSF",
        "T48 · degré moyen d'IPP\n\nla moyenne de\nPE_TAUX_INCAPACITE\ndes nouvelles rentes\n"
        "viagères (de 15 à 100,\nmédiane 24 en 2024)",
        "T53, T56, T57 · montants\n\nT53 : les allocations\nd'incapacité (AAI)\nT56, T57 : la branche "
        "AT/MP\ndébours PM au statut D ou P\n(R3), au mois payé (R4)",
    ]
    for i, txt in enumerate(cards):
        d.box(25 + i * 514, y5 + 70, 494, 260, txt, 18, bg=C["rouge_moyen"], stroke=C["rouge"])

    return d


if __name__ == "__main__":
    main(build, OUTPUT)
