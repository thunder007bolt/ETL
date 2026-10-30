"""
Schéma « La vie d'un dossier, de la demande au paiement » (état au 30/10/2026).

Développe la partie 2 du schéma tables_prestations : ce que chaque étape écrit dans la base.

Sources : analyses/prestations (P01 volumes, P02 statuts des dossiers, P06 statuts, P09 et P10
dossiers et délais de 2025, P13 contrôles, P20 à P23 débours et effets, P24 et P25 statut des
prestations, P27 et P28 notification et premier paiement, P37, P40, P41 listes du lot 3 ; lot 3 B,
P48 à P58 : nouvelles demandes et réversions, stock en instance, refus ; règles R3 à R9 et D1 à
D8 ; t25_requete_proposee.sql pour les dates du dossier).

Usage :
    python3 excalidraw/sources/prestations/vie_dossier.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, LINE_HEIGHT, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "vie_dossier" / "vie_dossier.excalidraw"

# Partie 2 : une bande par étape ; colonnes du cadre (décalage, largeur)
STEP_W = 300
FRAME_X, FRAME_W = 345, 2230
CELLS = [(18, 450), (488, 640), (1148, 470), (1638, 574)]
HEADERS = ["Ce qui se passe", "Tables et colonnes", "Chiffres", "À savoir"]

STEPS = [
    ("1. DÉPÔT\nde la demande", [
        "La personne dépose sa demande au\nguichet, avec les pièces demandées :\n"
        "une nouvelle demande (NOUVEAU), ou\nune demande sur un dossier qui existe\n"
        "déjà (ANCIEN), surtout la réversion.",
        "RECEPTION_DOSSIER · clé RD_ID\nRD_DATE_RECEPTION : date du dépôt\n"
        "RD_TYPE_RECEPTION : NOUVEAU ou ANCIEN\nDOS_CODE : le dossier visé (ANCIEN)\n"
        "TDOS_CODE : la branche demandée\nRD_STATUT : E presque toujours\n"
        "RECEPTION_DOS_PIECE : les pièces fournies\n(86 types de pièce, TYPE_PIECE)",
        "273 005 réceptions en tout\n2025 : V 4 671 · A 1 481\nF 9 968 · M 4 338 (P09)\n"
        "Pensions 2025 : 4 055 NOUVEAU,\n616 ANCIEN (P52)",
        "Une réception = un dépôt (D3). 96 % des\npièces sont saisies le jour même : la\n"
        "date de réception est celle du dossier\ncomplet (P49). 94 % des demandes ANCIEN\n"
        "sont suivies d'une réversion (P58).",
    ]),
    ("2. OUVERTURE\ndu dossier,\nou rejet", [
        "La CNSS ouvre un dossier pour l'assuré\ndans une branche, ou rejette la\n"
        "demande. Une demande ANCIEN n'ouvre\npas de dossier : elle complète celui\n"
        "qui existe.",
        "DOSSIER · clé DOS_CODE · lien RD_ID\nDOS_DATE_OUVERTURE · TDOS_CODE V, A, F, M\n"
        "IND_ID : l'assuré · DOS_CATEGORIE\nDOS_STATUT : A actif, F fermé\n"
        "DOSSIER_REJETE : DOSRE_DATE_REJET, motif\nTSINISTRE : l'accident (AT/MP)",
        "367 947 dossiers · 486 rejets en tout\n2025, ouverts : V 3 638 · A 1 237\n"
        "F 9 205 · M 4 238 ; rejetés : 36 (P09)\nune demande ouvre au plus un\n"
        "dossier, sauf 11 (P52)",
        "A veut dire actif, pas « en instance » :\n61 632 dossiers de vieillesse sont\n"
        "actifs (P02). Les refus ne sont notés que\ndans DOSSIER_REJETE : une quinzaine\n"
        "par an en pensions (P56). TSINISTRE\nn'est plus alimentée depuis fin 2024.",
    ]),
    ("3. CALCUL\ndes droits", [
        "Pour chaque droit, la CNSS crée une\nprestation et calcule son montant.\n"
        "Un dossier peut ouvrir plusieurs droits\n(par exemple 3 allocations familiales,\n"
        "ou une pension puis sa réversion).",
        "PRESTATION_ESP · clé PE_ID · lien DOS_CODE\nTPE_CODE : le droit (28 codes)\n"
        "PE_DATE_DEMANDE, PE_DATE_EFFET (début)\nIND_ID_PREST : l'ayant droit\n"
        "IND_ID_BENEF : la personne payée\nETAPE · lien PE_ID : CIP, calcul initial",
        "947 953 prestations\n947 441 calculs initiaux (CIP)\n512 prestations sans calcul\n"
        "initial (P40)",
        "La date d'effet remonte souvent avant la\ndécision : un tiers des pensions de\n"
        "vieillesse notifiées en 2025 ont un effet\nen 2024 ou avant (P28).",
    ]),
    ("4. NOTIFICATION\nde la décision :\nle dossier est\nliquidé", [
        "La décision est signée et envoyée à la\npersonne : le dossier est liquidé.",
        "NOTIFICATION_PREST · lien DOS_CODE + TPE_CODE\nmontant, date d'effet, date de signature\n"
        "NOT_STATUT : A, I (sens inconnu)\nPRESTATION_ESP.PE_DATE_NOTIFICATION",
        "304 355 notifications\nDu dépôt à la notification, médiane\n2025 : V 62 j · A 156 j · F 13,5 j\n"
        "M 17 j (P10)",
        "Liquidation = 1re notification des droits\nde chaque demande (D4) : une réversion\n"
        "compte à part. Norme CIPRES : 45 jours\nau plus, dépassée pour 61 % des dossiers\n"
        "V et 98 % des AT/MP. La date de\nnotification du dossier est vide (P54).",
    ]),
    ("5. DÉBOURS\nla somme due\npour chaque mois", [
        "Pour chaque mois (chaque trimestre\npour FAF et FAP), une somme due est\n"
        "préparée pour le droit.",
        "DEBOURS · clé DEB_ID · liens PE_ID, EFP_ID, AJ_ID\n"
        "DEB_TYPE : PM paiement, RB remboursement,\nAJ ajustement, CP compensation\n"
        "DEB_DATE_EFFET : le mois payé · DEB_MONTANT\nDEB_DATE_INSERT, DEB_DATE_APPROBATION\n"
        "DEB_STATUT : D, P, V, I, N, A, S",
        "71,7 millions de débours\nPaiements de 2025 : D 53,72 Md,\nP 7,63 Md ; V 0,64 Md jamais\n"
        "décaissés (P20)",
        "Un vrai paiement : PM au statut D ou P,\nmontant DEB_MONTANT (R3), au mois\n"
        "DEB_DATE_EFFET (R4). Préparé environ\n3 semaines avant ; 9 % sont des rappels\n"
        "saisis 2 mois ou plus après (P20).",
    ]),
    ("6. PAIEMENT\nl'effet, puis le\nlot remis à la\nbanque", [
        "Les débours sont réunis dans un titre\nde paiement (l'effet), et les effets\n"
        "dans un lot (le bordereau) remis à la\nbanque ou à la caisse.",
        "EFFET · clé EFP_ID · lien BO_ID\nTEP_CODE : BVB virement, BE espèces…\n"
        "EFP_DATE_EDITION, EFP_DATE_DECAISSEMENT\nNET_A_PAYER · EFP_STATUT : D décaissé, A, I, N\n"
        "BORDEREAU_PREST · clé BO_ID : le lot\nIND_COMPTE_BANCAIRE : le compte du payé",
        "8,7 millions d'effets : virement\n4,30 M, espèces 4,24 M (P37)\n2025 : 507 631 effets décaissés\n"
        "(statut D, P21)",
        "62 % des paiements PF de 2025 n'ont pas\nd'effet : statut P, autre circuit (P23).\n"
        "Le débours prend le statut de son effet.\nNET_A_PAYER est répété sur chaque débours\n"
        "de l'effet : ne jamais l'additionner.",
    ]),
    ("7. SUITE\ndu droit, jusqu'à\nsa fin", [
        "Le droit peut être suspendu, repris,\nrévisé ou corrigé, puis il prend fin\n"
        "(radiation). Au décès d'un pensionné,\nla réversion s'ajoute à son dossier.\n"
        "Un contrôle a posteriori peut\nrevérifier le dossier payé.",
        "PRESTATION_ESP : PE_DATE_SUSPENSION,\nPE_DATE_REACTIVATION, PE_DATE_RADIATION\n"
        "PE_STATUT : A active, S suspendue, F fermée\nETAPE : RP révision, MAJ majoration\n"
        "AJUSTEMENT (via PE_AJ) : trop-perçu, rappel…\nDOSSIERS_CONTROLES : contrôles",
        "588 919 révisions, 167 810\nmajorations (P40)\n1,36 million d'ajustements (P41)\n"
        "115 contrôles en 2025 (P13)",
        "Statut et dates sont fiables (P24) : ils\ndisent qui est pensionné au 31/12 (R6).\n"
        "8 447 pensions d'orphelin actives sans\npaiement au 4e trimestre 2025 (P25).\n"
        "Plus de contrôle saisi depuis 07/2025.",
    ]),
]


def build():
    d = Diagram(seed=37)
    line_h = 18 * LINE_HEIGHT

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "La vie d'un dossier, de la demande au paiement", 48)
    d.text(0, 66, "La partie 2 du schéma tables_prestations, développée : ce que chaque étape "
                  "écrit dans la base · état au 30/10/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. parcours
    d.section(0, 120, 2600, 520, "1. Le parcours en 7 étapes")
    timeline = [
        "1. DÉPÔT\nla demande au guichet\n\nRECEPTION_DOSSIER\nRD_DATE_RECEPTION",
        "2. OUVERTURE\nle dossier de l'assuré\n(ou le rejet)\nDOSSIER\nDOS_DATE_OUVERTURE",
        "3. CALCUL\nun droit par prestation\n\nPRESTATION_ESP, ETAPE\nPE_DATE_EFFET",
        "4. NOTIFICATION\nla décision envoyée :\ndossier liquidé\nNOTIFICATION_PREST\n"
        "PE_DATE_NOTIFICATION",
        "5. DÉBOURS\nla somme due\nchaque mois\nDEBOURS\nDEB_DATE_EFFET",
        "6. PAIEMENT\nl'effet, puis le lot\nremis à la banque\nEFFET, BORDEREAU_PREST\n"
        "EFP_DATE_DECAISSEMENT",
        "7. SUITE DU DROIT\nsuspension, révision,\ncorrection, fin\nAJUSTEMENT, ETAPE\n"
        "PE_DATE_RADIATION",
    ]
    boxes, prev = [], None
    for i, txt in enumerate(timeline):
        box = d.box(25 + i * 370, 330, 330, 160, txt, 18, bg=C["blanc"], stroke=C["encre"],
                    stroke_width=3)
        if prev:
            d.harrow(prev, box, stroke_width=3)
        boxes.append(box)
        prev = box

    def span(first, last, txt, **kw):
        """Bandeau au-dessus des étapes first à last (incluses)."""
        a, b = boxes[first], boxes[last]
        return d.box(a["x"], 195, b["x"] + b["width"] - a["x"], 110, txt, 18, **kw)

    span(0, 3, "Durée de liquidation : du dépôt à la notification\nmédiane 2025 : vieillesse 62 j · "
               "AT/MP 156 j · PF 13,5 j · maternité 17 j (P10)\nnorme CIPRES : 45 jours au plus",
         bg=C["rouge_clair"], stroke=C["rouge"], color=C["rouge"])
    span(4, 5, "Premier paiement : 4 jours après\nla notification (médiane, P28),\n"
               "avec le rappel depuis la date d'effet", bg=C["gris"], stroke=C["gris_bord"])
    span(6, 6, "Contrôle a posteriori possible\naprès paiement (plus saisi\ndepuis 07/2025)",
         bg=C["gris"], stroke=C["gris_bord"], style="dashed")
    d.box(25, 520, 2550, 90,
          "Dans la base, chaque étape écrit une ligne dans sa table, reliée à la précédente par "
          "une clé : RD_ID, DOS_CODE, PE_ID, EFP_ID, BO_ID.\nLes dates de ces lignes donnent les "
          "tableaux CIPRES : dépôt, liquidation, présence au 31/12, montants payés (partie 4).", 20,
          bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 2. étape par étape
    sec2 = d.section(0, 690, 2600, 0, "2. Étape par étape : ce que la base enregistre")
    for (dx, _), head in zip(CELLS, HEADERS):
        d.text(FRAME_X + dx, 750, head, 22, C["doux"])
    y = 795
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

    # ------------------------------------------------------------------ 3. exemple
    y3 = sec2["y"] + sec2["height"] + 50
    d.section(0, y3, 2600, 420, "3. Un exemple suivi de bout en bout (personne fictive)")
    d.text(25, y3 + 62, "M. A part à la retraite. Les délais sont les médianes de 2025 pour la "
                        "vieillesse (P10, P28).", 20, C["doux"])
    example = [
        "10/01/2025 · dépôt\n\nM. A apporte sa demande, son\nacte de naissance (AN), son\n"
        "certificat de travail (CTR)\net son RIB\nRECEPTION_DOSSIER",
        "14/01/2025 · ouverture\n\nun dossier V (vieillesse)\nest ouvert pour M. A,\n"
        "au statut A (actif)\n\nDOSSIER",
        "20/02/2025 · calcul\n\nune prestation RPV,\navec un effet au 01/11/2024 ;\n"
        "calcul initial CIP\n\nPRESTATION_ESP, ETAPE",
        "13/03/2025 · notification\n\nla décision est envoyée,\n62 jours après le dépôt :\n"
        "le dossier est liquidé\n\nNOTIFICATION_PREST",
        "17/03/2025 · 1er paiement\n\n5 débours, de 11/2024\nà 03/2025 (le rappel),\n"
        "réunis dans un effet BVB\n(virement), décaissé : D\nDEBOURS, EFFET",
        "Ensuite, chaque mois\n\nun débours, préparé 3\nsemaines avant ; au décès\n"
        "de M. A, radiation (F) ; sa\nveuve dépose sur le même\ndossier : la réversion",
    ]
    prev = None
    for i, txt in enumerate(example):
        box = d.box(25 + i * 431, y3 + 110, 395, 270, txt, 18, bg=C["bleu_clair"],
                    stroke=C["bleu"])
        if prev:
            d.harrow(prev, box, color=C["bleu"], stroke_width=2)
        prev = box

    # ------------------------------------------------------------------ 4. tableaux CIPRES
    y4 = y3 + 470
    d.section(0, y4, 2600, 340, "4. Ce que chaque étape donne aux tableaux CIPRES")
    cards = [
        "T25 à T27 · demandes\n\ndéposées : réceptions de\nl'année (D3) ; liquidées :\n"
        "1re notification de chaque\ndemande (D4) ; en instance :\nni liquidées ni rejetées\n"
        "au 31/12 (D6)",
        "T28 à T30 · contrôles\n\nles dossiers payés,\nrevérifiés après coup\n"
        "(DOSSIERS_CONTROLES)\n0,07 % en 2025 (P13) ;\nnorme : 5 % au moins",
        "T31 · indus\n\nles ajustements de\ntrop-perçu et de\nmoins-perçu, par mois\n"
        "et par branche",
        "T32 à T49 · bénéficiaires\n\nau 31/12 : droit actif et\npayé au 4e trimestre (R6)\n"
        "nouveaux : droits notifiés\ndans l'année (R7)",
        "T50 à T56 · montants\n\ndébours PM au statut D\nou P, montant DEB_MONTANT\n(R3), "
        "au mois payé\nDEB_DATE_EFFET (R4)",
    ]
    for i, txt in enumerate(cards):
        d.box(25 + i * 514, y4 + 70, 494, 240, txt, 18, bg=C["rouge_moyen"], stroke=C["rouge"])

    # ------------------------------------------------------------------ 5. vérifié, ouvert
    y5 = y4 + 390
    d.section(0, y5, 2600, 330, "5. Ce que le lot 3 B a vérifié, et ce qui reste ouvert")
    g = d.group()
    frame = d.box(25, y5 + 70, 1265, 230, bg=C["vert_clair"], stroke=C["vert"], groups=g)
    d.text(45, y5 + 86, "Vérifié dans les données (30/10/2026)", 20, C["vert"], groups=g,
           inside=frame)
    d.text(45, y5 + 124,
           "Une réception ouvre au plus un dossier : oui, sauf 11 cas (P52).\n"
           "La réversion d'un pensionné décédé (la veuve de M. A) est ajoutée à\n"
           "son dossier, par une demande ANCIEN (P51, P58).\n"
           "Une seconde notification vient en médiane 9 ans après la première :\n"
           "c'est une réversion, pas une révision (P54).\n"
           "Les vieilles réceptions jamais closes restaient en instance pour\n"
           "toujours : il faut les sortir du stock (P50, P57 ; règle D6).",
           18, groups=g, inside=frame)
    g = d.group()
    frame = d.box(1310, y5 + 70, 1265, 230, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(1330, y5 + 86, "Encore ouvert", 20, C["jaune_bord"], groups=g, inside=frame)
    d.text(1330, y5 + 124,
           "Que veulent dire RD_STATUT A (depuis mai 2025) et le lien L, V,\n"
           "T, F, A du déposant ? (DSI ; P53, P55)\n"
           "Dossiers AT/MP de frais médicaux seuls : leur premier remboursement\n"
           "vaut-il liquidation ?\n"
           "Que veulent dire NOT_STATUT, EFP_STATUT I et N, DEB_STATUT I ? (DSI)\n"
           "Depuis quelle date compter le stock en instance ? (question Q3, T25)",
           18, groups=g, inside=frame)

    return d


if __name__ == "__main__":
    main(build, OUTPUT)
