"""
Schéma « Le parcours d'un dossier, en modèle conceptuel » (état au 30/10/2026).

Les tables du parcours (vie_dossier) dessinées comme un modèle conceptuel de données à la
manière de Merise : entités (rectangles), associations (ovales, un verbe) et cardinalités.
Le parcours principal se lit de gauche à droite ; les tables de détail sont au-dessus et
au-dessous, les personnes en bas.

Sources : analyses/prestations (P01 volumes, P10 réceptions des dossiers de 2025, P16 personnes,
P20 débours et prestations, P21 et P23 effets, P34 à P44 listes du lot 3 ; lot 3 B : P48 lien
des anciens dossiers, P52 et P58 demandes NOUVEAU et ANCIEN, P56 motifs de rejet) ; les
cardinalités suivies de « ? » restent à vérifier.

Usage :
    python3 excalidraw/sources/prestations/modele_conceptuel_dossier.py [--force]
"""

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, LINE_HEIGHT, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "vie_dossier" / "modele_conceptuel_dossier.excalidraw"

# Grille : 6 colonnes pour le parcours principal, une association entre deux colonnes
ENT_W, PITCH, X0 = 340, 620, 40
Y_TOP, Y_SPINE, Y_BOT, Y_PERS = 330, 760, 1240, 1700
H_SAT, H_SPINE = 200, 260
ASSOC_W, ASSOC_H = 170, 90
Y_ASSOC_TOP, Y_ASSOC_SPINE, Y_ASSOC_BOT, Y_ASSOC_PERS = 640, 890, 1130, 1560


def col(i):
    return X0 + i * PITCH


def gap(i):
    """Milieu de l'intervalle entre les colonnes i et i + 1."""
    return col(i) + ENT_W + (PITCH - ENT_W) / 2


def center(shape):
    return shape["x"] + shape["width"] / 2, shape["y"] + shape["height"] / 2


def border_point(shape, toward):
    """Point du bord de shape sur la droite qui va de son centre vers toward."""
    cx, cy = center(shape)
    dx, dy = toward[0] - cx, toward[1] - cy
    if shape["type"] == "ellipse":
        t = 1 / math.hypot(dx / (shape["width"] / 2), dy / (shape["height"] / 2))
    else:
        tx = shape["width"] / 2 / abs(dx) if dx else math.inf
        ty = shape["height"] / 2 / abs(dy) if dy else math.inf
        t = min(tx, ty)
    return cx + dx * t, cy + dy * t


def build():
    d = Diagram(seed=41)

    def entity(x, y, w, h, title, table, attrs, spine=False):
        """Entité : un en-tête (nom en clair, puis la table) et ses colonnes utiles."""
        g = d.group()
        width = 3 if spine else 2
        frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["encre"], stroke_width=width, groups=g)
        head = d.box(x, y, w, 64, bg=C["gris"] if spine else C["section"], stroke=C["encre"],
                     stroke_width=width, groups=g)
        d.text(x + 14, y + 8, title, 20, groups=g, inside=head)
        d.text(x + 14, y + 36, table, 16, C["doux"], groups=g, inside=head)
        d.text(x + 14, y + 76, "\n".join(attrs), 16, groups=g, inside=frame)
        return frame

    def assoc(cx, cy, verb, w=ASSOC_W):
        return d.ellipse(cx - w / 2, cy - ASSOC_H / 2, w, ASSOC_H, verb, 16, bg=C["blanc"],
                         stroke=C["doux"])

    def link(ent, asc, card, at=None):
        """Trait entre une entité et une association ; la cardinalité est écrite près de
        l'entité, au-dessus du trait ou à sa droite, sinon de l'autre côté si elle mordait sur
        l'entité. at : point du bord de l'entité à viser."""
        p_ent = at or border_point(ent, center(asc))
        p_asc = border_point(asc, p_ent)
        length = math.dist(p_ent, p_asc)
        ux, uy = (p_asc[0] - p_ent[0]) / length, (p_asc[1] - p_ent[1]) / length
        d.arrow(p_ent[0] + ux * 4, p_ent[1] + uy * 4, p_asc[0] - ux * 4, p_asc[1] - uy * 4,
                ent, asc, color=C["doux"], head=None)
        w, h = text_size(card, 18)
        px, py = uy, -ux
        if py > 0 or (py == 0 and px < 0):
            px, py = -px, -py
        along = 10 + abs(ux) * w / 2 + abs(uy) * h / 2
        side = 4 + abs(px) * w / 2 + abs(py) * h / 2
        for sign in (1, -1):
            x = p_ent[0] + ux * along + sign * px * side - w / 2
            y = p_ent[1] + uy * along + sign * py * side - h / 2
            if not (x < ent["x"] + ent["width"] and ent["x"] < x + w
                    and y < ent["y"] + ent["height"] and ent["y"] < y + h):
                break
        d.text(x, y, card, 18, C["jaune_bord"] if card.endswith("?") else C["encre"])

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "Le parcours d'un dossier, en modèle conceptuel", 48)
    d.text(0, 66, "Les tables du parcours, leurs liens et leurs cardinalités, à la manière d'un "
                  "modèle conceptuel Merise · état au 30/10/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. modèle
    sec1 = d.section(0, 120, 3520, 0, "1. Le modèle : de la demande au paiement, et les personnes")
    d.text(25, 182, "Chaque rectangle porte le numéro de son étape dans le schéma vie_dossier : "
                    "1 dépôt, 2 ouverture, 3 calcul, 4 notification, 5 débours, 6 paiement, "
                    "7 suite du droit.", 20, C["doux"])

    g = d.group()
    legend = d.box(col(4), Y_TOP, col(5) + ENT_W - col(4), H_SAT, bg=C["jaune_clair"],
                   stroke=C["jaune_bord"], groups=g)
    d.text(col(4) + 18, Y_TOP + 14, "Comment lire ce modèle", 20, groups=g, inside=legend)
    d.text(col(4) + 18, Y_TOP + 48,
           "Rectangle : ce que la base enregistre (une table) ; cadre épais : le parcours.\n"
           "Ovale : le lien entre deux rectangles, à lire comme un verbe.\n"
           "Nombres : combien de fois. « 0,n » près de Dossier sur « accorde » : un dossier\n"
           "accorde 0, 1 ou plusieurs droits ; « 1,1 » près de Droit : il vient d'un seul dossier.\n"
           "En orange, avec « ? » : encore à vérifier dans les données.",
           18, groups=g, inside=legend)

    # le parcours principal
    demande = entity(col(0), Y_SPINE, ENT_W, H_SPINE, "Demande", "RECEPTION_DOSSIER · étape 1", [
        "clé : RD_ID",
        "RD_DATE_RECEPTION : dépôt",
        "RD_TYPE_RECEPTION :",
        "   NOUVEAU ou ANCIEN",
        "DOS_CODE : dossier visé (ANCIEN)",
        "TDOS_CODE : branche demandée",
        "RD_STATUT : E presque toujours",
    ], spine=True)
    dossier = entity(col(1), Y_SPINE, ENT_W, H_SPINE, "Dossier", "DOSSIER · étape 2", [
        "clé : DOS_CODE",
        "TDOS_CODE : branche V, A, F, M",
        "DOS_DATE_OUVERTURE : ouverture",
        "DOS_CATEGORIE : ordinaire…",
        "DOS_STATUT : A actif, F fermé",
    ], spine=True)
    droit = entity(col(2), Y_SPINE, ENT_W, H_SPINE, "Droit", "PRESTATION_ESP · étapes 3 et 7", [
        "clé : PE_ID",
        "TPE_CODE : type de droit (28)",
        "PE_DATE_EFFET : début du droit",
        "PE_DATE_NOTIFICATION",
        "PE_DATE_RADIATION : fin",
        "PE_STATUT : A, S, F",
    ], spine=True)
    debours = entity(col(3), Y_SPINE, ENT_W, H_SPINE, "Somme due", "DEBOURS · étape 5", [
        "clé : DEB_ID",
        "DEB_TYPE : PM, RB, AJ, CP",
        "DEB_DATE_EFFET : mois payé",
        "DEB_MONTANT : montant",
        "DEB_STATUT : D, P, V, I, N, A, S",
        "TPN_CODE : soin payé (RB)",
    ], spine=True)
    effet = entity(col(4), Y_SPINE, ENT_W, H_SPINE, "Titre de paiement", "EFFET · étape 6", [
        "clé : EFP_ID",
        "TEP_CODE : virement, espèces…",
        "EFP_DATE_DECAISSEMENT",
        "NET_A_PAYER : total de l'effet",
        "EFP_STATUT : D décaissé, A, I, N",
    ], spine=True)
    bordereau = entity(col(5), Y_SPINE, ENT_W, H_SPINE, "Lot de paiement",
                       "BORDEREAU_PREST · étape 6", [
        "clé : BO_ID",
        "remis à une banque ou une caisse",
        "nombre d'effets, net, décaissement",
    ], spine=True)
    spine = [
        (demande, "ouvre", dossier, "0,1", "0,1"),
        (dossier, "accorde", droit, "0,n", "1,1"),
        (droit, "est due\nau titre de", debours, "0,n", "0,1"),
        (debours, "est réglée\npar", effet, "0,1", "1,n"),
        (effet, "regroupé\ndans", bordereau, "0,1 ?", "1,n"),
    ]
    for i, (left, verb, right, card_left, card_right) in enumerate(spine):
        a = assoc(gap(i), Y_ASSOC_SPINE, verb, w=150)  # plus étroit : place pour les cardinalités
        link(left, a, card_left)
        link(right, a, card_right)

    # sous le premier lien : la demande déposée sur un dossier existant (réversion)
    a = assoc(gap(0), 1060, "porte sur\n(ANCIEN)", w=150)
    link(demande, a, "0,1")
    link(dossier, a, "0,n")

    # au-dessus : le détail des étapes
    piece = entity(col(0), Y_TOP, ENT_W, H_SAT, "Pièce fournie", "RECEPTION_DOS_PIECE · étape 1", [
        "TPJ_CODE : 86 types de pièce",
        "acte de naissance (AN), RIB…",
    ])
    sinistre = entity(col(1), Y_TOP, ENT_W, H_SAT, "Accident du travail", "TSINISTRE · branche A", [
        "lésion, incapacité…",
        "plus alimentée depuis 12/2024",
    ])
    etape = entity(col(2), Y_TOP, ENT_W, H_SAT, "Calcul du montant", "ETAPE · étapes 3 et 7", [
        "TEA_CODE : CIP calcul initial,",
        "RP révision, MAJ majoration…",
    ])
    ajustement = entity(col(3), Y_TOP, ENT_W, H_SAT, "Correction", "AJUSTEMENT · étape 7", [
        "clé : AJ_ID",
        "TAJ_CODE : trop-perçu, rappel,",
        "avance… (13 types)",
        "AJ_STATUT : A, C, I, R (sens ?)",
    ])
    for ent, verb, target, card_ent, card_target in [
        (piece, "accompagne", demande, "1,1", "0,n"),
        (sinistre, "décrit", dossier, "1,1", "0,1 ?"),
        (etape, "calcule", droit, "1,1", "0,n"),
        (ajustement, "se règle\npar", debours, "0,n", "0,1"),
    ]:
        a = assoc(ent["x"] + ENT_W / 2, Y_ASSOC_TOP, verb)
        link(ent, a, card_ent)
        link(target, a, card_target)
    a = assoc(gap(2), Y_ASSOC_TOP, "corrige\n(PE_AJ)")
    link(droit, a, "0,n")
    link(ajustement, a, "1,n ?")

    # au-dessous : les autres issues et décisions
    rejet = entity(col(0), Y_BOT, ENT_W, H_SAT, "Refus de la demande", "DOSSIER_REJETE · étape 2", [
        "DOSRE_DATE_REJET",
        "DOSRE_MOTIF : prescription,",
        "pas de bénéficiaire, pièce…",
        "une quinzaine par an (pensions)",
    ])
    notification = entity(col(1), Y_BOT, ENT_W, H_SAT, "Décision notifiée",
                          "NOTIFICATION_PREST · étape 4", [
        "TPE_CODE : le droit notifié",
        "montant, date d'effet, signature",
        "NOT_STATUT : A, I (sens ?)",
    ])
    controle = entity(col(2), Y_BOT, 300, H_SAT, "Contrôle", "DOSSIERS_CONTROLES · 7", [
        "a posteriori, après paiement",
        "plus saisi depuis 07/2025",
    ])
    for ent, verb, target, card_ent, card_target, cx in [
        (rejet, "refuse", demande, "1,1", "0,1 ?", col(0) + ENT_W / 2),
        (notification, "notifie", dossier, "1,1", "0,n", col(1) + ENT_W / 2),
        (controle, "vérifie", dossier, "1,1", "0,n", gap(1)),
    ]:
        a = assoc(cx, Y_ASSOC_BOT, verb)
        link(ent, a, card_ent)
        link(target, a, card_target)

    # en bas : les personnes, une seule table pour tous les rôles
    personne = entity(600, Y_PERS, 2160, 230, "Personne", "INDIVIDU · une même table pour tous les rôles", [
        "clé : IND_ID · sexe (1 ou 2), date de naissance, date de décès",
        "Rôles dans le parcours : l'assuré (dossier), l'ayant droit et la personne payée (droit), "
        "la personne payée (effet), le tuteur d'un orphelin,",
        "un membre de la famille (LIEN). Pour un orphelin, l'ayant droit et la personne payée "
        "sont deux personnes différentes (P16).",
    ], spine=True)
    compte = entity(3140, Y_PERS, ENT_W, 160, "Compte bancaire", "IND_COMPTE_BANCAIRE", [
        "banque, agence, numéro :",
        "où arrive un virement",
    ])
    top = personne["y"]
    bottom = Y_SPINE + H_SPINE
    for source, verb, cx, card_source, at in [
        (dossier, "concerne\n(l'assuré)", 440, "1,1", None),
        # deux liens du droit vers la personne : départs écartés pour ne pas mêler les cardinalités
        (droit, "a pour\nayant droit", 1800, "1,1", (col(2) + 200, bottom)),
        (droit, "est versé à", 2070, "1,1", (col(2) + 310, bottom)),
        (effet, "est payé à", 2690, "1,1", None),
    ]:
        a = assoc(cx, Y_ASSOC_PERS, verb)
        link(source, a, card_source, at=at)
        x = min(max(cx, personne["x"] + 40), personne["x"] + personne["width"] - 40)
        link(personne, a, "0,n", at=(x, top))
    a = assoc(2950, 1790, "possède")
    link(personne, a, "0,n", at=(personne["x"] + personne["width"], 1790))
    link(compte, a, "1,1", at=(compte["x"], 1790))
    a = assoc(400, 1815, "est lié à\n(famille)")
    link(personne, a, "0,n", at=(personne["x"], 1755))
    link(personne, a, "0,n", at=(personne["x"], 1875))
    sec1["height"] = Y_PERS + 230 + 40 - sec1["y"]

    # ------------------------------------------------------------------ 2. en clair
    y2 = sec1["y"] + sec1["height"] + 50
    d.section(0, y2, 3520, 330, "2. Ce que le modèle dit, en clair")
    g = d.group()
    frame = d.box(25, y2 + 70, 2150, 230, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, y2 + 88,
           "Une nouvelle demande ouvre au plus un dossier (11 exceptions, P52) ; les anciens "
           "dossiers n'ont pas de lien vers leur demande (P48).\n"
           "Une demande ANCIEN porte sur un dossier existant sans en ouvrir : c'est presque "
           "toujours la réversion, ajoutée au dossier du défunt (P58).\n"
           "Un dossier accorde 0, 1 ou plusieurs droits (3 allocations familiales, une pension "
           "puis sa réversion) ; un droit appartient à un seul dossier.\n"
           "Un droit donne une somme due par mois. Un vrai paiement (PM) a toujours son droit "
           "(P20) ; un remboursement de soins (RB) n'en a pas.\n"
           "Une somme due est réglée par au plus un titre de paiement, qui en réunit plusieurs ; "
           "réglée sans titre, elle a le statut P (P23).\n"
           "La décision notifiée se rattache au dossier et au type de droit (DOS_CODE + TPE_CODE), "
           "pas à un droit précis.\n"
           "Trois rôles de personne : l'assuré, l'ayant droit et la personne payée ; on compte "
           "l'ayant droit dans les tableaux CIPRES (règle R5).",
           18, groups=g, inside=frame)
    g = d.group()
    frame = d.box(2195, y2 + 70, 1300, 230, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(2215, y2 + 88, "À vérifier : les cardinalités suivies de « ? »", 20, groups=g,
           inside=frame)
    d.text(2215, y2 + 124,
           "Une demande a-t-elle au plus un rejet ?\n"
           "Un seul accident (TSINISTRE) par dossier AT/MP ?\n"
           "Un ajustement corrige-t-il un ou plusieurs droits (PE_AJ) ?\n"
           "Tous les effets passent-ils par un bordereau ?",
           18, groups=g, inside=frame)

    return d


if __name__ == "__main__":
    main(build, OUTPUT)
