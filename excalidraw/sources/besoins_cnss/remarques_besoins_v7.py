"""
Le recueil des besoins de la CNSS (v7) : les remarques, face à notre travail (état au 04/11/2026).

Premier schéma du dossier « besoins cnss », qui regroupe ce qu'on tire du document
cnss_files/2566BF_CNSS_Analytics_Description_Besoins_v7.docx (ateliers du 14 au 21 janvier 2026).
Les remarques sont numérotées B01 à B41 pour pouvoir y renvoyer. Rien n'est encore reporté dans
les règles, les fiches de lecture ni les requêtes.

Usage :
    python3 excalidraw/sources/besoins_cnss/remarques_besoins_v7.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import WIDTH, badge, height_of, wrap  # noqa: E402

OUTPUT = ROOT / "schemas" / "besoins cnss" / "remarques_besoins_v7.excalidraw"
INNER = WIDTH - 50
SIZE = 18

DOCUMENT = [
    ("Les termes de référence",
     "La structure de la CNSS (12 directions centrales, 5 directions régionales : Ouagadougou, "
     "Bobo-Dioulasso, Ouahigouya, Fada N'Gourma, Dédougou), ses 7 applications (immatriculation, "
     "recouvrement, prestations, GRH et paie, loyers, immobilisations, Oracle E-Business), les "
     "attentes et les exigences techniques de l'outil décisionnel."),
    ("Les tableaux CIPRES, domaine par domaine",
     "Un atelier par domaine, avec un commentaire de la CNSS pour chaque tableau : Prestation "
     "(14/01, 30 tableaux), RH (15/01, T58 à T65), Finance (15/01, T66 à T76 et 2 tableaux en "
     "plus), Recouvrement (16/01, T1 à T22 et 2 maquettes en plus)."),
    ("Les indicateurs du plan stratégique",
     "Une centaine d'indicateurs (20/01), par objectif, avec parfois leur formule, leur "
     "fréquence, leur découpage et la structure responsable ; puis les informations encore à "
     "fournir par 9 structures (21/01) et 16 notes de bas de page."),
]

# (numéro, tableau, ce que dit la CNSS, chez nous aujourd'hui, où dans le document)
CONFIRME = [
    ("B01", "T25, T28", "« Ajouter une ligne Pension de retraite anticipée ».",
     "C'est notre proposition (terme 2 de T25 dans la fiche de lecture).",
     "Prestation, lignes 18 et 19"),
    ("B02", "T25", "Délai de liquidation d'une pension : du dépôt du dossier complet au guichet à "
     "la notification des droits.", "Notre règle de durée pour T25.",
     "Indicateurs, gestion des pensions"),
    ("B03", "T27", "Dossier en instance : déposé, ni liquidé ni rejeté au cours de la période.",
     "Notre règle des dossiers en instance.", "Prestation, ligne 1"),
    ("B04", "T35", "« Supprimer la tranche d'âge 25 ans et plus ».",
     "Un orphelin n'est compté que jusqu'à 24 ans.", "Prestation, ligne 23"),
    ("B05", "T41, et T37, T38, T45 à T49", "Allocataire : le titulaire du dossier (l'assuré).",
     "Notre règle (terme 3 de T41).", "Prestation, ligne 4"),
    ("B06", "T42, T50", "Services passés : le total des périodes d'activité salariée, en mois ; "
     "pour une pension de coordination, seulement la part CNSS.", "Notre règle R9.",
     "Prestation, lignes 25 et 28"),
    ("B07", "T32", "Pensions de coordination : les montants ne concernent que la part CNSS.",
     "Notre montant est ce que la CNSS a payé.", "Prestation, ligne 20"),
    ("B08", "T48", "Degré d'IPP : la somme des taux divisée par l'effectif de la tranche ; chaque "
     "rentier a un taux dans l'application.", "La moyenne simple des taux.",
     "Prestation, ligne 14"),
    ("B09", "T29, T30", "Nombre de bénéficiaires payés : le nombre de dossiers payés ; le Total "
     "est le total des trimestres.", "Notre terme 1 de T28 à T30 (un dossier payé).",
     "Prestation, lignes 2 et 8"),
    ("B10", "T58", "Pour la CNSS : agents d'exécution 1re et 2e catégories, agents de maîtrise "
     "3e, cadres moyens 4e et 5e, cadres supérieurs 6e. Pour la CIPRES : cadres = 4e, 5e et 6e.",
     "Notre proposition : la 5e en cadres moyens. Répond à la question du terme 3 de T58.",
     "RH, ligne 1"),
    ("B11", "T63, T64", "Frais de personnel : les comptes 65 ; masse salariale : les comptes 651 "
     "et 652.", "Les comptes 65 étaient une proposition BI : confirmés.", "RH, ligne 6"),
    ("B12", "T65", "Taux d'encadrement : norme de 10 % au plus.", "Notre règle.", "RH, ligne 8"),
]

CHANGE = [
    ("B13", "T31", "Source : les fichiers Excel de la DAI.",
     "Les trop-perçus de la base (table AJUSTEMENT) : 0,39 % d'indus en 2024.",
     "Prestation, ligne 3"),
    ("B14", "T28 (et T29, T30)", "Source : les fichiers Excel de la DAI.",
     "La table des contrôles de la base, où plus rien n'est saisi depuis juillet 2025.",
     "Prestation, ligne 19"),
    ("B15", "T54, T55", "Montant : le montant liquidé.",
     "Le montant payé (débours de paiement au statut D ou P).", "Prestation, lignes 5 et 6"),
    ("B16", "T26, durée d'une rente", "Du certificat médical de consolidation à la notification. "
     "Écrit sous « rentes de survivants », sans doute par erreur : c'est la rente de la victime "
     "qui suit la consolidation.", "Depuis la demande au guichet : 14 jours en médiane en 2025, "
     "contre 64 depuis le certificat (lot 4, P62).", "Indicateurs, risques professionnels"),
    ("B17", "T26, durée des soins", "Du certificat du médecin traitant à la date de convocation.",
     "De la déclaration au premier remboursement saisi ; ces deux dates ne sont pas dans la base "
     "(atelier de Kaya).", "Indicateurs, risques professionnels"),
    ("B18", "T27, durée d'un dossier PF", "Du dépôt du dossier complet au guichet à la date de "
     "validation.", "Jusqu'à la notification.", "Indicateurs, prestations familiales"),
    ("B19", "T59", "5 domaines : recouvrement, prestations, prévention des risques "
     "professionnels, action sanitaire et sociale, gestion administrative et financière.",
     "Les 7 lignes de l'Annuaire CIPRES et « autres domaines » ; l'immatriculation et le "
     "contentieux ne sont plus cités.", "RH, ligne 2"),
    ("B20", "T61", "Prendre en compte les sorties et les retours dans le calcul de l'ancienneté.",
     "Les années pleines depuis l'embauche, sans retirer les interruptions.", "RH, ligne 4"),
]

# (numéro, élément, grille des ratios CIPRES 2025, recueil des besoins v7)
COMPTES = [
    ("B21", "Charges de fonctionnement", "comptes 61 à 68, gestion G ou 7",
     "comptes 61 à 66, gestion G seulement (Finance, ligne 1, T66)"),
    ("B22", "Dépenses de formation", "comptes 63812, 6386 et 6577",
     "« 63, 86 et 87 » des gestions G et S (RH, ligne 6) : à faire préciser, peut-être 6386 et "
     "6387"),
    ("B23", "Frais de personnel", "rien", "comptes 65 des gestions G et S (RH, ligne 6)"),
    ("B24", "Masse salariale", "comptes 651 et 652", "comptes 651 et 652, gestion non dite"),
    ("B25", "Produits techniques", "compte 70", "comptes 701, 702 et 708 (Finance, ligne 1)"),
]

REPONSES = [
    ("B26", "T57", "« Assurés décédés » ; remplacer « Autres » par « TOTAL » ; détailler les "
     "branches par type de bénéficiaire ; ajouter un filtre assuré ou ayant droit.",
     "Répond à notre question du terme 7 de T50 à T57.", "Prestation, ligne 7"),
    ("B27", "T27", "Renommer « Autres prestations de la branche » en « Frais médicaux de "
     "maternité ».", "Des frais de maternité sont prévus dans la base, jamais payés.",
     "Prestation, ligne 1"),
    ("B28", "T38", "« Crédirentiers : rentiers ; le cumul sans préciser un exercice ».",
     "Flou : un stock à une date choisie ? À faire préciser.", "Prestation, ligne 10"),
    ("B29", "T60", "« Autre » : à détailler, en listant les cas pour la CNSS.",
     "Nos autres entrées et autres sorties sont regroupées.", "RH, ligne 3"),
    ("B30", "T62", "Faire un autre tableau pour la formation des personnes externes (Excel, "
     "bilan social).", "Pas prévu.", "RH, ligne 5"),
    ("B31", "T33", "La DSI doit développer un module pour les pensions d'invalidité, converties "
     "en pension normale quand l'assuré atteint l'âge de la retraite.", "Pas prévu.",
     "Prestation, ligne 21"),
]

NOUVEAU = [
    ("B32", "Périodicité", "La plupart des tableaux sont demandés en mensuel, trimestriel et "
     "annuel (T27, T31, T41, T42, T58…), T59 aussi en journalier. Nos requêtes sont toutes "
     "annuelles."),
    ("B33", "Période en paramètre", "Une date de début et une date de fin, au choix, pour T43, "
     "T44 et T45 à T49."),
    ("B34", "Déclinaison", "Par direction, agence et bureau pour la RH ; par direction régionale "
     "et agence pour les indicateurs."),
    ("B35", "Les ratios", "En pourcentage, avec 2 chiffres après la virgule (T69, T70, T76, "
     "SP1)."),
    ("B36", "Type d'employé", "T58 par type d'employé : permanent ou temporaire, pour la CNSS "
     "comme pour la CIPRES."),
    ("B37", "Indicateurs faisables avec nos tables", "La part des prestations payées par "
     "virement (EFFET, code BVB) ; le rapport démographique (retraités = pensions directes : "
     "vieillesse, anticipée, invalidité ; cotisants actifs = dossiers actifs) ; le taux de "
     "remplacement (dernier revenu = salaire moyen des 5 meilleures années) ; le ratio des "
     "ressources allouées aux prestations (prestations ordonnancées = notifiées)."),
]

MANQUE = [
    ("B38", "T26, T39, T40 et T56 n'ont aucun commentaire."),
    ("B39", "T65 est « en attente du retour de la CNSS »."),
    ("B40", "T22 (recouvrement) : « données non disponibles »."),
    ("B41", "Les termes de référence reprennent des passages d'un cahier des charges bancaire "
            "(activité bancaire, PNB, encours par agences, reporting MPR du Groupe) : à ignorer."),
]

HORS = [
    ("Recouvrement, T1 à T22 (16/01)",
     "Deux définitions selon le destinataire, par exemple l'employeur actif : celui qui a "
     "déclaré ou payé (CIPRES), celui qui a le statut actif (CNSS). Renommages, fusion des "
     "assurés volontaires et des travailleurs indépendants, nomenclatures CITI et NAEMA, calculs "
     "sur le compte individuel, deux maquettes en plus (SP REC1, SP REC2)."),
    ("Finance, T66 à T76 (15/01)",
     "Comptes des charges techniques (601, 602, 603, 605), des produits financiers (771 à 778) "
     "et des placements ; budget lu dans Oracle GL (module budget) ; taux d'absorption du budget "
     "(SP1) ; immeubles de rapport, gestion I (SP2) ; réserves de T75, disponibles vers juillet "
     "de l'année suivante."),
    ("Indicateurs du plan stratégique (20/01 et 21/01)",
     "Beaucoup ne sont pas dans les bases : enquêtes biennales, fichiers Excel des directions. "
     "Plusieurs formules restent à proposer par le comité de pilotage ou le consultant en "
     "budget programme."),
]


def rows(d, y, title, headers, widths, data, bg, stroke, note=None):
    """Une section de remarques, une rangée par remarque : pastille, puis les colonnes."""
    sec = d.section(0, y, WIDTH, 0, title)
    y += 70
    if note:
        t = wrap(note, 20, INNER - 20)
        d.text(25, y, t, 20, C["doux"])
        y += height_of(t, 20) + 20
    x = 25 + 90
    for head, w in zip(headers, widths):
        d.text(x, y, head, 20, C["doux"])
        x += w
    y += 40
    line_w = 25 + 90 + sum(widths)
    for item in data:
        num, cells = item[0], item[1:]
        texts = [wrap(c, SIZE, w - 30) for c, w in zip(cells, widths)]
        h = max(height_of(t, SIZE) for t in texts) + 30
        g = d.group()
        frame = d.box(25, y, line_w - 25, h, bg=bg, stroke=stroke, groups=g)
        badge(d, 40, y + (h - 44) / 2 if h < 90 else y + 15, num, groups=g)
        x = 25 + 90
        for i, (t, w) in enumerate(zip(texts, widths)):
            color = C["doux"] if i == len(texts) - 1 else C["encre"]
            d.text(x, y + 15, t, SIZE, color, groups=g, inside=frame)
            x += w
        y += h + 12
    sec["height"] = y + 20 - sec["y"]
    return y + 70


def cards(d, y, title, data, bg, stroke, cols=3):
    """Des cartes : un titre et un texte, sur plusieurs colonnes."""
    sec = d.section(0, y, WIDTH, 0, title)
    y += 80
    gap = 20
    w = (INNER - gap * (cols - 1)) / cols
    for start in range(0, len(data), cols):
        line = data[start:start + cols]
        bodies = [wrap(body, SIZE, w - 40) for *_, body in line]
        heads = [wrap(" ".join(item[:-1]), 20, w - 100) for item in line]
        h = max(height_of(b, SIZE) + height_of(hd, 20) for b, hd in zip(bodies, heads)) + 60
        for i, (item, hd, body) in enumerate(zip(line, heads, bodies)):
            x = 25 + i * (w + gap)
            g = d.group()
            frame = d.box(x, y, w, h, bg=bg, stroke=stroke, groups=g)
            d.text(x + 20, y + 15, hd, 20, stroke, groups=g, inside=frame)
            d.text(x + 20, y + 25 + height_of(hd, 20), body, SIZE, groups=g, inside=frame)
        y += h + gap
    sec["height"] = y + 10 - sec["y"]
    return y + 70


def build():
    d = Diagram(seed=74)

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "Le recueil des besoins de la CNSS (v7) : les remarques", 48)
    d.text(0, 66, "2566BF_CNSS_Analytics_Description_Besoins_v7.docx (cnss_files), ateliers du 14 "
                  "au 21 janvier 2026 · lu le 04/11/2026 · rien n'est encore reporté dans nos "
                  "règles, fiches ou requêtes", 22, C["doux"])

    # ------------------------------------------------------------------ 0. le document
    y = cards(d, 130, "Le document en bref", [(t, b) for t, b in DOCUMENT], C["blanc"],
              C["encre"])

    legend = ("Chaque remarque porte un numéro (B01 à B41) pour y renvoyer. « Où dans le "
              "document » donne la partie et le numéro de ligne du tableau de la CNSS.")
    y = rows(d, y, "1. Ce qui confirme nos règles (B01 à B12)",
             ["Tableau", "Ce que dit la CNSS", "Chez nous aujourd'hui", "Où dans le document"],
             [330, 900, 850, 375], CONFIRME, C["vert_clair"], C["vert"], note=legend)
    y = rows(d, y, "2. Ce qui contredit ou change nos règles (B13 à B20)",
             ["Tableau", "Ce que dit la CNSS", "Chez nous aujourd'hui", "Où dans le document"],
             [330, 900, 850, 375], CHANGE, C["rouge_clair"], C["rouge"],
             note="Ce sont les remarques à trancher en premier : chacune changerait une règle ou "
                  "une source de nos requêtes.")
    y = rows(d, y, "3. Les comptes : deux documents de la CNSS qui ne disent pas la même chose "
                   "(B21 à B25)",
             ["Élément", "Grille des ratios CIPRES 2025", "Recueil des besoins v7"],
             [500, 800, 1155], COMPTES, C["orange_clair"], C["orange"],
             note="La grille « Evaluation des Ratios CIPRES 2025 » (feuilles 07 et 09) et le "
                  "recueil des besoins donnent des comptes différents : à faire trancher par la "
                  "direction financière (DFC).")
    y = rows(d, y, "4. Réponses à nos questions, renommages et demandes en plus (B26 à B31)",
             ["Tableau", "Ce que dit la CNSS", "Chez nous aujourd'hui", "Où dans le document"],
             [330, 900, 850, 375], REPONSES, C["bleu_clair"], C["bleu"])
    y = cards(d, y, "5. Ce qui est nouveau pour la BI (B32 à B37)", NOUVEAU, C["jaune_clair"],
              C["jaune_bord"])
    y = rows(d, y, "6. Ce que le document ne dit pas (B38 à B41)", ["Remarque"], [2365], MANQUE,
             C["gris"], C["gris_bord"])
    cards(d, y, "7. Hors de notre périmètre pour l'instant : recouvrement, finance, "
                "indicateurs", HORS, C["section"], C["doux"])
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
