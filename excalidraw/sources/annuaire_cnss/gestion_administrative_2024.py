"""
Annuaire statistique de la CNSS 2024 (n° 32) · La gestion administrative : ce que les tableaux
119 à 126 apprennent sur nos règles GRH (état au 08/11/2026).

Demande de l'utilisateur (08/11/2026) : dans le tableau du mouvement du personnel, la remise en
activité est une entrée et la disponibilité une sortie ; est-ce la réponse à notre question
(QG6) ? Et quelles autres règles de la gestion administrative ces tableaux donnent-ils ?
1. la réponse : le tableau 121 recopié, avec les contrôles calculés ici ;
2. les autres règles, tableau par tableau (T58 à T65) ;
3. ce que je propose d'intégrer, et ce qui reste ouvert.

Rangée avec les autres analyses de l'annuaire de la CNSS. Ses conclusions ont été intégrées le
08/11/2026, à la demande de l'utilisateur : disponibilité hors effectif dans T58 et T60, règles,
questions, fiches et schémas GRH mis à jour.
Les chiffres sont recopiés de cnss_files/Annuaire_2024_CNSS_BURKINA-FASO.pdf, p. 89 à 94
(pdftotext -layout) ; ceux de la base viennent des lots GRH (analyses/grh).

Usage :
    python3 excalidraw/sources/annuaire_cnss/gestion_administrative_2024.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402
from fiche_cipres import WIDTH, height_of, wrap  # noqa: E402

OUTPUT = ROOT / "schemas" / "annuaire cnss" / "gestion_administrative_2024.excalidraw"
INNER = WIDTH - 50

# ---------------------------------------------------------------- le tableau 121 (p. 91)
ENTREES = ["Recrutement", "Remise en activité", "Détachement", "Autres"]
SORTIES = ["Retraite", "Décès", "Licenciement", "Démission", "Disponibilité",
           "Fin de détachement", "Autres"]
# année, début, 4 entrées, 7 sorties, fin
T121 = [
    (2015, 938, (42, 1, 2, 0), (51, 3, 1, 0, 0, 2, 0), 926),
    (2016, 926, (81, 0, 1, 0), (39, 2, 0, 1, 1, 1, 1), 963),
    (2017, 963, (8, 0, 0, 0), (43, 2, 0, 0, 0, 0, 0), 926),
    (2018, 926, (0, 0, 0, 0), (45, 3, 0, 0, 1, 0, 0), 877),
    (2019, 877, (0, 0, 0, 84), (35, 3, 0, 0, 3, 1, 1), 918),
    (2020, 918, (0, 3, 0, 0), (43, 4, 0, 0, 5, 0, 0), 869),
    (2021, 869, (0, 2, 0, 0), (31, 2, 0, 0, 3, 0, 1), 834),
    (2022, 834, (0, 3, 0, 2), (26, 3, 1, 0, 2, 0, 1), 806),
    (2023, 806, (0, 5, 2, 0), (32, 5, 0, 1, 4, 2, 2), 767),
    (2024, 767, (104, 6, 1, 0), (23, 3, 1, 2, 7, 0, 84), 758),
]
# le tableau 126 (p. 94) : effectif par catégorie détaillée, toutes directions
T126 = {"1re A": 15, "1re B": 6, "2e A": 92, "2e B": 64, "3e A": 126, "3e B": 82, "4e A": 27,
        "4e B": 175, "4e C": 23, "5e A": 59, "5e B": 27, "6e A": 35, "6e B": 26, "HC": 1}
GROUPES = [("Employés", ["1re A", "1re B", "2e A", "2e B"], 177),
           ("Agents de maîtrise", ["3e A", "3e B"], 208),
           ("Cadres moyens", ["4e A", "4e B", "4e C", "5e A", "5e B"], 311),
           ("Cadres supérieurs", ["6e A", "6e B", "HC"], 62)]   # dernier : le tableau 119

VERDICTS = {
    "tranche": (C["bleu_moyen"], C["bleu"], "tranche une question"),
    "confirme": (C["vert_moyen"], C["vert"], "confirme une règle"),
    "precise": (C["jaune"], C["jaune_bord"], "précise une question"),
    "ecart": (C["rouge_moyen"], C["rouge"], "écart à expliquer"),
}


def controles():
    """Contrôle de chaque ligne du tableau 121, et stock des agents en disponibilité (cumul des
    disponibilités moins les remises en activité depuis 2015)."""
    res, stock, fin_avant = [], 0, None
    for an, debut, e, s, fin in T121:
        stock += s[4] - e[1]
        res.append((debut + sum(e) - sum(s) - fin, fin_avant in (None, debut), stock))
        fin_avant = fin
    return res


CTRL = controles()
assert all(ecart == 0 and suite for ecart, suite, _ in CTRL), "tableau 121 mal recopié"
for nom, cles, total in GROUPES:
    assert sum(T126[k] for k in cles) == total, nom
assert sum(T126.values()) == 758

PREUVES = [
    ("1. Les colonnes",
     "« Remise en activité » est rangée sous les entrées, « Disponibilité » sous les sorties, "
     "à côté du recrutement et de la retraite."),
    ("2. L'effectif suit",
     "Chaque année, de 2015 à 2024 : début + entrées − sorties = fin (colonne contrôle, "
     "calculée). L'effectif publié, 758 fin 2024, ne compte donc pas les agents en "
     "disponibilité : ils sont sortis."),
    ("3. Le même ordre de grandeur que la base",
     f"Disponibilités moins remises, cumulées depuis 2015 : +{CTRL[-1][2]} fin 2024, plus les "
     "agents déjà en disponibilité avant 2015 (au moins un : −1 en 2015). La base trouve 6 "
     "agents en disponibilité au 31/12/2024 (décision 07 sans 18 après, lot 24)."),
]

VERDICT = ("OUI, ÇA RÉPOND À QG6 (et à la question 2 de T60). Pour la CNSS, un agent mis en "
           "disponibilité sort de l'effectif ; il y rentre à sa remise en activité. "
           "L'interlocuteur de la CNSS l'avait dit à la réunion du 03/11 (M11). Le recueil des "
           "besoins, l'atelier de Kaya et l'Annuaire CIPRES n'en disent rien : c'est la seule "
           "règle écrite par la CNSS sur ce point, et les décisions 07 et 18 de la base la "
           "suivent.\n"
           "Chez nous : le réglage disponibilite passe à HORS_EFFECTIF, dans la CTE parametres "
           "des deux requêtes, T58 et T60 (fait le 08/11/2026, à ta demande).")

EFFET = ("CE QUE ÇA CHANGE DANS NOS CHIFFRES (2024, permanents)\n"
         "· Effectif fin 2024 : 741 au lieu de 747, les 6 agents en disponibilité sortent ; "
         "l'écart avec les 758 publiés passe de 11 à 17, à expliquer.\n"
         "· T60 2024 : environ 5 sorties « disponibilité » et 4 entrées « remise en activité » "
         "(décisions 07 et 18) ; l'annuaire en publie 7 et 6.\n"
         "· Les colonnes actifs et inactifs de l'effectif (G13) n'ont plus d'inactif : "
         "l'inactif était justement l'agent en disponibilité.")

# ---------------------------------------------------------------- 2. les autres règles
# (titre, tableau CIPRES, ce qu'il montre, ce que j'en tire, [(verdict, étiquette)])
REGLES = [
    ("Tableau 119 · tout le personnel, par statut", "T58",
     "758 permanents et 46 contractuels, 804 en tout, au 31/12/2024. Chaque contractuel a une "
     "catégorie : 18, 13, 10 et 5. Deux statuts seulement : ni agent de la fonction publique, "
     "ni agent affecté.",
     ["Le tableau des temporaires se fait avec les mêmes catégories ; le recueil demande T58 "
      "par type d'employé (RH ligne 1).",
      "Qualité A seulement (G1).",
      "46 = les 46 de la base (lot 19) ; avec G7, sans durée de contrat, la requête de "
      "l'effectif peut en trouver plus : à comparer à l'exécution."],
     [("confirme", "G1, G7"), ("ecart", "G7 à vérifier")]),
    ("Tableau 126 · les catégories détaillées", "T58, T65",
     "\n".join(f"{nom} : {' + '.join(f'{k} {T126[k]}' for k in cles)} = {total}"
               for nom, cles, total in GROUPES)
     + "\nLes quatre totaux sont ceux du tableau 119, à l'unité près.",
     ["Les quatre groupes de la CNSS, exactement : 1re et 2e employés, 3e maîtrise, 4e et 5e "
      "cadres moyens, 6e et HC cadres supérieurs.",
      "La 5e est bien « cadre moyen » (QG1, déjà tranchée par le recueil).",
      "HC compte en cadres supérieurs : c'était une proposition BI (G3), c'est confirmé."],
     [("confirme", "QG1, G3"), ("tranche", "HC")]),
    ("Tableau 120 · catégorie, âge et sexe", "T58",
     "La grille d'âge de l'Annuaire CIPRES, de moins de 20 ans à 60 ans et plus. Aucun "
     "permanent de 60 ans ou plus. 412 hommes et 346 femmes. La base (742, lot 18) : 400 et "
     "342.",
     ["L'âge au 31/12, en années pleines (G4).",
      "Personne au-delà de 59 ans : sans doute l'âge de la retraite, à vérifier dans la base."],
     [("confirme", "G4")]),
    ("Tableau 121 · le détail des « autres »", "T60",
     "Entrées : remise en activité, détachement, autres. Sorties : licenciement, démission, "
     "disponibilité, fin de détachement, autres. Ni fin de contrat ni mise à disposition pour "
     "les permanents.",
     ["La liste des « autres » est celle de la CNSS (question 1 de T60) : confirmée.",
      "La mise à disposition n'apparaît pas comme une sortie (question 6) : notre proposition "
      "« non » se renforce.",
      "Les détachements sont comptés (2 en 2023, 1 en 2024) : où sont-ils saisis ? (QG7, "
      "toujours ouverte)."],
     [("confirme", "question 1"), ("precise", "questions 5, 6")]),
    ("Tableau 121 · le reste", "T60",
     "Le début d'une année est la fin de l'année d'avant, de 2015 à 2024. Le tableau ne suit "
     "que les permanents. 104 recrutements en 2024, comme la base. Les 84 de 2019 entrent en "
     "« autres », pas en recrutement, et sortent en « autres » en 2024.",
     ["Les deux photos de G14 : confirmées.",
      "Filtre de nature PERMANENT par défaut pour T60.",
      "QG2 se précise : les 84 ne sont pas des recrutés ; par quel acte sont-ils entrés en "
      "2019, et sortis en 2024 ?"],
     [("confirme", "G14"), ("precise", "QG2")]),
    ("Tableau 122 · l'ancienneté", "T61",
     "103 agents de moins d'un an, pour 104 recrutés en 2024 ; 1 seul de 1 à 4 ans, car aucun "
     "recrutement de 2018 à 2023 (tableau 121). 232 de 10 à 14 ans. La base (ancien calcul) : "
     "101 et 224.",
     ["L'ancienneté part de l'entrée : un recruté de 2024 a moins d'un an.",
      "La grille de l'Annuaire CIPRES (moins de 1 an … 40 ans et plus) ; G5 reste à "
      "recalculer dans la base."],
     [("confirme", "G5")]),
    ("Tableau 123 · les formés", "T62",
     "480 permanents en formation ou formés : 132, 142, 118, 88. Finalité : 6 avec promotion, "
     "474 sans. Pas de sexe. Mais 88 cadres supérieurs formés pour 62 cadres supérieurs en "
     "poste.",
     ["T62 ne compte que les permanents : confirmé.",
      "La finalité avec ou sans promotion de l'Annuaire CIPRES : confirmée ; la base en "
      "trouve 2 avec promotion.",
      "Les 480 ne sont pas des agents distincts, au moins pour les cadres supérieurs : sans "
      "doute des participations. La base : 409 agents, 524 inscriptions (QG4)."],
     [("confirme", "permanents"), ("precise", "QG4")]),
    ("Tableau 124 · le taux d'encadrement", "T65",
     "Cadres 373 = cadres moyens 311 + cadres supérieurs 62, sur 758 permanents : 49 % en "
     "2024 ; 41 % de 2020 à 2023. Le saut de 2024 (+57 cadres, −66 non cadres) suit l'année "
     "des 104 recrutements et des 84 sorties.",
     ["Cadres = catégories 4, 5, 6 et HC, sur l'effectif permanent (G6).",
      "La base : 48,8 % en 2024 (sur 742)."],
     [("confirme", "G6")]),
    ("Tableaux 125 et 126 · par direction", "T58, T59",
     "Les permanents par direction : 20 directions, du DG aux directions régionales (DRO 233, "
     "DRB 157…), par âge, sexe et catégorie. Aucun tableau par domaine d'activité (T59).",
     ["La déclinaison par direction que demande le recueil (RH lignes 1 à 4).",
      "T59 n'est pas publié par domaine : la correspondance unité → domaine reste à demander "
      "(G12) ; elle peut partir de ces 20 directions."],
     [("confirme", "déclinaison"), ("precise", "G12")]),
]

PROPOSITIONS = [
    "QG6 tranchée : réglage disponibilite = HORS_EFFECTIF dans T58 et T60 ; la question 2 de "
    "T60 et la question Q4 de la fiche T58 se ferment.",
    "G3 : HC en cadres supérieurs, confirmé par le tableau 126.",
    "Question 1 de T60 (la liste des « autres ») : confirmée par le tableau 121.",
    "Question 6 de T60 (mise à disposition) : pas une sortie, aucune colonne dans le tableau 121.",
    "QG4 (T62) : demander à la CNSS si les 480 sont des participations ; 88 cadres supérieurs "
    "formés pour 62 en poste.",
    "QG2 : les 84 de 2019 sont entrés en « autres », pas en recrutement : par quel acte ?",
    "G12 (T59) : partir des 20 directions des tableaux 125 et 126 pour la correspondance.",
]
OUVERTES = ("Restent ouvertes : QG3 (date de décès), QG7 (où sont saisis les détachements), et "
            "les questions 4 (date d'entrée), 7 (entré et sorti la même année) et 8 (mutations "
            "dans une agence) de T60.")


def build():
    d = Diagram(seed=124)

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2):
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b)
        return b

    def pastille(x, y, genre, label, size=16):
        """Étiquette de verdict ; renvoie sa largeur."""
        bg, bord, _ = VERDICTS[genre]
        w = text_size(label, size)[0] + 26
        d.box(x, y, w, 30, label, size, bg=bg, stroke=bord)
        return w

    # ------------------------------------------------------------------ titre et légende
    d.text(0, 0, "Annuaire CNSS 2024 · La gestion administrative, face à nos règles", 48)
    d.text(0, 66, "Annuaire statistique de la CNSS n° 32, partie IX, tableaux 119 à 126 (p. 89 à "
                  "94) : les tableaux du personnel au format CIPRES\nce qu'ils disent de nos "
                  "règles GRH et de nos questions ouvertes · conclusions intégrées le 08/11/2026", 22, C["doux"])
    lx = 25
    for genre, (bg, bord, label) in VERDICTS.items():
        d.box(lx, 130, 40, 22, bg=bg, stroke=bord)
        d.text(lx + 50, 130, label, 17)
        lx += 50 + text_size(label, 17)[0] + 30

    # ------------------------------------------------------------------ 1. la réponse
    s1 = 180
    sec1 = d.section(0, s1, WIDTH, 0, "1. Ta question : la disponibilité et la remise en "
                                      "activité (tableau 121, T60)")
    d.text(25, s1 + 70, "Tableau 121 · Mouvement du personnel permanent de la CNSS (p. 91), "
                        "recopié ; les deux dernières colonnes sont calculées ici (le cumul part "
                        "du 1er janvier 2015 : −1 en 2015, l'agent remis en activité était en "
                        "disponibilité avant)", 20, C["doux"])
    colonnes = (["Année", "Effectif début"] + ENTREES + SORTIES
                + ["Effectif fin", "Début + E − S − fin", "Dispo − remises (cumul)"])
    n = len(colonnes)
    cw = INNER / n
    top = s1 + 110
    groupe_h, tete_h, ligne_h = 40, 74, 38
    xs = [25 + i * cw for i in range(n)]
    d.box(xs[2], top, 4 * cw, groupe_h, "ENTRÉES", 18, bg=C["vert_clair"], stroke=C["vert"])
    d.box(xs[6], top, 7 * cw, groupe_h, "SORTIES", 18, bg=C["orange_clair"], stroke=C["orange"])
    d.box(xs[14], top, 2 * cw, groupe_h, "CALCULÉ ICI", 18, bg=C["gris"], stroke=C["gris_bord"])
    for i, titre in enumerate(colonnes):
        if titre == "Remise en activité":
            fond = (C["vert_moyen"], C["vert"])
        elif titre == "Disponibilité":
            fond = (C["orange_moyen"], C["orange"])
        else:
            fond = (C["gris"], C["encre"])
        d.box(xs[i], top + groupe_h, cw, tete_h, wrap(titre, 15, cw - 16), 15, bg=fond[0],
              stroke=fond[1])
    y = top + groupe_h + tete_h
    for (an, debut, e, s, fin), (ecart, _, stock) in zip(T121, CTRL):
        valeurs = [an, debut, *e, *s, fin, ecart, stock]
        for i, v in enumerate(valeurs):
            titre = colonnes[i]
            if titre == "Remise en activité":
                fond = C["vert_clair"]
            elif titre == "Disponibilité":
                fond = C["orange_clair"]
            elif i >= 14:
                fond = C["section"]
            else:
                fond = C["blanc"]
            d.box(xs[i], y, cw, ligne_h, str(v), 16, bg=fond, stroke=C["gris_bord"],
                  stroke_width=1)
        y += ligne_h
    bas_tableau = y

    # trois preuves, côte à côte
    py = bas_tableau + 30
    pw = (INNER - 2 * 30) / 3
    textes = [wrap(t, 19, pw - 44) for _, t in PREUVES]
    ph = max(54 + height_of(t, 19) + 20 for t in textes)
    for i, ((titre, _), texte) in enumerate(zip(PREUVES, textes)):
        x = 25 + i * (pw + 30)
        g = d.group()
        b = d.box(x, py, pw, ph, bg=C["blanc"], stroke=C["bleu"], stroke_width=2, groups=g)
        d.text(x + 22, py + 16, titre, 22, groups=g, inside=b)
        d.text(x + 22, py + 54, texte, 19, groups=g, inside=b)
    vy = py + ph + 30
    verdict = wrap(VERDICT, 21, INNER - 50)
    vh = height_of(verdict, 21) + 34
    bloc(25, vy, INNER, vh, verdict, 21, couleurs=(C["bleu_clair"], C["bleu"]), sw=3)
    ey = vy + vh + 24
    effet = wrap(EFFET, 19, INNER - 50)
    eh = height_of(effet, 19) + 34
    bloc(25, ey, INNER, eh, effet, 19, couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec1["height"] = ey + eh + 30 - s1

    # ------------------------------------------------------------------ 2. les autres règles
    s2 = sec1["y"] + sec1["height"] + 50
    sec2 = d.section(0, s2, WIDTH, 0, "2. Les autres règles, tableau par tableau")
    par_ligne, gap = 3, 30
    w = (INNER - (par_ligne - 1) * gap) / par_ligne
    inner = w - 44

    def mise_en_page(regle):
        titre, cipres, montre, tire, _ = regle
        t_titre = wrap(titre, 22, inner - 90)
        t_montre = wrap(montre, 18, inner)
        t_tire = [wrap(x, 18, inner - 22) for x in tire]
        h = (16 + height_of(t_titre, 22) + 14 + 24 + height_of(t_montre, 18) + 18 + 24
             + sum(height_of(t, 18) + 8 for t in t_tire) + 14 + 30 + 18)
        return t_titre, t_montre, t_tire, h

    y = s2 + 75
    for debut in range(0, len(REGLES), par_ligne):
        rang = REGLES[debut:debut + par_ligne]
        poses = [mise_en_page(r) for r in rang]
        h = max(p[3] for p in poses)
        for i, (regle, (t_titre, t_montre, t_tire, _)) in enumerate(zip(rang, poses)):
            x = 25 + i * (w + gap)
            g = d.group()
            cadre = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
            d.text(x + 22, y + 16, t_titre, 22, groups=g, inside=cadre)
            wc = text_size(regle[1], 16)[0] + 22
            d.box(x + w - 22 - wc, y + 14, wc, 30, regle[1], 16, bg=C["violet"],
                  stroke=C["encre"], groups=g)
            cy = y + 16 + height_of(t_titre, 22) + 14
            d.text(x + 22, cy, "Ce que montre l'annuaire", 16, C["doux"], groups=g, inside=cadre)
            cy += 24
            d.text(x + 22, cy, t_montre, 18, groups=g, inside=cadre)
            cy += height_of(t_montre, 18) + 18
            d.text(x + 22, cy, "Ce que j'en tire", 16, C["doux"], groups=g, inside=cadre)
            cy += 24
            for t in t_tire:
                d.text(x + 22, cy, "•", 18, groups=g, inside=cadre)
                d.text(x + 44, cy, t, 18, groups=g, inside=cadre)
                cy += height_of(t, 18) + 8
            px = x + 22
            for genre, label in regle[4]:
                px += pastille(px, y + h - 18 - 30, genre, label) + 10
        y += h + gap
    sec2["height"] = y - gap + 30 - s2

    # ------------------------------------------------------------------ 3. propositions
    s3 = sec2["y"] + sec2["height"] + 50
    sec3 = d.section(0, s3, WIDTH, 0, "3. Ce qui est intégré dans nos règles (08/11/2026), et "
                                      "ce qui reste ouvert")
    lignes = [wrap(f"{i}. {p}", 20, INNER - 70) for i, p in enumerate(PROPOSITIONS, 1)]
    hp = 20 + sum(height_of(t, 20) + 10 for t in lignes) + 10
    g = d.group()
    cadre = d.box(25, s3 + 75, INNER, hp, bg=C["vert_clair"], stroke=C["vert"], groups=g)
    cy = s3 + 75 + 20
    for t in lignes:
        d.text(50, cy, t, 20, groups=g, inside=cadre)
        cy += height_of(t, 20) + 10
    oy = s3 + 75 + hp + 20
    ouvertes = wrap(OUVERTES + "\nSource : l'annuaire CNSS 2024 est produit par la CNSS (DPC). "
                    "Il ne prime pas sur le recueil des besoins ; ici, il comble un silence du "
                    "recueil, de Kaya et de l'Annuaire CIPRES.", 19, INNER - 50)
    ho = height_of(ouvertes, 19) + 34
    bloc(25, oy, INNER, ho, ouvertes, 19, couleurs=(C["gris"], C["gris_bord"]))
    sec3["height"] = oy + ho + 30 - s3
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
