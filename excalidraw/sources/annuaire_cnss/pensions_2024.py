"""
Annuaire statistique de la CNSS 2024 (n° 32) · Les pensions : notre base face aux chiffres
publiés (état au 30/10/2026).

Premier schéma du dossier « annuaire cnss », qui regroupe les analyses faites sur l'annuaire de
la CNSS. Il reprend la comparaison de analyses/comparaison_annuaire_cnss_2024 (requêtes A01 à
A08) : le tableau 18 comparé, les contrôles sur les tableaux 19 à 23, ce que la comparaison dit
de nos règles, et ce qui reste à faire.

Usage :
    python3 excalidraw/sources/annuaire_cnss/pensions_2024.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import WIDTH, height_of, wrap  # noqa: E402

OUTPUT = ROOT / "schemas" / "annuaire cnss" / "pensions_2024.excalidraw"
INNER = WIDTH - 50

KEY_POINTS = [
    "C'est la même base que celle de l'annuaire : mêmes types de pension, mêmes directions, "
    "montants payés à 1 ou 2 % près. Les chiffres de 2020 à 2024 sont exploitables.",
    "La CNSS compte un stock au 31 décembre : les pensions en cours ce jour-là, titulaire en vie, "
    "payées ou non. Pour les orphelins, seulement les moins de 25 ans.",
    "Un nouveau pensionné est compté l'année où sa pension est saisie, pas celle de sa "
    "notification.",
    "Les montants publiés sont les montants payés (débours de paiement au statut D ou P).",
    "On ne retombe jamais à l'unité près : notre base est une copie au 02/02/2026, la CNSS a "
    "compté sur la base de l'époque. Les veuves et les orphelins décrochent depuis 2023 (partie "
    "4).",
]

YEARS = [2020, 2021, 2022, 2023, 2024]
# (type de pension, notre chiffre, chiffre publié) pour chaque année
TABLE_18 = [
    ("Retraite vieillesse", [25246, 26558, 28037, 29230, 31180], [25161, 26415, 27873, 29042, 30730]),
    ("Veuves", [19643, 20300, 20861, 19945, 20369], [19309, 19964, 20734, 20832, 21438]),
    ("Veufs", [236, 276, 304, 324, 356], [225, 249, 274, 298, 329]),
    ("Orphelins (moins de 25 ans)", [9705, 9558, 9296, 8123, 7995], [8982, 8789, 8667, 8687, 9478]),
    ("Invalidité", [111, 109, 119, 121, 125], [114, 111, 115, 116, 119]),
    ("Anticipée", [70, 70, 78, 78, 87], [69, 66, 68, 75, 78]),
    ("Ascendants", [58, 60, 65, 59, 59], [48, 53, 59, 54, 59]),
]
GAPS = {
    "proche": (C["vert_moyen"], C["vert"], "écart de 2 % au plus, ou de 5 unités au plus"),
    "moyen": (C["jaune"], C["jaune_bord"], "écart de 2 à 5 %, ou de 6 à 15 unités"),
    "loin": (C["rouge_moyen"], C["rouge"], "écart de plus de 5 % et de plus de 15 unités"),
}

CONTROLS = [
    ("Tableau 22 · par direction régionale", "A02",
     "Retraite vieillesse, nous / publié :\n"
     "DRO 20 352 / 20 067 · DRB 7 638 / 7 538\n"
     "DRN 1 366 / 1 340 · DRF 1 007 / 968 · DRD 818 / 817\n"
     "Ascendants : justes dans les cinq directions.\n"
     "Veuves : il en manque à la DRO (−403) et à la DRB (−617).",
     "La direction du dossier est celle de la CNSS."),
    ("Tableau 23 · nouveaux pensionnés de 2024", "A03",
     "Publié : 4 037. Selon la date retenue :\n"
     "saisie de la pension 4 090 (+1,3 %)\n"
     "notification 3 888 (−3,7 %) · date d'effet 4 235\n"
     "premier paiement 3 616\n"
     "Par la saisie : vieillesse 2 520 pour 2 486, veuves 740\n"
     "pour 732, orphelins 779 pour 769, ascendants 7 pour 7.",
     "La CNSS compte un nouveau pensionné à la saisie."),
    ("Montants · tableaux 19 à 21 et branche", "A01, A05",
     "Payé en 2024, nous / publié :\n"
     "vieillesse 38,50 Md / 38,24 Md (+0,7 %)\n"
     "veuves, veufs, orphelins, ascendants 7,22 Md / 7,05 Md\n"
     "(+2,5 %) · invalidité 116,1 M / 113,6 M (+2,2 %)\n"
     "branche des pensions 47,24 Md / 46,63 Md (+1,3 %)",
     "La CNSS publie les montants payés."),
]

VERDICTS = {
    "Confirmée": (C["vert_moyen"], C["vert"]),
    "Corrigée": (C["orange_moyen"], C["orange"]),
    "Revue": (C["jaune"], C["jaune_bord"]),
    "Pas testée": (C["gris"], C["gris_bord"]),
}
RULES = [
    ("R1, R2 · branches, code → ligne", "Confirmée",
     "les 7 lignes du tableau 18 sont nos 7 codes de pension ; les petites lignes tombent à "
     "quelques unités près"),
    ("R3 · un paiement = débours PM au statut D ou P", "Confirmée",
     "montants à 0,7 à 2,5 % près ; branche des pensions à 1,3 % près"),
    ("R4 · un paiement compte au mois payé", "Confirmée",
     "les montants de l'année ainsi calculés retombent sur l'annuaire"),
    ("R5 · on compte l'orphelin, pas le tuteur", "Confirmée",
     "le tableau 20 compte des orphelins de 0 à 24 ans ; les allocataires ne sont pas testés"),
    ("A1 · un accessoire ne fait pas un pensionné", "Confirmée",
     "le tableau 18 ne compte que les 7 types de pension ; les accessoires sont dans le montant de "
     "la branche"),
    ("Direction régionale = celle du dossier", "Confirmée",
     "ascendants justes dans les cinq directions ; vieillesse à 0 à 4 % près"),
    ("R6 · le pensionné au 31 décembre", "Corrigée",
     "avant : pension active et payée au 4e trimestre, soit 8 % de retraités de moins que "
     "l'annuaire. Maintenant : pension en cours au 31/12, titulaire en vie, payée ou non ; "
     "orphelins de moins de 25 ans"),
    ("R7 · le nouveau pensionné", "Revue",
     "avant : pension notifiée dans l'année (−3,7 %). Maintenant : pension saisie dans l'année "
     "(+1,3 %), à confirmer avec la direction des prestations"),
    ("R8 · l'âge ; R9 · les services passés", "Pas testée",
     "la requête par sexe et par âge (A04) est à relancer"),
    ("D1 à D14 (dossiers T25 à T27), I1, C1", "Pas testée",
     "le tableau 18 ne parle que de pensionnés : rien à en tirer pour les dossiers, les indus ou "
     "les contrôles"),
]

GAPS_TEXT = [
    "Notre base est une copie au 02/02/2026 ; la CNSS a compté sur la base de l'époque. On ne "
    "retombera jamais à l'unité près.",
    "En plus chez nous : des pensions créées après la publication avec un effet rétroactif "
    "(vieillesse : +85 pour 2020, +450 pour 2024).",
    "En moins chez nous depuis 2023 : des veuves (−1 069 en 2024) et des orphelins (−1 483). "
    "Hypothèse : des radiations datées de 2023, saisies après la publication, surtout à la DRO et "
    "à la DRB. Non vérifiée.",
    "Orphelins : la CNSS ne compte que les moins de 25 ans ; notre base garde près de 6 000 "
    "pensions d'orphelins plus âgés, jamais radiées.",
]
TODO_TEXT = [
    "A04 : relancer (répartition par sexe et montants des tableaux 19 à 21).",
    "A06 à A08 : exécuter, pour vérifier l'hypothèse des radiations saisies après coup et l'âge "
    "des orphelins.",
    "Autres chiffres de l'annuaire à comparer, si besoin : rentes AT/MP (2 232 bénéficiaires), "
    "victimes d'AT/MP déclarées (1 171), enfants d'allocataires (268 481), bénéficiaires d'IJ "
    "(3 893).",
]


def fmt(n):
    return f"{n:,}".replace(",", " ")


def gap(ours, published):
    diff = ours - published
    pct = diff / published * 100 if published else 0
    if abs(pct) <= 2 or abs(diff) <= 5:
        level = "proche"
    elif abs(pct) <= 5 or abs(diff) <= 15:
        level = "moyen"
    else:
        level = "loin"
    sign = "+" if diff > 0 else ("−" if diff < 0 else "")
    return level, f"{sign}{abs(pct):.1f} %".replace(".", ",")


def bullets(d, x, y, w, items, size=18, groups=()):
    """Liste à puces ; renvoie le bas."""
    for item in items:
        text = wrap("• " + item, size, w)
        d.text(x, y, text, size, groups=groups)
        y += height_of(text, size) + 10
    return y


def key_points(d, y):
    lines = [wrap(f"{i}. {p}", 18, INNER - 60) for i, p in enumerate(KEY_POINTS, 1)]
    h = 60 + sum(height_of(t, 18) + 10 for t in lines) + 10
    g = d.group()
    frame = d.box(25, y, INNER, h, bg=C["blanc"], stroke=C["encre"], stroke_width=3, groups=g)
    d.text(45, y + 16, "En bref : ce que la comparaison montre", 26, groups=g, inside=frame)
    ty = y + 60
    for t in lines:
        d.text(45, ty, t, 18, groups=g, inside=frame)
        ty += height_of(t, 18) + 10
    return y + h


def table_18(d, y):
    """Le tableau 18 : une case par type et par année, colorée selon l'écart au chiffre publié."""
    label_w, row_h, head_h = 420, 74, 50
    col_w = (INNER - label_w) / len(YEARS)
    d.box(25, y, label_w, head_h, "Type de pension", 18, bg=C["gris"], stroke=C["encre"])
    for i, year in enumerate(YEARS):
        d.box(25 + label_w + i * col_w, y, col_w, head_h, str(year), 18, bg=C["gris"],
              stroke=C["encre"])
    ry = y + head_h
    totals_ours = [sum(row[1][i] for row in TABLE_18) for i in range(len(YEARS))]
    totals_pub = [sum(row[2][i] for row in TABLE_18) for i in range(len(YEARS))]
    for label, ours, published in TABLE_18 + [("TOTAL", totals_ours, totals_pub)]:
        d.box(25, ry, label_w, row_h, label, 18, bg=C["gris"] if label == "TOTAL" else C["blanc"],
              stroke=C["encre"])
        for i in range(len(YEARS)):
            level, pct = gap(ours[i], published[i])
            bg, stroke, _ = GAPS[level]
            d.box(25 + label_w + i * col_w, ry, col_w, row_h,
                  f"{fmt(ours[i])}\npublié {fmt(published[i])} · {pct}", 18, bg=bg, stroke=stroke)
        ry += row_h
    lx = 25
    for bg, stroke, label in GAPS.values():
        d.box(lx, ry + 18, 40, 22, bg=bg, stroke=stroke)
        d.text(lx + 50, ry + 18, label, 16)
        lx += 50 + 16 * 0.56 * len(label) + 50
    note = wrap("Les autres définitions testées s'éloignent davantage : les pensions payées dans "
                "l'année donnent environ 2 000 retraités de vieillesse de moins que l'annuaire "
                "chaque année, celles payées au 4e trimestre environ 2 500 (A01).", 18, INNER)
    d.text(25, ry + 60, note, 18, C["doux"])
    return ry + 60 + height_of(note, 18)


def controls(d, y):
    gap_x = 24
    w = (INNER - 2 * gap_x) / 3
    bodies = [wrap(body, 18, w - 40) for _, _, body, _ in CONTROLS]
    concls = [wrap(c, 18, w - 40) for *_, c in CONTROLS]
    h = 100 + max(height_of(b, 18) for b in bodies) + 20 + max(height_of(c, 18) for c in concls) + 30
    for i, ((title, refs, _, _), body, concl) in enumerate(zip(CONTROLS, bodies, concls)):
        x = 25 + i * (w + gap_x)
        g = d.group()
        frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
        d.text(x + 20, y + 16, title, 22, groups=g, inside=frame)
        d.text(x + 20, y + 52, f"requête {refs}", 16, C["doux"], groups=g, inside=frame)
        d.text(x + 20, y + 90, body, 18, groups=g, inside=frame)
        cy = y + 90 + max(height_of(b, 18) for b in bodies) + 20
        d.text(x + 20, cy, concl, 18, C["vert"], groups=g, inside=frame)
    return y + h


def rules(d, y):
    rule_w, verdict_w = 560, 200
    text_w = INNER - rule_w - verdict_w
    d.box(25, y, rule_w, 44, "Notre règle", 18, bg=C["gris"], stroke=C["encre"])
    d.box(25 + rule_w, y, verdict_w, 44, "Verdict", 18, bg=C["gris"], stroke=C["encre"])
    d.box(25 + rule_w + verdict_w, y, text_w, 44, "Ce qui le montre", 18, bg=C["gris"],
          stroke=C["encre"])
    ry = y + 44
    for rule, verdict, proof in RULES:
        rule_t = wrap(rule, 18, rule_w - 30)
        proof_t = wrap(proof, 18, text_w - 30)
        h = max(height_of(rule_t, 18), height_of(proof_t, 18)) + 28
        cell = d.box(25, ry, rule_w, h, bg=C["blanc"], stroke=C["encre"])
        d.text(40, ry + 14, rule_t, 18, inside=cell)
        bg, stroke = VERDICTS[verdict]
        d.box(25 + rule_w, ry, verdict_w, h, verdict, 18, bg=bg, stroke=stroke)
        cell = d.box(25 + rule_w + verdict_w, ry, text_w, h, bg=C["blanc"], stroke=C["encre"])
        d.text(40 + rule_w + verdict_w, ry + 14, proof_t, 18, inside=cell)
        ry += h
    note = wrap("Les règles R1 à R7 ont été mises à jour le 30/10/2026 dans "
                "analyses/prestations/README.md et dans le schéma prestations/regles_prestations.",
                18, INNER)
    d.text(25, ry + 20, note, 18, C["doux"])
    return ry + 20 + height_of(note, 18)


def gaps_and_todo(d, y):
    w = (INNER - 24) / 2
    blocks = [("Pourquoi il reste des écarts", GAPS_TEXT, C["jaune_clair"], C["jaune_bord"]),
              ("Ce qui reste à faire", TODO_TEXT, C["gris"], C["gris_bord"])]
    heights = []
    for _, items, _, _ in blocks:
        heights.append(sum(height_of(wrap("• " + t, 18, w - 50), 18) + 10 for t in items))
    h = 70 + max(heights) + 10
    for i, (title, items, bg, stroke) in enumerate(blocks):
        x = 25 + i * (w + 24)
        g = d.group()
        d.box(x, y, w, h, bg=bg, stroke=stroke, groups=g)
        d.text(x + 20, y + 16, title, 24, groups=g)
        bullets(d, x + 20, y + 64, w - 50, items, groups=g)
    return y + h


def section(d, y, title, subtitle, build_part):
    sec = d.section(0, y, WIDTH, 0, title)
    top = y + 70
    if subtitle:
        text = wrap(subtitle, 18, INNER)
        d.text(25, top, text, 18, C["doux"])
        top += height_of(text, 18) + 20
    bottom = build_part(top)
    sec["height"] = bottom + 30 - y
    return bottom + 30 + 40


def build():
    d = Diagram(seed=2024)
    d.text(0, 0, "Annuaire CNSS 2024 · Les pensions : notre base face aux chiffres publiés", 48)
    d.text(0, 66, "Annuaire statistique de la CNSS n° 32, année 2024, tableaux 18 à 23 (p. 16 à "
                  "18) · requêtes A01 à A08 dans analyses/comparaison_annuaire_cnss_2024 · état au "
                  "30/10/2026", 22, C["doux"])
    y = key_points(d, 120) + 40
    y = section(d, y, "1. Le tableau 18 comparé : bénéficiaires de pension de 2020 à 2024",
                "Dans chaque case, notre chiffre, puis le chiffre publié et l'écart. Notre chiffre : "
                "les pensions en cours au 31/12 dont le titulaire est en vie ; pour les orphelins, "
                "les moins de 25 ans, comme dans le tableau 20.",
                lambda top: table_18(d, top))
    y = section(d, y, "2. Les autres contrôles de 2024", None, lambda top: controls(d, top))
    y = section(d, y, "3. Ce que la comparaison dit de nos règles", None,
                lambda top: rules(d, top))
    section(d, y, "4. Pourquoi il reste des écarts, et ce qui reste à faire", None,
            lambda top: gaps_and_todo(d, top))
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
