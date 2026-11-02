"""
Fiche de validation de T42 « Répartition des nouveaux pensionnés de vieillesse, montants et
services passés de la branche PVID par tranches d'âges et par sexe » (état au 01/11/2026) :
fiche pilote de la famille T42 à T49 (nouveaux pensionnés et crédirentiers).

Même gabarit que T32 : la maquette, six retraités fictifs sur un calendrier, les tranches d'âge,
les huit règles avec leur SQL et cinq questions. Les règles valent pour T43 à T49, aux
différences près dites dans la requête commune (périmètre, grille d'âge ; services passés pour
T42 seulement, degré d'IPP pour T48).

Sources : Annuaire CIPRES (p. 15, 42, 62) ; atelier de Kaya 2017 (T42 : services passés en
mois, part CNSS de la coordination) ; annuaire de la CNSS 2024 (tableaux 23, 25) ;
analyses/prestations (A03 ; lot 7, P82 à P86, P88 à P90) ; t42_t49_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t42_nouveaux_retraites.py [--force]
"""

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import (WIDTH, banner, cards_grid, cards_with_sql, chip, height_of,  # noqa: E402
                          line_cards, mockup, timeline, wrap)

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T42_nouveaux_retraites.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "C'est la fiche pilote de T42 à T49 : les mêmes règles servent aux nouveaux veufs, "
          "orphelins, crédirentiers d'accident du travail et bénéficiaires de rente viagère. "
          "Valider T42, c'est valider la famille.")

# sort d'un retraité fictif dans le calendrier : compté ou non dans T42 de 2025
SORTIES = {
    "compte": (C["vert_moyen"], C["vert"], "compté dans T42 de 2025"),
    "exclu": (C["gris"], C["gris_bord"], "pas compté dans T42 de 2025"),
    "question": (C["jaune"], C["jaune_bord"], "selon la réponse à une question"),
}

EXAMPLES = [
    dict(nom="Retraité A", detail="pension saisie en 2025", debut=date(2025, 3, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="pension saisie le 15/03/2025, effet au 01/03/2025",
         resultat="Dans T42 de 2025 : compté, dans sa tranche d'âge ; montant : les mois de 2025 "
                  "payés ; services passés : ses mois cotisés."),
    dict(nom="Retraité B", detail="effet en 2024, saisie en 2025", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="effet au 01/11/2024, pension saisie en février 2025, payée avec un rappel",
         resultat="Dans T42 de 2025 : compté, l'année de la saisie ; montant : les mois de 2025, "
                  "le rappel de 2024 à part (question Q4)."),
    dict(nom="Retraité C", detail="pension révisée", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="exclu",
         dates="retraité depuis 2018, pension reliquidée en juin 2025",
         resultat="Dans T42 de 2025 : pas compté, ce n'est pas une entrée (règle 3, question Q2) ; "
                  "il reste dans T32."),
    dict(nom="Retraité D", detail="décédé en cours d'année", debut=date(2025, 4, 1),
         fin=date(2025, 9, 30), sortie="compte",
         dates="pension saisie en avril 2025, décédé en septembre",
         resultat="Dans T42 de 2025 : compté, c'est une entrée de l'année (règle 3) ; absent de "
                  "T32 au 31/12."),
    dict(nom="Retraité E", detail="coordination", debut=date(2025, 5, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="pension saisie en mai 2025, payée par une autre caisse (COOR_AC)",
         resultat="Dans T42 de 2025 : compté, sans montant ; services passés : la seule part "
                  "CNSS (règle 8, question Q5)."),
    dict(nom="Retraité F", detail="saisi en 2025, notifié en 2026", debut=date(2025, 7, 1),
         fin=date(2026, 2, 28), sortie="question",
         dates="effet au 01/07/2025, saisie le 18/12/2025, notifiée le 12/01/2026",
         resultat="Dans T42 de 2025 si l'entrée est la saisie (règle 2) ; de 2026 si c'est la "
                  "notification (question Q1)."),
]

LINES = [
    (1, "Moins de 50 ans", "les retraités partis très tôt",
     "aucun parmi les nouveaux de 2024 (P84)", "Données"),
    (2, "De 50 à 99 ans", "dix tranches de 5 ans : 50-54, 55-59… 95-99",
     "presque tous entre 55 et 64 ans : 2 392 sur 2 510 en 2024 (P84)", "Données"),
    (3, "100 ans et plus", "la dernière tranche de la maquette",
     "vide en pratique : aucun nouveau de 2024 n'avait 80 ans (P84)", "Données"),
    (4, "TOTAL", "la somme des tranches, pour les hommes et pour les femmes",
     "pas de colonne Total dans la maquette : hommes et femmes seulement", "Annuaire"),
]

RULES = [
    dict(n="1", titre="Les pensions concernées",
         regle="Les pensions de vieillesse et les pensions de retraite anticipée, comme dans T32.",
         sources=[("Annuaire", "p. 15 : T42 répartit « les nouveaux bénéficiaires de pension de "
                               "vieillesse de l'exercice »"),
                  ("Base", "deux codes : RPV, pension de vieillesse ; RPRA, pension de retraite "
                           "anticipée"),
                  ("Proposition BI", "la retraite anticipée dans T42, comme dans T32 : la "
                                     "question Q2 de la fiche T32 vaut aussi ici")]),
    dict(n="2", titre="Le nouveau retraité",
         regle="La personne dont la première pension de vieillesse est saisie dans l'exercice.",
         sources=[("Annuaire", "p. 15 : les nouveaux bénéficiaires « de l'exercice »"),
                  ("Données", "c'est ainsi que compte l'annuaire de la CNSS : 2 463 en 2024 pour "
                              "2 489 publiés (−1,0 %) ; la notification en donne 2 397 (−3,7 %) "
                              "(A03, P82)"),
                  ("Proposition BI", "la saisie ; voir la question Q1")],
         chiffre="De 2020 à 2025 : 1 891, 1 965, 2 127, 2 333, 2 463, 2 725 nouveaux retraités."),
    dict(n="3", titre="La personne comptée",
         regle="Une seule fois, l'année de son entrée, même si elle meurt ou que sa pension est "
               "arrêtée avant le 31 décembre. Une révision ou une reliquidation n'en fait pas un "
               "nouveau retraité.",
         sources=[("Données", "47 retraités ont reçu une nouvelle pension en 2024 alors qu'ils en "
                              "avaient déjà une, 220 en 2025 ; 35 nouveaux de 2024 sont sortis "
                              "avant le 31/12 (P82)"),
                  ("Proposition BI", "compter les entrées, pas les pensions ; voir la question "
                                     "Q2")]),
    dict(n="4", titre="L'âge",
         regle="L'année de l'exercice moins l'année de naissance, quelle que soit la date "
               "d'anniversaire. 120 ans ou plus : « Âge inconnu ».",
         sources=[("Annuaire", "glossaire, p. 62 : « la différence entre l'année de "
                               "constatation et l'année de naissance »"),
                  ("Données", "par tranche de 5 ans, on retombe sur le tableau 25 de l'annuaire "
                              "de la CNSS : 55-59 ans 1 450 pour 1 429, 60-64 ans 941 pour 956, "
                              "65-69 ans 89 pour 89 (P83)")],
         chiffre="Exemple : né en 1965, il a 60 ans dans T42 de 2025."),
    dict(n="5", titre="Les tranches d'âge",
         regle="Moins de 50 ans, puis 50-54, 55-59… 95-99, et 100 ans et plus.",
         sources=[("Annuaire", "p. 42 : la maquette de T42")]),
    dict(n="6", titre="Le sexe",
         regle="Hommes et femmes d'après la fiche de la personne : 1 pour les hommes, 2 pour les "
               "femmes.",
         sources=[("Données", "1 = hommes, confirmé sur les retraités au 31/12 (P75) ; mais 641 "
                              "femmes parmi les nouveaux de 2024 pour 527 publiées : voir la "
                              "question Q3 (P83)")]),
    dict(n="7", titre="Le montant",
         regle="Ce qui a été payé pour les mois de l'exercice : les débours de paiement au statut "
               "D ou P, au mois payé. Le rappel des mois d'avant l'exercice est montré à part.",
         sources=[("Annuaire", "p. 15 : « les montants servis »"),
                  ("Données", "2,86 Md pour les mois de 2024 ; le rappel ajoute 0,23 Md (8 %) ; "
                              "la coordination payée par une autre caisse n'a pas de montant "
                              "(P85)"),
                  ("Proposition BI", "les mois de l'exercice, comme T32 ; voir la question Q4")],
         chiffre="En 2024 : 2,86 Md pour les mois de 2024 des pensions saisies dans l'année."),
    dict(n="8", titre="Le total des services passés",
         regle="Les mois cotisés de la dernière pension, de 1 à 600, additionnés par tranche et "
               "par sexe. En coordination, la seule part CNSS : le total moins les mois dans "
               "l'autre caisse.",
         sources=[("Kaya 2017", "« total services passés » en nombre de mois ; pour la "
                                "coordination, « prendre uniquement la partie CNSS »"),
                  ("Données", "mois toujours remplis (P86) ; en coordination, PE_COT_AUTRE donne "
                              "les mois dans l'autre caisse, toujours moins que le total (P89, "
                              "P90)"),
                  ("Proposition BI", "le total moins PE_COT_AUTRE ; voir la question Q5")],
         chiffre="En 2024 : 270 mois en médiane, 22 ans et demi, pour les dossiers ordinaires."),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_DWH), pour
# l'année 2024, avec en commentaire le chiffre à retrouver. La règle 2 construit la liste
# nouveaux_2024, que les règles 3 à 8 réutilisent.
SQL = {
    "1": """-- les mêmes codes que T32 (TPE_CODE)
SELECT TPE_CODE, COUNT(*)
FROM   USER_DWH.PRESTATION_ESP
WHERE  TPE_CODE IN ('RPV', 'RPRA')
GROUP  BY TPE_CODE
-- RPV  : pension de vieillesse
-- RPRA : pension de retraite anticipée""",
    "2": """-- la liste des nouveaux retraités de
-- 2024 : leur première pension est saisie
-- en 2024 ; réutilisée ensuite
WITH nouveaux_2024 AS (
  SELECT IND_ID_PREST,
         MIN(PE_DATE_INSERT) AS premiere_saisie
  FROM   USER_DWH.PRESTATION_ESP
  WHERE  TPE_CODE IN ('RPV', 'RPRA')
  GROUP  BY IND_ID_PREST
  HAVING MIN(PE_DATE_INSERT)
           >= DATE '2024-01-01'
  AND    MIN(PE_DATE_INSERT)
           <  DATE '2025-01-01')
SELECT COUNT(*) FROM nouveaux_2024
-- 2024 : 2 463 (l'annuaire : 2 489)
-- de 2020 à 2025 : 1 891, 1 965, 2 127,
-- 2 333, 2 463, 2 725""",
    "3": """-- la règle 2 regroupe par personne : une
-- seule fois, même si la pension s'arrête
-- avant le 31/12 ; une révision n'entre
-- pas, car la première saisie est plus
-- ancienne. Les révisions de 2024 :
SELECT COUNT(*)
FROM   USER_DWH.PRESTATION_ESP
WHERE  TPE_CODE IN ('RPV', 'RPRA')
AND    PE_DATE_INSERT >= DATE '2024-01-01'
AND    PE_DATE_INSERT <  DATE '2025-01-01'
AND    IND_ID_PREST NOT IN
       (SELECT IND_ID_PREST FROM nouveaux_2024)
-- 18 à 47 par an jusqu'en 2024""",
    "4": """-- l'année 2024 moins l'année de naissance
SELECT 2024 - EXTRACT(YEAR FROM
         i.IND_DATE_NAISSANCE) AS age,
       COUNT(*)
FROM   nouveaux_2024 n
JOIN   USER_DWH.INDIVIDU i
       ON i.IND_ID = n.IND_ID_PREST
GROUP  BY 2024 - EXTRACT(YEAR FROM
            i.IND_DATE_NAISSANCE)
ORDER  BY 1
-- 120 ans ou plus, ou vide : âge inconnu""",
    "5": """SELECT tranche, COUNT(*)
FROM  (SELECT CASE
                WHEN age IS NULL OR age < 0
                  OR age >= 120 THEN 'Âge inconnu'
                WHEN age < 50   THEN 'Moins de 50'
                WHEN age >= 100 THEN '100 et plus'
                ELSE TO_CHAR(FLOOR(age / 5) * 5)
              END AS tranche
       FROM  (SELECT 2024 - EXTRACT(YEAR FROM
                       i.IND_DATE_NAISSANCE) AS age
              FROM   nouveaux_2024 n
              JOIN   USER_DWH.INDIVIDU i
                     ON i.IND_ID = n.IND_ID_PREST))
GROUP  BY tranche
ORDER  BY tranche
-- 2024 : 55-59 ans 1 450 (l'annuaire :
-- 1 429), 60-64 ans 941 (956)""",
    "6": """SELECT i.IND_SEXE, COUNT(*)
FROM   nouveaux_2024 n
JOIN   USER_DWH.INDIVIDU i
       ON i.IND_ID = n.IND_ID_PREST
GROUP  BY i.IND_SEXE
-- 1 hommes, 2 femmes ; 2024 : le sexe ne
-- retombe pas sur l'annuaire (641 femmes
-- pour 527 publiées)""",
    "7": """-- payé pour les mois de 2024 aux
-- nouveaux retraités
SELECT SUM(deb.DEB_MONTANT)
FROM   USER_DWH.DEBOURS deb
JOIN   USER_DWH.PRESTATION_ESP p
       ON p.PE_ID = deb.PE_ID
WHERE  p.TPE_CODE IN ('RPV', 'RPRA')
AND    p.IND_ID_PREST IN
       (SELECT IND_ID_PREST FROM nouveaux_2024)
AND    deb.DEB_TYPE = 'PM'
AND    deb.DEB_STATUT IN ('D', 'P')
AND    deb.DEB_DATE_EFFET >= DATE '2024-01-01'
AND    deb.DEB_DATE_EFFET <  DATE '2025-01-01'
-- 2024 : 2,86 Md ; le rappel des mois
-- d'avant : DEB_DATE_EFFET < 2024, à part""",
    "8": """-- les mois cotisés de la dernière pension
-- saisie ; en coordination, moins les
-- mois dans l'autre caisse (PE_COT_AUTRE)
SELECT SUM(GREATEST(p.PE_MOIS_COTISATION
             - NVL(p.PE_COT_AUTRE, 0), 0))
         AS services_passes_mois
FROM   USER_DWH.PRESTATION_ESP p
JOIN   nouveaux_2024 n
       ON n.IND_ID_PREST = p.IND_ID_PREST
WHERE  p.TPE_CODE IN ('RPV', 'RPRA')
AND    p.PE_MOIS_COTISATION BETWEEN 1 AND 600
AND    p.PE_DATE_INSERT = (
         SELECT MAX(x.PE_DATE_INSERT)
         FROM   USER_DWH.PRESTATION_ESP x
         WHERE  x.IND_ID_PREST = p.IND_ID_PREST
         AND    x.TPE_CODE IN ('RPV', 'RPRA')
         AND    x.PE_DATE_INSERT
                  < DATE '2025-01-01')
-- par tranche et par sexe : GROUP BY ;
-- 2024 : 270 mois en médiane (22 ans et
-- demi) pour les dossiers ordinaires""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_DWH), pour l'année 2024, à copier dans SQL Developer. La règle 2 "
            "construit la liste des nouveaux retraités de 2024 (nouveaux_2024) ; pour les règles "
            "3 à 8, on colle leur requête sous celle de la règle 2, à la place de sa dernière "
            "requête. Le commentaire en fin de requête donne le chiffre à retrouver. La requête "
            "complète, sur les copies de l'entrepôt, est dans "
            "analyses/prestations/t42_t49_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="L'entrée : saisie ou notification",
         regle="Un nouveau retraité de l'exercice est-il celui dont la pension est saisie dans "
               "l'année, comme dans l'annuaire de la CNSS, ou notifiée dans l'année ?",
         cas=[("La saisie", "2 463 nouveaux retraités en 2024, comme l'annuaire (2 489)."),
              ("La notification", "2 397, 4 % de moins ; une pension saisie en décembre peut "
                                  "être notifiée en janvier.")],
         sources=[("Données", "annuaire de la CNSS 2024, tableau 23 ; nos requêtes A03 et P82"),
                  ("Proposition BI", "la saisie, comme l'annuaire")]),
    dict(n="Q2", titre="Les révisions",
         regle="Un retraité dont la pension est reliquidée ou révisée est-il un nouveau "
               "retraité ?",
         cas=[("Si non", "2 463 nouveaux en 2024, 2 725 en 2025."),
              ("Si oui", "2 510 en 2024, 2 945 en 2025 : 220 de plus, qui étaient déjà "
                         "retraités.")],
         sources=[("Annuaire", "p. 15 : le tableau sert à « définir la loi des entrées »"),
                  ("Proposition BI", "non : une révision n'est pas une entrée")]),
    dict(n="Q3", titre="Le sexe et l'âge du tableau 25",
         regle="Le tableau 25 de l'annuaire de la CNSS compte 527 femmes parmi les nouveaux "
               "retraités de 2024 ; la base en a 641. Comment y sont établis le sexe et l'âge ?",
         cas=[("L'âge", "âge par âge, les chiffres se décalent (60 ans : 377 contre 519), mais "
                        "les tranches de 5 ans retombent."),
              ("Le sexe", "114 femmes de plus et 94 hommes de moins : à comprendre avant de "
                          "publier T42.")],
         sources=[("Données", "notre requête P83, âge par âge"),
                  ("Proposition BI", "l'âge de la règle 4 et le sexe de la fiche de la "
                                     "personne")]),
    dict(n="Q4", titre="Le montant et le rappel",
         regle="Les « montants servis » d'un nouveau retraité comprennent-ils le rappel des mois "
               "d'avant l'année ?",
         cas=[("Non : les mois de l'année", "2,86 Md en 2024."),
              ("Oui : tout ce qui est payé dans l'année",
               "3,22 Md saisis en 2024, dont 0,23 Md de rappel.")],
         sources=[("Données", "notre requête P85 ; l'annuaire de la CNSS ne publie pas ce "
                              "montant"),
                  ("Proposition BI", "les mois de l'année, le rappel montré à part")]),
    dict(n="Q5", titre="La coordination et les services passés",
         regle="PE_COT_AUTRE est-il le nombre de mois cotisés dans l'autre caisse, compris dans "
               "le total ? On en tire la part CNSS des services passés.",
         cas=[("Si oui", "services passés = total moins PE_COT_AUTRE : 280 mois dans l'autre "
                         "caisse sur 362 en moyenne laissent 82 mois CNSS."),
              ("Si non", "il faut une autre source pour la part CNSS demandée par Kaya 2017.")],
         sources=[("Kaya 2017", "pour la coordination, prendre uniquement la partie CNSS"),
                  ("Données", "357 nouveaux retraités de coordination en 2024 (P86, P89)"),
                  ("Proposition BI", "le total moins PE_COT_AUTRE")]),
]

BI_CHECKS = ("La règle d'entrée retombe sur l'annuaire de la CNSS : 2 463 nouveaux retraités en "
             "2024 pour 2 489 publiés (lot 7, P82).\n"
             "L'âge retombe par tranche de 5 ans sur le tableau 25 ; pas le sexe (question Q3).\n"
             "Les mois cotisés sont toujours remplis ; la part de l'autre caisse est connue en "
             "coordination (P86, P89, P90).\n"
             "Le montant n'est pas publié par la CNSS : pas de contrôle possible (question Q4).")

# nouveaux retraités de 2024 (pension de vieillesse) par tranche : publiés par la CNSS
# (annuaire 2024, tableau 25) et les nôtres (lot 7, P83)
PYRAMIDE_2024 = [("moins de 55", 1, 1), ("55-59", 1429, 1450), ("60-64", 956, 941),
                 ("65-69", 89, 89), ("70 et plus", 11, 25)]


def figures(d, x, y, w):
    """Encadré des premiers chiffres : les nouveaux retraités publiés par la CNSS et les nôtres."""
    g = d.group()
    h = 620
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "En 2024 : la base face à l'annuaire de la CNSS", 22, groups=g,
           inside=frame)
    d.text(x + 20, y + 54, "2 463 nouveaux retraités chez nous, 2 489 publiés par la\n"
                           "CNSS (−1,0 %). Montant : 2,86 Md pour les mois de 2024,\n"
                           "et 0,23 Md de rappel.", 18, groups=g, inside=frame)
    d.text(x + 20, y + 140, "Nouveaux retraités par âge :", 16, C["doux"], groups=g, inside=frame)
    lx = x + 340
    d.box(lx, y + 142, 18, 14, bg=C["bleu_moyen"], stroke=C["bleu"], groups=g)
    d.text(lx + 26, y + 140, "annuaire de la CNSS", 16, groups=g, inside=frame)
    d.box(lx + 210, y + 142, 18, 14, bg=C["vert_moyen"], stroke=C["vert"], groups=g)
    d.text(lx + 236, y + 140, "la base (P83)", 16, groups=g, inside=frame)
    by = y + 176
    biggest = max(max(a, b) for _, a, b in PYRAMIDE_2024)
    scale = (w - 260) / biggest
    for label, cnss, nous in PYRAMIDE_2024:
        d.text(x + 20, by + 13, label, 16, groups=g, inside=frame)
        for k, (n, bg, stroke) in enumerate([(cnss, C["bleu_moyen"], C["bleu"]),
                                             (nous, C["vert_moyen"], C["vert"])]):
            top = by + k * 24
            d.box(x + 130, top, max(n * scale, 4), 20, bg=bg, stroke=stroke, groups=g)
            d.text(x + 140 + max(n * scale, 4), top, f"{n:,}".replace(",", " "), 15, groups=g,
                   inside=frame)
        by += 56
    note = wrap("Âge par âge, les chiffres se décalent ; par tranche de 5 ans, ils retombent. Le "
                "sexe, lui, ne retombe pas : 641 femmes chez nous pour 527 publiées.", 16, w - 40)
    d.text(x + 20, by + 8, note, 16, C["doux"], groups=g, inside=frame)
    by += 8 + height_of(note, 16) + 12
    w_chip = chip(d, x + 20, by, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 5, "annuaire CNSS 2024, tableaux 23 et 25 ; P82, P83, P85",
           16, groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=42)
    inner = WIDTH - 50

    d.text(0, 0, "T42 · Les nouveaux retraités de l'exercice, par âge et par sexe", 48)
    d.text(0, 66, "« Répartition des nouveaux pensionnés de vieillesse, montants et services passés "
                  "par tranches d'âges et par sexe » · fiche pilote de T42 à T49 · ce que l'équipe "
                  "BI a compris, à valider · état au 01/11/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T42", 22, groups=g, inside=about)
    text = wrap("Il dit combien de personnes sont parties à la retraite dans l'année, par tranche "
                "d'âge et par sexe, ce qui leur a été payé et combien de mois elles avaient "
                "cotisé. C'est la « loi des entrées » des études actuarielles.", 18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 15, maquette p. 42 ; fin d'exercice"),
                      ("Kaya 2017", "services passés en mois ; part CNSS")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(d, 25, top + 310, 760)

    bottom = mockup(
        d, 830, top, 300, (WIDTH - 25 - 830 - 300) / 6, "Tranches d'âge", "4 et 5",
        columns=[("Effectif", "2 et 3"), ("Montant", "7"), ("Total services passés", "8"),
                 ("Effectif", "2 et 3"), ("Montant", "7"), ("Total services passés", "8")],
        groups=[(0, 2, "HOMMES", "6"), (3, 5, "FEMMES", "6")],
        rows=[("Moins de 50 ans", None, None), ("50 – 54 ans", None, None),
              ("55 – 59 ans", None, None), ("60 – 64 ans", None, None),
              ("65 – 69 ans", None, None), ("70 – 74 ans", None, None),
              ("75 – 79 ans", None, None), ("80 – 84 ans", None, None),
              ("85 – 89 ans", None, None), ("90 – 94 ans", None, None),
              ("95 – 99 ans", None, None), ("100 ans et plus", None, None),
              ("TOTAL", None, None)],
        row_h=44)
    d.text(830, bottom + 14, wrap("Les colonnes Effectif comptent les nouveaux retraités de "
                                  "l'exercice (règles 1 à 3) ; Montant, ce qui leur a été payé "
                                  "pour les mois de l'exercice (règle 7) ; Total services passés, "
                                  "leurs mois cotisés à la CNSS (règle 8).", 18,
                                  WIDTH - 25 - 830), 18, C["doux"])
    sec["height"] = max(bottom + 110, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Qui est compté : six retraités fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 2, 28), 2025,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : la pension, de son effet à sa fin")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : les tranches d'âge")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "Une ligne par tranche d'âge de 5 ans, de « moins de 50 ans » à "
                                "« 100 ans et plus » (maquette p. 42). Les autres tableaux ont "
                                "leur propre grille (T43 jusqu'à 60 ans et plus, T44 de moins de "
                                "5 ans à 20 ans et plus…).", 18)
    bottom = line_cards(d, 25, y + 120, inner, LINES)
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 4. règles
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "4. Les règles, une par une : coche ta réponse")
    note = wrap(SQL_NOTE, 18, inner)
    d.text(25, y + 70, note, 18, C["doux"])
    bottom = cards_with_sql(d, 25, y + 70 + height_of(note, 18) + 20, inner,
                            [dict(rule, sql=SQL[rule["n"]]) for rule in RULES])
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 5. questions
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "5. Les questions pour le métier")
    bottom = cards_grid(d, 25, y + 70, inner, 3, QUESTIONS)
    g = d.group()
    checks = d.box(25, bottom + 20, inner, 190, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (01/11/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 190 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
