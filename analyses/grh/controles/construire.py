"""
Construit les fichiers de contrôle des requêtes proposées de la GRH, prêts à lancer en mode
script (F5) dans SQL Developer, sans rien saisir.

Chaque contrôle entoure la requête proposée (lue dans son fichier, à partir de son WITH) d'un
SELECT de contrôle. Les requêtes proposées lisent l'entrepôt (DWH et DTM), qui n'est pas encore
chargé : la copie du contrôle lit donc LA SOURCE, USER_GRH. Les faits de l'entrepôt sont des
copies des tables de la source, avec les mêmes colonnes et un cliché en plus (domaines/grh/
facts/sql) ; la copie change seulement le nom des tables et retire la condition sur le cliché.
Les deux tables de tranches CIPRES, qui n'existent que dans l'entrepôt, sont écrites dans la
copie. Les commentaires et les lignes vides sont retirés, et :2 (l'exercice) devient 2024.
À relancer après toute modification d'une requête proposée :

    python3 analyses/grh/controles/construire.py
"""

import re
from pathlib import Path

ICI = Path(__file__).resolve().parent
GRH = ICI.parent
# une table de l'entrepôt -> la table de la source qu'elle copie (le nom le plus long d'abord)
SOURCE = [
    ("DWH.FAIT_GRH_DECISION_PERSONNE", "USER_GRH.GRH_DECISION_PERSONNE"),
    ("DWH.FAIT_GRH_DECISION", "USER_GRH.GRH_DECISION"),
    ("DWH.FAIT_GRH_PERSONNE", "USER_GRH.GRH_PERSONNE"),
    ("DWH.FAIT_GRH_SITUATION", "USER_GRH.GRH_SITUATION"),
    ("DWH.FAIT_GRH_MOUVEMENT", "USER_GRH.GRH_MOUVEMENT"),
    ("DTM.DIM_GRH_UNITE_ADMINISTRATIVE", "USER_GRH.GRH_UNITE_ADMINISTRATIVE"),
    ("DTM.DIM_GRH_TYPE_MOUVEMENT", "USER_GRH.GRH_TYPE_MOUVEMENT"),
    ("DTM.DIM_GRH_FORMATION", "USER_GRH.GRH_FORMATION"),
]
# les tranches de l'Annuaire CIPRES (domaines/grh/dtm/ddl/20261025_tranches_cipres.sql)
AGES = [("<20", 0, 19), ("20-24", 20, 24), ("25-29", 25, 29), ("30-34", 30, 34), ("35-39", 35, 39),
        ("40-44", 40, 44), ("45-49", 45, 49), ("50-54", 50, 54), ("55-59", 55, 59), ("60+", 60, None)]
ANCIENNETES = [("<1", 0, 0), ("1-4", 1, 4), ("5-9", 5, 9), ("10-14", 10, 14), ("15-19", 15, 19),
               ("20-24", 20, 24), ("25-29", 25, 29), ("30-34", 30, 34), ("35-39", 35, 39),
               ("40+", 40, None)]


def tranches(lignes, code, mini, maxi):
    """Une table de tranches écrite dans la requête : (SELECT ... FROM DUAL UNION ALL ...)."""
    selects = [f"SELECT '{c}' AS {code}, {a} AS {mini}, "
               f"{b if b is not None else 'TO_NUMBER(NULL)'} AS {maxi} FROM DUAL"
               for c, a, b in lignes]
    return "(" + "\n               UNION ALL ".join(selects) + ")"


def vers_la_source(texte):
    """La requête proposée, lue sur la source au lieu de l'entrepôt."""
    # la condition sur le cliché n'a pas de sens sur la source : elle devient toujours vraie
    texte = re.sub(r"(?:\b\w+\.)?CLICHE\s*=\s*:1(?!\d)", "1 = 1", texte)
    texte = texte.replace("DTM.DIM_GRH_TRANCHE_AGE", tranches(AGES, "TRANCHE_AGE_CODE", "AGE_MIN", "AGE_MAX"))
    texte = texte.replace("DTM.DIM_GRH_TRANCHE_ANCIENNETE",
                          tranches(ANCIENNETES, "TRANCHE_ANC_CODE", "ANC_MIN", "ANC_MAX"))
    for entrepot, source in SOURCE:
        texte = re.sub(re.escape(entrepot) + r"(?!\w)", source, texte)
    assert "DWH." not in texte and "DTM." not in texte, "une table de l'entrepôt reste lue"
    return texte
TRAIT = "-- " + "-" * 77


def sans_commentaire(ligne):
    """La ligne sans son commentaire -- (un -- entre apostrophes est gardé)."""
    dans_texte = False
    for i, c in enumerate(ligne):
        if c == "'":
            dans_texte = not dans_texte
        elif c == "-" and not dans_texte and ligne[i:i + 2] == "--":
            return ligne[:i].rstrip()
    return ligne.rstrip()


def corps(fichier, exercice=None):
    """La requête proposée, du WITH à la fin, prête à être entourée."""
    lignes = (GRH / fichier).read_text(encoding="utf-8").splitlines()
    debut = next(i for i, ligne in enumerate(lignes) if ligne.strip() == "WITH")
    gardees = [sans_commentaire(ligne) for ligne in lignes[debut:]]
    texte = "\n".join("    " + ligne for ligne in gardees if ligne.strip())
    assert ";" not in texte and "&" not in texte, fichier
    # la colonne CLICHE de la sortie ne sert pas au contrôle : une constante
    texte = re.sub(r":1(\s+AS CLICHE)", r"0\1", texte)
    texte = vers_la_source(texte)
    if exercice:
        texte = re.sub(r":2(?!\d)", str(exercice), texte)
    assert not re.search(r":\d", texte), fichier
    return texte


def controle(titre, commentaire, select, fichier, where_group, exercice=None):
    entete = [TRAIT, f"-- {titre}"] + [f"-- {ligne}" for ligne in commentaire] + ["-- Résultat :", TRAIT]
    return "\n".join(entete) + f"\n{select}\nFROM (\n{corps(fichier, exercice)}\n)\n{where_group};\n"


def fichier(nom, entete, blocs):
    texte = "\n".join(f"-- {ligne}" if ligne else "--" for ligne in entete)
    (ICI / nom).write_text(texte + "\n\n\n" + "\n\n".join(blocs), encoding="utf-8")
    print(nom, ":", len(blocs), "contrôles")


EFFECTIF, T60, T62 = ("t58_t65_effectif_requete_proposee.sql", "t60_requete_proposee.sql",
                      "t62_requete_proposee.sql")
COMMUN = ["Exécution : mode script (F5) dans SQL Developer, rien à saisir. Comptages seulement.",
          "Lit LA SOURCE (USER_GRH), pas l'entrepôt : la requête proposée lit DWH et DTM, sa",
          "copie ci-dessous lit les mêmes tables dans USER_GRH, sans condition de cliché.",
          "Construit par analyses/grh/controles/construire.py à partir de la requête proposée :",
          "ne pas modifier ce fichier à la main, relancer le script.",
          "Coller le résultat sous la ligne « Résultat » de chaque contrôle."]

fichier("controle_1_effectif.sql", [
    "=" * 77,
    "CONTRÔLE 1 — La requête de l'effectif (T58, T59, T61, T65), 17/11/2026",
    "Requête contrôlée : analyses/grh/t58_t65_effectif_requete_proposee.sql (jamais lancée",
    "dans sa version du 08/11/2026).", *COMMUN,
    "=" * 77,
], [
    controle("C1 — T58 au 31/12/2024 : par nature, catégorie et sexe", [
        "Attendu  : environ 741 permanents (publié 758) : employés 129 H / 44 F, maîtrise 92 /",
        "           115, cadres moyens 139 / 165, cadres supérieurs 40 / 18, à quelques agents",
        "           près (6 agents en disponibilité en moins). Temporaires : plus de 46",
        "           (publié), faute de fin de contrat saisie (points 20 et 15 de la v2).",
    ], "SELECT NATURE_EMPLOI, GROUPE_CATEGORIE, SEXE, SUM(NB_AGENTS) AS agents", EFFECTIF,
        "WHERE ID_TEMPS = 20241231\n  AND QUALITE_EMPLOYE = 'A'\n"
        "GROUP BY NATURE_EMPLOI, GROUPE_CATEGORIE, SEXE\nORDER BY 1, 2, 3"),
    controle("C2 — T65 et l'effectif de 2019 à 2025 : par année et nature", [
        "Attendu  : permanents 2024 environ 741, taux d'encadrement 48,8 % (publié 49,2 %) ;",
        "           environ 42 % de 2020 à 2023 (publié 41 %) ; publié : 767 agents fin 2023,",
        "           758 fin 2024. Les agents inactifs valent 0 (disponibilité hors effectif).",
        "           Sert aussi aux points 57 (années passées) et 59 (taux des temporaires).",
    ], "SELECT ID_TEMPS, NATURE_EMPLOI, SUM(NB_AGENTS) AS agents, SUM(NB_CADRES) AS cadres,\n"
       "       ROUND(100 * SUM(NB_CADRES) / SUM(NB_AGENTS), 1) AS taux_encadrement,\n"
       "       SUM(NB_AGENTS_INACTIFS) AS inactifs", EFFECTIF,
        "WHERE ID_TEMPS BETWEEN 20191231 AND 20251231\n  AND MOD(ID_TEMPS, 10000) = 1231\n"
        "  AND QUALITE_EMPLOYE = 'A'\nGROUP BY ID_TEMPS, NATURE_EMPLOI\nORDER BY 2, 1"),
    controle("C3 — T61 au 31/12/2024, permanents : par tranche d'ancienneté et sexe", [
        "Attendu  : à comparer au tableau 122 de l'annuaire CNSS 2024. Premier calcul avec la",
        "           nouvelle règle (prise de service d'abord, absences retirées).",
    ], "SELECT TRANCHE_ANC_CODE, SEXE, SUM(NB_AGENTS) AS agents", EFFECTIF,
        "WHERE ID_TEMPS = 20241231\n  AND QUALITE_EMPLOYE = 'A'\n  AND NATURE_EMPLOI = 'PERMANENT'\n"
        "GROUP BY TRANCHE_ANC_CODE, SEXE\nORDER BY 1, 2"),
    controle("C4 — T58 au 31/12/2024, permanents : par tranche d'âge et sexe", [
        "Attendu  : à quelques agents près, les tranches d'âge du tableau 119 de l'annuaire",
        "           CNSS 2024 (lot 17).",
    ], "SELECT TRANCHE_AGE_CODE, SEXE, SUM(NB_AGENTS) AS agents", EFFECTIF,
        "WHERE ID_TEMPS = 20241231\n  AND QUALITE_EMPLOYE = 'A'\n  AND NATURE_EMPLOI = 'PERMANENT'\n"
        "GROUP BY TRANCHE_AGE_CODE, SEXE\nORDER BY 1, 2"),
])

MAQUETTE = ("SELECT ID_TEMPS,\n"
            "       SUM(CASE WHEN CODE_TYPE_MVT = 'DEBUT' THEN NB_AGENTS ELSE 0 END) AS debut,\n"
            "       SUM(CASE WHEN CODE_TYPE_MVT = 'RECRU' THEN NB_AGENTS ELSE 0 END) AS recrut,\n"
            "       SUM(CASE WHEN CODE_NAT_MVT = 'ENTREES' AND CODE_TYPE_MVT <> 'RECRU'\n"
            "                THEN NB_AGENTS ELSE 0 END)                             AS autres_ent,\n"
            "       SUM(CASE WHEN CODE_TYPE_MVT = 'RET' THEN NB_AGENTS ELSE 0 END)   AS retraite,\n"
            "       SUM(CASE WHEN CODE_TYPE_MVT = 'DEC' THEN NB_AGENTS ELSE 0 END)   AS deces,\n"
            "       SUM(CASE WHEN CODE_NAT_MVT = 'SORTIES'\n"
            "                 AND CODE_TYPE_MVT NOT IN ('RET', 'DEC')\n"
            "                THEN NB_AGENTS ELSE 0 END)                             AS autres_sort,\n"
            "       SUM(CASE WHEN CODE_NAT_MVT IN ('INITIAL', 'ENTREES') THEN NB_AGENTS\n"
            "                WHEN CODE_NAT_MVT = 'SORTIES' THEN -NB_AGENTS\n"
            "                ELSE 0 END)                                            AS fin")
fichier("controle_2_t60.sql", [
    "=" * 77,
    "CONTRÔLE 2 — La requête de T60 (mouvement du personnel), 17/11/2026",
    "Requête contrôlée : analyses/grh/t60_requete_proposee.sql (jamais lancée). Réglage en",
    "place : disponibilite = HORS_EFFECTIF.", *COMMUN,
    "=" * 77,
], [
    controle("C1 — T60, permanents, 2020 à 2025 : la maquette", [
        "Attendu  : début · recrut · autres_ent · retraite · deces · autres_sort · fin",
        "           2024 : environ 672 · 104 · 4 · 25 · 5 · 9 · 741.",
        "           Annuaire CNSS 2024 (tableau 121) : 767 · 104 · 7 · 23 · 3 · 94 · 758.",
        "           Chaque année, la fin doit être le début de l'année suivante.",
    ], MAQUETTE, T60,
        "WHERE QUALITE_EMPLOYE = 'A'\n  AND NATURE_EMPLOI = 'PERMANENT'\n"
        "  AND ID_TEMPS BETWEEN 20201231 AND 20251231\nGROUP BY ID_TEMPS\nORDER BY 1"),
    controle("C2 — T60, permanents, 2020 à 2025 : par code de mouvement", [
        "Lecture  : ne dépend pas de la table des codes. Un code dont la nature est vide",
        "           manque dans USER_GRH.GRH_TYPE_MOUVEMENT (voir Q152 du lot 26) : la maquette",
        "           du contrôle C1 l'oublie.",
    ], "SELECT ID_TEMPS, CODE_NAT_MVT, CODE_TYPE_MVT, SUM(NB_AGENTS) AS agents", T60,
        "WHERE QUALITE_EMPLOYE = 'A'\n  AND NATURE_EMPLOI = 'PERMANENT'\n"
        "  AND ID_TEMPS BETWEEN 20201231 AND 20251231\n"
        "GROUP BY ID_TEMPS, CODE_NAT_MVT, CODE_TYPE_MVT\nORDER BY 1, 2, 3"),
    controle("C3 — T60, permanents, 2024 : le détail des « autres » par motif", [
        "Attendu  : REM 4 ; 2 démissions, 1 licenciement, 5 disponibilités, 1 sorti sans motif",
        "           daté. C'est la liste que demande le recueil (point 49 de la v2).",
    ], "SELECT CODE_TYPE_MVT, MOTIF_MVT, SUM(NB_AGENTS) AS agents", T60,
        "WHERE QUALITE_EMPLOYE = 'A'\n  AND NATURE_EMPLOI = 'PERMANENT'\n  AND ID_TEMPS = 20241231\n"
        "GROUP BY CODE_TYPE_MVT, MOTIF_MVT\nORDER BY 1, 2"),
    controle("C4 — T60, temporaires, 2020 à 2025 : la maquette", [
        "Lecture  : premier chiffrage du tableau des temporaires. La fin d'une année doit être",
        "           le début de la suivante, et l'effectif de fin doit être celui des",
        "           temporaires du contrôle 1 (C2).",
    ], MAQUETTE, T60,
        "WHERE QUALITE_EMPLOYE = 'A'\n  AND NATURE_EMPLOI = 'TEMPORAIRE'\n"
        "  AND ID_TEMPS BETWEEN 20201231 AND 20251231\nGROUP BY ID_TEMPS\nORDER BY 1"),
])

fichier("controle_3_t62.sql", [
    "=" * 77,
    "CONTRÔLE 3 — La requête de T62 (personnel formé), exercice 2024, 17/11/2026",
    "Requête contrôlée : analyses/grh/t62_requete_proposee.sql (jamais lancée).", *COMMUN,
    "=" * 77,
], [
    controle("C1 — T62, exercice 2024 : la maquette", [
        "Attendu  : 409 permanents formés (employés 81, maîtrise 96, cadres moyens 181, cadres",
        "           supérieurs 51), 524 inscriptions, 2 avec promotion.",
        "           Annuaire CNSS 2024 (tableau 123) : 480 (132, 142, 118, 88), 6 avec promotion.",
        "           L'écart est le point 4 de la v2 ; Q149 et Q150 du lot 26 le creusent.",
    ], "SELECT *", T62, "ORDER BY RANG", exercice=2024),
])
