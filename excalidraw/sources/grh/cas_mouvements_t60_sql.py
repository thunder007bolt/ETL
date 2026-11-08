"""
Schéma « T60 : les cas de mouvement dans la requête SQL » (état au 08/11/2026).

Version technique de cas_mouvements_t60 : chaque cas est relié à la partie de
analyses/grh/t60_requete_proposee.sql qui le traite, au bout de SQL qui décide, et à la raison.
1. la requête en un coup d'œil (parties 0 à 14, CTE, lignes du fichier, grain) ;
2. le cœur : deux photos, puis qui entre, qui sort, et avec quel code ;
3. les entrées ; 4. les sorties ; 5. les cas sans mouvement ;
6. le rangement (axes et mesures) ; 7. le comptage final, et le passage au tableau T60.

Même modèle que « Les cas d'agents dans la requête SQL » (cas_agents_effectif_sql.py), avec
deux garde-fous : les numéros de ligne sont lus dans le fichier SQL à chaque génération, et
chaque bout de SQL est comparé au fichier (le script s'arrête si l'un d'eux n'y est plus).
« … » marque une coupure ; les commentaires « -- » ajoutés disent ce que vaut la condition.
Chaque cas des parties 3 à 5 a été rejoué dans la requête, un agent fictif par cas, avec les
quatre combinaisons des réglages disponibilite et passages_annee (08/11/2026).

Usage :
    python3 excalidraw/sources/grh/cas_mouvements_t60_sql.py [--force]
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "T60 mouvements" / "cas_mouvements_t60_sql.excalidraw"
SQL = ROOT.parent / "analyses" / "grh" / "t60_requete_proposee.sql"
LIGNES = SQL.read_text(encoding="utf-8").splitlines()

WIDTH = 3040
SQL_SIZE = 17
GAP_COL = 15

# genre : (fond du mouvement, bord, épaisseur, style, fond de la ligne)
GENRES = {
    "entree": (C["vert_moyen"], C["vert"], 2, "solid", C["blanc"]),
    "sortie": (C["orange_moyen"], C["orange"], 2, "solid", C["blanc"]),
    "rien": (C["gris"], C["gris_bord"], 2, "solid", C["blanc"]),
    "reglage": (C["jaune"], C["jaune_bord"], 2, "solid", C["blanc"]),
    "attention": (C["rouge_moyen"], C["rouge"], 3, "solid", C["rouge_clair"]),
    "absent": (C["blanc"], C["gris_bord"], 2, "dashed", C["blanc"]),
    "colonne": (C["bleu_clair"], C["bleu"], 2, "solid", C["blanc"]),
}

# colonnes des tableaux : (titre, largeur)
COLONNES = [("Le cas", 420), ("Le mouvement", 330), ("Où dans la requête", 400),
            ("Le SQL qui décide", 1180), ("Pourquoi", 570)]
COLONNES_RANGEMENT = [("L'axe", 420), ("Colonnes de la table", 330),
                      ("Où dans la requête", 400), ("Le SQL", 1180), ("À savoir", 570)]


# ---------------------------------------------------------------- lire le fichier SQL
def _plages():
    """Les lignes de chaque partie : de son en-tête « -- ── N. » à la dernière ligne non vide
    avant la partie suivante."""
    debuts = [(int(m.group(1)), i) for i, ligne_sql in enumerate(LIGNES, 1)
              if (m := re.match(r"-- ── (\d+)\.", ligne_sql))]
    res = {}
    for k, (n, debut) in enumerate(debuts):
        fin = debuts[k + 1][1] - 1 if k + 1 < len(debuts) else len(LIGNES)
        while fin > debut and not LIGNES[fin - 1].strip():
            fin -= 1
        res[n] = (debut, fin)
    return res


PARTIES = _plages()


def ligne(partie, motif, n=None):
    """Le numéro de la ligne de la partie qui contient motif : la seule, ou la n-ième."""
    debut, fin = PARTIES[partie]
    trouves = [i for i in range(debut, fin + 1) if motif in LIGNES[i - 1]]
    if (n is None and len(trouves) != 1) or (n is not None and len(trouves) < n):
        sys.exit(f"partie {partie} : « {motif} » trouvé {len(trouves)} fois dans {SQL.name}")
    return trouves[(n or 1) - 1]


def cte(nom):
    """Première et dernière ligne d'une CTE : de « nom AS ( » à la parenthèse qui la ferme."""
    for debut, ligne_sql in enumerate(LIGNES, 1):
        if ligne_sql.startswith(f"{nom} AS ("):
            for fin in range(debut + 1, len(LIGNES) + 1):
                if LIGNES[fin - 1].startswith(")"):
                    return debut, fin
    sys.exit(f"CTE {nom} introuvable dans {SQL.name}")


def branche(partie, motif_then):
    """Un WHEN … THEN 'cas' d'un CASE : du WHEN à la ligne de son THEN."""
    fin = ligne(partie, motif_then)
    debut = fin
    while not LIGNES[debut - 1].strip().startswith("WHEN"):
        debut -= 1
    return debut, fin


def ou(*items):
    """« partie · libellé (l. a à b) », un item par ligne ; lignes = un numéro ou (début, fin)."""
    res = []
    for partie, libelle, lignes_sql in items:
        a, b = lignes_sql if isinstance(lignes_sql, tuple) else (lignes_sql, lignes_sql)
        res.append(f"{partie} · {libelle} (l. {a})" if a == b
                   else f"{partie} · {libelle} (l. {a} à {b})")
    return "\n".join(res)


def _code(ligne_sql):
    """La ligne sans son commentaire « -- » (hors des chaînes entre apostrophes)."""
    dans = False
    for i, ch in enumerate(ligne_sql):
        if ch == "'":
            dans = not dans
        elif not dans and ligne_sql.startswith("--", i):
            return ligne_sql[:i]
    return ligne_sql


def _normal(txt):
    return " ".join(txt.split())


CODE = _normal(" ".join(_code(x) for x in LIGNES))
ENTETE = _normal(" ".join(x.strip()[2:] for x in LIGNES if x.strip().startswith("--")))
ECARTS = []


def sql(txt, entete=False):
    """Un bout de SQL du schéma : chaque morceau (hors commentaires ajoutés et « … ») doit être
    dans le fichier, à l'espace près. entete : le bout vient des commentaires de l'en-tête."""
    corpus = ENTETE if entete else CODE
    for x in txt.split("\n"):
        for morceau in _code(x).split("…"):
            m = _normal(morceau)
            if m and m not in corpus:
                ECARTS.append(m)
    return txt


# ---------------------------------------------------------------- 3. les entrées
ENTREES = [
    ("Embauché dans l'année (permanent)", "RECRU · EMBAUCHE", "entree",
     ou((9, "entrees, [E4]", branche(9, "AND p.date_entree <= f.d1")),
        (9, "le code", ligne(9, "WHEN 'EMBAUCHE'", 1)),
        (2, "personnes", ligne(2, "AS date_entree"))),
     sql("COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree   -- partie 2\n"
         "…\n"
         "WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'        -- vrai : permanent à la fin\n"
         " AND p.date_entree >  f.d0                           -- vrai : 01/03/2024 > 31/12/2023\n"
         " AND p.date_entree <= f.d1          THEN 'EMBAUCHE'   -- vrai\n"
         "…\n"
         "WHEN 'EMBAUCHE'    THEN 'RECRU'"),
     "Sa date d'entrée (embauche, sinon prise de service) tombe dans l'exercice. [E3] passe "
     "avant : un détaché embauché la même année sort en DETAC. 2024 : 104, comme l'annuaire CNSS."),
    ("Nouveau contrat de temporaire", "RECRU · ENGAGEMENT_50", "entree",
     ou((9, "entrees, [E6]", branche(9, "THEN 'ENGAGEMENT'")),
        (9, "entrees, tc0", ligne(9, "LEFT JOIN temporaires_sous_contrat tc0"))),
     sql("WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'                   -- vrai : temporaire à la fin\n"
         " AND tc0.PERS_ID IS NULL           THEN 'ENGAGEMENT'  -- vrai : aucun contrat au début\n"
         "…\n"
         "LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete = f.d0\n"
         "…\n"
         "WHEN 'ENGAGEMENT'  THEN 'ENGAGEMENT_' || x.code_engagement     -- '50'"),
     "Aucun contrat ne couvrait le 31/12 d'avant : tc0 est vide. Son engagement 50 est un "
     "recrutement, dans le tableau des temporaires."),
    ("Temporaire devenu permanent (vu des permanents)", "AUTRE_ENT · ETAIT_TEMPORAIRE", "entree",
     ou((9, "entrees, WHERE [E2]",
         ligne(9, "OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')")),
        (9, "entrees, [E7]", ligne(9, "THEN 'NATURE'")),
        (9, "le motif", ligne(9, "THEN 'ETAIT_'"))),
     sql("WHERE dbt.PERS_ID IS NULL                                      -- faux : présent au début…\n"
         "   OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-') -- vrai : …mais temporaire\n"
         "…\n"
         "WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'   -- [E3] à [E6] faux\n"
         "…\n"
         "WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')"),
     "Sur les deux photos, mais pas dans la même nature : il entre chez les permanents et sort "
     "des temporaires ([S6]). [E3] à [E6] sont faux : ni 37, ni embauche dans l'année, ni dispo."),
    ("Revenu de disponibilité", "REM · DECISION_18\n(réglage HORS_EFFECTIF)", "reglage",
     ou((9, "entrees, [E5]", branche(9, "THEN 'REMISE'")),
        (9, "entrees, po", ligne(9, "LEFT JOIN positions po")),
        (7, "base, WHERE", ligne(7, "AND NOT (pr.disponibilite = 'HORS_EFFECTIF'"))),
     sql("AND NOT (pr.disponibilite = 'HORS_EFFECTIF'           -- base : avec HORS_EFFECTIF,\n"
         "         AND NVL(po.date_dispo, DATE '1900-01-01')    -- l'agent en dispo n'est pas\n"
         "             > NVL(po.date_remise, DATE '1900-01-01')))   -- sur la photo\n"
         "…\n"
         "WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
         " AND pr.disponibilite = 'HORS_EFFECTIF'               -- faux avec 'PRESENT'\n"
         " AND NVL(po.date_dispo, DATE '1900-01-01')\n"
         "     > NVL(po.date_remise, DATE '1900-01-01')     THEN 'REMISE'\n"
         "…\n"
         "LEFT JOIN positions po ON po.date_arrete = f.d0       -- ses 07 et 18 au 31/12 d'avant"),
     "Avec 'PRESENT' (réglage actuel), il est sur les deux photos : il n'arrive même pas au CASE. "
     "Avec 'HORS_EFFECTIF', base l'écarte au 31/12 d'avant, où sa 07 est plus récente que sa 18."),
    ("Détaché à la CNSS", "DETAC · DECISION_37", "absent",
     ou((8, "detaches_annee", cte("detaches_annee")),
        (9, "entrees, [E3]", branche(9, "THEN 'DETACHEMENT'"))),
     sql("JOIN decisions d ON d.date_decision >  e.d0\n"
         "                AND d.date_decision <= e.d1\n"
         "…\n"
         "  AND d.code = cd.classement_detache                  -- '37'\n"
         "…\n"
         "WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
         " AND dt.PERS_ID IS NOT NULL        THEN 'DETACHEMENT'  -- le premier WHEN"),
     "Une 37 datée dans l'exercice. C'est le premier WHEN : il passe avant l'embauche (vérifié). "
     "Aucune 37 chez les permanents aujourd'hui (QG7)."),
    ("Embauché fin décembre, sans situation au 31/12", "AUTRE_ENT ·\nETAIT_SANS_SITUATION",
     "attention",
     ou((7, "base, WHERE", ligne(7, "(NVL(si.nature, '-') <> 'TEMPORAIRE'")),
        (9, "entrees, WHERE [E2]",
         ligne(9, "OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')")),
        (9, "le motif", ligne(9, "THEN 'ETAIT_'"))),
     sql("(NVL(si.nature, '-') <> 'TEMPORAIRE'       -- base : sans situation, compté, nature NULL\n"
         "…\n"
         "   OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')   -- '-' <> 'PERMANENT'\n"
         "…\n"
         "WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')"),
     "Au 31/12/2023, il est compté sans nature : RECRU, NATURE_EMPLOI vide, dans aucun tableau. "
     "En 2024, il quitte cette population (AUTRE_SORT) et entre chez les permanents. Sans le NVL, "
     "il ne bougerait jamais (vérifié)."),
    ("Entré sans explication (ex. : ancien temporaire, sans contrat au 31/12 d'avant, nommé "
     "permanent)", "AUTRE_ENT · SANS_MOTIF", "attention",
     ou((9, "entrees, [E8]", ligne(9, "'SANS_MOTIF'", 2)),
        (9, "le code", ligne(9, "'AUTRE_ENT'"))),
     sql("WHERE dbt.PERS_ID IS NULL                         -- vrai : sans contrat, absent au début\n"
         "…\n"
         "-- [E3] à [E7] faux : ni 37, ni embauche dans l'année (sa date d'entrée est celle\n"
         "-- de son premier contrat), ni dispo, permanent à la fin, absent au début\n"
         "ELSE                                                   'SANS_MOTIF'\n"
         "…\n"
         "ELSE                    'AUTRE_ENT'"),
     "Rien ne l'explique : MOTIF_MVT le signale. Seulement si sa fiche garde la date d'embauche "
     "de son premier contrat ; sinon, c'est un RECRU. À lister pour la DRH."),
]

# ---------------------------------------------------------------- 4. les sorties
SORTIES = [
    ("Parti à la retraite", "RET · DECISION_02", "sortie",
     ou((0, "decisions_depart", ligne(0, "SELECT '02' AS code")),
        (3, "departs", cte("departs")),
        (10, "sorties, [S3]", branche(10, "THEN 'DEPART_PERMANENT'"))),
     sql("SELECT '02' AS code, 'RET' AS type_mvt, …          -- partie 0 : le type de chaque code\n"
         "…\n"
         "dd.type_mvt, 'DECISION_' || dc.code AS motif       -- partie 3 : departs\n"
         "…\n"
         "JOIN decisions_depart dd ON dd.code = dc.code\n"
         "…\n"
         "WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
         " AND pd.date_depart >  dbt.d0\n"
         " AND pd.date_depart <= dbt.d1          THEN 'DEPART_PERMANENT'   -- vrai : 30/06/2024\n"
         "…\n"
         "WHEN 'DEPART_PERMANENT'  THEN x.type_premier       -- 'RET'"),
     "Son premier départ (pd, comme pour l'effectif) tombe dans l'exercice. Le code vient de "
     "decisions_depart : 02 donne RET. 2024 : 25, pour 23 publiés."),
    ("Décédé : date de décès, sinon liquidation des droits (34)",
     "DEC · DATE_DECES\nou DEC · DECISION_34", "attention",
     ou((3, "departs", ligne(3, "SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'")),
        (0, "decisions_depart", ligne(0, "SELECT '34'")),
        (3, "premier_depart", cte("premier_depart"))),
     sql("SELECT '34', 'DEC',       'liquidation des droits après décès'   …   -- partie 0\n"
         "…\n"
         "UNION ALL                                          -- partie 3, departs :\n"
         "SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'    -- la date de décès, un départ de plus\n"
         "FROM DWH.FAIT_GRH_PERSONNE\n"
         "WHERE CLICHE = :1\n"
         "  AND DATE_DECES IS NOT NULL"),
     "La date de décès est un départ comme un autre. Sans elle, l'agent sort à la 34, environ "
     "8 mois après, souvent l'année suivante : aucune date de décès depuis 2010 (QG3). "
     "2024 : 5, pour 3 publiés."),
    ("Démission (36) ou licenciement (54)", "DEM · DECISION_36\nLIC · DECISION_54", "sortie",
     ou((0, "decisions_depart", (ligne(0, "SELECT '36'"), ligne(0, "SELECT '54'"))),
        (3, "decisions", (ligne(3, "SELECT NVL(d.PERS_ID, dp.PERS_ID)"),
                          ligne(3, "ON dp.DECI_NO = d.DECI_NO"))),
        (10, "sorties, [S3]", branche(10, "THEN 'DEPART_PERMANENT'"))),
     sql("SELECT '36', 'DEM',       'démission'          FROM DUAL UNION ALL\n"
         "…\n"
         "SELECT '54', 'LIC',       'licenciement'       FROM DUAL\n"
         "…\n"
         "SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,    -- l'agent, ou la liste de la décision\n"
         "…\n"
         "LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp\n"
         "       ON dp.DECI_NO = d.DECI_NO"),
     "Le même chemin que la retraite ; seul le code change. Une décision collective porte ses "
     "agents dans GRH_DECISION_PERSONNE : le NVL les retrouve. 2024 : 2 démissions et "
     "1 licenciement, comme l'annuaire CNSS."),
    ("Fin de contrat (35) ou rupture (52), temporaire", "FIN_CTRAT ·\nDECISION_35 ou _52",
     "sortie",
     ou((8, "dernier_depart_annee", cte("dernier_depart_annee")),
        (10, "sorties, [S4]", branche(10, "THEN 'DEPART_TEMPORAIRE'"))),
     sql("ROW_NUMBER() OVER (PARTITION BY e.annee, dep.PERS_ID\n"
         "                   ORDER BY dep.date_depart DESC, dep.motif DESC) AS rn   -- le dernier\n"
         "…\n"
         "JOIN departs dep ON dep.date_depart >  e.d0\n"
         "                AND dep.date_depart <= e.d1\n"
         "…\n"
         "WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'\n"
         " AND dda.PERS_ID IS NOT NULL                     THEN 'DEPART_TEMPORAIRE'"),
     "Pour un temporaire, le dernier départ de l'année, pas le premier de sa vie. Une 35 en "
     "mars, un contrat en mai, une 52 en octobre : la sortie est DECISION_52 (vérifié)."),
    ("Mis en disponibilité", "DISP · DECISION_07\n(réglage HORS_EFFECTIF)", "reglage",
     ou((10, "sorties, [S5]", branche(10, "THEN 'DISPONIBILITE'")),
        (10, "sorties, po", ligne(10, "LEFT JOIN positions po"))),
     sql("WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
         " AND pr.disponibilite = 'HORS_EFFECTIF'               -- faux avec 'PRESENT'\n"
         " AND NVL(po.date_dispo, DATE '1900-01-01')\n"
         "     > NVL(po.date_remise, DATE '1900-01-01')   THEN 'DISPONIBILITE'\n"
         "…\n"
         "LEFT JOIN positions po ON po.date_arrete = dbt.d1     -- ses 07 et 18 au 31/12\n"
         "…\n"
         "WHEN 'DISPONIBILITE'     THEN 'DECISION_' || x.code_dispo   -- '07'"),
     "Avec 'HORS_EFFECTIF', base l'écarte au 31/12 de l'année ; sans départ daté, sa 07 sans 18 "
     "donne DISP. Avec 'PRESENT', il reste sur les deux photos : aucune sortie. 2024 : 5 décisions "
     "07."),
    ("Titularisé, vu des temporaires", "AUTRE_SORT ·\nDEVIENT_PERMANENT", "sortie",
     ou((10, "sorties, WHERE [S2]",
         ligne(10, "OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')")),
        (10, "sorties, [S6]", ligne(10, "THEN 'NATURE'")),
        (10, "le motif", ligne(10, "THEN 'DEVIENT_'"))),
     sql("WHERE fin.PERS_ID IS NULL                                        -- faux : présent à la fin…\n"
         "   OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-') -- vrai : …mais permanent\n"
         "…\n"
         "WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'\n"
         "…\n"
         "WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')"),
     "Le pendant de l'entrée AUTRE_ENT · ETAIT_TEMPORAIRE. [S4] passe avant : avec une 35 ou une "
     "52 dans l'année, il sort en FIN_CTRAT, puis entre chez les permanents (vérifié)."),
    ("Retraite, puis décès la même année", "RET · DECISION_02\n(pas DEC)", "sortie",
     ou((3, "premier_depart", (ligne(3, "ROW_NUMBER() OVER (PARTITION BY x.PERS_ID"),
                               ligne(3, "WHERE rn = 1")))),
     sql("ROW_NUMBER() OVER (PARTITION BY x.PERS_ID\n"
         "                   ORDER BY x.date_depart, x.motif) AS rn\n"
         "…\n"
         "WHERE rn = 1                       -- la 02 du 01/02, pas la 34 du 01/11"),
     "premier_depart ne garde que le départ le plus ancien, comme pour l'effectif (G2). À date "
     "égale, le motif le plus petit dans l'ordre alphabétique (DATE_DECES avant DECISION_…)."),
    ("Sorti sans départ daté (ex. : nouvelle situation TEMPORAIRE, sans engagement 50)",
     "AUTRE_SORT · SANS_MOTIF", "attention",
     ou((10, "sorties, [S7]", ligne(10, "'SANS_MOTIF'", 2)),
        (10, "le code", ligne(10, "'AUTRE_SORT'"))),
     sql("WHERE fin.PERS_ID IS NULL                         -- vrai : absent de la photo de fin\n"
         "…\n"
         "-- [S3] à [S6] faux : aucun départ daté dans l'exercice, pas de dispo,\n"
         "-- absent de toutes les natures à la fin\n"
         "ELSE                                                   'SANS_MOTIF'\n"
         "…\n"
         "ELSE                          'AUTRE_SORT'"),
     "Présent au début, absent à la fin, sans départ daté pour l'expliquer. 2024 : 1 permanent "
     "dans ce cas. MOTIF_MVT le signale ; à lister pour la DRH."),
]

# ---------------------------------------------------------------- 5. sans mouvement
SANS_MOUVEMENT = [
    ("Changé d'unité, de qualification ou de catégorie", "AUCUN MOUVEMENT DE T60\n(MUT, AFF… à "
     "part)", "rien",
     ou((9, "entrees, WHERE", (ligne(9, "WHERE dbt.PERS_ID IS NULL"),
                               ligne(9, "OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI"))),
        (12, "internes", cte("internes"))),
     sql("WHERE dbt.PERS_ID IS NULL                                        -- faux : présent au début\n"
         "   OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')   -- faux : même nature\n"
         "…\n"
         "WHERE pr.mouvements_internes = 'COMME_AVANT'          -- partie 12, GRH_MOUVEMENT\n"
         "  AND NVL(tm.CODE_NAT_MVT, '-') NOT IN ('INITIAL', 'ENTREES', 'SORTIES')"),
     "Seules la présence et la nature sont comparées, pas l'unité ni la qualification. Ses "
     "mutations viennent de GRH_MOUVEMENT, comme avant, en lignes INTERNE : deux mutations font "
     "2 mouvements, 1 agent (vérifié)."),
    ("Contrat renouvelé", "AUCUN MOUVEMENT", "rien",
     ou((4, "engagements", (ligne(4, "LEAD(e.date_decision)"), ligne(4, "AS debut_suivant"))),
        (4, "contrats", (ligne(4, "LEAST(NVL(MIN(dep.date_depart)"),
                         ligne(4, "NVL(g.debut_suivant, DATE '9999-12-31'))")))),
     sql("LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID\n"
         "                            ORDER BY e.date_decision) AS debut_suivant\n"
         "…\n"
         "LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),\n"
         "      NVL(g.debut_suivant, DATE '9999-12-31'))         AS fin   -- = début du suivant"),
     "Le contrat 1 finit le jour où le 2 commence : aucun 31/12 ne tombe dans un trou. L'agent "
     "est sur les deux photos."),
    ("Parti puis revenu dans l'année (temporaire)", "AUCUN MOUVEMENT", "rien",
     ou((4, "temporaires_sous_contrat", (ligne(4, "JOIN contrats c ON"),
                                         ligne(4, "AND da.date_arrete <  c.fin"))),
        (10, "sorties, WHERE", (ligne(10, "WHERE fin.PERS_ID IS NULL"),
                                ligne(10, "OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE")))),
     sql("JOIN contrats c ON c.debut       <= da.date_arrete\n"
         "               AND da.date_arrete <  c.fin           -- le contrat de juin couvre le 31/12\n"
         "…\n"
         "WHERE fin.PERS_ID IS NULL                            -- faux : présent à la fin\n"
         "   OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')   -- faux"),
     "Sous contrat aux deux 31/12. Sa 35 de mars est bien dans dernier_depart_annee, mais sorties "
     "ne garde que les absents de la photo de fin (lot 25)."),
    ("Départ annulé (décision 40)", "AUCUN MOUVEMENT", "rien",
     ou((3, "decisions", (ligne(3, "AND NOT EXISTS (SELECT 1"),
                          ligne(3, "AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))")))),
     sql("AND NOT EXISTS (SELECT 1\n"
         "                FROM DWH.FAIT_GRH_DECISION a\n"
         "                WHERE a.CLICHE = :1\n"
         "                  AND TO_CHAR(a.CDEC_CODE) = cd.annulation          -- '40'\n"
         "                  AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))"),
     "La 02 visée par une 40 n'entre pas dans decisions, donc ni dans departs ni dans "
     "premier_depart : l'agent reste présent, sans sortie."),
    ("En disponibilité (réglage actuel)", "AUCUN MOUVEMENT\n(réglage PRESENT)", "reglage",
     ou((0, "parametres", ligne(0, "AS disponibilite")),
        (7, "base, WHERE", (ligne(7, "AND NOT (pr.disponibilite = 'HORS_EFFECTIF'"),
                            ligne(7, "> NVL(po.date_remise, DATE '1900-01-01')))")))),
     sql("'PRESENT'               AS disponibilite,           -- partie 0 : valeur actuelle\n"
         "…\n"
         "AND NOT (pr.disponibilite = 'HORS_EFFECTIF'          -- faux : le NOT est vrai\n"
         "         AND NVL(po.date_dispo, DATE '1900-01-01')\n"
         "             > NVL(po.date_remise, DATE '1900-01-01')))"),
     "Avec 'PRESENT', l'agent en disponibilité reste dans base : il est sur les deux photos, ni "
     "sortie ni retour. Même valeur dans la requête de l'effectif (QG6, en attente de la DRH)."),
    ("Entré et sorti la même année", "PAS COMPTÉ\n(passages_annee = NON)", "reglage",
     ou((11, "passages, permanents", (ligne(11, "WHERE pr.passages_annee = 'OUI'", 1),
                                      ligne(11, "AND fin.PERS_ID IS NULL", 1))),
        (11, "passages, temporaires", (ligne(11, "WHERE pr.passages_annee = 'OUI'", 2),
                                       ligne(11, "AND fin.PERS_ID IS NULL", 2)))),
     sql("WHERE pr.passages_annee = 'OUI'           -- faux avec 'NON' : aucune ligne\n"
         "  AND p.etat = 'INACTIF'\n"
         "  AND NVL(si.nature, '-') <> 'TEMPORAIRE'\n"
         "  AND dbt.PERS_ID IS NULL                 -- absent de la photo du début\n"
         "  AND fin.PERS_ID IS NULL                 -- et de celle de la fin"),
     "Absent des deux photos, il n'est ni dans entrees ni dans sorties. Avec 'OUI', passages "
     "ajoute un RECRU et la sortie de son départ ; de même pour un contrat commencé et fini dans "
     "l'année (vérifié)."),
    ("Fiche INACTIF, sans aucune date de départ", "JAMAIS DANS T60", "attention",
     ou((7, "base, LEFT JOIN", ligne(7, "LEFT JOIN premier_depart pd")),
        (7, "base, WHERE", ligne(7, "AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)"))),
     sql("LEFT JOIN premier_depart pd ON pd.PERS_ID = p.PERS_ID          -- rien : pd.* = NULL\n"
         "…\n"
         "AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)   -- faux OR NULL : faux\n"
         "…\n"
         "JOIN base b ON b.date_arrete = e.d0                            -- partie 8 : aucune photo"),
     "Jamais dans base, donc sur aucune photo : ni entrée, ni sortie. Ce sont surtout les 84 "
     "engagés de 2019 (G11), que l'annuaire CNSS sort tous en 2024 : 84 de ses 94 autres sorties."),
    ("Mis à disposition, détaché hors de la CNSS", "AUCUN MOUVEMENT", "rien",
     ou((3, "decisions, les codes lus",
         (ligne(3, "AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)"),
          ligne(3, "cd.classement_detache))")))),
     sql("AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)   -- 02 34 35 36 52 54\n"
         "     OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,\n"
         "                                 cd.mise_en_disponibilite, cd.remise_en_activite,\n"
         "                                 cd.classement_detache))      -- 50 51 07 18 37 : pas 14"),
     "La décision 14 n'est pas lue, et le statut de la situation n'est pas daté (lot 24) : "
     "l'agent reste dans l'effectif. MISE_DIS et FIN_DETACH restent vides (questions 5 et 6)."),
]

# ---------------------------------------------------------------- 6. rangement
RANGEMENT = [
    ("L'exercice", "ID_TEMPS", "colonne",
     ou((1, "exercices", cte("exercices")), (14, "SELECT", ligne(14, "AS ID_TEMPS"))),
     sql("ADD_MONTHS(da.date_arrete, -12)                            AS d0,\n"
         "da.date_arrete                                             AS d1\n"
         "…\n"
         "TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,"),
     "AAAA1231, l'exercice du mouvement. Les lignes DEBUT de 2024 sont la photo du 31/12/2023 ; "
     "la fin de 2024 se calcule, ou se lit dans les lignes DEBUT de 2025."),
    ("Le code et sa famille", "CODE_TYPE_MVT\nCODE_NAT_MVT\nCODE_SENS_MVT", "colonne",
     ou((14, "SELECT", (ligne(14, "CASE tm.CODE_NAT_MVT", 1), ligne(14, "AS CODE_SENS_MVT"))),
        (14, "la table des codes", (ligne(14, "LEFT JOIN DTM.DIM_GRH_TYPE_MOUVEMENT tm"),
                                    ligne(14, "ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT")))),
     sql("LEFT JOIN DTM.DIM_GRH_TYPE_MOUVEMENT tm\n"
         "       ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT\n"
         "…\n"
         "CASE tm.CODE_NAT_MVT\n"
         "    WHEN 'ENTREES' THEN 'PRISE'\n"
         "    WHEN 'SORTIES' THEN 'CESSA'\n"
         "    ELSE NULL\n"
         "END                                                   AS CODE_SENS_MVT,"),
     "La famille (INITIAL, ENTREES, SORTIES, INTERNE) vient de la table des codes de la CNSS, "
     "comme avant. Un code absent de cette table aurait une famille vide : contrôle 2 de "
     "regles_t60.md."),
    ("Qualité et nature d'emploi", "QUALITE_EMPLOYE\nNATURE_EMPLOI", "colonne",
     ou((7, "base", (ligne(7, "AS QUALITE_EMPLOYE"), ligne(7, "AS NATURE_EMPLOI"))),
        (9, "entrees, f", ligne(9, "FROM a_la_fin f")),
        (10, "sorties, dbt", ligne(10, "FROM au_debut dbt"))),
     sql("p.qualite                                              AS QUALITE_EMPLOYE,\n"
         "si.nature                                              AS NATURE_EMPLOI,\n"
         "…\n"
         "FROM a_la_fin f          -- entrees : rangée avec la photo de fin\n"
         "…\n"
         "FROM au_debut dbt        -- sorties : rangée avec la photo du début"),
     "La requête garde les qualités A, B, C et les deux natures ; OBIEE filtre A et la nature "
     "(filtre global). Une entrée prend la situation de fin, une sortie celle du début (vérifié)."),
    ("Unité, qualification, sexe", "UA_CODE\nQUAL_CODE\nSEXE, LIBELLE_SEXE", "colonne",
     ou((6, "situations", ligne(6, "s.QUAL_CODE, s.UA_CODE")),
        (14, "SELECT", (ligne(14, "CASE m.SEXE", 1), ligne(14, "AS LIBELLE_SEXE")))),
     sql("SELECT da.date_arrete, s.PERS_ID, s.QUAL_CODE, s.UA_CODE,\n"
         "…\n"
         "CASE m.SEXE\n"
         "    WHEN 'M' THEN 'Masculin'\n"
         "    WHEN 'F' THEN 'Féminin'\n"
         "    ELSE 'NON RENSEIGNE'\n"
         "END                                                   AS LIBELLE_SEXE,"),
     "Même règle : la photo où l'agent est vu. Par unité, début + entrées − sorties ne donne pas "
     "la fin : un muté ne fait ni entrée ni sortie. La fin d'une unité se lit dans "
     "DTM_GRH_EFFECTIF ; l'écart = ses mutations nettes."),
    ("Le motif", "MOTIF_MVT", "colonne",
     ou((13, "mouvements", ligne(13, "AS MOTIF_MVT")),
        (9, "entrees", (ligne(9, "CASE x.cas", 2), ligne(9, "AS MOTIF_MVT"))),
        (10, "sorties", (ligne(10, "CASE x.cas", 2), ligne(10, "AS MOTIF_MVT")))),
     sql("CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT                       -- DEBUT : vide\n"
         "…\n"
         "WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')\n"
         "…\n"
         "QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'   -- internes"),
     "Le détail des « autres » demandé par le recueil v7 (RH ligne 3) : DECISION_xx, DATE_DECES, "
     "EMBAUCHE, ENGAGEMENT_50, ETAIT_…, DEVIENT_…, SANS_MOTIF."),
    ("Les mesures", "NB_MOUVEMENTS\nNB_AGENTS", "colonne",
     ou((13, "mouvements, id_mvt", ligne(13, "PERS_ID AS id_mvt")),
        (14, "SELECT", (ligne(14, "AS NB_MOUVEMENTS"), ligne(14, "AS NB_AGENTS")))),
     sql("SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,\n"
         "…\n"
         "SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,        -- internes : id_mvt = MVT_ID\n"
         "…\n"
         "COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,\n"
         "COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,"),
     "Pour T60, une ligne par agent, exercice et code : les deux mesures sont égales. Pour les "
     "mouvements internes, deux mutations du même agent font 2 mouvements, 1 agent. T60 lit "
     "NB_AGENTS."),
]

# ---------------------------------------------------------------- 2. le cœur
PHOTOS_SQL = sql(
    "SELECT EXTRACT(YEAR FROM da.date_arrete)                          AS annee,   -- exercices\n"
    "       ADD_MONTHS(da.date_arrete, -12)                            AS d0,\n"
    "       da.date_arrete                                             AS d1\n"
    "…\n"
    "au_debut AS (\n"
    "    SELECT e.annee, e.d0, e.d1, b.*\n"
    "    FROM exercices e\n"
    "    JOIN base b ON b.date_arrete = e.d0          -- la photo du début : le 31/12 d'avant\n"
    "…\n"
    "a_la_fin AS (\n"
    "    …\n"
    "    JOIN base b ON b.date_arrete = e.d1          -- la photo de fin : le 31/12 de l'année")

ENTREES_SQL = sql(
    "FROM a_la_fin f\n"
    "JOIN personnes p ON p.PERS_ID = f.PERS_ID\n"
    "…\n"
    "LEFT JOIN au_debut dbt ON dbt.annee = f.annee AND dbt.PERS_ID = f.PERS_ID\n"
    "…\n"
    "WHERE dbt.PERS_ID IS NULL                                        -- [E1]\n"
    "   OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')   -- [E2]\n"
    "\n"
    "-- et avec quel cas : le premier WHEN vrai l'emporte\n"
    "CASE\n"
    "    WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
    "     AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'   -- [E3]\n"
    "    WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
    "     AND p.date_entree >  f.d0\n"
    "     AND p.date_entree <= f.d1                        THEN 'EMBAUCHE'      -- [E4]\n"
    "    WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
    "     AND pr.disponibilite = 'HORS_EFFECTIF'\n"
    "     AND NVL(po.date_dispo, DATE '1900-01-01')\n"
    "         > NVL(po.date_remise, DATE '1900-01-01')     THEN 'REMISE'        -- [E5]\n"
    "    WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'\n"
    "     AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'    -- [E6]\n"
    "    WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'        -- [E7]\n"
    "    ELSE                                                   'SANS_MOTIF'    -- [E8]\n"
    "END                        AS cas")

SORTIES_SQL = sql(
    "FROM au_debut dbt\n"
    "…\n"
    "LEFT JOIN a_la_fin fin ON fin.annee = dbt.annee AND fin.PERS_ID = dbt.PERS_ID\n"
    "LEFT JOIN premier_depart pd ON pd.PERS_ID = dbt.PERS_ID\n"
    "…\n"
    "WHERE fin.PERS_ID IS NULL                                        -- [S1]\n"
    "   OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-') -- [S2]\n"
    "\n"
    "-- et avec quel cas : le premier WHEN vrai l'emporte\n"
    "CASE\n"
    "    WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
    "     AND pd.date_depart >  dbt.d0\n"
    "     AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'    -- [S3]\n"
    "    WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'\n"
    "     AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'   -- [S4]\n"
    "    WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'\n"
    "     AND pr.disponibilite = 'HORS_EFFECTIF'\n"
    "     AND NVL(po.date_dispo, DATE '1900-01-01')\n"
    "         > NVL(po.date_remise, DATE '1900-01-01')     THEN 'DISPONIBILITE'       -- [S5]\n"
    "    WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'              -- [S6]\n"
    "    ELSE                                                   'SANS_MOTIF'          -- [S7]\n"
    "END                        AS cas")

NOTES_ENTREES = [
    ("[E1] Absent de la photo du début, dans toutes les natures.", "entree"),
    ("[E2] Ou présent, mais dans une autre nature : il entre dans la sienne et sort de l'autre "
     "([S2]). Le NVL fait de « sans situation » une nature à part : sans lui, NULL <> "
     "'PERMANENT' ne serait jamais vrai.", "attention"),
    ("[E3] DETACHEMENT → DETAC · DECISION_37 : une 37 dans l'exercice (detaches_annee).",
     "absent"),
    ("[E4] EMBAUCHE → RECRU · EMBAUCHE : permanent entré dans l'exercice.", "entree"),
    ("[E5] REMISE → REM · DECISION_18 : en disponibilité au 31/12 d'avant ; réglage "
     "HORS_EFFECTIF seulement.", "reglage"),
    ("[E6] ENGAGEMENT → RECRU · ENGAGEMENT_50 : temporaire sans contrat au 31/12 d'avant.",
     "entree"),
    ("[E7] NATURE → AUTRE_ENT · ETAIT_ et la nature du début (TEMPORAIRE, PERMANENT, "
     "SANS_SITUATION).", "entree"),
    ("[E8] SANS_MOTIF → AUTRE_ENT · SANS_MOTIF : rien ne l'explique.", "attention"),
]
NOTES_SORTIES = [
    ("[S1] Absent de la photo de fin, dans toutes les natures.", "sortie"),
    ("[S2] Ou présent, mais dans une autre nature : le pendant de [E2].", "sortie"),
    ("[S3] DEPART_PERMANENT → le type et le motif de son premier départ (pd), s'il tombe dans "
     "l'exercice : RET, DEC, DEM, LIC · DECISION_xx ou DATE_DECES.", "sortie"),
    ("[S4] DEPART_TEMPORAIRE → le type et le motif du dernier départ de l'exercice (dda) : "
     "FIN_CTRAT · DECISION_35 ou 52…", "sortie"),
    ("[S5] DISPONIBILITE → DISP · DECISION_07 : en disponibilité au 31/12 de l'année ; réglage "
     "HORS_EFFECTIF seulement.", "reglage"),
    ("[S6] NATURE → AUTRE_SORT · DEVIENT_ et la nature de fin.", "sortie"),
    ("[S7] SANS_MOTIF → AUTRE_SORT · SANS_MOTIF : aucun départ daté ne l'explique.",
     "attention"),
]

# ---------------------------------------------------------------- 7. comptage et OBIEE
FINAL_SQL = sql(
    "mouvements AS (\n"
    "    SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,   -- G14\n"
    "    …\n"
    "    FROM au_debut\n"
    "    UNION ALL\n"
    "    …\n"
    "    FROM entrees                     -- G8\n"
    "    UNION ALL\n"
    "    …\n"
    "    FROM sorties                     -- G9\n"
    "    UNION ALL\n"
    "    …\n"
    "    FROM passages                    -- G16 : l'entrée (RECRU)…\n"
    "    UNION ALL\n"
    "    …\n"
    "    FROM passages                    -- … puis la sortie\n"
    "    UNION ALL\n"
    "    …\n"
    "    FROM internes                    -- G17\n"
    ")\n"
    "SELECT\n"
    "    TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,\n"
    "    m.CODE_TYPE_MVT,\n"
    "    tm.CODE_NAT_MVT,\n"
    "    …\n"
    "    COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,\n"
    "    COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,\n"
    "    …\n"
    "FROM mouvements m\n"
    "LEFT JOIN DTM.DIM_GRH_TYPE_MOUVEMENT tm\n"
    "       ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT\n"
    "GROUP BY …                           -- les mêmes axes")

OBIEE_SQL = sql(
    "SUM(CASE WHEN CODE_TYPE_MVT = 'DEBUT' THEN NB_AGENTS ELSE 0 END) AS debut,\n"
    "SUM(CASE WHEN CODE_TYPE_MVT = 'RECRU' THEN NB_AGENTS ELSE 0 END) AS recrut,\n"
    "SUM(CASE WHEN CODE_NAT_MVT = 'ENTREES' AND CODE_TYPE_MVT <> 'RECRU'\n"
    "         THEN NB_AGENTS ELSE 0 END)                             AS autres_ent,\n"
    "SUM(CASE WHEN CODE_TYPE_MVT = 'RET' THEN NB_AGENTS ELSE 0 END)   AS retraite,\n"
    "SUM(CASE WHEN CODE_TYPE_MVT = 'DEC' THEN NB_AGENTS ELSE 0 END)   AS deces,\n"
    "SUM(CASE WHEN CODE_NAT_MVT = 'SORTIES'\n"
    "          AND CODE_TYPE_MVT NOT IN ('RET', 'DEC')\n"
    "         THEN NB_AGENTS ELSE 0 END)                             AS autres_sort,\n"
    "SUM(CASE WHEN CODE_NAT_MVT IN ('INITIAL', 'ENTREES') THEN NB_AGENTS\n"
    "         WHEN CODE_NAT_MVT = 'SORTIES' THEN -NB_AGENTS\n"
    "         ELSE 0 END)                                            AS fin\n"
    "FROM ( …la requête… )\n"
    "WHERE QUALITE_EMPLOYE = 'A' AND NATURE_EMPLOI = 'PERMANENT'", entete=True)


def couper(txt, size, largeur, mono=False):
    """Coupe un texte en lignes qui tiennent dans la largeur (mot par mot)."""
    lignes = []
    for para in txt.split("\n"):
        courante = ""
        for mot in para.split(" "):
            essai = f"{courante} {mot}".strip()
            if courante and text_size(essai, size, mono)[0] > largeur:
                lignes.append(courante)
                courante = mot
            else:
                courante = essai
        lignes.append(courante)
    return "\n".join(lignes)


def build():
    if ECARTS:
        sys.exit("Bouts de SQL absents de " + SQL.name + " :\n  " + "\n  ".join(ECARTS))
    d = Diagram(seed=61)

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2, mono=False):
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b, mono=mono)
        return b

    def lignes_de(n):
        a, b = PARTIES[n]
        return f"l. {a} à {b}"

    def tableau(lignes, top, colonnes=COLONNES):
        """Un tableau : une ligne d'en-tête, puis une ligne par cas. Renvoie le bas."""
        xs, x = [], 40
        for _, w in colonnes:
            xs.append(x)
            x += w + GAP_COL
        for (titre, w), xc in zip(colonnes, xs):
            d.text(xc + 4, top, titre, 20, C["doux"])
        y = top + 40
        for cas, mouvement, genre, ou_txt, sql_txt, pourquoi in lignes:
            fond_mvt, bord, sw, style, fond = GENRES[genre]
            w_cas, w_mvt, w_ou, w_sql, w_pq = (w for _, w in colonnes)
            t_cas = couper(cas, 21, w_cas - 40)
            t_ou = couper(ou_txt, 18, w_ou - 30)
            t_pq = couper(pourquoi, 18, w_pq - 40)
            mouvement = couper(mouvement, 18, w_mvt - 40)
            hauteurs = [text_size(t_cas, 21)[1], text_size(mouvement, 18)[1] + 20,
                        text_size(t_ou, 18)[1], text_size(sql_txt, SQL_SIZE, mono=True)[1],
                        text_size(t_pq, 18)[1]]
            h = max(max(hauteurs) + 36, 96)
            g = d.group()
            ligne_box = d.box(40, y, sum(w for _, w in colonnes) + GAP_COL * (len(colonnes) - 1),
                              h, bg=fond, stroke=bord, stroke_width=sw, style=style, groups=g)
            d.text(xs[0] + 18, y + 18, t_cas, 21, groups=g, inside=ligne_box)
            d.box(xs[1], y + 14, w_mvt, h - 28, mouvement, 18, bg=fond_mvt, stroke=bord,
                  style=style, groups=g)
            d.text(xs[2] + 14, y + 18, t_ou, 18, C["bleu"], groups=g, inside=ligne_box)
            code = d.box(xs[3], y + 10, w_sql, h - 20, bg=C["gris"], stroke=C["gris_bord"],
                         stroke_width=1, groups=g)
            d.text(xs[3] + 14, y + 18, sql_txt, SQL_SIZE, groups=g, inside=code, mono=True)
            d.text(xs[4] + 14, y + 18, t_pq, 18, C["doux"], groups=g, inside=ligne_box)
            y += h + 14
        return y - 14

    def notes(liste, x, y, w):
        """Des notes l'une sous l'autre, bordées de la couleur de leur genre. Renvoie le bas."""
        for txt, genre in liste:
            _, bord, sw, style, _ = GENRES[genre]
            t = couper(txt, 19, w - 44)
            h = text_size(t, 19)[1] + 26
            g = d.group()
            b = d.box(x, y, w, h, bg=C["blanc"], stroke=bord, stroke_width=sw, style=style,
                      groups=g)
            d.text(x + 22, y + 13, t, 19, groups=g, inside=b)
            y += h + 10
        return y - 10

    # ------------------------------------------------------------------ titre et légende
    d.text(0, 0, "T60 : les cas de mouvement dans la requête SQL", 48)
    d.text(0, 66, "Version technique de « cas_mouvements_t60 » : chaque cas relié à la partie de "
                  "la requête qui le traite, au bout de SQL qui décide, et à la raison\n"
                  "analyses/grh/t60_requete_proposee.sql · numéros de ligne lus dans le fichier "
                  "à chaque génération · « … » = coupure · les commentaires « -- » disent ce que "
                  "vaut la condition · état au 08/11/2026", 22, C["doux"])
    legende = [("entrée", "entree"), ("sortie", "sortie"), ("aucun mouvement", "rien"),
               ("selon un réglage", "reglage"), ("à surveiller", "attention")]
    for i, (txt, genre) in enumerate(legende):
        fond_mvt, bord, sw, _, _ = GENRES[genre]
        d.box(1620 + i * 276, 8, 256, 44, txt, 18, bg=fond_mvt, stroke=bord, stroke_width=sw)

    # ------------------------------------------------------------------ 1. vue d'ensemble
    s1 = 170
    sec1 = d.section(0, s1, WIDTH, 1500, "1. La requête en un coup d'œil : les parties, leurs "
                                         "CTE (blocs du WITH) et leur grain")
    bloc(40, s1 + 80, 2960, 100,
         f"0 · LES RÉGLAGES ({lignes_de(0)}) : parametres, decisions_depart, codes_decision. "
         "De petites tables écrites dans la requête (SELECT … FROM DUAL),\nlues par les autres "
         "parties avec CROSS JOIN ou JOIN. decisions_depart donne aussi le type de sortie de "
         "chaque décision de départ (02 RET, 34 DEC, 35 et 52 FIN_CTRAT, 36 DEM, 54 LIC).", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))

    yb = s1 + 215
    communes = [
        ("1 · LES DATES", ["annees", "dates", "exercices"],
         "exercices : 1 ligne = 1 année\nd0 = 31/12 d'avant\nd1 = 31/12 de l'année"),
        ("2 · LES AGENTS", ["personnes"],
         "1 ligne = 1 agent : etat,\nqualite, date_entree\nlit FAIT_GRH_PERSONNE"),
        ("3 · LES DÉPARTS", ["decisions", "departs", "premier_depart"],
         "departs : 1 ligne = 1 départ,\navec type_mvt et motif\npremier_depart : 1 par agent"),
        ("4 · LES CONTRATS", ["engagements", "contrats", "temporaires_sous_contrat"],
         "contrats : 1 ligne = 1 contrat\ntemporaires_sous_contrat :\n1 ligne = 1 date × 1 agent"),
        ("5 · LES DISPONIBILITÉS", ["positions"],
         "la dernière 07 et la\ndernière 18, au plus tard\nà chaque date"),
        ("6 · LA SITUATION", ["situations"],
         "rn = 1 : la dernière situation\ncommencée à la date : nature,\nqualification, unité"),
        ("7 · LES PRÉSENTS", ["base"],
         "1 ligne = 1 date × 1 agent\nprésent, avec les règles\nde T58 (le même WHERE)"),
    ]
    w_p = (2960 - 48 - 6 * 20) / 7
    hauteurs = []
    for titre, noms, grain in communes:
        largeur = max(len(n) for n in noms)
        ctes = "\n".join(f"{n:<{largeur}}  l. {cte(n)[0]}" for n in noms)
        hauteurs.append(14 + 30 + 12 + text_size(ctes, 15, mono=True)[1] + 14
                        + text_size(grain, 17)[1] + 18)
    h_p = max(hauteurs)
    g_bande = d.group()
    bande = d.box(40, yb, 2960, 70 + h_p + 24, bg=C["section"], stroke=C["gris_bord"],
                  style="dashed", groups=g_bande)
    d.text(64, yb + 16, "PARTIES 1 À 7 : LES MÊMES QUE LA REQUÊTE DE L'EFFECTIF (le détail de "
                        "leurs cas est dans « les cas d'agents dans la requête SQL », dossier T58 "
                        "effectif)", 20, groups=g_bande, inside=bande)
    for i, (titre, noms, grain) in enumerate(communes):
        x = 64 + i * (w_p + 20)
        y = yb + 70
        largeur = max(len(n) for n in noms)
        ctes = "\n".join(f"{n:<{largeur}}  l. {cte(n)[0]}" for n in noms)
        g = d.group()
        b = d.box(x, y, w_p, h_p, bg=C["blanc"], stroke=C["encre"], groups=g)
        d.text(x + 16, y + 14, titre, 20, groups=g, inside=b)
        d.text(x + 16, y + 56, ctes, 15, groups=g, inside=b, mono=True)
        h_ctes = text_size(ctes, 15, mono=True)[1]
        d.text(x + 16, y + 56 + h_ctes + 14, grain, 17, C["doux"], groups=g, inside=b)
    bas_bande = yb + 70 + h_p + 24

    # partie 8
    y8 = bas_bande + 60
    ctes8 = "\n".join(f"{n:<20}  l. {cte(n)[0]} à {cte(n)[1]}"
                      for n in ("au_debut", "a_la_fin", "dernier_depart_annee", "detaches_annee"))
    texte8 = ("au_debut : les présents au 31/12 d'avant · a_la_fin : les présents au 31/12 de "
              "l'année\n1 ligne = 1 exercice × 1 agent présent, avec sa nature, sa qualité, sa "
              "qualification et son unité à cette date\ndernier_depart_annee : le dernier départ "
              "de chaque agent dans l'exercice (pour les temporaires)\ndetaches_annee : les "
              "agents classés comme détachés (37) dans l'exercice\nlit : base, exercices, "
              "departs, decisions")
    h8 = max(62 + text_size(ctes8, 17, mono=True)[1], 60 + text_size(texte8, 20)[1]) + 28
    g8 = d.group()
    p8 = d.box(40, y8, 2960, h8, bg=C["vert_clair"], stroke=C["vert"], stroke_width=3, groups=g8)
    d.text(70, y8 + 16, f"8 · LES DEUX PHOTOS DE CHAQUE EXERCICE, G14 ({lignes_de(8)})", 22,
           groups=g8, inside=p8)
    d.text(70, y8 + 62, ctes8, 17, groups=g8, inside=p8, mono=True)
    d.text(760, y8 + 60, texte8, 20, groups=g8, inside=p8)
    d.arrow(1520, bas_bande + 6, 1520, y8 - 6, bande, p8, stroke_width=3)

    # parties 9 à 12
    y9 = y8 + h8 + 60
    w9 = (2960 - 3 * 30) / 4
    parties = [
        ("9 · LES ENTRÉES, G8", ["entrees"],
         "1 ligne = 1 entrée : exercice × agent\n× code ; présent à la fin, absent de\nsa "
         "nature au début\nlit : a_la_fin, au_debut, personnes,\ndetaches_annee, positions,\n"
         "temporaires_sous_contrat", (C["vert_clair"], C["vert"])),
        ("10 · LES SORTIES, G9", ["sorties"],
         "1 ligne = 1 sortie ; présent au début,\nabsent de sa nature à la fin\nlit : au_debut, "
         "a_la_fin, premier_depart,\ndernier_depart_annee, positions", (C["orange_clair"],
                                                                         C["orange"])),
        ("11 · ENTRÉ ET SORTI DANS L'ANNÉE, G16", ["contrats_annee", "passages"],
         "absent des deux photos, présent entre\nles deux ; des lignes seulement avec\n"
         "passages_annee = 'OUI'", (C["jaune_clair"], C["jaune_bord"])),
        ("12 · LES MOUVEMENTS INTERNES, G17", ["internes"],
         "1 ligne = 1 mouvement validé (O) de\nGRH_MOUVEMENT : MUT, AFF, NOM…,\ncomme avant ; "
         "hors T60\nlit FAIT_GRH_MOUVEMENT", (C["blanc"], C["encre"])),
    ]
    def ctes_de(noms):
        largeur = max(len(n) for n in noms)
        return "\n".join(f"{n:<{largeur}}  l. {cte(n)[0]} à {cte(n)[1]}" for n in noms)

    h9 = max(56 + text_size(ctes_de(noms), 16, mono=True)[1] + 14 + text_size(texte, 19)[1]
             for _, noms, texte, _ in parties) + 22
    boites9 = []
    for i, (titre, noms, texte, couleurs) in enumerate(parties):
        x = 40 + i * (w9 + 30)
        g = d.group()
        b = d.box(x, y9, w9, h9, bg=couleurs[0], stroke=couleurs[1], stroke_width=2, groups=g)
        d.text(x + 20, y9 + 14, titre, 21, groups=g, inside=b)
        ctes = ctes_de(noms)
        d.text(x + 20, y9 + 56, ctes, 16, groups=g, inside=b, mono=True)
        h_ctes = text_size(ctes, 16, mono=True)[1]
        d.text(x + 20, y9 + 56 + h_ctes + 14, texte, 19, C["doux"], groups=g, inside=b)
        boites9.append(b)
    for b in boites9[:3]:
        cx = b["x"] + b["width"] / 2
        d.arrow(cx, y8 + h8 + 6, cx, y9 - 6, p8, b, stroke_width=2)

    # parties 13 et 14, puis OBIEE
    y13 = y9 + h9 + 60
    a13, b13 = cte("mouvements")
    p13 = bloc(40, y13, 960, 200,
               f"13 · TOUT ENSEMBLE · mouvements (l. {a13} à {b13})\nUNION ALL de six branches : "
               "DEBUT (au_debut),\nentrees, sorties, passages deux fois (l'entrée,\npuis la "
               "sortie), internes\n1 ligne = 1 agent × exercice × code", 20)
    p14 = bloc(1040, y13, 960, 200,
               f"14 · LE COMPTAGE ({lignes_de(14)})\nGROUP BY exercice, code, qualification, "
               "sexe,\nqualité, nature d'emploi, unité, motif ;\nla famille et le sens viennent "
               "de\nDIM_GRH_TYPE_MOUVEMENT → DTM_GRH_MOUVEMENT", 20)
    obiee = d.box(2040, y13, 960, 200,
                  "PUIS OBIEE : LE TABLEAU T60\nfiltres qualité A et nature d'emploi ;\nles "
                  "colonnes de la maquette (partie 7)", 21, bg=C["violet"], stroke=C["encre"])
    for b in boites9:
        cx = b["x"] + b["width"] / 2
        tx = min(max(cx, 100), 940)
        d.arrow(cx, y9 + h9 + 6, tx, y13 - 6, b, p13, stroke_width=1, color=C["doux"])
    d.harrow(p13, p14, stroke_width=3)
    d.harrow(p14, obiee, stroke_width=3)

    y_al = y13 + 200 + 40
    bloc(40, y_al, 2960, 170,
         "LES ALIAS, À GARDER SOUS LES YEUX\n"
         "e = l'exercice (annee, d0, d1) · f = la photo de fin, vue d'une entrée · dbt = la photo "
         "du début · fin = la photo de fin, vue d'une sortie · p = l'agent · pr = les réglages · "
         "cd = les codes de décision\n"
         "pd = son premier départ · dda = son dernier départ de l'exercice · dt = sa 37 de "
         "l'exercice · po = ses 07 et 18 (au 31/12 d'avant pour une entrée, de l'année pour une "
         "sortie) · tc0 = son contrat au début\n"
         "Un LEFT JOIN qui ne trouve rien laisse ses colonnes à NULL : c'est souvent ce NULL qui "
         "décide (dbt.PERS_ID IS NULL = absent de la photo du début).", 20)
    sec1["height"] = y_al + 170 + 40 - s1

    # ------------------------------------------------------------------ 2. le cœur
    s2 = sec1["y"] + sec1["height"] + 50
    sec2 = d.section(0, s2, WIDTH, 1500, "2. Le cœur : deux photos, puis deux comparaisons "
                                         "(parties 8 à 10)")
    h_ph = text_size(PHOTOS_SQL, SQL_SIZE, mono=True)[1] + 40
    bloc(40, s2 + 80, 1440, h_ph, PHOTOS_SQL, SQL_SIZE, couleurs=(C["gris"], C["gris_bord"]),
         mono=True)
    bloc(1520, s2 + 80, 1480, h_ph,
         couper("LES DEUX PHOTOS\nbase (partie 7) dit qui est présent à chaque 31/12, avec les "
                "règles de T58. Chaque exercice en prend deux photos : au_debut, le 31/12 "
                "d'avant, et a_la_fin, le 31/12 de l'année. Une ligne par agent présent, avec sa "
                "nature, sa qualité, sa qualification et son unité à cette date.\nPuis la "
                "requête compare les deux photos, agent par agent et nature par nature : dans "
                "un sens pour les entrées, dans l'autre pour les sorties.", 20, 1480 - 50), 20,
         couleurs=(C["vert_clair"], C["vert"]))

    y_col = s2 + 80 + h_ph + 50
    w_col = 1460
    x_s = 40 + w_col + 40
    d.text(40, y_col, f"LES ENTRÉES (partie 9, {lignes_de(9)}) : qui entre, et avec quel cas", 22,
           C["vert"])
    d.text(x_s, y_col, f"LES SORTIES (partie 10, {lignes_de(10)}) : qui sort, et avec quel cas",
           22, C["orange"])
    h_code = max(text_size(ENTREES_SQL, SQL_SIZE, mono=True)[1],
                 text_size(SORTIES_SQL, SQL_SIZE, mono=True)[1]) + 40
    bloc(40, y_col + 44, w_col, h_code, ENTREES_SQL, SQL_SIZE,
         couleurs=(C["gris"], C["vert"]), mono=True)
    bloc(x_s, y_col + 44, w_col, h_code, SORTIES_SQL, SQL_SIZE,
         couleurs=(C["gris"], C["orange"]), mono=True)
    y_notes = y_col + 44 + h_code + 24
    bas_e = notes(NOTES_ENTREES, 40, y_notes, w_col)
    bas_s = notes(NOTES_SORTIES, x_s, y_notes, w_col)
    y_trad = max(bas_e, bas_s) + 30
    a_ce, b_ce = ligne(9, "CASE x.cas", 1), ligne(9, "AS MOTIF_MVT")
    a_cs, b_cs = ligne(10, "CASE x.cas", 1), ligne(10, "AS MOTIF_MVT")
    bloc(40, y_trad, 2960, 90,
         "Le CASE s'arrête au premier WHEN vrai : l'ordre est une règle. Le cas n'est pas une "
         f"colonne de la table : deux autres CASE x.cas le traduisent en CODE_TYPE_MVT et en "
         f"MOTIF_MVT\n(entrées l. {a_ce} à {b_ce}, sorties l. {a_cs} à {b_cs}). Un agent fait au "
         "plus une entrée et une sortie par exercice et par nature.", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec2["height"] = y_trad + 90 + 40 - s2

    # ------------------------------------------------------------------ 3 à 6 : tableaux
    s = sec2["y"] + sec2["height"] + 50
    sections = [
        ("3. Les entrées (règle G8)", ENTREES, COLONNES),
        ("4. Les sorties (règle G9)", SORTIES, COLONNES),
        ("5. Les cas sans mouvement dans T60", SANS_MOUVEMENT, COLONNES),
        ("6. Puis chaque mouvement est rangé : les axes et les mesures", RANGEMENT,
         COLONNES_RANGEMENT),
    ]
    for titre, lignes, colonnes in sections:
        sec = d.section(0, s, WIDTH, 500, titre)
        bas = tableau(lignes, s + 80, colonnes)
        sec["height"] = bas + 40 - s
        s = bas + 40 + 50

    # ------------------------------------------------------------------ 7. comptage et OBIEE
    sec7 = d.section(0, s, WIDTH, 900, "7. Le comptage final (parties 13 et 14), puis le "
                                       "tableau T60 dans OBIEE")
    h_final = text_size(FINAL_SQL, SQL_SIZE, mono=True)[1] + 40
    h_obiee = text_size(OBIEE_SQL, SQL_SIZE, mono=True)[1] + 40
    bloc(40, s + 80, 1440, h_final, FINAL_SQL, SQL_SIZE, couleurs=(C["gris"], C["gris_bord"]),
         mono=True)
    d.text(1520, s + 80, "LA REQUÊTE DE CONTRÔLE DE L'EN-TÊTE : LES COLONNES DE T60", 20,
           C["doux"])
    bloc(1520, s + 120, 1480, h_obiee, OBIEE_SQL, SQL_SIZE, couleurs=(C["violet"], C["encre"]),
         mono=True)
    y_ret = s + 120 + h_obiee + 30
    h_ret = max(s + 80 + h_final - y_ret, 300)
    bloc(1520, y_ret, 1480, h_ret,
         couper("À RETENIR\nUne ligne de mouvements = un agent, un exercice, un code. On "
                "additionne NB_AGENTS sur tous les axes d'un même exercice, jamais les lignes "
                "DEBUT de deux exercices.\nDébut + entrées − sorties = fin, par nature, qualité "
                "et sexe, et la fin retombe sur l'effectif de T58 (vérifié). Par unité, non : "
                "voir la partie 6.\nVérifié : chaque cas des parties 3 à 5 rejoué dans la "
                "requête, un agent fictif par cas, avec les quatre combinaisons des réglages "
                "disponibilite et passages_annee.\nLire avec : cas_mouvements_t60 (la version "
                "simple) et requete_mouvement_pas_a_pas (six agents rejoués).", 20, 1480 - 50),
         20, couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec7["height"] = max(s + 80 + h_final, y_ret + h_ret) + 40 - s
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
