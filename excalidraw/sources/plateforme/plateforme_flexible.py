"""
La plateforme BI flexible (proposition du 20/11/2026).

Demande de l'utilisateur : proposer une plateforme ETL de data engineering, en
pensant à Fivetran et Airflow, « avec une excellente flexibilité afin de
répondre à n'importe quel projet BI », sans être tenu de rester proche du code
CNSS. Elle remplace la première approche, partie de ce code
(schemas/archives/plateforme, script vision_plateforme.py).

Sept schémas, dans schemas/plateforme/ :
  00 vision et principes   l'idée, les 8 principes, face à Fivetran, Airflow,
                           dbt, ce qu'on garde de la CNSS
  01 architecture          plan de contrôle, plan de données, socle ; les
                           façons de l'installer ; les briques techniques
  02 modele de projet      les 7 blocs, un dossier de projet, un indicateur
                           avec ses règles, la porte de sortie
  03 flexibilite           ce qui varie d'un projet à l'autre et la réponse ;
                           les limites ; la preuve par deux projets
  04 feuille de route      les 5 étapes, la CNSS pendant ce temps, points
                           d'attention, à trancher (« pour qui ? » décidé)
  05 produit optiware      décision du 20/11 : un produit vendu ; ce que ça
                           change, modèle économique, premiers clients,
                           ajouts aux étapes, conditions de réussite
  06 console               les écrans en maquettes (accueil, exécutions, lignage,
                           indicateurs, sans code, administration), les profils,
                           le principe console = code, ce qu'elle ne fait pas,
                           quand chaque écran arrive

Couleurs : jaune = à construire ; gris = existe ailleurs (open source, chez le
client) ; violet = le métier ; vert = ce qui est acquis ou validé ; rouge =
limites et risques.

Usage :
    python3 excalidraw/sources/plateforme/plateforme_flexible.py [--force]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "vues_cnss"))

from excalidraw_builder import C, Diagram, text_size  # noqa: E402
from commun import WIDTH, bloc, couper, entete, rangee, tableau  # noqa: E402

OUT_DIR = ROOT / "schemas" / "plateforme"
DATE = "état au 20/11/2026"

VERT = (C["vert_clair"], C["vert"])
JAUNE = (C["jaune_clair"], C["jaune_bord"])
GRIS = (C["gris"], C["gris_bord"])
BLANC = (C["blanc"], C["encre"])
ROUGE = (C["rouge_clair"], C["rouge"])
VIOLET = ("#f3f0ff", "#7048e8")


# ---------------------------------------------------------------- mise en page
def section(d, top, titre):
    return d.section(0, top, WIDTH, 300, titre)


def fermer(sec, bas):
    """Ajuste la hauteur d'une section à son contenu ; renvoie le haut de la suivante."""
    sec["height"] = bas + 40 - sec["y"]
    return bas + 40 + 50


def hauteur(txt, size, w):
    return text_size(couper(txt, size, w - 44), size)[1] + 34


def ligne_de_cadres(d, top, textes, size=20, x=40, largeur=WIDTH - 80, gap=50, fleches=False,
                    sw=2):
    """Des cadres côte à côte, de même hauteur ; textes : liste de (texte, couleurs).
    Renvoie la liste des cadres."""
    n = len(textes)
    w = (largeur - (n - 1) * gap) / n
    h = max(hauteur(t, size, w) for t, _ in textes)
    cadres = [bloc(d, x + i * (w + gap), top, w, t, size, c, sw=sw, h=h)
              for i, (t, c) in enumerate(textes)]
    if fleches:
        for a, b in zip(cadres, cadres[1:]):
            d.harrow(a, b, stroke_width=3)
    return cadres


def conteneur(d, x, y, w, titre, couleur=C["gris_bord"]):
    """Un cadre pointillé blanc avec un titre ; fixer sa hauteur ensuite."""
    c = d.box(x, y, w, 100, bg=C["blanc"], stroke=couleur, style="dashed", stroke_width=2)
    d.text(x + 25, y + 18, titre, 24, C["doux"])
    return c


def code_et_explication(d, top, code, expl, code_w=1000, size_code=20, size_expl=21,
                        couleurs=VERT):
    """Un cadre de code à gauche, un cadre d'explication à droite, de même hauteur.
    Renvoie le bas."""
    ex = 40 + code_w + 60
    ew = WIDTH - 40 - ex
    _, ch = text_size(code, size_code, mono=True)
    h = max(ch + 50, hauteur(expl, size_expl, ew))
    g = d.group()
    cadre = d.box(40, top, code_w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(65, top + 25, code, size_code, groups=g, inside=cadre, mono=True)
    bloc(d, ex, top, ew, expl, size_expl, couleurs, sw=2, h=h)
    return top + h


# ======================================================= 00 vision et principes
def vision():
    d = Diagram(seed=2101)
    s = entete(d, "Une plateforme BI de bout en bout, flexible",
               "Nouvelle proposition, pensée sans partir du code CNSS : l'idée, les 8 principes, "
               "sa place face à Fivetran, Airflow et dbt · " + DATE + " · elle remplace la "
               "première approche, rangée dans archives/plateforme")

    sec = section(d, s, "1. L'idée")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             "UN SEUL MODÈLE DE PROJET, DE LA SOURCE À L'INDICATEUR VALIDÉ.\nUn projet BI se "
             "décrit dans un dossier (fichiers YAML et SQL, suivi par git) : où sont les données, "
             "quoi lire, comment transformer, quels indicateurs, quels contrôles, vers quels "
             "outils. La plateforme en déduit tout le reste : l'ordre des traitements, les "
             "relances, le lignage, la doc, la qualité.\nSous ce modèle, tout est "
             "interchangeable : la source, l'entrepôt, l'outil BI, le moteur d'exécution, le "
             "lieu (cloud, sur site, hors ligne). C'est ce qui permet de répondre à n'importe "
             "quel projet BI.", 24, VIOLET, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    sec = section(d, s, "2. Les 8 principes")
    l1 = ligne_de_cadres(d, s + 80, [
        ("1 · DÉCLARER, PAS PROGRAMMER\nOn dit ce qu'on veut obtenir (des tables, des "
         "indicateurs), pas l'ordre des tâches. La plateforme calcule l'ordre.", JAUNE),
        ("2 · PENSER EN RÉSULTATS\nChaque table et chaque indicateur est un objet suivi : d'où "
         "il vient, quand il a été calculé, s'il est à jour, s'il est validé.", JAUNE),
        ("3 · TOUT SE BRANCHE\nSources, entrepôts, outils BI, moteurs, coffres de secrets, "
         "alertes : des plugins, avec un contrat stable et versionné.", JAUNE),
        ("4 · LE SQL ÉCRIT UNE FOIS\nIl est traduit dans le dialecte de chaque entrepôt (Oracle, "
         "PostgreSQL, SQL Server…). Changer d'entrepôt ne force pas à tout réécrire.", JAUNE),
    ], size=21, gap=40)
    y = l1[0]["y"] + l1[0]["height"] + 40
    l2 = ligne_de_cadres(d, y, [
        ("5 · LE TEMPS EST UNE PARTITION\nJour, mois, trimestre, ou pays, agence : on calcule, "
         "relance ou rattrape une partition précise. La photo mensuelle de la CNSS n'est qu'un "
         "cas.", JAUNE),
        ("6 · LE MÉTIER DEDANS\nUn indicateur porte sa définition, ses règles et leurs sources, "
         "ses contrôles et son statut de validation.", VIOLET),
        ("7 · UNE PORTE DE SORTIE PARTOUT\nCe que le modèle ne prévoit pas s'écrit en SQL ou en "
         "Python libre, dans le même projet, suivi comme le reste. On n'est jamais bloqué.",
         JAUNE),
        ("8 · LES DONNÉES RESTENT CHEZ LE CLIENT\nLe cerveau peut être dans le cloud ou sur "
         "site ; le travail sur les données se fait par un agent installé chez le client, même "
         "hors ligne.", JAUNE),
    ], size=21, gap=40)
    s = fermer(sec, l2[0]["y"] + l2[0]["height"])

    sec = section(d, s, "3. Face à Fivetran, Airflow, dbt et la « modern data stack »")
    bas = tableau(d, s + 80, [
        ("Outil", 380, {"size": 20}),
        ("Ce qu'il fait bien", 560, {"size": 19}),
        ("Ce qui manque pour un projet BI complet", 640, {"size": 19, "couleur": C["rouge"]}),
        ("Notre plateforme", 640, {"size": 19}),
    ], [
        ("Fivetran", "Lire des centaines de sources sans écrire de code",
         "Ne transforme pas, n'enchaîne pas le reste ; service en ligne payé au volume de "
         "lignes",
         "Des connecteurs branchables, compatibles avec des catalogues de connecteurs existants"),
        ("Airflow", "Enchaîner et planifier n'importe quelle tâche",
         "Tout s'écrit en Python ; il ne connaît ni les tables ni les indicateurs",
         "L'enchaînement est déduit du projet ; Airflow peut servir de moteur chez un client "
         "qui l'a déjà"),
        ("dbt", "Transformer en SQL, avec dépendances, tests et docs",
         "Ne lit pas les sources, ne planifie pas ; le SQL est écrit pour un entrepôt donné",
         "Les modèles SQL au même endroit que le reste, traduits pour chaque entrepôt"),
        ("La « modern data stack » : Fivetran + Airflow + dbt + catalogue + couche sémantique",
         "Chaque brique est parmi les meilleures de sa catégorie",
         "5 ou 6 outils à acheter, relier, apprendre et faire tourner ; aucun ne connaît le "
         "métier",
         "Un seul projet de bout en bout, et un seul endroit pour comprendre d'où vient un "
         "chiffre"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "4. Ce qu'on garde du projet CNSS")
    bas = rangee(d, s + 80, [
        ("LES LEÇONS DU MÉTIER\nRègles avec leurs sources, requêtes de preuve, comparaison à une "
         "publication, validation par le métier, case vide jamais 0. Elles deviennent des "
         "fonctions de la plateforme.", VIOLET, 2),
        ("L'EXPÉRIENCE DU TERRAIN\nOracle ancien, serveur sur site, pas toujours d'Internet, "
         "petite équipe. La plateforme est conçue d'abord pour ces conditions : qui peut le "
         "plus peut le moins.", VERT, 2),
        ("LE PREMIER PROJET\nLa CNSS devient le premier projet de la plateforme, et le pack "
         "CIPRES son premier pack. Le code Python actuel n'est pas repris tel quel ; ses "
         "requêtes SQL et ses règles, oui.", VERT, 2),
    ], size=21)
    fermer(sec, bas)
    return d


# ======================================================= 01 architecture
def architecture():
    d = Diagram(seed=2102)
    s = entete(d, "Architecture de la plateforme flexible",
               "Le cerveau (plan de contrôle), les bras chez le client (plan de données), le "
               "socle commun, les façons de l'installer et les briques techniques · " + DATE +
               " · jaune = à construire, gris = existe ailleurs (open source, chez le client), "
               "violet = le métier")

    sec = section(d, s, "1. Vue d'ensemble")
    top = s + 80

    # ---- le plan de contrôle
    cp = conteneur(d, 40, top, WIDTH - 80,
                   "PLAN DE CONTRÔLE · le cerveau · dans le cloud ou sur site")
    cerveau = ligne_de_cadres(d, top + 70, [
        ("PROJETS\ndossiers YAML, SQL et Python, suivis par git ; packs installés", VIOLET),
        ("COMPILATEUR\nvérifie le projet · calcule les dépendances · traduit le SQL pour "
         "l'entrepôt · calcule le lignage · produit le plan", JAUNE),
        ("PLANIFICATEUR\ndécide quoi calculer et quand : horaire, événement, arrivée d'un "
         "fichier, fraîcheur attendue · partitions · reprises · rattrapages", JAUNE),
        ("MÉTADONNÉES\nruns, états (curseurs), lignage, résultats des contrôles, catalogue, "
         "validations", JAUNE),
        ("INTERFACES\nconsole web · API · ligne de commande · alertes (mail, Teams, "
         "WhatsApp)", JAUNE),
    ], size=20, x=70, largeur=WIDTH - 140, fleches=True)
    cp["height"] = cerveau[0]["y"] + cerveau[0]["height"] + 30 - top

    # ---- le plan de données
    dp_top = top + cp["height"] + 170
    dp = conteneur(d, 40, dp_top, WIDTH - 80,
                   "PLAN DE DONNÉES · les bras · chez le client : les données ne sortent pas")
    col_w, gap = 335, 50
    xs = [70 + i * (col_w + gap) for i in range(6)]
    agent = bloc(d, xs[1], dp_top + 70, xs[4] + col_w - xs[1],
                 "AGENT D'EXÉCUTION : il reçoit les tâches, lance les connecteurs et les "
                 "transformations, renvoie l'état. Un ou plusieurs par site ; il marche hors "
                 "ligne et peut être remplacé par Dagster ou Airflow", 21, JAUNE, sw=3)
    flux = [
        ("SOURCES\nbases (Oracle, SQL Server, MySQL…), ERP, fichiers Excel et CSV, API, SFTP",
         GRIS),
        ("CONNECTEURS\nplugins, protocole standard ; complet ou seulement le nouveau ; les "
         "données passent en Arrow", JAUNE),
        ("ZONE BRUTE\nla copie des sources, datée, rejouable", JAUNE),
        ("MODÈLES\nles transformations SQL ou Python, en couches libres, calculées dans "
         "l'entrepôt", JAUNE),
        ("COUCHE SÉMANTIQUE\nles indicateurs, définis une fois, servis en vues SQL, API et "
         "extraits", VIOLET),
        ("USAGES\nOAC, Power BI, Superset, Metabase, Excel, autres applications", GRIS),
    ]
    y_flux = agent["y"] + agent["height"] + 80
    h_flux = max(hauteur(t, 20, col_w) for t, _ in flux)
    boites = [bloc(d, x, y_flux, col_w, t, 20, c, sw=2, h=h_flux) for (t, c), x in zip(flux, xs)]
    for a, b in zip(boites, boites[1:]):
        d.harrow(a, b, stroke_width=3)
    for b in boites[1:5]:
        cx = b["x"] + b["width"] / 2
        d.arrow(cx, agent["y"] + agent["height"] + 6, cx, b["y"] - 6, agent, b, style="dashed",
                color=C["jaune_bord"])
    entrepot = bloc(d, xs[2], y_flux + h_flux + 30, xs[3] + col_w - xs[2],
                    "Zone brute et modèles vivent dans l'entrepôt du client (Oracle, PostgreSQL, "
                    "SQL Server, Snowflake, BigQuery, ClickHouse…) ou, s'il n'en a pas, dans le "
                    "lakehouse intégré (fichiers Parquet + DuckDB)", 19, GRIS)
    dp["height"] = entrepot["y"] + entrepot["height"] + 30 - dp_top

    # ---- les échanges entre le cerveau et les bras
    bas_cp = top + cp["height"]
    x_down, x_up = agent["x"] + 300, agent["x"] + agent["width"] - 300
    d.arrow(x_down, bas_cp + 6, x_down, agent["y"] - 6, cp, agent, stroke_width=3)
    d.text(x_down + 20, bas_cp + 55, "le plan, les tâches", 21, C["doux"])
    d.arrow(x_up, agent["y"] - 6, x_up, bas_cp + 6, agent, cp, stroke_width=3)
    d.text(x_up - 20 - text_size("états, journaux, contrôles (jamais les données)", 21)[0],
           bas_cp + 55, "états, journaux, contrôles (jamais les données)", 21, C["doux"])

    # ---- le socle commun
    y_socle = dp_top + dp["height"] + 50
    d.text(40, y_socle, "LE SOCLE COMMUN", 24, C["doux"])
    socle = ligne_de_cadres(d, y_socle + 50, [
        ("QUALITÉ ET CONTRATS\ntests, contrats avec les sources, écart avec une publication, "
         "blocage de la publication", JAUNE),
        ("SÉCURITÉ\ncoffre de secrets, droits par rôle, données personnelles masquées, journal "
         "d'audit", JAUNE),
        ("OBSERVABILITÉ\ndurées, volumes, erreurs, fraîcheur ; standard OpenTelemetry", JAUNE),
        ("LIGNAGE ET DOCS\nde l'indicateur à la colonne source ; standard OpenLineage ; docs "
         "générées", JAUNE),
        ("PACKS\nmodèles, indicateurs, règles et paramètres prêts à l'emploi : CIPRES, "
         "finance, RH…", VIOLET),
        ("DÉPLOIEMENT\nDocker Compose (archive hors ligne), Kubernetes, cloud", JAUNE),
    ], size=20, gap=40)
    s = fermer(sec, socle[0]["y"] + socle[0]["height"])

    sec = section(d, s, "2. Quatre façons de l'installer, avec le même produit")
    bas = rangee(d, s + 80, [
        ("TOUT SUR SITE, HORS LIGNE\nUn serveur : le cerveau et l'agent ensemble, installés par "
         "une archive, sans Internet. Pour une caisse comme la CNSS.", JAUNE, 2),
        ("HYBRIDE\nLe cerveau chez nous, dans le cloud ; l'agent chez le client. Les données ne "
         "sortent pas, seuls les états remontent.", JAUNE, 2),
        ("CLOUD\nTout dans le cloud, pour les clients dont les données y sont déjà.", JAUNE, 2),
        ("CHEZ UN CLIENT QUI A DÉJÀ AIRFLOW OU DAGSTER\nLe compilateur produit le plan, son "
         "moteur le joue. On s'intègre dans l'existant.", JAUNE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "3. Les briques techniques proposées")
    bas = tableau(d, s + 80, [
        ("Brique", 330, {"size": 20}),
        ("Proposition", 880, {"size": 19}),
        ("Pourquoi", 760, {"size": 19, "couleur": C["doux"]}),
        ("On…", 230, {"size": 19}),
    ], [
        ("Langage du cœur", "Python 3.12",
         "L'équipe le connaît ; le calcul lourd passe par Arrow et par l'entrepôt", "construit"),
        ("Format des projets", "YAML vérifié par des schémas (Pydantic, JSON Schema)",
         "Lisible, vérifié avant de lancer ; l'éditeur aide à l'écrire", "construit"),
        ("SQL", "SQLGlot pour lire le SQL, le traduire d'un dialecte à l'autre et calculer le "
         "lignage par colonne ; Jinja pour les paramètres",
         "Écrire le SQL une fois pour plusieurs entrepôts", "reprend"),
        ("Transfert des données", "Apache Arrow (et ADBC), Parquet pour le passage ; le "
         "chargement natif de chaque entrepôt (chargement direct Oracle, COPY PostgreSQL, BCP "
         "SQL Server)", "Bien plus rapide que l'insertion ligne par ligne", "reprend"),
        ("Connecteurs", "Un SDK maison ; compatibilité avec dlt et avec le protocole "
         "Airbyte/Singer, à évaluer", "Réutiliser des centaines de connecteurs existants",
         "les deux"),
        ("Moteur local, lakehouse", "DuckDB sur des fichiers Parquet",
         "Pour les clients sans entrepôt, et pour tester un projet sur un portable", "reprend"),
        ("Exécution", "Un moteur léger maison (un serveur : dépendances, reprises, "
         "partitions) ; Dagster ou Airflow 3 pour les gros besoins",
         "Léger et hors ligne par défaut, sans refaire la haute disponibilité", "les deux"),
        ("Métadonnées", "PostgreSQL", "Fiable, gratuit, connu partout", "reprend"),
        ("API et console", "FastAPI ; console en React",
         "React est déjà utilisé par la visionneuse Excalidraw du projet", "construit"),
        ("Lignage, observabilité", "OpenLineage ; OpenTelemetry",
         "Des standards : on se branche sur OpenMetadata, DataHub, Grafana", "reprend"),
        ("Déploiement", "Docker Compose (archive hors ligne) ; Helm pour Kubernetes",
         "Le même produit sur site et dans le cloud", "construit"),
    ])
    fermer(sec, bas)
    return d


# ======================================================= 02 modele de projet
ARBRE = """\
cnss_grh/
  projet.yml           # nom, environnements, packs
  parametres/
    dev.yml  prod.yml  # schémas, seuils, choix métier
  connexions.yml       # base GRH, entrepôt
                       # (mots de passe au coffre)
  sources/
    grh.yml            # tables à lire, mode, fréquence
  modeles/
    brut/              # la copie des sources
    propre/            # nettoyé, relié
    indicateurs/
      effectif.sql
  indicateurs/
    t58_effectif.yml   # définition, règles, validation
  controles/
    t58.yml
  livraisons/
    oac.yml"""

INDICATEUR = """\
indicateur: t58_effectif_permanent
libelle: Effectif du personnel permanent (T58)
modele: indicateurs/effectif
mesure: count_distinct(matricule)
axes: [categorie, sexe, tranche_age]
partition: mois            # la photo mensuelle

regles:
  - id: R1
    texte: Un agent en disponibilité sort
           de l'effectif
    source: Annuaire CNSS 2024, partie IX
  - id: R2
    texte: Sans donnée, la case reste vide,
           jamais 0

controles:
  - ecart_max_pct: 2
    avec: publication.annuaire_2024.t58

validation:
  statut: a_valider
  par: DRH"""


def modele_de_projet():
    d = Diagram(seed=2103)
    s = entete(d, "Le modèle de projet : décrire n'importe quel projet BI",
               "Les 7 blocs d'un projet, à quoi ressemble un dossier de projet, un indicateur "
               "avec ses règles et sa validation, la porte de sortie · exemple illustratif tiré "
               "de la GRH de la CNSS (noms de colonnes et règles à titre d'exemple) · " + DATE)

    sec = section(d, s, "1. Les 7 blocs d'un projet")
    blocs = ligne_de_cadres(d, s + 80, [
        ("CONNEXIONS\noù sont les données : bases, fichiers, API, l'entrepôt. Les mots de passe "
         "restent dans le coffre", JAUNE),
        ("SOURCES\nquoi lire : tables, fichiers, appels d'API ; en entier ou seulement le "
         "nouveau", JAUNE),
        ("MODÈLES\ncomment transformer : SQL ou Python ; couches libres ; matérialisés en vue, "
         "table, incrémental, photo ou historique", JAUNE),
        ("INDICATEURS\nce que voit le métier : mesure, axes d'analyse, règles et leurs "
         "sources, responsable, validation", VIOLET),
        ("CONTRÔLES\nce qui doit être vrai : tests, contrat avec la source, écart maximal avec "
         "une publication ; un contrôle rouge peut bloquer la publication", JAUNE),
        ("LIVRAISONS\nvers où et quand : outils BI, API, Excel ; fraîcheur attendue (par "
         "exemple : le 5 du mois)", JAUNE),
    ], size=20, gap=45, fleches=True)
    y = blocs[0]["y"] + blocs[0]["height"] + 40
    b = bloc(d, 40, y, WIDTH - 80,
             "7 · PARAMÈTRES, pour tous les blocs : ce qui change d'un client, d'un pays ou d'un "
             "environnement à l'autre (développement, recette, production) : noms de schémas, "
             "seuils, dates d'arrêt, choix métier. Un pack + des paramètres = un nouveau "
             "déploiement, sans toucher au reste.", 21, VIOLET, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    sec = section(d, s, "2. Un dossier de projet")
    bas = code_et_explication(d, s + 80, ARBRE,
                              "UN PROJET = UN DOSSIER, suivi par git\n"
                              "· chaque bloc a son fichier ou son sous-dossier ; un gros projet "
                              "découpe ses fichiers par domaine\n"
                              "· les couches de modèles (brut, propre, indicateurs) sont un "
                              "choix du projet, pas de la plateforme\n"
                              "· les paramètres de production ne contiennent aucun mot de "
                              "passe : seulement le nom du secret dans le coffre\n"
                              "· le même dossier s'écrit à la main, se lit dans la console, ou "
                              "se génère par l'interface sans code\n"
                              "· avant chaque mise en production, la plateforme vérifie le "
                              "projet : fichiers, dépendances, SQL, contrôles", code_w=960)
    s = fermer(sec, bas)

    sec = section(d, s, "3. Un indicateur, avec ses règles et sa validation")
    bas = code_et_explication(d, s + 80, INDICATEUR,
                              "CE QUE LA PLATEFORME EN FAIT\n"
                              "· elle calcule l'indicateur pour chaque mois, et seulement les "
                              "mois à refaire\n"
                              "· elle le compare au chiffre publié dans l'Annuaire ; au-delà de "
                              "2 % d'écart, il n'est pas publié et la DRH est prévenue\n"
                              "· la console montre la définition, les règles avec leurs "
                              "sources, le lignage jusqu'aux colonnes source et le statut "
                              "« à valider »\n"
                              "· la DRH valide dans la console ; la validation est datée et "
                              "gardée, et tombe si une règle change\n"
                              "· l'indicateur est servi de la même façon à OAC, Power BI ou "
                              "Excel\n\n"
                              "Ce que la CNSS fait aujourd'hui à la main (fiches, points à "
                              "valider, requêtes de preuve) devient une fonction de la "
                              "plateforme.", code_w=960, couleurs=VIOLET)
    s = fermer(sec, bas)

    sec = section(d, s, "4. La porte de sortie")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             "QUAND LE MODÈLE NE PRÉVOIT PAS UN CAS :\n"
             "· un modèle en Python libre (lire une API exotique, un calcul statistique)\n"
             "· un SQL brut, propre à un entrepôt, sans traduction\n"
             "· un plugin maison : connecteur, contrôle, livraison, alerte\n"
             "· des crochets avant et après chaque étape\n"
             "Ce code vit dans le même projet : il est planifié, suivi, contrôlé et documenté "
             "comme le reste. C'est ce qui permet de dire « n'importe quel projet BI » sans "
             "devoir tout prévoir.", 22, JAUNE, sw=3)
    fermer(sec, b["y"] + b["height"])
    return d


# ======================================================= 03 flexibilite
def flexibilite():
    d = Diagram(seed=2104)
    s = entete(d, "La flexibilité, point par point",
               "Ce qui change d'un projet BI à l'autre et comment la plateforme l'absorbe sans "
               "toucher à son cœur · les limites · la preuve par deux projets · " + DATE)

    sec = section(d, s, "1. Ce qui varie d'un projet à l'autre, et la réponse de la plateforme")
    bas = tableau(d, s + 80, [
        ("Ce qui varie", 330, {"size": 21}),
        ("Exemples", 760, {"size": 19, "couleur": C["doux"]}),
        ("La réponse de la plateforme", 1170, {"size": 19}),
    ], [
        ("1 · Les sources", "Oracle ancien, ERP, Excel envoyé par mail, API d'une banque, "
         "dépôt SFTP",
         "Des connecteurs branchables ; un connecteur générique SQL, fichier et API REST qui se "
         "règle sans code ; un SDK pour le reste"),
        ("2 · L'entrepôt", "Oracle à la CNSS, PostgreSQL ailleurs, Snowflake, ou aucun entrepôt",
         "Un adaptateur par entrepôt ; le SQL traduit ; le lakehouse intégré (Parquet + "
         "DuckDB) si le client n'a rien"),
        ("3 · L'outil BI", "OAC, Power BI, Superset, Excel",
         "La couche sémantique : un indicateur défini une fois, servi à tous les outils"),
        ("4 · Le rythme", "Mensuel avec photo, chaque nuit, toutes les heures, à l'arrivée d'un "
         "fichier",
         "Des partitions (jour, mois, trimestre, pays…) et des déclencheurs (horaire, "
         "événement, fichier, fraîcheur attendue)"),
        ("5 · La façon de charger", "Tout remplacer, ajouter, fusionner, garder l'historique, "
         "prendre une photo",
         "Une stratégie déclarée par modèle : vue, table, incrémental, fusion, photo, "
         "historique (SCD2)"),
        ("6 · Les règles métier", "CIPRES, normes comptables, règles propres à une entreprise",
         "Les indicateurs portent leurs règles, leurs sources et leur validation ; les "
         "paramètres changent par client ou par pays"),
        ("7 · Le volume", "Quelques milliers de lignes, ou des centaines de millions",
         "Arrow et le chargement natif ; le calcul fait dans l'entrepôt ; un moteur distribué "
         "(Spark, Trino) en plugin si besoin"),
        ("8 · L'hébergement", "Serveur isolé sans Internet, cloud, mixte",
         "Quatre façons de l'installer, avec le même produit (schéma 01)"),
        ("9 · L'équipe", "Un analyste qui ne code pas, des ingénieurs data",
         "L'interface sans code et le code écrivent le même dossier de projet"),
        ("10 · Le cas imprévu", "Une API exotique, un calcul statistique, une règle unique",
         "La porte de sortie : Python ou SQL libre, plugins, crochets (schéma 02)"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "2. Les limites, dites franchement")
    bas = rangee(d, s + 80, [
        ("CE QUE LA PLATEFORME COUVRE\nLa BI en lots, ou presque en temps réel (toutes les "
         "quelques minutes) ; des bases, des fichiers et des API ; de quelques milliers à des "
         "centaines de millions de lignes ; sur site, en hybride ou dans le cloud. C'est "
         "l'essentiel des projets BI des entreprises et des administrations.", VERT, 2),
        ("CE QUI N'EST PAS SON CŒUR\nLe vrai temps réel (flux à la seconde, Kafka), les "
         "téraoctets (Spark, en plugin seulement), les données non structurées (textes, "
         "images), l'apprentissage automatique. La plateforme s'y branche ; elle ne les "
         "remplace pas.", ROUGE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "3. La preuve : deux projets très différents, dès le début")
    cadres = ligne_de_cadres(d, s + 80, [
        ("PROJET A · LA GRH DE LA CNSS\n· Oracle ancien, sur site, sans Internet\n· une photo "
         "chaque mois\n· règles CIPRES, validation par la DRH\n· OAC", VIOLET),
        ("PROJET B · UN PROJET DE DÉMONSTRATION\n· PostgreSQL et fichiers Excel\n· chargé "
         "chaque nuit\n· historique des clients (SCD2)\n· Superset ou Power BI, dans le cloud",
         GRIS),
    ], size=22, gap=80)
    y = cadres[0]["y"] + cadres[0]["height"] + 40
    b = bloc(d, 40, y, WIDTH - 80,
             "LA RÈGLE : les deux projets tournent sur la même version de la plateforme, sans "
             "une ligne de code propre à l'un ou à l'autre dans le cœur. Toute différence se "
             "règle dans le projet (YAML, SQL, paramètres) ou par un plugin. Si le cœur doit "
             "changer pour un projet, c'est un défaut de la plateforme, à corriger pour tous.",
             22, JAUNE, sw=3)
    fermer(sec, b["y"] + b["height"])
    return d


# ======================================================= 04 feuille de route
def feuille_de_route():
    d = Diagram(seed=2105)
    s = entete(d, "Feuille de route de la plateforme flexible",
               "5 étapes, chacune utile seule ; la CNSS comme premier projet ; les points "
               "d'attention et ce qu'il faut trancher · proposition, " + DATE)

    sec = section(d, s, "1. Les 5 étapes")
    etapes = [
        ("ÉTAPE 1 · LE MODÈLE ET LE MOTEUR MINIMAL\n"
         "· le format du projet, vérifié avant de lancer\n"
         "· le compilateur : dépendances, SQL traduit, lignage\n"
         "· un moteur local : dépendances, reprises\n"
         "· connecteurs SQL (Oracle, PostgreSQL) et fichiers\n"
         "· entrepôts Oracle, PostgreSQL, DuckDB\n"
         "· ligne de commande, table des runs, coffre\n\n"
         "PREUVE : la GRH de la CNSS refaite en projet donne les mêmes chiffres que les "
         "requêtes actuelles, et le projet B tourne aussi", JAUNE),
        ("ÉTAPE 2 · PLANIFIER ET EXPLOITER\n"
         "· le planificateur : partitions, rattrapages, fraîcheur attendue\n"
         "· l'agent installé chez le client, hors ligne\n"
         "· les alertes ; la console en lecture\n"
         "· l'installation par une archive\n\n"
         "PREUVE : la chaîne mensuelle de la CNSS tourne seule, et la relance d'un mois aussi",
         JAUNE),
        ("ÉTAPE 3 · LE MÉTIER : INDICATEURS ET QUALITÉ\n"
         "· la couche sémantique ; livraisons OAC, Power BI, Excel\n"
         "· contrôles, contrats, comparaison à une publication, blocage\n"
         "· règles, sources et validation dans la console\n"
         "· lignage par colonne, docs générées\n\n"
         "PREUVE : un tableau CIPRES validé par la DRH dans la console, avec son lignage",
         VIOLET),
        ("ÉTAPE 4 · L'ÉCOSYSTÈME\n"
         "· le SDK des connecteurs ; compatibilité dlt, Airbyte\n"
         "· l'interface sans code, qui écrit le projet\n"
         "· les packs : CIPRES, puis finance, RH\n"
         "· plusieurs clients ; le mode hybride\n\n"
         "PREUVE : un nouveau client branché avec un pack, sans toucher au cœur", JAUNE),
        ("ÉTAPE 5 · PASSER À L'ÉCHELLE\n"
         "· Dagster ou Airflow comme moteur, pour les gros clients\n"
         "· Kubernetes, cloud\n"
         "· l'aide par l'IA : écrire un modèle, expliquer un chiffre\n\n"
         "PREUVE : un client à gros volume en production", JAUNE),
    ]
    cadres = ligne_de_cadres(d, s + 80, etapes, size=20, gap=55, fleches=True)
    s = fermer(sec, cadres[0]["y"] + cadres[0]["height"])

    sec = section(d, s, "2. La CNSS pendant ce temps")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             "Le projet CNSS se livre avec son code actuel : rien ne change pour la mission. Il "
             "bascule sur la plateforme à la fin de l'étape 2, une fois la preuve faite. Ses "
             "requêtes SQL, ses règles et ses fiches deviennent le pack CIPRES : rien du travail "
             "métier n'est perdu, et le pack sert ensuite aux autres pays.", 22, VERT, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    sec = section(d, s, "3. Points d'attention")
    bas = rangee(d, s + 80, [
        ("LE PÉRIMÈTRE\nLe piège est de vouloir tout faire. Chaque étape livre quelque chose "
         "d'utilisable, et la porte de sortie évite de tout prévoir.", ROUGE, 2),
        ("LE CONTRAT DES PLUGINS\nC'est lui qui rend la plateforme flexible. Il doit être "
         "stable et versionné dès l'étape 1 : le changer plus tard casse tous les plugins.",
         ROUGE, 2),
        ("SECRETS ET DONNÉES PERSONNELLES\nCoffre dès l'étape 1, masquage, journal d'audit. "
         "Rappel : le .env du projet CNSS est suivi par git.", ROUGE, 2),
        ("L'ÉQUIPE\nUne plateforme est un produit : il faut la maintenir, la documenter, "
         "répondre aux clients, en plus des projets.", ROUGE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "4. À trancher par toi")
    bas = rangee(d, s + 80, [
        ("POUR QUI ? DÉCIDÉ LE 20/11/2026\nUn produit Optiware, vendu à d'autres clients. Ce que "
         "ça change (installation, licence, support, propriété du code, ajouts à chaque "
         "étape) : schéma « 05 produit optiware ».", VERT, 3),
        ("QUEL PROJET B ?\nUn vrai deuxième client, ou un projet de démonstration, pour prouver "
         "la flexibilité dès l'étape 1 ?", JAUNE, 3),
        ("QUI ET QUAND ?\nQui construit l'étape 1, et à quel moment par rapport à la fin de la "
         "mission CNSS ?", JAUNE, 3),
    ], size=21)
    fermer(sec, bas)
    return d


# ======================================================= 05 produit optiware
def produit():
    d = Diagram(seed=2106)
    s = entete(d, "Un produit Optiware : ce que ça change",
               "Décision du 20/11/2026 : la plateforme est un produit vendu à d'autres clients, "
               "pas seulement un outil interne · ce que ça change, le modèle économique, les "
               "premiers clients, les ajouts à la feuille de route, ce qu'il faut pour réussir · "
               "propositions à discuter avec la direction d'Optiware")

    sec = section(d, s, "1. La décision")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             "LA PLATEFORME EST UN PRODUIT D'OPTIWARE, vendu à d'autres clients. Le projet CNSS en "
             "est le premier client et la première référence.\nConséquence : on ne construit "
             "plus seulement un outil qui marche entre nos mains, mais un produit qu'un autre peut "
             "installer, comprendre, mettre à jour, payer, et faire évoluer avec nous.",
             24, VERT, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    sec = section(d, s, "2. Ce qui change entre un outil interne et un produit")
    bas = tableau(d, s + 80, [
        ("Sujet", 330, {"size": 20}),
        ("Outil interne", 700, {"size": 19, "couleur": C["doux"]}),
        ("Produit Optiware", 1230, {"size": 19}),
    ], [
        ("Installation", "Par nous, à la main",
         "Par le client ou un partenaire : un installateur, une doc, des mises à jour qui ne "
         "cassent rien"),
        ("Versions", "Une seule : la nôtre",
         "Des versions numérotées, des notes de version ; le format de projet et le contrat "
         "des plugins restent compatibles d'une version à l'autre"),
        ("Documentation", "Nos schémas internes",
         "Doc d'installation, doc utilisateur, référence du format de projet, tutoriels, "
         "projets d'exemple"),
        ("Qualité du code", "Ça marche chez nous",
         "Tests automatiques, intégration continue, versions stables et versions de test"),
        ("Sécurité", "On se fait confiance",
         "Droits par rôle, journal d'audit, chiffrement, revue de sécurité, respect des lois "
         "de protection des données de chaque pays"),
        ("Licence", "Aucune",
         "Un contrat de licence, un prix, une clé de licence qui marche aussi hors ligne"),
        ("Support", "On se débrouille",
         "Des niveaux de support, des délais de réponse, un outil de tickets"),
        ("Plusieurs clients", "Un seul",
         "Clients isolés les uns des autres (en hybride et en cloud), espaces de travail"),
        ("Image", "Aucune",
         "Un nom, un site, une démo, des références clients"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "3. Le modèle économique, à choisir")
    l1 = ligne_de_cadres(d, s + 80, [
        ("A · LICENCE SUR SITE\nUn prix annuel par serveur, ou par nombre de projets ou de "
         "sources, avec la maintenance et les mises à jour. L'habitude des administrations et "
         "des caisses.", JAUNE),
        ("B · ABONNEMENT, CLOUD OU HYBRIDE\nUn prix mensuel par projet ou par volume. Des "
         "revenus réguliers, mais il faut héberger le cerveau et l'exploiter.", JAUNE),
        ("C · OPEN CORE\nLe cœur gratuit et ouvert (confiance, adoption) ; payants : les packs "
         "métier, la console avancée, le mode hybride, le support. Le modèle de dbt, Airbyte, "
         "Dagster.", JAUNE),
    ], size=21, gap=40)
    y = l1[0]["y"] + l1[0]["height"] + 40
    b = bloc(d, 40, y, WIDTH - 80,
             "PROPOSITION : commencer par A, la licence sur site, plus les SERVICES (mise en place, "
             "packs, formation), parce que les premiers clients ressemblent à la CNSS. Au début, "
             "les services pèsent souvent plus que les licences. Garder C possible plus tard, "
             "quand le produit sera stable.", 22, VIOLET, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    sec = section(d, s, "4. Les premiers clients visés")
    bas = rangee(d, s + 80, [
        ("1 · LES CAISSES DE SÉCURITÉ SOCIALE DE LA CIPRES\nMême métier, mêmes tableaux, même "
         "contexte (Oracle, sur site). Le pack CIPRES existera grâce à la CNSS. Le marché le "
         "plus naturel, et le projet doit déjà être déployé dans d'autres pays.", VIOLET, 2),
        ("2 · LES ADMINISTRATIONS ET ORGANISMES PUBLICS\nDonnées sensibles, installation sur "
         "site souvent obligatoire, peu d'ingénieurs data. Nos points forts : hors ligne, "
         "métier dedans, validation par le métier.", VIOLET, 2),
        ("3 · LES ENTREPRISES DE LA RÉGION\nBanques, assurances, télécoms, distribution : plus "
         "tard, avec les packs finance, RH et ventes, et le mode hybride.", GRIS, 2),
    ], size=21)
    b = bloc(d, 40, bas + 30, WIDTH - 80,
             "UNE OUVERTURE À VÉRIFIER : Talend Open Studio, très utilisé pour ce type de projet, "
             "n'est plus maintenu depuis 2024. Des organisations cherchent une solution sur site "
             "pour le remplacer. Les grands du marché (Fivetran, Informatica, Microsoft Fabric) "
             "visent surtout le cloud et les grands comptes : ne pas les affronter sur leur "
             "terrain.", 21, JAUNE)
    s = fermer(sec, b["y"] + b["height"])

    sec = section(d, s, "5. Ce que ça ajoute à la feuille de route (schéma 04)")
    cadres = ligne_de_cadres(d, s + 80, [
        ("ÉTAPE 1 · EN PLUS\n· format de projet et contrat des plugins en version 1, figés\n"
         "· tests automatiques, intégration continue\n· un nom de produit", JAUNE),
        ("ÉTAPE 2 · EN PLUS\n· un installateur et une procédure de mise à jour\n· la doc "
         "d'installation\n· la clé de licence hors ligne", JAUNE),
        ("ÉTAPE 3 · EN PLUS\n· la doc utilisateur et les tutoriels\n· la démo sur le projet B"
         "\n· VERSION 1.0 VENDABLE : la CNSS en production et une démo qui tourne", VERT),
        ("ÉTAPE 4 · EN PLUS\n· l'offre, les prix, le site\n· le support\n· le deuxième client "
         "payant", JAUNE),
        ("ÉTAPE 5 · EN PLUS\n· le mode cloud, l'abonnement\n· les partenaires qui installent "
         "le produit", JAUNE),
    ], size=20, gap=45, fleches=True)
    s = fermer(sec, cadres[0]["y"] + cadres[0]["height"])

    sec = section(d, s, "6. Ce qu'il faut pour réussir")
    bas = rangee(d, s + 80, [
        ("UNE ÉQUIPE PRODUIT\nAu moins une personne responsable du produit et des développeurs "
         "qui y travaillent en continu, à part des projets clients. Sinon le produit n'avance "
         "qu'entre deux projets.", ROUGE, 2),
        ("LA PROPRIÉTÉ DU CODE ET DU MODÈLE\nÀ qui appartiennent les requêtes, le modèle et les "
         "règles faits pour la CNSS ? Le contrat le dit. Le pack CIPRES en dépend : à vérifier "
         "avec la direction avant de le vendre ailleurs.", ROUGE, 2),
        ("LES LICENCES OPEN SOURCE\nArrow, DuckDB, SQLGlot, Dagster, Airflow, dlt ont des "
         "licences qui permettent la revente. Certaines briques en limitent l'usage commercial "
         "(Airbyte en partie) : à vérifier une par une.", ROUGE, 2),
        ("LE CONTRAT DES PLUGINS\nDans un produit, c'est une promesse aux clients et aux "
         "partenaires : le changer casse leurs connecteurs. Le figer tôt, le faire évoluer "
         "sans casser.", ROUGE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "7. Les prochaines décisions")
    bas = rangee(d, s + 80, [
        ("LE MODÈLE ÉCONOMIQUE\nA (licence sur site), B (abonnement) ou C (open core) ? La "
         "proposition : A, plus les services.", JAUNE, 3),
        ("LA PROPRIÉTÉ\nQue dit le contrat CNSS sur le code, le modèle et les règles ? À "
         "demander à la direction d'Optiware.", JAUNE, 3),
        ("L'ÉQUIPE ET LE BUDGET\nQui travaille sur le produit, à quel rythme, et à partir de "
         "quand ?", JAUNE, 3),
        ("LE NOM\nUn nom de produit, court, facile à dire en français et en anglais.", JAUNE, 3),
    ], size=21)
    fermer(sec, bas)
    return d


# ======================================================= 06 la console
# Des maquettes d'écrans : une fenêtre (barre de titre, menu à gauche, contenu) et, à sa
# droite, l'explication. Les numéros orange posés sur la maquette renvoient à l'explication.
MENU = ["Accueil", "Exécutions", "Lignage", "Indicateurs", "Sans code", "Administration"]
FEN_W, MENU_W, BARRE = 1480, 250, 54
EXPL_X = 40 + FEN_W + 60
EXPL_W = WIDTH - 40 - EXPL_X
ETATS = {  # cases du calendrier des exécutions : fond, bord, trait
    "ok": (C["vert_moyen"], C["vert"], "solid"),
    "echec": (C["rouge_moyen"], C["rouge"], "solid"),
    "refaire": (C["jaune"], C["jaune_bord"], "solid"),
    "bloque": (C["gris"], C["rouge"], "dashed"),
    "venir": (C["blanc"], C["gris_bord"], "dashed"),
}


def fenetre(d, x, y, actif, connecte):
    """Le cadre d'un écran, menu à gauche avec l'écran ouvert en bleu. Sa hauteur se fixe
    ensuite avec fermer_fenetre. Renvoie le cadre, le menu et la zone de contenu (x, y, w)."""
    cadre = d.box(x, y, FEN_W, 600, bg=C["blanc"], stroke=C["encre"], stroke_width=2)
    d.box(x, y, FEN_W, BARRE, bg=C["gris"], stroke=C["encre"], stroke_width=2)
    for i, fond in enumerate((C["rouge_moyen"], C["orange_moyen"], C["vert_moyen"])):
        d.ellipse(x + 22 + i * 32, y + 17, 20, 20, bg=fond, stroke=C["gris_bord"],
                  stroke_width=1)
    d.text(x + 140, y + 15, "PLATEFORME  ·  projet : CNSS · GRH", 20, C["doux"])
    d.text(x + FEN_W - 30 - text_size(connecte, 19)[0], y + 15, connecte, 19, C["doux"])
    menu = d.box(x, y + BARRE, MENU_W, 600 - BARRE, bg=C["section"], stroke=C["gris_bord"],
                 stroke_width=1)
    my = y + BARRE + 34
    for nom in MENU:
        if nom == actif:
            d.box(x + 14, my - 11, MENU_W - 28, 48, bg=C["bleu_clair"], stroke=C["bleu"])
        d.text(x + 34, my, nom, 21, C["bleu"] if nom == actif else C["doux"])
        my += 64
    return {"cadre": cadre, "menu": menu, "min": my + 20 - y,
            "contenu": (x + MENU_W + 30, y + BARRE + 28, FEN_W - MENU_W - 60)}


def fermer_fenetre(f, bas):
    h = max(f["min"], bas + 40 - f["cadre"]["y"])
    f["cadre"]["height"] = h
    f["menu"]["height"] = h - BARRE
    return f["cadre"]["y"] + h


def repere(d, x, y, n):
    """Un numéro orange posé sur la maquette, repris dans l'explication."""
    d.box(x, y, 40, 40, str(n), 20, bg=C["orange"], stroke=C["orange"], color=C["blanc"],
          stroke_width=1)


def coin(d, cadre, n):
    """Un numéro sur le coin haut droit d'un cadre."""
    repere(d, cadre["x"] + cadre["width"] - 30, cadre["y"] - 20, n)


def bouton(d, x, y, txt, plein=True, couleur=C["bleu"], size=18):
    """Un bouton ; renvoie le x du suivant."""
    w = text_size(txt, size)[0] + 44
    d.box(x, y, w, 46, txt, size, bg=couleur if plein else C["blanc"], stroke=couleur,
          color=C["blanc"] if plein else couleur)
    return x + w + 16


def champ(d, x, y, w, txt, size=18, h=44):
    """Un champ de saisie ou une liste déroulante."""
    b = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], stroke_width=1)
    d.text(x + 14, y + (h - size * 1.25) / 2, txt, size, inside=b)
    return b


def grille(d, x, y, colonnes, lignes, size=17, h=42):
    """Un petit tableau de maquette : l'en-tête en gris, puis les lignes. colonnes : liste de
    (titre, largeur) ; une valeur peut être (texte, couleur). Renvoie le bas."""
    total = sum(w for _, w in colonnes)
    for i, ligne in enumerate([tuple(t for t, _ in colonnes)] + list(lignes)):
        cellules = []
        for (_, w), val in zip(colonnes, ligne):
            txt, couleur = val if isinstance(val, tuple) else (val, C["encre"])
            cellules.append((couper(txt, size, w - 24), C["doux"] if i == 0 else couleur))
        hl = max(h, max(text_size(t, size)[1] for t, _ in cellules) + 18)
        r = d.box(x, y, total, hl, bg=C["gris"] if i == 0 else C["blanc"],
                  stroke=C["gris_bord"], stroke_width=1)
        cx = x
        for (t, couleur), (_, w) in zip(cellules, colonnes):
            if t:
                d.text(cx + 12, y + (hl - text_size(t, size)[1]) / 2, t, size, couleur,
                       inside=r)
            cx += w
        y += hl
    return y


def ecran(d, s, titre, actif, connecte, dessiner, explication, couleurs=JAUNE):
    """Une section : la maquette à gauche, l'explication à droite. Renvoie le haut de la
    section suivante."""
    sec = section(d, s, titre)
    top = s + 80
    f = fenetre(d, 40, top, actif, connecte)
    bas = fermer_fenetre(f, dessiner(d, *f["contenu"]))
    b = bloc(d, EXPL_X, top, EXPL_W, explication, 20, couleurs, sw=2)
    return fermer(sec, max(bas, b["y"] + b["height"]))


def ecran_accueil(d, cx, cy, cw):
    d.text(cx, cy, "Accueil", 30)
    cartes = ligne_de_cadres(d, cy + 64, [
        ("DERNIÈRE EXÉCUTION\n31/10/2026 à 02:00, terminée en 38 min", VERT),
        ("CONTRÔLES\n46 réussis\n2 en échec", ROUGE),
        ("À VALIDER\n3 indicateurs attendent la DRH", VIOLET),
        ("AGENT CNSS\nen ligne · version 1.2", VERT),
    ], size=18, x=cx, largeur=cw, gap=24)
    coin(d, cartes[-1], 1)
    y = cartes[0]["y"] + cartes[0]["height"] + 40
    d.text(cx, y, "Alertes récentes", 22)
    y += 44
    repere(d, cx + cw - 30, y - 20, 2)
    for date, msg, coul in [
        ("31/10 02:14", "T60 · octobre : écart de 2,8 % avec l'Annuaire (seuil 2 %). "
                        "Livraison bloquée.", C["rouge"]),
        ("31/10 02:05", "Source GRH_SITUATION : 12 agents sans date d'embauche",
         C["jaune_bord"]),
        ("27/10 09:00", "Règle R1 modifiée : T58 et T60 repassent « à valider »",
         VIOLET[1]),
        ("12/10 15:20", "T58 validé par la DRH", C["vert"]),
    ]:
        r = d.box(cx, y, cw, 50, bg=C["blanc"], stroke=C["gris_bord"], stroke_width=1)
        d.box(cx + 16, y + 15, 20, 20, bg=coul, stroke=coul, stroke_width=1)
        d.text(cx + 52, y + 13, date, 18, C["doux"], inside=r)
        d.text(cx + 190, y + 13, msg, 18, inside=r)
        y += 58
    y += 26
    d.text(cx, y, "Prochaines exécutions", 22)
    y += 44
    repere(d, cx + cw - 30, y - 20, 3)
    for t in ("30/11/2026 à 02:00 · chaîne mensuelle GRH, photo de novembre",
              "31/01/2027 à 02:00 · photo de fin janvier : l'année 2026 recalculée après "
              "les corrections"):
        r = d.box(cx, y, cw, 50, bg=C["blanc"], stroke=C["gris_bord"], stroke_width=1)
        d.text(cx + 20, y + 13, t, 18, inside=r)
        y += 58
    return y - 8


def ecran_executions(d, cx, cy, cw):
    d.text(cx, cy, "Exécutions · chaîne mensuelle GRH", 30)
    y = cy + 66
    fx = cx
    for t in ("Année : 2026 ▾", "Domaine : GRH ▾", "État : tous ▾"):
        w = text_size(t, 18)[0] + 40
        champ(d, fx, y, w, t)
        fx += w + 16
    y += 80
    lab_w, col_w, gap = 280, 70, 8
    d.text(cx, y, "Résultat", 18, C["doux"])
    for j, m in enumerate(["juin", "juil.", "août", "sept.", "oct.", "nov."]):
        d.text(cx + lab_w + j * (col_w + gap) + col_w / 2, y, m, 18, C["doux"], align="center")
    repere(d, cx + lab_w - 54, y - 8, 1)
    y += 38
    haut_grille = y
    lignes = [
        ("source · agents", "ok ok ok ok ok venir"),
        ("source · situations", "ok ok ok ok ok venir"),
        ("source · décisions", "ok ok ok ok ok venir"),
        ("modèle · situations", "ok ok ok ok ok venir"),
        ("modèle · mouvements", "ok ok ok ok ok venir"),
        ("indicateur · T58", "refaire refaire refaire refaire ok venir"),
        ("indicateur · T60", "refaire refaire refaire refaire echec venir"),
        ("livraison · OAC", "ok ok ok ok bloque venir"),
    ]
    for nom, etats in lignes:
        d.text(cx, y + 8, nom, 18)
        for j, e in enumerate(etats.split()):
            fond, bord, style = ETATS[e]
            choisi = nom == "indicateur · T60" and j == 4
            d.box(cx + lab_w + j * (col_w + gap), y, col_w, 40, bg=fond,
                  stroke=C["bleu"] if choisi else bord, style=style,
                  stroke_width=5 if choisi else 1)
        if nom == "indicateur · T58":
            repere(d, cx + lab_w - 50, y, 2)
        if nom == "livraison · OAC":
            repere(d, cx + lab_w + 6 * (col_w + gap) + 6, y, 4)
        y += 50
    lx, ly = cx, y + 10
    for e, lib in (("ok", "réussi"), ("echec", "en échec"),
                   ("refaire", "à refaire : une règle a changé"),
                   ("bloque", "bloqué par un contrôle"), ("venir", "à venir")):
        fond, bord, style = ETATS[e]
        d.box(lx, ly, 26, 26, bg=fond, stroke=bord, style=style, stroke_width=1)
        d.text(lx + 36, ly + 1, lib, 17, C["doux"])
        lx += 36 + text_size(lib, 17)[0] + 30
    # le détail de la case choisie
    px = cx + lab_w + 6 * (col_w + gap) + 60
    pw = cx + cw - px
    b = bloc(d, px, haut_grille, pw,
             "T60 · OCTOBRE 2026\nEn échec : contrôle « écart à l'Annuaire »\n2,8 % pour un "
             "seuil de 2 %\n4 min · 1 214 lignes", 18, ROUGE)
    by = b["y"] + b["height"] + 20
    repere(d, px + pw - 40, by + 3, 3)
    for t, plein in (("Voir le journal", False), ("Relancer ce mois", True),
                     ("Relancer avec ce qui en dépend", True)):
        bouton(d, px, by, t, plein)
        by += 62
    return max(ly + 26, by - 16)


def ecran_lignage(d, cx, cy, cw):
    d.text(cx, cy, "Lignage · T60, mouvements du personnel", 30)
    y = cy + 70
    nw, nh = 240, 56
    gap = (cw - 4 * nw) / 3
    xs = [cx + i * (nw + gap) for i in range(4)]
    for x, t in zip(xs, ["Sources · USER_GRH", "Modèles", "Indicateur", "Livraisons"]):
        d.text(x, y, t, 19, C["doux"])
    repere(d, cx + cw - 30, y - 8, 1)
    y += 44

    def noeud(col, rang, txt, couleurs, sw=2):
        return d.box(xs[col], y + rang * 90, nw, nh, txt, 18, bg=couleurs[0],
                     stroke=couleurs[1], stroke_width=sw)

    agent, situation, decision = (noeud(0, i, t, GRIS) for i, t in
                                  enumerate(["GRH_AGENT", "GRH_SITUATION", "GRH_DECISION"]))
    situations = noeud(1, 0.5, "situations", JAUNE)
    mouvements = noeud(1, 1.5, "mouvements", JAUNE)
    t60 = noeud(2, 1, "T60", VIOLET)
    oac = noeud(3, 0.5, "OAC · sujet GRH", VERT)
    excel = noeud(3, 1.5, "Excel · Annuaire", VERT)

    def lien(a, b, chemin=False):
        d.arrow(a["x"] + a["width"] + 6, a["y"] + a["height"] / 2, b["x"] - 6,
                b["y"] + b["height"] / 2, a, b, color=C["bleu"] if chemin else C["encre"],
                stroke_width=4 if chemin else 2)

    lien(agent, situations)
    lien(situation, situations)
    lien(decision, mouvements, True)
    d.varrow(situations, mouvements, stroke_width=2)
    lien(mouvements, t60, True)
    lien(t60, oac, True)
    lien(t60, excel)
    y += 2 * 90 + nh + 40
    w = (cw - 30) / 2
    textes = [
        ("LIGNAGE PAR COLONNE · les départs de T60 (en bleu)\nT60.departs  ←  "
         "mouvements.type_mouvement  ←  GRH_DECISION.CDEC_CODE, les codes de décision de la "
         "CNSS", (C["bleu_clair"], C["bleu"])),
        ("ANALYSE D'IMPACT · si GRH_DECISION change\n1 modèle, 1 indicateur et 2 livraisons "
         "touchés. À prévenir : la DRH, qui valide T60.", ROUGE),
    ]
    h = max(hauteur(t, 18, w) for t, _ in textes)
    for i, (t, c) in enumerate(textes):
        b = bloc(d, cx + i * (w + 30), y, w, t, 18, c, h=h)
        coin(d, b, i + 2)
    return y + h


def ecran_indicateur(d, cx, cy, cw):
    d.text(cx, cy, "Indicateurs  ›  T58 · Effectif du personnel permanent", 28)
    d.box(cx + cw - 170, cy - 4, 170, 44, "À VALIDER", 18, bg=VIOLET[0], stroke=VIOLET[1],
          color=VIOLET[1])
    lw, rx = 700, cx + 740
    rw = cx + cw - rx
    y = cy + 74
    d.text(cx, y, "DÉFINITION", 18, C["doux"])
    y += 34
    t = couper("Les agents permanents en activité à la fin du mois, par catégorie, sexe et "
               "tranche d'âge. Publié chaque année dans l'Annuaire (tableau 58).", 20, lw)
    d.text(cx, y, t, 20)
    y += text_size(t, 20)[1] + 28
    d.text(cx, y, "RÈGLES", 18, C["doux"])
    repere(d, cx + text_size("RÈGLES", 18)[0] + 16, y - 8, 1)
    y = grille(d, cx, y + 34, [("", 60), ("Règle", 400), ("Source", 240)], [
        ("R1", "Un agent en disponibilité sort de l'effectif",
         ("Annuaire CNSS 2024, partie IX", C["doux"])),
        ("R2", "Sans donnée, la case reste vide, jamais 0", ("Règle du projet", C["doux"])),
        ("R3", "La catégorie est celle de la situation en cours",
         ("Proposition BI", C["orange"])),
    ])
    y += 30
    d.text(cx, y, "CHIFFRE DE DÉCEMBRE 2024", 18, C["doux"])
    repere(d, cx + text_size("CHIFFRE DE DÉCEMBRE 2024", 18)[0] + 16, y - 8, 2)
    stats = ligne_de_cadres(d, y + 34, [
        ("CALCULÉ\n752", BLANC), ("ANNUAIRE 2024\n758", BLANC),
        ("ÉCART\n0,8 % · seuil 2 %", VERT)], size=20, x=cx, largeur=lw, gap=20)
    y = stats[0]["y"] + stats[0]["height"] + 20
    d.text(cx, y, "Voir le détail par catégorie, sexe et âge  ›", 18, C["bleu"])
    bas_gauche = y + 24
    # la validation
    ry = cy + 74
    d.text(rx, ry, "VALIDATION", 18, C["doux"])
    ry += 34
    b = bloc(d, rx, ry, rw, "À valider depuis le 27/10/2026 : la règle R1 a changé.", 19,
             VIOLET)
    ry = b["y"] + b["height"] + 20
    champ(d, rx, ry, rw, "Votre remarque…", h=90)
    ry += 106
    nx = bouton(d, rx, ry, "Valider", True, C["vert"])
    nx = bouton(d, nx, ry, "Refuser", False, C["rouge"])
    repere(d, nx, ry + 3, 3)
    ry += 46 + 34
    d.text(rx, ry, "HISTORIQUE", 18, C["doux"])
    repere(d, rx + text_size("HISTORIQUE", 18)[0] + 16, ry - 8, 4)
    ry += 34
    hist = couper("· 27/10/2026 : validation tombée, R1 modifiée\n· 12/10/2026 : validé par la "
                  "DRH\n· 03/10/2026 : refusé par la DRH, « les contractuels sont comptés »",
                  18, rw)
    d.text(rx, ry, hist, 18)
    ry += text_size(hist, 18)[1] + 24
    nx = bouton(d, rx, ry, "Exporter la fiche (Word, PDF)", False)
    repere(d, nx, ry + 3, 5)
    return max(bas_gauche, ry + 46)


CODE_SANS_CODE = """\
indicateur: t58_effectif_permanent
modele: situations
filtre: statut = 'permanent' and en_activite
mesure: count_distinct(matricule)
axes: [categorie, sexe]
partition: mois"""


def ecran_sans_code(d, cx, cy, cw):
    d.text(cx, cy, "Sans code  ›  Nouvel indicateur", 30)
    y = cy + 66
    sx = cx
    for i, t in enumerate(["1 · Partir de", "2 · Garder", "3 · Compter", "4 · Contrôler",
                           "5 · Proposer"]):
        w = text_size(t, 18)[0] + 40
        if i < 2:
            d.box(sx, y, w, 44, t, 18, bg=C["vert_clair"], stroke=C["vert"], color=C["vert"])
        elif i == 2:
            d.box(sx, y, w, 44, t, 18, bg=C["bleu_clair"], stroke=C["bleu"], color=C["bleu"],
                  stroke_width=3)
        else:
            d.box(sx, y, w, 44, t, 18, bg=C["blanc"], stroke=C["gris_bord"], color=C["doux"],
                  style="dashed", stroke_width=1)
        sx += w + 24
    y += 84
    haut = y
    repere(d, cx + 120 + 440 + 12, y + 2, 1)
    for lab, val in (("Partir de", "situations (modèle propre) ▾"),
                     ("Garder", "statut = permanent · en activité"),
                     ("Compter", "les agents, une fois chacun ▾"),
                     ("Par", "catégorie · sexe ▾"),
                     ("Chaque", "mois : la photo de fin de mois ▾")):
        d.text(cx, y + 11, lab, 18, C["doux"])
        champ(d, cx + 120, y, 440, val)
        y += 60
    bas_form = y - 16
    ax = cx + 640
    d.text(ax, haut, "APERÇU · décembre 2024", 18, C["doux"])
    repere(d, ax + text_size("APERÇU · décembre 2024", 18)[0] + 16, haut - 8, 2)
    ay = grille(d, ax, haut + 34, [("Catégorie", 230), ("Femmes", 150), ("Hommes", 150)], [
        ("Cadres", "48", "112"), ("Agents de maîtrise", "95", "160"),
        ("Agents d'exécution", "120", "217"), (("Total", C["doux"]), "263", "489")])
    d.text(ax, ay + 12, "Lu par l'agent, chez le client :\nles données ne sortent pas.", 17,
           C["doux"])
    y = max(bas_form, ay + 12 + text_size("a\nb", 17)[1]) + 40
    titre = "CE QUI SERA ÉCRIT DANS LE PROJET : indicateurs/t58_effectif_permanent.yml"
    d.text(cx, y, titre, 18, C["doux"])
    repere(d, cx + text_size(titre, 18)[0] + 16, y - 8, 3)
    y += 34
    _, ch = text_size(CODE_SANS_CODE, 18, mono=True)
    cadre = d.box(cx, y, 720, ch + 40, bg=C["blanc"], stroke=C["gris_bord"])
    d.text(cx + 20, y + 20, CODE_SANS_CODE, 18, inside=cadre, mono=True)
    bouton(d, cx + 760, y, "Enregistrer le brouillon", False)
    nx = bouton(d, cx + 760, y + 62, "Proposer pour revue", True)
    repere(d, nx, y + 65, 4)
    return y + ch + 40


def ecran_administration(d, cx, cy, cw):
    d.text(cx, cy, "Administration", 30)
    y = cy + 66
    tx = cx
    for t in ["Connexions", "Agents", "Utilisateurs et rôles", "Licence", "Journal d'audit",
              "Mises à jour"]:
        w = text_size(t, 18)[0] + 36
        if t == "Connexions":
            d.box(tx, y, w, 44, t, 18, bg=C["bleu_clair"], stroke=C["bleu"], color=C["bleu"])
        else:
            d.box(tx, y, w, 44, t, 18, bg=C["blanc"], stroke=C["gris_bord"], color=C["doux"],
                  stroke_width=1)
        tx += w + 10
    y += 74
    repere(d, cx + 200 + 200 + 170 + 300 - 50, y - 22, 1)
    y = grille(d, cx, y, [("Nom", 200), ("Type", 200), ("Agent", 170), ("Mot de passe", 300),
                          ("État", 300)], [
        ("source_cnss", "Oracle 11g", "agent-cnss", "secret : cnss/source",
         ("testée le 31/10 : ok", C["vert"])),
        ("entrepot", "Oracle", "agent-cnss", "secret : cnss/entrepot",
         ("testée le 31/10 : ok", C["vert"])),
        ("fichiers_drh", "dossier partagé", "agent-cnss", ("aucun", C["doux"]),
         ("3 fichiers en attente", C["jaune_bord"])),
    ])
    nx = bouton(d, cx, y + 16, "+ Nouvelle connexion", True)
    bouton(d, nx, y + 16, "Tester", False)
    y += 16 + 46 + 40
    cartes = ligne_de_cadres(d, y, [
        ("AGENTS\nagent-cnss, Ouagadougou\nen ligne · version 1.2\ndernier contact : il y a "
         "1 min", VERT),
        ("LICENCE\nCNSS Burkina · sur site\nvalable jusqu'au 31/12/2027\nclé hors ligne",
         BLANC),
        ("JOURNAL D'AUDIT\n· 31/10 08:30, administrateur : donne le droit « valider » à la "
         "DAI\n· 30/10 11:05, administrateur : ajoute fichiers_drh\n· 27/10 09:00, "
         "ingénieur : modifie la règle R1", BLANC),
        ("RÔLES, PROJET PAR PROJET\nexploitant · métier (valideur) · analyste · "
         "administrateur", BLANC),
    ], size=17, x=cx, largeur=cw, gap=20)
    for i, c in enumerate(cartes):
        coin(d, c, i + 2)
    return cartes[0]["y"] + cartes[0]["height"]


def la_console():
    d = Diagram(seed=2107)
    s = entete(d, "La console : les écrans de la plateforme",
               "Maquettes de principe, pour en discuter : qui s'en sert, ce qu'il y voit, ce "
               "qu'il peut y faire, et quand chaque écran arrive · l'exemple est le projet GRH "
               "de la CNSS ; les chiffres sont des exemples · dans une maquette, l'écran ouvert "
               "est en bleu dans le menu, et les numéros orange renvoient à l'explication à sa "
               "droite")

    sec = section(d, s, "1. Une console, cinq profils")
    bas = tableau(d, s + 80, [
        ("Profil", 260, {"size": 21}),
        ("Qui, par exemple à la CNSS", 520, {"size": 19, "couleur": C["doux"]}),
        ("Ses écrans", 560, {"size": 19}),
        ("Ce qu'il peut y faire", 920, {"size": 19}),
    ], [
        ("Exploitant", "La DSI de la CNSS, un ingénieur Optiware",
         "Accueil, Exécutions, Lignage",
         "Suivre les exécutions, relancer un mois, recevoir les alertes, voir ce qu'un "
         "changement va toucher"),
        ("Métier", "La DRH, la DAI, la direction", "Accueil, Indicateurs",
         "Lire les règles et leurs sources, comparer au chiffre publié, valider ou refuser "
         "avec une remarque, exporter la fiche"),
        ("Analyste", "Un agent formé, à la DSI ou au métier", "Sans code, Indicateurs, Lignage",
         "Créer ou changer un indicateur sans écrire de SQL, puis le proposer pour revue"),
        ("Administrateur", "Le responsable de la plateforme chez le client ; Optiware",
         "Administration",
         "Gérer les utilisateurs et leurs droits, les connexions, les agents, la licence ; lire "
         "le journal d'audit"),
        ("Ingénieur", "Optiware, un partenaire, la DSI",
         "Tous, en lecture ; il travaille surtout dans son éditeur de code",
         "Écrire le projet (YAML, SQL, Python), relire et accepter les propositions faites "
         "dans la console"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "2. Le principe : la console et le code écrivent le même projet")
    top = s + 80
    haut = [
        (40, 700, "L'ÉDITEUR DE CODE\nL'ingénieur écrit le YAML, le SQL et le Python, et les "
                  "suit par git.", GRIS),
        (800, 700, "LA CONSOLE WEB\nÀ la souris : l'exploitant, le métier, l'analyste, "
                   "l'administrateur.", JAUNE),
        (1560, 800, "LA LIGNE DE COMMANDE\nLes scripts, l'intégration continue, l'exploitant "
                    "pressé. Chaque écran a sa commande.", JAUNE),
    ]
    h1 = max(hauteur(t, 21, w) for _, w, t, _ in haut)
    editeur, console, commande = [bloc(d, x, top, w, t, 21, c, h=h1) for x, w, t, c in haut]
    y2 = top + h1 + 90
    api = bloc(d, 800, y2, 1560,
               "L'API\nTout ce que fait la console passe par elle : mêmes droits, même journal "
               "d'audit. Un client peut aussi l'appeler depuis ses propres outils.", 21, JAUNE)
    y3 = y2 + api["height"] + 90
    bas_ = [
        (40, 1100, "LE DOSSIER DE PROJET, suivi par git : ce qui est défini\nSources, modèles, "
                   "indicateurs, règles, contrôles, livraisons. Ce que la console y écrit est le "
                   "fichier qu'un ingénieur aurait écrit : on peut tout relire, comparer, revenir "
                   "en arrière. Rien n'est caché dans l'outil.", VERT, 3),
        (1260, 1100, "LES MÉTADONNÉES : ce qui s'est passé\nLes exécutions, les résultats des "
                     "contrôles, les validations et les remarques du métier, le journal "
                     "d'audit.", BLANC, 2),
    ]
    h3 = max(hauteur(t, 21, w) for _, w, t, _, _ in bas_)
    dossier, meta = [bloc(d, x, y3, w, t, 21, c, sw=sw, h=h3) for x, w, t, c, sw in bas_]
    d.arrow(390, top + h1 + 6, 390, y3 - 6, editeur, dossier, stroke_width=3)
    d.arrow(1150, top + h1 + 6, 1150, y2 - 6, console, api, stroke_width=3)
    d.arrow(1960, top + h1 + 6, 1960, y2 - 6, commande, api, stroke_width=3)
    d.arrow(1000, y2 + api["height"] + 6, 1000, y3 - 6, api, dossier, stroke_width=3)
    d.arrow(1800, y2 + api["height"] + 6, 1800, y3 - 6, api, meta, stroke_width=3)
    s = fermer(sec, y3 + h3)

    s = ecran(d, s, "3. Écran « Accueil » · pour tous", "Accueil", "connecté : exploitant",
              ecran_accueil,
              "POUR QUI : tout le monde, à l'ouverture. Chacun voit d'abord ce qui le "
              "concerne.\n\n"
              "1 · L'état en un coup d'œil : la dernière exécution, les contrôles, ce qui "
              "attend une validation, l'agent installé chez le client.\n"
              "2 · Les alertes, les mêmes que par mail, Teams ou WhatsApp. Un clic mène à "
              "l'exécution ou à l'indicateur en cause.\n"
              "3 · Ce qui va tourner, et quand. La photo de fin janvier recalcule l'année "
              "passée après les corrections.\n\n"
              "ARRIVE À L'ÉTAPE 2.")

    s = ecran(d, s, "4. Écran « Exécutions » · pour l'exploitant", "Exécutions",
              "connecté : exploitant", ecran_executions,
              "POUR QUI : l'exploitant, à la DSI de la CNSS ou chez Optiware.\n\n"
              "1 · Une ligne par résultat, une colonne par mois : la partition. Toute l'année "
              "se lit d'un coup.\n"
              "2 · En jaune, à refaire : une règle ou une source a changé. La plateforme le "
              "sait seule, et ne recalcule que ces mois.\n"
              "3 · Relancer un mois sans toucher aux autres ; ce qui en dépend suit. Plusieurs "
              "mois choisis d'un coup : c'est un rattrapage.\n"
              "4 · Bloqué : tant qu'un contrôle échoue, la livraison ne se fait pas. OAC garde "
              "les derniers chiffres bons.\n\n"
              "Aujourd'hui, avec Jenkins : un job par chaîne, et le mois se saisit à la "
              "main.\n\n"
              "ARRIVE À L'ÉTAPE 2 EN LECTURE ; la relance depuis la console à l'étape 3.")

    s = ecran(d, s, "5. Écran « Lignage » · d'où vient un chiffre", "Lignage",
              "connecté : exploitant", ecran_lignage,
              "POUR QUI : l'exploitant, l'ingénieur, et le métier qui veut savoir d'où vient "
              "un chiffre.\n\n"
              "1 · Le chemin de chaque résultat, de la table source à la livraison. Un clic sur "
              "une case ouvre sa fiche : SQL, contrôles, dernières exécutions, responsable.\n"
              "2 · Le lignage par colonne : quelle colonne de la base donne quel chiffre, et "
              "par quels codes.\n"
              "3 · Avant de changer une source ou une règle : tout ce qui sera touché, et qui "
              "prévenir.\n\n"
              "Personne ne dessine ce graphe : le compilateur le calcule à partir du projet.\n\n"
              "ARRIVE À L'ÉTAPE 2 (le graphe) ET À L'ÉTAPE 3 (les colonnes, l'impact).")

    s = ecran(d, s, "6. Écran « Indicateurs » · la fiche et la validation par le métier",
              "Indicateurs", "connecté : DRH (valideur)", ecran_indicateur,
              "POUR QUI : le métier (DRH, DAI, direction). Aucun SQL à lire.\n\n"
              "1 · La définition et les règles en clair, chaque règle avec sa source "
              "(Annuaire, Kaya 2017, recueil des besoins) ou marquée « Proposition BI ».\n"
              "2 · Le chiffre calculé face au chiffre publié, avec l'écart et le seuil.\n"
              "3 · Valider ou refuser, avec une remarque. La validation est nominative et "
              "datée.\n"
              "4 · L'historique. Si une règle change, la validation tombe et l'indicateur "
              "repasse « à valider ».\n"
              "5 · La fiche s'exporte pour une réunion.\n\n"
              "Ce que la CNSS fait aujourd'hui à la main (fiches de validation, points à "
              "valider, requêtes de preuve) devient cet écran.\n\n"
              "ARRIVE À L'ÉTAPE 3.", VIOLET)

    s = ecran(d, s, "7. Écran « Sans code » · un indicateur sans écrire de SQL", "Sans code",
              "connecté : analyste", ecran_sans_code,
              "POUR QUI : l'analyste qui ne code pas, à la DSI ou au métier.\n\n"
              "1 · Il avance pas à pas, avec des listes : partir de, garder, compter, par, "
              "chaque. Il ne voit que les modèles déjà propres, pas les tables brutes.\n"
              "2 · Il voit tout de suite le résultat sur les vraies données. Elles sont lues "
              "par l'agent : elles ne quittent pas le client.\n"
              "3 · La console écrit un fichier du projet, celui qu'un ingénieur aurait écrit à "
              "la main.\n"
              "4 · Rien ne part en production sans revue : « Proposer » ouvre une demande de "
              "revue ; un ingénieur relit, la plateforme vérifie le projet, puis on publie.\n\n"
              "ARRIVE À L'ÉTAPE 4 (à avancer pour les clients sans équipe technique : voir "
              "section 11).")

    s = ecran(d, s, "8. Écran « Administration » · connexions, agents, droits, licence",
              "Administration", "connecté : administrateur", ecran_administration,
              "POUR QUI : l'administrateur, chez le client et chez Optiware.\n\n"
              "1 · Les connexions. La console ne montre jamais un mot de passe : seulement le "
              "nom du secret dans le coffre. « Tester » essaie la connexion depuis l'agent.\n"
              "2 · Les agents installés chez les clients : état, version, mise à jour.\n"
              "3 · La licence, avec sa clé qui marche hors ligne.\n"
              "4 · Le journal d'audit : qui a fait quoi et quand ; gardé, et personne ne peut "
              "le modifier.\n"
              "5 · Les rôles, donnés projet par projet.\n\n"
              "ARRIVE À L'ÉTAPE 3 ; plusieurs clients à l'étape 4.")

    sec = section(d, s, "9. Ce que la console ne fait pas")
    bas = rangee(d, s + 80, [
        ("PAS DE TABLEAUX DE BORD\nLes rapports restent dans OAC, Power BI ou Excel. La console "
         "prépare et contrôle les chiffres ; elle ne les met pas en page.", ROUGE, 2),
        ("PAS DE MOT DE PASSE AFFICHÉ\nSeulement le nom du secret dans le coffre, même pour "
         "l'administrateur.", ROUGE, 2),
        ("PAS DE CHANGEMENT DIRECT EN PRODUCTION\nTout changement passe par une revue et la "
         "vérification du projet, comme du code.", ROUGE, 2),
        ("PAS DE DONNÉES QUI SORTENT\nEn mode hybride, la console dans le cloud ne garde que "
         "les métadonnées ; un aperçu passe par l'agent, à la demande, et n'est pas gardé.",
         ROUGE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "10. Comment elle est faite")
    bas = rangee(d, s + 80, [
        ("REACT\nComme la visionneuse Excalidraw du projet, avec une bibliothèque de composants "
         "ouverte.", JAUNE, 2),
        ("HORS LIGNE\nAucune ressource prise sur internet : elle marche sur un réseau fermé, "
         "comme celui de la CNSS.", JAUNE, 2),
        ("DEUX LANGUES\nFrançais et anglais dès le début, pour les caisses de la CIPRES et les "
         "autres clients.", JAUNE, 2),
        ("SUR TÉLÉPHONE\nEn lecture : les alertes, et la validation d'un indicateur par le "
         "métier.", JAUNE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "11. Quand arrive chaque écran (feuille de route, schéma 04)")
    cadres = ligne_de_cadres(d, s + 80, [
        ("ÉTAPE 1\nPas de console : la ligne de commande et la table des exécutions.", GRIS),
        ("ÉTAPE 2\nAccueil, Exécutions et le graphe du Lignage, en lecture. Relancer et "
         "administrer : en ligne de commande.", JAUNE),
        ("ÉTAPE 3\nIndicateurs et validation · relance depuis la console · lignage par colonne "
         "· Administration\nVERSION 1.0 VENDABLE", VIOLET),
        ("ÉTAPE 4\nSans code · plusieurs clients dans l'Administration.", JAUNE),
    ], size=21, gap=50, fleches=True)
    y = cadres[0]["y"] + cadres[0]["height"] + 40
    b = bloc(d, 40, y, WIDTH - 80,
             "À TRANCHER : dans une démo, c'est la console que le client voit en premier. Pour "
             "les caisses qui ont une DSI, la version 1.0 sans l'écran « Sans code » suffit. Pour "
             "des clients sans équipe technique, faut-il l'avancer à l'étape 3 ? C'est l'écran "
             "le plus cher à construire.", 22, JAUNE, sw=3)
    fermer(sec, b["y"] + b["height"])
    return d


SCHEMAS = {
    "00 vision et principes": vision,
    "01 architecture": architecture,
    "02 modele de projet": modele_de_projet,
    "03 flexibilite": flexibilite,
    "04 feuille de route": feuille_de_route,
    "05 produit optiware": produit,
    "06 console": la_console,
}


def main():
    parser = argparse.ArgumentParser(description="Schémas de la plateforme BI flexible.")
    parser.add_argument("--force", action="store_true", help="écraser les schémas existants")
    args = parser.parse_args()
    for nom, build in SCHEMAS.items():
        d = build()
        path = OUT_DIR / f"{nom}.excalidraw"
        try:
            d.save(path, force=args.force)
        except FileExistsError as e:
            print(e)
            continue
        print(("inchangé : " if d.inchange else f"{len(d.elements)} éléments écrits dans ")
              + str(path.relative_to(ROOT)))
        for w in d.warnings:
            print("ATTENTION", w)


if __name__ == "__main__":
    main()
