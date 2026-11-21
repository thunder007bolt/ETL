"""
Vision : une plateforme ETL de data engineering née du projet BI CNSS (20/11/2026).

Demande ponctuelle de l'utilisateur : « créer, à partir de la vision de ce
projet ETL, une plateforme ETL de data engineering, en pensant à Fivetran et
Airflow ». Proposition à discuter, rangée à part : elle ne touche ni le projet
CNSS, ni ses règles, ni le journal.

Deux schémas, dans schemas/plateforme/ :
  00 vision et architecture   l'idée, Fivetran et Airflow face à notre code,
                              l'architecture cible, ce qui nous distingue
  01 feuille de route         construire ou reprendre, les 5 étapes, le premier
                              pas (un manifeste), points d'attention, à trancher
  02 generique ou specifique  « est-ce pensé pour faire n'importe quel projet
                              BI ? » : noyau, adaptateurs, packs ; ce qui est
                              encore CNSS ; les limites ; le test qui le prouve

Couleurs : vert = existe déjà dans le projet CNSS ; jaune = à construire ou à
trancher ; gris = hors de la plateforme ; violet = le métier (pack CIPRES).

Usage :
    python3 excalidraw/sources/plateforme/vision_plateforme.py [--force]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "vues_cnss"))

from excalidraw_builder import C, Diagram, text_size  # noqa: E402
from commun import WIDTH, bloc, couper, entete, rangee, tableau  # noqa: E402

# Archivé le 20/11/2026 : remplacé par la plateforme flexible (plateforme_flexible.py),
# conçue sans partir du code CNSS, à la demande de l'utilisateur.
OUT_DIR = ROOT / "schemas" / "archives" / "plateforme"

VERT = (C["vert_clair"], C["vert"])
JAUNE = (C["jaune_clair"], C["jaune_bord"])
GRIS = (C["gris"], C["gris_bord"])
BLANC = (C["blanc"], C["encre"])
VIOLET = ("#f3f0ff", "#7048e8")


def fermer(sec, bas):
    """Ajuste la hauteur d'une section à son contenu ; renvoie le haut de la suivante."""
    sec["height"] = bas + 40 - sec["y"]
    return bas + 40 + 50


def section(d, top, titre):
    return d.section(0, top, WIDTH, 300, titre)


def hauteur(txt, size, w):
    return text_size(couper(txt, size, w - 44), size)[1] + 34


# ======================================================= 00 vision et architecture
def vision():
    d = Diagram(seed=2011)
    s = entete(d, "Une plateforme ETL née du projet BI CNSS",
               "Ce qu'on a déjà, ce que font Fivetran et Airflow, l'architecture cible et ce qui "
               "nous distinguerait · proposition à discuter, état au 20/11/2026 · vert = existe "
               "déjà dans le projet CNSS, jaune = à construire, gris = hors plateforme, violet = "
               "le métier")

    # ------------------------------------------------------------ 1. l'idée
    sec = section(d, s, "1. L'idée en une phrase")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             "Faire de notre chaîne Python (Oracle, puis ODS, DWH, DTM) une plateforme : on "
             "DÉCLARE ce qu'on veut charger dans un fichier lisible (un manifeste), et la "
             "plateforme se charge de LIRE les sources (comme Fivetran), d'ENCHAÎNER et de "
             "RELANCER les traitements (comme Airflow), de CONTRÔLER et de DOCUMENTER. Elle "
             "tourne sur le serveur de la caisse, même sans Internet, et arrive avec des PACKS "
             "MÉTIER (le pack CIPRES) : c'est ce que ni Fivetran ni Airflow ne font.",
             24, VERT, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    # ------------------------------------------------------------ 2. comparaison
    sec = section(d, s, "2. Fivetran, Airflow et notre code d'aujourd'hui")
    bas = tableau(d, s + 80, [
        ("Besoin", 300, {"size": 20}),
        ("Fivetran", 390, {"size": 18}),
        ("Airflow", 390, {"size": 18}),
        ("Chez nous aujourd'hui", 580, {"size": 18, "couleur": C["vert"]}),
        ("Dans la plateforme", 600, {"size": 18}),
    ], [
        ("Lire une source",
         "Des centaines de connecteurs prêts, maintenus par Fivetran",
         "Rien de prêt : on écrit le code de chaque tâche",
         "Un fichier SQL par table, lu avec oracledb, par paquets de 100 000 lignes. Oracle "
         "seulement",
         "Des connecteurs branchables : Oracle d'abord, puis Excel/CSV, PostgreSQL, SQL Server, "
         "API"),
        ("Ne lire que le nouveau",
         "Lecture incrémentale et CDC, automatiques",
         "À coder dans chaque tâche",
         "Tout est relu à chaque fois. Un curseur sur la date de mise à jour est prévu pour "
         "DIM_DOSSIER, en commentaire",
         "Un mode par table : complet, curseur (date de mise à jour), CDC plus tard"),
        ("La structure de la source change",
         "Détecté ; les nouvelles colonnes arrivent seules",
         "Non",
         "À la main : SUIVI_MODIFICATIONS_DWH.md, scripts ddl/ datés",
         "Comparaison du schéma à chaque lecture : alerte et DDL proposé"),
        ("Écrire dans la cible",
         "Un schéma normalisé ; mode historique possible",
         "Non",
         "Quatre façons dans BaseLoader : ajout en masse, MERGE par GTT, cliché archivé dans "
         "l'ODS puis TRUNCATE, suppression d'un cliché par lots",
         "Des stratégies nommées : ajout, fusion, cliché, remplacement, historique"),
        ("Enchaîner les traitements",
         "Non (un planning par connecteur)",
         "Le DAG : dépendances, parallélisme, files",
         "L'ordre des jobs Jenkins : dimensions, faits, DTM",
         "Le DAG construit à partir des manifestes (champ « après »)"),
        ("Planifier, reprendre, relancer",
         "Fréquence de synchro",
         "Planning, reprises, rattrapage (backfill)",
         "Jenkins ; option --table ; scripts/clone_dtm_cliche.py",
         "Reprises automatiques ; relance d'un cliché (la photo de janvier refaite fin janvier)"),
        ("Suivre",
         "Tableau de bord des synchros",
         "Interface : runs, journaux, durées",
         "Le fichier logs/etl.log",
         "Une table des runs (lignes, durée, statut, erreur), alertes par mail"),
        ("Transformer",
         "Par dbt, branché à côté",
         "Par des opérateurs à écrire",
         "Les requêtes DTM et leur CTE parametres",
         "Des modèles SQL qui se citent entre eux, avec des paramètres par pays"),
        ("Contrôler la qualité",
         "Contrôles de base",
         "À coder",
         "Requêtes de preuve ; comparaison à l'annuaire CNSS (à 1 % près)",
         "Des tests déclarés dans le manifeste et un rapprochement source et cible à chaque run"),
        ("Documenter, expliquer",
         "Catalogue des tables",
         "Non",
         "Schémas Excalidraw, règles par tableau, points à valider",
         "Le lignage de chaque indicateur jusqu'à la source, la doc générée, la validation par "
         "le métier"),
        ("Où ça tourne",
         "Service en ligne, payé au volume de lignes ; un mode hybride existe, piloté depuis "
         "leur cloud",
         "Sur site possible, mais à installer et à faire vivre",
         "Le serveur de la CNSS, avec Jenkins",
         "Sur site, hors ligne, installable d'un bloc"),
    ])
    s = fermer(sec, bas)

    # ------------------------------------------------------------ 3. architecture
    sec = section(d, s, "3. L'architecture cible")
    top = s + 80

    # l'orchestrateur, au-dessus des étapes qu'il pilote
    col_w, gap = 340, 60
    xs = [40 + i * (col_w + gap) for i in range(6)]
    orch_txt = ("ORCHESTRATEUR (façon Airflow) : planning · enchaînement dimensions, faits, DTM · "
                "reprises · relance d'un cliché · alertes. Aujourd'hui : Jenkins")
    orch = bloc(d, xs[1], top, xs[4] + col_w - xs[1], orch_txt, 22, JAUNE, sw=3)

    # le flux des données
    flux = [
        ("SOURCES\n· Oracle GRH\n· Oracle prestations et recouvrement\n· vues V_BI_ "
         "alimentées par Excel\n· plus tard : autres bases, API", GRIS),
        ("CONNECTEURS (façon Fivetran)\n· découvrir le schéma\n· lire, en complet ou "
         "seulement le nouveau\n· signaler un changement de structure", JAUNE),
        ("ZONE BRUTE\n· ODS : la copie archivée de chaque cliché\n· fichiers Parquet de "
         "passage", VERT),
        ("DWH\n· faits et dimensions\n· chargés par cliché ou par fusion", VERT),
        ("DTM\n· les indicateurs\n· tableaux CIPRES T25 à T65, et les suivants", VERT),
        ("USAGES\n· Oracle Analytics (OAC)\n· exports Excel\n· API, autres outils", GRIS),
    ]
    y_flux = top + orch["height"] + 90
    h_flux = max(hauteur(t, 21, col_w) for t, _ in flux)
    boites = [bloc(d, x, y_flux, col_w, t, 21, c, sw=2, h=h_flux) for (t, c), x in zip(flux, xs)]
    for a, b in zip(boites, boites[1:]):
        d.harrow(a, b, stroke_width=3)
    for b in boites[1:5]:
        cx = b["x"] + b["width"] / 2
        d.arrow(cx, orch["y"] + orch["height"] + 6, cx, b["y"] - 6, orch, b, style="dashed",
                color=C["jaune_bord"])

    # le socle commun
    y_socle = y_flux + h_flux + 60
    d.text(40, y_socle, "LE SOCLE, commun à toutes les étapes", 24, C["doux"])
    socle = [
        ("MANIFESTES\nun fichier YAML par pipeline, suivi par git. Aujourd'hui : FACT_CONFIG, "
         "DIM_CONFIG, DTM_CONFIG en Python", JAUNE),
        ("SUIVI DES RUNS\nqui, quand, combien de lignes, combien de temps, quelle erreur", JAUNE),
        ("QUALITÉ ET PREUVES\ntests déclarés, rapprochement source et cible, comparaison à une "
         "publication", JAUNE),
        ("CATALOGUE ET LIGNAGE\nde l'indicateur à la table source, avec ses règles", JAUNE),
        ("SECRETS ET DROITS\nmots de passe dans un coffre, droits par rôle, données "
         "personnelles masquées", JAUNE),
        ("CONSOLE\nune page web : pipelines, runs, qualité, règles, validation", JAUNE),
    ]
    y_socle += 50
    h_socle = max(hauteur(t, 20, col_w) for t, _ in socle)
    for (t, c), x in zip(socle, xs):
        bloc(d, x, y_socle, col_w, t, 20, c, sw=2, h=h_socle)

    # le pack métier
    y_pack = y_socle + h_socle + 50
    pack = bloc(d, 40, y_pack, WIDTH - 80,
                "PACKS MÉTIER, posés sur le socle · le premier : le PACK CIPRES = les connecteurs "
                "des logiciels de caisse · le modèle DWH et DTM · les tableaux de l'Annuaire · "
                "les règles par tableau, avec leurs sources · les paramètres par pays · les "
                "fiches de validation. Brancher un nouveau pays = brancher ses sources, régler "
                "ses paramètres, faire valider. C'est ce qui nous distingue.",
                22, VIOLET, sw=3)

    # légende
    y_leg = y_pack + pack["height"] + 40
    x = 40
    for txt, (bg, bord) in [("existe déjà dans le projet CNSS", VERT), ("à construire", JAUNE),
                            ("hors plateforme", GRIS), ("le métier", VIOLET)]:
        d.box(x, y_leg, 50, 34, bg=bg, stroke=bord)
        d.text(x + 64, y_leg + 3, txt, 20, C["doux"])
        x += 64 + text_size(txt, 20)[0] + 60
    s = fermer(sec, y_leg + 34)

    # ------------------------------------------------------------ 4. ce qui distingue
    sec = section(d, s, "4. Ce qui nous distinguerait de Fivetran et d'Airflow")
    bas = rangee(d, s + 80, [
        ("SUR SITE, HORS LIGNE\nLes données des assurés restent chez la caisse, dans le pays. "
         "Tout s'installe sur son serveur et marche sans Internet, comme la visionneuse "
         "Excalidraw du projet aujourd'hui.", VIOLET, 2),
        ("DÉCLARER PLUTÔT QUE CODER\nUn pipeline = un manifeste lisible : source, tables, mode "
         "de chargement, cible, tests, planning. Avec Airflow, chaque DAG s'écrit en Python.",
         VIOLET, 2),
        ("LE MÉTIER DEDANS\nChaque indicateur porte ses règles, sa source et sa preuve, et le "
         "métier les valide (visa, points à valider). Ni Fivetran ni Airflow ne savent ce "
         "qu'est un tableau CIPRES.", VIOLET, 2),
        ("ORACLE D'ABORD\nLes caisses tournent sur Oracle, parfois ancien (avant 12c). Le "
         "code sait déjà charger en masse, passer par des GTT, archiver des clichés dans "
         "l'ODS.", VIOLET, 2),
    ], size=21)
    fermer(sec, bas)
    return d


# ======================================================= 01 feuille de route
MANIFESTE = """\
pipeline: grh_faits
source: oracle_grh        # connexion déclarée à part,
                          # mot de passe dans le coffre
cible: dwh
planning: "0 2 1 * *"     # le 1er du mois à 2 h
apres: [grh_dims]         # attend les dimensions

tables:
  - cible: FAIT_GRH_SITUATION
    sql: grh_situation.sql
    chargement: cliche    # ODS, TRUNCATE, ajout en masse
    tests: [non_vide, meme_nombre_que_la_source]

  - cible: FAIT_GRH_MOUVEMENT
    sql: grh_mouvement.sql
    chargement: cliche
    tests: [non_vide]"""


def feuille_de_route():
    d = Diagram(seed=2012)
    s = entete(d, "Feuille de route de la plateforme",
               "Ce qu'on construit, ce qu'on reprend de l'open source, les 5 étapes, le premier "
               "pas et les points d'attention · proposition à discuter, état au 20/11/2026")

    # ------------------------------------------------------------ 1. construire ou reprendre
    sec = section(d, s, "1. Construire ou reprendre")
    bas = tableau(d, s + 80, [
        ("Brique", 320, {"size": 20}),
        ("On construit", 700, {"size": 19}),
        ("On reprend (open source, sur site)", 620, {"size": 19}),
        ("Pourquoi", 620, {"size": 19, "couleur": C["doux"]}),
    ], [
        ("Orchestration", "La génération des DAG à partir des manifestes",
         "Apache Airflow (ou Dagster)",
         "Un ordonnanceur fiable demande des années ; Airflow est le standard et s'installe "
         "sur site"),
        ("Connecteurs", "Oracle (le code actuel) et Excel/CSV, pour les vues V_BI_",
         "dlt ou Airbyte pour les autres bases ; Debezium pour le CDC. À évaluer",
         "Écrire et maintenir des centaines de connecteurs n'est pas notre métier"),
        ("Transformations", "Les conventions et les paramètres par pays (la CTE parametres "
         "devient un fichier par pays)",
         "dbt-core avec son adaptateur Oracle (dbt-oracle). À évaluer",
         "Dépendances entre requêtes, tests et docs déjà résolus"),
        ("Qualité", "Les tests métier : case vide jamais 0, comparaison à une publication "
         "(Annuaire)", "Les tests de dbt, ou Soda", "Les tests génériques existent déjà"),
        ("Catalogue et lignage", "Le lien indicateur, règle, table source",
         "OpenMetadata, plus tard. À évaluer", "Le lignage technique est un problème connu"),
        ("Console", "La vue métier : règles, validation, fraîcheur des indicateurs",
         "L'interface d'Airflow pour la vue technique", "Ne pas refaire ce qui existe"),
        ("Packs métier", "Tout : modèle, règles, paramètres, fiches de validation", "Rien",
         "C'est notre valeur : personne d'autre ne le fait"),
    ])
    b = bloc(d, 40, bas + 30, WIDTH - 80,
             "EN BREF : on ne réécrit ni Airflow ni des centaines de connecteurs. On construit la "
             "couche qui manque : les manifestes, les packs métier, la validation par le métier "
             "et l'installation sur site, hors ligne.", 22, JAUNE, sw=3)
    s = fermer(sec, b["y"] + b["height"])

    # ------------------------------------------------------------ 2. les étapes
    sec = section(d, s, "2. Les 5 étapes : chacune sert seule, même si la suivante n'arrive pas")
    etapes = [
        ("ÉTAPE 0 · SORTIR LE CŒUR, SANS RIEN CASSER\n"
         "· un manifeste YAML par pipeline, tiré de FACT_CONFIG, DIM_CONFIG, DTM_CONFIG\n"
         "· un seul lanceur à la place des 6 run_*.py ; Jenkins l'appelle\n"
         "· une table de suivi des runs\n"
         "· les mots de passe sortis de git\n\n"
         "PREUVE : un même cliché chargé par l'ancien et le nouveau lanceur donne les mêmes "
         "lignes"),
        ("ÉTAPE 1 · LES CONNECTEURS\n"
         "· une interface commune : découvrir, lire, garder l'état\n"
         "· Oracle, puis Excel/CSV (les vues V_BI_)\n"
         "· lecture incrémentale par curseur (DIM_DOSSIER d'abord)\n"
         "· alerte quand la structure de la source change\n\n"
         "PREUVE : sur un mois, l'incrémental donne le même résultat que le complet"),
        ("ÉTAPE 2 · L'ORCHESTRATION\n"
         "· Airflow sur le serveur, installé sans Internet\n"
         "· DAG généré : dimensions, puis faits, puis DTM\n"
         "· reprises, relance d'un cliché, alertes par mail\n"
         "· Jenkins arrêté\n\n"
         "PREUVE : la chaîne du mois tourne seule, et la relance de fin janvier aussi"),
        ("ÉTAPE 3 · QUALITÉ, LIGNAGE, DOCS\n"
         "· tests déclarés dans les manifestes\n"
         "· rapprochement source et cible à chaque run\n"
         "· lignage de chaque tableau jusqu'aux tables source\n"
         "· docs générées (Excalidraw, HTML)\n\n"
         "PREUVE : chaque tableau CIPRES a son lignage et ses tests au vert"),
        ("ÉTAPE 4 · CONSOLE, PACK CIPRES, 2E PAYS\n"
         "· console web : runs, qualité, règles, validation\n"
         "· plusieurs pays, des paramètres par pays\n"
         "· le pack CIPRES, installable hors ligne\n"
         "· un deuxième pays branché\n\n"
         "PREUVE : le deuxième pays est branché sans toucher au code"),
    ]
    w, gap = 420, 60
    h = max(hauteur(t, 20, w) for t in etapes)
    boites = [bloc(d, 40 + i * (w + gap), s + 80, w, t, 20, JAUNE, sw=3 if i == 0 else 2, h=h)
              for i, t in enumerate(etapes)]
    for a, b in zip(boites, boites[1:]):
        d.harrow(a, b, stroke_width=3)
    s = fermer(sec, s + 80 + h)

    # ------------------------------------------------------------ 3. le premier pas
    sec = section(d, s, "3. Le premier pas : à quoi ressemble un manifeste")
    top = s + 80
    code_w = 1000
    expl_x = 40 + code_w + 60
    expl_w = WIDTH - 40 - expl_x
    expl_txt = ("CE QUE LA PLATEFORME EN FAIT\n"
                "· elle lit le fichier et place grh_faits dans le DAG, après grh_dims et avant "
                "grh_dtm\n"
                "· elle ouvre la connexion oracle_grh, avec le mot de passe du coffre (plus "
                "dans .env)\n"
                "· pour chaque table : elle archive le cliché dans l'ODS, vide la table, la "
                "recharge en masse, avec le code actuel de BaseLoader\n"
                "· elle lance les tests, note le run (lignes, durée, statut) et alerte en cas "
                "d'échec\n"
                "· relancer un mois : plateforme run grh_faits --cliche 012027\n\n"
                "CE QUI CHANGE POUR LA CNSS : rien. Même SQL, mêmes tables, mêmes résultats. Seule "
                "la façon de déclarer change.")
    _, ch = text_size(MANIFESTE, 20, mono=True)
    h = max(ch + 50, hauteur(expl_txt, 21, expl_w))
    g = d.group()
    cadre = d.box(40, top, code_w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(65, top + 25, MANIFESTE, 20, groups=g, inside=cadre, mono=True)
    bloc(d, expl_x, top, expl_w, expl_txt, 21, VERT, sw=2, h=h)
    s = fermer(sec, top + h)

    # ------------------------------------------------------------ 4. attention
    sec = section(d, s, "4. Points d'attention")
    bas = rangee(d, s + 80, [
        ("LA LIVRAISON CNSS D'ABORD\nLa plateforme naît à côté du projet. La CNSS ne bascule "
         "qu'une fois la preuve de l'étape 0 faite.", JAUNE, 2),
        ("LES GROS VOLUMES\nLe passage par pandas et executemany a ses limites sur des faits de "
         "dizaines de millions de lignes. Prévoir un chemin direct d'Oracle à Oracle pour les "
         "plus grosses tables.", JAUNE, 2),
        ("LES SECRETS\nLes mots de passe sont dans .env, et ce fichier est suivi par git depuis "
         "le premier commit. À sortir dès l'étape 0.", (C["rouge_clair"], C["rouge"]), 2),
        ("LES DONNÉES PERSONNELLES\nNoms, numéros d'assurés, comptes bancaires : masquage et "
         "droits par rôle ; rien ne sort du pays.", JAUNE, 2),
        ("UNE PETITE ÉQUIPE\nChaque étape doit être utile seule. Pas de grand chantier qui ne "
         "rapporte qu'à la fin.", JAUNE, 2),
    ], size=20)
    s = fermer(sec, bas)

    # ------------------------------------------------------------ 5. à trancher
    sec = section(d, s, "5. À trancher par toi")
    bas = rangee(d, s + 80, [
        ("POUR QUI ?\nUn produit d'Optiware, vendu à d'autres caisses (CIPRES et au-delà), ou "
         "un outil interne qui sert à déployer le projet dans les autres pays ?", JAUNE, 3),
        ("QUEL MOTEUR ?\nAirflow comme moteur, avec nos manifestes au-dessus (proposé), ou "
         "notre propre ordonnanceur ?", JAUNE, 3),
        ("QUAND ?\nL'étape 0 pendant la mission CNSS (elle aide aussi la CNSS), le reste après "
         "la livraison ?", JAUNE, 3),
    ], size=21)
    fermer(sec, bas)
    return d


# ======================================================= 02 generique ou specifique
def etage(d, top, label, couleurs_label, boites, size=20):
    """Un étage de l'architecture : un cartouche à gauche, des cadres à droite.
    boites : liste de (texte, couleurs). Renvoie le cartouche."""
    x0, lw, gap = 40, 380, 30
    cx = x0 + lw + 40
    w = (WIDTH - 40 - cx - gap * (len(boites) - 1)) / len(boites)
    h = max([hauteur(t, size, w) for t, _ in boites] + [hauteur(label, 22, lw)])
    cartouche = bloc(d, x0, top, lw, label, 22, couleurs_label, sw=3, h=h)
    for i, (t, c) in enumerate(boites):
        bloc(d, cx + i * (w + gap), top, w, t, size, c, sw=2, h=h)
    return cartouche


def generique():
    d = Diagram(seed=2013)
    s = entete(d, "Générique, ou fait pour la CNSS ?",
               "Réponse à « l'architecture est-elle pensée pour faire n'importe quel projet BI ? » "
               "· pas encore tout à fait : ce qui est déjà générique, ce qui reste CNSS, la règle "
               "et le test · état au 20/11/2026 · vert = existe déjà, jaune = à construire, "
               "violet = le métier, rouge = encore CNSS")
    ROUGE = (C["rouge_clair"], C["rouge"])

    # ------------------------------------------------------------ 1. réponse courte
    sec = section(d, s, "1. La réponse courte")
    bas = rangee(d, s + 80, [
        ("OUI POUR LE SOCLE\nL'orchestrateur, les connecteurs, les manifestes, le suivi des "
         "runs, la qualité et la console ne parlent pas de la CNSS. Le dépôt sépare déjà le "
         "code commun (shared/) et le métier (domaines/grh, domaines/prestation_recouvrement).",
         VERT, 2),
        ("PAS ENCORE POUR LE RESTE\nLe schéma 00 et le code gardent des choses de la CNSS : "
         "Oracle partout, la photo mensuelle (cliché) imposée, les couches ODS, DWH, DTM "
         "figées, les noms de schémas écrits dans le SQL, le moteur recopié dans chaque "
         "domaine.", ROUGE, 2),
        ("LA RÈGLE POUR Y ARRIVER\nLe noyau ne connaît aucun mot métier, aucun nom de table, "
         "aucune syntaxe de base. Tout le reste se branche : les bases par des adaptateurs, "
         "le métier par des packs.", JAUNE, 3),
    ], size=21)
    s = fermer(sec, bas)

    # ------------------------------------------------------------ 2. trois étages
    sec = section(d, s, "2. L'architecture générique : trois étages")
    top = s + 80
    packs = etage(d, top, "PROJETS ET PACKS\nchangent à chaque projet", VIOLET, [
        ("PACK CIPRES\nCNSS Burkina, puis les autres pays : modèle, tableaux de l'Annuaire, "
         "règles, paramètres par pays, fiches de validation", VIOLET),
        ("PACK FINANCE (exemple)\nbalance, budget, trésorerie, à partir d'un ERP", VIOLET),
        ("PACK RH (exemple)\neffectifs, mouvements, masse salariale", VIOLET),
        ("UN PROJET, C'EST\ndes manifestes, du SQL, des paramètres, des règles et des tests. "
         "Rien d'autre", VIOLET),
    ])
    top = packs["y"] + packs["height"] + 60
    adapt = etage(d, top, "ADAPTATEURS\nse branchent selon le client", JAUNE, [
        ("SOURCES\nOracle (fait) · PostgreSQL · SQL Server · MySQL · Excel/CSV · API REST · "
         "SFTP", JAUNE),
        ("DESTINATIONS, l'entrepôt\nOracle (fait) · PostgreSQL · SQL Server · DuckDB\nChacune "
         "a son dialecte : elle sait faire un ajout en masse, une fusion, une photo", JAUNE),
        ("OUTILS BI\nOAC (fait) · Power BI · Metabase · Superset · exports Excel\nIls lisent "
         "les tables d'indicateurs", JAUNE),
    ])
    top = adapt["y"] + adapt["height"] + 60
    noyau = etage(d, top, "NOYAU\nne change jamais d'un projet à l'autre", JAUNE, [
        ("lire et vérifier les manifestes", JAUNE),
        ("construire le DAG, lancer, reprendre, relancer", JAUNE),
        ("charger : ajout, fusion, photo, remplacement, historique", JAUNE),
        ("suivre les runs, alerter", JAUNE),
        ("tests et preuves génériques", JAUNE),
        ("lignage, docs, secrets, console", JAUNE),
    ], size=20)
    d.varrow(packs, adapt, stroke_width=3)
    d.varrow(adapt, noyau, stroke_width=3)
    b = bloc(d, 40, noyau["y"] + noyau["height"] + 40, WIDTH - 80,
             "Aujourd'hui, l'embryon du noyau est shared/ (BaseLoader, BaseExtractor, réglages) et "
             "les deux premiers packs sont domaines/grh et domaines/prestation_recouvrement. Les "
             "flèches se lisent : un pack s'appuie sur des adaptateurs, qui s'appuient sur le "
             "noyau. Jamais l'inverse.", 20, BLANC)
    s = fermer(sec, b["y"] + b["height"])

    # ------------------------------------------------------------ 3. ce qui reste CNSS
    sec = section(d, s, "3. Ce qui est encore CNSS, et comment le rendre générique")
    bas = tableau(d, s + 80, [
        ("Ce qui est encore CNSS", 560, {"size": 20, "couleur": C["rouge"]}),
        ("Où on le voit", 760, {"size": 19, "mono": False, "couleur": C["doux"]}),
        ("Pour être générique", 940, {"size": 19}),
    ], [
        ("Oracle partout",
         "shared/ : une cinquantaine de lignes propres à Oracle (oracledb, INSERT /*+ APPEND */, "
         "MERGE … FROM DUAL, tables GTT, ROWID, ROWNUM)",
         "Une interface de dialecte : le noyau demande « fusionner ces lignes », l'adaptateur "
         "de la base sait comment. Oracle est le premier dialecte, pas le seul"),
        ("Une seule source et une seule cible",
         "get_source_connection() et get_dw_connection() dans shared/utils/db_utils.py",
         "Autant de connexions nommées que le projet en déclare"),
        ("La photo mensuelle imposée",
         "Le pipeline des faits calcule le cliché MMYYYY à partir du jour et ajoute la colonne "
         "CLICHE",
         "Une stratégie « photo » réglable : période (jour, mois, trimestre), nom de colonne, "
         "durée de conservation. Ou pas de photo du tout"),
        ("Les couches ODS, DWH, DTM figées",
         "Le schéma 00 ; l'archivage ODS codé dans BaseLoader",
         "Des couches déclarées par le projet : leur nombre et leurs noms sont libres (brute, "
         "propre, indicateurs, ou ODS, DWH, DTM)"),
        ("Les noms de schémas écrits dans le SQL",
         "USER_GRH dans 38 fichiers SQL, DWH. dans 36, DTM. dans 30",
         "Des variables remplacées au lancement, propres à chaque client"),
        ("Le moteur recopié dans chaque domaine",
         "pipeline_facts, pipeline_dims, pipeline_dtm existent en double (grh et "
         "prestation_recouvrement)",
         "Un seul moteur dans le noyau ; un domaine n'apporte que des manifestes et du SQL"),
        ("OAC en sortie",
         "Le schéma 00 (usages)",
         "N'importe quel outil BI lit les tables d'indicateurs ; plus tard, des indicateurs "
         "définis une seule fois pour tous les outils"),
        ("Les règles CIPRES et leur validation",
         "Les règles par tableau, les points à valider, les visas",
         "Elles restent dans le pack. Le noyau offre le mécanisme : une règle, sa preuve, sa "
         "validation"),
    ])
    s = fermer(sec, bas)

    # ------------------------------------------------------------ 4. limites
    sec = section(d, s, "4. Ce que « n'importe quel projet BI » veut dire ici")
    bas = rangee(d, s + 80, [
        ("OUI\nLes projets BI en lots (chaque nuit, chaque mois), à partir de bases "
         "relationnelles et de fichiers, jusqu'à des dizaines de millions de lignes par table, "
         "sur site ou dans le cloud. C'est l'essentiel des projets BI des entreprises et des "
         "administrations.", VERT, 2),
        ("PAS SANS AUTRE OUTIL\nLe temps réel (flux, Kafka), les très gros volumes (téraoctets, "
         "Spark), les données non structurées (textes, images), l'apprentissage automatique. "
         "D'autres outils le font ; la plateforme pourra s'y brancher plus tard.", GRIS, 2),
    ], size=21)
    s = fermer(sec, bas)

    # ------------------------------------------------------------ 5. le test
    sec = section(d, s, "5. Le test qui prouve que c'est générique")
    b = bloc(d, 40, s + 80, WIDTH - 80,
             "Monter un deuxième projet qui n'a rien à voir avec la CNSS (par exemple la BI "
             "financière d'une entreprise, sur PostgreSQL, avec Power BI) sans changer une ligne "
             "du noyau. S'il faut toucher au noyau, il n'est pas encore générique.\n\n"
             "CE QUE ÇA CHANGE À LA FEUILLE DE ROUTE (schéma 01) : à l'étape 0, un seul moteur, "
             "les noms de schémas en variables, une interface de dialecte (Oracle seul au "
             "début) ; à l'étape 1, une destination PostgreSQL et un petit pack de démonstration "
             "pour faire la preuve.", 22, JAUNE, sw=3)
    fermer(sec, b["y"] + b["height"])
    return d


SCHEMAS = {
    "00 vision et architecture": vision,
    "01 feuille de route": feuille_de_route,
    "02 generique ou specifique": generique,
}


def main():
    parser = argparse.ArgumentParser(description="Schémas de la vision de la plateforme ETL.")
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
