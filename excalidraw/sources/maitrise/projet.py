"""
Fiches de maîtrise communes à tous les domaines : le sommaire (00 commencer ici) et le dossier
« 0 projet » (le projet en bref, la méthode de travail, les documents de référence).
"""

from fiches import Bloc, Fiche, Partie

SOMMAIRE = Fiche(
    fichier="00 commencer ici.excalidraw",
    titre="Maîtriser le projet BI de la CNSS : par où commencer",
    sous_titre="Le dossier « maitrise » rassemble des fiches de révision, domaine par domaine, "
               "pour s'approprier tout le travail fait et pouvoir le défendre · état au "
               "30/10/2026",
    objectif="dans quel ordre lire les fiches, ce que contient chaque dossier, comment chaque "
             "bloc se lit, et comment le dossier grandira quand on attaquera les autres domaines.",
    parties=[
        Partie("Comment réviser", colonnes=4, blocs=[
            Bloc("Lire dans l'ordre", [
                ("", "Commence par le dossier « 0 projet », puis un domaine. Dans un domaine, "
                     "suis les numéros : 01, 02, 03… Chaque fiche dit en haut ce que tu sauras "
                     "à la fin."),
            ]),
            Bloc("Un bloc = une idée", [
                ("", "Chaque bloc se lit seul : un titre, puis des paragraphes courts (« Ce que "
                     "c'est », « Pourquoi », « La preuve », « Si on te demande »). Tu peux "
                     "déplacer un bloc, l'annoter, le colorer."),
            ]),
            Bloc("Les pastilles", [
                ("", "R6, D2, B12, H4, P25, Q1 : ce sont les références du travail. R et D sont "
                     "des règles, B un fait découvert dans la base, H une erreur supposée des "
                     "DTM, P une requête, Q une question au métier."),
            ]),
            Bloc("Pour retenir", [
                ("", "Après une fiche, relis seulement les « Si on te demande » et essaie d'y "
                     "répondre à voix haute. La dernière fiche de chaque domaine rassemble les "
                     "questions qu'on peut te poser, avec la réponse."),
            ]),
        ]),
        Partie("Les dossiers", colonnes=3,
               intro="Un dossier par domaine de l'Annuaire CIPRES, numéroté comme lui, plus un "
                     "dossier commun. Les domaines pas encore commencés auront le même plan.",
               blocs=[
            Bloc("0 projet", [
                ("Les fiches", "01 Le projet en bref\n02 La méthode de travail\n03 Les "
                               "documents de référence"),
                ("À lire", "en premier : le contexte, la façon de travailler et les sources, "
                           "communs à tous les domaines."),
            ], genre="etape", etat="fait"),
            Bloc("3 prestations (T25 à T57)", [
                ("Les fiches", "01 Le domaine en bref · 02 Le vocabulaire · 03 Les données · "
                               "04 La démarche · 05 Les découvertes · 06 Les règles des "
                               "pensionnés et des paiements · 07 Les règles des dossiers · 08 "
                               "Les erreurs des DTM actuels · 09 Les questions ouvertes · 10 Où "
                               "on en est · 11 Se préparer aux questions"),
                ("À lire", "le domaine sur lequel on travaille depuis le 26/10/2026."),
            ], genre="etape", etat="fait"),
            Bloc("4 gestion administrative (T58 à T65)", [
                ("Ce qui existe", "le travail sur le personnel fait en octobre : effectif, "
                                  "mouvements, formation, masse salariale, requêtes DTM "
                                  "corrigées (analyses/grh)."),
                ("À faire", "rédiger ses fiches sur le même plan que les prestations."),
            ], genre="etape", etat="à faire"),
            Bloc("1 immatriculation (T1 à T10)", [
                ("Ce que c'est", "les employeurs et les travailleurs immatriculés, leurs "
                                 "mouvements."),
                ("À faire", "pas encore commencé ; mêmes fiches quand on l'attaquera."),
            ], genre="etape", etat="à faire"),
            Bloc("2 recouvrement (T11 à T24)", [
                ("Ce que c'est", "les déclarations, les cotisations recouvrées, les créances."),
                ("À faire", "pas encore commencé ; mêmes fiches quand on l'attaquera."),
            ], genre="etape", etat="à faire"),
            Bloc("5 gestion financière (T66 à T76)", [
                ("Ce que c'est", "les ratios de gestion, le budget, les placements, les "
                                 "réserves."),
                ("À faire", "pas encore commencé ; mêmes fiches quand on l'attaquera."),
            ], genre="etape", etat="à faire"),
        ]),
        Partie("Le même plan pour chaque domaine", colonnes=4, flux=True,
               intro="Chaque domaine suit ce chemin. Les flèches donnent l'ordre de lecture.",
               blocs=[
            Bloc("Le domaine en bref", [("", "Ce que fait la CNSS dans ce domaine, les "
                                                "tableaux CIPRES demandés, où on en est.")],
                 ref="01"),
            Bloc("Le vocabulaire", [("", "Les mots du métier et du projet, un bloc par "
                                            "mot, avec un exemple.")], ref="02"),
            Bloc("Les données", [("", "Les tables de la base, leurs liens, les codes et "
                                         "leurs pièges.")], ref="03"),
            Bloc("La démarche", [("", "Les lots de requêtes, dans l'ordre : la question, ce "
                                         "qu'on a trouvé, ce que ça a changé.")], ref="04"),
            Bloc("Les découvertes", [("", "Les faits établis dans les données, avec leur "
                                             "chiffre et leur preuve.")], ref="05"),
            Bloc("Les règles", [("", "Chaque règle de calcul : en clair, pourquoi, la "
                                              "preuve, son état.")], ref="06-07"),
            Bloc("Les erreurs des DTM", [("", "Ce que font les requêtes actuelles, et ce "
                                                 "qui est faux.")], ref="08"),
            Bloc("Questions ouvertes et suite", [("", "Ce qui attend une réponse, ce qui "
                                                      "reste à faire, et par qui.")], ref="09-10"),
            Bloc("Se préparer aux questions", [("", "Les chiffres à connaître et les "
                                                       "questions qu'on peut te poser.")],
                 ref="11"),
        ]),
        Partie("À côté de la maîtrise", colonnes=3, blocs=[
            Bloc("Les schémas de travail", [
                ("", "schemas/prestations : le schéma de toutes les règles, les tables, le "
                     "parcours d'un dossier, les fiches de validation T25 à T27. Plus "
                     "détaillés, ils restent la référence."),
            ], genre="donnee"),
            Bloc("L'annuaire de la CNSS", [
                ("", "schemas/annuaire cnss : les comparaisons entre notre base et l'annuaire "
                     "statistique de la CNSS (2024 : les pensions)."),
            ], genre="donnee"),
            Bloc("Les analyses", [
                ("", "analyses/<domaine> : les requêtes (lots), leurs résultats, le README "
                     "(constats, règles) et le TODO de chaque domaine."),
            ], genre="donnee"),
        ]),
    ],
)

PROJET = [
    Fiche(
        fichier="0 projet/01 le projet en bref.excalidraw",
        titre="Le projet BI de la CNSS en bref",
        sous_titre="Qui, quoi, pourquoi, et comment les données circulent · état au 30/10/2026",
        objectif="ce qu'est la CNSS et ce que lui demande la CIPRES, les cinq domaines de "
                 "l'Annuaire, le chemin des données de la base jusqu'aux tableaux, et les règles "
                 "du jeu du projet.",
        parties=[
            Partie("Qui et quoi", colonnes=3, blocs=[
                Bloc("La CNSS", [
                    ("Ce que c'est", "La Caisse nationale de sécurité sociale du Burkina "
                                     "Faso. Elle immatricule les employeurs et les "
                                     "travailleurs, recouvre les cotisations (21,5 % des "
                                     "salaires) et verse les prestations."),
                    ("Ses branches", "les pensions (vieillesse, invalidité, décès), les "
                                     "risques professionnels (accidents du travail, maladies "
                                     "professionnelles), les prestations familiales (avec la "
                                     "maternité), et une action sanitaire et sociale."),
                    ("Un ordre de grandeur", "en 2024 : 617 024 travailleurs immatriculés, "
                                             "155,5 Md de cotisations, 62 231 pensionnés "
                                             "(annuaire de la CNSS)."),
                ]),
                Bloc("La CIPRES", [
                    ("Ce que c'est", "La Conférence interafricaine de la prévoyance sociale. "
                                     "Elle fixe un Annuaire statistique commun aux caisses : 76 "
                                     "tableaux, de T1 à T76, que chaque caisse doit remplir."),
                    ("Ses normes", "par exemple : liquider un dossier en moins de 45 jours, "
                                   "contrôler au moins 5 % des dossiers payés, garder les indus "
                                   "sous 2 %."),
                    ("Pourquoi c'est important", "nos tableaux seront comparés à ces normes : "
                                                 "un mauvais chiffre peut faire croire à une "
                                                 "mauvaise gestion."),
                ]),
                Bloc("Le projet BI", [
                    ("Le but", "produire automatiquement les tableaux CIPRES et les indicateurs "
                               "de la CNSS, à partir de sa base de gestion."),
                    ("Ton rôle", "l'intégration (Python, Jenkins) et le modèle des tables DTM, "
                                 "repris de l'équipe précédente ; mission sur le terrain au "
                                 "Burkina depuis le 19/10/2026."),
                    ("Ce qu'on attend de toi", "des chiffres justes, que tu sais expliquer et "
                                               "défendre devant le métier et la DSI."),
                ]),
            ]),
            Partie("Les cinq domaines de l'Annuaire CIPRES", colonnes=5,
                   intro="Le dossier « maitrise » suit ces domaines, avec leurs numéros.",
                   blocs=[
                Bloc("1 Immatriculation", [("Tableaux", "T1 à T10"),
                                           ("Contenu", "employeurs et travailleurs "
                                                       "immatriculés, leurs mouvements")],
                     genre="etape", etat="à faire"),
                Bloc("2 Recouvrement", [("Tableaux", "T11 à T24"),
                                        ("Contenu", "déclarations, cotisations recouvrées, "
                                                    "créances, masse salariale déclarée")],
                     genre="etape", etat="à faire"),
                Bloc("3 Prestations", [("Tableaux", "T25 à T57"),
                                       ("Contenu", "dossiers, contrôles, indus, pensionnés, "
                                                   "nouveaux bénéficiaires, évolution sur 5 "
                                                   "ans")],
                     genre="etape", etat="en cours"),
                Bloc("4 Gestion administrative", [("Tableaux", "T58 à T65"),
                                                  ("Contenu", "le personnel de la CNSS : "
                                                              "effectif, mouvements, "
                                                              "formation, masse salariale")],
                     genre="etape", etat="fait"),
                Bloc("5 Gestion financière", [("Tableaux", "T66 à T76"),
                                              ("Contenu", "ratios de gestion, budget, "
                                                          "placements, réserves")],
                     genre="etape", etat="à faire"),
            ]),
            Partie("Le chemin des données", colonnes=5, flux=True,
                   intro="Chaque mois, les données vont de la base de gestion jusqu'aux "
                         "tableaux.",
                   blocs=[
                Bloc("La base de gestion", [
                    ("", "La base Oracle des applications de la CNSS. Les prestations sont dans "
                         "le schéma USER_DWH, le personnel dans USER_GRH."),
                    ("Attention", "notre copie de travail est arrêtée au 02/02/2026."),
                ], genre="donnee"),
                Bloc("L'ETL", [
                    ("", "Des programmes Python, lancés par Jenkins, copient chaque mois les "
                         "tables de la base dans l'entrepôt."),
                ], genre="donnee"),
                Bloc("L'entrepôt (DWH)", [
                    ("", "Les tables FAIT_… : une copie des tables de la base, avec le cliché "
                         "du mois (MMYYYY). Seul le dernier cliché est gardé."),
                ], genre="donnee"),
                Bloc("Les DTM", [
                    ("", "Des tables calculées à partir des faits : elles portent les chiffres "
                         "des tableaux. Leurs requêtes actuelles contiennent des erreurs (fiche "
                         "08 de chaque domaine)."),
                ], genre="donnee"),
                Bloc("Oracle Analytics", [
                    ("", "L'outil de restitution : les tableaux CIPRES et les tableaux de bord "
                         "lisent les DTM."),
                ], genre="donnee"),
            ]),
            Partie("Les règles du jeu", colonnes=3, blocs=[
                Bloc("Pas de refonte du modèle", [
                    ("La règle", "On garde les tables DTM existantes. On corrige leurs "
                                 "requêtes, avec des changements compatibles, et on ne les "
                                 "met en production qu'après le visa du métier."),
                    ("Pourquoi", "Oracle Analytics et ses tableaux reposent sur ces tables : "
                                 "tout changer casserait l'existant."),
                ]),
                Bloc("Les calculs CIPRES côté BI", [
                    ("La règle", "La DSI fournit des données simples ; l'équipe BI applique "
                                 "les règles CIPRES (décision du 20/10/2026)."),
                    ("Pourquoi", "les règles ont besoin d'être expliquées, validées et "
                                 "corrigées : c'est notre travail."),
                ]),
                Bloc("Les DTM actuels ne font pas foi", [
                    ("La règle", "On n'analyse pas le contenu des tables DTM : il est jugé "
                                 "faux. On refait les règles depuis la base, et on relit le "
                                 "code des DTM seulement pour savoir quoi corriger."),
                    ("Si on te demande", "« Pourquoi pas les chiffres actuels ? » : parce que "
                                         "leurs requêtes se trompent (exemple : montants "
                                         "gonflés 3,8 fois en prestations)."),
                ]),
            ]),
            Partie("Les interlocuteurs", colonnes=4, blocs=[
                Bloc("La DSI", [("", "Elle connaît la base : le sens des statuts et des codes, "
                                     "les circuits de saisie. On lui pose les questions "
                                     "techniques.")], genre="notion"),
                Bloc("Les directions métier", [("", "Direction des prestations, DRH… Elles "
                                                    "valident les règles de calcul, avec les "
                                                    "fiches de validation.")],
                     genre="notion"),
                Bloc("Oracle Analytics", [("", "La personne en charge d'Oracle Analytics "
                                               "(Jules) doit accepter tout changement de sens "
                                               "d'une colonne DTM.")], genre="notion"),
                Bloc("La DPC", [("", "La Direction de la prospective et de la coopération "
                                     "publie l'annuaire statistique de la CNSS : des chiffres "
                                     "officiels pour vérifier les nôtres.")], genre="notion"),
            ]),
        ],
    ),
    Fiche(
        fichier="0 projet/02 la methode de travail.excalidraw",
        titre="La méthode de travail : comment on avance dans un domaine",
        sous_titre="Le cycle, les lots de requêtes, les livrables, et d'où vient chaque règle · "
                   "état au 30/10/2026",
        objectif="les étapes qu'on suit pour chaque domaine, comment se présente une requête de "
                 "profilage, ce qu'on produit, et comment une règle est justifiée.",
        parties=[
            Partie("Le cycle, domaine par domaine", colonnes=3, flux=True, blocs=[
                Bloc("1. Comprendre ce qui est demandé", [
                    ("", "Lire l'Annuaire CIPRES tableau par tableau, et les décisions de "
                         "l'atelier de Kaya 2017."),
                ], genre="etape"),
                Bloc("2. Relire les requêtes DTM actuelles", [
                    ("", "Lister les erreurs possibles : ce sont les hypothèses H1, H2… à "
                         "vérifier dans les données."),
                ], genre="etape"),
                Bloc("3. Interroger la base, par lots", [
                    ("", "Des requêtes en lecture seule, qui ne sortent que des comptages. Tu "
                         "les exécutes, je lis les résultats et j'en tire des constats."),
                ], genre="etape"),
                Bloc("4. Proposer des règles", [
                    ("", "Chaque règle dit en clair comment compter, avec sa preuve et sa "
                         "source ; puis une requête proposée qui l'applique."),
                ], genre="etape"),
                Bloc("5. Faire valider par le métier", [
                    ("", "Une fiche de validation par tableau : le métier coche D'accord, Pas "
                         "d'accord ou À discuter, et répond aux questions."),
                ], genre="etape"),
                Bloc("6. Vérifier, puis mettre en production", [
                    ("", "Exécuter les requêtes, comparer à des chiffres publiés (annuaire de "
                         "la CNSS), corriger les DTM après visa."),
                ], genre="etape"),
            ]),
            Partie("Les lots de requêtes", colonnes=3, blocs=[
                Bloc("Un lot, c'est quoi", [
                    ("", "Un fichier analyses/<domaine>/lotN_<sujet>.sql, qui répond à une "
                         "série de questions. Les requêtes sont numérotées (P01, P02… en "
                         "prestations, Q… en GRH), jamais deux fois le même numéro."),
                ]),
                Bloc("L'en-tête d'une requête", [
                    ("", "Question, Objectif, Lecture (ce que le résultat changera), Tableaux "
                         "touchés, puis Résultat et Constat, remplis après l'exécution."),
                    ("Pour toi", "pour comprendre une requête, lis son en-tête : tout y est "
                                 "dit en français."),
                ]),
                Bloc("Le va-et-vient", [
                    ("", "Tu exécutes le lot dans SQL Developer (F5) et tu me colles les "
                         "résultats. Je les reporte dans les en-têtes, j'en tire les "
                         "constats, et j'écris les questions suivantes."),
                ]),
                Bloc("Les précautions", [
                    ("", "Lecture seule ; jamais de nom, d'adresse, de téléphone ni de "
                         "compte bancaire ; des comptages seulement. Les listes longues vont "
                         "dans resultats/."),
                ]),
                Bloc("Les pièges d'Oracle", [
                    ("", "La base est antérieure à Oracle 12c : pas de FETCH FIRST, on limite "
                         "avec ROWNUM. Un nom de colonne fait 30 caractères au plus "
                         "(ORA-00972)."),
                ]),
                Bloc("Le suivi", [
                    ("", "Chaque dossier d'analyse a un README (lots, constats, règles, "
                         "questions) et un TODO (tâches, ordre, qui fait quoi)."),
                ]),
            ]),
            Partie("Ce qu'on produit", colonnes=4, blocs=[
                Bloc("Les requêtes proposées", [
                    ("", "La règle traduite en SQL, sur les faits du DWH, avec le cliché en "
                         "paramètre. Elles ne remplacent les DTM qu'après validation."),
                ], genre="donnee"),
                Bloc("Les fiches de validation", [
                    ("", "Pour le métier, dans Excalidraw : la maquette du tableau, des "
                         "exemples, chaque règle avec ses cases de réponse, les questions."),
                ], genre="donnee"),
                Bloc("Les documents pour la CNSS", [
                    ("", "Simples, sans SQL ni codes : constats, règles et questions, à "
                         "envoyer aux directions."),
                ], genre="donnee"),
                Bloc("Les schémas de travail", [
                    ("", "Les tables, le parcours d'un dossier, toutes les règles avec leurs "
                         "preuves : la référence détaillée."),
                ], genre="donnee"),
            ]),
            Partie("D'où vient une règle : les cinq sources", colonnes=5,
                   intro="Chaque règle cite sa source. Plus la source est haute, plus la "
                         "règle est solide.",
                   blocs=[
                Bloc("Annuaire", [("", "L'Annuaire CIPRES, avec la page. Ce que la CIPRES "
                                       "demande : on ne le discute pas.")], genre="notion"),
                Bloc("Kaya 2017", [("", "L'atelier où la CNSS a validé les tableaux en 2017. "
                                        "Une décision du métier, à reconfirmer.")],
                     genre="notion"),
                Bloc("Base", [("", "Un libellé lu dans la base (une liste de codes). Sûr pour "
                                   "le sens d'un code.")], genre="notion"),
                Bloc("Données", [("", "Ce que montrent nos requêtes (Pnn). Un fait, mais pas "
                                      "une décision.")], genre="notion"),
                Bloc("Proposition BI", [("", "Notre choix, cohérent avec les données, sans "
                                             "source écrite : à faire valider en premier.")],
                     genre="notion"),
            ]),
        ],
    ),
    Fiche(
        fichier="0 projet/03 les documents de reference.excalidraw",
        titre="Les documents de référence",
        sous_titre="Ce que dit chaque document, et quand t'en servir · état au 30/10/2026",
        objectif="les documents sur lesquels reposent nos règles, ce qu'on y trouve, et quand "
                 "les citer.",
        parties=[
            Partie("Les documents", colonnes=3, blocs=[
                Bloc("L'Annuaire statistique CIPRES", [
                    ("Où", "cnss_files/Annuaire_statistique_CIPRES_finalise V110920.pdf (68 "
                           "pages)."),
                    ("Ce qu'on y trouve", "la liste des 76 tableaux et leur utilité (p. 7 à "
                                          "19), leurs maquettes vides (p. 21 à 60), le "
                                          "glossaire (p. 61 à 66), par exemple la durée de "
                                          "liquidation : du dépôt à la notification des "
                                          "droits (p. 64)."),
                    ("Quand", "pour savoir ce qu'un tableau demande, et justifier une règle."),
                ], genre="donnee"),
                Bloc("L'atelier de Kaya 2017", [
                    ("Où", "cnss_files/Atelier de kaya.docx."),
                    ("Ce qu'on y trouve", "la validation par la CNSS des tableaux T1 à T76 et "
                                          "des indicateurs. Par exemple : la retraite "
                                          "anticipée a sa ligne en T25 ; T57 compte les "
                                          "assurés décédés déclarés ; le module des rejets "
                                          "n'est pas utilisé ; la durée part du dossier "
                                          "complet."),
                    ("Quand", "avant de poser une question au métier : elle a peut-être déjà "
                              "été tranchée en 2017."),
                ], genre="donnee"),
                Bloc("L'annuaire statistique de la CNSS 2024", [
                    ("Où", "cnss_files/Annuaire_2024_CNSS_BURKINA-FASO.pdf (n° 32)."),
                    ("Ce qu'on y trouve", "les chiffres publiés par la CNSS pour 2024 : "
                                          "pensionnés, cotisations, prestations familiales, "
                                          "risques professionnels."),
                    ("Quand", "pour vérifier nos chiffres : le 30/10/2026, notre base a "
                              "retrouvé son tableau 18 des pensionnés à 2 ou 3 % près."),
                ], genre="donnee"),
                Bloc("La documentation du DataMart", [
                    ("Où", "cnss_files/Documentation_DataMart_BI_CNSS.docx."),
                    ("Ce qu'on y trouve", "le modèle actuel des tables DTM."),
                    ("Quand", "pour savoir quelles tables et quelles colonnes on doit "
                              "garder."),
                ], genre="donnee"),
                Bloc("L'évaluation des ratios CIPRES 2025", [
                    ("Où", "cnss_files/Evaluation des Ratios CIPRES 2025.xlsx."),
                    ("Ce qu'on y trouve", "un tableur d'évaluation des ratios CIPRES."),
                    ("Quand", "pour les indicateurs et les normes."),
                ], genre="donnee"),
            ]),
            Partie("Bien citer une source", colonnes=3, blocs=[
                Bloc("Une page, pas un souvenir", [
                    ("", "Cite « Annuaire p. 64 » ou « Kaya 2017, tableau 1 », pas « la "
                         "CIPRES dit que »."),
                ], genre="reponse"),
                Bloc("Sans source, le dire", [
                    ("", "Si aucun document ne tranche, c'est une « Proposition BI » : le dire "
                         "franchement, et demander l'avis du métier."),
                ], genre="reponse"),
                Bloc("Les chiffres publiés ont une date", [
                    ("", "L'annuaire de la CNSS a été fait sur la base de l'époque : quelques "
                         "pourcents d'écart avec notre copie de 2026 sont normaux."),
                ], genre="reponse"),
            ]),
        ],
    ),
]

FICHES = [SOMMAIRE] + PROJET
