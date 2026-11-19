"""
Fiches 04, 05 et 08 du domaine de la gestion administrative (T58 à T65) : la démarche suivie du
21/10 au 17/11/2026, les faits établis dans les données, et les erreurs des requêtes DTM d'aujourd'hui.
"""

from fiches import Bloc, Fiche, Partie
from grh_commun import D, ETAT_AU, constat, erreur, lot

# =============================================================================================
# 04 La démarche
# =============================================================================================

F04 = Fiche(
    fichier=D + "04 la demarche.excalidraw",
    titre="La démarche : six étapes, de la table des mouvements aux contrôles du 17 novembre",
    sous_titre="Fiche 04 du domaine de la gestion administrative (T58 à T65) · du 21/10 au "
               "17/11/2026 · " + ETAT_AU,
    objectif="dans quel ordre on a travaillé sur le personnel, la question que posait chaque "
             "groupe de lots, ce qu'il a montré, ce qu'il a changé, et la manière de travailler "
             "qu'on applique à chaque fois.",
    parties=[
        Partie("Le chemin parcouru", colonnes=3, flux=True,
               intro="Vingt-huit lots de requêtes, 159 requêtes numérotées Q01 à Q159, lancées "
                     "sur la base source. Chaque étape répond aux questions laissées par la "
                     "précédente ; les flèches donnent l'ordre. Deux lots ont été abandonnés en "
                     "route : le 16 (le code de domaine de la situation, remplacé par le lot 23) "
                     "et le 22 (le grand livre de la copie, que la CNSS a dit non valide).",
               blocs=[
            lot("Lots 1 à 6", "La table des mouvements",
                "La table des mouvements de l'application contient-elle les recrutements et les "
                "départs, comme le suppose la requête de T60 ?",
                "39 requêtes (Q01 à Q39) le 21/10/2026 : contenu de GRH_MOUVEMENT (la table des "
                "mouvements), sens des codes, décisions du personnel, recoupements, et premier "
                "calcul de l'effectif avec la règle du DTM.",
                "GRH_MOUVEMENT ne contient presque que des mouvements internes : 6 127 lignes, "
                "dont 4 040 validées, et aucune ligne de recrutement, de retraite ni de décès. "
                "Les entrées se lisent sur la fiche de l'agent (GRH_PERSONNE, la date "
                "d'embauche), les sorties dans les décisions. Des codes trompent : REC veut dire "
                "reclassement, RET rétrogradation, DIS mise à disposition. La règle d'effectif "
                "du DTM compte 2 845 agents au 31/12/2025, pour 879 agents réellement actifs.",
                "T60 ne peut pas venir de cette table : il faudra la réécrire. L'effectif se "
                "recalcule depuis la fiche de l'agent et les décisions.",
                "21/10/2026 · lot1 à lot6 · Q01 à Q39"),
            lot("Lots 7 à 9", "L'effectif et ses zones d'ombre",
                "Peut-on dater les départs et reconstituer l'effectif des années passées ? D'où "
                "viendront la formation et les chiffres financiers ?",
                "30 requêtes (Q40 à Q69) le 21/10/2026 : dates de sortie, bornes des tranches "
                "d'âge et d'ancienneté, formation, paie, pistes comptables.",
                "817 départs n'ont aucune date : l'effectif de 2019 est quelque part entre 621 "
                "et 1 613 agents. Les tranches du DTM perdent 704 agents sur 2 822, dont tous "
                "les recrutés de l'année : 2 822 moins 704 donne exactement les 2 118 publiés. "
                "Aucune formation n'a le statut T, le seul que le DTM garde. La paie du "
                "personnel n'est pas dans la copie : ses 22 vues sont invalides.",
                "Quatre requêtes DTM à corriger. T63 et T64 ne peuvent pas venir de la GRH : "
                "ils passeront par la comptabilité.",
                "21/10/2026 · lot7, lot8, lot9 · Q40 à Q69"),
            lot("Lots 10 à 15", "Les requêtes DTM corrigées, puis testées",
                "Les corrections écrites le 22/10 donnent-elles les bons chiffres, avant tout "
                "chargement ?",
                "Les requêtes corrigées enveloppées dans un comptage (Q70 à Q74), un diagnostic "
                "des résultats vides (Q75 à Q79), un nouveau test avec le cliché écrit en clair "
                "(Q80 à Q84), puis les temporaires et l'axe « type d'agent » (Q85 à Q94).",
                "Les premiers essais sortent vides : c'est la valeur du cliché saisie dans SQL "
                "Developer, pas la requête. Avec le cliché écrit en clair, l'effectif donne 885 "
                "agents en 2025 et le taux d'encadrement 41,7 %, comme attendu. L'entrepôt ne "
                "garde qu'un seul cliché (102026). Le champ BUDGET d'une formation contient une "
                "année budgétaire, pas un montant.",
                "Les quatre requêtes corrigées sont validées (commit 1b44a0c) ; le budget de "
                "formation reste vide ; T60 attend les réponses de la DRH.",
                "du 22 au 25/10/2026 · lot10 à lot15 · Q70 à Q94"),
            lot("Lots 17 à 20", "La comparaison avec l'annuaire de la CNSS 2024",
                "Nos règles donnent-elles les chiffres que la CNSS publie elle-même (partie IX, "
                "tableaux 119 à 126) ?",
                "21 requêtes (Q97 à Q117) le 01/11/2026 : effectif par catégorie, âge, sexe, "
                "ancienneté, mouvements, formés et taux d'encadrement de 2020 à 2024, puis une "
                "requête par écart à expliquer.",
                "742 permanents au 31/12/2024 pour 758 publiés. La catégorie de la CNSS est "
                "celle de la grille de salaire (GRH_SITUATION.CATEGORIE), pas le niveau de "
                "qualification : avec elle, T58 et T65 retombent. Les 46 contractuels publiés "
                "sont les temporaires engagés dans l'année. T62 ne retombe pas. Les 84 agents "
                "engagés en 2019 expliquent l'écart des années passées.",
                "Les règles G1 à G11 et les sept questions QG1 à QG7, chacune avec une valeur "
                "par défaut.",
                "01/11/2026 · lot17 à lot20 · Q97 à Q117"),
            lot("Lots 21 à 25", "Les unités, les décisions et les situations",
                "Où lire le domaine d'activité, la promotion après une formation, et l'absence "
                "d'un agent ?",
                "25 requêtes (Q118 à Q142) du 02 au 07/11/2026 : classement avant et après une "
                "formation, unités administratives, décisions face aux situations, agents partis "
                "puis revenus.",
                "Le code de domaine de la situation est incohérent dans 66 unités sur 139 : le "
                "domaine se lira dans l'unité de l'agent. La position de la situation n'est pas "
                "datée : l'application l'écrase sur la situation en cours. Les absences se lisent "
                "donc dans les décisions 07 (mise en disponibilité) et 18 (remise en activité). "
                "Aucun permanent n'est parti puis revenu.",
                "Les règles G12 (le domaine vient de l'unité) et G13 (les inactifs se lisent dans "
                "les décisions) ; le fichier unité vers domaine, à remplir par la CNSS.",
                "du 02 au 07/11/2026 · lot21 à lot25 · Q118 à Q142"),
            lot("Lots 26 à 28", "Les contrôles des requêtes proposées",
                "Les trois requêtes proposées, jamais lancées jusque-là, donnent-elles ce qu'on "
                "attend d'elles ?",
                "Trois fichiers de contrôle (effectif, T60, T62) lancés le 17/11/2026 sur la "
                "base source, puis 17 comptages (Q143 à Q159) pour les points restés sans "
                "chiffre avant la réunion.",
                "Les permanents sont justes : 741 au 31/12/2024, T60 équilibré chaque année, T61 "
                "à 4 agents près de l'annuaire. Les temporaires ne le sont pas : 299 pour 46 "
                "publiés. T62 donne 407 formés pour 480. Trois erreurs sont de notre côté : 12 "
                "agents en disponibilité effacés, 5 vieilles fiches comptées à tort, les "
                "temporaires sans fin de contrat.",
                "Trois questions fermées par les chiffres, trois corrections à faire dans nos "
                "requêtes, et tous les chiffres de la réunion.",
                "17/11/2026 · lot26 à lot28 · Q143 à Q159 · controles/resultats_2026-11-17.md"),
        ]),
        Partie("La méthode, à chaque étape", colonnes=3,
               intro="Ce qui ne change jamais, quel que soit le tableau.",
               blocs=[
            Bloc("On lit la base source, pas l'entrepôt", [
                ("Ce qu'on fait", "Toutes les requêtes de profilage lisent USER_GRH, la copie de "
                                  "la base de l'application du personnel. Les contrôles du 17/11 "
                                  "sont des copies des requêtes proposées, réécrites sur ces "
                                  "mêmes tables."),
                ("Pourquoi", "Le contenu des tables DTM est jugé faux tant qu'il n'est pas "
                             "refait, et l'entrepôt n'est pas encore chargé pour la GRH. Un "
                             "chiffre tiré du DTM ne prouverait rien."),
            ], genre="notion", pied="README d'analyses/grh ; controles/construire.py"),
            Bloc("Chaque requête est historisée", [
                ("Ce qu'on fait", "Un fichier par lot, numéroté dans l'ordre d'exécution. Les "
                                  "requêtes portent un numéro, Q01 à Q159, jamais réutilisé. "
                                  "L'en-tête de chacune donne son objectif, les tableaux visés, "
                                  "le résultat obtenu et le constat."),
                ("Pourquoi", "N'importe qui peut relancer la requête et retrouver le chiffre. "
                             "Chaque règle et chaque fiche renvoie au numéro qui la prouve."),
            ], genre="notion", pied="analyses/grh, conventions"),
            Bloc("On confronte tout à l'annuaire de la CNSS", [
                ("Ce qu'on fait", "Chaque règle est mesurée contre l'annuaire statistique CNSS "
                                  "2024 (partie IX, tableaux 119 à 126), qui publie l'effectif, "
                                  "les mouvements, l'ancienneté, les formés et le taux "
                                  "d'encadrement."),
                ("Pourquoi", "C'est le seul chiffre officiel de la CNSS sur son propre "
                             "personnel. Une règle qui retombe dessus est défendable en réunion ; "
                             "un écart devient une question précise."),
            ], genre="notion", pied="lot17 à lot20 ; controles/resultats_2026-11-17.md"),
            Bloc("Une question sans réponse ne bloque pas", [
                ("Ce qu'on fait", "Toute décision en attente devient un réglage écrit en tête de "
                                  "la requête, dans la partie « parametres » : la disponibilité "
                                  "dans ou hors de l'effectif, la fin d'un contrat inconnue, le "
                                  "regroupement des catégories, la périodicité."),
                ("Pourquoi", "La réponse de la CNSS change une valeur, pas la requête. On "
                             "continue à avancer sans attendre, et la question reste visible "
                             "dans les points à valider."),
            ], genre="notion", pied="regles_effectif.md et regles_t60.md, partie « réglages »"),
            Bloc("L'ordre des sources", [
                ("Ce qu'on fait", "Une décision prise en réunion avec la CNSS passe avant tout. "
                                  "Viennent ensuite le recueil des besoins v7 (janvier 2026), "
                                  "l'atelier de Kaya (2017), puis l'Annuaire CIPRES."),
                ("Pourquoi", "Deux documents se contredisent parfois. L'annuaire CNSS 2024, lui, "
                             "ne sert jamais de règle : il sert à contrôler les chiffres."),
            ], genre="notion", pied="regles_effectif.md, « ordre des sources »"),
            Bloc("On ne change pas la structure des tables", [
                ("Ce qu'on fait", "Chaque requête proposée garde les colonnes, le grain et le "
                                  "paramètre de la table DTM qu'elle remplace ; les colonnes "
                                  "nouvelles s'ajoutent à la fin, aucune n'est retirée."),
                ("Pourquoi", "Les tableaux déjà construits dans OBIEE continuent de "
                             "fonctionner : on change les règles de calcul, pas le modèle."),
            ], genre="notion", pied="regles_effectif.md et regles_t60.md, « la structure ne "
                                    "change pas »"),
        ]),
        Partie("Ce qu'on a produit", colonnes=4, blocs=[
            Bloc("Les requêtes proposées", [
                ("", "t58_t65_effectif_requete_proposee.sql (T58, T59, T61, T65), "
                     "t60_requete_proposee.sql, t62_requete_proposee.sql et "
                     "t63_t64_requete_proposee.sql, avec son fichier de déploiement. Les trois "
                     "premières ont été contrôlées sur la base le 17/11/2026 ; la quatrième "
                     "attend la comptabilité."),
            ], genre="suite"),
            Bloc("Les règles et les questions", [
                ("", "regles_effectif.md (G1 à G7, G12, G13) et regles_t60.md (G8, G9, G14 à "
                     "G17), avec leurs réglages et leurs points d'attention ; les questions QG1 "
                     "à QG7 ; points_a_valider_v2.md, les 64 points, dont 45 encore ouverts et "
                     "19 déjà répondus."),
            ], genre="suite"),
            Bloc("Les fiches et les schémas", [
                ("", "Les fiches de validation T58 (pilote), T60 et T62 pour la CNSS ; les huit "
                     "fiches de règles par tableau, T58 à T65 ; la fiche de lecture GRH ; le "
                     "schéma des règles regles_grh et les trois schémas de T60 (les cas, les cas "
                     "en SQL, la requête pas à pas)."),
            ], genre="suite"),
            Bloc("Les contrôles et le suivi", [
                ("", "Le dossier controles : construire.py et les trois fichiers prêts à "
                     "lancer, avec resultats_2026-11-17.md. Le README et le TODO "
                     "d'analyses/grh. Le fichier correspondance_unite_domaine.xlsx (432 unités) "
                     "que la CNSS doit remplir pour T59."),
            ], genre="suite"),
        ]),
    ],
)

# =============================================================================================
# 05 Les découvertes
# =============================================================================================

F05 = Fiche(
    fichier=D + "05 les decouvertes.excalidraw",
    titre="Les découvertes : ce que la base du personnel nous a appris",
    sous_titre="Fiche 05 du domaine de la gestion administrative (T58 à T65) · les faits "
               "établis, avec le chiffre qui les prouve · " + ETAT_AU,
    objectif="les faits que les requêtes ont établis, avec leur chiffre et la raison pour "
             "laquelle ils comptent : ce que la base sait et ne sait pas, les agents qu'on ne "
             "retrouve pas, ce que l'annuaire de la CNSS a tranché, la formation et les "
             "finances. Ce sont eux qui fondent les règles G1 à G17.",
    parties=[
        Partie("Ce que la base sait, et ce qu'elle ne sait pas", colonnes=3,
               intro="La pastille donne la source du fait : G pour une règle, Q pour la requête "
                     "qui l'a mesuré.",
               blocs=[
            constat("Q03", "La table des mouvements ne bouge personne",
                    "GRH_MOUVEMENT, la table des mouvements de l'application, ne contient que "
                    "des changements de poste : affectations, mutations, nominations. Ni "
                    "recrutement, ni retraite, ni décès, ni démission.",
                    "6 127 lignes, dont 4 040 validées : AFF 2 792, NOM 812, MUT 323, REM 63, "
                    "DIS 30, DET 7. Aucune ligne RECRU, RET, DEC, LIC ou DEM.",
                    "C'est pourtant la seule source de la requête T60 d'aujourd'hui : toute la "
                    "ligne des entrées et des sorties est à réécrire.",
                    "Preuve : lot 1, Q03 ; confirmé par la CNSS en réunion le 03/11/2026"),
            constat("Q15", "Les entrées et les sorties sont dans les décisions",
                    "L'entrée d'un agent est la date d'embauche de sa fiche (GRH_PERSONNE). Sa "
                    "sortie est une décision : 02 l'admission à la retraite, 34 la liquidation "
                    "des droits après un décès, 35 l'indemnité de fin de contrat, 36 la "
                    "démission, 52 la rupture du contrat, 54 le licenciement.",
                    "29 115 décisions, dont 838 retraites, 1 504 engagements de temporaires "
                    "(50), 310 engagements de permanents (51), 397 fins de contrat, 213 "
                    "liquidations de droits.",
                    "C'est la base des règles G2 (le permanent présent), G7 (le temporaire sous "
                    "contrat), G8 et G9 (les entrées et les sorties de T60).",
                    "Preuve : lot 3, Q15 ; lot 4, Q17 ; lot 5, Q20"),
            constat("Q08", "Trois codes qui trompent",
                    "Dans les situations, REC veut dire reclassement et non recrutement, RET "
                    "rétrogradation et non retraite, DIS mise à disposition et non "
                    "disponibilité.",
                    "la requête T60 d'aujourd'hui convertit pourtant DIS en DISP, la "
                    "disponibilité.",
                    "Un code mal lu range des agents dans la mauvaise ligne du tableau. Chaque "
                    "code est vérifié dans la liste de l'application avant d'être utilisé.",
                    "Preuve : lot 2, Q08"),
            constat("Q137", "La position de l'agent n'est pas datée",
                    "La position de la situation (STATUT_EMPLOYE : en activité, détaché, en "
                    "disponibilité, retraité, radié, décédé) est écrasée sur la situation en "
                    "cours au lieu d'ouvrir une nouvelle situation. Elle dit l'état "
                    "d'aujourd'hui, jamais celui d'une date passée.",
                    "au 31/12/2024, les 6 permanents en disponibilité d'après les décisions "
                    "étaient tous en position « activité normale » ; 24 permanents présents ce "
                    "jour-là sont aujourd'hui en position « retraité ».",
                    "On ne peut pas s'en servir pour une photo au 31/12 : les absences se lisent "
                    "dans les décisions 07 et 18 (règle G13).",
                    "Preuve : lot 24, Q136 à Q139"),
            constat("Q42", "Les décès ne sont plus datés depuis 2010",
                    "La date de décès de la fiche n'est plus remplie depuis 2010. Un décès ne se "
                    "voit plus que par la décision 34, la liquidation des droits des ayants "
                    "droit, prise longtemps après.",
                    "59 dates de décès pour 435 agents décédés ; la liquidation vient en moyenne "
                    "8 mois et demi après le décès. Depuis 2019, aucun décès n'est daté par la "
                    "fiche.",
                    "La ligne « décès » de T60 glisse d'une année sur l'autre : 19 sorties pour "
                    "17 décès publiés de 2020 à 2024, et aucun décès encore visible pour 2025.",
                    "Preuve : lot 7, Q42 ; lot 26, Q147"),
            constat("Q35", "La base est une copie figée",
                    "La base consultée est une copie de l'application, arrêtée au 18/02/2026. "
                    "Rien n'y a été écrit après cette date.",
                    "dernières écritures : 02/02/2026 pour les mouvements, 18/02/2026 pour les "
                    "fiches des agents et les décisions.",
                    "Les années jusqu'à 2025 sont exploitables ; 2026 ne l'est pas. Un départ "
                    "saisi après février 2026 manque encore.",
                    "Preuve : lot 6, Q35"),
        ]),
        Partie("Les agents qu'on ne retrouve pas", colonnes=3,
               intro="À chaque fois, la base ne dit pas qu'un agent est parti, ou ne dit pas "
                     "quand. C'est la première cause d'écart avec l'annuaire de la CNSS.",
               blocs=[
            constat("Q44", "817 personnes parties sans aucune date",
                    "Leur fiche est inactive, mais elles n'ont ni décision de départ, ni date de "
                    "décès, ni indemnité de fin de contrat : rien ne dit quand elles sont "
                    "parties.",
                    "817 agents sur 2 234 fiches inactives, soit 37 % des départs : surtout des "
                    "radiés (272), des décédés (200) et des agents restés en position "
                    "« activité » (201).",
                    "Elles ne sont comptées dans aucune année. L'effectif des années passées "
                    "n'est pas reconstituable : entre 621 et 1 613 agents en 2019.",
                    "Preuve : lot 7, Q44, Q45 ; point 25 de la v2"),
            constat("G11", "Les 84 agents engagés en 2019",
                    "84 personnes engagées comme permanents en août et septembre 2019 (décision "
                    "51), en service depuis 2018. Toutes ont aujourd'hui une fiche inactive, "
                    "sans aucune décision de départ, et aucune n'a de fiche plus récente.",
                    "la base ne les compte présentes à aucune date. L'annuaire les fait entrer "
                    "en 2019 en « autres entrées » et sortir en 2024 en « autres sorties ».",
                    "C'est l'essentiel de l'écart des années 2019 à 2023 : 670 permanents fin "
                    "2023 dans la base, pour 767 publiés. Sans effet à partir de 2025.",
                    "Preuve : lot 18, Q105 ; lot 27, Q154, Q155 ; point 2 de la v2"),
            constat("Q153", "Les temporaires sans fin de contrat",
                    "La base ne garde pas la fin prévue d'un contrat. Sans départ saisi et sans "
                    "contrat suivant, la règle laisse le temporaire compté pour toujours, et le "
                    "stock grossit chaque année.",
                    "299 temporaires au 31/12/2024 pour 46 contractuels publiés ; 237 fin 2020, "
                    "338 fin 2025. En ne gardant que les fiches non inactives : 47 fin 2024.",
                    "C'est le premier point à trancher avec la DRH. Le réglage "
                    "fin_contrat_inconnue = FICHE est déjà prévu dans la requête et donne 47.",
                    "Preuve : lot 27, Q153 ; point 20 de la v2"),
            constat("Q159", "12 agents en disponibilité effacés par notre règle",
                    "La fiche d'un agent mis en disponibilité passe à « inactif ». Comme elle "
                    "n'a aucun départ daté, notre règle ne le compte à aucune date, même pendant "
                    "les années où il travaillait. « Inactif » ne veut donc pas dire « parti ».",
                    "12 agents mis en disponibilité de 2021 à 2025 ne sont jamais revenus : il "
                    "manque 12 permanents fin 2020, 10 fin 2021, 9 fin 2022, 6 fin 2023 et 3 fin "
                    "2024.",
                    "C'est une erreur de notre côté, à corriger : la mise en disponibilité sans "
                    "retour servira de date de sortie. T60 montrerait alors 2 à 3 disponibilités "
                    "de plus par an, au niveau de l'annuaire.",
                    "Preuve : lot 28, Q159 ; points 25, 49 et 64 de la v2"),
            constat("Q158", "5 vieilles fiches sans état comptées à tort",
                    "Une fiche sans état (ni active, ni inactive) est comptée comme encore là, "
                    "parce que c'est le cas d'une embauche récente. Mais quelques fiches "
                    "anciennes n'ont jamais été fermées.",
                    "5 permanents de 69 à 80 ans sont comptés présents au 31/12/2024, sans "
                    "aucune retraite saisie ; l'annuaire ne publie aucun agent de 60 ans et "
                    "plus. Sans eux : 736 permanents fin 2024.",
                    "Deuxième erreur de notre côté : une fiche sans état ne devra compter que si "
                    "l'entrée est récente.",
                    "Preuve : lot 27, Q158 ; point 45 de la v2"),
            constat("Q157", "Des agents sans situation, et des temporaires sans engagement",
                    "Quelques agents n'ont aucune situation : ils n'ont donc ni nature d'emploi "
                    "ni catégorie, et n'apparaissent dans aucun des deux tableaux, celui des "
                    "permanents comme celui des temporaires.",
                    "4 agents comptés sans nature à chaque 31/12, dont 3 fiches anciennes sans "
                    "état. À côté, 230 temporaires n'ont aucune décision d'engagement, dont 227 "
                    "fiches inactives entrées avant 2015.",
                    "Les volumes sont petits et anciens, mais ils expliquent les lignes « sans "
                    "catégorie » des contrôles.",
                    "Preuve : lot 26, Q151 ; lot 27, Q157"),
        ]),
        Partie("Ce que l'annuaire de la CNSS 2024 nous a appris", colonnes=3,
               intro="L'annuaire statistique de la CNSS publie son propre personnel (partie IX, "
                     "tableaux 119 à 126). Les chiffres ci-dessous sont ceux des contrôles "
                     "lancés le 17/11/2026 sur la base source.",
               blocs=[
            constat("G1", "La base est bien celle de la CNSS",
                    "Avec les règles G1 et G2, le nombre de permanents au 31/12/2024 retombe "
                    "presque sur le chiffre publié, et la case des employés est exacte.",
                    "741 permanents pour 758 publiés ; employés 133 hommes et 44 femmes, "
                    "exactement comme l'annuaire ; agents de maîtrise 206 pour 208.",
                    "Nos chiffres sont défendables devant la CNSS : les écarts restants sont "
                    "nommés un par un, pas subis.",
                    "Contrôle 1, C1, du 17/11/2026 ; annuaire CNSS 2024, tableau 119"),
            constat("G3", "La catégorie de la grille, pas la qualification",
                    "L'annuaire range les agents par la catégorie de la grille de salaire "
                    "(GRH_SITUATION.CATEGORIE : 01 à 06 et HC), et non par le niveau de "
                    "qualification, qui est un autre champ de la base.",
                    "avec la catégorie, chaque case de T58 retombe à 4 agents près ; le tableau "
                    "126 de l'annuaire (1re A à 6e B, plus HC) redonne le tableau 119, HC rangé "
                    "en cadres supérieurs.",
                    "C'est la règle G3 : 01 et 02 employés, 03 agents de maîtrise, 04 et 05 "
                    "cadres moyens, 06 et HC cadres supérieurs. La requête du taux "
                    "d'encadrement, elle, lit encore la qualification.",
                    "Preuve : lot 18, Q103 ; lot 19, Q110, Q111"),
            constat("G6", "Le taux d'encadrement retombe à un point près",
                    "Les cadres (catégories 04, 05, 06 et HC) rapportés aux permanents présents "
                    "au 31/12 donnent le taux publié par la CNSS.",
                    "48,3 % au 31/12/2024 pour 49,2 % publiés ; environ 42 % chaque année de "
                    "2020 à 2023, contre environ 41 % publiés.",
                    "T65 est tenu. La norme CIPRES est de 10 % au plus : la CNSS est très "
                    "au-dessus, et c'est un fait, pas une erreur de calcul.",
                    "Contrôle 1, C2, du 17/11/2026 ; lot 19, Q112"),
            constat("G14", "T60 est équilibré chaque année",
                    "Avec les deux photos du 31/12 (règle G14), effectif de début plus entrées "
                    "moins sorties donne toujours l'effectif de fin, et la fin d'une année est "
                    "le début de la suivante.",
                    "2024 : 670 au début, 101 recrutements, 0 autre entrée, 24 retraites, 5 "
                    "décès, 1 autre sortie, 741 à la fin. L'annuaire publie 104 recrutements et "
                    "23 retraites.",
                    "Le tableau se tient tout seul : plus besoin de rapprocher une table "
                    "d'entrées et de sorties d'une table d'effectif.",
                    "Contrôle 2, C1, du 17/11/2026 ; annuaire CNSS 2024, tableau 121"),
            constat("G5", "L'ancienneté tombe juste, sauf tout en haut",
                    "L'ancienneté compte les années pleines depuis la prise de service, à défaut "
                    "l'embauche, moins les périodes d'absence.",
                    "chaque tranche de T61 est à 4 agents près de l'annuaire au 31/12/2024, sauf "
                    "« 40 ans et plus » : 6 agents dans la base pour 1 publié.",
                    "Cinq des six agents de 40 ans et plus sont les vieilles fiches sans état : "
                    "une fois celles-ci écartées, la tranche tombe à 1, comme l'annuaire.",
                    "Contrôle 1, C3, du 17/11/2026 ; annuaire CNSS 2024, tableau 122"),
            constat("QG6", "La disponibilité fait sortir de l'effectif",
                    "Le tableau 121 de l'annuaire range la mise en disponibilité dans les "
                    "sorties et la remise en activité dans les entrées : un agent en "
                    "disponibilité ne fait donc pas partie de l'effectif publié.",
                    "son égalité début plus entrées moins sorties égale fin tient chaque année "
                    "de 2015 à 2024 ; disponibilités moins remises, cumulées, donnent +6 fin "
                    "2024, l'ordre de grandeur des 6 agents en disponibilité de la base.",
                    "La question QG6 a été tranchée le 08/11/2026 : réglage disponibilite = "
                    "HORS_EFFECTIF, la même valeur dans la requête de l'effectif et dans celle "
                    "de T60.",
                    "regles_t60.md, point 3 ; réunion du 03/11/2026 (M11)"),
        ]),
        Partie("La formation, les unités et les finances", colonnes=3, blocs=[
            constat("Q50", "Toutes les formations ont le statut N",
                    "Le champ « statut » d'une formation vaut N pour toutes les formations "
                    "saisies, sans exception. Or la requête du DTM ne gardait que celles au "
                    "statut T.",
                    "30 à 45 formations et 435 à 886 inscriptions par an depuis 2019, et aucune "
                    "au statut T : la table de formation du DTM ne publie aucune ligne depuis "
                    "2019.",
                    "T62 et T64 étaient entièrement vides pour cette seule raison. Le filtre a "
                    "été retiré le 22/10/2026.",
                    "Preuve : lot 8, Q50, Q56 ; lot 9, Q69"),
            constat("Q150", "Les 480 formés de l'annuaire qu'on ne retrouve pas",
                    "Quelle que soit la date retenue (début, fin, ou formation en cours dans "
                    "l'année) et quel que soit le type d'agent, la base ne donne jamais les 480 "
                    "formés publiés pour 2024.",
                    "407 permanents formés avec la date de début, 409 avec la date de fin, pour "
                    "522 inscriptions. Une seule personne de plus est un agent de la fonction "
                    "publique ; aucun temporaire, aucun externe.",
                    "L'annuaire vient sans doute d'un autre registre, tenu à côté de "
                    "l'application : c'est la question du point 4 à la DRH.",
                    "Preuve : lot 26, Q149, Q150 ; contrôle 3 du 17/11/2026"),
            constat("Q79", "Le budget d'une formation est une année",
                    "Le champ BUDGET d'une formation ne contient pas un montant : il contient "
                    "l'année budgétaire sur laquelle la formation est imputée.",
                    "220 budgets saisis, tous en chiffres, et seulement depuis 2020.",
                    "Le budget de formation de T64 ne peut pas venir de la GRH : la requête le "
                    "laisse vide, et le montant viendra de la comptabilité.",
                    "Preuve : lot 11, Q78, Q79 ; lot 8, Q53"),
            constat("Q65", "Les durées de formation mélangent les unités",
                    "La durée d'une inscription est saisie avec une unité, que la requête du DTM "
                    "ignore quand elle additionne des jours.",
                    "9 357 inscriptions en jours, 139 en mois, 11 en années, 1 en KG et 1 en "
                    "CFA ; la durée la plus basse est −17.",
                    "Les durées « en mois » et « en années » sont en réalité des jours mal "
                    "saisis. Le total reste approximatif tant que l'unité n'est pas tenue.",
                    "Preuve : lot 9, Q65"),
            constat("Q129", "Le code de domaine de la situation est incohérent",
                    "Le code de domaine d'activité porté par la situation de l'agent diffère "
                    "d'un agent à l'autre à l'intérieur d'une même unité administrative.",
                    "66 unités sur 139 sont dans ce cas, et elles portent 77 % des agents.",
                    "T59 ne peut pas se faire avec ce code. Le domaine viendra de l'unité, par "
                    "une correspondance que la CNSS remplit (règle G12, 432 unités).",
                    "Preuve : lot 23, Q129 ; réunion du 03/11/2026"),
            constat("Q55", "La paie et la comptabilité ne sont pas exploitables",
                    "La paie du personnel n'est pas dans la copie de la base : ses 22 vues "
                    "existent mais sont invalides. Et la CNSS a dit le 07/11/2026 que les "
                    "données comptables de la copie ne sont pas valides.",
                    "aucune table de paie dans la base du personnel ; le lot 22, qui devait "
                    "lire le grand livre, a été abandonné.",
                    "T63 et T64 n'ont aucun chiffre aujourd'hui. Tout passera par une vue "
                    "commune, V_BI_BALANCE_COMPTABLE, que la DSI doit créer et alimenter depuis "
                    "la comptabilité Oracle.",
                    "Preuve : lot 8, Q55 ; lot 9, Q66 ; décision CNSS du 07/11/2026"),
            constat("Q75", "L'entrepôt n'est pas encore chargé",
                    "Les requêtes proposées sont écrites pour l'entrepôt (DWH et DTM), mais tout "
                    "ce qu'on lance aujourd'hui lit la base source. Le chargement des faits "
                    "archive l'entrepôt puis vide les tables : il n'y reste qu'un seul cliché.",
                    "un seul cliché dans les faits du personnel, 102026, avec 3 180 fiches "
                    "d'agents et 10 906 situations.",
                    "Les trois contrôles du 17/11 sont des copies des requêtes proposées, "
                    "réécrites sur les tables de la source : mêmes règles, mêmes colonnes, sans "
                    "condition de cliché.",
                    "Preuve : lot 11, Q75 ; controles/construire.py"),
        ]),
    ],
)

# =============================================================================================
# 08 Les erreurs des DTM actuels
# =============================================================================================

F08 = Fiche(
    fichier=D + "08 les erreurs des dtm actuels.excalidraw",
    titre="Les erreurs des requêtes DTM actuelles",
    sous_titre="Fiche 08 du domaine de la gestion administrative (T58 à T65) · ce que font les "
               "cinq requêtes d'aujourd'hui, et ce qui est faux · " + ETAT_AU,
    objectif="quelles requêtes alimentent les tables du domaine, ce que chacune fait de faux et "
             "ce que les données en ont dit, ce qui a été corrigé le 22/10/2026 et ce qui reste "
             "à remplacer par les requêtes proposées.",
    parties=[
        Partie("Les cinq requêtes du domaine", colonnes=3,
               intro="Elles sont dans domaines/grh/dtm/sql. Chacune remplit une table DTM, lue "
                     "ensuite par OBIEE. Le paramètre est toujours le même : le cliché, au "
                     "format MMAAAA, saisi sans guillemets.",
               blocs=[
            Bloc("DTM_GRH_EFFECTIF", [("", "T58, T59 et T61 : l'effectif au 31/12 par "
                                           "qualification, tranche d'âge, tranche d'ancienneté, "
                                           "sexe, domaine, unité et fonction. Requête "
                                           "grh_effectif.sql.")], genre="donnee"),
            Bloc("DTM_GRH_MOUVEMENT", [("", "T60 : les mouvements du personnel par exercice, "
                                            "type, nature, sens, qualification et sexe. Requête "
                                            "grh_mouvement.sql, la seule qui n'a pas été "
                                            "corrigée.")], genre="donnee"),
            Bloc("DTM_GRH_RATIO", [("", "T65 : le taux d'encadrement par exercice, calculé à "
                                        "part de l'effectif. Requête grh_ratio.sql.")],
                 genre="donnee"),
            Bloc("DTM_GRH_FORMATION", [("", "T62 et la partie formation de T64 : agents formés "
                                            "et coûts, par exercice, catégorie de formation, "
                                            "qualification et sexe. Requête "
                                            "grh_formation.sql.")], genre="donnee"),
            Bloc("DTM_INDICATEURS_FINANCES_GRH", [("", "Les taux : encadrement (T65), frais de "
                                                       "personnel (T63), formation (T64), "
                                                       "exécution budgétaire. Requête "
                                                       "indicateurs_finances_grh.sql.")],
                 genre="donnee"),
        ]),
        Partie("Ce qui est faux, et ce que les données en ont dit", colonnes=3,
               intro="La pastille est le numéro de la requête de profilage qui a tranché, ou le "
                     "tableau concerné quand la preuve vient de l'Annuaire CIPRES. "
                     "« Confirmée » : l'erreur existe et on l'a mesurée. « En partie » : elle "
                     "existe, mais son effet est plus petit ou pas encore mesuré. "
                     "« Tranchée » : c'est un choix de conception, décidé depuis.",
               blocs=[
            erreur("Q03", "T60 est lu dans la table des mouvements", "confirmée",
                   "Elle lit la table des mouvements de l'application (GRH_MOUVEMENT), ne garde "
                   "que les lignes validées, et convertit DIS en DISP (disponibilité) et DET en "
                   "DETAC (détachement).",
                   "cette table n'a que des mouvements internes : 6 127 lignes, aucune de "
                   "recrutement, de retraite, de décès, de démission ni de licenciement. Et DIS "
                   "veut dire mise à disposition, pas disponibilité.",
                   "T60 n'a aucune de ses lignes principales : ni effectif de début ou de fin, "
                   "ni recrutements, ni retraites, ni décès. La requête est à réécrire "
                   "entièrement.",
                   "Preuve : lot 1, Q01 à Q03 ; réunion du 03/11/2026 (M06)"),
            erreur("Q23", "L'effectif compte la fiche d'aujourd'hui", "confirmée",
                   "Avant le 22/10/2026, elle comptait tout agent ayant une situation en "
                   "vigueur, sans date d'arrêté : un agent parti y restait, car sa situation "
                   "n'est jamais clôturée à son départ.",
                   "2 845 agents comptés au 31/12/2025, pour 879 actifs réels, dont 1 915 fiches "
                   "inactives ; et l'effectif publié monte chaque année, de 2 472 en 2019 à "
                   "2 845 en 2025, parce que les partants s'y accumulent.",
                   "Tous les tableaux de l'effectif étaient trois fois trop hauts, et leur "
                   "évolution sur cinq ans allait dans le mauvais sens.",
                   "Preuve : lot 6, Q23, Q24, Q39 ; corrigée le 22/10/2026"),
            erreur("Q40", "Les bornes des tranches excluent des agents", "confirmée",
                   "Elle compare l'âge et l'ancienneté à la borne haute de chaque tranche avec "
                   "« < », alors que cette borne est incluse dans la table des tranches.",
                   "704 agents perdus sur 2 822 au 31/12/2025 : 329 par l'âge, 461 par "
                   "l'ancienneté. La tranche « moins d'un an » (de 0 à 0) ne reçoit jamais "
                   "personne, donc aucun recruté de l'année n'est compté. 2 822 moins 704 donne "
                   "exactement les 2 118 publiés.",
                   "Un quart des agents disparaissait de T58 et de T61, et l'effectif publié "
                   "montait et descendait sans raison visible.",
                   "Preuve : lot 7, Q40 ; lot 8, Q58 ; corrigée le 22/10/2026"),
            erreur("T58", "Les tranches ne sont pas celles de l'Annuaire", "confirmée",
                   "Les tranches d'âge de l'entrepôt vont de « moins de 25 » à « 55 et plus », "
                   "et les tranches d'ancienneté s'arrêtent à « 30 et plus ».",
                   "l'Annuaire CIPRES demande, pour T58, « moins de 20 », 20-24, puis 55-59 et "
                   "« 60 ans et plus » ; pour T61, il va jusqu'à 30-34, 35-39 et « 40 ans et "
                   "plus ».",
                   "Quatre lignes manquent à T58 et trois à T61 : les maquettes de l'Annuaire ne "
                   "peuvent pas être remplies telles quelles. Les tranches CIPRES sont écrites "
                   "dans les requêtes proposées.",
                   "Source : reference_cipres_T58_T65.md (Annuaire CIPRES, p. 50 à 52)"),
            erreur("Q36", "Le taux d'encadrement est calculé sur la qualification", "confirmée",
                   "La requête du ratio appelle encadrant un agent dont la qualification vaut "
                   "HC, 04, 05 ou 06 ; la requête des indicateurs, elle, ne retient que HC, 05 "
                   "et 06.",
                   "l'annuaire de la CNSS range les agents par la catégorie de la grille de "
                   "salaire, un autre champ. Avec la catégorie, le taux retombe à un point près "
                   "sur le chiffre publié (48,3 % pour 49,2 % en 2024).",
                   "Deux taux différents sont publiés pour la même année : 31,1 % dans une table "
                   "et 13,6 % dans l'autre pour 2025. C'est la correction E10, pas encore faite.",
                   "Preuve : lot 6, Q36, Q39 ; lot 8, Q56 ; lot 19, Q111, Q112"),
            erreur("Q137", "Les agents absents sont lus dans la position", "confirmée",
                   "Elle appelle inactif un agent dont la position de situation vaut détaché, "
                   "mis à disposition, en disponibilité, suspendu ou sous les drapeaux.",
                   "cette position n'est pas datée : l'application l'écrase sur la situation en "
                   "cours. Au 31/12/2024, les 6 permanents en disponibilité d'après les "
                   "décisions étaient tous en position « activité normale ».",
                   "La colonne des agents inactifs ne veut rien dire à une date passée. Les "
                   "absences se lisent désormais dans les décisions 07 et 18 (règle G13).",
                   "Preuve : lot 24, Q136 à Q139"),
            erreur("Q50", "La formation ne gardait que le statut T", "confirmée",
                   "Elle ne retenait que les formations au statut T, qu'elle lisait comme "
                   "« terminée ».",
                   "aucune formation n'a ce statut : toutes les formations saisies, de 2015 à "
                   "2025, sont au statut N. Le statut n'est plus tenu depuis 2015.",
                   "T62 et T64 ne publiaient aucune ligne depuis 2019, alors que la base a, "
                   "depuis cette année-là, 30 à 45 formations et 435 à 886 inscriptions par an. "
                   "C'est la correction E12.",
                   "Preuve : lot 8, Q50, Q56 ; lot 9, Q69 ; corrigée le 22/10/2026"),
            erreur("Q79", "Le budget de formation est une année", "confirmée",
                   "Elle prenait le champ BUDGET d'une formation pour un montant, et "
                   "l'additionnait une fois par agent inscrit.",
                   "le champ contient l'année budgétaire d'imputation, saisie en chiffres, et "
                   "seulement depuis 2020.",
                   "Le budget de T64 était faux deux fois : mauvaise donnée, et comptée autant "
                   "de fois qu'il y a d'inscrits. Il est désormais laissé vide, en attendant la "
                   "comptabilité.",
                   "Preuve : lot 11, Q78, Q79 ; lot 8, Q52, Q53 ; corrigée le 22/10/2026"),
            erreur("Q62", "Un frais non saisi annulait le coût", "en partie",
                   "Elle additionnait les sept frais d'une inscription avec un simple « + », ce "
                   "qui donne un total vide dès qu'un seul frais manque.",
                   "la formule est bien celle-là, et les frais sont souvent incomplets ; mais la "
                   "mesure de l'effet (Q62) n'a pas été reçue, le filtre du statut T ayant vidé "
                   "le premier essai.",
                   "Le coût des formations de T64 était sous-estimé d'un montant inconnu. Un "
                   "frais vide compte maintenant pour zéro (correction E13).",
                   "Preuve : lot 8, Q51 ; lot 9, Q62 ; corrigée le 22/10/2026"),
            erreur("Q65", "Les durées sont additionnées sans leur unité", "en partie",
                   "Elle additionne les durées des inscriptions comme des jours, sans regarder "
                   "l'unité saisie à côté.",
                   "9 357 inscriptions sont en jours, 139 en mois, 11 en années, 1 en KG et 1 en "
                   "CFA ; les « mois » et les « années » sont en fait des jours mal saisis, et "
                   "une durée vaut −17.",
                   "Le total des jours de formation reste approximatif. La correction E15 n'est "
                   "pas faite : il faut d'abord savoir si certaines durées sont de vrais mois.",
                   "Preuve : lot 9, Q65"),
            erreur("Q56", "Les indicateurs de finances sont vides", "confirmée",
                   "Quatre de ses cinq taux sont écrits en dur à vide : frais de personnel "
                   "(T63), charges administratives, formation (T64) et exécution budgétaire. "
                   "Seul le taux d'encadrement est calculé.",
                   "la paie du personnel n'est pas dans la copie de la base (22 vues invalides), "
                   "et la comptabilité de cette copie a été jugée non valide par la CNSS le "
                   "07/11/2026.",
                   "T63 et T64 n'ont aucun chiffre depuis l'origine. Ils viendront de la vue "
                   "comptable V_BI_BALANCE_COMPTABLE, que la DSI doit alimenter (correction "
                   "E18).",
                   "Preuve : lot 8, Q55, Q56 ; lot 9, Q66 ; décision CNSS du 07/11/2026"),
            erreur("Q56", "Des exercices publiés avant d'être clos", "tranchée",
                   "Les tables publiaient l'année en cours, et même une année à venir : les "
                   "indicateurs donnaient déjà un taux pour 2026.",
                   "un exercice n'est comparable que s'il est complet ; une photo de mi-année ne "
                   "se compare ni à l'annuaire ni à l'année précédente.",
                   "Les requêtes ne publient plus que les exercices clos, arrêtés au 31/12. La "
                   "périodicité mensuelle reste possible par un réglage.",
                   "Preuve : lot 8, Q56 ; corrigée le 22/10/2026"),
            erreur("Q49", "La formation lit la base source directement", "confirmée",
                   "Elle va chercher les inscriptions dans la base de l'application "
                   "(USER_GRH.GRH_INSCRIPTION) au lieu des faits de l'entrepôt, et sa dimension "
                   "des catégories vient d'une table GRH_CAT_FORMATION qui n'existe pas.",
                   "la table n'existe pas dans la base : la dimension du DTM a été remplie à la "
                   "main. L'application ne connaît que deux catégories (perfectionnement et "
                   "diplômante), alors que quatre sont saisies.",
                   "Une table du DTM ne devrait pas dépendre de la base de production, et le "
                   "chiffre ne suit plus le cliché. C'est la correction E17, pas encore faite.",
                   "Preuve : lot 8, Q49 ; lot 9, Q68"),
        ]),
        Partie("Ce qui a été corrigé, et ce qui reste", colonnes=3,
               intro="Quatre requêtes ont été corrigées le 22/10/2026 (commit 1b44a0c, branche "
                     "fix/grh-requetes-dtm) et testées sur la base le même jour. Aucune colonne "
                     "n'a changé, pour ne rien casser dans OBIEE. Les règles de gestion restent "
                     "à faire viser par la DRH avant la mise en production.",
               blocs=[
            Bloc("grh_effectif.sql", [
                ("Corrigé le 22/10/2026", "Agent présent au 31/12 (actif, ou sorti après cette "
                                          "date) ; dernière situation commencée avant le 31/12 ; "
                                          "bornes des tranches incluses ; exercices clos "
                                          "seulement (E1 à E5). Testé : 885 agents en 2025, dans "
                                          "la fourchette attendue."),
                ("Ce qui reste", "Être remplacé par t58_t65_effectif_requete_proposee.sql, qui "
                                 "garde ses colonnes et son grain et en ajoute cinq (catégorie, "
                                 "groupe de catégories, unité, domaine, nombre de cadres)."),
            ], genre="suite", etat="fait", pied="lot 12, Q80 ; regles_effectif.md"),
            Bloc("grh_ratio.sql", [
                ("Corrigé le 22/10/2026", "Même règle de présence que l'effectif, exercices clos "
                                          "(E1, E2, E5). Testé : 41,7 % pour 2025, au lieu de "
                                          "31,1 % publiés."),
                ("Ce qui reste", "L'encadrant est toujours défini par la qualification et non "
                                 "par la catégorie (E10). Son sort est à décider : aligner cette "
                                 "requête, ou lire le taux dans la table de l'effectif "
                                 "(NB_CADRES ÷ NB_AGENTS)."),
            ], genre="suite", etat="en cours", pied="lot 12, Q82 ; regles_effectif.md, G6"),
            Bloc("grh_formation.sql", [
                ("Corrigé le 22/10/2026", "Plus de filtre sur le statut de la formation (E12) ; "
                                          "un frais vide compte pour zéro (E13) ; budget laissé "
                                          "vide, car il contient une année. Testé : conforme."),
                ("Ce qui reste", "Les unités de durée (E15) et la lecture directe de la base "
                                 "source (E17). À remplacer ensuite par "
                                 "t62_requete_proposee.sql, qui compte les agents distincts "
                                 "formés dans l'année (règle G10)."),
            ], genre="suite", etat="en cours", pied="lot 10, Q73 ; lot 12, Q83"),
            Bloc("indicateurs_finances_grh.sql", [
                ("Corrigé le 22/10/2026", "Elle lit l'effectif du cliché chargé, sans plus trier "
                                          "des années écrites en texte (E11)."),
                ("Ce qui reste", "Les quatre taux financiers restent vides (E18). Elle sera "
                                 "remplacée par t63_t64_requete_proposee.sql, qui calcule le "
                                 "taux des frais de personnel et celui de la formation à partir "
                                 "des montants comptables, dès que la DSI les aura fournis."),
            ], genre="suite", etat="à faire", pied="README d'analyses/grh ; t63_t64_deploiement.sql"),
            Bloc("grh_mouvement.sql", [
                ("Pas corrigée", "C'est la seule des cinq qui n'a pas été touchée : elle attend "
                                 "les réponses de la DRH sur les entrées et les sorties."),
                ("Ce qui reste", "Être remplacée par t60_requete_proposee.sql (08/11/2026) : "
                                 "mêmes colonnes et même grain, quatre colonnes de plus (qualité, "
                                 "nature d'emploi, unité, motif). Les mouvements internes de la "
                                 "table d'aujourd'hui sont gardés tels quels (règle G17)."),
            ], genre="suite", etat="à faire", pied="regles_t60.md ; points 2, 3, 20 de la v2"),
            Bloc("La règle qu'on s'est donnée", [
                ("", "On ne change ni les colonnes, ni le grain, ni le paramètre d'une table "
                     "DTM : seules les règles de calcul changent, et les colonnes nouvelles "
                     "s'ajoutent à la fin. Chaque remplacement attend le visa de la DRH, et "
                     "s'accompagne d'un script qui ajoute les colonnes dans le DTM et dans "
                     "l'ODS. En attendant, les contrôles se lancent sur la base source, car "
                     "l'entrepôt n'est pas encore chargé."),
            ], genre="suite", etat="en cours",
                 pied="regles_effectif.md et regles_t60.md, « la structure ne change pas »"),
        ]),
    ],
)
