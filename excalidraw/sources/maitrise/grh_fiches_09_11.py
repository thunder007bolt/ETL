"""
Fiches 09 à 11 de la maîtrise du domaine de la gestion administrative (T58 à T65) : les questions
encore ouvertes, l'avancement du domaine, et les chiffres et réponses à connaître avant une réunion.
"""

from fiches import Bloc, Fiche, Partie
from grh_commun import D, ETAT_AU, chiffre, constat, qr, question

# =============================================================================================
# 09 Les questions ouvertes
# =============================================================================================

F09 = Fiche(
    fichier=D + "09 les questions ouvertes.excalidraw",
    titre="Les questions encore ouvertes",
    sous_titre="Fiche 09 du domaine de la gestion administrative (T58 à T65) · à la DRH, à la "
               "DFC et à la DSI · " + ETAT_AU,
    objectif="ce qui attend encore une réponse de la CNSS, à qui on le demande, et de combien "
             "chaque réponse déplace un chiffre des tableaux. La fiche des points à valider en "
             "compte 64 : 45 ouverts, 19 déjà répondus. Les plus lourds sont ici.",
    parties=[
        Partie("À trancher en priorité", colonnes=3,
               intro="Les sept points que la DRH doit voir d'abord. Aucun ne bloque le travail : "
                     "en attendant, la requête applique ce qui est écrit dans « Notre "
                     "proposition », par un réglage qu'une réponse suffit à changer. La pastille "
                     "est le numéro du point dans la fiche des points à valider (version 2).",
               blocs=[
            question("20", "La fin d'un contrat temporaire",
                     "Compte-t-on un contractuel sans fin de contrat connue seulement si sa "
                     "fiche n'est pas inactive ?",
                     "Aucune date de fin de contrat n'est saisie. La seule trace est la décision "
                     "d'indemnité de fin de contrat (35), prise environ 9 mois après la fin "
                     "réelle.",
                     "299 temporaires au 31/12/2024 avec la règle d'aujourd'hui, pour 46 "
                     "contractuels publiés. 47 en ne gardant que les fiches non inactives.",
                     "Ne compter que les fiches non inactives, en attendant la saisie des fins "
                     "de contrat.",
                     "points v2, point 20 · lot 27, Q153 · contrôle 1 du 17/11"),
            question("2", "Les 84 agents engagés en 2019",
                     "Que sont devenus ces 84 agents, et pouvez-vous saisir leur départ dans "
                     "l'application ?",
                     "84 permanents en service depuis 2018, engagés en 2019 (décision 51), tous "
                     "à la fiche inactive, sans aucune décision de départ ni fiche plus récente.",
                     "C'est l'essentiel de l'écart des années passées : 670 permanents dans la "
                     "base fin 2023, pour 767 publiés.",
                     "Laisser les années 2019 à 2023 telles que la base les donne, et le dire. "
                     "Sans effet à partir de 2025.",
                     "points v2, point 2 · lot 27, Q154 et Q155 · README, G11"),
            question("3", "La date d'un décès",
                     "Peut-on saisir la date du décès, ou au moins celle du communiqué "
                     "nécrologique ?",
                     "La date de décès de la fiche (DATE_DECES) n'est plus remplie depuis 2010 : "
                     "59 dates pour 435 agents décédés.",
                     "Le total des décès est juste, mais décalé d'un an : 5 comptés en 2024 pour "
                     "3 publiés, et aucun décès encore visible en 2025.",
                     "Prendre la décision de liquidation des droits (34), qui arrive environ 8 "
                     "mois après le décès.",
                     "points v2, point 3 · lot 26, Q147 · regles_t60.md, G9"),
            question("4", "Les agents formés en 2024",
                     "D'où viennent les 480 formés de l'annuaire ? Si c'est un autre fichier que "
                     "l'application, pouvez-vous nous le donner ?",
                     "Ni la date retenue ni le type d'agent n'expliquent l'écart : aucun "
                     "temporaire ni externe n'est formé.",
                     "407 agents formés avec la date de début des formations, 409 avec la date "
                     "de fin, pour 480 publiés et 522 inscriptions.",
                     "Compter les agents distincts inscrits à une formation commencée dans "
                     "l'année, comme l'a décidé l'atelier de Kaya en 2017.",
                     "points v2, point 4 · contrôle 3 du 17/11 · README, QG4"),
            question("25", "Les départs sans date",
                     "Pouvez-vous saisir dans l'application la date et le motif des départs des "
                     "cinq dernières années ?",
                     "817 personnes sont parties sans aucune date : ni décision de départ, ni "
                     "date de décès. Leur fiche dit seulement « inactif ».",
                     "Un départ sans date ne tombe dans aucune année : l'agent est effacé de "
                     "toutes, même celles où il travaillait.",
                     "Ne compter que ce qui est saisi dans l'application : un registre tenu à "
                     "côté ne serait pas lu.",
                     "points v2, point 25 · README, Q44"),
            question("48", "Le domaine de chaque unité",
                     "Pouvez-vous remplir le domaine de chaque unité dans le fichier, d'abord "
                     "les 132 lignes « À décider » ?",
                     "Le domaine se lit sur l'unité où travaille l'agent (UA_CODE). Le code de "
                     "domaine de la situation n'est pas fiable : dans 66 unités sur 139, les "
                     "agents d'une même unité ont des codes différents.",
                     "Tout T59 : sans ce fichier, chaque agent tombe dans « Domaine non "
                     "renseigné » et le tableau ne montre rien.",
                     "La BI a préparé le fichier des 432 unités avec une proposition par unité ; "
                     "la CNSS tranche, ligne par ligne.",
                     "points v2, point 48 · lot 23, Q129 · README, G12"),
            question("57", "Les années passées",
                     "Pour 2021 à 2024, faut-il reprendre les chiffres publiés dans l'annuaire, "
                     "ou les recalculer sur la base d'aujourd'hui ?",
                     "T60, T63, T64 et T65 montrent cinq années, mais il n'existe aucune photo "
                     "de la base pour les années passées : une seule copie, du 18/02/2026.",
                     "Permanents recalculés aujourd'hui : 769, 734, 703, 670 et 741 de 2020 à "
                     "2024 ; annuaire : 869, 834, 806, 767 et 758.",
                     "En attendant, on recalcule toutes les années sur la copie du moment. Pour "
                     "les prestations, les années d'avant reprennent les chiffres de leur photo "
                     "officielle.",
                     "points v2, point 57 · contrôle 1 du 17/11"),
        ]),
        Partie("Les autres questions à la DRH qui changent un chiffre", colonnes=3,
               intro="Moins lourdes, mais chacune déplace une case d'un tableau.",
               blocs=[
            question("45", "La fiche sans état",
                     "Une fiche sans état entrée depuis plus de deux ans est-elle celle d'un "
                     "agent parti, à ne plus compter ?",
                     "Quand un agent vient d'être embauché, sa fiche n'a pas encore d'état : ni "
                     "active, ni inactive. Depuis le 05/11, elle compte comme encore là, pour ne "
                     "pas perdre les recrutements de fin d'année.",
                     "67 fiches sont sans état ; au moins 11 sont anciennes. Elles ajoutent 5 "
                     "permanents de 60 ans et plus fin 2024, alors que l'annuaire n'en a aucun.",
                     "Garder les fiches récentes, écarter celles entrées depuis plus de deux "
                     "ans. Correction à faire de notre côté.",
                     "points v2, point 45 · lot 27, Q158 · contrôle 1 du 17/11"),
            question("64", "La fiche inactive d'un agent en disponibilité",
                     "La fiche d'un agent passe-t-elle bien à « inactif » pendant sa "
                     "disponibilité, et pour quelles autres absences ?",
                     "On lisait « inactif » comme « parti ». Or 12 agents mis en disponibilité "
                     "de 2021 à 2025 ne sont pas revenus, et leur fiche est inactive sans aucun "
                     "départ daté.",
                     "Ces 12 agents sont effacés de toutes les années : il manque 12 permanents "
                     "fin 2020 et encore 3 fin 2024, et T60 ne voit aucune de leurs sorties.",
                     "Prendre la mise en disponibilité comme date de sortie. Correction à faire "
                     "de notre côté.",
                     "points v2, point 64 · lot 28, Q159"),
            question("15", "La catégorie des contractuels",
                     "Comment classez-vous un contractuel dans une catégorie ?",
                     "Pour les permanents, la catégorie de la grille de salaire de la situation "
                     "(CATEGORIE) retombe sur l'annuaire. Pour les contractuels, non.",
                     "L'annuaire range ses 46 contractuels en 18 employés, 13 agents de "
                     "maîtrise, 10 cadres moyens et 5 cadres supérieurs ; la base trouve 18, 8, "
                     "18 et aucun.",
                     "Garder la catégorie de la situation, comme pour les permanents, en "
                     "attendant la réponse.",
                     "points v2, point 15 · lot 19, Q114 · annuaire CNSS 2024, tableau 119"),
            question("22", "La date de la retraite",
                     "La date d'effet de la décision 02 est-elle le jour du départ, et quelle "
                     "date garder quand la retraite a été modifiée après coup ?",
                     "Une retraite est la décision 02, prise à sa date d'effet. 218 décisions "
                     "modifient une retraite déjà prise.",
                     "24 retraites comptées en 2024 pour 23 publiées ; 39, 31, 29 et 30 de 2020 "
                     "à 2023, pour 43, 31, 26 et 32.",
                     "Garder la date d'effet de la décision 02, sans tenir compte des "
                     "modifications.",
                     "points v2, point 22 · lot 19, Q116 · contrôle 2 du 17/11"),
            question("23", "Les détachements",
                     "Où sont saisis les détachements et leurs fins ?",
                     "La décision d'accueil d'un agent détaché (37) n'existe pour aucun "
                     "permanent dans la base.",
                     "Les autres entrées et sorties de T60 : l'annuaire compte 2 détachements et "
                     "2 fins de détachement en 2023, puis 1 détachement en 2024 ; la base, "
                     "aucun.",
                     "Ne rien compter tant qu'on ne sait pas où ils sont saisis.",
                     "points v2, point 23 · README, QG7 · annuaire CNSS 2024, tableau 121"),
            question("49", "Le détail des « autres »",
                     "La liste des autres entrées et sorties vous convient-elle ?",
                     "La maquette CIPRES n'a que deux colonnes « autres ». Le recueil des "
                     "besoins demande de les détailler pour la CNSS.",
                     "Les colonnes de détail de T60. De 2020 à 2025, seuls cinq motifs sont "
                     "remplis : remise en activité, disponibilité, démission, licenciement, fin "
                     "de contrat.",
                     "Reprendre telle quelle la liste des 18 codes de mouvement de "
                     "l'application, qui est celle de l'annuaire de la CNSS.",
                     "points v2, point 49 · lot 26, Q152 · recueil v7, RH ligne 3"),
            question("56", "L'agence et le bureau d'une unité",
                     "Notre lecture est-elle juste, ou l'application garde-t-elle ailleurs le "
                     "type de chaque unité ?",
                     "Le recueil demande chaque tableau par direction, agence et bureau. Or les "
                     "157 unités en vigueur n'ont que deux natures : Direction (21) et Service "
                     "(136).",
                     "La déclinaison de T58 à T65 : 16 agences, 17 bureaux et 5 directions "
                     "régionales ne se reconnaissent aujourd'hui qu'à leur libellé.",
                     "Lire l'agence et le bureau dans le libellé, la direction régionale dans le "
                     "code de région.",
                     "points v2, point 56 · lot 23 · recueil v7, RH lignes 1 à 7"),
        ]),
        Partie("À la direction financière et à la DSI", colonnes=3,
               intro="T63 et T64 n'ont aucun chiffre aujourd'hui : ces trois points les "
                     "commandent.",
               blocs=[
            question("55", "La vue de la comptabilité",
                     "Quand pourrez-vous créer et alimenter cette vue depuis Oracle GL, et "
                     "depuis quel exercice ?",
                     "La comptabilité de la copie de la base n'est pas valide : la CNSS l'a dit "
                     "le 07/11. Tout passe par une vue commune, V_BI_BALANCE_COMPTABLE, que la "
                     "DSI doit créer.",
                     "Tout T63 et tout T64 : aucun montant n'est calculable sans elle, et il "
                     "faut cinq exercices (N−4 à N).",
                     "La structure est écrite et envoyée : un montant par exercice, compte, "
                     "gestion et section, réalisé et budgété.",
                     "points v2, points 30 et 55 · README, requêtes proposées T63-T64"),
            question("29", "Les comptes des charges de fonctionnement",
                     "Les charges de fonctionnement se prennent-elles sur la gestion G seule, ou "
                     "sur G et S comme les frais de personnel ?",
                     "T63 compare les frais de personnel aux charges de fonctionnement : les "
                     "deux doivent porter sur le même périmètre.",
                     "Le dénominateur de T63, donc le ratio comparé à la norme CIPRES de 85 % au "
                     "plus.",
                     "Frais de personnel sur G et S, charges de fonctionnement sur la gestion G "
                     "seule, en attendant la réponse.",
                     "points v2, point 29 · recueil v7, RH ligne 6 et Finance ligne 1"),
            question("31", "Les comptes de la formation",
                     "« 63, 86 et 87 » veut-il dire tous ces comptes, ou des sous-comptes comme "
                     "6386 et 6387 ?",
                     "Pris à la lettre, le compte 63 compte tous les services consommés, pas "
                     "seulement la formation.",
                     "La dépense de formation de T64, donc le ratio comparé à la norme CIPRES de "
                     "1 à 5 % de la masse salariale.",
                     "Prendre les comptes du recueil des besoins (63, 86 et 87), par un réglage "
                     "qu'une réponse suffit à changer.",
                     "points v2, point 31 · recueil v7, RH ligne 6"),
        ]),
        Partie("Les questions que la base a fermées", colonnes=3,
               intro="Trois points attendaient la CNSS. Un comptage lancé le 17/11 a montré "
                     "qu'ils n'avaient aucun effet : ils sont retirés de la liste, et la réponse "
                     "est écrite dans la fiche des points à valider.",
               blocs=[
            constat("12", "Le décès d'un retraité",
                    "On se demandait si un agent mort après son départ à la retraite devait "
                    "compter dans les décès de T60. Le comptage a compté, année par année depuis "
                    "2019, les décès survenus avant et après une retraite.",
                    "Aucun décès après la retraite depuis 2019 : les 21 décès connus sont tous "
                    "survenus en activité.",
                    "Le point tombe. Si un cas arrive un jour, l'agent avait déjà quitté "
                    "l'effectif : il ne comptera pas dans les décès.",
                    "points v2, point 12 · lot 26, Q147"),
            constat("60", "La formation de plus d'un an",
                    "Une formation diplômante peut durer plus d'un an : il aurait fallu décider "
                    "dans quelle année la compter. Le comptage a mesuré, pour chaque année "
                    "depuis 2019, les formations finies une autre année et celles de plus de 365 "
                    "jours.",
                    "Aucune formation de plus d'un an en sept ans. Une seule est à cheval sur "
                    "deux années : commencée en 2023, finie en 2024, avec 2 agents.",
                    "Le point tombe. La règle de l'atelier de Kaya, compter la formation l'année "
                    "où elle commence, ne déplace que ces 2 agents.",
                    "points v2, points 60 et 28 · lot 26, Q148"),
            constat("61", "Les temporaires sans engagement",
                    "Un temporaire n'est compté que pendant ses contrats, qui commencent à la "
                    "décision d'engagement (50) : ceux qui n'en ont aucune ne sont comptés nulle "
                    "part. Le comptage les a rangés par état de fiche et par période d'entrée.",
                    "230 temporaires sans décision 50, dont 227 à la fiche inactive et entrés "
                    "avant 2015. Restent 3 fiches sans état, elles aussi anciennes.",
                    "Le point tombe pour les années suivies. Il a servi à autre chose : une "
                    "fiche sans état n'est donc pas toujours une embauche récente (point 45).",
                    "points v2, point 61 · lot 26, Q151"),
        ]),
    ],
)

# =============================================================================================
# 10 Où on en est
# =============================================================================================

F10 = Fiche(
    fichier=D + "10 ou on en est.excalidraw",
    titre="Où on en est, et la suite",
    sous_titre="Fiche 10 du domaine de la gestion administrative (T58 à T65) · " + ETAT_AU,
    objectif="pour chaque tableau, ce qui est prêt et ce qui manque ; ce qu'on attend de la "
             "CNSS ; ce qui reste à faire de notre côté ; et où trouver chaque livrable.",
    parties=[
        Partie("Tableau par tableau", colonnes=3,
               intro="Les chiffres sont ceux du 17/11/2026 : les trois requêtes proposées ont "
                     "été lancées ce jour-là sur la base source, et comparées à l'annuaire de la "
                     "CNSS 2024.",
               blocs=[
            Bloc("T58 · L'effectif par catégorie, âge et sexe", [
                ("Prêt", "Les règles de présence, de catégorie, d'âge et de sexe, et la requête "
                         "proposée avec cinq colonnes en plus."),
                ("Le chiffre du 17/11", "741 permanents au 31/12/2024 pour 758 publiés ; les "
                                        "employés exactement comme l'annuaire (133 hommes, 44 "
                                        "femmes) ; les trois premières tranches d'âge exactes."),
                ("Manque", "Les temporaires : 299 pour 46 publiés, faute de fins de contrat. Et "
                           "5 vieilles fiches sans état comptées à tort."),
                ("Source", "contrôle 1 du 17/11 · regles_effectif.md, G1 à G4"),
            ], ref="T58", genre="suite", etat="en cours"),
            Bloc("T59 · L'effectif par domaine d'activité", [
                ("Prêt", "La règle : le domaine vient de l'unité de l'agent, pas du code de la "
                         "situation. La colonne est ajoutée à la table, et le fichier des 432 "
                         "unités est préparé et envoyé à la CNSS."),
                ("Manque", "Tout le reste : la CNSS doit remplir le domaine de chaque unité, "
                           "d'abord 132 lignes « À décider ». Sans elles, chaque agent tombe "
                           "dans « Domaine non renseigné »."),
                ("Source", "regles_effectif.md, G12 · lot 23 · points v2, point 48"),
            ], ref="T59", genre="suite", etat="à faire"),
            Bloc("T60 · Le mouvement du personnel", [
                ("Prêt", "Les règles des entrées et des sorties, le détail des « autres » par "
                         "les codes de la CNSS, et la requête réécrite le 08/11 sur la structure "
                         "de la table."),
                ("Le chiffre du 17/11", "L'égalité tient chaque année : en 2024, 670 au début, "
                                        "101 recrutements, 24 retraites, 5 décès, 1 autre "
                                        "sortie, 741 à la fin. La fin d'une année est bien le "
                                        "début de la suivante."),
                ("Manque", "Les disponibilités (0 à 1 par an dans la base, 2 à 7 publiées) et "
                           "les détachements. Le visa de la DRH sur les neuf règles."),
                ("Source", "contrôle 2 du 17/11 · regles_t60.md, G8, G9, G14 à G17"),
            ], ref="T60", genre="suite", etat="en cours"),
            Bloc("T61 · L'effectif par ancienneté", [
                ("Prêt", "La règle : l'ancienneté part de la prise de service, à défaut de "
                         "l'embauche, et les périodes d'absence sont retirées."),
                ("Le chiffre du 17/11", "Première mesure avec cette règle : chaque tranche "
                                        "tombe à 4 agents près de l'annuaire."),
                ("Manque", "Une seule tranche résiste : « 40 ans et plus », 6 agents pour 1 "
                           "publié."),
                ("Source", "contrôle 1 du 17/11 · regles_effectif.md, G5"),
            ], ref="T61", genre="suite", etat="en cours"),
            Bloc("T62 · Les agents formés", [
                ("Prêt", "La règle : les agents distincts inscrits à une formation commencée "
                         "dans l'année, par catégorie, sexe et finalité. La requête proposée."),
                ("Le chiffre du 17/11", "407 permanents formés en 2024 pour 480 publiés, et 522 "
                                        "inscriptions. Aucun formé « avec promotion », pour 6 "
                                        "publiés."),
                ("Manque", "L'explication de l'écart : ni la date retenue ni le type d'agent ne "
                           "le donnent. Question posée à la DRH (point 4)."),
                ("Source", "contrôle 3 du 17/11 · README, G10 et QG4"),
            ], ref="T62", genre="suite", etat="en cours"),
            Bloc("T63 · Frais de personnel sur charges de fonctionnement", [
                ("Prêt", "La structure de la vue comptable à fournir, la requête proposée et les "
                         "comptes retenus, tous réglables."),
                ("Manque", "Les montants : la comptabilité de la copie n'est pas valide, et la "
                           "vue V_BI_BALANCE_COMPTABLE n'existe pas encore. Aucun chiffre "
                           "aujourd'hui."),
                ("Source", "points v2, points 29, 54, 55 · README, requêtes proposées"),
            ], ref="T63", genre="suite", etat="à faire"),
            Bloc("T64 · Formation sur masse salariale et sur budget", [
                ("Prêt", "La même requête : masse salariale sur les comptes 651 et 652, dépense "
                         "de formation et budget par réglage."),
                ("Manque", "Les mêmes montants que T63, et trois réponses de la direction "
                           "financière : les comptes de la formation, le budget à prendre, les "
                           "gestions de la masse salariale."),
                ("Source", "points v2, points 31, 32, 63 · recueil v7, RH ligne 6"),
            ], ref="T64", genre="suite", etat="à faire"),
            Bloc("T65 · Le taux d'encadrement", [
                ("Prêt", "La règle : les cadres (catégories 04, 05, 06 et HC) divisés par "
                         "l'effectif, à chaque 31 décembre."),
                ("Le chiffre du 17/11", "48,3 % en 2024 pour 49,2 % publiés, et le taux suit "
                                        "l'annuaire à un point près de 2020 à 2024."),
                ("Manque", "Rien en propre : le tableau suivra l'effectif. Reste à dire si le "
                           "taux CIPRES porte sur les seuls permanents (point 59)."),
                ("Source", "contrôle 1 du 17/11 · regles_effectif.md, G6"),
            ], ref="T65", genre="suite", etat="en cours"),
        ]),
        Partie("Ce qui attend la CNSS", colonnes=3,
               intro="Rien de tout cela ne bloque le travail : chaque question a une valeur par "
                     "défaut dans les requêtes. Mais tant qu'elles ne sont pas réglées, certains "
                     "chiffres resteront loin de ceux publiés.",
               blocs=[
            Bloc("La DRH · répondre aux points", [
                ("Quoi", "La fiche des points à valider, version 2 du 17/11 : 64 points, dont "
                         "45 encore ouverts."),
                ("D'abord", "Les points 20 (les fins de contrat), 2 (les 84 de 2019), 3 (les "
                            "dates de décès), 4 (les 480 formés), 25 (les départs sans date), "
                            "48 (le fichier des unités) et 57 (les années passées)."),
            ], genre="suite", etat="à faire"),
            Bloc("La DRH · saisir les fins de contrat", [
                ("Quoi", "La fin de chaque contrat temporaire dans l'application, au lieu d'une "
                         "indemnité de fin de contrat saisie 9 mois plus tard."),
                ("Sans quoi", "Un contractuel reste compté pour toujours : 299 temporaires fin "
                              "2024 pour 46 publiés, et le stock monte chaque année."),
            ], genre="suite", etat="à faire"),
            Bloc("La DRH · saisir les dates de décès", [
                ("Quoi", "La date du décès sur la fiche de l'agent, ou au moins celle du "
                         "communiqué nécrologique."),
                ("Sans quoi", "Le décès est daté par la liquidation des droits, environ 8 mois "
                              "plus tard : le total est bon, mais décalé d'un an."),
            ], genre="suite", etat="à faire"),
            Bloc("La DRH · dater les départs", [
                ("Quoi", "La date et le motif des départs des cinq dernières années : 817 "
                         "personnes sont parties sans aucune date."),
                ("Dont", "Les 84 agents engagés en 2019, que l'annuaire compte jusqu'en 2023 et "
                         "sort en 2024."),
            ], genre="suite", etat="à faire"),
            Bloc("La DRH · remplir le fichier des unités", [
                ("Quoi", "Le domaine d'activité de chaque unité, dans le fichier préparé par la "
                         "BI : 432 unités, une proposition par ligne."),
                ("D'abord", "Les 132 lignes « À décider » : agences, bureaux, guichets et "
                            "directions régionales. Sans elles, T59 est vide."),
            ], genre="suite", etat="à faire"),
            Bloc("La DFC · les comptes de T63 et T64", [
                ("Quoi", "Les charges de fonctionnement sur quelle gestion (point 29) ; les "
                         "comptes de la formation (point 31) ; le budget à prendre (point 32) ; "
                         "les gestions de la masse salariale (point 63)."),
                ("Pour", "Les deux ratios et leur comparaison aux normes CIPRES : 85 % au plus, "
                         "et de 1 à 5 %."),
            ], genre="suite", etat="à faire"),
            Bloc("La DSI · la vue de la comptabilité", [
                ("Quoi", "Créer et alimenter V_BI_BALANCE_COMPTABLE depuis Oracle GL : un "
                         "montant par exercice, compte, gestion et section, réalisé et budgété."),
                ("Pour", "T63 et T64, qui n'ont aucun chiffre aujourd'hui, sur cinq exercices."),
            ], genre="suite", etat="à faire"),
            Bloc("La DSI · la vue des décisions", [
                ("Quoi", "Une vue simple des décisions avec les agents qu'elles visent "
                         "(V_BI_DECISION_AGENT)."),
                ("Pour", "Lire les entrées et les sorties de T60 sans dépendre des tables "
                         "internes de l'application."),
            ], genre="suite", etat="à faire"),
        ]),
        Partie("Ce qui reste de notre côté", colonnes=3,
               intro="Les trois premiers sont nos propres erreurs, vues le 17/11. Elles seront "
                     "corrigées en une seule passe, après les réponses de la CNSS, pour ne pas "
                     "refaire les requêtes trois fois.",
               blocs=[
            Bloc("Corriger · l'agent en disponibilité", [
                ("Le défaut", "La fiche d'un agent en disponibilité passe à « inactif ». Notre "
                              "règle lit une fiche inactive sans départ daté comme un départ "
                              "sans date, et ne compte l'agent à aucune date."),
                ("La correction", "La dernière mise en disponibilité sans retour sert de date de "
                                  "sortie : 12 agents retrouvent leurs années, et T60 montre "
                                  "enfin leurs sorties."),
                ("Source", "lot 28, Q159 · points v2, point 64"),
            ], genre="suite", etat="à faire"),
            Bloc("Corriger · les fiches sans état anciennes", [
                ("Le défaut", "Une fiche sans état compte comme présente, pour ne pas perdre les "
                              "embauches de fin d'année. Mais 11 de ces fiches sont anciennes."),
                ("La correction", "Écarter les fiches sans état entrées depuis plus de deux ans : "
                                  "les 5 permanents de 69 à 80 ans disparaissent de l'effectif, "
                                  "comme dans l'annuaire."),
                ("Source", "lot 27, Q158 · points v2, point 45"),
            ], genre="suite", etat="à faire"),
            Bloc("Corriger · les temporaires sans fin de contrat", [
                ("Le défaut", "Sans fin connue, le contractuel reste compté : 299 fin 2024 pour "
                              "46 publiés."),
                ("La correction", "Passer le réglage à « seulement les fiches non inactives » "
                                  "dès que la DRH aura répondu au point 20 : 47 temporaires fin "
                                  "2024."),
                ("Source", "lot 27, Q153 · points v2, point 20"),
            ], genre="suite", etat="à faire"),
            Bloc("Charger le fichier des unités", [
                ("Quoi", "Créer la table de correspondance unité → domaine dans l'entrepôt, son "
                         "chargement depuis le fichier Excel, et la brancher en tête de la "
                         "requête de l'effectif."),
                ("Quand", "Dès que la CNSS aura rendu le fichier rempli."),
            ], genre="suite", etat="à faire"),
            Bloc("Ajouter les colonnes aux tables DTM", [
                ("Quoi", "Les scripts qui ajoutent les colonnes nouvelles dans le DTM et dans "
                         "l'ODS : cinq à l'effectif, quatre aux mouvements, sept aux indicateurs "
                         "financiers. Aucune colonne n'est retirée."),
                ("Quand", "Après le visa de la DRH, et avec l'accord de la personne en charge "
                          "d'Oracle Analytics."),
            ], genre="suite", etat="à faire"),
            Bloc("La mise en page dans OBIEE", [
                ("Quoi", "Le filtre de nature (permanents ou temporaires), le filtre des agents "
                         "de la CNSS, les maquettes CIPRES par catégorie et sexe, le taux "
                         "d'encadrement, la déclinaison par direction et par agence."),
                ("Attention", "Avant de passer aux fins de mois, régler l'effectif en « dernière "
                              "valeur » sur le temps : sinon une année additionne ses douze "
                              "mois."),
            ], genre="suite", etat="à faire"),
        ]),
        Partie("Les livrables : où trouver quoi", colonnes=3,
               intro="Tout est dans le dépôt. Ce qui part à la CNSS est en Excalidraw ; ce qui "
                     "sert à la BI est en markdown et en SQL.",
               blocs=[
            Bloc("Les règles, tableau par tableau", [
                ("Où", "schemas/grh/1 pour la cnss/regles par tableau"),
                ("Quoi", "Huit fiches, une par tableau de T58 à T65. Chaque ligne : le numéro, "
                         "la règle en français, ce qu'on lit dans la base, et une case « Votre "
                         "remarque ». C'est ce qu'on envoie aux directions."),
            ], genre="notion"),
            Bloc("Les fiches de validation", [
                ("Où", "schemas/grh/1 pour la cnss/fiches de validation"),
                ("Quoi", "Trois fiches à faire remplir en réunion, avec les cases D'accord, Pas "
                         "d'accord, À discuter : T58 (pilote de T58, T59, T61 et T65), T60 et "
                         "T62. T63 et T64 attendent la comptabilité."),
            ], genre="notion"),
            Bloc("La fiche des points à valider", [
                ("Où", "analyses/grh/points_a_valider_v2.md"),
                ("Quoi", "64 points numérotés, 45 ouverts et 19 répondus. Chacun dit de quoi on "
                         "parle, à quoi il sert dans le tableau, ce qu'on voit dans la base, ce "
                         "qu'on fait en attendant, à qui on pose la question, et le chiffre."),
            ], genre="notion"),
            Bloc("Les lots de requêtes", [
                ("Où", "analyses/grh, un fichier par lot"),
                ("Quoi", "28 lots, de Q01 à Q159. L'en-tête de chaque requête donne son "
                         "objectif, le tableau visé, le résultat obtenu et le constat. Le README "
                         "les résume."),
            ], genre="notion"),
            Bloc("Les contrôles du 17/11", [
                ("Où", "analyses/grh/controles"),
                ("Quoi", "Trois fichiers prêts à lancer, qui entourent chaque requête proposée "
                         "d'un contrôle avec son attendu. Ils lisent la base source, car "
                         "l'entrepôt n'est pas chargé. Les résultats sont dans "
                         "resultats_2026-11-17.md."),
            ], genre="notion"),
            Bloc("Les règles écrites et les requêtes", [
                ("Où", "analyses/grh : README.md, regles_effectif.md, regles_t60.md"),
                ("Quoi", "Les règles G1 à G17 avec leur preuve, les réglages de chaque requête, "
                         "les points susceptibles de changer, et les quatre requêtes proposées "
                         "(effectif, T60, T62, T63-T64)."),
            ], genre="notion"),
        ]),
    ],
)

# =============================================================================================
# 11 Se préparer aux questions
# =============================================================================================

F11 = Fiche(
    fichier=D + "11 se preparer aux questions.excalidraw",
    titre="Se préparer aux questions : les chiffres et les réponses",
    sous_titre="Fiche 11 du domaine de la gestion administrative (T58 à T65) · à relire avant "
               "une réunion · " + ETAT_AU,
    objectif="les chiffres à connaître par cœur, et une réponse courte et appuyée aux questions "
             "qu'on peut te poser sur le personnel de la CNSS.",
    parties=[
        Partie("Les chiffres à connaître", colonnes=4,
               intro="Tous mesurés sur la base source le 17/11/2026, sauf mention contraire.",
               blocs=[
            chiffre("741", "Permanents présents au 31/12/2024, pour 758 publiés par la CNSS : "
                           "2,2 % d'écart.",
                    "contrôle 1 du 17/11 · annuaire CNSS 2024, tableau 119"),
            chiffre("133 / 44", "Employés hommes et femmes fin 2024 : exactement les chiffres de "
                                "l'annuaire.",
                    "contrôle 1 du 17/11 · annuaire CNSS 2024, tableau 120"),
            chiffre("299 pour 46", "Temporaires fin 2024 : sans fin de contrat saisie, un "
                                   "contractuel reste compté. 47 en ne gardant que les fiches "
                                   "non inactives.",
                    "lot 27, Q153 · points v2, point 20"),
            chiffre("48,3 %", "Taux d'encadrement des permanents en 2024, pour 49,2 % publiés. "
                              "La norme CIPRES est de 10 % au plus.",
                    "contrôle 1 du 17/11 · annuaire CNSS 2024, tableau 124"),
            chiffre("101", "Recrutements de permanents en 2024, pour 104 publiés : 2 des 3 "
                           "manquants sont entrés et partis dans l'année.",
                    "contrôle 2 du 17/11 · lot 26, Q146"),
            chiffre("24", "Départs à la retraite en 2024, lus dans la décision 02, pour 23 "
                          "publiés.",
                    "contrôle 2 du 17/11 · points v2, point 22"),
            chiffre("5 décès", "Comptés en 2024 pour 3 publiés : ils sont datés par la "
                               "liquidation des droits, environ 8 mois après.",
                    "contrôle 2 du 17/11 · lot 26, Q147"),
            chiffre("407 pour 480", "Permanents formés en 2024 : 409 avec la date de fin, 522 "
                                    "inscriptions. L'écart n'est expliqué ni par la date ni par "
                                    "le type d'agent.",
                    "contrôle 3 du 17/11 · points v2, point 4"),
            chiffre("84", "Agents engagés en 2019, fiches inactives, sans aucun départ saisi : "
                          "l'annuaire les compte jusqu'en 2023 et les sort en 2024.",
                    "lot 27, Q154 et Q155 · points v2, point 2"),
            chiffre("817", "Personnes parties sans aucune date : ni décision de départ, ni date "
                           "de décès.",
                    "README, Q44 · points v2, point 25"),
            chiffre("12", "Agents mis en disponibilité et jamais revenus : notre règle les "
                          "efface de toutes les années. Correction à faire.",
                    "lot 28, Q159 · points v2, point 64"),
            chiffre("5 fiches", "Permanents de 60 ans et plus comptés fin 2024, pour aucun "
                                "publié : de vieilles fiches sans état, de 69 à 80 ans.",
                    "lot 27, Q158 · points v2, point 45"),
            chiffre("18/02/2026", "La date de la copie de la base du personnel sur laquelle on "
                                  "travaille.",
                    "points v2, en-tête · README, Q35"),
            chiffre("132", "Unités dont le domaine reste à décider par la CNSS, sur 432 : sans "
                           "elles, T59 est vide.",
                    "lot 23 · points v2, point 48"),
            chiffre("883", "Permanents au 31/12/2025, et 338 temporaires : l'année en cours "
                           "monte vite, avec 159 recrutements.",
                    "contrôles 1 et 2 du 17/11"),
            chiffre("64 points", "La fiche des points à valider : 45 encore ouverts, 19 déjà "
                                 "répondus.",
                    "points_a_valider_v2.md, 17/11"),
        ]),
        Partie("Les questions qu'on peut te poser", colonnes=3, blocs=[
            qr("Pourquoi vos chiffres ne sont-ils pas ceux de l'annuaire ?",
               "Pour 2024, ils le sont presque : 741 permanents pour 758, et les employés à "
               "l'unité près. L'écart des années 2019 à 2023 vient de départs jamais saisis : "
               "les 84 agents engagés en 2019 et 12 agents partis en disponibilité.",
               "contrôle 1 du 17/11 · points v2, points 2 et 64."),
            qr("Pourquoi comptez-vous 299 temporaires pour 46 publiés ?",
               "Aucune fin de contrat n'est saisie dans l'application. Sans date de fin, un "
               "contractuel reste compté pour toujours, et le stock monte chaque année. En ne "
               "gardant que les fiches non inactives, on tombe à 47.",
               "lot 27, Q153 · points v2, point 20."),
            qr("Que sont devenus les 84 agents engagés en 2019 ?",
               "La base ne le dit pas : 84 fiches de permanents, en service depuis 2018, toutes "
               "inactives, sans aucune décision de départ ni fiche plus récente. L'annuaire les "
               "fait entrer en 2019 et les sort tous en 2024.",
               "lot 27, Q154 et Q155 · annuaire CNSS 2024, tableau 121."),
            qr("Pourquoi un taux d'encadrement de 48 % quand la norme CIPRES est de 10 % ?",
               "Parce qu'ici un cadre est un agent des catégories 04, 05, 06 et HC, cadres "
               "moyens compris : c'est ce que la CNSS a retenu le 03/11, et c'est ce qu'elle "
               "publie elle-même (49,2 %). Sans les cadres moyens, le taux tombe à 7,8 %. Le "
               "calcul est juste ; c'est la structure du personnel qu'il interroge.",
               "points v2, points 9 et 34 · annuaire CNSS 2024, tableau 124."),
            qr("Pourquoi ne lisez-vous pas la position de la situation ?",
               "Elle n'est pas datée : l'application l'écrase sur la situation en cours, donc "
               "elle dit l'état d'aujourd'hui, pas celui du 31 décembre. Au 31/12/2024, les 6 "
               "permanents en disponibilité étaient encore en « activité normale ». On lit donc "
               "les absences dans les décisions 07 et 18.",
               "lot 24, Q137 à Q139 · README, G13."),
            qr("Pourquoi les décès arrivent-ils un an en retard ?",
               "La date de décès n'est plus saisie depuis 2010 : 59 dates pour 435 agents "
               "décédés. On prend donc la décision de liquidation des droits, qui vient environ "
               "8 mois après. Sur cinq ans, le total est le bon à deux près, mais décalé d'un an.",
               "points v2, point 3 · lot 26, Q147."),
            qr("D'où viennent les 480 agents formés de l'annuaire ?",
               "On ne le sait pas encore : c'est la question 4 posée à la DRH. La base donne 407 "
               "agents avec la date de début, 409 avec la date de fin, pour 522 inscriptions, et "
               "aucun temporaire ni externe formé. L'annuaire compte sans doute des "
               "participations ou un autre registre : il publie 88 cadres supérieurs formés pour "
               "62 en poste.",
               "contrôle 3 du 17/11 · points v2, point 4 · README, QG4."),
            qr("Pourquoi T59 est-il vide ?",
               "Le domaine d'un agent se lit sur son unité, dans un fichier que la CNSS doit "
               "remplir : 132 unités restent « À décider ». Le code de domaine de la situation "
               "n'est pas utilisable : dans 66 unités sur 139, les agents d'une même unité ont "
               "des codes différents.",
               "lot 23, Q129 · points v2, point 48 · README, G12."),
            qr("Quand aurez-vous T63 et T64 ?",
               "Dès que la DSI aura créé et alimenté la vue de la comptabilité depuis Oracle GL : "
               "la comptabilité de la copie n'est pas valide, la CNSS l'a dit le 07/11. La "
               "structure et la requête sont prêtes ; il faut cinq exercices, et quatre réponses "
               "de la direction financière sur les comptes.",
               "points v2, points 30, 55, 29, 31 · README, requêtes proposées."),
            qr("T60 est-il juste ?",
               "L'égalité tient chaque année : en 2024, 670 au début, 101 recrutements, 24 "
               "retraites, 5 décès, 1 autre sortie, 741 à la fin, et la fin d'une année est le "
               "début de la suivante. Manquent les disponibilités et les détachements, qui ne "
               "sont pas tous saisis.",
               "contrôle 2 du 17/11 · regles_t60.md, G14."),
            qr("Sur quelles données ces chiffres ont-ils été calculés ?",
               "Sur la copie de la base du personnel du 18/02/2026, lue directement. L'entrepôt "
               "n'est pas encore chargé : les contrôles du 17/11 sont les copies exactes des "
               "requêtes proposées, posées sur les mêmes tables de la source.",
               "README, contrôles prêts à lancer (17/11)."),
            qr("Qu'est-ce qui est de votre faute, et qu'est-ce qui vient de la saisie ?",
               "Trois choses sont de notre côté, et seront corrigées : la fiche d'un agent en "
               "disponibilité passe à inactif et notre règle l'efface de toutes les années (12 "
               "agents) ; une fiche sans état est comptée comme présente, ce qui ramène 5 "
               "vieilles fiches de 69 à 80 ans ; un temporaire sans fin de contrat reste compté. "
               "Le reste tient à ce qui n'est pas saisi dans l'application : 817 départs sans "
               "date, aucune date de décès depuis 2010, aucune fin de contrat.",
               "lot 28, Q159 · lot 27, Q158 et Q153 · points v2, points 25, 45, 64."),
        ]),
    ],
)
