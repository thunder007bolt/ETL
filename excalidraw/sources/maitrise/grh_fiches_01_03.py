"""
Fiches 01 à 03 de maîtrise du domaine de la gestion administrative (T58 à T65) : le domaine en
bref, le vocabulaire, et les données de la base du personnel.
"""

from fiches import Bloc, Fiche, Partie
from grh_commun import D, ETAT_AU, constat, mot

# =============================================================================================
# 01 Le domaine en bref
# =============================================================================================

F01 = Fiche(
    fichier=D + "01 le domaine en bref.excalidraw",
    titre="La gestion administrative en bref : le personnel de la CNSS, et ce que la CIPRES "
          "demande",
    sous_titre="Fiche 01 du domaine de la gestion administrative (T58 à T65) · " + ETAT_AU,
    objectif="ce que couvre le domaine, les huit tableaux T58 à T65 et ce que chacun montre, "
             "qui est compté dans l'effectif, dans quel ordre on lit les sources, et où en est "
             "chaque tableau aujourd'hui.",
    parties=[
        Partie("Ce qu'est le domaine", colonnes=3,
               intro="Les sept autres domaines de l'Annuaire CIPRES parlent des assurés. "
                     "Celui-ci parle de la CNSS elle-même : ses agents, ses mouvements de "
                     "personnel, sa formation et ses charges de personnel.",
               blocs=[
            Bloc("Le personnel de la CNSS", [
                ("Ce que c'est", "Les personnes qui travaillent pour la CNSS : les agents "
                                 "permanents, engagés sans limite de durée, et les agents "
                                 "temporaires (les contractuels), engagés pour une durée "
                                 "limitée."),
                ("Un ordre de grandeur", "au 31/12/2024, la CNSS publie 758 permanents et 46 "
                                         "contractuels, soit 804 agents."),
            ], pied="annuaire CNSS 2024, tableau 119"),
            Bloc("Ce que la CIPRES veut voir", [
                ("Ce que c'est", "Huit tableaux, T58 à T65 : combien d'agents, comment ils se "
                                 "répartissent, comment ils entrent et sortent, combien sont "
                                 "formés, et ce que le personnel coûte."),
                ("Pourquoi", "la CIPRES compare les caisses entre elles. Trois tableaux ont une "
                             "norme : T63 au plus 85 %, T64 entre 1 % et 5 %, T65 au plus "
                             "10 %."),
            ], pied="Annuaire CIPRES, p. 17 et p. 50 à 52"),
            Bloc("D'où viennent les chiffres", [
                ("Ce que c'est", "L'application de gestion du personnel de la CNSS, copiée dans "
                                 "le schéma USER_GRH : les fiches des agents, leurs situations, "
                                 "les décisions, les formations, les unités."),
                ("Attention", "la copie lue date du 18/02/2026. La paie n'y est pas, et la "
                              "comptabilité de la copie n'est pas valide : T63 et T64 passeront "
                              "par une vue que la DSI doit créer."),
            ], genre="donnee", pied="points v2, « Pour lire ce document » ; la CNSS, le 07/11"),
        ]),
        Partie("Les huit tableaux de la CIPRES", colonnes=4,
               intro="Quatre d'entre eux (T58, T59, T61, T65) sont la même photo de l'effectif "
                     "au 31 décembre, découpée autrement : une seule requête les sert tous.",
               blocs=[
            Bloc("T58 · L'effectif au 31 décembre", [
                ("Ce qu'il montre", "Les agents présents à la clôture, par catégorie "
                                    "professionnelle, par tranche d'âge de 5 ans (moins de 20 "
                                    "jusqu'à 60 et plus) et par sexe."),
                ("Où on en est", "règles écrites et requête lancée le 17/11 : les permanents "
                                 "retombent sur l'annuaire (741 pour 758), les temporaires non "
                                 "(299 pour 46)."),
            ], ref="T58", genre="etape", etat="en cours"),
            Bloc("T59 · L'effectif par domaine d'activité", [
                ("Ce qu'il montre", "Les agents répartis entre les cinq domaines de la CNSS : "
                                    "recouvrement, prestations, prévention des risques "
                                    "professionnels, action sanitaire et sociale, gestion "
                                    "administrative et financière."),
                ("Où on en est", "bloqué : le domaine se lit par l'unité de l'agent, dans un "
                                 "fichier que la CNSS doit remplir. Tant qu'il est vide, tout "
                                 "T59 est en « Domaine non renseigné »."),
            ], ref="T59", genre="etape", etat="à faire"),
            Bloc("T60 · Le mouvement du personnel", [
                ("Ce qu'il montre", "Sur cinq ans : l'effectif au début de l'année, les entrées "
                                    "(recrutements, autres), les sorties (retraites, décès, "
                                    "autres), l'effectif à la fin."),
                ("Où on en est", "requête réécrite et lancée le 17/11 : début + entrées − "
                                 "sorties = fin chaque année. Les « autres » restent loin de "
                                 "l'annuaire."),
            ], ref="T60", genre="etape", etat="en cours"),
            Bloc("T61 · L'effectif par ancienneté", [
                ("Ce qu'il montre", "Les agents présents au 31 décembre, par tranche "
                                    "d'ancienneté (moins d'un an, 1-4, 5-9… 40 ans et plus) et "
                                    "par sexe."),
                ("Où on en est", "mesuré le 17/11 avec la nouvelle règle : chaque tranche est à "
                                 "4 agents près de l'annuaire, sauf « 40 ans et plus » (6 pour "
                                 "1)."),
            ], ref="T61", genre="etape", etat="en cours"),
            Bloc("T62 · Les agents formés", [
                ("Ce qu'il montre", "Les agents en formation ou formés dans l'année, par "
                                    "catégorie, par sexe et par finalité : avec promotion ou "
                                    "sans promotion."),
                ("Où on en est", "l'écart ne s'explique pas : 407 formés pour 480 publiés, "
                                 "quelle que soit la date retenue. Question posée à la DRH."),
            ], ref="T62", genre="etape", etat="à faire"),
            Bloc("T63 · Les frais de personnel", [
                ("Ce qu'il montre", "Sur cinq ans, les frais de personnel rapportés aux charges "
                                    "de fonctionnement. Norme CIPRES : 85 % au plus."),
                ("Où on en est", "aucun chiffre. Les montants viendront d'Oracle GL, par la vue "
                                 "V_BI_BALANCE_COMPTABLE que la DSI doit créer."),
            ], ref="T63", genre="etape", etat="à faire"),
            Bloc("T64 · La dépense de formation", [
                ("Ce qu'il montre", "Sur cinq ans, la dépense de formation rapportée à la masse "
                                    "salariale (norme : de 1 % à 5 %) et au budget."),
                ("Où on en est", "aucun chiffre, pour la même raison que T63. Les comptes sont "
                                 "donnés par le recueil des besoins ; les gestions restent à "
                                 "confirmer avec la direction financière."),
            ], ref="T64", genre="etape", etat="à faire"),
            Bloc("T65 · Le taux d'encadrement", [
                ("Ce qu'il montre", "Sur cinq ans, la part des cadres dans l'effectif. Norme "
                                    "CIPRES : 10 % au plus."),
                ("Où on en est", "mesuré le 17/11 : 48,3 % en 2024 (358 cadres sur 741) pour "
                                 "49,2 % publiés. La CNSS est très au-dessus de la norme, et "
                                 "l'assume : c'est le même chiffre qu'elle publie."),
            ], ref="T65", genre="etape", etat="en cours"),
        ]),
        Partie("Qui est compté : la nature et la qualité de l'agent", colonnes=4,
               intro="Deux colonnes de la base décident du périmètre. La nature vient de la "
                     "dernière situation de l'agent ; la qualité est sur sa fiche.",
               blocs=[
            Bloc("Permanent", [
                ("Ce que c'est", "Un agent engagé sans limite de durée. C'est le personnel dont "
                                 "parle l'annuaire de la CNSS."),
                ("Combien", "741 au 31/12/2024 dans la base, 758 publiés."),
                ("Comment on le lit", "la nature de sa dernière situation commencée au plus "
                                      "tard à la date d'arrêté (NATURE_EMPLOIE)."),
            ], pied="regles_effectif.md, G1"),
            Bloc("Temporaire (contractuel)", [
                ("Ce que c'est", "Un agent engagé pour une durée limitée, par une décision "
                                 "d'engagement. Il peut être réengagé plusieurs fois."),
                ("Combien", "299 au 31/12/2024 dans la base, 46 publiés : la base ne connaît "
                            "pas la fin des contrats."),
                ("Comment on le lit", "la date d'arrêté tombe entre le début et la fin d'un de "
                                      "ses contrats."),
            ], pied="regles_effectif.md, G7 ; points v2, point 20"),
            Bloc("La qualité : A, B, C", [
                ("Ce que c'est", "A agent de la CNSS · B agent de la fonction publique en poste "
                                 "à la CNSS · C agent affecté."),
                ("Combien", "l'application a connu 40 agents B (3 actifs) et 21 agents C, tous "
                            "inactifs et embauchés avant 2007."),
                ("La règle", "les tableaux CIPRES ne comptent que les agents A. Les autres "
                             "restent dans la table, pour que la CNSS puisse les afficher."),
            ], pied="lot 13, Q86 ; regles_effectif.md, G1"),
            Bloc("Les agents sans situation", [
                ("Ce que c'est", "Un agent dont la fiche existe mais qui n'a aucune situation : "
                                 "on ne connaît ni sa nature, ni sa catégorie, ni son unité."),
                ("Combien", "4 agents (2 hommes, 2 femmes) à chaque 31 décembre."),
                ("La règle", "ils sont comptés dans le total, avec des axes vides : ils "
                             "n'apparaissent donc ni dans le tableau des permanents, ni dans "
                             "celui des temporaires."),
            ], pied="controles du 17/11, C1 ; lot 27, Q157"),
        ]),
        Partie("Les quatre groupes de catégories", colonnes=3,
               intro="La catégorie d'un agent est celle de la grille de salaire, portée par sa "
                     "situation (CATEGORIE). Les quatre groupes de la CNSS sont les colonnes de "
                     "T58, T59, T61 et T62.",
               blocs=[
            Bloc("Employés — catégories 01 et 02", [
                ("Pour la CNSS", "Agents d'exécution."),
                ("Pour la CIPRES", "Employés."),
                ("Le chiffre", "177 au 31/12/2024 dans la base, 177 publiés : le seul groupe "
                               "retrouvé à l'unité près."),
            ], genre="donnee", pied="recueil des besoins v7, RH ligne 1 ; controles du 17/11, C1"),
            Bloc("Agents de maîtrise — catégorie 03", [
                ("Pour la CNSS", "Agents de maîtrise."),
                ("Pour la CIPRES", "Agents de maîtrise."),
                ("Le chiffre", "206 dans la base, 208 publiés."),
            ], genre="donnee", pied="recueil des besoins v7, RH ligne 1 ; controles du 17/11, C1"),
            Bloc("Cadres moyens — catégories 04 et 05", [
                ("Pour la CNSS", "Cadres moyens."),
                ("Pour la CIPRES", "Cadres."),
                ("Attention", "l'atelier de Kaya (2017) rangeait la 5e catégorie en cadres "
                              "supérieurs. Le recueil des besoins v7 a tranché : cadre moyen."),
            ], genre="donnee", pied="recueil des besoins v7, RH ligne 1 ; QG1"),
            Bloc("Cadres supérieurs — catégorie 06, et HC", [
                ("Pour la CNSS", "Cadres supérieurs."),
                ("Pour la CIPRES", "Cadres."),
                ("Attention", "HC veut sans doute dire « hors catégorie ». On la range en cadres "
                              "supérieurs : 2 agents seulement, aux fiches aujourd'hui "
                              "inactives."),
            ], genre="donnee", pied="points v2, point 47 ; lot 26, Q143"),
            Bloc("Les cadres au sens de la CIPRES", [
                ("Ce que c'est", "Les catégories 04, 05, 06 et HC réunies. C'est le numérateur "
                                 "du taux d'encadrement de T65."),
                ("La preuve", "en 2024, 358 cadres sur 741 permanents, soit 48,3 %, pour 49,2 % "
                              "publiés par la CNSS."),
            ], genre="donnee", pied="regles_effectif.md, G6 ; controles du 17/11, C2"),
            Bloc("Les autres codes de catégorie", [
                ("Ce que c'est", "À côté de 01 à 06 et HC, la base porte les codes I à V et 001 "
                                 "à 005."),
                ("La règle", "aucun permanent de 2024 ne les porte. On les suppose venus de la "
                             "fonction publique ; la question est posée à la DSI."),
            ], genre="donnee", pied="points v2, point 41"),
        ]),
        Partie("Les sources, dans l'ordre", colonnes=5, flux=True,
               intro="Quand deux documents ne disent pas la même chose, c'est celui de gauche "
                     "qui l'emporte. Le dernier ne décide de rien : il sert à contrôler nos "
                     "chiffres.",
               blocs=[
            Bloc("1 · La réunion avec la CNSS", [
                ("Ce que c'est", "Ce que la CNSS a dit en réunion, en particulier le "
                                 "03/11/2026 : la nature de l'agent, la présence au 31 "
                                 "décembre, l'âge, l'ancienneté, le domaine par l'unité."),
                ("Pourquoi en premier", "c'est le métier qui parle, sur son propre cas."),
            ], genre="donnee"),
            Bloc("2 · Le recueil des besoins v7", [
                ("Ce que c'est", "Les ateliers de janvier 2026. Sa partie RH a huit lignes, une "
                                 "par tableau de T58 à T65 : les groupes de catégories, les "
                                 "cinq domaines, le détail des « autres », les comptes "
                                 "comptables."),
                ("Pourquoi", "c'est la commande écrite de la CNSS."),
            ], genre="donnee"),
            Bloc("3 · L'atelier de Kaya (2017)", [
                ("Ce que c'est", "La CNSS y a validé les tableaux CIPRES. Pour la GRH : "
                                 "l'année d'une formation est celle de son début ; la note de "
                                 "prise de service et le communiqué nécrologique valent en "
                                 "attendant l'acte."),
                ("Attention", "il est plus ancien : le recueil v7 le corrige parfois."),
            ], genre="donnee"),
            Bloc("4 · L'Annuaire CIPRES", [
                ("Ce que c'est", "Le modèle des tableaux : la définition de chacun (p. 17), sa "
                                 "maquette (p. 50 à 52), ses tranches d'âge et d'ancienneté, "
                                 "ses normes."),
                ("Pourquoi en dernier", "il dit la forme ; la CNSS dit le contenu."),
            ], genre="donnee"),
            Bloc("5 · L'annuaire CNSS 2024", [
                ("Ce que c'est", "La partie IX de l'annuaire statistique de la CNSS, tableaux "
                                 "119 à 126 : l'effectif, les mouvements, l'ancienneté, les "
                                 "formés, le taux d'encadrement, déjà publiés."),
                ("Attention", "il ne décide de rien. Il sert à vérifier que nos règles "
                              "retombent sur les chiffres que la CNSS a déjà publiés."),
            ], genre="donnee"),
        ]),
        Partie("Où en est chaque tableau aujourd'hui", colonnes=3,
               intro="Les trois requêtes proposées (effectif, T60, T62) ont été lancées sur la "
                     "base le 17/11/2026. Voilà ce qu'elles donnent.",
               blocs=[
            constat("1", "Ce qui retombe : les permanents",
                    "L'effectif des permanents, leur âge, leur ancienneté, leur taux "
                    "d'encadrement et les mouvements de T60 sont proches des chiffres publiés "
                    "par la CNSS.",
                    "741 permanents au 31/12/2024 pour 758 publiés ; employés 133 hommes et 44 "
                    "femmes, exactement comme l'annuaire ; T65 à 48,3 % pour 49,2 % ; chaque "
                    "tranche d'ancienneté à 4 agents près.",
                    "C'est la preuve que les règles G1 à G6 sont celles de la CNSS : on peut "
                    "les défendre devant la DRH.",
                    "controles/resultats_2026-11-17.md, C1 à C4"),
            constat("2", "Ce qui retombe : l'équilibre de T60",
                    "Chaque année, effectif de début + entrées − sorties = effectif de fin, et "
                    "la fin d'une année est le début de la suivante.",
                    "2024 : 670 au début, 101 recrutements, 0 autre entrée, 24 retraites, 5 "
                    "décès, 1 autre sortie, 741 à la fin. L'annuaire : 767, 104, 7, 23, 3, 94, "
                    "758.",
                    "Le tableau est cohérent avec lui-même et avec T58. Restent à expliquer les "
                    "« autres » : la base a 0 à 1 disponibilité par an, l'annuaire 2 à 7.",
                    "controles/resultats_2026-11-17.md, contrôle 2"),
            constat("3", "Ce qui ne retombe pas : les temporaires",
                    "Une fin de contrat n'est pas saisie dans l'application. Un temporaire "
                    "engagé reste donc compté pour toujours, et le stock grossit chaque année.",
                    "299 temporaires au 31/12/2024 pour 46 contractuels publiés ; 237 fin 2020, "
                    "338 fin 2025. En ne gardant que les fiches non inactives : 47.",
                    "C'est la question à trancher en priorité avec la DRH (point 20). La "
                    "réponse est un réglage, pas une réécriture.",
                    "controles du 17/11, C1 ; lot 27, Q153 ; points v2, point 20"),
            constat("4", "Ce qui ne retombe pas : T62",
                    "Le nombre d'agents formés en 2024 ne se retrouve pas, quelle que soit la "
                    "date retenue (début ou fin de la formation) et quel que soit le type "
                    "d'agent.",
                    "407 permanents formés (81, 96, 179, 51) pour 480 publiés (132, 142, 118, "
                    "88) ; 522 inscriptions. Aucun temporaire ni externe formé.",
                    "L'annuaire vient sans doute d'un autre registre que l'application : la "
                    "question est posée à la DRH (point 4).",
                    "controles/resultats_2026-11-17.md, contrôle 3 ; points v2, point 4"),
            Bloc("Ce qui n'a aucun chiffre : T59", [
                ("Ce qui manque", "Le domaine de chaque unité. La BI a préparé le fichier "
                                  "« unité → domaine » (432 unités, 157 en vigueur) ; 132 "
                                  "lignes restent « À décider » par la CNSS : agences, bureaux, "
                                  "guichets, directions régionales."),
                ("En attendant", "tout T59 est en « Domaine non renseigné ». La BI ne choisit "
                                 "pas de domaine à la place de la CNSS."),
            ], genre="suite", etat="à faire", pied="points v2, point 48 ; regles_effectif.md, G12"),
            Bloc("Ce qui n'a aucun chiffre : T63 et T64", [
                ("Ce qui manque", "Les montants comptables. La comptabilité de la copie de la "
                                  "base n'est pas valide : la CNSS l'a dit le 07/11/2026."),
                ("En attendant", "tout passera par la vue V_BI_BALANCE_COMPTABLE, que la DSI "
                                 "doit créer et alimenter depuis Oracle GL : un montant par "
                                 "exercice, compte, gestion et section, réalisé et budgété."),
            ], genre="suite", etat="à faire", pied="points v2, points 30 et 55"),
            Bloc("Trois corrections de notre côté", [
                ("Ce qu'on a trouvé", "Le 17/11, trois défauts de nos propres requêtes, pas de "
                                      "la CNSS : une fiche inactive sans départ daté n'est "
                                      "comptée à aucune date, alors que la fiche d'un agent en "
                                      "disponibilité passe à inactif (12 agents effacés de "
                                      "toutes les années) ; une fiche sans état est comptée "
                                      "comme présente, ce qui ramène 5 vieilles fiches de 69 à "
                                      "80 ans ; les temporaires sans fin de contrat."),
                ("Ce qu'on en fait", "ce sont nos erreurs : on les dit et on les corrige avant "
                                     "la réunion, sans les mettre sur le dos de la saisie."),
            ], genre="erreur", etat="confirmée", pied="lot 28, Q159 ; lot 27, Q158 ; points v2, "
                                                      "points 64 et 45"),
            Bloc("L'entrepôt n'est pas chargé", [
                ("Ce que c'est", "Les requêtes proposées sont écrites pour l'entrepôt (les "
                                 "schémas DWH et DTM). Mais l'entrepôt n'a pas encore les "
                                 "données de la GRH."),
                ("Ce qu'on fait", "tout ce qu'on lance aujourd'hui lit la base source "
                                  "USER_GRH : les contrôles sont des copies des requêtes "
                                  "proposées, sur les mêmes colonnes."),
            ], genre="suite", etat="en cours", pied="controles/construire.py ; README de la GRH"),
            Bloc("Les deux chantiers de la CNSS", [
                ("Ce qu'on attend d'elle", "La saisie : les fins de contrat (décision 35), les "
                                           "dates de décès, les départs des cinq dernières "
                                           "années (817 personnes parties sans aucune date), le "
                                           "sort des 84 agents engagés en 2019."),
                ("Et", "le fichier « unité → domaine » pour T59, et la vue comptable pour T63 "
                       "et T64."),
            ], genre="suite", etat="à faire", pied="points v2, points 2, 3, 20, 25, 48, 55"),
        ]),
    ],
)

# =============================================================================================
# 02 Le vocabulaire
# =============================================================================================

F02 = Fiche(
    fichier=D + "02 le vocabulaire.excalidraw",
    titre="Le vocabulaire de la gestion administrative : les mots à connaître",
    sous_titre="Fiche 02 du domaine de la gestion administrative · un bloc par mot, avec un "
               "exemple · " + ETAT_AU,
    objectif="le sens précis de chaque mot du personnel et de chaque mot des tableaux, et les "
             "pièges de ceux qui ne veulent pas dire ce qu'ils ont l'air de dire.",
    parties=[
        Partie("Les gens et leur statut", colonnes=4, blocs=[
            mot("Agent permanent", "Un agent engagé par la CNSS sans limite de durée. Il entre "
                                   "par une embauche, ou par un engagement permanent (décision "
                                   "51).",
                "741 permanents au 31/12/2024 dans la base, 758 publiés par la CNSS.",
                "Un agent est permanent ou temporaire selon sa DERNIÈRE situation : un "
                "temporaire titularisé devient permanent, et change de tableau."),
            mot("Agent temporaire (contractuel)", "Un agent engagé pour une durée limitée, par "
                                                  "une décision d'engagement temporaire "
                                                  "(décision 50). Il peut être réengagé "
                                                  "plusieurs fois.",
                "Un contrat commence le 01/03/2024 ; un nouveau commence le 01/09/2024 : le "
                "premier finit la veille, le 31/08/2024.",
                "La base ne garde pas la fin prévue d'un contrat. Sans départ saisi et sans "
                "contrat suivant, l'agent reste compté : 299 temporaires au lieu de 46."),
            mot("Qualité de l'agent", "Ce que l'agent est pour la CNSS : A agent de la CNSS, B "
                                      "agent de la fonction publique en poste à la CNSS, C "
                                      "agent affecté. C'est sur sa fiche (QUALITE).",
                "Les tableaux CIPRES se lisent avec le filtre qualité A.",
                "Ne pas confondre la qualité (A, B, C) avec la nature de l'emploi (permanent ou "
                "temporaire) : un agent de la fonction publique est permanent, mais pas "
                "compté."),
            mot("Situation", "Une ligne de la carrière d'un agent, avec sa date de début. Elle "
                             "porte la nature de l'emploi, la catégorie, l'unité et la "
                             "qualification.",
                "Pour connaître un agent au 31/12/2024, on prend sa dernière situation "
                "commencée ce jour-là ou avant.",
                "La situation d'un agent qui part n'est jamais close : 691 personnes parties "
                "gardent la position « activité normale »."),
            mot("Catégorie de la grille", "La catégorie de la grille de salaire : 01 à 06, ou "
                                          "HC. Elle est portée par la situation (CATEGORIE).",
                "01 et 02 employés · 03 agents de maîtrise · 04 et 05 cadres moyens · 06 et HC "
                "cadres supérieurs.",
                "C'est elle, et non la qualification, qui donne les colonnes de l'annuaire de "
                "la CNSS : chaque case retombe à 4 agents près."),
            mot("Qualification", "Le niveau de qualification saisi sur la situation "
                                 "(QUAL_CODE). C'est ce que lisaient les anciennes requêtes du "
                                 "DTM.",
                "La table DTM_GRH_RATIO calcule encore le taux d'encadrement avec elle.",
                "Ce n'est pas la catégorie de la grille. T58, T62 et T65 se font avec la "
                "catégorie ; la qualification sera alignée ou abandonnée."),
            mot("Cadre, encadrant", "Un agent des catégories 04, 05, 06 ou HC. Pour la CIPRES, "
                                    "un encadrant est un cadre.",
                "358 cadres sur 741 permanents au 31/12/2024, soit 48,3 %.",
                "La définition par les postes de commandement (le type de fonction R) ne suit "
                "pas l'Annuaire : elle donnait 20,4 %."),
            mot("Unité administrative", "La direction, le service, l'agence, le bureau ou le "
                                        "guichet où travaille l'agent. Son code est UA_CODE, "
                                        "sur la situation.",
                "157 unités sont en vigueur aujourd'hui, sur 432 connues.",
                "10 agents actifs sont rattachés à une unité fermée : ils sont comptés, avec le "
                "domaine de l'ancienne unité."),
        ]),
        Partie("Les absences et les départs", colonnes=4,
               intro="Chacune de ces absences a un sens précis, et la plupart se lisent dans "
                     "les décisions, pas dans la situation.",
               blocs=[
            mot("Disponibilité", "L'agent arrête de travailler pour un temps, à sa demande et "
                                 "sans salaire. Elle commence par la décision 07.",
                "Mise en disponibilité le 01/03/2023, remise en activité le 01/06/2025.",
                "Depuis le 08/11/2026, l'agent en disponibilité SORT de l'effectif : une sortie "
                "à la mise en disponibilité, une entrée au retour, comme le fait l'annuaire de "
                "la CNSS."),
            mot("Remise en activité", "Le retour d'un agent après une disponibilité ou une mise "
                                      "à disposition. C'est la décision 18.",
                "Elle remplit la colonne « autres entrées » de T60, sous le code REM.",
                "Sans décision 18 saisie, l'agent reste en disponibilité pour toujours aux yeux "
                "de la base."),
            mot("Mise à disposition", "L'agent travaille pour un autre organisme en restant "
                                      "agent de la CNSS. C'est la décision 14.",
                "33 agents ont été mis à disposition ; 17 sont revenus, 7 sont partis, 5 "
                "seraient encore dehors.",
                "Il n'y a pas de décision de fin : on ne la compte pas comme une sortie, car "
                "l'annuaire de la CNSS n'a pas de colonne pour elle."),
            mot("Détachement", "Un agent d'une autre administration vient travailler pour un "
                               "temps à la CNSS, ou l'inverse. L'accueil est la décision 37.",
                "L'annuaire de la CNSS compte 2 détachements en 2023 et 1 en 2024.",
                "On n'en trouve aucun dans la base pour un permanent : on ne sait pas où ils "
                "sont saisis. Question posée à la DRH."),
            mot("Radiation", "L'agent est retiré de la liste du personnel. La position « radié » "
                             "est le code X de la situation.",
                "387 personnes portent cette position : 233 temporaires, 94 permanents.",
                "272 d'entre elles n'ont ni décision ni date : une radiation ne suffit donc pas "
                "à dater un départ."),
            mot("Suspension, service militaire", "L'agent suspendu est écarté de son poste en "
                                                 "attendant une décision ; l'agent sous les "
                                                 "drapeaux fait son service militaire.",
                "Positions F et H de la situation.",
                "Elles n'ont pas de paire de décisions début et fin : ces agents restent "
                "comptés dans l'effectif. Au 31/12/2024, un seul permanent est dans ce cas."),
            mot("Départ", "La sortie définitive d'un agent. Elle se lit dans une décision : 02 "
                          "retraite, 34 liquidation des droits après décès, 35 fin de contrat, "
                          "36 démission, 52 rupture de contrat, 54 licenciement.",
                "Retraite au 15/12/2024 : l'agent n'est pas compté au 31/12/2024. Au "
                "15/01/2025 : il est compté.",
                "817 personnes sont parties sans aucune date : ni décision, ni date de décès. "
                "Elles ne sont comptées dans aucune année."),
            mot("Annulation d'une décision", "La décision 40 annule une autre décision, qu'elle "
                                             "désigne par son numéro.",
                "Une retraite annulée par une 40 ne fait pas sortir l'agent.",
                "Il existe aussi une décision de modification (41) : 218 retraites ont été "
                "modifiées après coup. On garde la date de la décision d'origine."),
        ]),
        Partie("Les dates et les décisions", colonnes=4, blocs=[
            mot("Date d'embauche", "La date d'entrée d'un agent, sur sa fiche (DATE_EMBAUCHE). "
                                   "C'est la date retenue pour l'entrée dans l'effectif et pour "
                                   "le recrutement de T60.",
                "Embauché le 10/02/2024 : il est compté au 31/12/2024 et recruté en 2024.",
                "Si elle est vide, on prend la prise de service."),
            mot("Prise de service", "Le jour où l'agent commence vraiment à travailler "
                                    "(DATE_PRISE_SERVICE). C'est de là que part l'ancienneté.",
                "De 2020 à 2025, 5 agents seulement ont pris leur service une autre année que "
                "leur embauche. En 2019, 88 sur 122.",
                "Deux dates, deux usages : l'entrée part de l'embauche, l'ancienneté de la "
                "prise de service. La CNSS doit confirmer."),
            mot("Date de vigueur d'une décision", "La date à laquelle une décision prend effet "
                                                  "(DATE_VIGUEUR). C'est elle qui date le "
                                                  "mouvement, pas la date de signature.",
                "Une décision de retraite en vigueur au 15/01/2025 sort l'agent en 2025.",
                "Des décisions sont datées de l'an 5 ou des années 200 à 209 : des fautes de "
                "frappe à corriger à la source."),
            mot("Date d'arrêté", "Le jour où l'on compte l'effectif : le 31 décembre pour "
                                 "l'annuaire, et au besoin une fin de mois ou de trimestre.",
                "T58 au 31/12/2024 : les agents entrés au plus tard ce jour-là et pas encore "
                "partis.",
                "Tout le domaine tourne autour de cette date : présence, âge, ancienneté, "
                "catégorie et unité se lisent tous à la date d'arrêté."),
            mot("Décision", "L'acte qui change la vie administrative d'un agent : embauche, "
                            "engagement, disponibilité, retraite, licenciement. Chaque décision "
                            "a un code et une date de vigueur.",
                "La décision 02 est l'admission à la retraite ; la 50 l'engagement d'un "
                "temporaire.",
                "Une décision peut viser plusieurs agents à la fois : ils sont alors dans une "
                "liste à part (GRH_DECISION_PERSONNE)."),
            mot("État de la fiche", "L'agent est ACTIF, INACTIF, ou sans état (ETAT_EMPLOYE). "
                                    "Une fiche sans état est en général une embauche récente.",
                "67 fiches sont sans état, jusqu'au 28/01/2026.",
                "« Inactif » ne veut pas dire « parti » : la fiche d'un agent en disponibilité "
                "passe aussi à inactif. 12 agents mis en disponibilité de 2021 à 2025 ont ainsi "
                "disparu de toutes les années."),
            mot("Position de la situation", "L'état de l'agent dans sa situation "
                                            "(STATUT_EMPLOYE) : A activité normale, B détaché, "
                                            "D mis à disposition, E en disponibilité, F "
                                            "suspendu, H sous les drapeaux, R retraité, X "
                                            "radié, Y décédé.",
                "Au 31/12/2024, les 6 permanents en disponibilité selon les décisions étaient "
                "tous en position A.",
                "La position n'est PAS datée : l'application la change sur la situation en "
                "cours au lieu d'ouvrir une nouvelle situation. Elle dit l'état d'aujourd'hui, "
                "pas celui de la date d'arrêté."),
            mot("Agent inactif", "Un agent compté dans l'effectif mais qui ne travaille pas à "
                                 "la date d'arrêté.",
                "On le lit dans les décisions : une mise en disponibilité (07) sans remise en "
                "activité (18) à cette date.",
                "Depuis que la disponibilité fait sortir de l'effectif, cette colonne vaut 0 : "
                "elle ne sert plus qu'avec l'autre réglage."),
        ]),
        Partie("Les mots des tableaux CIPRES", colonnes=4, blocs=[
            mot("Effectif", "Le nombre d'agents présents à la date d'arrêté : entrés au plus "
                            "tard ce jour-là, et pas encore partis.",
                "741 permanents au 31/12/2024.",
                "C'est un stock à une date, pas une moyenne de l'année : dans OBIEE, une année "
                "ne doit jamais additionner ses douze mois."),
            mot("Ancienneté", "Le temps passé en activité à la CNSS, en années pleines, depuis "
                              "la prise de service, moins les périodes où l'agent était parti.",
                "Prise de service le 01/01/2008, disponibilité de 2015 à 2018 : au 31/12/2024, "
                "13 ans d'ancienneté (17 moins 3), tranche 10-14.",
                "Les tranches sont celles de l'Annuaire CIPRES : moins d'un an, 1-4, 5-9… "
                "40 ans et plus. L'ancien calcul s'arrêtait à « 30 et plus »."),
            mot("Domaine d'activité", "Un des cinq métiers de la CNSS : recouvrement, "
                                      "prestations, prévention des risques professionnels, "
                                      "action sanitaire et sociale, gestion administrative et "
                                      "financière. C'est la ligne de T59.",
                "Le domaine d'un agent est celui de son unité, lu dans le fichier tenu par la "
                "CNSS.",
                "Le code de domaine porté par la situation n'est pas fiable : dans 66 unités "
                "sur 139, les agents d'une même unité ont des codes différents."),
            mot("Taux d'encadrement", "La part des cadres dans l'effectif, à la date d'arrêté : "
                                      "nombre de cadres divisé par l'effectif. C'est T65.",
                "48,3 % en 2024 : 358 cadres sur 741 permanents.",
                "La norme CIPRES est de 10 % au plus. La CNSS en est très loin et le publie "
                "elle-même : ce n'est pas une erreur de calcul."),
            mot("Formation avec ou sans promotion", "La finalité d'une formation dans T62 : "
                                                    "« avec promotion » quand l'agent change de "
                                                    "classement après la formation, « sans "
                                                    "promotion » sinon.",
                "L'inscription garde le classement avant et après (CLA_AVANT, CLA_APRES).",
                "CLA_APRES n'a été rempli que 10 fois en 45 ans. En 2024 : aucun formé avec "
                "promotion dans la base, 6 publiés."),
            mot("Perfectionnement", "Une formation qui améliore le métier de l'agent sans "
                                    "donner de diplôme. C'est la catégorie PERF d'une "
                                    "formation.",
                "C'est le cas le plus courant à la CNSS.",
                "Les montées de catégorie qui suivent un perfectionnement viennent de la "
                "carrière, pas de la formation."),
            mot("Formation diplômante", "Une formation qui se termine par un diplôme. C'est la "
                                        "catégorie DIPLO d'une formation.",
                "C'est la seule pour laquelle le classement après la formation est rempli.",
                "Aucune formation diplômante n'a été trouvée pour 2024."),
            mot("Frais de personnel", "Ce que la CNSS paie pour son personnel : les comptes 65 "
                                      "des gestions G et S. C'est le numérateur de T63.",
                "T63 = frais de personnel ÷ charges de fonctionnement. Norme : 85 % au plus.",
                "Ils viennent de la comptabilité, pas de la paie : la paie n'est pas dans la "
                "copie de la base."),
            mot("Charges de fonctionnement", "Ce que la CNSS dépense pour fonctionner : les "
                                             "comptes 61 à 66. C'est le dénominateur de T63.",
                "Aujourd'hui, on les prend sur la gestion G seule.",
                "La grille des ratios de la CNSS dit 61 à 68, le recueil des besoins 61 à 66 : "
                "le recueil l'emporte, mais la question est posée à la direction financière."),
            mot("Masse salariale", "Les comptes 651 et 652. C'est le dénominateur du premier "
                                   "ratio de T64.",
                "T64 = dépense de formation ÷ masse salariale. Norme : entre 1 % et 5 %.",
                "Le recueil des besoins ne dit pas sur quelles gestions la prendre : on fait "
                "comme la formation, G et S."),
            mot("Gestion (G, S)", "La CNSS tient ses comptes en plusieurs gestions : G la "
                                  "gestion générale, S la gestion sociale.",
                "Les frais de personnel se prennent sur G et S.",
                "Deux rubriques d'un même ratio doivent porter sur les mêmes gestions, sinon le "
                "ratio ne veut rien dire."),
            mot("Budget", "Le montant prévu, à côté du montant réalisé. Le second ratio de T64 "
                          "rapporte la dépense de formation au budget.",
                "Aujourd'hui : le budget définitif des comptes 6 et 2, toutes gestions.",
                "Le champ BUDGET d'une formation ne contient pas un montant, mais une année "
                "budgétaire : il ne sert à rien ici."),
        ]),
        Partie("Les mots du projet", colonnes=4, blocs=[
            mot("Cliché", "La photo mensuelle de la base source copiée dans l'entrepôt, "
                          "identifiée par son mois (MMAAAA).",
                "Le cliché 102026 = octobre 2026.",
                "Chaque requête de l'entrepôt reçoit le cliché en paramètre, saisi sans "
                "guillemets."),
            mot("Fait (FAIT_GRH_…)", "Une table de l'entrepôt, copie d'une table de la base du "
                                     "personnel.",
                "DWH.FAIT_GRH_PERSONNE est la copie de GRH_PERSONNE.",
                "Les faits de la GRH ne sont pas encore chargés : tout ce qu'on lance "
                "aujourd'hui lit la base source."),
            mot("DTM", "Une table calculée qui porte les chiffres d'un ou plusieurs tableaux, "
                       "lue ensuite par OBIEE.",
                "DTM_GRH_EFFECTIF alimente T58, T59 et T61 ; DTM_GRH_MOUVEMENT alimente T60.",
                "On ne change jamais ses colonnes ni son grain : on remplace seulement le "
                "calcul, et on ajoute des colonnes à la fin."),
            mot("Lot, requête Qnn", "Un fichier de requêtes de profilage, numéroté dans l'ordre "
                                    "où il a été lancé ; chaque requête a un numéro qui ne sert "
                                    "qu'une fois.",
                "Le lot 28 contient la requête Q159, sur les disponibilités sans retour.",
                "Chaque requête porte son objectif, son résultat et son constat dans son "
                "en-tête : c'est la preuve d'un chiffre."),
            mot("Règle G", "Une façon de compter, écrite et justifiée par une source.",
                "G2 : le permanent présent à la date d'arrêté. G9 : les sorties de T60.",
                "Une règle sans source écrite est marquée « proposition BI » : elle doit être "
                "visée par la DRH."),
            mot("Réglage (les paramètres)", "Un choix qui peut changer, écrit en tête de la "
                                            "requête, dans la partie « paramètres ».",
                "Le réglage de la disponibilité vaut HORS_EFFECTIF : l'agent sort de l'effectif.",
                "Changer un réglage ne demande pas de réécrire la requête : c'est pour cela "
                "qu'une question en attente ne bloque jamais le travail."),
            mot("Point à valider", "Une question posée à la CNSS, numérotée une fois pour "
                                   "toutes, avec ce qu'on fait en attendant la réponse.",
                "Point 20 : compte-t-on un temporaire sans fin de contrat ?",
                "64 points : 45 ouverts, 19 déjà répondus. Chacun dit à quelle direction il "
                "s'adresse : DRH, DFC, DSI ou DPC."),
            mot("Proposition BI", "Notre choix, cohérent avec les données mais sans source "
                                  "écrite dans un document de la CNSS.",
                "Compter une fiche sans état comme encore présente.",
                "À dire comme tel en réunion : c'est ce qui doit être validé en priorité."),
        ]),
    ],
)

# =============================================================================================
# 03 Les données
# =============================================================================================


def table(nom, garde, colonnes, piege):
    return Bloc(nom, [("Ce qu'elle garde", garde), ("Les colonnes à connaître", colonnes),
                      ("Le piège", piege)], genre="donnee")


F03 = Fiche(
    fichier=D + "03 les donnees.excalidraw",
    titre="Les données : les tables et les codes de la base du personnel",
    sous_titre="Fiche 03 du domaine de la gestion administrative · base source, schéma "
               "USER_GRH, copie du 18/02/2026 · " + ETAT_AU,
    objectif="les tables de la base du personnel et ce que chacune garde, comment elles se "
             "relient, les listes de codes à connaître par cœur, ce que la base ne dit pas, et "
             "la différence entre la base source, l'entrepôt et la vue comptable à venir.",
    parties=[
        Partie("Les tables de la base du personnel", colonnes=3,
               intro="Tout le domaine tient dans une poignée de tables. Les trois premières "
                     "portent presque tous les chiffres des tableaux.",
               blocs=[
            table("GRH_PERSONNE",
                  "La fiche de chaque personne connue de la CNSS : une ligne par personne, "
                  "3 180 en tout.",
                  "PERS_ID (son identifiant), SEXE (M ou F), DATE_NAISS, DATE_EMBAUCHE, "
                  "DATE_PRISE_SERVICE, DATE_DECES, ETAT_EMPLOYE (ACTIF, INACTIF ou vide), "
                  "QUALITE (A, B, C).",
                  "L'état n'est pas daté : une fiche inactive ne dit pas quand l'agent est "
                  "parti, ni même s'il est parti (une disponibilité la passe aussi à inactif). "
                  "La date d'état (DATE_ETAT_EMPLOYE) n'est plus remplie depuis 2020."),
            table("GRH_SITUATION",
                  "Une ligne à chaque changement dans la carrière d'un agent : sa nature "
                  "d'emploi, sa catégorie, son unité, sa qualification.",
                  "SITU_ID, PERS_ID, DATE_DEBUT, NATURE_EMPLOIE (PERMANENT ou TEMPORAIRE), "
                  "CATEGORIE (01 à 06, HC), UA_CODE (l'unité), QUAL_CODE (la qualification), "
                  "STATUT_EMPLOYE (la position).",
                  "La position (STATUT_EMPLOYE) n'est pas datée : l'application la modifie sur "
                  "la situation en cours. Et le code de domaine (CODE_DOMAINE_ACT) n'est pas "
                  "fiable : 66 unités sur 139 portent des codes différents selon l'agent."),
            table("GRH_DECISION",
                  "Chaque décision administrative : engagement, disponibilité, retraite, "
                  "licenciement, annulation. C'est là que se lisent les départs et les retours.",
                  "DECI_NO (son numéro), PERS_ID (l'agent visé, s'il est seul), CDEC_CODE (le "
                  "code de la décision), DATE_VIGUEUR (sa date d'effet), NO_DECI_MODIF (la "
                  "décision qu'elle annule ou modifie).",
                  "Des dates impossibles : l'an 5, les années 200 à 209. Et une décision "
                  "annulée par une 40 ne doit jamais être comptée."),
            table("GRH_DECISION_PERSONNE",
                  "La liste des agents d'une décision collective : une décision peut viser "
                  "plusieurs personnes à la fois.",
                  "DECI_NO (la décision), PERS_ID (l'agent).",
                  "Un agent peut être porté soit par la décision elle-même, soit par cette "
                  "liste : il faut toujours lire les deux, sinon on perd des départs (par "
                  "exemple un licenciement collectif)."),
            table("GRH_MOUVEMENT",
                  "Les changements de poste : affectations, mutations, nominations, promotions.",
                  "MVT_ID, PERS_ID, SITU_ID, DATE_DEBUT, NATURE_MVT (le code du mouvement), "
                  "VALIDE (O, E ou A).",
                  "Elle ne contient PAS les recrutements ni les départs : 94 à 95 % de son "
                  "contenu est interne, la CNSS l'a confirmé. C'est pour cela que T60 a été "
                  "réécrit. 91 % des mouvements ne sont jamais validés."),
            table("GRH_FORMATION",
                  "Chaque session de formation organisée par la CNSS, avec ses dates et sa "
                  "catégorie.",
                  "FORM_NO (son numéro), DATE_DEBUT, DATE_FIN, CATEGORIE (PERF "
                  "perfectionnement, DIPLO diplômante, CONF conférence), BUDGET.",
                  "BUDGET contient une ANNÉE budgétaire, pas un montant : il ne sert pas à T64. "
                  "Toutes les formations sont au statut N, et la table des catégories que lit "
                  "le chargement n'existe pas."),
            table("GRH_INSCRIPTION",
                  "Un agent inscrit à une formation : c'est la ligne qui fait un agent formé "
                  "dans T62.",
                  "INS_ID, PERS_ID, FORM_NO, les sept champs de frais, CLA_AVANT et CLA_APRES "
                  "(le classement avant et après la formation).",
                  "Un agent peut avoir plusieurs inscriptions dans l'année : il ne compte "
                  "qu'une fois. CLA_APRES n'a été rempli que 10 fois en 45 ans, et seulement "
                  "pour des formations diplômantes."),
            table("GRH_UNITE_ADMINISTRATIVE",
                  "Les unités de la CNSS : directions, services, agences, bureaux, guichets. "
                  "722 lignes, dont 157 en vigueur.",
                  "UA_CODE, NOM, UA_CODE_SUP (l'unité de rattachement), UA_NATURE, REGION, "
                  "UA_STATUT, UA_DATE_VIGUEUR.",
                  "Seules deux natures existent, « Direction » et « Service » : les agences, "
                  "les bureaux et les directions régionales ne se reconnaissent qu'à leur "
                  "libellé. L'adresse, le téléphone et le compte bancaire ne sont jamais lus."),
            table("GRH_TYPE_MOUVEMENT",
                  "La liste des types de mouvement de l'application : 18 codes, avec leur "
                  "libellé et leur nature. C'est elle qui donne les colonnes de T60 et leur "
                  "détail.",
                  "CODE_TYPE_MVT (le code), LIB_TYPE_MVT (son libellé), CODE_NAT_MVT (INITIAL, "
                  "ENTREES, SORTIES, INTERNE), DOMAINE_TYPE_MVT (ALL ou CNSS).",
                  "Le code NOM (nomination) manque dans la table, alors que les mouvements "
                  "l'utilisent : sa nature reste vide."),
        ]),
        Partie("Comment elles se relient", colonnes=4,
               intro="Quatre identifiants suffisent à relier tout le domaine.",
               blocs=[
            Bloc("PERS_ID · l'agent", [
                ("Ce qu'il relie", "La fiche de l'agent à ses situations, ses décisions, ses "
                                   "mouvements et ses inscriptions aux formations."),
                ("À retenir", "c'est la clé de tout le domaine. Un agent, une fiche, un "
                              "PERS_ID."),
            ], genre="donnee"),
            Bloc("SITU_ID · la situation", [
                ("Ce qu'il relie", "Une situation à l'agent qu'elle décrit, et un mouvement "
                                   "interne à la situation qu'il a créée."),
                ("À retenir", "pour décrire un agent à une date, on prend sa dernière situation "
                              "commencée à cette date ou avant."),
            ], genre="donnee"),
            Bloc("DECI_NO · la décision", [
                ("Ce qu'il relie", "Une décision à la liste de ses agents, et une annulation "
                                   "(décision 40) à la décision qu'elle annule."),
                ("À retenir", "l'agent se lit sur la décision OU dans sa liste. Et toute "
                              "décision doit être testée contre une annulation."),
            ], genre="donnee"),
            Bloc("UA_CODE et FORM_NO", [
                ("Ce qu'ils relient", "UA_CODE relie la situation à l'unité administrative, et "
                                      "l'unité à son unité de rattachement. FORM_NO relie une "
                                      "inscription à sa formation."),
                ("À retenir", "le domaine d'activité de T59 passe par UA_CODE ; l'année d'une "
                              "formation passe par FORM_NO."),
            ], genre="donnee"),
        ]),
        Partie("Les codes de décision à connaître", colonnes=4,
               intro="Treize codes suffisent pour tout le domaine. Chacun est toujours nommé en "
                     "clair quand on en parle à la CNSS.",
               blocs=[
            Bloc("Les entrées : 50, 51, 37", [
                ("", "50 engagement d'un agent temporaire : il commence un contrat · 51 "
                     "engagement d'un agent permanent : la titularisation · 37 accueil d'un "
                     "agent détaché, rangé par l'application parmi les engagements."),
                ("À retenir", "l'embauche d'un permanent se lit sur sa fiche (la date "
                              "d'embauche), pas dans une décision."),
            ], genre="donnee"),
            Bloc("Les départs : 02, 34, 35, 36, 52, 54", [
                ("", "02 admission à la retraite · 34 liquidation des droits après un décès · "
                     "35 indemnité de fin de contrat · 36 démission · 52 rupture du contrat · "
                     "54 licenciement."),
                ("À retenir", "c'est la liste réglable des « décisions de départ ». Pour un "
                              "permanent, on prend le PREMIER départ ; pour un temporaire, le "
                              "dernier de l'année."),
            ], genre="donnee"),
            Bloc("Les absences et les retours : 07, 14, 18", [
                ("", "07 mise en disponibilité · 14 mise à disposition · 18 remise en "
                     "activité."),
                ("À retenir", "la 07 et la 18 vont par paire : c'est la seule absence dont on "
                              "connaisse le début ET la fin. La 14 n'a pas de décision de "
                              "retour."),
            ], genre="donnee"),
            Bloc("L'annulation : 40", [
                ("", "40 annulation d'une décision. Elle désigne la décision annulée par son "
                     "numéro (NO_DECI_MODIF). La décision 41 la modifie au lieu de l'annuler."),
                ("À retenir", "toute décision lue doit d'abord être testée : annulée par une "
                              "40, elle ne compte pas."),
            ], genre="donnee"),
        ]),
        Partie("Les autres listes de codes", colonnes=3, blocs=[
            Bloc("La nature de l'emploi", [
                ("", "PERMANENT · TEMPORAIRE. Elle est portée par la situation "
                     "(NATURE_EMPLOIE)."),
                ("À retenir", "c'est le filtre global des tableaux : un tableau des permanents, "
                              "un tableau des temporaires, chacun juste. Un agent sans "
                              "situation n'est dans ni l'un ni l'autre."),
            ], genre="donnee"),
            Bloc("La qualité de l'agent", [
                ("", "A agent de la CNSS · B agent de la fonction publique en poste à la CNSS · "
                     "C agent affecté. Elle est sur la fiche (QUALITE)."),
                ("À retenir", "les tableaux CIPRES se lisent avec le filtre A. La qualité C "
                              "n'est plus utilisée depuis 2007."),
            ], genre="donnee"),
            Bloc("Les catégories", [
                ("", "01 et 02 employés · 03 agents de maîtrise · 04 et 05 cadres moyens · 06 "
                     "cadres supérieurs · HC hors catégorie, rangé en cadres supérieurs."),
                ("À retenir", "les codes I à V et 001 à 005 existent aussi, pour aucun "
                              "permanent de 2024."),
            ], genre="donnee"),
            Bloc("Les positions (STATUT_EMPLOYE)", [
                ("", "A activité normale · B détaché · D mis à disposition · E en disponibilité "
                     "· F suspendu · H sous les drapeaux · R retraité · X radié · Y décédé."),
                ("À retenir", "aucune de ces positions n'est datée : elles disent l'état "
                              "d'aujourd'hui. Les positions de fin de carrière (R, X, Y) ne "
                              "sont presque jamais saisies."),
            ], genre="donnee"),
            Bloc("Les catégories de formation", [
                ("", "PERF perfectionnement · DIPLO formation diplômante · CONF conférence."),
                ("À retenir", "c'est ce qui se rapproche le plus de la finalité demandée par "
                              "T62. Le vrai critère, « avec promotion », se lit sur le "
                              "classement de l'inscription."),
            ], genre="donnee"),
            Bloc("La validation d'un mouvement", [
                ("", "VALIDE = O, E ou A. Seuls les mouvements O sont repris."),
                ("À retenir", "1 621 mouvements sont à E et 91 % ne sont jamais validés : on ne "
                              "sait pas ce que veulent dire E et A. Question posée à la DSI."),
            ], genre="donnee"),
        ]),
        Partie("Les 18 codes de mouvement de la CNSS", colonnes=3,
               intro="L'application a déjà sa propre liste de types de mouvement. Ses codes "
                     "« ALL » sont les colonnes de la maquette CIPRES, ses codes « CNSS » en "
                     "sont le détail : exactement les rubriques que publie l'annuaire de la "
                     "CNSS. On la reprend telle quelle.",
               blocs=[
            Bloc("INITIAL — 1 code", [
                ("", "DEBUT : l'effectif au début de l'année, c'est-à-dire les agents présents "
                     "au 31 décembre de l'année d'avant."),
                ("Dans T60", "la colonne « effectif début »."),
            ], genre="donnee"),
            Bloc("ENTREES — 4 codes", [
                ("", "RECRU recrutement · REM remise en activité · DETAC détaché · AUTRE_ENT "
                     "autres entrées."),
                ("Dans T60", "RECRU fait la colonne « recrutement » ; les trois autres font "
                             "« autres entrées »."),
            ], genre="donnee"),
            Bloc("SORTIES — 9 codes", [
                ("", "RET retraite · DEC décès · LIC licenciement · DEM démission · DISP "
                     "disponibilité · MISE_DIS mise à disposition · FIN_DETACH fin de "
                     "détachement · FIN_CTRAT fin de contrat · AUTRE_SORT autres sorties."),
                ("Dans T60", "RET et DEC ont leur colonne ; les sept autres font « autres "
                             "sorties ». MISE_DIS et FIN_DETACH ne sont pas remplis "
                             "aujourd'hui."),
            ], genre="donnee"),
            Bloc("INTERNE — 4 codes", [
                ("", "AFF affectation · MUT mutation · PROMO promotion · RETRO rétrogradation."),
                ("Dans T60", "aucune colonne : un changement de poste n'est ni une entrée ni "
                             "une sortie. Ces mouvements restent dans la table, comme avant."),
            ], genre="donnee"),
            Bloc("Le code NOM, absent", [
                ("", "La table des mouvements utilise aussi NOM (nomination), 2 à 52 fois par "
                     "an, mais ce code n'est pas dans la liste de référence."),
                ("Ce que ça change", "sa nature reste vide : une colonne « autres » qui "
                                     "additionnerait tout l'oublierait. Question posée à la "
                                     "DSI."),
            ], genre="donnee"),
            Bloc("Des codes trompeurs", [
                ("", "Dans la table des mouvements, REC veut dire reclassement et RET "
                     "rétrogradation ; DIS veut dire mise à disposition."),
                ("Ce que ça change", "RET n'a donc pas le même sens partout : retraite dans la "
                                     "liste des types de mouvement, rétrogradation dans la "
                                     "table des mouvements. Toujours vérifier la table avant "
                                     "de lire un code."),
            ], genre="donnee", pied="lot 1, Q08 ; points v2, point 42"),
        ]),
        Partie("Ce que la base ne dit pas", colonnes=3,
               intro="Ces manques ne sont pas des erreurs de calcul : ce sont des choses qui "
                     "n'ont jamais été saisies. Il faut les dire comme telles à la CNSS, avec "
                     "leur chiffre.",
               blocs=[
            Bloc("Pas de fin de contrat", [
                ("Ce qui manque", "Aucune date de fin de contrat n'est saisie pour un "
                                  "temporaire. La seule trace est l'indemnité de fin de contrat "
                                  "(décision 35), prise environ 9 mois après la fin réelle."),
                ("Ce que ça change", "299 temporaires au 31/12/2024 pour 46 publiés, et le "
                                     "stock monte chaque année."),
            ], genre="erreur", etat="confirmée", pied="points v2, point 20 ; lot 27, Q153"),
            Bloc("Plus de date de décès depuis 2010", [
                ("Ce qui manque", "La date de décès n'est plus remplie : 59 dates pour 435 "
                                  "agents décédés."),
                ("Ce que ça change", "un décès est daté par la liquidation des droits (décision "
                                     "34), qui vient environ 8 mois et demi plus tard : les "
                                     "décès glissent d'une année sur l'autre, et aucun n'est "
                                     "encore visible pour 2025."),
            ], genre="erreur", etat="confirmée", pied="points v2, point 3 ; lot 7, Q42"),
            Bloc("La position n'est pas datée", [
                ("Ce qui manque", "Quand un agent s'absente ou part, l'application change la "
                                  "position de sa situation en cours au lieu d'en ouvrir une "
                                  "nouvelle."),
                ("Ce que ça change", "la position dit l'état d'aujourd'hui, pas celui de la "
                                     "date d'arrêté. Les absences et les retours se lisent donc "
                                     "dans les décisions (07 et 18), pas dans les situations."),
            ], genre="erreur", etat="confirmée", pied="lot 24, Q137 à Q139 ; regles_effectif.md, "
                                                      "G13"),
            Bloc("817 départs sans aucune date", [
                ("Ce qui manque", "817 personnes sont parties sans décision de départ ni date "
                                  "de décès. Leur fiche est inactive, et c'est tout. En 2020, "
                                  "1 247 états de sortie ont même été changés d'un seul coup."),
                ("Ce que ça change", "elles ne sont comptées dans aucune année. L'effectif des "
                                     "années passées n'est pas reconstituable à l'agent près."),
            ], genre="erreur", etat="confirmée", pied="points v2, points 25 et 37 ; lot 7, Q44"),
            Bloc("Pas de paie", [
                ("Ce qui manque", "La paie du personnel n'est pas dans la copie de la base : "
                                  "ses 22 vues sont invalides."),
                ("Ce que ça change", "la masse salariale de T64 ne vient pas de la paie mais de "
                                     "la comptabilité, par les comptes 651 et 652. Ce point est "
                                     "tranché."),
            ], genre="erreur", etat="écartée", pied="points v2, point 40 ; recueil des besoins "
                                                    "v7, RH ligne 6"),
            Bloc("Pas de type d'unité", [
                ("Ce qui manque", "Les unités n'ont que deux natures, « Direction » et "
                                  "« Service » : rien ne dit qu'une unité est une agence, un "
                                  "bureau ou une direction régionale."),
                ("Ce que ça change", "on les reconnaît à leur libellé, en attendant la réponse "
                                     "de la DSI. Or le recueil des besoins demande chaque "
                                     "tableau par direction, agence et bureau."),
            ], genre="erreur", etat="en partie", pied="points v2, point 56 ; lot 23, Q125"),
        ]),
        Partie("Trois endroits différents, à ne pas confondre", colonnes=3,
               intro="Quand on parle d'« une table », il faut toujours dire laquelle des trois.",
               blocs=[
            Bloc("La base source · USER_GRH", [
                ("Ce que c'est", "La copie de l'application de gestion du personnel de la CNSS, "
                                 "figée au 18/02/2026. C'est la vérité de référence."),
                ("Ce qu'on y fait", "tout le profilage, et aujourd'hui tous les contrôles : les "
                                    "28 lots de requêtes et les trois fichiers de contrôle du "
                                    "17/11 lisent USER_GRH."),
                ("Attention", "elle ne se rafraîchit pas toute seule. La règle du projet veut "
                              "une photo de fin janvier N+1 pour arrêter l'année N."),
            ], genre="donnee", pied="points v2, point 44"),
            Bloc("L'entrepôt · DWH et DTM", [
                ("Ce que c'est", "DWH porte les faits, copies des tables source par cliché "
                                 "mensuel. DTM porte les tables calculées que lit OBIEE : "
                                 "DTM_GRH_EFFECTIF, DTM_GRH_MOUVEMENT, DTM_GRH_FORMATION, "
                                 "DTM_GRH_RATIO, DTM_INDICATEURS_FINANCES_GRH."),
                ("Ce qu'on y fait", "rien pour l'instant : l'entrepôt n'est pas chargé pour la "
                                    "GRH. Les requêtes proposées sont écrites pour lui, et "
                                    "leurs contrôles sont des copies qui lisent la source."),
                ("Attention", "le contenu actuel des tables DTM n'est pas fiable : on ne "
                              "l'analyse pas, on relit seulement leur code pour le corriger."),
            ], genre="donnee", pied="README de la GRH, contrôles du 17/11"),
            Bloc("La vue comptable · V_BI_BALANCE_COMPTABLE", [
                ("Ce que c'est", "Une vue que la DSI doit créer dans la base source et "
                                 "alimenter depuis Oracle GL : un montant par exercice, mois, "
                                 "gestion, compte et section analytique, réalisé et budgété."),
                ("Ce qu'on y fait", "rien encore : elle n'existe pas. C'est elle qui portera "
                                    "T63 et T64, et elle servira aussi à la gestion "
                                    "financière."),
                ("Attention", "la comptabilité présente dans la copie de la base ne doit pas "
                              "être lue : la CNSS l'a déclarée non valide le 07/11/2026."),
            ], genre="donnee", pied="points v2, points 30 et 55"),
        ]),
    ],
)
