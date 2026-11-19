"""
Fiches 06 et 07 du domaine de la gestion administrative : les règles de calcul du personnel —
qui est compté dans l'effectif (G1 à G7, G12, G13) et les réglages de la requête ; puis les
mouvements (G8, G9, G14 à G17), la formation (G10) et les deux ratios de T63 et T64.
"""

from fiches import Bloc, Fiche, Partie
from grh_commun import D, ETAT_AU, constat, mot, qr, question, regle

# =============================================================================================
# 06 Les règles qui comptent les agents (T58, T59, T61, T65)
# =============================================================================================

F06 = Fiche(
    fichier=D + "06 les regles qui comptent les agents.excalidraw",
    titre="Les règles qui comptent les agents",
    sous_titre="Fiche 06 du domaine de la gestion administrative (T58 à T65) · G1 à G7, G12 et "
               "G13, pour T58, T59, T61 et T65 · " + ETAT_AU,
    objectif="qui est compté dans l'effectif à une date, comment chaque agent est rangé par "
             "catégorie, âge, sexe, ancienneté et domaine, d'où vient le taux d'encadrement, et "
             "ce qu'un réglage de la requête permet de changer sans la réécrire.",
    parties=[
        Partie("Qui est compté", colonnes=3,
               intro="L'effectif est une photo : à une date d'arrêté, on dit qui fait partie du "
                     "personnel. T58, T59, T61 et T65 en découlent tous, et T60 aussi, qui "
                     "compare deux photos.",
               blocs=[
            regle("G1", "La nature de l'agent et sa qualité", "prouvé",
                  "Chaque agent est rangé d'après sa dernière situation commencée au plus tard à "
                  "la date d'arrêté : permanent ou temporaire. Les tableaux CIPRES ne gardent que "
                  "les agents de la CNSS (qualité A) ; la fonction publique (B) et les agents "
                  "affectés (C) restent dans la table, écartés par un filtre.",
                  "La CNSS veut un tableau des permanents et un tableau des temporaires (réunion "
                  "du 03/11/2026, recueil des besoins v7). La nature est dans la situation, pas "
                  "sur la fiche de l'agent.",
                  "au 31/12/2024 : 741 permanents (annuaire CNSS 2024 : 758) et 299 temporaires "
                  "(publié : 46). 4 agents sont comptés sans nature, faute de situation "
                  "utilisable : ils ne sont dans aucun des deux tableaux.",
                  "et les agents de la fonction publique ?",
                  "Ils travaillent à la CNSS sans être son personnel : la table les garde, le "
                  "tableau CIPRES les écarte avec le filtre qualité A.",
                  "regles_effectif.md, G1 · contrôle du 17/11, C1 · points v2, point 6 · fiche "
                  "des règles, règles 78 et 79"),
            regle("G2", "Le permanent présent à la date d'arrêté", "prouvé",
                  "Deux conditions : il est entré au plus tard à la date d'arrêté (date "
                  "d'embauche, à défaut prise de service), et il n'est pas encore parti (sa "
                  "fiche n'est pas INACTIF, ou son premier départ est daté après la date "
                  "d'arrêté).",
                  "La base ne garde aucun effectif : on le reconstruit agent par agent, avec sa "
                  "date d'entrée et sa date de départ.",
                  "741 permanents au 31/12/2024 pour 758 publiés, soit 2 % d'écart ; les "
                  "employés tombent exactement : 133 hommes et 44 femmes, comme l'annuaire.",
                  "et un agent parti sans date de départ ?",
                  "Il n'est compté à aucune date. 817 fiches sont dans ce cas : on demande à la "
                  "DRH de saisir la date et le motif des départs des cinq dernières années "
                  "(point 25).",
                  "regles_effectif.md, G2 · contrôle du 17/11, C1 · points v2, points 7 et 25"),
            regle("G2", "Les six décisions de départ", "prouvé",
                  "Un départ est une décision : 02 l'admission à la retraite, 34 la liquidation "
                  "des droits après un décès, 35 la fin de contrat, 36 la démission, 52 la "
                  "rupture du contrat, 54 le licenciement. Annulée par une décision 40, elle ne "
                  "compte pas. Sans décision, c'est la date de décès de la fiche.",
                  "Les départs ne sont écrits nulle part ailleurs : la situation de l'agent n'est "
                  "jamais fermée, et la table des mouvements n'a ni retraite ni décès.",
                  "de 2020 à 2024, la base trouve 39, 31, 29, 30 et 24 retraites, l'annuaire 43, "
                  "31, 26, 32 et 23 ; les démissions tombent juste et les licenciements à un "
                  "près.",
                  "pourquoi la liquidation des droits pour un décès ?",
                  "Parce que la date de décès n'est plus saisie depuis 2010 : 59 dates pour 435 "
                  "agents décédés. La décision 34 vient environ huit mois après le décès "
                  "(point 3).",
                  "regles_effectif.md, G2 · points v2, points 3 et 22 · fiche des règles, "
                  "règle 81"),
            regle("G2", "La fiche sans état", "ouvert",
                  "Une fiche dont l'état est vide compte comme encore là. Ce sont en général des "
                  "embauches récentes, que l'application n'a pas encore passées à ACTIF.",
                  "Avant le 05/11/2026, la requête exigeait une fiche ACTIF : ces agents "
                  "n'étaient comptés nulle part, et les recrutements de fin d'année se "
                  "perdaient.",
                  "67 fiches sans état. Mais 5 sont de vieilles fiches jamais fermées : elles "
                  "ajoutent 5 permanents de 69 à 80 ans au 31/12/2024, alors que l'annuaire n'en "
                  "publie aucun de 60 ans et plus. Sans elles : 736 permanents.",
                  "comment on corrige ?",
                  "Une fiche sans état ne comptera que si l'entrée est récente. La DRH doit dire "
                  "au bout de combien de temps une fiche sans état est celle d'un agent parti "
                  "(point 45).",
                  "regles_effectif.md, G2 · lot 27, Q158 · points v2, point 45"),
            regle("G7", "Le temporaire présent, et la fin d'un contrat", "ouvert",
                  "Un temporaire est compté si la date d'arrêté tombe dans un de ses contrats. "
                  "Un contrat commence à la décision 50, l'engagement d'un temporaire. Il finit "
                  "au premier départ saisi après ce début ; sinon la veille du contrat suivant ; "
                  "sinon aucune fin n'est connue et l'agent reste compté.",
                  "La base ne garde pas la fin prévue d'un contrat, et la BI ne fixe pas une "
                  "durée à la place de la CNSS (réunion du 03/11/2026 : l'ancien réglage de 18 "
                  "mois a été supprimé).",
                  "299 temporaires au 31/12/2024 pour 46 contractuels publiés, et le stock monte "
                  "chaque année : 237 fin 2020, 338 fin 2025. En ne gardant que les fiches non "
                  "inactives : 47 fin 2024.",
                  "pourquoi six fois trop ?",
                  "Parce que les fins de contrat ne sont pas saisies : presque personne ne sort. "
                  "C'est le premier point à trancher avec la DRH (point 20).",
                  "regles_effectif.md, G7 · lot 27, Q153 · points v2, point 20"),
            regle("G7", "Le temporaire sans engagement", "prouvé",
                  "Un temporaire sans aucune décision d'engagement (50) n'est compté à aucune "
                  "date : on ne connaît pas le début de son contrat.",
                  "Un contrat commence à une décision ; sans elle, la BI n'a pas de date de "
                  "départ pour le compter.",
                  "230 temporaires sont dans ce cas. 227 ont une fiche inactive et sont entrés "
                  "avant 2015 : ce sont de vieilles fiches. Les 3 autres sont des fiches sans "
                  "état (lot 26, Q151).",
                  "perd-on des contractuels en poste ?",
                  "Non : aucun d'eux n'est un agent d'aujourd'hui. La question est devenue sans "
                  "objet le 17/11/2026 (point 61).",
                  "lot 26, Q151 · points v2, point 61 (déjà répondu)"),
        ]),
        Partie("Comment on range chaque agent", colonnes=3,
               intro="Chaque agent compté tombe dans une case : une catégorie, une tranche "
                     "d'âge, un sexe, une tranche d'ancienneté, un domaine d'activité.",
               blocs=[
            regle("G3", "La catégorie et son regroupement", "prouvé",
                  "La catégorie est celle de la dernière situation : la catégorie de la grille "
                  "de salaire. Regroupement : 01 et 02 employés, 03 agents de maîtrise, 04 et 05 "
                  "cadres moyens, 06 et HC cadres supérieurs. Pour la CIPRES, les cadres sont "
                  "04, 05, 06 et HC.",
                  "C'est le découpage du recueil des besoins v7 (RH ligne 1), et ce sont les "
                  "colonnes de T58, T59, T61 et T62.",
                  "au 31/12/2024 : 177 employés (publié 177), 206 agents de maîtrise (208), 301 "
                  "cadres moyens (311), 57 cadres supérieurs (62). Le tableau 126 de l'annuaire "
                  "retombe sur son tableau 119 avec ce regroupement.",
                  "et la qualification de l'agent ?",
                  "Elle ne sert plus : l'annuaire publie bien la catégorie de la grille. La table "
                  "DTM_GRH_RATIO, qui calcule encore le taux avec la qualification, sera alignée "
                  "ou remplacée.",
                  "regles_effectif.md, G3 · contrôle du 17/11, C1 · points v2, point 1 (déjà "
                  "répondu)"),
            regle("G3", "La catégorie HC", "ouvert",
                  "HC, sans doute « hors catégorie », est rangé en cadres supérieurs, donc dans "
                  "les cadres de la CIPRES.",
                  "Avec ce rangement, le tableau 126 de l'annuaire CNSS 2024 retombe sur son "
                  "tableau 119, HC compris.",
                  "2 agents seulement ont la catégorie HC : deux permanents dont la fiche est "
                  "aujourd'hui inactive (lot 26, Q143).",
                  "le chiffre change-t-il si on les met ailleurs ?",
                  "Non, l'effet est nul aujourd'hui. Mais la DRH doit confirmer qu'un agent hors "
                  "catégorie est un cadre supérieur (point 47).",
                  "lot 26, Q143 · points v2, points 41 et 47"),
            regle("G4", "L'âge", "prouvé",
                  "L'âge est le nombre d'années pleines à la date d'arrêté. Les tranches sont "
                  "celles de l'Annuaire CIPRES : moins de 20 ans, 20-24… 55-59, 60 et plus. Sans "
                  "date de naissance, ou avec un âge hors de 0 à 119 ans, l'agent reste compté, "
                  "avec une tranche vide.",
                  "Avant, un âge inconnu faisait disparaître l'agent du tableau : on perdait un "
                  "agent pour une date manquante.",
                  "au 31/12/2024, les trois premières tranches sont exactes (12, 28 et 70 "
                  "agents) et les autres à 9 près. Seule vraie différence : 5 permanents de 60 "
                  "ans et plus, pour 0 publiés — les 5 vieilles fiches sans état.",
                  "et l'âge atteint dans l'année, comme pour les prestations ?",
                  "Non. La GRH compte les années pleines à la date d'arrêté ; les prestations "
                  "prennent l'année moins l'année de naissance (règle R8). Les deux domaines ne "
                  "comptent pas pareil.",
                  "regles_effectif.md, G4 · contrôle du 17/11, C4 · README GRH, G4"),
            regle("", "Le sexe", "prouvé",
                  "Le sexe se lit sur la fiche de l'agent : M pour les hommes, F pour les "
                  "femmes. Un autre code ne compte que dans le total.",
                  "Tous les tableaux du personnel séparent les hommes et les femmes, dans chaque "
                  "catégorie.",
                  "au 31/12/2024 : 402 hommes et 339 femmes chez les permanents, pour 412 et 346 "
                  "publiés. Chez les employés, les deux chiffres sont exacts : 133 et 44.",
                  "d'où vient l'écart ?",
                  "Pas du sexe : il suit l'écart de l'effectif (17 agents), qui se concentre sur "
                  "les cadres moyens et les cadres supérieurs.",
                  "regles_effectif.md, terme 5 · contrôle du 17/11, C1"),
            regle("G5", "L'ancienneté et les périodes retirées", "prouvé",
                  "L'ancienneté est le temps passé en activité à la CNSS, en années pleines, de "
                  "la prise de service (à défaut l'embauche) à la date d'arrêté, moins les "
                  "périodes où l'agent était parti : une disponibilité (décision 07) jusqu'à la "
                  "remise en activité (18), et un départ suivi d'un nouvel engagement (50 ou 51).",
                  "Le recueil des besoins v7 (RH ligne 4) demande de tenir compte des sorties et "
                  "des retours ; la réunion du 03/11/2026 a fixé l'ordre des deux dates.",
                  "première mesure avec cette règle, le 17/11/2026 : chaque tranche de T61 est à "
                  "4 agents près de l'annuaire. La seule exception, 40 ans et plus (6 pour 1 "
                  "publié), vient des 5 vieilles fiches sans état.",
                  "pourquoi la prise de service avant l'embauche ?",
                  "Parce que l'ancienneté se compte depuis le jour où l'agent a commencé à "
                  "travailler. L'entrée dans l'effectif, elle, part de l'embauche : c'est le "
                  "point 50, posé à la DRH.",
                  "regles_effectif.md, G5 · contrôle du 17/11, C3 · points v2, point 8 (déjà "
                  "répondu)"),
            regle("G12", "Le domaine d'activité, par l'unité", "ouvert",
                  "Le domaine d'un agent est celui de son unité administrative à la date "
                  "d'arrêté (UA_CODE de la dernière situation), lu dans la correspondance unité "
                  "→ domaine tenue par la CNSS. Cinq domaines : recouvrement ; prestations ; "
                  "prévention des risques professionnels ; action sanitaire et sociale ; gestion "
                  "administrative et financière.",
                  "Le code de domaine de la situation n'est pas fiable : dans 66 unités sur 139, "
                  "les agents d'une même unité portent des codes différents.",
                  "le fichier compte 432 unités ; 132 lignes restent « À décider » (agences, "
                  "bureaux, guichets, directions régionales). Tant qu'il n'est pas rendu, tout "
                  "T59 est en « Domaine non renseigné ».",
                  "pourquoi la BI ne choisit-elle pas à leur place ?",
                  "Parce que rattacher une agence à un domaine est une décision de la CNSS, pas "
                  "une lecture de la base (point 48).",
                  "regles_effectif.md, G12 · lot 23, Q129 · points v2, point 48"),
        ]),
        Partie("Les deux chiffres qu'on en tire", colonnes=2, blocs=[
            regle("G6", "Le taux d'encadrement (T65)", "prouvé",
                  "Taux d'encadrement = nombre de cadres (catégories 04, 05, 06 et HC) ÷ "
                  "effectif, à chaque 31/12, sur les cinq derniers exercices. La norme CIPRES "
                  "est de 10 % au plus.",
                  "Un encadrant est un cadre, moyen ou supérieur : c'est ce qu'a dit la réunion "
                  "du 03/11/2026, et c'est ce que publie l'annuaire de la CNSS.",
                  "48,3 % en 2024 (358 cadres sur 741 permanents) pour 49,2 % publiés ; environ "
                  "42 % de 2020 à 2023 ; 50,1 % fin 2025. Avec les seuls cadres supérieurs, on "
                  "tomberait à 7,8 %.",
                  "la CNSS est-elle hors norme ?",
                  "Oui, très au-dessus des 10 % — et l'annuaire de la CNSS publie le même ordre "
                  "de grandeur. Le tableau montre la situation, il ne la corrige pas.",
                  "regles_effectif.md, G6 · contrôle du 17/11, C2 · points v2, points 9 et 34 "
                  "(déjà répondus)"),
            regle("G13", "Les agents actifs et les agents inactifs", "proposé",
                  "Parmi les agents comptés, est inactif celui qui est en disponibilité à la "
                  "date d'arrêté : une décision 07 sans décision 18 après. Les autres sont "
                  "actifs. On ne lit plus le statut de la situation (détaché, mis à disposition, "
                  "suspendu, sous les drapeaux).",
                  "Le statut de la situation n'est pas daté : l'application le modifie sur la "
                  "situation en cours. Il dit donc l'état d'aujourd'hui, pas celui de la date "
                  "d'arrêté.",
                  "au 31/12/2024, les 6 permanents en disponibilité d'après les décisions "
                  "avaient tous le statut « activité normale » (lot 24). Depuis que la "
                  "disponibilité sort de l'effectif, la colonne des inactifs vaut 0 : le "
                  "contrôle du 17/11 ne trouve aucun inactif.",
                  "et un agent détaché ou suspendu ?",
                  "Il reste compté, et compté actif : ces absences n'ont pas de paire de "
                  "décisions début et fin. Au 31/12/2024, un seul permanent est dans ce cas "
                  "(point 14).",
                  "regles_effectif.md, G13 · lot 24, Q137 à Q139 · points v2, points 14 et 46"),
        ]),
        Partie("La date d'arrêté et la photo qui fait foi", colonnes=2, blocs=[
            mot("La date d'arrêté",
                "Le jour où l'on photographie l'effectif. Pour l'annuaire, le 31 décembre ; la "
                "requête sait aussi photographier une fin de mois. Tous les tableaux du "
                "personnel sont des photos à cette date, et T60 compare celle du 31/12 d'avant "
                "et celle du 31/12.",
                "une retraite qui prend effet le 15/12/2024 : l'agent n'est pas compté au "
                "31/12/2024. Si elle prend effet le 15/01/2025, il est compté.",
                "Ne pas confondre avec la date de la copie de la base : la date d'arrêté est "
                "une date de calcul, la copie est l'état de la base le jour où on l'a prise."),
            regle("", "La photo de fin janvier N+1", "proposé",
                  "Les chiffres de l'année N viennent de la photo de la base prise à la fin de "
                  "janvier N+1 : les directions font leurs corrections de l'année N dans "
                  "l'application avant cette date, et seul ce qui est saisi dans l'application "
                  "compte.",
                  "Sans date d'arrêt commune, le même tableau change à chaque nouveau "
                  "chargement. C'est la règle 2 de la fiche des règles CIPRES, valable pour tous "
                  "les tableaux, prestations comprises.",
                  "la copie dont on dispose date du 18/02/2026, et il n'existe aucune photo de "
                  "2021 à 2024. Recalculés sur cette copie, les permanents donnent 769, 734, "
                  "703, 670 et 741 de 2020 à 2024, pour 869, 834, 806, 767 et 758 publiés.",
                  "reprend-on les chiffres déjà publiés pour les années passées ?",
                  "C'est la question posée à la DRH avec la DPC (point 57). En attendant, on "
                  "recalcule toutes les années sur la copie du moment, et on l'annonce.",
                  "fiche des règles, règles 1 à 4 · points v2, points 44 et 57"),
        ]),
        Partie("Les réglages : ce qui se change sans réécrire la requête", colonnes=3,
               intro="Une question en attente ne bloque jamais le travail : elle devient un "
                     "réglage en tête de la requête (la partie « parametres »). On applique une "
                     "valeur par défaut, et la réponse de la CNSS se posera en une ligne. Voici "
                     "les sept réglages de l'effectif, leur valeur retenue et pourquoi.",
               blocs=[
            Bloc("categorie_05 · où va la catégorie 05", [
                ("Ce qu'il change", "Le groupe de la catégorie 05 : cadres moyens ou cadres "
                                    "supérieurs. Dans la requête de l'effectif, c'est la ligne "
                                    "05 de la table des regroupements ; dans celle de T62, le "
                                    "réglage categorie_05."),
                ("La valeur retenue", "MOYEN : la catégorie 05 est un cadre moyen."),
                ("Pourquoi", "Le recueil des besoins v7 (RH ligne 1) range 04 et 05 en cadres "
                             "moyens, et l'annuaire CNSS 2024 le confirme : avec ce découpage, "
                             "ses tableaux 119 et 126 donnent les mêmes 177, 208, 311 et 62 "
                             "agents."),
                ("L'autre valeur", "SUPERIEUR, comme l'atelier de Kaya en 2017. Dans les deux "
                                   "cas l'agent reste un cadre : le taux d'encadrement ne bouge "
                                   "pas."),
            ], genre="notion", etat="prouvé",
                pied="points v2, point 1 (déjà répondu) · README GRH, QG1"),
            Bloc("disponibilite · l'agent en disponibilité", [
                ("Ce qu'il change", "Ce que devient un agent mis en disponibilité (décision 07) "
                                    "et pas encore revenu (décision 18)."),
                ("La valeur retenue", "HORS_EFFECTIF : il sort de l'effectif, et T60 montre une "
                                      "sortie DISP puis une entrée REM à son retour. Même valeur "
                                      "dans la requête de l'effectif et dans celle de T60, sinon "
                                      "la fin de T60 ne retombe plus sur T58."),
                ("Pourquoi", "Le tableau 121 de l'annuaire CNSS 2024 range la disponibilité dans "
                             "les sorties et la remise en activité dans les entrées, et son "
                             "égalité début + entrées − sorties = fin tient chaque année de 2015 "
                             "à 2024. La CNSS l'avait dit en réunion le 03/11."),
                ("L'autre valeur", "PRESENT : l'agent reste compté, inactif. L'effectif du "
                                   "31/12/2024 passerait de 741 à 747."),
            ], genre="notion", etat="prouvé",
                pied="tranché le 08/11/2026 · regles_effectif.md, réglages · points v2, point 5 "
                     "(déjà répondu)"),
            Bloc("fin_contrat_inconnue · le contrat sans fin", [
                ("Ce qu'il change", "Le sort d'un temporaire dont aucune fin de contrat n'est "
                                    "connue."),
                ("La valeur retenue", "PRESENT : il reste compté. La BI ne fixe pas de durée de "
                                      "contrat à la place de la CNSS (réunion du 03/11/2026)."),
                ("Ce que disent les chiffres", "299 temporaires fin 2024 pour 46 publiés. Avec "
                                               "FICHE, on ne garde que ceux dont la fiche n'est "
                                               "pas inactive : 47 fin 2024. Une durée de 12, 18 "
                                               "ou 24 mois donnerait 56 à 61."),
                ("L'autre valeur", "FICHE. C'est celle qu'on proposera à la DRH en attendant la "
                                   "saisie des fins de contrat (point 20)."),
            ], genre="notion", etat="ouvert",
                pied="lot 27, Q153 · points v2, point 20"),
            Bloc("ordre_date_anciennete · d'où part l'ancienneté", [
                ("Ce qu'il change", "La date d'où part l'ancienneté de T61."),
                ("La valeur retenue", "PRISE_SERVICE : la prise de service, à défaut "
                                      "l'embauche (réunion du 03/11/2026)."),
                ("Pourquoi", "L'ancienneté se compte depuis le jour où l'agent a commencé à "
                             "travailler. Attention : l'entrée dans l'effectif, elle, part de la "
                             "date d'embauche."),
                ("Ce que ça change", "De 2020 à 2025, 5 agents seulement ont pris leur service "
                                     "une autre année que leur embauche ; en 2019, 88 sur 122, "
                                     "dont les 84 engagés de 2019."),
            ], genre="notion", etat="prouvé",
                pied="lot 26, Q144 · points v2, points 8 (déjà répondu) et 50"),
            Bloc("retirer_interruptions · les absences de l'ancienneté", [
                ("Ce qu'il change", "Retirer ou non de l'ancienneté les périodes où l'agent "
                                    "était parti."),
                ("La valeur retenue", "OUI : on retire les disponibilités (de la décision 07 à "
                                      "la 18) et les départs suivis d'un nouvel engagement (50 "
                                      "ou 51)."),
                ("Pourquoi", "Le recueil des besoins v7 (RH ligne 4) demande de prendre en "
                             "compte les sorties et les retours."),
                ("La preuve", "Avec cette règle, chaque tranche de T61 est à 4 agents près de "
                              "l'annuaire au 31/12/2024. Exemple : prise de service en 2008, "
                              "disponibilité de 2015 à 2018 ; fin 2024, l'agent a 13 ans "
                              "d'ancienneté et non 17."),
            ], genre="notion", etat="prouvé",
                pied="regles_effectif.md, G5 · contrôle du 17/11, C3"),
            Bloc("periodicite · les dates d'arrêté", [
                ("Ce qu'il change", "Les dates d'arrêté que la requête calcule."),
                ("La valeur retenue", "ANNUELLE : le 31/12 de chaque exercice clos, comme "
                                      "aujourd'hui."),
                ("L'autre valeur", "MENSUELLE : ajoute la fin de chaque mois clos de l'année "
                                   "passée et de l'année en cours, comme le demande le recueil "
                                   "des besoins v7 (une fin de trimestre est une fin de mois)."),
                ("À faire avant", "Régler l'effectif en « dernière valeur » sur le temps dans "
                                  "OBIEE, avec Jules : sinon une année additionnerait ses douze "
                                  "mois. La DRH dira si les fins de mois suffisent (point 58)."),
            ], genre="notion", etat="ouvert",
                pied="regles_effectif.md, réglages · points v2, point 58"),
            Bloc("decisions_depart · ce qui fait sortir un agent", [
                ("Ce qu'il change", "La liste des décisions qui font sortir un agent de "
                                    "l'effectif, et, dans T60, la colonne de sa sortie."),
                ("La valeur retenue", "Six codes : 02 retraite, 34 liquidation des droits après "
                                      "décès, 35 fin de contrat, 36 démission, 52 rupture du "
                                      "contrat, 54 licenciement."),
                ("La preuve", "Face à l'annuaire CNSS 2024, les retraites tombent à 1 à 4 près "
                              "par an, les démissions juste, les licenciements à 1 près."),
                ("Ce qui peut changer", "Un code en plus ou en moins, sur un mot de la DRH. La "
                                        "rupture du contrat (52) est rangée en fin de contrat : "
                                        "proposition BI, un seul cas dans la base. Restent à "
                                        "ranger les radiations (point 21)."),
            ], genre="notion", etat="prouvé",
                pied="regles_effectif.md, réglages · points v2, point 21"),
        ]),
        Partie("Si on te demande", colonnes=3, blocs=[
            qr("Votre effectif n'est pas le nôtre : qui a raison ?",
               "Les deux comptent la même chose, à 17 agents près sur 758 en 2024. L'écart "
               "vient de départs non saisis dans l'application : 5 vieilles fiches sans état "
               "et des agents en disponibilité effacés par notre règle. On les corrige.",
               "Contrôle du 17/11/2026, C1 et C2 ; lots 27 et 28."),
            qr("Pourquoi 299 contractuels au lieu de 46 ?",
               "Parce que la base ne garde aucune fin de contrat : un temporaire engagé y reste "
               "pour toujours. En ne gardant que les fiches non inactives, on retrouve 47 fin "
               "2024. La vraie solution est de saisir la décision 35.",
               "Lot 27, Q153 ; point 20 des points à valider."),
            qr("Qui a décidé qu'un agent en disponibilité ne compte plus ?",
               "L'annuaire de la CNSS lui-même : son tableau 121 met la disponibilité en sortie "
               "et la remise en activité en entrée, et son total tient chaque année de 2015 à "
               "2024. La CNSS l'avait dit en réunion le 03/11. C'est un réglage : la DRH peut "
               "revenir en arrière.",
               "Annuaire CNSS 2024, tableau 121 ; décision du 08/11/2026 (QG6)."),
        ]),
    ],
)

# =============================================================================================
# 07 Les règles des mouvements, de la formation et des finances (T60, T62, T63, T64)
# =============================================================================================

F07 = Fiche(
    fichier=D + "07 les regles des mouvements et de la formation.excalidraw",
    titre="Les règles des mouvements, de la formation et des finances",
    sous_titre="Fiche 07 du domaine de la gestion administrative (T58 à T65) · G8, G9 et G14 à "
               "G17 pour T60, G10 pour T62, F1 à F4 pour T63 et T64 · " + ETAT_AU,
    objectif="comment T60 se lit entre deux photos de l'effectif, ce qu'est une entrée et une "
             "sortie et comment on détaille les « autres », qui compte comme agent formé, et "
             "d'où viendront les montants et les deux ratios de T63 et T64.",
    parties=[
        Partie("Les mouvements du personnel (T60)", colonnes=3,
               intro="T60 suit les entrées et les sorties sur cinq ans. On ne le lit pas dans la "
                     "table des mouvements de l'application : elle ne contient presque que des "
                     "changements de poste, ni recrutement, ni retraite, ni décès.",
               blocs=[
            regle("G14", "Les deux photos de l'exercice", "prouvé",
                  "L'effectif au début de l'année N, ce sont les agents présents au 31/12 de "
                  "N-1 ; à la fin, ceux présents au 31/12 de N. Être présent suit exactement les "
                  "règles de T58 (G1, G2, G7), avec les mêmes réglages. Conséquence : début + "
                  "entrées − sorties = fin, toujours.",
                  "Un mouvement n'est pas une ligne saisie quelque part : c'est la différence "
                  "entre deux photos. C'est la seule façon d'avoir un tableau qui tombe juste et "
                  "dont la fin est l'effectif de T58.",
                  "le contrôle du 17/11/2026 le vérifie de 2020 à 2025 : l'égalité tient chaque "
                  "année, et la fin d'une année est le début de la suivante. 2024 : 670 au "
                  "début, 741 à la fin.",
                  "un agent qui change d'unité fait-il un mouvement ?",
                  "Non : il est présent aux deux photos, il n'entre ni ne sort. Pour une "
                  "direction ou une agence, l'effectif de fin se lit donc dans T58, pas dans "
                  "T60.",
                  "regles_t60.md, G14 · contrôle du 17/11, C1 · fiche des règles, règle 88"),
            regle("G8", "Les entrées, dans l'ordre", "prouvé",
                  "Un agent entre quand il est présent à la fin dans une nature et absent de "
                  "cette nature au début. Le code se lit dans l'ordre : DETAC s'il a une "
                  "décision 37 (accueil d'un agent détaché) ; RECRU s'il est entré dans l'année "
                  "(embauche, à défaut prise de service) ; REM s'il était en disponibilité au "
                  "début ; sinon AUTRE_ENT, avec son motif. Pour un temporaire, RECRU est un "
                  "nouvel engagement 50.",
                  "Les entrées ne sont écrites nulle part : on les lit sur la fiche de l'agent "
                  "et dans les décisions.",
                  "2024 : 101 recrutements pour 104 publiés ; 2 des 3 manquants sont des agents "
                  "entrés et repartis dans l'année. 2025 : 159 recrutements. Aucune entrée "
                  "DETAC ni AUTRE_ENT depuis 2020.",
                  "un temporaire devenu permanent est-il un recrutement ?",
                  "Non. Il sort du tableau des temporaires et entre dans celui des permanents en "
                  "« autre entrée ». Il n'est compté qu'une fois en recrutement, à son premier "
                  "engagement.",
                  "regles_t60.md, G8 · contrôle du 17/11, C1 et C2 · lot 26, Q146"),
            regle("G9", "Les sorties, dans l'ordre", "prouvé",
                  "Un agent sort quand il est présent au début dans une nature et absent de "
                  "cette nature à la fin. Le code se lit dans l'ordre : le départ qui l'a fait "
                  "sortir, daté dans l'année (02 → RET retraite ; 34 ou la date de décès → DEC ; "
                  "35 et 52 → FIN_CTRAT ; 36 → DEM démission ; 54 → LIC licenciement) ; sinon "
                  "DISP s'il est en disponibilité à la fin ; sinon AUTRE_SORT, avec son motif.",
                  "Pour un permanent, c'est son premier départ ; pour un temporaire, le dernier "
                  "départ de l'année, celui qui a fini son dernier contrat.",
                  "2024 : 24 retraites (publié 23), 5 décès (3) et 1 démission ; aucune autre "
                  "sortie. Sur cinq ans, les décès ont le même total que l'annuaire, mais "
                  "décalés d'un an : ils sont datés par la liquidation des droits.",
                  "un agent parti à la retraite puis décédé la même année ?",
                  "Il sort en retraite : c'est son premier départ. Depuis 2019, aucun décès "
                  "après une retraite n'est d'ailleurs enregistré (point 12).",
                  "regles_t60.md, G9 · contrôle du 17/11, C1 et C2 · lot 26, Q147"),
            regle("G15", "Le détail des « autres »", "proposé",
                  "Chaque entrée et chaque sortie reçoit un code de mouvement de la CNSS, et un "
                  "motif qui précise ce que le code ne dit pas. Autres entrées : REM remise en "
                  "activité, DETAC détaché, AUTRE_ENT. Autres sorties : LIC licenciement, DEM "
                  "démission, DISP disponibilité, MISE_DIS mise à disposition, FIN_DETACH fin de "
                  "détachement, FIN_CTRAT fin de contrat, AUTRE_SORT.",
                  "Le recueil des besoins v7 (RH ligne 3) demande de détailler les « autres » et "
                  "d'en donner la liste à la CNSS. Cette liste n'est pas inventée : c'est celle "
                  "de l'application, la même que le tableau 121 de l'annuaire.",
                  "les 18 codes sont bien dans la table des types de mouvement, aucun en double "
                  "(lot 26, Q152). De 2020 à 2025, T60 ne remplit que REM, DISP, DEM, LIC et "
                  "FIN_CTRAT : aucun détachement, aucune mise à disposition.",
                  "pourquoi aucun détachement ?",
                  "La base n'a aucune décision 37 chez les permanents, alors que l'annuaire "
                  "publie 2 détachements en 2023 et 1 en 2024. On demande à la DRH où ils sont "
                  "saisis (point 23).",
                  "regles_t60.md, G15 · lot 26, Q152 · points v2, points 23 et 49"),
            regle("G16", "Entré et sorti dans la même année", "ouvert",
                  "Un agent embauché en février et parti en octobre n'est présent à aucune des "
                  "deux photos : il n'est pas compté (réglage passages_annee = NON).",
                  "C'est la conséquence des deux photos. Le réglage OUI le compterait une fois "
                  "en entrée et une fois en sortie ; dans les deux cas, début + entrées − "
                  "sorties = fin reste vrai.",
                  "2 permanents sont entrés et partis en 2024, aucun autre depuis 2019. C'est "
                  "une partie de l'écart sur les recrutements : 101 comptés pour 104 publiés.",
                  "pourquoi ne pas les compter ?",
                  "C'est la proposition de la BI, pour que le tableau se lise entre deux photos. "
                  "La DRH tranchera (point 24) ; c'est un réglage, pas une réécriture.",
                  "regles_t60.md, G16 · lot 26, Q146 · points v2, point 24"),
            regle("G17", "Les mouvements internes restent dehors", "prouvé",
                  "Les affectations, mutations, promotions, rétrogradations et nominations "
                  "restent reprises de la table des mouvements de l'application comme avant "
                  "(lignes validées, sur l'année de leur date de début), mais elles n'entrent "
                  "pas dans T60.",
                  "Ce ne sont pas des entrées ni des sorties : l'agent reste au personnel. C'est "
                  "d'ailleurs presque tout ce que contient cette table : 94 à 95 % de mouvements "
                  "internes (lot 1 ; réunion du 03/11/2026).",
                  "le contrôle du 17/11 les sort à part, avec la nature INTERNE. Le code NOM, la "
                  "nomination, manque dans la table des types de mouvement : sa nature reste "
                  "vide (lot 26, Q152).",
                  "et les mutations entre agences ?",
                  "Elles ne sont ni une entrée ni une sortie. La DRH dira s'il faut montrer les "
                  "arrivées et les départs par mutation pour une direction ou une agence "
                  "(point 52).",
                  "regles_t60.md, G17 · lot 1 · points v2, points 42 et 52"),
        ]),
        Partie("T60 face à l'annuaire de la CNSS", colonnes=2,
               intro="Les requêtes proposées ont été lancées sur la base source le 17/11/2026. "
                     "Voici ce que donne T60, et ce qui manque encore pour retomber sur le "
                     "tableau 121 de l'annuaire CNSS 2024.",
               blocs=[
            constat("C1", "La maquette des permanents, de 2020 à 2025",
                    "La requête rend la maquette de l'Annuaire CIPRES : effectif début, "
                    "recrutements, autres entrées, retraites, décès, autres sorties, effectif "
                    "fin. Chaque année s'équilibre, et la fin d'une année est le début de la "
                    "suivante.",
                    "2024, base : 670 au début, 101 recrutements, 0 autre entrée, 24 retraites, "
                    "5 décès, 1 autre sortie, 741 à la fin. Annuaire : 767, 104, 7, 23, 3, 94, "
                    "758. 2025 : 741, 159 recrutements, 3 autres entrées, 20 retraites, 883 à la "
                    "fin.",
                    "Les colonnes qu'on sait lire tombent juste : recrutements, retraites, "
                    "décès. L'écart est dans le début, la fin et les « autres » — et il "
                    "s'explique, ligne par ligne, par les trois constats suivants.",
                    "contrôle du 17/11, C1 · annuaire CNSS 2024, tableau 121"),
            constat("C2", "Les 84 agents engagés en 2019",
                    "En août et septembre 2019, 84 personnes ont été engagées comme permanents "
                    "(décision 51), en service depuis 2018. Leur fiche est inactive, sans aucune "
                    "décision de départ, et aucune n'a de fiche plus récente.",
                    "L'annuaire les fait entrer en 2019 (84 « autres entrées ») et les sort en "
                    "2024 (84 des 94 « autres sorties »). Sans date de départ, la base ne les "
                    "compte dans aucune année : 670 permanents fin 2023 pour 767 publiés.",
                    "C'est l'essentiel de l'écart de 2019 à 2023 ; sans eux, il tombe à 8 à 16 "
                    "agents par an. À partir de 2025, il n'a plus d'effet. La DRH doit dire ce "
                    "qu'ils sont devenus et saisir leur départ (point 2).",
                    "lot 27, Q154 et Q155 · points v2, point 2"),
            constat("C3", "Les disponibilités qui manquent dans T60",
                    "La base a bien les décisions de mise en disponibilité (07), au niveau de "
                    "l'annuaire : 3, 4, 2, 5 et 6 de 2020 à 2024, pour 5, 3, 2, 4 et 7 publiées. "
                    "Mais T60 n'en montre que 0 à 1 par an.",
                    "12 agents mis en disponibilité de 2021 à 2025 ne sont jamais revenus, et "
                    "leur fiche est passée à inactive sans aucun départ daté : notre règle ne "
                    "les compte alors à aucune date, même les années où ils travaillaient. Il "
                    "manque 12 permanents fin 2020, et encore 3 fin 2024.",
                    "C'est une erreur de notre requête, pas de la CNSS : « inactif » ne veut pas "
                    "dire « parti ». Correction trouvée : pour une fiche inactive sans départ "
                    "daté, la dernière mise en disponibilité sert de date de sortie. T60 "
                    "donnerait alors 2, 1, 3 et 3 disponibilités de plus de 2021 à 2024.",
                    "lot 27, Q156 ; lot 28, Q159 · points v2, points 49, 51 et 64"),
            constat("C4", "Les temporaires n'en sortent jamais",
                    "Le tableau des temporaires s'équilibre lui aussi, mais presque personne "
                    "n'en sort : les fins de contrat ne sont pas saisies, et le stock grossit "
                    "d'année en année.",
                    "De 2020 à 2025 : 1 à 5 sorties par an pour 5 à 39 entrées. 2024 : 268 au "
                    "début, 35 recrutements, 1 autre entrée, 5 sorties, 299 à la fin — pour 46 "
                    "contractuels publiés.",
                    "Tant que les fins de contrat ne sont pas saisies, le tableau des "
                    "temporaires ne veut rien dire, ni en effectif ni en mouvements. C'est le "
                    "premier point à trancher avec la DRH (point 20).",
                    "contrôle du 17/11, C4 · points v2, point 20"),
        ]),
        Partie("La formation (T62)", colonnes=3, blocs=[
            regle("G10", "Qui est un agent formé", "proposé",
                  "Un formé est un agent permanent de la CNSS inscrit à une formation commencée "
                  "dans l'année. Il compte une fois, même s'il a suivi plusieurs formations. Sa "
                  "catégorie et son sexe sont ceux de sa situation au 31/12.",
                  "L'atelier de Kaya (2017) retient la date de début de la formation. L'agent "
                  "n'a pas à être encore présent au 31/12 : un agent formé puis parti à la "
                  "retraite dans l'année compte.",
                  "2024 : 407 agents permanents formés, pour 522 inscriptions. Avec la date de "
                  "fin : 409. Aucun temporaire formé, aucune personne externe dans "
                  "l'application, et un seul agent de la fonction publique.",
                  "une formation commencée en décembre et finie en janvier ?",
                  "Elle compte pour l'année de son début. Depuis 2019, une seule formation est à "
                  "cheval sur deux années, avec 2 agents, et aucune ne dure plus d'un an "
                  "(points 28 et 60).",
                  "README GRH, G10 · contrôle du 17/11 · lot 26, Q148 à Q150 · points v2, "
                  "points 13 et 28"),
            regle("G10", "Avec ou sans promotion", "ouvert",
                  "Une formation est « avec promotion » quand le classement de l'agent a changé "
                  "après elle : l'inscription garde son classement avant et après (CLA_AVANT et "
                  "CLA_APRES). Sinon, elle est « sans promotion ». C'est la finalité demandée "
                  "par la maquette de l'Annuaire.",
                  "C'est la seule trace d'un reclassement dans l'application, et ces deux "
                  "colonnes ne sont remplies que pour les formations diplômantes.",
                  "2024 : aucun formé avec promotion, pour 6 publiés. Les 2 seuls changements de "
                  "classement connus portent sur la formation commencée en 2023 et finie en "
                  "2024 — ce sont aussi les 2 agents d'écart entre 407 et 409.",
                  "où lit-on une promotion ?",
                  "On ne le sait pas encore. On demande à la DRH où est noté le reclassement, et "
                  "si toute formation diplômante compte comme « avec promotion » (point 26).",
                  "t62_requete_proposee.sql · contrôle du 17/11 · points v2, point 26"),
            constat("C5", "L'écart de T62 : 407 pour 480",
                    "T62 est le seul tableau du personnel qui ne retombe pas sur l'annuaire, et "
                    "aucune des pistes essayées ne l'explique : ni la date retenue, ni le type "
                    "d'agent, ni les formations longues.",
                    "2024 : 407 formés dans la base (81 employés, 96 agents de maîtrise, 179 "
                    "cadres moyens, 51 cadres supérieurs) pour 480 publiés (132, 142, 118, 88). "
                    "L'annuaire publie 88 cadres supérieurs formés, alors que la CNSS n'en "
                    "compte que 62 en poste.",
                    "Les chiffres publiés sont sans doute des participations et non des agents, "
                    "ou viennent d'un autre fichier que l'application. On demande sa source à la "
                    "DRH, avec la liste de ses colonnes (point 4).",
                    "contrôle du 17/11, T62 · annuaire CNSS 2024, tableau 123 · points v2, "
                    "points 4 et 13"),
        ]),
        Partie("Les finances (T63 et T64)", colonnes=3,
               intro="T63 et T64 ne se lisent pas dans l'application du personnel, mais dans la "
                     "comptabilité. Aucun chiffre n'est mesurable aujourd'hui : la source "
                     "n'existe pas encore. Tout le reste est écrit et n'attend que les montants.",
               blocs=[
            regle("F1", "D'où viendront les montants", "ouvert",
                  "Tous les montants de T63 et T64 viendront d'une seule vue de la comptabilité, "
                  "V_BI_BALANCE_COMPTABLE, que la DSI doit créer et alimenter depuis Oracle GL : "
                  "un montant par exercice, compte, gestion (G générale, S sociale) et section "
                  "analytique, en réalisé et en budgété. Le montant d'un compte est le net : "
                  "débit moins crédit, sur tous les mois de l'exercice.",
                  "La comptabilité de la copie de la base n'est pas valide : la CNSS l'a dit le "
                  "07/11/2026. On ne la lit plus, et les montants qu'on en avait tirés sont "
                  "abandonnés.",
                  "aucun chiffre mesuré : la vue n'existe pas encore. Les deux colonnes de taux "
                  "de la table d'aujourd'hui sont vides depuis le début.",
                  "quand aura-t-on T63 et T64 ?",
                  "Quand la vue sera créée et chargée, avec cinq exercices (N-4 à N). C'est la "
                  "question posée à la DSI avec la DFC (point 55).",
                  "t63_t64_requete_proposee.sql, règle F1 · points v2, points 30 (déjà répondu) "
                  "et 55"),
            regle("F2", "Les comptes de chaque rubrique", "ouvert",
                  "Une rubrique est une liste de débuts de compte et une liste de gestions : "
                  "frais de personnel = comptes 65 ; charges de fonctionnement = comptes 61 à "
                  "66 ; dépenses de formation = comptes 63, 86 et 87 ; masse salariale = comptes "
                  "651 et 652 ; budget = comptes 6 et 2 budgétés.",
                  "C'est ce que dit le recueil des besoins v7 (RH lignes 6 et 7, Finance ligne "
                  "1), notre première source. L'Annuaire CIPRES donne les mêmes comptes 651 et "
                  "652 pour la masse salariale.",
                  "rien n'est encore mesurable. Pris à la lettre, le compte 63 compte tous les "
                  "services consommés, pas seulement la formation : le ratio de T64 sortirait "
                  "sûrement de sa norme.",
                  "« 63, 86 et 87 », est-ce bien tous ces comptes ?",
                  "C'est la question à la DFC (point 31) : ce sont peut-être des sous-comptes. "
                  "Chaque liste est un réglage, une ligne par compte : la réponse se pose sans "
                  "réécrire la requête.",
                  "t63_t64_requete_proposee.sql, règle F2 · points v2, points 31, 33 (déjà "
                  "répondu) et 63"),
            regle("F4", "Les deux ratios et leurs normes", "proposé",
                  "T63 = frais de personnel ÷ charges de fonctionnement ; la norme CIPRES est de "
                  "85 % au plus. T64 = dépenses de formation ÷ masse salariale ; la norme est de "
                  "1 à 5 %. T64 montre aussi la formation rapportée au budget. Un taux dont le "
                  "dénominateur manque reste vide, jamais à zéro.",
                  "Ce sont les formules de l'Annuaire CIPRES (p. 17 et 52) et du recueil des "
                  "besoins v7 ; les cinq derniers exercices sont montrés.",
                  "aucun ratio calculé à ce jour. Un taux ne sera donné que pour un exercice "
                  "clos : tant que les douze mois ne sont pas fermés dans la comptabilité, les "
                  "montants s'affichent et les taux restent vides.",
                  "les deux côtés d'un ratio portent-ils sur le même périmètre ?",
                  "Pas encore sûr : le recueil prend les frais de personnel sur les gestions G "
                  "et S, et les charges de fonctionnement sur la gestion G seule. On le demande "
                  "à la DFC (points 29 et 63).",
                  "t63_t64_requete_proposee.sql, règles F3 et F4 · points v2, points 29 et 63"),
            regle("", "Le budget de la formation", "ouvert",
                  "Le second ratio de T64 rapporte la dépense de formation au budget. Par "
                  "défaut : le budget définitif des comptes 6 et 2 (fonctionnement et "
                  "investissement), toutes gestions.",
                  "C'est une proposition de la BI : ni le recueil des besoins ni l'Annuaire ne "
                  "disent quel budget prendre.",
                  "la base du personnel garde l'année du budget d'une formation, jamais son "
                  "montant : le budget ne peut venir que de la comptabilité.",
                  "quel budget prendre ?",
                  "Fonctionnement seul ou avec l'investissement, initial ou définitif, et "
                  "quelles gestions : c'est la question à la DFC (point 32).",
                  "t63_t64_requete_proposee.sql, réglages · points v2, point 32"),
            question("54", "Les montants par direction et par agence",
                     "Les charges de la comptabilité portent-elles une section analytique qui "
                     "dit la direction ou l'agence ?",
                     "Le recueil des besoins v7 demande T63 et T64 par direction, agence et "
                     "bureau (RH lignes 6 et 7), comme les autres tableaux du personnel.",
                     "Si la section analytique est remplie, une table de plus viendra à côté de "
                     "celle d'aujourd'hui, au grain exercice × unité ; celle d'aujourd'hui garde "
                     "une ligne par année, pour ne rien casser dans OBIEE.",
                     "En attendant, T63 et T64 se donnent pour toute la CNSS.",
                     "t63_t64_requete_proposee.sql, QF5 · points v2, point 54"),
        ]),
        Partie("Si on te demande", colonnes=3, blocs=[
            qr("Votre T60 ne ressemble pas au nôtre : pourquoi ?",
               "Parce qu'on ne lit pas la même chose. Nous comparons deux photos de l'effectif, "
               "donc début + entrées − sorties = fin tombe toujours juste. L'annuaire ajoute des "
               "mouvements qui ne sont pas dans l'application : les 84 engagés de 2019, les "
               "détachements, et des disponibilités que notre règle efface encore.",
               "Contrôle du 17/11/2026, C1 ; annuaire CNSS 2024, tableau 121 ; lots 27 et 28."),
            qr("Pourquoi 407 agents formés alors que nous en publions 480 ?",
               "On a cherché : la date de la formation ne change rien (407 à 409), et aucun "
               "temporaire ni externe n'est formé. L'annuaire publie même 88 cadres supérieurs "
               "formés pour 62 en poste : ce sont sans doute des participations, ou un autre "
               "fichier. On a besoin de sa source.",
               "Contrôle du 17/11 ; lot 26, Q148 à Q150 ; point 4 des points à valider."),
            qr("Où en sont T63 et T64 ?",
               "Les règles, les comptes et les deux ratios sont écrits ; il n'y manque que les "
               "montants. Ils viendront de la vue V_BI_BALANCE_COMPTABLE, que la DSI doit "
               "alimenter depuis Oracle GL, avec cinq exercices. La comptabilité de la copie de "
               "la base n'est pas utilisable.",
               "t63_t64_requete_proposee.sql ; points 55 et 30 des points à valider."),
        ]),
    ],
)
