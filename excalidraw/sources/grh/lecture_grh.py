"""
Contenu de la fiche de lecture de la gestion administrative (T58 à T65), générée par
sources/fiche_de_lecture.py dans schemas/grh/fiche_de_lecture.excalidraw.

Même modèle que la fiche des prestations : pour chaque tableau, chaque terme de l'Annuaire CIPRES avec
ce que j'en ai compris, la question à valider, où je le prends dans la base et la requête pour
vérifier. Les requêtes viennent des fiches de validation de la GRH (t58_effectif, t60_mouvement,
t62_formation). T58, T59, T61 et T65 se lisent sur le même effectif : une seule section.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import t58_effectif as t58  # noqa: E402
import t60_mouvement as t60  # noqa: E402
import t62_formation as t62  # noqa: E402
from fiche_de_lecture import renvoi  # noqa: E402

TITRE = "Fiche de lecture : la gestion administrative"
SOUS_TITRE = ("Les tableaux CIPRES T58 à T65, le personnel de la CNSS, terme par terme. À lire à "
              "côté de l'Annuaire statistique CIPRES (version du 11/09/2020).")
INTRO = [
    "Pour chaque tableau de l'Annuaire CIPRES, je reprends chaque terme (effectif, catégorie, "
    "recrutement, formé…) et je dis comment je l'ai compris, d'où je le prends dans la base et "
    "comment on refait le chiffre. Le but : que tu me dises si ma lecture est la bonne, terme par "
    "terme.",
]
TITRE_TABLES = "Les tables de la base du personnel (schéma USER_GRH, copie du 18/02/2026)"
TABLES = [
    "GRH_PERSONNE : un agent par ligne. QUALITE : A agent de la CNSS, B agent de la fonction "
    "publique, C agent affecté. SEXE M ou F, DATE_NAISS, DATE_EMBAUCHE, DATE_PRISE_SERVICE, "
    "ETAT_EMPLOYE (ACTIF ou INACTIF), DATE_DECES (plus jamais remplie depuis 2010).",
    "GRH_SITUATION : les situations successives de l'agent, chacune depuis DATE_DEBUT. "
    "NATURE_EMPLOIE : PERMANENT ou TEMPORAIRE. CATEGORIE : 01 à 06, la catégorie de la grille de "
    "la CNSS. CODE_DOMAINE_ACT : le domaine d'activité. STATUT_EMPLOYE : A activité, B détaché, "
    "C stage, D mise à disposition, E disponibilité, F suspendu, G congé payé, H sous les "
    "drapeaux (service militaire), R retraite, X radié, Y décédé.",
    "GRH_DECISION : les décisions de l'agent, CDEC_CODE et DATE_VIGUEUR. 02 admission à la "
    "retraite, 07 mise en disponibilité, 14 mise à disposition, 18 remise en activité, 34 "
    "liquidation des droits après décès, 35 fin de contrat, 36 démission, 40 annulation d'une "
    "décision, 50 engagement temporaire, 51 engagement permanent, 52 rupture, 54 licenciement. "
    "Les décisions collectives sont dans GRH_DECISION_PERSONNE.",
    "GRH_FORMATION : les formations (DATE_DEBUT, DATE_FIN, CATEGORIE : PERF perfectionnement, "
    "DIPLO diplômante, CONF conférence ou séminaire). GRH_INSCRIPTION : qui suit quelle "
    "formation, son classement avant et après (CLA_AVANT, CLA_APRES) et sept frais.",
    "La paie et la comptabilité ne sont pas dans USER_GRH : les vues de paie sont invalides, le "
    "grand livre est dans le schéma STG_GL.",
]
SUITE = [
    "Dans l'ordre de l'Annuaire CIPRES : T58, T59, T61 et T65 (l'effectif au 31 décembre, par "
    "catégorie, âge, sexe, domaine, ancienneté, et le taux d'encadrement), T60 (le mouvement du "
    "personnel), T62 (les formés), T63 et T64 (les ratios de frais de personnel et de "
    "formation, qui attendent la comptabilité).",
    "Les chiffres sont ceux de 2024, pour les comparer à l'annuaire CNSS 2024, qui "
    "publie T58, T60, T61, T62 et T65 (ses tableaux 119 à 124, p. 90 à p. 92).",
]


def sql(fiche, regle):
    """La requête de la règle d'une fiche de validation, avec « terme » à la place de « règle »."""
    return renvoi(fiche.SQL[regle]).replace("selon la réponse à la question Q1",
                                            "selon ta réponse au terme 3")


T58 = dict(
    code="T58, T59, T61, T65",
    titre="L'effectif du personnel à la clôture de l'exercice et le taux d'encadrement",
    pages="Annuaire CIPRES : description p. 17, maquettes p. 50 à p. 52, glossaire p. 62, p. 64 "
          "et p. 66",
    annee="2024",
    resume=("Pour moi, ces quatre tableaux font la photo du personnel de la CNSS au 31 "
            "décembre : par catégorie, âge et sexe (T58), par domaine d'activité (T59), par "
            "ancienneté (T61), et la part des cadres sur cinq ans (T65). Ils partent tous de la "
            "même liste : les agents permanents présents au 31 décembre."),
    termes=[
        dict(terme="Effectif des travailleurs de l'organisme",
             annuaire="Répartir « l'effectif de l'organisme à la clôture de l'exercice » (p. 17). "
                      "L'atelier de Kaya distingue les permanents et les temporaires.",
             compris="Le personnel permanent : un agent de la CNSS dont la dernière situation au "
                     "31 décembre est permanente. Les agents de la fonction publique et les agents "
                     "affectés n'en font pas partie. Les contractuels sont comptés à part "
                     "(terme 6).",
             question="Tu valides : l'effectif = les agents permanents de la CNSS ?",
             base=["GRH_PERSONNE.QUALITE = 'A' : agent de la CNSS.",
                   "GRH_SITUATION.NATURE_EMPLOIE = 'PERMANENT', dans la situation qui a le plus "
                   "grand DATE_DEBUT avant le 31/12."],
             sql=sql(t58, "1")),
        dict(terme="À la clôture de l'exercice",
             annuaire="L'effectif « à la clôture de l'exercice », fin d'exercice (p. 17).",
             compris="Entré au plus tard le 31 décembre, et encore en poste, ou parti après le "
                     "31 décembre. Le départ est la première décision de départ (retraite, "
                     "liquidation après décès, fin de contrat, démission, rupture, licenciement), "
                     "sauf si elle a été annulée.",
             exemple="une retraite au 15/12/2024 (décision 02) : l'agent n'est pas compté au "
                     "31/12/2024 ; une retraite au 15/01/2025 : il est compté.",
             question="Tu valides : présent au 31/12 = entré avant, et pas encore parti ?",
             base=["Entrée : GRH_PERSONNE.DATE_EMBAUCHE, à défaut DATE_PRISE_SERVICE.",
                   "Départ : GRH_DECISION.CDEC_CODE 02, 34, 35, 36, 52 ou 54, à sa DATE_VIGUEUR ; "
                   "pas s'il est annulé par une décision 40.",
                   "2024 : 742 permanents (l'annuaire CNSS 2024 : 758 ; les 16 qui manquent ne "
                   "forment aucun groupe)."],
             sql=sql(t58, "2")),
        dict(terme="Catégorie professionnelle",
             annuaire="Employés, agents de maîtrise et cadres (p. 17 et p. 50). Le cadre est "
                      "« l'agent reconnu comme tel par l'accord d'établissement » (glossaire, "
                      "p. 62) : chaque caisse garde ses propres catégories (p. 50).",
             compris="La catégorie de la grille de la CNSS, dans la situation de l'agent au 31 "
                     "décembre : 1re et 2e catégories, employés ; 3e, agents de maîtrise ; 4e, "
                     "5e et 6e, cadres. L'annuaire CNSS 2024 (tableau 119, p. 90) sépare les "
                     "cadres moyens et les cadres supérieurs, sans dire quelles catégories : "
                     "ses chiffres retombent sur la base avec la 4e et la 5e en cadres moyens, "
                     "la 6e en cadres supérieurs. L'atelier de Kaya mettait la 5e avec les "
                     "cadres supérieurs.",
             question="La 5e catégorie : cadres moyens ou cadres supérieurs ?",
             base=["GRH_SITUATION.CATEGORIE : 01 à 06 (quelques HC, I à V, 001 à 005 sur de "
                   "vieilles situations).",
                   "2024 : employés 173, maîtrise 207, cadres moyens 304, cadres supérieurs 58 "
                   "(l'annuaire CNSS 2024, tableau 119 : 177, 208, 311, 62)."],
             sql=sql(t58, "3")),
        dict(terme="Tranche d'âge",
             annuaire="Moins de 20 ans, 20-24… 55-59, 60 ans et plus (p. 50). L'âge est "
                      "« l'année de constatation moins l'année de naissance » (glossaire, p. 62).",
             compris="L'âge au 31 décembre. À cette date, c'est exactement l'année moins "
                     "l'année de naissance, comme le dit le glossaire.",
             question="Tu valides : l'âge au 31 décembre, rangé dans la grille de l'Annuaire CIPRES ?",
             base=["GRH_PERSONNE.DATE_NAISS.",
                   "2024, de 20-24 à 55-59 ans : 12, 28, 70, 115, 139, 155, 151, 72."],
             sql=sql(t58, "4")),
        dict(terme="Le sexe",
             annuaire="Hommes et femmes, en colonnes (p. 50).",
             compris="Le sexe de la fiche de l'agent.",
             question="Tu valides : le sexe vient de la fiche de l'agent ?",
             base=["GRH_PERSONNE.SEXE : M ou F.", "2024 : 400 hommes, 342 femmes."],
             sql=sql(t58, "5")),
        dict(terme="Les contractuels",
             annuaire="Pas dans l'Annuaire CIPRES ; l'atelier de Kaya et l'annuaire CNSS 2024 "
                      "comptent les temporaires à part (46 en 2024).",
             compris="Un agent temporaire au 31 décembre, dont le dernier contrat a moins de "
                     "18 mois, sans départ depuis. Une fin de contrat suivie d'un nouveau contrat "
                     "n'est pas un départ. La base ne garde pas la fin prévue d'un contrat : les "
                     "18 mois sont une hypothèse.",
             question="Combien de temps dure un contrat temporaire à la CNSS ?",
             base=["GRH_SITUATION.NATURE_EMPLOIE = 'TEMPORAIRE'.",
                   "GRH_DECISION.CDEC_CODE = '50' : engagement temporaire ; 35 : fin de contrat.",
                   "2024 : 46, comme l'annuaire CNSS 2024 (44 engagés en 2024, 2 fin 2023)."],
             sql=sql(t58, "6")),
        dict(terme="Ancienneté dans l'organisme (T61)",
             annuaire="Moins d'un an, 1-4 ans… 35-39 ans, 40 ans et plus (p. 51).",
             compris="Les années pleines au 31 décembre depuis l'embauche, à défaut depuis la "
                     "prise de service.",
             question="Tu valides : l'ancienneté part de la date d'embauche ?",
             base=["GRH_PERSONNE.DATE_EMBAUCHE, à défaut DATE_PRISE_SERVICE.",
                   "2024 : 101 agents de moins d'un an, 224 de 10 à 14 ans."],
             sql=sql(t58, "7")),
        dict(terme="Domaine d'activité (T59)",
             annuaire="Immatriculation, recouvrement, contentieux, prestations, prévention des "
                      "risques professionnels, action sanitaire et sociale, autres domaines "
                      "(p. 50).",
             compris="Le domaine de la situation de l'agent au 31 décembre. Le pilotage, la "
                     "comptabilité et les autres codes vont dans « autres domaines ».",
             question="Tu valides : PIL, CPT et AUT dans « autres domaines », et ACT = action "
                      "sanitaire et sociale ?",
             base=["GRH_SITUATION.CODE_DOMAINE_ACT : IMM, REC, CONT, PRS, PREV, ACT ; PIL, "
                   "CPT, AUT.",
                   "Libellés : table GRH_DOMAINE_ACTIVITE."],
             sql=sql(t58, "8")),
        dict(terme="Taux d'encadrement (T65)",
             annuaire="« Rapport de l'effectif des cadres sur l'effectif total du personnel » "
                      "(glossaire, p. 66) ; norme : au plus 10 % (p. 17).",
             compris="Les cadres (4e, 5e et 6e catégories) divisés par l'effectif permanent au 31 "
                     "décembre, sur les cinq derniers exercices. La CNSS est à 48,8 % en 2024, "
                     "loin de la norme : la grille de la CNSS compte beaucoup de cadres.",
             question="Tu valides : les cadres sont les 4e, 5e et 6e catégories, même si le taux "
                      "dépasse de loin la norme ?",
             base=["La catégorie du terme 3, sur l'effectif du terme 2.",
                   "2024 : 48,8 % (l'annuaire CNSS 2024 : 49,2 %)."],
             sql=sql(t58, "9")),
    ],
)

T60 = dict(
    code="T60",
    titre="Mouvement du personnel au cours des 5 dernières années",
    pages="Annuaire CIPRES : description p. 17, maquette p. 51, glossaire p. 63",
    annee="2024",
    resume=("Pour moi, ce tableau explique, année par année, comment on passe de l'effectif du "
            "1er janvier à celui du 31 décembre : les entrées (recrutements, autres) et les "
            "sorties (retraites, décès, autres)."),
    termes=[
        dict(terme="Effectif début et fin d'année",
             annuaire="Les colonnes « effectif début année » et « effectif fin année » (p. 51).",
             compris="Les permanents présents au 31 décembre, comptés comme dans T58. L'effectif "
                     "du début d'une année est celui du 31 décembre de l'année d'avant.",
             question="Tu valides : l'effectif du 1er janvier = celui du 31 décembre d'avant ?",
             base=["La liste du terme 2 de T58, au 31/12/2023 et au 31/12/2024.",
                   "2024 : 672 au début, 742 à la fin (l'annuaire CNSS 2024 : 767 et 758)."],
             sql=sql(t60, "1")),
        dict(terme="Entrées et sorties",
             annuaire="Les entrées et les sorties de l'année (p. 51).",
             compris="Une entrée est un permanent présent à la fin de l'année et absent au "
                     "début ; une sortie, l'inverse. Ainsi début + entrées − sorties = fin, "
                     "toujours. La table des mouvements de la base n'a ni recrutement ni départ : "
                     "elle ne peut pas servir.",
             question="Un agent recruté puis parti la même année : faut-il le compter en entrée "
                      "et en sortie ?",
             base=["Les deux listes du terme 1, comparées.",
                   "GRH_MOUVEMENT : sans recrutement ni départ."],
             sql=sql(t60, "2")),
        dict(terme="Recrutement",
             annuaire="La colonne « recrutement » des entrées (p. 51).",
             compris="Une entrée d'un agent embauché dans l'année. L'atelier de Kaya : la note "
                     "de prise de service vaut en attendant la décision d'engagement.",
             question="Tu valides : un recrutement = une entrée embauchée dans l'année ?",
             base=["GRH_PERSONNE.DATE_EMBAUCHE dans l'année, à défaut DATE_PRISE_SERVICE.",
                   "2024 : 104, exactement comme l'annuaire CNSS 2024."],
             sql=sql(t60, "3")),
        dict(terme="Autres entrées",
             annuaire="La colonne « autres » des entrées (p. 51).",
             compris="Une remise en activité après une disponibilité, un temporaire devenu "
                     "permanent, un retour sans décision.",
             question="Une remise en activité après une disponibilité est-elle une entrée ?",
             base=["GRH_DECISION.CDEC_CODE = '18' : remise en activité.",
                   "2024 : 4 décisions 18 (6 remises publiées)."],
             sql=sql(t60, "4")),
        dict(terme="Retraite",
             annuaire="La colonne « retraite » des sorties (p. 51).",
             compris="Une sortie dont la première décision de départ, dans l'année, est "
                     "l'admission à la retraite, à sa date de vigueur. Compter une retraite de "
                     "décembre l'année suivante ne retombe pas mieux sur l'annuaire CNSS 2024.",
             question="Tu valides : la retraite compte à la date de vigueur de la décision ?",
             base=["GRH_DECISION.CDEC_CODE = '02', DATE_VIGUEUR.",
                   "2024 : 25 (l'annuaire CNSS 2024 : 23) ; à 4 près chaque année."],
             sql=sql(t60, "5")),
        dict(terme="Décès",
             annuaire="Décédé : « personne dont la mort a été dûment constatée par un acte » "
                      "d'état civil (glossaire, p. 63).",
             compris="Une sortie par décès, à la date du décès. Aucune date de décès n'est "
                     "saisie depuis 2010 : je prends la décision de liquidation des droits, qui "
                     "arrive environ 8 mois et demi après le décès. L'atelier de Kaya : le "
                     "communiqué nécrologique vaut en attendant l'acte.",
             question="Peut-on saisir la date du décès, ou celle du communiqué nécrologique ?",
             base=["GRH_PERSONNE.DATE_DECES : vide depuis 2010.",
                   "GRH_DECISION.CDEC_CODE = '34' : liquidation des droits après décès.",
                   "2020 à 2024 : 19 sorties pour 17 décès publiés."],
             sql=sql(t60, "6")),
        dict(terme="Autres sorties",
             annuaire="La colonne « autres » des sorties (p. 51).",
             compris="Démission, licenciement, fin de contrat, mise en disponibilité, permanent "
                     "devenu temporaire. L'annuaire CNSS 2024 compte aussi des détachements, "
                     "introuvables dans la base.",
             question="Les détachements : où sont-ils saisis ?",
             base=["GRH_DECISION.CDEC_CODE : 36 démission, 54 licenciement, 35 et 52 fin de "
                   "contrat, 07 mise en disponibilité.",
                   "2024 : 2 démissions et 1 licenciement, comme l'annuaire CNSS 2024 ; 5 "
                   "disponibilités (7 publiées)."],
             sql=sql(t60, "7")),
        dict(terme="Les 5 dernières années",
             annuaire="Les années N-4 à N (p. 51).",
             compris="Une ligne par année, des quatre années d'avant à l'année choisie, et un "
                     "contrôle : début + entrées − sorties − fin = 0. De 2019 à 2023, il manque "
                     "84 agents engagés en 2019, que la base ne fait jamais sortir.",
             question="Les 84 engagés de 2019 : que sont-ils devenus, et quand sont-ils partis ?",
             base=["Les termes 1 à 7, une fois par année.",
                   "Les 84 de 2019 : engagement non annulé, fiche inactive, aucune date de "
                   "départ."],
             sql=sql(t60, "8")),
    ],
)

T62 = dict(
    code="T62",
    titre="Travailleurs en formation ou formés, par catégorie, sexe et finalité de la formation",
    pages="Annuaire CIPRES : description p. 17, maquette p. 51",
    annee="2024",
    resume=("Pour moi, ce tableau compte les agents formés dans l'année, par catégorie et par "
            "sexe, en séparant les formations qui ont débouché sur une promotion."),
    termes=[
        dict(terme="Travailleur en formation ou formé",
             annuaire="Le « nombre de travailleurs en formation ou formés » (p. 51).",
             compris="Un agent inscrit à au moins une formation dans l'année, compté une fois, "
                     "quel que soit le nombre de ses formations.",
             question="Un agent formé deux fois dans l'année compte-t-il une ou deux fois ?",
             base=["GRH_INSCRIPTION : PERS_ID, FORM_NO.",
                   "2024 : 410 agents et 525 inscriptions (l'annuaire CNSS 2024 : 480 formés)."],
             sql=sql(t62, "1")),
        dict(terme="L'année d'une formation",
             annuaire="Rien dans l'Annuaire CIPRES ; l'atelier de Kaya retient la date de début.",
             compris="L'année où la formation commence. Presque aucune formation ne chevauche "
                     "le 31 décembre.",
             question="Une formation commencée en décembre et finie en janvier compte-t-elle "
                      "l'année de son début ?",
             base=["GRH_FORMATION.DATE_DEBUT (et DATE_FIN)."],
             sql=sql(t62, "2")),
        dict(terme="Toutes les formations",
             annuaire="Rien dans l'Annuaire CIPRES.",
             compris="Toutes les formations comptent, quel que soit leur statut : toutes sont au "
                     "statut N. La requête actuelle ne gardait que le statut T et ne publiait "
                     "rien.",
             question="Tu valides : on ne filtre pas sur le statut de la formation ?",
             base=["GRH_FORMATION.STATUT : N partout (plus tenu depuis 2015).",
                   "GRH_FORMATION.CATEGORIE : PERF, DIPLO, CONF."],
             sql=sql(t62, "3")),
        dict(terme="Le personnel compté",
             annuaire="L'effectif de l'organisme (p. 17), comme T58.",
             compris="Un agent de la CNSS permanent au 31 décembre. Il n'a pas à être encore en "
                     "poste : un agent formé puis retraité dans l'année compte. Les contractuels "
                     "formés sont laissés de côté.",
             question="Faut-il compter aussi les contractuels formés ?",
             base=["GRH_PERSONNE.QUALITE = 'A' ; GRH_SITUATION.NATURE_EMPLOIE = 'PERMANENT' au "
                   "31/12.", "2024 : 409 des 410 agents formés sont permanents."],
             sql=sql(t62, "4")),
        dict(terme="Catégorie professionnelle",
             annuaire="Employés, agents de maîtrise, cadres (p. 51).",
             compris="La catégorie de la situation de l'agent au 31 décembre, comme dans T58. La "
                     "base donne deux fois plus de cadres moyens formés que l'annuaire "
                     "CNSS 2024 : ses chiffres viennent sans doute d'un autre registre.",
             question="D'où viennent les 480 formés publiés par l'annuaire CNSS 2024 ?",
             base=["GRH_SITUATION.CATEGORIE (terme 3 de T58).",
                   "2024 : 81, 96, 181, 51 (l'annuaire CNSS 2024 : 132, 142, 118, 88)."],
             sql=sql(t62, "5")),
        dict(terme="Le sexe",
             annuaire="Hommes et femmes, en colonnes (p. 51).",
             compris="Le sexe de la fiche de l'agent.",
             question="Tu valides : le sexe vient de la fiche de l'agent ?",
             base=["GRH_PERSONNE.SEXE : M ou F."],
             sql=sql(t62, "6")),
        dict(terme="Finalité : avec ou sans promotion",
             annuaire="La finalité de la formation : « avec promotion », « sans promotion » "
                      "(p. 51).",
             compris="Avec promotion : le classement de l'agent a changé après une formation de "
                     "l'année. Sinon, sans promotion. Le classement n'est rempli que pour les "
                     "formations diplômantes : 2 changements en 2024, pour 6 promotions publiées.",
             question="Où est saisi le changement de classement après une formation ?",
             base=["GRH_INSCRIPTION.CLA_AVANT et CLA_APRES."],
             sql=sql(t62, "7")),
        dict(terme="TOTAL et détail",
             annuaire="La ligne TOTAL (p. 51).",
             compris="Le Total est la somme des catégories. Je donne en plus les inscriptions, "
                     "et les formés en perfectionnement et en formation diplômante.",
             question="Tu valides ce détail en plus du tableau ?",
             base=["GRH_FORMATION.CATEGORIE : PERF perfectionnement, DIPLO diplômante.",
                   "2024 : 409 formés, 524 inscriptions."],
             sql=sql(t62, "8")),
    ],
)

ATTENTE = "-- pas encore de requête : {}"

T63 = dict(
    code="T63 et T64",
    titre="Ratios des frais de personnel et des dépenses de formation sur 5 ans",
    pages="Annuaire CIPRES : description p. 17, maquettes p. 52, glossaire p. 62 et p. 64",
    annee="2024",
    resume=("Pour moi, ces deux tableaux mesurent ce que coûte le personnel : la part des frais "
            "de personnel dans les charges de fonctionnement (T63), et la part de la formation "
            "dans la masse salariale et dans le budget (T64). Ils attendent la comptabilité : "
            "seules les dépenses de formation sont dans la base du personnel."),
    termes=[
        dict(terme="Charges de fonctionnement (T63)",
             annuaire="Les charges ou dépenses administratives : « total des dépenses de "
                      "fonctionnement et d'investissement de l'organisme » (glossaire, p. 62).",
             compris="Les charges de fonctionnement de l'année, lues dans la comptabilité de la "
                     "CNSS. La table du DTM qui devait les porter contient des valeurs de test.",
             question="Quels comptes font les charges de fonctionnement de la CNSS ?",
             base=["Pas dans USER_GRH.",
                   "Le grand livre : schéma STG_GL (1,5 million d'écritures au 27/02/2026)."],
             sql=ATTENTE.format("""les charges sont
-- dans la comptabilité (schéma STG_GL) :
-- la requête viendra avec la liste des
-- comptes donnée par la direction
-- financière""")),
        dict(terme="Frais de personnel (T63)",
             annuaire="La colonne « frais de personnel (FP) » (p. 52).",
             compris="Tout ce que coûte le personnel dans l'année : salaires, primes, charges "
                     "sociales. La paie n'est pas dans la copie : je les prendrai dans la "
                     "comptabilité.",
             question="Quels comptes font les frais de personnel ?",
             base=["La paie : les vues de paie de USER_GRH sont invalides.",
                   "La comptabilité : schéma STG_GL."],
             sql=ATTENTE.format("""la paie n'est pas
-- dans la copie de USER_GRH ; les frais
-- viendront des comptes de charges de
-- personnel du grand livre (STG_GL)""")),
        dict(terme="Ratio frais de personnel sur charges de fonctionnement",
             annuaire="« Frais de personnel sur charges de fonctionnement (norme : au plus "
                      "85 %) » (p. 17).",
             compris="FP divisé par CF, chaque année, sur cinq ans.",
             question="Tu valides la formule FP / CF, sur les comptes de l'année ?",
             base=["Les termes 1 et 2."],
             sql="""SELECT ROUND(100 * :frais_personnel
         / :charges_fonctionnement, 1)
         AS ratio_fp_cf_pct
FROM   DUAL
-- norme : au plus 85 %"""),
        dict(terme="Dépenses de formation (T64)",
             annuaire="La colonne « dépenses de formation (FOR) » (p. 52).",
             compris="La somme des frais des inscriptions aux formations commencées dans "
                     "l'année ; un frais vide compte zéro. Le champ « budget » des formations "
                     "contient une année, pas un montant.",
             question="Les frais saisis sur les inscriptions sont-ils toutes les dépenses de "
                      "formation ?",
             base=["GRH_INSCRIPTION : FRAIS_INSCRIPTION, FRAIS_SCOLARITE, FRAIS_TRANSPORT, "
                   "FRAIS_SEJOUR, FRAIS_DOSSIER, FRAIS_ASSURANCE, FRAIS_ACCESSOIRE.",
                   "GRH_FORMATION.DATE_DEBUT dans l'année ; GRH_FORMATION.BUDGET = une année."],
             sql="""SELECT SUM(NVL(i.FRAIS_INSCRIPTION, 0)
         + NVL(i.FRAIS_SCOLARITE, 0)
         + NVL(i.FRAIS_TRANSPORT, 0)
         + NVL(i.FRAIS_SEJOUR, 0)
         + NVL(i.FRAIS_DOSSIER, 0)
         + NVL(i.FRAIS_ASSURANCE, 0)
         + NVL(i.FRAIS_ACCESSOIRE, 0))
         AS depenses_formation
FROM   USER_GRH.GRH_INSCRIPTION i
JOIN   USER_GRH.GRH_FORMATION f
       ON f.FORM_NO = i.FORM_NO
WHERE  f.DATE_DEBUT BETWEEN DATE '2024-01-01'
                        AND DATE '2024-12-31'
-- un frais vide compte 0"""),
        dict(terme="Masse salariale (T64)",
             annuaire="« Montant total des rémunérations payées aux salariés par un employeur "
                      "pour une période donnée » (glossaire, p. 64) ; dans T64, les comptes 651 "
                      "et 652 (p. 52).",
             compris="Les rémunérations du personnel de la CNSS de l'année, lues dans les "
                     "comptes 651 et 652 de la comptabilité.",
             question="Les comptes 651 et 652 de la CNSS sont-ils bien les salaires du "
                      "personnel ?",
             base=["La comptabilité : schéma STG_GL, comptes 651 et 652."],
             sql=ATTENTE.format("""le solde des
-- comptes 651 et 652 de l'année, dans le
-- grand livre (STG_GL)""")),
        dict(terme="Total budget et ratios (T64)",
             annuaire="Les ratios FOR / MSAL (norme : entre 1 % et 5 %) et FOR / BUD (p. 17 et "
                      "p. 52).",
             compris="Les dépenses de formation divisées par la masse salariale, puis par le "
                     "budget total de l'année. Le budget est dans la gestion budgétaire (montant, "
                     "gestion, direction).",
             question="Le total budget : le budget voté, ou le budget exécuté ?",
             base=["Le budget : STG_BUDGET.", "Les dépenses de formation : le terme 4."],
             sql="""SELECT ROUND(100 * :depenses_formation
         / :masse_salariale, 2) AS for_msal,
       ROUND(100 * :depenses_formation
         / :total_budget, 2)    AS for_bud
FROM   DUAL
-- FOR / MSAL : entre 1 % et 5 %"""),
    ],
)

TABLEAUX = [T58, T60, T62, T63]
