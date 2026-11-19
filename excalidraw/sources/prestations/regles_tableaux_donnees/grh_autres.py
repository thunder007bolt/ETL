"""
Règles par tableau : GRH, les tableaux hors effectif : T60 (mouvement du personnel sur cinq ans),
T62 (agents formés par catégorie, sexe et finalité), T63 (frais de personnel ÷ charges de
fonctionnement) et T64 (dépenses de formation ÷ masse salariale et ÷ budget).

Même format que T32 dans regles_par_tableau.py. Sources : fiche des règles
(analyses/regles_cipres/fiche_regles.md, règles 1 à 4 et 78 à 98) ; T60 : analyses/grh/regles_t60.md
(G8, G9, G14 à G17, chiffres de la partie 5) et t60_requete_proposee.sql, les deux photos reprenant
les règles de l'effectif (regles_effectif.md, G1, G2, G7) ; T62 : t62_requete_proposee.sql (G10),
README de la GRH, recueil des besoins v7 (RH ligne 5) ; T63 et T64 : t63_t64_requete_proposee.sql,
champs de la vue V_BI_BALANCE_COMPTABLE (sources/vues_cnss/commun.py). Points à valider :
analyses/grh/points_a_valider_v2.md (17/11) ; le point 5, disponibilité, y est répondu (appliqué depuis
le 08/11 selon l'annuaire CNSS 2024).

Écarts voulus avec les requêtes (on suit la fiche) : T62, la catégorie HC va dans les cadres
supérieurs (la requête la met « sans catégorie ») ; T62, le découpage par direction, agence et bureau
demandé par le recueil n'est pas encore dans la requête. Pas de chiffres pour T63 et T64 : la
comptabilité de la copie (STG_GL) n'est pas valide, la vue n'est pas encore alimentée.
"""

_CHAPEAU_FIN = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)


def _chapeau(debut):
    return debut + "\n" + _CHAPEAU_FIN


_PHOTO = ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi plus "
          "tard n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None)

# ---------------------------------------------------------------- T63 et T64 : ce qui est commun
_VUE = ("Les montants viennent de la comptabilité (Oracle GL), par la balance comptable que la CNSS "
        "met à la disposition de la BI.", "vue V_BI_BALANCE_COMPTABLE", None)
_PAIE = ("La paie n'est pas lue : la masse salariale vient de la comptabilité (comptes 651 et 652).",
         "vues de paie de la GRH : non lues", None)
_COPIE = ("La comptabilité de la copie de la base n'est pas lue : la CNSS la dit non valide.",
          "STG_GL : non lu", None)
_MONTANT = ("Le montant d'un compte = ses débits moins ses crédits réalisés sur l'exercice, écritures "
            "de clôture comprises.",
            "MONTANT_DEBIT − MONTANT_CREDIT,\nNATURE_MONTANT = REALISE, MOIS 1 à 13", None)
_SOUS_COMPTES = ("Un compte se prend avec tous ses sous-comptes : « 65 » prend le 651, le 652…",
                 "COMPTE commençant par le numéro", None)
_A_FAIRE = ("À faire (DSI et DFC) : alimenter la vue depuis Oracle GL, après chaque clôture mensuelle. "
            "Des totaux par compte, sans nom ni numéro bancaire.",
            "V_BI_BALANCE_COMPTABLE (à faire)", None)
_CLOS = ("Le taux ne se calcule que pour un exercice clos (ses 12 mois clos dans Oracle GL). "
         "Avant, les montants se voient, le taux reste vide.",
         "PERIODE_CLOSE = O pour les mois 1 à 12", None)
_VIDE = ("Sans dénominateur, le taux reste vide, jamais 0.", "taux vide si le diviseur est nul", None)
_AFFICHAGE = ("Le taux s'affiche en pourcentage, avec deux décimales.",
              "taux arrondi à 4 décimales (0,4880)", None)
_CINQ_ANS = [
    ("Une ligne par exercice, des cinq derniers exercices clos (N−4 à N).",
     "EXERCICE ; EXERCICE_CLOS = O", "57"),
    ("Périodicité annuelle, comme le demande le recueil des besoins.", "EXERCICE", None),
    ("Le découpage par direction, agence et bureau, demandé par le recueil, passera par la section "
     "analytique des écritures, si elles en portent une (question à la DFC).",
     "V_BI_BALANCE_COMPTABLE.SECTION_ANALYTIQUE", None),
]


TABLEAUX = {
    # ------------------------------------------------------------------ T60
    "T60": {
        "titre": "T60 — Le mouvement du personnel sur cinq ans : les règles de la BI",
        "chapeau": _chapeau(
            "T60 suit, pour chacun des cinq derniers exercices, l'effectif au début de l'année, les "
            "entrées (recrutements, autres), les sorties (retraites, décès, autres) et l'effectif à la fin."
        ),
        "groupes": [
            ("Les deux photos", [
                ("Effectif de début = les agents présents au 31 décembre de l'année d'avant ; effectif de "
                 "fin = les présents au 31 décembre de l'année. Être présent suit les règles de l'effectif "
                 "(T58).", "mêmes règles et réglages que\nla requête de l'effectif (T58)", None),
                ("Rappel : seuls les agents de la CNSS comptent. Un permanent est présent s'il est entré "
                 "(embauche, à défaut prise de service) et pas encore parti ; un agent en disponibilité "
                 "n'est pas présent.",
                 "GRH_PERSONNE.QUALITE = A ;\nDATE_EMBAUCHE, à défaut DATE_PRISE_SERVICE", None),
                ("Un temporaire est présent si le 31 décembre tombe dans un de ses contrats. La fin d'un "
                 "contrat n'est pas saisie : sans départ ni nouveau contrat, il reste compté.",
                 "GRH_DECISION.CDEC_CODE = 50 (engagement)", "20"),
                ("Un tableau des permanents, un des temporaires : la nature est celle de la dernière "
                 "situation à chaque 31 décembre.", "GRH_SITUATION.NATURE_EMPLOIE", None),
            ]),
            ("Les entrées", [
                ("Une entrée : un agent présent à la fin, absent au début. Le motif, dans cet ordre : un "
                 "détachement, un recrutement, une remise en activité, sinon « autre ».",
                 "absent de la photo de début,\nprésent sur celle de fin", None),
                ("Recrutement d'un permanent : il est entré dans l'année (date d'embauche, à défaut prise "
                 "de service).", "GRH_PERSONNE.DATE_EMBAUCHE,\nà défaut DATE_PRISE_SERVICE → RECRU", "50"),
                ("Recrutement d'un temporaire : un nouveau contrat alors qu'il n'était sous aucun contrat "
                 "au début. Un contrat renouvelé n'est pas une entrée.",
                 "GRH_DECISION.CDEC_CODE = 50 → RECRU", None),
            ]),
            ("Les sorties", [
                ("Une sortie : un agent présent au début, absent à la fin. Le motif, dans cet ordre : le "
                 "départ qui l'a fait sortir, sinon une mise en disponibilité, sinon « autre ».",
                 "présent sur la photo de début,\nabsent de celle de fin", None),
                ("Retraite : la décision d'admission à la retraite, à sa date d'effet.",
                 "GRH_DECISION.CDEC_CODE = 02,\nDATE_VIGUEUR → RET", None),
                ("Décès : la date de décès de la fiche. Elle n'est plus saisie depuis 2010 : on prend la "
                 "liquidation des droits après décès, qui vient environ 8 mois plus tard.",
                 "GRH_PERSONNE.DATE_DECES,\nsinon CDEC_CODE = 34 → DEC", "3"),
                ("Les autres départs : démission, licenciement, fin ou rupture du contrat.",
                 "CDEC_CODE = 36 DEM, 54 LIC,\n35 et 52 FIN_CTRAT", None),
                ("Une décision annulée ne compte pas. Un permanent sort par son premier départ : un "
                 "retraité qui meurt ensuite n'est pas un décès de T60.",
                 "annulation : CDEC_CODE = 40\n(NO_DECI_MODIF)", None),
            ]),
            ("Le détail des « autres »", [
                ("Les autres entrées et sorties se détaillent par code de mouvement de la CNSS, puis par "
                 "motif (la décision qui a servi).",
                 "GRH_TYPE_MOUVEMENT.CODE_TYPE_MVT,\npuis MOTIF_MVT", "49"),
                ("La mise en disponibilité est une sortie, la remise en activité une entrée, comme dans "
                 "l'annuaire CNSS 2024.", "CDEC_CODE = 07 → DISP ;\n18 → REM", "64"),
                ("Un agent détaché accueilli à la CNSS est une autre entrée. Aucun n'est saisi chez les "
                 "permanents ; les fins de détachement ne sont pas lues.",
                 "CDEC_CODE = 37 → DETAC", "23"),
                ("Un temporaire devenu permanent sort des temporaires et entre chez les permanents en "
                 "« autre entrée » : il n'est compté qu'une fois en recrutement.",
                 "motifs DEVIENT_PERMANENT,\nETAIT_TEMPORAIRE", None),
            ]),
            ("Ce qui n'est pas un mouvement", [
                ("Les affectations, mutations, nominations et promotions restent hors de T60 : elles ne "
                 "changent pas l'effectif.", "GRH_MOUVEMENT (AFF, MUT, NOM…) : hors T60", None),
                ("Un agent entré et sorti dans la même année n'est pas compté : il n'est sur aucune des "
                 "deux photos.", "réglage passages_annee = NON", "24"),
                ("Une mise à disposition n'est pas une sortie.", "CDEC_CODE = 14 : non lu", "51"),
                ("Un temporaire parti puis réengagé dans l'année, présent aux deux 31 décembre, ne fait "
                 "aucun mouvement.", "présent sur les deux photos", None),
            ]),
            ("Les cinq ans et le contrôle", [
                ("Une ligne par exercice, des cinq derniers (N−4 à N).", "ID_TEMPS = AAAA1231", "57"),
                _PHOTO,
                ("Contrôle : début + entrées − sorties = fin, et la fin est l'effectif de T58 au 31 décembre.",
                 "T58 au 31/12", None),
                ("Les 84 agents engagés en 2019, sans date de départ, ne sont comptés dans aucune année : "
                 "de 2019 à 2023, la base a moins d'agents que l'annuaire.",
                 "GRH_DECISION.CDEC_CODE = 51 (2019),\nGRH_PERSONNE.ETAT_EMPLOYE = INACTIF", "2"),
                ("Par direction, agence ou bureau, un changement d'unité n'est ni une entrée ni une sortie : "
                 "la fin d'une unité se lit dans l'effectif (T58).", "GRH_SITUATION.UA_CODE", "52"),
            ]),
        ],
        "chiffres": (
            "Requête lancée le 17/11/2026 sur la base, permanents : chaque année, début + entrées − sorties = "
            "fin. 2024 : 670 au début, 101 recrutements, 0 autre entrée, 24 retraites, 5 décès, 1 autre sortie, "
            "741 à la fin ; annuaire CNSS 2024 : 767, 104, 7, 23, 3, 94, 758. L'écart du début et de la fin, et "
            "les 84 « autres sorties », viennent des 84 engagés de 2019 (point 2). Les disponibilités de la "
            "base (0 à 1 par an) sont sous celles de l'annuaire (2 à 7) : 12 agents en disponibilité ont une "
            "fiche inactive sans départ daté, et la règle ne les voit pas encore (point 64)."
        ),
    },

    # ------------------------------------------------------------------ T62
    "T62": {
        "titre": "T62 — Les agents formés par catégorie, sexe et finalité : les règles de la BI",
        "chapeau": _chapeau(
            "T62 compte les agents permanents formés dans l'année, par catégorie et par sexe, en séparant "
            "les formations suivies d'une promotion et les autres."
        ),
        "groupes": [
            ("Qui est compté", [
                ("Les agents de la CNSS de nature permanente : leur dernière situation au 31 décembre est "
                 "« permanent ».",
                 "GRH_PERSONNE.QUALITE = A ;\nGRH_SITUATION.NATURE_EMPLOIE = PERMANENT", "27"),
                ("L'agent n'a pas à être encore là au 31 décembre : formé puis parti à la retraite dans "
                 "l'année, il compte.", "aucune présence au 31/12 exigée", None),
                ("Un agent compte une fois, même avec plusieurs formations dans l'année.",
                 "GRH_INSCRIPTION.PERS_ID, compté une fois", "4"),
                ("Les personnes externes formées ne sont pas dans T62 : le recueil des besoins demande un "
                 "autre tableau, à partir de fichiers Excel (bilan social).", "hors de la base GRH", None),
            ]),
            ("La formation de l'année", [
                ("Un formé est un agent inscrit à une formation.",
                 "GRH_INSCRIPTION (PERS_ID, FORM_NO)", None),
                ("La formation compte l'année où elle commence (atelier de Kaya 2017), même si elle finit "
                 "l'année suivante.", "GRH_FORMATION.DATE_DEBUT dans l'année", "28"),
                ("Toutes les formations comptent, quel que soit leur statut : ce statut n'est plus tenu "
                 "depuis 2015.", "GRH_FORMATION.STATUT : non lu", "13"),
                ("Le nombre d'inscriptions est donné à côté du nombre de formés.",
                 "nombre de lignes GRH_INSCRIPTION", None),
            ]),
            ("Catégorie et sexe", [
                ("La catégorie est celle de la dernière situation au 31 décembre : 01 et 02 employés, 03 "
                 "agents de maîtrise, 04 et 05 cadres moyens, 06 et HC cadres supérieurs.",
                 "GRH_SITUATION.CATEGORIE", "47"),
                ("Les cadres de la maquette CIPRES = cadres moyens + cadres supérieurs.",
                 "CATEGORIE = 04, 05, 06, HC", None),
                ("Une catégorie vide ou inconnue va dans « Sans catégorie ».",
                 "autre CATEGORIE ou vide", None),
                ("Le sexe se lit sur la fiche : M hommes, F femmes.", "GRH_PERSONNE.SEXE", None),
            ]),
            ("La finalité", [
                ("Avec promotion : le classement de l'agent a changé après une formation de l'année ; "
                 "sinon, sans promotion. Le classement d'après n'est rempli que pour les formations "
                 "diplômantes.", "GRH_INSCRIPTION.CLA_AVANT ≠ CLA_APRES", "26"),
                ("Le détail : les formés en perfectionnement et les formés en formation diplômante. Un "
                 "agent peut être dans les deux.", "GRH_FORMATION.CATEGORIE = PERF, DIPLO", None),
            ]),
            ("Le découpage", [
                ("Une ligne par année : les formations commencées du 1er janvier au 31 décembre.",
                 "année de GRH_FORMATION.DATE_DEBUT", None),
                _PHOTO,
                ("Chaque chiffre se découpe par direction, agence et bureau : l'unité de la situation de "
                 "l'agent (pas encore dans la requête).", "GRH_SITUATION.UA_CODE (à ajouter)", "56"),
            ]),
        ],
        "chiffres": (
            "Requête lancée le 17/11/2026 sur la base, exercice 2024 : 407 permanents formés (employés 81, "
            "agents de maîtrise 96, cadres moyens 179, cadres supérieurs 51), 522 inscriptions, aucun « avec "
            "promotion ». Annuaire CNSS 2024 : 480 (132, 142, 118, 88), dont 6 avec promotion. Avec la date de "
            "fin des formations : 409. Ni la date ni le type d'agent n'expliquent l'écart (point 4)."
        ),
    },

    # ------------------------------------------------------------------ T63
    "T63": {
        "titre": "T63 — Les frais de personnel sur les charges de fonctionnement : les règles de la BI",
        "chapeau": _chapeau(
            "T63 rapporte, pour chacun des cinq derniers exercices, les frais de personnel de la CNSS à "
            "ses charges de fonctionnement (norme CIPRES : 85 % au plus)."
        ),
        "groupes": [
            ("D'où viennent les montants", [_VUE, _PAIE, _COPIE, _MONTANT, _SOUS_COMPTES, _A_FAIRE]),
            ("Le numérateur : les frais de personnel", [
                ("Frais de personnel = comptes 65, gestions G et S (administrative, et action sanitaire "
                 "et sociale).", "COMPTE 65… ;\nCODE_GESTION = G, 7, S, 5", None),
            ]),
            ("Le dénominateur : les charges de fonctionnement", [
                ("Charges de fonctionnement = comptes 61 à 66, gestion G seule, comme pour les finances "
                 "(T66). Les frais de personnel, eux, prennent G et S.",
                 "COMPTE 61… à 66… ;\nCODE_GESTION = G, 7", "29"),
            ]),
            ("Le taux et sa norme", [
                ("Taux = frais de personnel ÷ charges de fonctionnement.",
                 "TX_FRAIS_PERSONNEL =\nMT_FRAIS_PERSONNEL ÷ MT_CHARGES_FONCT", None),
                ("Norme CIPRES : 85 % au plus.", "norme de l'Annuaire CIPRES", None),
                _CLOS,
                _VIDE,
                _AFFICHAGE,
            ]),
            ("Les cinq ans", _CINQ_ANS),
        ],
        "chiffres": None,
    },

    # ------------------------------------------------------------------ T64
    "T64": {
        "titre": "T64 — Les dépenses de formation sur la masse salariale et sur le budget : les règles de la BI",
        "chapeau": _chapeau(
            "T64 rapporte, pour chacun des cinq derniers exercices, les dépenses de formation de la CNSS à "
            "sa masse salariale (norme CIPRES : de 1 à 5 %) et à son budget."
        ),
        "groupes": [
            ("D'où viennent les montants", [_VUE, _PAIE, _COPIE, _MONTANT, _SOUS_COMPTES, _A_FAIRE]),
            ("Le numérateur : la formation", [
                ("Dépenses de formation = comptes 63, 86 et 87, gestions G et S, à la lettre du recueil des "
                 "besoins. Le compte 63 entier prend aussi les autres services consommés. Les frais saisis "
                 "sur les inscriptions de la GRH ne servent pas.",
                 "COMPTE 63…, 86…, 87… ;\nCODE_GESTION = G, 7, S, 5", "31"),
            ]),
            ("Les dénominateurs", [
                ("Masse salariale = comptes 651 et 652 (Annuaire CIPRES, recueil des besoins), gestions G "
                 "et S.", "COMPTE 651…, 652… ;\nCODE_GESTION = G, 7, S, 5", "63"),
                ("Budget = le budget définitif de l'exercice (voté et modifié) : charges et investissements "
                 "budgétés, toutes gestions.", "NATURE_MONTANT = BUDGET ;\nCOMPTE 6… et 2…", "32"),
            ]),
            ("Les deux taux et la norme", [
                ("Taux 1 = dépenses de formation ÷ masse salariale. Norme CIPRES : de 1 à 5 %.",
                 "TX_FORMATION =\nMT_FORMATION ÷ MT_MASSE_SALARIALE", None),
                ("Taux 2 = dépenses de formation ÷ budget, montré à côté.",
                 "TX_FORMATION_BUDGET =\nMT_FORMATION ÷ MT_TOTAL_BUDGET", None),
                _CLOS,
                _VIDE,
                _AFFICHAGE,
            ]),
            ("Les cinq ans", _CINQ_ANS),
        ],
        "chiffres": None,
    },
}
