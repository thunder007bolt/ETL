"""
Annuaire statistique de la CNSS 2024 (n° 32) · Les prestations : ce que les tableaux 18 à 118
apprennent sur nos règles des tableaux CIPRES T25 à T57 (état au 08/11/2026).

Demande de l'utilisateur (08/11/2026) : refaire les prestations « à l'image du GRH », et
« infirmer ou valider » nos règles avec l'annuaire de la CNSS. Le schéma se lit de haut en bas :
1. l'inventaire : chaque tableau de l'annuaire qui touche les prestations, par thème, avec son
   tableau CIPRES ;
2. les contrôles internes : les totaux de l'annuaire se recoupent-ils ?
3. les verdicts : une carte par comparaison des cinq documents de famille (mêmes verdicts) ;
4. ce que l'annuaire ne publie pas ;
5. les modèles de ratios de la CNSS (classeur 2025, feuilles 03 à 06) ;
6. ce qu'on propose, et les questions ouvertes (renvoyées aux familles).

Le détail est dans analyses/prestations/annuaire_cnss_2024_prestations.md : mêmes tableaux, mêmes
verdicts, mêmes renvois. Rien d'autre n'est modifié (familles, README, requêtes, fiches,
pensions_2024, gestion_administrative_2024). Les chiffres de l'annuaire viennent de
cnss_files/Annuaire_2024_CNSS_BURKINA-FASO.pdf (texte extrait) ; ceux de la base, des lots déjà
lancés, cités par les familles. Aucun nom ni numéro n'est repris du classeur des ratios.

Usage :
    python3 excalidraw/sources/annuaire_cnss/prestations_2024.py [--force]
"""

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, main  # noqa: E402
from fiche_cipres import height_of, wrap  # noqa: E402
from gabarit_famille import INNER, VERDICTS, WIDTH, X0, Page, chip_verdict  # noqa: E402

OUTPUT = ROOT / "schemas" / "annuaire cnss" / "prestations_2024.excalidraw"

TITRE = "Annuaire CNSS 2024 · Les prestations, face aux tableaux CIPRES T25 à T57"
SOUS_TITRE = (
    "Annuaire statistique de la CNSS n° 32 (année 2024), tableaux 18 à 118 : ce qu'il publie, ce "
    "qu'il confirme ou contredit dans nos règles, ce qu'il ne publie pas. État au 08/11/2026 ; "
    "tout est proposé, rien n'est validé par la CNSS. Les verdicts sont ceux des cinq documents "
    "de famille, qui font foi. Détail : analyses/prestations/annuaire_cnss_2024_prestations.md."
)

EN_BREF = [
    "Ce qu'il publie : 101 tableaux sur les prestations (pensions 17, PF et maternité 12, AT/MP "
    "53, contrôles de prévention 3, fonctionnement et récapitulatifs 11, action sanitaire et "
    "sociale 5), plus trois repères du recouvrement (8, 11, 17). 120 cartes de comparaison.",
    "Il confirme : le bénéficiaire au 31/12, payé ou non (T32-T41 R6) ; l'orphelin à son âge, "
    "avant 25 ans (R5) ; liquidé = notifié (D4 : 831 pour 833, tableau 24 ; les IJ de "
    "maternité, 3 890 pour 3 893, précisent sans trancher la date) ; "
    "une rente recalculée n'est pas une entrée (T42-T49 R7) ; maternité dans les PF (R1) ; "
    "l'accessoire suit la pension (A1) ; les grilles d'âge (R8).",
    "Il tranche : un nouveau compte à la saisie, coordination comprise (T42-T49 R7, N6) ; "
    "l'anticipée à part (T32-T41 N4, dans le texte de la famille, sans carte : voir 3.6) ; les "
    "dates par défaut dans « 100 ans et plus » (T32-T41 R8) ; le rachat est une prestation servie "
    "(T50-T57 N4) ; « Autres » de T56 = l'ASS (T50-T57 N5).",
    "Il contredit une seule affirmation : la fiche T27 disait les frais médicaux de maternité "
    "« jamais payés » ; 25 030 869 FCFA payés en 2024, absents de la base.",
    "Écarts à expliquer : paiements AT/MP (soins 13,5 % dans la base, rentes −12 à −25 %, "
    "prestations en espèces −20,3 %) ; veuves −5,0 % et orphelins −15,9 % en 2024 ; rentes "
    "viagères 39 pour 44 ; allocataires 2020 à 2023 (+5 à +11 %) ; sexe des nouveaux retraités "
    "(641 femmes pour 527) ; montants des IJ de maternité 2023 et 2024 (−2 à −4 %) ; rentiers "
    "décédés 16 pour 24.",
    "Ses propres chiffres ne tiennent pas toujours : 89 décès d'ascendants pour 61 pensions au "
    "plus, orphelins +791 sans sortie à 25 ans, ligne ASS du tableau 106 ; dans les tableaux des "
    "accidents du travail, trois erreurs d'étiquette et deux pourcentages faux.",
    "Il ne publie pas : dossiers (T25-T27), contrôles et indus (T28-T31), montants des nouveaux, "
    "services passés, degré d'incapacité (IPP), âge des allocataires, effectifs de T51 à T53 et T55, trimestres, "
    "agences.",
    "Ce qu'on en fait : rien de l'existant n'est modifié ; les trois verdicts qui différaient "
    "pour la même comparaison (tableaux 22, 26, 35) sont harmonisés dans les familles, le reste "
    "est expliqué (3.6) ; les questions renvoient aux familles, posées une seule fois (6.2).",
]

MOTS = (
    "Les mots. Carte : une ligne de comparaison d'un document de famille, nommée par sa famille "
    "et sa règle (« T32-T41 R6 » : la règle R6 du document T32 à T41). Liquidé : droits calculés "
    "et notifiés à l'assuré. Saisie : le jour où la prestation, ou le paiement, est enregistré "
    "dans l'application. Mois payé : le mois que le paiement couvre. Stock : ce qui est en cours à "
    "une date ; flux : les entrées et sorties d'une année. DR : direction régionale (DRO "
    "Ouagadougou, DRB Bobo-Dioulasso, DRN Nord, DRF Fada N'Gourma, DRD Dédougou). PF : prestations "
    "familiales. AT/MP : accidents du travail et maladies professionnelles, que la CNSS appelle "
    "aussi RP (risques professionnels). PVID et AVS : deux noms de la branche des pensions. IJ : "
    "indemnités journalières. IPP : incapacité permanente partielle. ASS : action sanitaire et "
    "sociale. Coordination : une pension due au titre de deux caisses, dont une seule paie. "
    "Réglages : les choix écrits dans le bloc parametres en tête de chaque requête. Pnn, lot n, "
    "Ann : le résultat d'une requête de contrôle (le lot 10, P107 à P114, puis P115 à P117 et A06 à A08 restent à lancer). "
    "Fiche n : une règle de la fiche de validation de la famille. Débours : un paiement de "
    "l'application. Md, M : milliard, million de FCFA. Directions : DCP, Direction centrale des "
    "prestations ; DPC, Direction de la prospective et de la coopération (son service Études et "
    "Statistique fait l'annuaire) ; DSI, Direction des systèmes d'information ; DFC, Direction "
    "financière et comptable ; DAI, Direction de l'audit interne ; DPASS, Direction de la "
    "prévention et de l'action sanitaire et sociale. DES, DP et DCG : sigles du recueil, à faire "
    "préciser (DCG est sans doute la DPCG, Direction de la planification et du contrôle de "
    "gestion)."
)
MOTS_DIFFERENTS = (
    "Des mots qui différaient dans une même famille, corrigés le 08/11 sans changer le sens des "
    "cartes : T32-T41 dit maintenant pourquoi la limite de 21 ans des enfants est tranchée (la "
    "dernière tranche « 20-21 ans » du tableau 36) alors que sa carte N6 reste « écart à "
    "expliquer » (la forme par âge) ; T50-T57 dit que R1 et A1 sont confirmés par les cartes de "
    "T25-T27 et T28-T31 ; T42-T49 et T50-T57 écrivent « écart à expliquer » et « pas publié », "
    "comme les autres familles."
)

# verdict écrit dans le md : clé du gabarit
CLE = {"confirme": "confirme", "tranche": "tranche", "précise": "precise",
       "écart à expliquer": "ecart", "contredit": "infirme", "non comparable": "non comparable"}
ORDRE = ["confirme", "tranche", "precise", "ecart", "infirme", "non comparable"]
# le décompte écrit dans le md (partie 3) : le générateur s'arrête s'il ne retombe pas
COMPTES_MD = {
    "T25-T27": (19, 3, 0, 11, 2, 1, 2),
    "T28-T31": (19, 2, 0, 9, 3, 0, 5),
    "T32-T41": (31, 10, 1, 12, 8, 0, 0),
    "T42-T49": (23, 6, 2, 7, 5, 0, 3),
    "T50-T57": (28, 2, 2, 16, 7, 0, 1),
}

NOTE_INVENTAIRE = (
    "Pour chaque tableau : ce qu'il compte, sa page, le tableau CIPRES qui lui répond, et ce qu'on "
    "en tire. « Aucun » : aucun tableau de T25 à T57 ne lui répond. La GRH (tableaux 119 à 126) "
    "est dans le schéma gestion_administrative_2024."
)
NOTE_CONTROLES = (
    "Chaque total a été recalculé depuis le texte de l'annuaire : lignes, colonnes, sous-totaux, "
    "hommes + femmes, DR face à l'ensemble, pourcentages. Une case vide est lue comme 0 quand la "
    "somme le confirme."
)
NOTE_VERDICTS = (
    "Une carte par ligne de comparaison des documents de famille, dans leur ordre. La carte porte "
    "sa famille et sa règle (« T32-T41 R6 » : la règle R6 du document T32 à T41) ou le tableau "
    "CIPRES confronté. Verdicts, chiffres et écarts sont ceux des familles ; « Ce que ça change » "
    "est raccourci. Les cinq lignes « pas publié » des familles sont en partie 4."
)
NOTE_DIVERGENCES = (
    "Les trois premiers portaient vraiment sur la même comparaison : ils ont été harmonisés dans "
    "les documents de famille le 08/11. Les autres viennent de ce que deux familles ne "
    "confrontent pas la même règle au même chiffre : ils sont justifiés."
)
NOTE_NON_PUBLIE = (
    "Ce qui suit ne peut être ni confirmé ni contredit par l'annuaire : ces colonnes se valident "
    "avec la CNSS, à partir de ses propres fichiers ou de nos requêtes."
)
NOTE_MODELES = (
    "Classeur « Evaluation des Ratios CIPRES 2025 », feuilles 03 à 06 seulement : quatre modèles "
    "vides (aucune donnée saisie) qui montrent comment la CNSS compte calculer ses ratios. La "
    "feuille 02 n'a pas été lue (consigne). Aucun nom, aucun numéro d'assuré, aucun numéro de "
    "compte n'est repris ici."
)
NOTE_QUESTIONS = (
    "Aucune question n'est reposée ici : chaque point renvoie aux questions des familles. Quand "
    "plusieurs familles posent la même question, elle se pose une seule fois à la CNSS."
)
SOURCES = (
    "Sources : annuaire statistique de la CNSS n° 32, année 2024 (tableaux 18 à 118 ; démarche "
    "p. xiii et xiv ; directions et agences p. xv à xix ; fiche signalétique p. xx) ; partie 4 des "
    "cinq documents de famille (125 lignes : 120 cartes, 5 « pas publié ») ; inventaire et "
    "recoupements de l'annuaire (analyse du 08/11/2026, totaux recalculés depuis son texte) ; "
    "classeur des ratios de la CNSS 2025, feuilles 03 à 06 ; comparaison du 30/10 (A01 à A03). "
    "Rien n'a été lu des tables DTM ni de STG_GL ; aucune requête nouvelle. Détail : "
    "analyses/prestations/annuaire_cnss_2024_prestations.md."
)

# ---------------------------------------------------------------- les données (reprises du md)
INVENTAIRE = [('1.1 Les pensions (tableaux 18 à 34)',
  [('18',
    'Les pensionnés au 31/12, par type (vieillesse, veuves, veufs, orphelins, invalidité, '
    'anticipée, ascendants), de 2020 à 2024 : 62 231 en 2024',
    '16',
    'T32 à T36 (totaux)',
    'Le stock au 31/12, payé ou non (T32-T41 R6) ; la retraite anticipée est un type à part (78 en '
    '2024)'),
   ('19',
    "Les pensionnés de vieillesse et leurs montants 2024, par tranche d'âge et sexe : 30 730, 38 "
    '235 880 033 FCFA',
    '16',
    "T32 (en entier, sans l'anticipée)",
    'La grille et le sexe de T32 (R8) ; le montant sans les accessoires (N13)'),
   ('20',
    'Les pensionnés survivants (veuves, veufs, orphelins, ascendants mélangés) et leurs montants '
    '2024, par âge et sexe : 31 304',
    '17',
    'T35 (moins de 25 ans) ; T34 et T36 mélangés',
    'Les moins de 25 ans sont exactement les 9 478 orphelins du tableau 18 (R5, R6) ; 679 femmes '
    'de 100 ans et plus (R8)'),
   ('21',
    "Les pensionnés d'invalidité et leurs montants 2024, par âge et sexe : 119",
    '18',
    'T33 (en entier)',
    "70 invalides ont 60 ans ou plus : l'invalidité est gardée après l'âge de la retraite (T32-T41 "
    'N5)'),
   ('22',
    'Les pensionnés au 31/12/2024 par type et par DR',
    '18',
    'T32 à T36 (par DR)',
    'La DR du dossier est plausible (T32-T41 N3, précise)'),
   ('23',
    'Les nouveaux pensionnés de 2024 par type et par DR : 4 037',
    '19',
    'T42, T43, T44 (nombres) ; T25 (repère)',
    'Un nouveau compte à la saisie (T42-T49 R7) ; des personnes, pas des demandes (T25-T27 D2)'),
   ('24',
    'Les allocations de vieillesse liquidées en 2024, par mois et par DR : 833',
    '19',
    'T50 (effectif 2024) ; T25 (liquidés)',
    'Liquidé = notifié (D4) ; modèle du mois et de la DR (T25-T27 N1, N2)'),
   ('25',
    'Les nouveaux retraités de vieillesse de 2024 par âge exact et sexe : 2 486',
    '20',
    'T42 (âge, sexe)',
    'Grille de T42 confirmée de 55 à 69 ans ; sexe en écart (T42-T49 Q8)'),
   ('26',
    'Les pensionnés décédés en 2024, par mois et par type : 648',
    '21',
    'T57 (branche vieillesse)',
    "La date d'arrêt de fin mars N+1 (R13) ; les ayants droit décédés (T50-T57 N2)"),
   ('27',
    'Les « départs à la retraite » de 2024, par mois et par DR : 3 729',
    '21',
    'Peut-être T25 (déposés)',
    "Non comparable tant que la CNSS n'a pas dit ce qu'est un départ (T25-T27 Q21)"),
   ('28',
    'Les prestations de vieillesse servies chaque mois de 2024, par type : 46 629 625 556 FCFA',
    '22',
    'T31 (payées PVID), T56, montants de T32 à T36, T50, T51',
    'Le rythme des mois suit la saisie des paiements (R4) ; ligne « solidarité » à zéro (T28-T31 '
    'N1)'),
   ('29 à 33',
    'Les mêmes prestations, une DR par tableau',
    '23 à 27',
    'T31 et T56 par DR',
    'Le détail par DR et par mois (T50-T57 N8) ; un test de R4 DR par DR'),
   ('34',
    'Les prestations de vieillesse 2024 par type et par DR',
    '28',
    'T56 ; T50, T51 ; montants de T32 à T36 par DR',
    'Montants par DR (T32-T41 N3) ; majoration rangée avec la pension (A1)')]),
 ('1.2 Les prestations familiales et la maternité (tableaux 35 à 46)',
  [('35',
    "Les bénéficiaires d'IJ de maternité et les montants, de 2020 à 2024 : 3 893 en 2024",
    '30',
    'T54 (en entier) ; T27 (liquidés)',
    "Une mère compte l'année de la liquidation (D4) ; les montants 2023 et 2024 restent en écart"),
   ('36',
    "Les enfants d'allocataires et les montants 2024, par âge (moins de 5 ans à 20-21 ans) et sexe "
    ': 268 481',
    '30',
    "T41 (montant) ; aucun pour l'âge des enfants",
    "La limite de 21 ans (T32-T41 N6) ; l'âge et le sexe sont ceux des enfants, pas des "
    'allocataires'),
   ('37',
    'Les allocataires, conjoints et enfants au 31/12/2024 par DR : 97 378 allocataires',
    '30',
    'T41 (allocataires par DR)',
    "La condition « enfant payé dans l'année » retombe en 2024 (T32-T41 R6 pour T41)"),
   ('38',
    'Les allocataires, conjoints et enfants de 2020 à 2024 par DR, avec la variation en %',
    '31',
    'T41 (sur cinq ans)',
    'Les années 2020 à 2023 restent en écart ; le taux de variation se calcule comme le dit le '
    'recueil (T25-T27 N8)'),
   ('39',
    "Les bénéficiaires d'IJ de maternité de 2024 par mois et par DR (chacun une fois dans l'année)",
    '32',
    'T54, T27 (au mois)',
    'La CNSS suit au mois et par DR, comme le demande le recueil'),
   ('40',
    'Les PF servies en 2024 par prestation (prénatales, familiales, IJ, frais médicaux) et par DR '
    ': 11 180 191 425 FCFA',
    '32',
    'T56 (PF) ; montants de T41, T54, T55 ; T27 (frais médicaux)',
    'Maternité dans les PF (R1) ; 25 030 869 FCFA de frais médicaux de maternité, absents de la '
    'base'),
   ('41',
    'Les mêmes PF, mois par mois',
    '33',
    'T31 (payées PF)',
    'Le rythme des mois suit la saisie (corrélation 0,92)'),
   ('42 à 46',
    'Les mêmes PF, une DR par tableau',
    '34 à 36',
    'T31, T56, T41 par DR',
    'Montants par DR et par mois (T32-T41 N1, N3)')]),
 ('1.3 Les accidents du travail et maladies professionnelles (tableaux 47 à 99)',
  [('47',
    'Les AT/MP déclarés de 2015 à 2024 : 1 171 en 2024',
    '38',
    'T26 (déposés, repère)',
    'Une déclaration est à peu près un dossier ouvert (T25-T27 D9) ; taux de variation −2,58 % '
    '(N8)'),
   ('48',
    'Les AT/MP déclarés en 2024 par DR',
    '38',
    'T26 (par DR, repère)',
    'Modèle du découpage par DR (T25-T27 N2, N8)'),
   ('49',
    'Les AT/MP déclarés par siège des lésions, de 2020 à 2024 ; ligne « décès immédiats » : 13, '
    '11, 13, 11, 14',
    '39',
    'Aucun ; T57 (sinistrés, repère)',
    'La CNSS ne compte que la mort sur le coup (T50-T57 N9)'),
   ('50 à 56, 58 à 63, 65, 67, 68, 70, 71',
    'Les AT/MP déclarés croisés par élément matériel, qualification, circonstance, sexe, branche '
    "d'activité, âge, nature des lésions",
    '40 à 57',
    'Aucun',
    'Statistique des accidents, hors des tableaux CIPRES des prestations ; utile pour contrôler '
    'les totaux (partie 2)'),
   ('57, 64, 66, 69',
    'Les AT/MP par siège ou nature des lésions, avec la ligne « décès immédiats » (14 en 2024, '
    'tous des hommes)',
    '46, 51, 53, 55',
    'Aucun ; T57 (sinistrés, repère)',
    'Mêmes décès immédiats que le tableau 49'),
   ('72',
    'Accidents du travail (1 150) et maladies professionnelles (21) de 2024 par DR',
    '58',
    'T26 (repère)',
    'T26 ne sépare pas accident et maladie : rien à changer'),
   ('73 et 74',
    'Les AT/MP de 2024 par « lieu CNSS » (22 lieux)',
    '58 et 59',
    'Aucun',
    'Modèle publié du découpage par agence (T25-T27 N2)'),
   ('75',
    'Les nouveaux bénéficiaires de rente viagère de 2024 par DR, âge et sexe : 44',
    '60',
    'T48 (effectifs) ; T45 ; T26 (repère)',
    'Écart : 39 dans la base à la première fois (T42-T49 Q6)'),
   ('76',
    'Tous les rentiers viagers au 31/12/2024 par DR, âge et sexe : 1 255',
    '60',
    'T37 (effectifs)',
    'T37 se calcule comme la CNSS (−1,5 %) ; grille arrêtée à « 90 ans et plus »'),
   ('77',
    'Les nouveaux ayants droit de rente de 2024 par nature et DR : 61',
    '61',
    'T46, T47, T49 (nombres) ; T45',
    'Orphelins et veuves exacts (44, 4) ; ascendants 15 pour 13'),
   ('78',
    'Les ayants droit de rente au 31/12/2024 par type et DR : 977',
    '61',
    'T38, T39, T40 (totaux)',
    '« Ensemble au 31-12 » : le stock, lecture du « cumul » de T38 (T32-T41 Q14)'),
   ('79',
    'Toutes les rentes au 31/12/2024 par nature et DR : 2 232',
    '61',
    'T37 à T40 (totaux par DR)',
    'Se recoupe avec 76 et 78'),
   ('80',
    "Les rentes en fin d'année par nature, de 2020 à 2024",
    '62',
    'T37 à T40 (sur cinq ans)',
    "Rentes d'orphelins : +106 en 2024 pour 44 entrées (partie 2)"),
   ('81',
    'Les rentiers décédés en 2024 par nature et DR : 24',
    '62',
    'T57 (AT/MP)',
    'Écart : 16 dans la base (T50-T57 Q14)'),
   ('82',
    'Les soins AT/MP (prestations en nature) servis chaque mois de 2024, par type : 139 668 695 '
    'FCFA',
    '63',
    'T26 (« autres prestations ») ; T56 ; T31',
    "La base n'a que 13,5 % de ce montant (T25-T27 D11)"),
   ('83 à 87',
    'Les mêmes soins, une DR par tableau',
    '64 à 68',
    'T56, T26 par DR',
    'Pour repérer par DR les soins absents de la base (T50-T57 Q16)'),
   ('88', 'Les soins AT/MP de 2024 par type et par DR', '69', 'T56 ; T26', 'Idem'),
   ('89',
    'Les soins AT/MP par type, de 2020 à 2024',
    '70',
    'T56 (sur cinq ans)',
    "La base couvre 13 à 23 % selon l'année (T50-T57 N6)"),
   ('90',
    "Les prestations AT/MP en argent servies chaque mois de 2024 (IJ, allocations d'incapacité, "
    'rentes, rachats) : 934 445 128 FCFA',
    '71',
    'T52, T53, montants de T37 à T40, T31, T56 ; « achat de rente » de T29',
    'Rentes −12 à −25 % dans la base ; rachat = prestation servie (T50-T57 N4)'),
   ('91 à 95',
    'Les mêmes prestations, une DR par tableau',
    '71 à 73',
    'Idem, par DR',
    'Pour repérer où manquent les paiements AT/MP'),
   ('96',
    'Les prestations AT/MP en argent de 2024 par type et par DR',
    '74',
    'Idem',
    'Montants des rentes par DR (T42-T49 N5)'),
   ('97',
    'Les prestations AT/MP en argent par type, de 2020 à 2024',
    '74',
    'T52, T53 (montants sur cinq ans), T56',
    "IJ et allocations d'incapacité en écart (T50-T57)"),
   ('98',
    'Toutes les prestations AT/MP de 2024 (soins et argent) par DR : 1 074 113 823 FCFA',
    '75',
    'T56 (AT/MP par DR)',
    'Payées AT/MP −28,9 % dans la base, soins compris (T28-T31 N7)'),
   ('99',
    'Toutes les prestations AT/MP (soins et argent), de 2020 à 2024',
    '75',
    'T56 (AT/MP sur cinq ans)',
    '= ligne RP (risques professionnels) du tableau 106')]),
 ('1.4 Les contrôles et les indus (tableaux 100 à 102 ; repère : tableau 11)',
  [('100 et 101',
    'Les entreprises contrôlées en 2024 pour la prévention des risques professionnels, par mois et '
    "par branche d'activité : 178 (DPASS 107, DRB 71)",
    '76',
    'Aucun',
    'Des contrôles de prévention dans les entreprises, pas des contrôles de dossiers : à ne pas '
    'confondre avec ceux de la DAI (T28-T31)'),
   ('102',
    'Les comités de santé et de sécurité au travail (CSST) installés ou renouvelés en 2024',
    '77',
    'Aucun',
    'Hors des prestations'),
   ('11 (repère)',
    'Les employeurs contrôlés en 2024 par mois et par DR : 8 794',
    '11',
    'Aucun (recouvrement)',
    "Contrôles d'employeurs, hors des prestations"),
   ('aucun',
    'Aucun tableau ne publie les contrôles a posteriori des dossiers, ni les trop-perçus, ni les '
    'moins-perçus',
    'aucune',
    'T28 à T31',
    "Ces tableaux ne se valident pas par l'annuaire (partie 4)")]),
 ('1.5 Le fonctionnement, les délais et les récapitulatifs (tableaux 103 à 113 ; repères : 8, 17)',
  [('103 à 105',
    'Par branche (PF, RP, pension), de 2020 à 2024 : charges de fonctionnement, cotisations '
    '(produits techniques), prestations (charges techniques), produits financiers et leurs ratios',
    '79',
    'Aucun dans T25 à T57 (Annuaire CIPRES T66 à T68)',
    "Les prestations servies de l'annuaire sont les charges techniques de la comptabilité (T50-T57 "
    'N6) ; repère du ratio des ressources (N11)'),
   ('106',
    "Les prestations servies par branche et l'ASS, de 2020 à 2024 : 59 534 630 903 FCFA en 2024",
    '80',
    'T56 (en entier)',
    'Juste au total par compensation (T28-T31 N7) ; ligne « Autres » = ASS (T50-T57 N5)'),
   ('107 à 113',
    'Le délai de remise en banque des titres de paiement en 2024, ensemble, siège et DR : 38 413 '
    "titres, 5 568 en moins d'un jour (14,5 %)",
    '80 à 83',
    'Aucun (recouvrement, Annuaire CIPRES T71)',
    'À ne pas confondre avec le délai de liquidation de T25 à T27'),
   ('8 (repère)',
    'Les cotisations de 2024 par mois et par branche',
    '8',
    'Aucun',
    'Explique les pics trimestriels des tableaux 107 à 113'),
   ('17 (repère)',
    'Les cotisations de 2024 par branche et par DR : 155 541 417 397 FCFA',
    '14',
    'Aucun (dénominateur du ratio des ressources)',
    'Prestations ÷ cotisations recalculées (T50-T57 N11)'),
   ('Pages xiii, xiv, xx',
    'La démarche (base exploitée, collecte par DR, accord avec le compte de résultat, atelier de '
    'validation) et la fiche signalétique',
    'xiii à xx',
    'Tous',
    "On ne retombera jamais à l'unité ; publication en juin, donc arrêt vers fin mars (R13)"),
   ('Pages xv à xix',
    'Les directions et les 16 agences de la CNSS',
    'xv à xix',
    'Tous (découpage)',
    'Appuie « agence = service du dossier » (T25-T27 N2)')]),
 ("1.6 L'action sanitaire et sociale (tableaux 114 à 118)",
  [('114',
    "Recettes et dépenses de l'ASS de 2020 à 2024 : dépenses 1 571 655 315 FCFA en 2024",
    '85',
    'Aucun dans T25 à T57 (Annuaire CIPRES T76) ; ligne « Autres » de T56',
    "Deux montants d'ASS pour 2024 dans le même annuaire (T50-T57 Q18) ; une année passée corrigée "
    'avec une note (T50-T57 N7)'),
   ('115 à 117',
    'Les activités des cinq centres de santé maternelle et infantile (SMI) en 2024 : '
    'consultations, vaccinations, échographies',
    '86 à 88',
    'Aucun',
    'Indicateurs ASS du recueil (normes SMI), hors des tableaux CIPRES des prestations'),
   ('118',
    "Les « dossiers médicaux PF » des centres SMI en 2024 : 4 555 dossiers, 368 dossiers d'IJ",
    '88',
    'Aucun',
    'Des dossiers vus par les médecins, pas des dossiers de prestations : ne pas les confondre '
    'avec les déposés de T27 (T25-T27)')])]

RECOUPE = [('Tableau 18 face aux tableaux 19, 20, 21 et 22',
  'Chaque type retombe : vieillesse 30 730 ; survivants 31 304 ; invalidité 119 ; les 7 types par '
  "DR. Les 78 pensions anticipées ne sont dans aucun tableau d'âge (19 + 20 + 21 = 62 153 pour 62 "
  '231)',
  '0'),
 ('Tableau 20 face au tableau 18',
  'Les cinq tranches de moins de 25 ans font exactement les 9 478 orphelins. Les 25 ans et plus '
  '(21 826) = veuves 21 438 + veufs 329 + ascendants 59',
  '0'),
 ('Tableaux 19, 20, 21 face au tableau 28',
  'Vieillesse, survivants, invalidité retombent sur leurs lignes de montant ; avec la majoration '
  "pour enfant (537 343 000), ils font les pensions (45 933 991 634). La majoration n'a pas de "
  'pensionné (A1)',
  '0'),
 ('Tableau 23 face au tableau 25',
  '2 486 nouveaux retraités de vieillesse dans les deux (1 959 hommes, 527 femmes)',
  '0'),
 ('Tableaux 29 à 33 face aux tableaux 28 et 34',
  "Les cinq DR font l'ensemble, mois par mois et ligne par ligne",
  '0'),
 ('Tableau 35 face aux tableaux 39 et 40',
  "3 893 bénéficiaires d'IJ = somme des 12 mois ; 2 865 630 056 FCFA = ligne IJ des PF",
  '0'),
 ('Tableaux 36, 37 et 38',
  '268 481 enfants dans les trois ; allocataires et conjoints identiques par DR ; le montant du '
  'tableau 36 = allocations familiales du tableau 40',
  '0'),
 ('Tableau 40 face aux tableaux 41 à 46',
  "Les cinq DR font l'ensemble, mois par mois et colonne par colonne",
  '0'),
 ('Tableau 47 face aux tableaux 48 à 74',
  '1 171 déclarations en 2024 partout ; de 2020 à 2023, les tableaux 49 à 54 font le tableau 47',
  '0, sauf les étiquettes de 2.2'),
 ('Tableaux 73 et 74 face au tableau 48',
  'Les 22 lieux retombent sur les 5 DR si Bagré (1 accident) va à la DRO',
  '0 (Bagré à vérifier)'),
 ('Tableaux 76 et 78 face aux tableaux 79 et 80',
  'Rentes viagères 1 255 (1 061 hommes, 194 femmes), ayants droit 977, ensemble 2 232',
  '0'),
 ('Soins (82 à 89) et argent (90 à 97) AT/MP',
  "Les DR font l'ensemble ; 2024 retombe sur les séries de cinq ans",
  'Arrondis de 1 ou 2 FCFA'),
 ('Tableaux 89, 97, 99 face au tableau 106',
  'Soins + argent = ensemble AT/MP, chaque année ; = ligne RP du tableau 106 (1 074 113 823 en '
  '2024)',
  '0'),
 ('Tableau 106 face aux tableaux 34 et 40',
  'Pension = tableau 34 ; PF = tableau 40, maternité comprise (R1)',
  '0'),
 ('Tableaux 103 à 105 face aux tableaux 106 et 17',
  'Les charges techniques sont les prestations du tableau 106 ; les cotisations 2024 des trois '
  'branches font le tableau 17',
  '0'),
 ('Fiche signalétique (p. xx) face aux tableaux',
  'Pensions, PF, AT/MP, 62 231 pensionnés, 268 481 enfants, 3 893 IJ, 1 171 victimes, 2 232 '
  "rentes : tout retombe, sauf l'ASS",
  "0, sauf l'ASS")]

NE_COLLE_PAS = [('Flux des pensions : tableaux 18 (2023 et 2024), 23 et 26',
  'Stock fin 2023 + nouveaux 2024 − décès 2024 : veufs 329 et invalidité 119, exacts ; veuves 21 '
  '461 pour 21 438 ; anticipée 77 pour 78. Vieillesse 31 081 pour 30 730 (351 sorties qui ne sont '
  'pas des décès). Orphelins 8 687 + 769 − 2 = 9 454 pour 9 478, alors que des orphelins '
  'atteignent 25 ans chaque année. Ascendants 54 + 7 − 89 = −28 pour 59',
  "Les 89 « décès d'ascendants » ne sont pas des pensions d'ascendant (T50-T57 Q15 ; "
  "points_a_valider n° 57). Le stock d'orphelins 2024 est à faire expliquer (T42-T49 Q20)"),
 ('Flux des rentes : tableaux 80, 75, 77 et 81',
  'Viagères 1 246 + 44 − 21 = 1 269 pour 1 255 (14 autres sorties : des rachats ?) ; veuves et '
  'veufs 287 pour 285 ; ascendants 208 pour 207 ; orphelins 379 + 44 − 0 = 423 pour 485',
  "62 rentes d'orphelins entrées sans être des « nouveaux » publiés (T42-T49 Q20 ; T32-T41 Q12)"),
 ('Tableau 106 (ligne ASS) face au tableau 114 et à la fiche signalétique',
  "650 700 099 FCFA en 2024 : ni les dépenses de l'ASS (1 571 655 315), ni les dépenses moins les "
  'charges de fonctionnement (802 758 809). En 2020, la ligne (1 164 507 418) dépasse même les '
  "dépenses totales de l'ASS (1 022 965 147)",
  'Le montant de la ligne « Autres » de T56 revient à la CNSS (T50-T57 Q18)'),
 ('Tableau 27 face aux tableaux 23 et 24',
  '3 729 « départs à la retraite » pour 3 322 nouvelles pensions de vieillesse, anticipées et '
  'allocations de vieillesse (2 486 + 3 + 833) : +407 (+12,3 %), plus haut dans chaque DR, pic en '
  'janvier (557)',
  'Ce ne sont pas des liquidations ; peut-être des dépôts (T25-T27 Q21)'),
 ('Tableau 24 face aux tableaux 28 à 34',
  "Le nombre d'allocations de vieillesse suit la liquidation, le montant suit le paiement : 5 mois "
  "où l'un vaut 0 et pas l'autre (DRD en janvier, avril et juin ; DRN en août ; DRF en novembre)",
  'Normal : deux dates différentes. Ne pas comparer mois par mois un effectif et un montant'),
 ('Tableau 57 (année 2021) face aux tableaux 53 et 58',
  "Colonnes « F » et « M » inversées pour toute l'année 2021 (1 022 « femmes », 312 « hommes »)",
  "Erreur d'étiquette, sans effet sur T25 à T57"),
 ('Tableau 60 face au tableau 52',
  'En-têtes de colonnes décalés : les chiffres (447, 142, 523, 58, 1) sont ceux de trajet, '
  'déplacement, lieu habituel, lieu occasionnel, domicile',
  "Erreur d'étiquette, chiffres justes"),
 ('Tableaux 49 et 57 face aux tableaux 64, 66, 73 et 74',
  '« Sièges non précisés » et « sièges internes » permutés (10 et 3, puis 3 et 10)',
  '7 victimes changent de ligne ; sans effet sur T25 à T57'),
 ('Tableaux 49 et 51 (année 2022)',
  'Deux pourcentages faux : 0,98 % publié pour 0,30 % (4 sur 1 330) ; 0,08 % publié pour 0,90 % '
  '(12 sur 1 330)',
  'Sans effet sur T25 à T57')]

ANNEES = ['Ce qui tient. Les variations annuelles du tableau 38 (allocataires, conjoints, enfants), '
 "recalculées, tombent à 0,01 point près. Les séries de cinq ans des soins et de l'argent AT/MP "
 "(tableaux 89, 97, 99) et du récapitulatif (tableau 106) s'additionnent chaque année.",
 'Les orphelins de pension montent de 791 en 2024 (8 687 fin 2023, 9 478 fin 2024, tableau 18) '
 'pour 769 nouveaux (tableau 23) : aucune sortie à 25 ans, alors que la base en voit sortir de '
 '900 à 1 900 par an (T32-T41).',
 "Les rentes d'orphelins montent de 106 en 2024 (379, puis 485, tableau 80) pour 44 nouvelles et "
 'aucun décès (tableaux 77 et 81).',
 'Les IJ de maternité de 2020 semblent incomplètes : 1 530 554 843 FCFA pour 3 428 bénéficiaires, '
 'soit 446 486 FCFA par bénéficiaire, contre 618 748 en 2021 et 736 098 en 2024 (tableau 35, '
 'recalculé) ; la base recalcule 2 193 049 270 pour 2020 (T50-T57 N7).',
 "Une année passée corrigée avec une note : les recettes 2023 de l'ASS sont « modifiées » "
 '(tableau 114). La CNSS corrige donc parfois une année déjà publiée (T50-T57 N7, Q6).']

CONTROLES_EN_CLAIR = ("les tableaux d'un même chapitre se recoupent presque toujours à l'unité. Ce qui ne colle pas "
 "porte sur les flux d'une année à l'autre (orphelins, ascendants), sur l'ASS et sur des "
 'étiquettes. Un chiffre publié isolé ne suffit donc pas à juger une règle : on regarde aussi '
 "s'il tient avec ses voisins.")

FAMILLES = [('3.1 T25 à T27 : les dossiers',
  [('T25-T27 D4',
    'Tableau 24 (p. 19)',
    'Allocations de vieillesse liquidées en 2024 (T25)',
    '831 personnes, 832 allocations (première notification, P92, P93)',
    '833',
    '−2 (−0,24 %) ; −1 (−0,12 %)',
    'confirme',
    'La CNSS compte à la notification (la saisie donnerait 873) ; elle publie au mois et par DR, '
    'modèle de N1 et N2'),
   ('T25-T27 D4, D14',
    'Tableau 35 (p. 30)',
    'IJ de maternité de 2020 à 2024 (T27)',
    'Liquidées : 3 438, 3 810, 3 834, 4 069, 3 890',
    '3 428, 3 810, 3 973, 4 050, 3 893',
    '+0,29 %, 0,00 %, −3,50 %, +0,47 %, −0,08 %',
    'précise',
    'Une grossesse = un bénéficiaire par an ; la date ne se départage pas (saisie et ouverture '
    'tombent mieux en 2022 et 2023)'),
   ('T25-T27 N1, N2',
    'Tableau 39 (p. 32)',
    'IJ de maternité de 2024 par mois et par DR',
    "3 890 sur l'année ; pas encore au mois",
    '3 893 (DRO 2 822, DRB 575, DRN 246, DRF 165, DRD 85)',
    '−3 (−0,08 %)',
    'précise',
    'La CNSS suit au mois et par DR ; une comparaison mois par mois dira si le mois publié est '
    'celui de la notification'),
   ('T25-T27 D9',
    'Tableau 47 (p. 38)',
    'Accidents déclarés de 2019 à 2024 (T26, déposés)',
    'Dossiers ouverts : 1 328, 1 403, 1 353, 1 370, 1 226, 1 189',
    '1 303, 1 383, 1 334, 1 330, 1 202, 1 171',
    '+1,42 % à +3,01 % (+1,54 % en 2024)',
    'précise',
    'Une déclaration ≈ un dossier ; t26 comptera les réceptions, plus nombreuses (+6 % en 2025)'),
   ('T25-T27 N8',
    'Tableau 47 (p. 38)',
    'Taux de variation des déclarations, 2024',
    '−3,02 % (dossiers ouverts)',
    '−2,58 %',
    '0,44 point',
    'précise',
    'Même sens, même ordre de grandeur'),
   ('T25-T27 N8',
    'Tableau 38 (p. 31)',
    'La façon de calculer un taux de variation',
    '(n − n−1) ÷ (n−1), recueil l. 1146',
    '« glissement » en %, deux décimales',
    'aucun',
    'confirme',
    'La formule de N8 est la pratique de la CNSS'),
   ('T25-T27 N2, N8',
    'Tableaux 48 et 72 (p. 38 et 58)',
    'Accidents déclarés en 2024 par DR et par type',
    'pas encore par DR',
    'DRO 736, DRB 347, DRN 31, DRF 26, DRD 31 ; 1 150 accidents, 21 maladies',
    '—',
    'précise',
    'Modèle du découpage par DR ; T26 ne sépare pas accident et maladie'),
   ('T25-T27 N2',
    'Tableaux 73 et 74 (p. 58 et 59)',
    'Accidents déclarés en 2024 par « lieu CNSS »',
    'pas calculé',
    '22 lieux, total 1 171',
    '—',
    'précise',
    'Modèle publié du découpage « par agence »'),
   ('T25-T27 N2',
    'Pages xv à xix',
    'Les directions et les agences',
    '15 agences et 5 sièges de DR dans la base, 4 agences inactives (P46)',
    '16 agences (DRO 5, DRB 6, DRN 2, DRF 2, DRD 1)',
    'à rapprocher',
    'précise',
    'Appuie « agence = service du dossier » ; donne les destinataires des questions'),
   ('T25-T27 N2',
    'Tableau 22 (p. 18)',
    'Pensionnés au 31/12/2024 par DR, DR du dossier',
    'Comparaison A02',
    '62 231',
    'Vieillesse +0,1 % à +4,0 % ; ascendants exacts ; veuves −8,1 % (DRB), −11,7 % (DRD)',
    'précise',
    'La DR du dossier donne des chiffres plausibles, rien de plus'),
   ('T25-T27 R2',
    'Tableau 23 (p. 19)',
    'Nouveaux pensionnés de 2024 (vieillesse et anticipée)',
    '2 397 notifiés ; 2 463 à la saisie (P82)',
    '2 489 (2 486 + 3 anticipées)',
    '−92 (−3,7 %) ; −26 (−1,0 %)',
    'précise',
    "L'anticipée est publiée à part (R2) ; des personnes à la saisie, et non des demandes à la "
    'notification'),
   ('T25-T27 Q21',
    'Tableau 27 (p. 21)',
    '« Départs à la retraite » de 2024',
    'pas calculé',
    '3 729',
    '—',
    'non comparable',
    'Peut-être le seul chiffre publié de dépôts ; 12,3 % au-dessus des 3 322 entrées publiées'),
   ('T25-T27 · T26 rentes viagères',
    'Tableau 75 (p. 60)',
    'Nouvelles rentes viagères de 2024',
    '39 premières rentes ; 57 notifiées ; 69 saisies (68 personnes) (P82, P91)',
    '44',
    '−5 (−11,4 %)',
    'écart à expliquer',
    'La CNSS semble compter quelques révisions comme nouvelles ; écart déjà noté au lot 7'),
   ('T25-T27 D2',
    'Tableau 77 (p. 61)',
    'Nouveaux ayants droit de rentes en 2024',
    '63 personnes (P82)',
    '61',
    '+2 (+3,3 %)',
    'précise',
    'Des personnes, pas des demandes : la ligne « Rentes de survivants » de T26 sera plus basse'),
   ('T25-T27 A1',
    'Tableaux 28 et 34 (p. 22 et 28)',
    'Majoration pour enfant 2024',
    '539 376 000 (P98)',
    '537 343 000, rangés avec la pension',
    '+2 033 000 (+0,4 %)',
    'confirme',
    'Un accessoire suit la pension'),
   ('T25-T27 N3',
    'Tableaux 40 et 41 (p. 32 et 33)',
    'Frais médicaux de maternité 2024 (T27)',
    '0 (P95)',
    '25 030 869, payés tous les mois, dans 4 DR sur 5',
    'tout le montant',
    'contredit',
    'Contredit la fiche T27 (« jamais payés ») : payés, mais hors de la base lue (Q15) ; confirme '
    'R1'),
   ('T25-T27 D11',
    'Tableaux 82, 89, 98 et 99 (p. 63 à 75)',
    'Soins AT/MP 2024 (T26, « Autres prestations »)',
    '18 787 945, 692 débours (P95)',
    '139 668 695',
    '−120 880 750 (−86,5 %)',
    'écart à expliquer',
    'Des dossiers de soins peuvent manquer à T26 ; question déjà posée à la DSI (README)'),
   ('T25-T27 Q11',
    'Tableaux 90, 96 et 97 (p. 71 et 74)',
    'Rachats de rente AT/MP 2024',
    '11 rachats, 21 519 361 (P104)',
    '25 366 909',
    '−3 847 548 (−15,2 %)',
    'précise',
    'Le rachat est une prestation à part : faut-il le compter dans T26 ?'),
   ('T25-T27 · T27',
    'Tableau 118 (p. 88)',
    '« Dossiers médicaux PF » des centres SMI',
    '—',
    '4 555 dossiers médicaux, 368 dossiers IJ',
    '—',
    'non comparable',
    'Des dossiers vus par les médecins, pas des dossiers de prestations')]),
 ('3.2 T28 à T31 : les contrôles a posteriori et les indus',
  [('T28-T31 R3, R4',
    'Tableaux 28 et 34 (p. 22 et 28)',
    'Payées PVID de 2024 (T31)',
    '47 237 434 040 au mois payé ; 47 486 010 786 à la saisie (P105)',
    '46 629 625 556',
    '+1,30 % ; +1,84 %',
    'précise',
    'R3 tient pour les pensions ; au mois, la saisie reproduit le rythme publié (corrélation 0,75 '
    "contre −0,12) ; taux d'indus PVID inchangé (0,47 %)"),
   ('T28-T31 N1',
    'Tableaux 28 et 34, lignes à zéro',
    '« Solidarité », « Pension gratuite », « Remboursements »',
    'Aucun code de solidarité (P34) ; pension gratuite de 2010 à 2015 seulement (P11)',
    '0, chaque mois et dans chaque DR',
    'aucun',
    'confirme',
    'Conforte la suppression de la pension de solidarité de T28 par Kaya 2017'),
   ('T28-T31 N1',
    'Tableaux 18, 22 et 23 (p. 16 à 19)',
    'Retraite anticipée au 31/12/2024 (ligne de T28)',
    '87 pensions en cours (A01)',
    '78',
    '+9 (+11,5 %)',
    'précise',
    "La ligne à part existe dans les effectifs publiés ; dans les sommes servies, l'anticipée est "
    'fondue dans la vieillesse'),
   ('T28-T31 R1',
    'Tableaux 40 et 41 (p. 32 et 33)',
    'Où va la maternité',
    'Maternité avec les PF (R1)',
    'Dans les PF ; tableau 106 : même total',
    'aucun',
    'confirme',
    'R1 confirmée pour T31 et T30'),
   ('T28-T31 R4',
    'Tableaux 40 et 41 (p. 32 et 33)',
    'Payées PF de 2024 (T31)',
    '10 750 873 342 au mois payé ; 10 802 063 700 à la saisie (P105)',
    '11 180 191 425',
    '−3,84 % ; −3,38 %',
    'précise',
    "Les frais médicaux n'expliquent que 0,2 point ; au mois, la saisie suit bien mieux"),
   ('T28-T31 R3',
    'Tableaux 90, 96 et 97 (p. 71 et 74)',
    'Payées AT/MP de 2024 face aux prestations en argent',
    '744 609 484 au mois payé ; 790 138 121 à la saisie (P105)',
    '934 445 128',
    '−20,3 % ; −15,4 %',
    'écart à expliquer',
    "Les payées AT/MP sont trop basses : taux d'indus AT/MP 0,47 % sur nos payées, 0,38 % sur le "
    'publié'),
   ('T28-T31 N7',
    'Tableaux 82, 98, 99 et 106',
    'Payées AT/MP, soins remboursés compris (comme T56)',
    '763 397 429 (P95)',
    '1 074 113 823',
    '−28,9 %',
    'écart à expliquer',
    'Savoir où sont payés les soins et une partie des rentes (question à la DSI, README ; Q22)'),
   ('T28-T31 N2',
    'Tableaux 90, 96 et 97, colonne « rachat de rente »',
    'Rachats de rente de 2024 (ligne de T29)',
    '21 519 361 (11 débours de rachat, P104)',
    '25 366 909',
    '−15,2 % (−3 847 548)',
    'écart à expliquer',
    'La CNSS compte le rachat dans les prestations servies (ligne « achat de rente » de T29) ; '
    "l'écart reste à expliquer"),
   ('T28-T31 N7',
    'Tableau 106 (et 103 à 105)',
    'Total des payées de 2024, trois branches',
    '58 732 916 866 au mois payé ; 59 078 212 607 à la saisie (P105)',
    "58 883 930 804 (sans l'ASS)",
    '−0,26 % ; +0,33 %',
    'précise',
    "Juste par compensation : pensions +1,3 %, PF −3,8 %, AT/MP −30,7 % ; taux d'indus 0,391 % "
    'contre 0,390 %'),
   ('T28-T31 N4',
    'Tableaux 35 et 39 (p. 30 et 32)',
    "Bénéficiaires d'IJ de maternité de 2024 (ligne de T30)",
    '3 890 liquidées ; 4 881 payées (P93)',
    '3 893',
    '−0,1 % ; +25 %',
    'précise',
    "La CNSS compte l'année de la liquidation ; avec N4, la ligne serait bien plus haute (Q7)"),
   ('T28-T31 N4',
    'Tableau 24 (p. 19)',
    'Allocations de vieillesse de 2024 (ligne de T28)',
    '832 liquidées (831 personnes) ; 753 payées (P93)',
    '833',
    '−1 ; −80',
    'précise',
    'Même pratique : compté à la liquidation (Q7)'),
   ('T28-T31 N4',
    'Tableaux 37 et 38 (p. 30 et 31)',
    'Allocataires de 2024 (ligne de T30)',
    "104 611 payés dans l'année ; 98 048 au 31/12 et payés (P79)",
    '97 378 au 31/12',
    '+7,4 % ; +0,7 %',
    'précise',
    'La CNSS compte un stock, différent de N4 ; avec N4, environ 5 230 contrôles pour atteindre 5 '
    '%'),
   ('T28-T31 N4 (T28)',
    'Tableaux 18 et 22 (p. 16 et 18)',
    'Pensions au 31/12/2024 face aux dossiers de pension payés',
    '47 732 dossiers payés en 2025 (P13)',
    '62 231 pensions',
    '—',
    'non comparable',
    "Une pension n'est pas un dossier ; utile pour Q4 et le dénominateur de T28"),
   ('T28-T31 N4 (T29)',
    'Tableaux 79 et 80 (p. 61 et 62)',
    'Rentes AT/MP au 31/12/2024 face aux dossiers AT/MP payés',
    '2 134 dossiers avec un débours en 2025 (P13)',
    '2 232 rentes',
    '—',
    'non comparable',
    'Un ordre de grandeur seulement pour les lignes de rentes de T29'),
   ('T28-T31 N9',
    'Tableaux 29 à 34, 40, 42 à 46, 91 à 96 et 98',
    'Sommes servies par DR',
    'pas encore calculé',
    'publié pour les trois branches',
    '—',
    'précise',
    "Contrôle des payées par DR, dénominateur du taux d'indus par DR"),
   ('T28-T31 R13',
    'Tableau 26 (p. 21)',
    'Décès de pensionnés de 2024',
    '444 décès de retraités connus fin mars 2025 (P99)',
    '447 retraités de vieillesse ; 648 en tout',
    '−3 (−0,7 %)',
    'précise',
    'Indice fort pour R13 (cliché de fin mars N+1), comme T50-T57 ; repère pour Q18'),
   ('T28-T31 C1',
    'Tableaux 100 et 101 (p. 76)',
    '« Entreprises contrôlées » en 2024',
    '115 contrôles de dossiers en 2025 (P13)',
    '178 entreprises (DPASS 107, DRB 71)',
    'sans objet',
    'non comparable',
    'Des contrôles de prévention, pas des contrôles de dossiers'),
   ('T28-T31 · hors famille',
    'Tableaux 102 (p. 77) et 11 (p. 11)',
    'CSST ; employeurs contrôlés',
    '—',
    '16 comités renouvelés, 23 nouveaux ; 8 794 employeurs',
    'sans objet',
    'non comparable',
    'Le tableau 11 sert au recouvrement'),
   ('T28-T31 · hors famille',
    'Tableaux 107 à 113 (p. 80 à 83), avec le tableau 8',
    'Délai de remise en banque des titres de paiement',
    '—',
    "38 413 titres, dont 5 568 en moins d'un jour (14,5 %)",
    'sans objet',
    'non comparable',
    'Un indicateur du recouvrement ; rien pour cette famille')]),
 ('3.3 T32 à T41 : les bénéficiaires au 31 décembre',
  [('T32-T41 R6',
    'Tableau 18 (p. 16)',
    'Vieillesse et anticipée, 2020 à 2024 (T32)',
    '25 273 ; 26 587 ; 28 077 ; 29 265 ; 31 093 personnes (P74)',
    '25 230 ; 26 481 ; 27 941 ; 29 117 ; 30 808',
    '+0,2 % ; +0,4 % ; +0,5 % ; +0,5 % ; +0,9 %',
    'confirme',
    'R6 est la définition de la CNSS (payée ou non) ; exiger un paiement donnerait 27 670 en 2024 '
    '(−10,2 %)'),
   ('T32-T41 R6',
    'Tableau 18 (p. 16)',
    'Invalidité, 2020 à 2024 (T33)',
    '111 ; 109 ; 119 ; 121 ; 125',
    '114 ; 111 ; 115 ; 116 ; 119',
    '−2,6 % à +5,0 %',
    'confirme',
    'Quelques unités sur de petits nombres'),
   ('T32-T41 R6, N3',
    'Tableaux 18 et 22 (p. 16 et 18)',
    'Ascendants 2024 (T36)',
    '59 ; par DR 48 / 8 / 1 / 1 / 1',
    '59 ; 48 / 8 / 1 / 1 / 1',
    '0',
    'confirme',
    'Exact, dans chacune des 5 DR'),
   ('T32-T41 R6',
    'Tableaux 18 et 80 (p. 16 et 62)',
    'Ascendants 2020 à 2023 (T36, T40)',
    'T36 : 58 ; 60 ; 65 ; 59. T40 : 225 ; 218 ; 230 ; 214',
    'T36 : 48 ; 53 ; 59 ; 54. T40 : 185 ; 187 ; 193 ; 196',
    'T36 +9,3 % à +20,8 % ; T40 +9,2 % à +21,6 %',
    'écart à expliquer',
    "Seule 2024 tombe juste ; pensions créées après coup ou autre définition à l'époque : pas "
    'bloquant'),
   ('T32-T41 R6',
    'Tableau 18 (p. 16)',
    'Veuves et veufs, 2020 à 2024 (T34)',
    '19 869 ; 20 567 ; 21 154 ; 20 258 ; 20 715',
    '19 534 ; 20 213 ; 21 008 ; 21 130 ; 21 767',
    '+1,7 % ; +1,8 % ; +0,7 % ; −4,1 % ; −4,8 % (veuves 2024 : −1 079, −5,0 %)',
    'écart à expliquer',
    "Tout l'écart est sur les veuves ; piste : décès ou radiations saisis après l'arrêt de "
    "l'annuaire (A06, A07 ; Q4)"),
   ('T32-T41 R5, R6',
    'Tableau 20 (p. 17)',
    'Les orphelins comptés à leur âge, moins de 25 ans (T35)',
    'La requête compte les orphelins de moins de 25 ans',
    '9 478, tous de moins de 25 ans',
    'aucun sur la règle',
    'confirme',
    'La limite de 25 ans est la pratique de la CNSS et la demande du recueil (l. 283)'),
   ('T32-T41 R6',
    'Tableaux 18, 20, 23 et 26',
    'Orphelins pensionnés, 2020 à 2024 (T35)',
    '9 686 ; 9 535 ; 9 276 ; 8 108 ; 7 970',
    '8 982 ; 8 789 ; 8 667 ; 8 687 ; 9 478',
    '+7,8 % ; +8,5 % ; +7,0 % ; −6,7 % ; −15,9 %',
    'écart à expliquer',
    'Le chiffre 2024 de la CNSS ne tient pas avec ses propres entrées ; A07 et A08 à exécuter'),
   ('T32-T41 N8',
    'Tableaux 18, 23 et 26',
    'Entrées et sorties de 2024',
    '29 265 + 2 463 − 31 093 = 635 sorties de retraités, presque toutes des décès (619 connus, '
    'P97)',
    'Vieillesse 31 081 (30 730) ; veufs 329 (329) ; invalidité 119 (119) ; orphelins 9 454 (9 '
    '478) ; ascendants −28 (59)',
    "351 retraités sortis sans être des décès ; orphelins 24 de trop ; 89 décès d'ascendants pour "
    '54',
    'précise',
    'Les chiffres publiés ne tiennent pas toujours entre eux : la BI montre ce contrôle à côté de '
    'chaque tableau'),
   ('T32-T41 R8',
    'Tableau 19 (p. 16)',
    'Grille et sexe des retraités (T32)',
    '24 814 hommes, 6 279 femmes (anticipée comprise)',
    '24 627 hommes, 6 103 femmes (vieillesse seule)',
    'Hommes +187 (+0,8 %) ; femmes +176 (+2,9 %)',
    'confirme',
    'La grille et le code du sexe sont ceux de la CNSS ; comparaison à refaire sur la vieillesse '
    'seule'),
   ('T32-T41 R8',
    'Tableaux 19 et 25 (p. 16 et 20)',
    'Forme par âge (T32)',
    '55-59 ans 3 778 ; 55-64 ans 12 632 ; 65-89 ans 18 080',
    '3 399 ; 12 037 ; 18 341',
    '+379 (+11,2 %) ; +595 ; −261',
    'écart à expliquer',
    "Plus de jeunes, moins d'âgés : âge calculé à une autre date, ou base qui a vécu depuis "
    '(Q10)'),
   ('T32-T41 R3',
    'Tableaux 19 et 34 (p. 16 et 28)',
    'Montant de T32',
    'Vieillesse et anticipée : 37 665 983 112 aux comptés, 38 625 184 454 tout le payé ; '
    'vieillesse seule : 38 496 066 498 au mois payé, 38 821 571 458 à la saisie',
    '38 235 880 033',
    '−1,5 % ; +1,0 % ; +0,7 % ; +1,5 %',
    'précise',
    'Le publié tombe entre nos montants ; Q1 et Q2 restent ouvertes'),
   ('T32-T41 A1, N13',
    'Tableaux 28 et 34 (p. 22 et 28)',
    'Majoration pour enfant 2024',
    '539 376 000 à la saisie ; 554 254 000 au mois payé (P98)',
    '537 343 000, à part de la pension',
    '+0,4 % ; +3,1 %',
    'confirme',
    "L'accessoire ne fait pas un pensionné de plus ; son montant reste hors de T32"),
   ('T32-T41 R8',
    'Tableau 20 (p. 17)',
    'Survivants de 100 ans et plus',
    '635 femmes, dont 500 de 120 ans et plus (« nées en 1900 ») ; 135 sans elles',
    '679 femmes, aucun homme',
    '−44 en gardant ces dates ; −544 sans elles',
    'tranche',
    'Les dates par défaut vont dans « 100 ans et plus » : réglage ages_impossibles = '
    'DERNIERE_TRANCHE, avec un contrôle'),
   ('T32-T41 R6',
    'Tableau 20 (p. 17)',
    'Veuves, veufs et ascendants par âge (T34, T36)',
    '20 777 ; 25-54 ans 2 314 ; 55-99 ans 17 828 ; 100 ans et plus 635 (P76)',
    '21 826 ; 2 315 ; 18 832 ; 679',
    '−1 049 (−4,8 %)',
    'précise',
    'Les veuves qui manquent ont presque toutes 55 ans ou plus : pas des remariages ; décès ou '
    'radiations tardifs (Q4)'),
   ('T32-T41 R3',
    'Tableaux 20, 21 et 36',
    'Montants 2024 des survivants, des invalides, des allocations familiales',
    'Survivants 7 161 046 495 / 7 222 987 423 / 7 184 934 762 ; invalidité 112 058 723 / 116 130 '
    '355 / 113 131 108 ; allocations familiales 7 688 913 000 / 7 954 458 000 / 8 000 440 000 '
    '(aux comptés / tout le payé / à la saisie)',
    '7 047 164 871 ; 113 603 730 ; 8 236 741 500',
    'Survivants +1,6 % ; +2,5 % ; +2,0 %. Invalidité −1,4 % ; +2,2 % ; −0,4 %. Allocations '
    'familiales −6,7 % ; −3,4 % ; −2,9 %',
    'précise',
    'La CNSS aligne ses montants sur la comptabilité ; Q1, Q2, Q9'),
   ('T32-T41 R8, N5',
    'Tableau 21 (p. 18)',
    'Invalides par âge et sexe (T33)',
    '125 (91 hommes, 34 femmes) ; 72 de 60 ans et plus',
    '119 (86 hommes, 33 femmes) ; 70 de 60 ans et plus',
    '+6 (+5,0 %)',
    'confirme',
    "La grille de T33 est celle de la CNSS ; l'invalidité est gardée après 60 ans"),
   ('T32-T41 N3',
    'Tableau 22 (p. 18)',
    'Pensionnés par DR',
    'Vieillesse 20 352 / 7 638 / 1 366 / 1 007 / 818 ; veuves et veufs 10 744 / 7 031 / 1 473 / '
    '917 / 563 (A02)',
    'Vieillesse 20 067 / 7 538 / 1 340 / 968 / 817 ; veuves et veufs 11 123 / 7 647 / 1 471 / 890 '
    '/ 636',
    'Vieillesse +0,1 % à +4,0 % ; ascendants exacts ; veuves et veufs DRB −8,1 %, DRD −11,5 %',
    'précise',
    'La DR du dossier est plausible, comme pour T25-T27 et T42-T49'),
   ('T32-T41 N6',
    'Tableau 36 (p. 30)',
    "Enfants d'allocataires par âge et sexe (T41)",
    '271 577 enfants payés (P79) ; par âge, sans la condition de paiement : 71 155 ; 79 887 ; 68 '
    '923 ; 50 538 ; 13 016',
    '268 481 ; 59 869 ; 74 632 ; 65 913 ; 48 889 ; 19 178',
    'Total +1,2 % ; moins de 5 ans +18,9 % ; 20-21 ans −32,1 %',
    'écart à expliquer',
    'La limite de 21 ans est confirmée, pas la forme par âge : la CNSS compte peut-être les '
    'enfants sortis à 21 ans (Q10, Q11)'),
   ('T32-T41 R6 (T41)',
    'Tableaux 37 et 38 (p. 30 et 31)',
    'Allocataires et enfants 2024 (T41)',
    '98 048 allocataires ; 271 577 enfants (P79)',
    '97 378 ; 268 481',
    '+0,7 % ; +1,2 % (sans la condition de paiement : +5,4 %, +5,6 %)',
    'confirme',
    "La condition « enfant payé dans l'année » retombe en 2024"),
   ('T32-T41 R6 (T41)',
    'Tableau 38 (p. 31)',
    'Allocataires 2020 à 2023 (T41)',
    '93 972 ; 96 656 ; 98 570 ; 98 128',
    '84 401 ; 88 587 ; 91 085 ; 93 233',
    '+11,3 % ; +9,1 % ; +8,2 % ; +5,3 %',
    'écart à expliquer',
    'Non expliqué ; 2024 seule ne suffit pas à confirmer la règle'),
   ('T32-T41 R6, N3',
    'Tableau 76 (p. 60)',
    'Rentiers viagers 2024 par âge et sexe (T37)',
    '1 236 (1 046 hommes, 190 femmes)',
    '1 255 (1 061 hommes, 194 femmes)',
    '−19 (−1,5 %)',
    'confirme',
    "T37 se calcule comme la CNSS ; sa grille s'arrête à « 90 ans et plus »"),
   ('T32-T41 R6',
    'Tableaux 78, 79 et 80',
    'Rentes en cours 2024 (T37, T38, T40) et 2020 à 2023',
    '2024 : 1 236 ; 288 ; 214',
    '2024 : 1 255 ; 285 ; 207',
    '2024 : −1,5 % ; +1,1 % ; +3,4 %. Avant : T37 −0,6 % à +4,2 % ; T38 +2,8 % à +5,1 %',
    'confirme',
    'R6 vaut pour les rentes ; « ensemble au 31-12 » appuie la lecture « stock » du « cumul » de '
    'T38 (Q14)'),
   ('T32-T41 Q12',
    'Tableaux 80, 77 et 81',
    "Rentes d'orphelins 2020 à 2024 (T39)",
    'Moins de 25 ans : 475 ; 482 ; 497 ; 450 ; 454',
    '492 ; 421 ; 399 ; 379 ; 485',
    '−3,5 % ; +14,5 % ; +24,6 % ; +18,7 % ; −6,4 %',
    'écart à expliquer',
    "Le chiffre publié saute de 106 en 2024 pour 44 entrées ; limite d'âge à demander, 25 ans en "
    'attendant'),
   ('T32-T41 R3',
    'Tableaux 90, 96 et 97',
    'Montants 2024 des rentes (T37 à T40)',
    'Viagères : 399 428 069 / 397 834 986 / 384 111 754 ; survivants : 275 923 240 / 322 631 157 '
    '/ 274 761 735 (mois payé / saisie / aux comptés)',
    '455 021 317 ; 368 602 710',
    'Viagères −12,2 % ; −12,6 % ; −15,6 %. Survivants −25,1 % ; −12,5 % ; −25,5 %',
    'écart à expliquer',
    'Une partie des paiements des rentes manque dans la base (Q21) : montants présentés avec une '
    'réserve'),
   ('T32-T41 R4, N1, N3',
    'Tableaux 28 à 33 (p. 22 à 27)',
    'Prestations vieillesse servies mois par mois, ensemble et par DR',
    'pas encore comparé',
    'Survivants : 396 à 424 millions en janvier, avril, juillet et octobre ; 602 à 773 millions '
    'les autres mois',
    '—',
    'précise',
    'Profil très marqué : comparer mois par mois peut trancher R4 sans attendre la CNSS'),
   ('T32-T41 R4',
    'Tableaux 90 à 95 (p. 71 à 73)',
    'Rentes servies mois par mois, ensemble et par DR',
    'pas encore comparé',
    'Rentes viagères : 22 à 25 millions en janvier, avril, juillet et octobre ; 39 à 51 millions '
    'les autres mois',
    '—',
    'précise',
    'Même test de R4 pour T37 à T40'),
   ('T32-T41 N1, N3',
    'Tableaux 40 à 46 (p. 32 à 36)',
    'Allocations familiales par DR et par mois (T41)',
    'pas encore comparé',
    'DRO 4 970 538 500 ; DRB 1 818 839 500 ; DRN 650 602 000 ; DRF 498 528 000 ; DRD 298 233 500',
    '—',
    'précise',
    'Montant de T41 par DR et par mois'),
   ('T32-T41 N3',
    'Tableaux 34 et 96 (p. 28 et 74)',
    'Montants par DR',
    'pas encore comparé',
    'Pension de retraite : DRO 26 978 969 899 ; DRB 9 088 718 494 ; DRN 937 252 400 ; DRF 664 250 '
    '549 ; DRD 566 688 691',
    '—',
    'précise',
    'Contrôle des montants par DR'),
   ('T32-T41 R8',
    'Tableau 25 (p. 20)',
    'Nouveaux retraités de 2024 par âge exact et par sexe',
    '377 de 60 ans, 353 de 61 ans ; 641 nouvelles retraitées (P83)',
    '519 ; 256 ; 527 femmes',
    "Décalage d'âge ; femmes +114",
    'précise',
    "La date de calcul de l'âge est à vérifier avant de dire R8 confirmée en entier (Q10)"),
   ('T32-T41 R3',
    'Tableaux 106 et 97 (p. 80 et 74)',
    'Montants servis de 2020 à 2024',
    'pas encore comparé',
    'Pensions : de 35 232 227 700 (2020) à 46 629 625 556 (2024)',
    '—',
    'précise',
    "Tester les règles de montant sur cinq ans ; dire si le manque d'environ 12 % des paiements "
    'AT/MP est constant'),
   ('T32-T41 R5, R13',
    'Démarche et fiche signalétique (p. xiii, xiv et xx)',
    'Comment la CNSS fait son annuaire',
    '—',
    'Base exploitée par la DPC, collecte dans chaque DR, montants conformés au compte de '
    'résultat, atelier, publication en juin 2025',
    '—',
    'précise',
    "On ne retombera jamais à l'unité ; arrêt vers fin mars (R13) ; 62 231 additionne des "
    'pensions, pas des personnes (R5)')]),
 ('3.4 T42 à T49 : les nouveaux bénéficiaires',
  [('T42-T49 R7',
    'Tableau 23 (p. 19)',
    "Nouveaux pensionnés de 2024 par type : la date d'entrée",
    'Première fois, à la saisie : T42 2 463, T43 771, T44 773 (P82). Toutes pensions : 4 090 '
    'saisies, 3 888 notifiées (A03)',
    'T42 2 489 ; T43 766 ; T44 769 ; total 4 037',
    'Saisie : −1,0 % ; +0,7 % ; +0,5 % ; toutes +1,3 %. Notification : −3,7 %',
    'tranche',
    'La pratique de la CNSS est la saisie : R7 la garde ; Q2 demande une confirmation '
    '(contre-indice de la feuille 03)'),
   ('T42-T49 R7, N6',
    'Tableau 23, vieillesse et anticipée',
    "Les pensions de coordination payées par l'autre caisse",
    'Avec : 2 463 ; sans les 234 de coordination : 2 229',
    '2 489',
    'Avec −1,0 % ; sans −10,4 %',
    'tranche',
    'La CNSS compte la coordination payée ailleurs : on la garde'),
   ('T42-T49 R7',
    'Tableau 23, vieillesse et anticipée',
    'Les nouveaux sortis avant le 31/12, les révisions',
    'Sans les 35 sortis : 2 428 ; avec les révisions : 2 510',
    '2 489',
    '−2,5 % ; +0,8 %',
    'précise',
    'Les chiffres ne départagent ni les sortis ni les révisions ; les sortis restent comptés '
    '(Annuaire CIPRES p. 15)'),
   ('T42-T49 R8',
    'Tableau 25 (p. 20), âges',
    'Âge des nouveaux retraités, par tranche de T42',
    '55-59 ans 1 450 ; 60-64 ans 941 ; 65-69 ans 89 ; 70 ans et plus 25 (P83)',
    '1 429 ; 956 ; 89 ; 11',
    '+21 (+1,5 %) ; −15 (−1,6 %) ; 0 ; +14',
    'confirme',
    "R8 suffit par tranche, de 55 à 69 ans ; âge par âge, nos âges glissent souvent d'un an (Q9)"),
   ('T42-T49 · T42 sexe',
    'Tableau 25 (p. 20), sexe',
    'Sexe des nouveaux retraités de 2024',
    '1 865 hommes, 641 femmes (25,6 %)',
    '1 959 hommes, 527 femmes (21,2 %)',
    'Hommes −94 (−4,8 %) ; femmes +114 (+21,6 %)',
    'écart à expliquer',
    "Le sexe de la base retombe ailleurs : l'écart est propre au tableau 25 ; on garde le sexe de "
    'la fiche (Q8)'),
   ('T42-T49 R7 (T48)',
    'Tableau 75 (p. 60)',
    'Nouvelles rentes viagères de 2024, par âge et sexe',
    'Première fois : 39 ; notifiées : 57 ; saisies : 68 personnes (P82, P84, P91)',
    '44 (32 hommes, 12 femmes), tous de 25 à 64 ans',
    '−11,4 % ; +29,5 % ; +54,5 %',
    'écart à expliquer',
    'Aucune règle testée ne donne 44 ; deux variantes à tester avant de poser Q6 (lot 10)'),
   ('T42-T49 R2, R5, R7',
    'Tableau 77 (p. 61)',
    'Nouveaux orphelins (T46), veuves et veufs (T49) de rentes',
    'T46 44 ; T49 4',
    '44 ; 4',
    '0 ; 0',
    'confirme',
    'T46 et T49 tombent juste'),
   ('T42-T49 R2 (T47)',
    'Tableau 77 (p. 61)',
    'Nouveaux ascendants de rentes de 2024',
    '15 (saisis, premières fois et notifiés, P82)',
    '13',
    '+2 (+15,4 %)',
    'écart à expliquer',
    "La date n'y change rien ; regarder ces 15 personnes (lot 10) et demander (Q7)"),
   ('T42-T49 N4',
    'Tableau 77, lecture du recueil',
    'La personne comptée face à « Idem. Tab 41 »',
    "En comptant l'ayant droit : 44 orphelins, 15 ascendants, 4 veuves",
    'Classés « selon la nature du bénéficiaire » : 44, 13, 4',
    'pas chiffrable',
    'précise',
    'La CNSS compte le survivant ; elle ne peut pas contredire le recueil : la décision reste à '
    'la CNSS (Q1)'),
   ('T42-T49 R7 (T45)',
    'Tableaux 75 et 77 ensemble',
    'Nouveaux rentiers de 2024 et les recalculs',
    'Première fois 102 ; avec les recalculs 131 ; notifiées 123',
    '105',
    '−2,9 % ; +24,8 % ; +17,1 %',
    'confirme',
    "Une rente recalculée n'est pas une entrée ; le périmètre de T45 repose sur le recueil"),
   ('T42-T49 Q20',
    'Tableaux 18, 23 et 26',
    'Stock fin 2023 + nouveaux 2024 − décès 2024',
    'non calculé avec nos données',
    'Veufs 329 (329) ; invalidité 119 (119) ; vieillesse 31 081 (30 730) ; orphelins 9 454 (9 '
    '478) ; ascendants −28 (59)',
    'Vieillesse −351 ; orphelins +24 ; ascendants +87',
    'non comparable',
    "Ce contrôle n'utilise que des chiffres publiés : il ne dit rien de la saisie ni du cliché"),
   ('T42-T49 R5, R6',
    'Tableau 20 (p. 17)',
    'Orphelins de pension au 31/12/2024, par âge',
    '—',
    'Les moins de 25 ans font exactement les 9 478 orphelins, dont 3 812 de 20 à 24 ans',
    '—',
    'confirme',
    "L'orphelin compte à son âge, limite de 25 ans ; le stock ne peut pas monter de 791 avec 769 "
    'entrées (Q20)'),
   ('T42-T49 Q20',
    'Tableaux 80, 81, 75 et 77',
    'Stock des rentes fin 2023 + nouveaux 2024 − décès 2024',
    'T46 2024 : 44',
    'Viagères 1 269 (1 255) ; veuves et veufs 287 (285) ; ascendants 208 (207) ; orphelins 423 '
    '(485)',
    'Orphelins +62 ; viagères −14',
    'écart à expliquer',
    'Sans effet sur T46 de 2024 ; des orphelins entrent dans le stock sans être des « nouveaux » '
    '(T39)'),
   ('T42-T49 A1',
    'Tableaux 19 et 28 (p. 16 et 22)',
    "Ce que la CNSS met dans le montant d'un pensionné",
    'Pension de vieillesse 2024 (mois payé) 38 496 066 498 ; bonification pour enfant 554 254 000 '
    '(P98)',
    '38 235 880 033 ; majoration 537 343 000, à part',
    '+0,7 % ; +3,1 %',
    'précise',
    'Le montant de T42 (pension seule) suit la pratique de la CNSS ; aucun montant de nouveaux '
    "n'est publié"),
   ('T42-T49 R3',
    'Tableau 90 (p. 71)',
    'Montants 2024 des rentes (tout le stock)',
    'Mois payé : viagères 399 428 069 ; survivants 275 923 240. Saisie : 397 834 986 ; 322 631 '
    '157 (P98)',
    '455 021 317 ; 368 602 710',
    'Mois payé −12,2 % ; −25,1 %. Saisie −12,6 % ; −12,5 %',
    'écart à expliquer',
    'Les montants de T45 à T49 seront trop bas (Q5) ; pour les survivants, la saisie colle mieux '
    ': indice pour R4'),
   ('T42-T49 N5',
    'Tableaux 96 et 97 (p. 74)',
    'Montants des rentes par DR (2024) et de 2020 à 2024',
    'pas calculé',
    'Viagères DRO 285 513 230 ; survivants 224,7 M (2020), 333,0 M (2023), 368,6 M (2024)',
    '—',
    'précise',
    'Pour contrôler N5 sur les montants et suivre la règle du montant sur cinq ans'),
   ('T42-T49 R4',
    'Montants mensuels des tableaux 28, 41 et 90',
    'La date des paiements que la CNSS publie',
    'Corrélation à la saisie 0,75 (pensions), 0,84 (AT/MP) ; au mois payé −0,12, −0,08 (P105)',
    'Pics en février, mai, août, novembre',
    '—',
    'précise',
    "Le profil publié suit la saisie : appuie 'SAISIE' pour R4, sans trancher pour les nouveaux "
    '(Q3)'),
   ('T42-T49 R2',
    'Tableau 24 (p. 19)',
    'Allocations de vieillesse liquidées en 2024',
    'Hors T42 ; 831 liquidées, 873 saisies (P92)',
    '833',
    '−0,2 % ; +4,8 %',
    'confirme',
    'Les allocations restent hors de T42 ; la CNSS compte les allocations à la notification, les '
    'pensions à la saisie'),
   ('T42-T49 R2',
    'Tableau 28 (p. 22)',
    'Allocations de survivants de 2024',
    "Hors T43 et T44 ; 81 parts de conjoint, 472 d'orphelin, 18 d'ascendant (P93)",
    '197 906 991, à part des pensions',
    '—',
    'confirme',
    "L'allocation de survivants reste hors de T43 et T44 (elle va dans T51)"),
   ('T42-T49 · T42 (T25-T27 Q21)',
    'Tableau 27 (p. 21)',
    '« Départs à la retraite » de 2024',
    'pas de chiffre comparable',
    '3 729 ; 3 322 dans les tableaux 23 et 24',
    '+407 (+12,3 %)',
    'non comparable',
    "Ce n'est pas T42 : ne pas s'en servir pour un T42 mensuel"),
   ('T42-T49 · T48',
    'Tableau 76 (p. 60)',
    'Stock des rentes viagères fin 2024, par DR, âge et sexe',
    'pas calculé',
    '1 255 (1 061 hommes, 194 femmes)',
    '—',
    'précise',
    'Éclaire les libellés du tableau 75 ; contrôle stock + entrées − sorties par tranche '
    'possible'),
   ('T42-T49 · T49',
    'Tableau 49 (p. 39)',
    'Accidents mortels (« décès immédiats ») de 2020 à 2024',
    '—',
    '13, 11, 13, 11, 14',
    '—',
    'non comparable',
    'Un ordre de grandeur pour les nouvelles rentes de survivants'),
   ('T42-T49 N5',
    'Tableaux 22 et 34 (p. 18 et 28)',
    'Pensionnés et montants par DR, fin 2024',
    'A02 (DR du dossier, stock)',
    '62 231 ; montants par DR',
    'Vieillesse 0 à 4 % selon la DR ; ascendants exacts',
    'précise',
    'Plausible pour le stock ; à tester sur les nouveaux (lot 10)')]),
 ("3.5 T50 à T57 : l'évolution, les branches et les décès",
  [('T50-T57 D4',
    'Tableau 24 (p. 19)',
    'Allocations de vieillesse liquidées en 2024 (T50)',
    '831 personnes (832 allocations) ; 873 saisies ; 753 payées (P92)',
    '833',
    '−2 (−0,24 %) ; saisies +4,8 % ; payées −9,6 %',
    'confirme',
    'Le titre dit « liquidées » ; garder le mois et la DR (N8)'),
   ('T50-T57 R4',
    'Tableaux 34 et 28',
    'Montant des allocations de vieillesse 2024 (T50)',
    'Saisie 497 818 548 ; mois payé 517 334 647 (P92)',
    '497 726 931',
    'Saisie +0,02 % ; mois payé +3,9 %',
    'précise',
    "Appuie fortement date_montant = 'SAISIE' pour cette ligne (Q1)"),
   ('T50-T57 R4',
    'Tableaux 34 et 28',
    'Montant des allocations de survivants 2024 (T51)',
    'Saisie 190 149 232 ; mois payé 187 083 247',
    '197 906 991',
    'Saisie −3,9 % ; mois payé −5,5 %',
    'précise',
    'La saisie fait mieux ; 4 % restent à expliquer'),
   ('T50-T57 R4',
    'Tableaux 28, 41 et 90',
    'Le rythme mois par mois des montants de 2024',
    'Corrélation : vieillesse −0,12 (mois payé), 0,75 (saisie) ; PF 0,26 et 0,92 ; AT/MP −0,08 et '
    '0,84 (P105)',
    'Profils mensuels',
    "Sur l'année : pension de vieillesse +0,68 % (mois payé), +1,53 % (saisie) ; AVS +1,3 % et "
    '+1,8 %',
    'précise',
    "Penche pour la saisie sans trancher : le niveau de l'année va dans l'autre sens pour les "
    'grosses lignes (P115)'),
   ('T50-T57 D4',
    'Tableau 35 (p. 30)',
    'IJ de maternité, effectifs 2020 à 2024 (T54)',
    'Liquidées : 3 438, 3 810, 3 834, 4 069, 3 890 ; payées : 4 421 à 5 125',
    '3 428, 3 810, 3 973, 4 050, 3 893',
    '+0,3 %, 0, −3,5 %, +0,5 %, −0,1 % ; payées +24 à +29 %',
    'précise',
    'Les IJ liquidées retombent (sauf 2022), sans départager la date : comme T25-T27'),
   ('T50-T57 N1',
    'Tableau 35 (p. 30)',
    'IJ de maternité, montants 2020 à 2024 (T54)',
    'Saisie : 2 193 049 270 ; 2 357 360 490 ; 2 552 489 880 ; 2 757 274 340 ; 2 749 474 700',
    '1 530 554 843 ; 2 357 429 788 ; 2 560 989 546 ; 2 817 073 092 ; 2 865 630 056',
    'Saisie : +43,3 %, −0,003 %, −0,3 %, −2,1 %, −4,1 %',
    'écart à expliquer',
    '2021 et 2022 tombent juste ; 2023 et 2024 restent 2 à 4 % sous le publié ; mesurer le '
    'montant liquidé (P116)'),
   ('T50-T57 N8',
    'Tableau 39 (p. 32)',
    'IJ de maternité 2024 par mois et par DR (T54)',
    "3 890 sur l'année ; pas encore au mois",
    '3 893 (DRO 2 822, DRB 575, DRN 246, DRF 165, DRD 85)',
    '−0,1 %',
    'précise',
    'Une mère compte sans doute le mois de la liquidation, à vérifier mois par mois'),
   ('T50-T57 R4',
    'Tableau 40 (p. 32)',
    'Allocations prénatales 2024, montant (T55)',
    'Saisie 52 149 000 ; mois payé 50 677 500',
    '52 789 000',
    'Saisie −1,2 % ; mois payé −4,0 %',
    'précise',
    "La saisie fait mieux ; l'effectif de T55 n'est pas publié"),
   ('T50-T57 N6',
    'Tableau 40 (p. 32)',
    'Total de la branche PF 2024 (T56)',
    'Mois payé 10 750 873 342 ; saisie 10 802 063 700 ; aucun frais médical de maternité',
    '11 180 191 425 (dont frais médicaux 25 030 869)',
    '−3,8 % ; −3,4 %',
    'écart à expliquer',
    "Les allocations familiales font 62 % de l'écart ; les frais médicaux manquent (Q17)"),
   ('T50-T57 R13',
    'Tableau 26 (p. 21)',
    "Retraités décédés en 2024 : la date d'arrêt",
    "619 aujourd'hui ; environ 444 d'après la pension modifiée avant avril 2025 ; 404 d'après la "
    'fiche de la personne (P99)',
    '447',
    '+38,5 % ; −0,7 % ; −9,6 %',
    'précise',
    'Indice fort pour R13 (cliché de fin mars N+1) ; à confirmer par la DPC (Q2)'),
   ('T50-T57 N2',
    'Tableau 26 (p. 21)',
    'Qui est compté parmi les pensionnés décédés',
    "2024, base d'aujourd'hui : pension directe 622 ; tous 747 (P97)",
    '648 : droits directs 451 ; ayants droit 197 (dont 89 ascendants)',
    "+37,9 % ; +15,3 % (pas encore à la date d'arrêt)",
    'précise',
    'La CNSS détaille par type et compte les ayants droit, comme le veut le recueil ; 89 '
    'ascendants impossibles (Q15)'),
   ('T50-T57 · Fiche 7',
    'Tableau 81 (p. 62)',
    'Rentiers décédés en 2024 (T57, AT/MP)',
    '16 : 15 rentes viagères, 1 rente de survivant (P97)',
    '24 : viagères 21, veuves et veufs 2, ascendants 1',
    '−8 (−33 %)',
    'écart à expliquer',
    'La condition « rente en cours » écarte peut-être des rentiers ; mesurer sans elle (P117 ; '
    'Q14)'),
   ('T50-T57 N9',
    'Tableau 49 (p. 39)',
    "Décès immédiats d'accidents du travail, 2020 à 2024",
    "Victimes mortes dans l'année, par année de l'accident : 20, 17, 24, 16, 9 (P100)",
    '13, 11, 13, 11, 14',
    '+7, +6, +11, +5, −5',
    'précise',
    'La CNSS ne compte que la mort sur le coup (Q12, Q13)'),
   ('T50-T57 R4 (T52)',
    'Tableau 97 (p. 74)',
    'IJ AT/MP, montants 2020 à 2024',
    'Saisie : 84 625 089 ; 83 914 536 ; 62 724 081 ; 93 389 501 ; 51 859 178. 2024 au '
    'décaissement : 60 773 127 (P98)',
    '94 182 006 ; 94 190 676 ; 69 008 587 ; 81 867 247 ; 65 420 082',
    'Saisie −10,1 %, −10,9 %, −9,1 %, +14,1 %, −20,7 % ; décaissement −7,1 % en 2024',
    'écart à expliquer',
    "Une partie de l'écart peut venir de la date retenue (P115)"),
   ('T50-T57 · T53',
    'Tableau 97 (p. 74)',
    "Allocations d'incapacité, montants 2020 à 2024",
    'Saisie : 25 902 000 ; 33 278 400 ; 18 457 200 ; 29 631 600 ; 17 812 800',
    '25 558 532 ; 34 091 560 ; 19 188 366 ; 34 496 568 ; 20 034 111',
    '+1,3 %, −2,4 %, −3,8 %, −14,1 %, −11,1 %',
    'écart à expliquer',
    "L'écart grandit en 2023 et 2024 ; effectif non publié"),
   ('T50-T57 N4',
    'Tableaux 90, 96 et 97',
    'Rachats de rente, 2020 à 2024',
    '21 519 361 en 2024, versés par ajustement, hors de la requête 2 (P104)',
    '22 314 632 ; 24 935 514 ; 37 970 784 ; 38 896 780 ; 25 366 909',
    '2024 : −15,2 %',
    'tranche',
    "Le rachat est une prestation servie : l'ajouter à T56 ; points_a_valider n° 52 réglé pour "
    'les rachats'),
   ('T50-T57 N6',
    'Tableaux 82, 88, 89 (et 98, 99)',
    'Soins des AT/MP, 2020 à 2024 (T56 AT/MP)',
    '26 779 244 ; 35 445 200 ; 39 322 757 ; 32 481 449 ; 18 787 945',
    '141 230 596 ; 184 562 935 ; 171 679 077 ; 142 926 750 ; 139 668 695',
    'La base couvre 19 %, 19 %, 23 %, 23 %, 13 %',
    'écart à expliquer',
    "T56 AT/MP restera sous le publié tant que la source manque (Q16) ou qu'on ne lit pas la "
    'comptabilité'),
   ('T50-T57 · Fiche 6',
    'Tableau 106 (p. 80)',
    'Prestations servies par branche, 2020 à 2024 (T56)',
    'AVS 35,66 / 38,00 / 40,85 / 44,17 / 47,24 Md ; PF 8,17 / 8,56 / 9,01 / 10,36 / 10,75 Md ; '
    'AT/MP 0,80 / 0,81 / 0,80 / 0,81 / 0,76 Md',
    'Pensions 35,23 à 46,63 Md ; PF 7,41 à 11,18 Md ; RP 0,92 à 1,07 Md',
    'AVS +0,04 % à +1,3 % ; PF +10,2 % à −5,9 % ; AT/MP −13,2 % à −28,9 % ; total +2,5 % à −0,6 %',
    'écart à expliquer',
    'Le total retombe (−0,2 % en 2024) parce que des écarts de sens contraire se compensent'),
   ('T50-T57 N5',
    'Tableaux 106 et 114, fiche signalétique',
    "La 4e ligne de T56 : l'ASS",
    'Ligne vide',
    "ASS 2024 : 650 700 099 (tableau 106) ; dépenses de l'ASS : 1 571 655 315 (tableau 114)",
    'Ligne entière',
    'tranche',
    "La ligne « Autres » est l'ASS ; deux montants pour 2024 : le choix revient à la CNSS (Q18)"),
   ('T50-T57 N6',
    'Tableaux 103 à 105, démarche (p. xiv)',
    'Les charges techniques par branche',
    'Paiements de la base',
    'PF 11 180 191 425 ; RP 1 074 113 823 ; pensions 46 629 625 556',
    'Celui du tableau 106',
    'précise',
    "Les prestations servies de l'annuaire sont les charges techniques de la comptabilité (Q5)"),
   ('T50-T57 N11',
    'Tableaux 17 et 103 à 105',
    'Prestations ÷ cotisations, 2024',
    'Pas calculé',
    'Recalculé : pensions 45,96 % ; PF 25,91 % ; RP 9,82 % (avec le tableau 17 : 47,07 % ; 26,42 '
    '% ; 10,19 %)',
    '—',
    'précise',
    'Repère pour le ratio des ressources du recueil, qui compte les prestations notifiées'),
   ('T50-T57 N7',
    'Tableaux 114 et 35 (année 2020)',
    "Les années passées d'un tableau sur cinq ans",
    'Recalcul de 2020 : 2 193 049 270',
    '1 530 554 843',
    '+43 %',
    'précise',
    'La CNSS corrige parfois une année passée ; reprise telle quelle ou non (Q6)'),
   ('T50-T57 R5',
    'Tableau 20 (p. 17)',
    'Les orphelins pensionnés par âge et sexe',
    '—',
    'Moins de 25 ans : 9 478',
    '—',
    'confirme',
    "La CNSS compte l'orphelin, et non son tuteur"),
   ('T50-T57 N8',
    'Tableaux 29 à 33',
    'Assurance vieillesse servie chaque mois, une DR par tableau',
    'Pas calculé',
    'DRO 32 173 124 168 … DRD 723 818 987',
    '—',
    'précise',
    'Modèle du détail par DR et par mois ; test de R4 DR par DR'),
   ('T50-T57 N8',
    'Tableaux 91 à 95',
    'Prestations AT/MP en argent chaque mois, une DR par tableau',
    'Pas calculé',
    'DRO 621 802 712 … DRD 21 297 717',
    '—',
    'précise',
    'Pour repérer où manquent les paiements AT/MP'),
   ('T50-T57 Q16',
    'Tableaux 83 à 87',
    'Soins AT/MP chaque mois, une DR par tableau',
    'Pas calculé',
    'DRO 96 737 361 … DRD 6 087 089',
    '—',
    'précise',
    'Pour repérer par DR les soins absents de la base'),
   ('T50-T57 · Fiche 7',
    'Tableaux 78 et 80 (et 75)',
    'Les rentes en cours fin 2024, face aux rentiers décédés',
    'Pas calculé',
    "1 255 rentes viagères, 977 d'ayants droit ; 44 nouvelles ; décès 2024 : 21 et 3",
    '1 246 + 44 − 21 = 1 269 pour 1 255 (14 autres sorties)',
    'précise',
    'Le stock à mettre face aux décès de rentiers (Q14) ; les 14 autres sorties sont peut-être '
    'des rachats'),
   ('T50-T57 N9',
    'Tableaux 82, 88 et 89, ligne « Funéraires »',
    'Frais funéraires des AT/MP, 2020 à 2024',
    'Pas calculé',
    '0 ; 600 000 ; 0 ; 700 000 ; 900 000',
    '—',
    'non comparable',
    'Peut-être un indice des sinistrés décédés de T57 : à faire expliquer')])]

DIVERGENCES = [('Tableau 35, IJ de maternité de 2020 à 2024 (mêmes chiffres : 3 438… pour 3 428…).',
  'Harmonisé le 08/11 : « précise » dans T25-T27 et dans T50-T57 (T50-T57 disait « confirme »). '
  'Les IJ liquidées retombent, mais la date de comptage ne se départage pas (2022, 2023).'),
 ('Tableau 26, retraités décédés en 2024 (444 pour 447, R13).',
  'Harmonisé le 08/11 : « précise » dans T28-T31 et dans T50-T57 (T28-T31 disait « confirme »). '
  "Un indice fort, pas une preuve : la base n'a pas de date de déclaration du décès."),
 ('Tableau 22, pensionnés par DR (comparaison A02).',
  'Harmonisé le 08/11 : « précise » dans T25-T27 (N2), T32-T41 (N3) et T42-T49 (N5) ; T32-T41 '
  "disait « confirme ». Les veuves s'écartent de −8,1 % à la DRB et de −11,5 à −11,7 % à la DRD "
  "selon qu'on compte les veufs ou non (deux chiffres justes, deux périmètres)."),
 ('Les rachats de rente de 2024 (21 519 361 pour 25 366 909).',
  'T25-T27 dit « précise » (faut-il les compter dans T26 ? Q11), T28-T31 « écart à expliquer » '
  '(le montant), T50-T57 « tranche » (un rachat est une prestation servie, N4). Trois questions '
  'différentes sur le même chiffre ; elles ne se contredisent pas.'),
 ('Le contrôle « stock + entrées − décès » des pensions (tableaux 18, 23, 26).',
  'T32-T41 dit « précise » (N8 : la BI montre ce contrôle) ; T42-T49 dit « non comparable » (il '
  "n'utilise que des chiffres publiés). Même calcul, deux usages."),
 ('La retraite anticipée publiée à part.',
  "Pour T25 et T28, c'est le recueil qui demande la ligne (l. 248, 255) : l'annuaire ne peut que "
  "« préciser » (T25-T27, tableau 23 ; T28-T31, tableaux 18, 22, 23) ; l'« En bref » de T25-T27 "
  "le dit ainsi depuis le 08/11. Pour T32, le recueil se tait : T32-T41 dit que l'annuaire "
  'tranche (N4), par sa présentation (anticipée à part dans les tableaux 18, 19, 22), sans '
  'carte chiffrée.'),
 ('Tableau 25, nouveaux retraités par âge et sexe.',
  "T32-T41 dit « précise » (décalage d'âge, R8 pas confirmée en entier) ; T42-T49 dit « confirme "
  '» pour les tranches de cinq ans (R8, de 55 à 69 ans) et « écart à expliquer » pour le sexe. '
  "Pas de contradiction : âge exact d'un côté, tranches de l'autre."),
 ('La majoration pour enfant (tableaux 28 et 34).',
  "T25-T27 et T32-T41 disent « confirme » pour le même chiffre (A1 : pas un pensionné de plus). "
  "T42-T49 dit « précise » pour une autre comparaison (le montant d'un pensionné, tableaux 19 et "
  "28, comme la carte « montant de T32 » de T32-T41). Que le montant de T32 se fasse sans les "
  "accessoires (T32-T41 N13) est tranché par la présentation de l'annuaire, pas par ce chiffre.")]

TENSION = ("tension entre deux règles, sans conflit de verdict : pour la même branche, l'annuaire compte "
 'les allocations de vieillesse à la notification (tableau 24, D4) et les nouvelles pensions à la '
 'saisie (tableau 23, T42-T49 R7). T25-T27 garde la notification pour les pensions liquidées de '
 'T25 (2 397, −3,7 %) alors que la saisie tombe plus près (2 463, −1,0 %). T42-T49 le note déjà '
 '(Q2).')

NON_PUBLIE = [('T25 à T27',
  'Dossiers déposés, durées de liquidation (moins de 15 jours, 15 à 45, plus de 45), dossiers en '
  'instance, dossiers rejetés ; délai moyen et taux de dossiers restant à traiter',
  'Des « liquidés » partiels, en personnes : nouveaux pensionnés (23), allocations de vieillesse '
  '(24), IJ de maternité (35, 39), nouvelles rentes (75, 77). Repères de dépôts : déclarations '
  "d'AT/MP (47, 48) et « départs à la retraite » (27, sens à vérifier). Rien pour les allocations "
  "familiales, les prénatales, les IJ AT/MP, les allocations d'incapacité. Le seul « délai » "
  'publié (107 à 113) est celui du recouvrement',
  'T25-T27, ligne « pas publié » ; fiches T25 à T27 ; Q21'),
 ('T28 à T30',
  'Dossiers payés, dossiers contrôlés par trimestre, taux de contrôle (norme 5 %)',
  "Rien : les contrôles des tableaux 11 et 100 à 102 sont d'autres contrôles",
  'T28-T31 ; source : fichiers de la DAI (recueil v7) ; Q1 à Q3'),
 ('T31',
  "Trop-perçus, moins-perçus, taux d'indus (norme 2 %)",
  'Seule la colonne « payées » se reconstitue, par mois et par branche : tableaux 28 (PVID), 41 '
  '(PF), 82 et 90 (AT/MP)',
  'T28-T31 ; Q14 à Q16'),
 ('T32',
  "L'âge et le montant de la retraite anticipée à part ; la part CNSS des pensions de "
  'coordination',
  'Le reste de T32 (tableau 19) ; 78 anticipées au tableau 18',
  'T32-T41 N4 ; T42-T49 Q11'),
 ('T34, T36',
  "L'âge des veuves, veufs et ascendants séparés ; le montant par type",
  'Les totaux (tableau 18) ; veuves, veufs et ascendants mélangés par âge (tableau 20)',
  'T32-T41'),
 ('T37',
  'Les tranches au-delà de 90 ans ; les montants par tranche',
  'Effectifs par âge et sexe (tableau 76) ; montant total (tableau 96)',
  'T32-T41'),
 ('T38 à T40',
  'Âge, sexe, montant par type',
  'Totaux par DR (tableau 78) ; montant des rentes de survivants confondu',
  'T32-T41'),
 ('T41',
  "L'âge et le sexe des allocataires",
  "Nombre d'allocataires (37, 38) et montant ; l'âge et le sexe des enfants (36)",
  'T32-T41 ; Q8'),
 ('T42',
  'Montant servi aux nouveaux retraités ; total des services passés en mois ; âges au-delà de 70 '
  'ans',
  'Âge et sexe (tableau 25), nombres par DR (tableau 23)',
  'T42-T49'),
 ('T43, T44, T45 à T47, T49',
  'Âge, sexe, montant',
  'Nombres par DR (tableaux 23 et 77)',
  'T42-T49'),
 ('T48', "Montant ; degré moyen d'IPP", 'Effectifs par âge et sexe (tableau 75)', 'T42-T49 ; Q15'),
 ('T50',
  'Série de cinq ans, sexe, nombre moyen de services passés',
  '2024 seulement : 833 allocations (24), 497 726 931 FCFA (34)',
  'T50-T57 ; Q9'),
 ('T51',
  'Nombre de bénéficiaires ; détail veufs et veuves, orphelins, ascendants',
  'Montant 2024 (197 906 991, tableau 34)',
  'T50-T57'),
 ('T52, T53',
  "Nombre de bénéficiaires d'IJ AT/MP et d'allocations d'incapacité ; sexe",
  'Montants de 2020 à 2024 (tableau 97)',
  'T50-T57'),
 ('T55',
  "Nombre de bénéficiaires d'allocations prénatales ; série de cinq ans",
  'Montant 2024 (52 789 000, tableaux 40 et 41)',
  'T50-T57'),
 ('T57',
  'Série de cinq ans par branche ; les allocataires décédés (PF) ; les « assurés décédés » au '
  'sens du recueil',
  '2024 seulement : pensionnés décédés (26 : 648), rentiers décédés (81 : 24) ; décès immédiats '
  'de 2020 à 2024 (49)',
  'T50-T57 ; Q4, Q10'),
 ('Tous',
  "Les trimestres ; l'agence (sauf le « lieu CNSS » des accidents, 73 et 74) ; le type assuré ou "
  "ayant droit en dehors des décès ; les montants par tranche d'âge des rentes et des nouveaux",
  'Mois et DR pour beaucoup de tableaux',
  'N1, N2 des familles')]

NON_PUBLIE_EN_CLAIR = ("l'annuaire ne dit rien des dossiers (T25 à T27), ni des contrôles et des indus (T28 à T31), ni "
 'des montants des nouveaux. Ces tableaux se valident avec la CNSS ; les chiffres de nos requêtes '
 'seront présentés comme « proposés, à valider ».')

MODELES = [('03 · délai de liquidation',
  'Pour chaque dossier : date de notification des droits − date de réception du dossier, en '
  'jours. Sur un échantillon de 100 dossiers par branche, première demande seulement (PF, '
  'première rente AT/MP, première pension). Synthèse : nombre et part des dossiers liquidés en 45 '
  'jours au plus et au-delà, délai moyen, écart type',
  '45 jours (norme CIPRES « moins de 45 jours »)',
  'La durée va de la réception à la notification (T25-T27 D5) ; la rente AT/MP part de la '
  'demande, pas du certificat de consolidation',
  'Un échantillon (la BI peut tout compter) ; deux tranches au lieu de trois ; seulement les '
  'premières demandes. Pour les PF, la notification, alors que le recueil dit « date de '
  'validation ». Défaut : une ligne vide compte comme un délai de 0 jour, dans les 45 jours et '
  'dans la moyenne',
  "T25-T27 Q16, Q18 ; T42-T49 Q2 (contre-indice sur la date d'entrée)"),
 ('04 · reste à traiter',
  "Dossiers à traiter = en instance au 31/12 de l'année d'avant + reçus − rejetés ; en instance à "
  'la fin = à traiter − liquidés ; taux = en instance à la fin ÷ à traiter. Trois lignes : PF, '
  'risques professionnels, pensions',
  '15 % au plus (norme qui ne vient que de ce modèle)',
  'Appuie la lecture « stock » des dossiers en instance (T25-T27 H2, Q1)',
  "Le recueil et Kaya 2017 divisent par les reçus de l'année : la BI suit le recueil par défaut "
  '(formule_taux = RECUEIL). Défaut : pour les risques professionnels, la formule oublie le stock '
  "du début d'année",
  'T25-T27 N4, Q19'),
 ('05 · contrôle a posteriori',
  "Taux = dossiers contrôlés ÷ dossiers « liquidés ou en portefeuille », par branche, sur l'année",
  '5 % au moins (norme CIPRES)',
  'Même norme que C1',
  "Le dénominateur : l'Annuaire CIPRES et le recueil disent « dossiers payés ». Pas de trimestre, "
  'pas de type, pas de DR',
  'T28-T31 C1, Q6'),
 ('06 · prestations indues',
  'Taux de trop-perçu et taux de moins-perçu, chacun ÷ le montant des dossiers contrôlés ; taux '
  'global = les deux additionnés',
  '2 % au plus (norme CIPRES), appliquée au taux global',
  'La source des indus est celle de la DAI, comme le dit le recueil',
  "Le recueil et l'Annuaire CIPRES divisent par toutes les prestations payées ; Kaya 2017 exclut "
  "les moins-perçus. Le montant des dossiers contrôlés n'est dans aucune source",
  'T28-T31 N8, Q14, Q15, Q16')]

MODELES_EN_CLAIR = ('les modèles confirment les normes (45 jours, 5 %, 2 %) et la durée « de la réception à la '
 'notification ». Ils diffèrent du recueil sur trois dénominateurs (restes à traiter, contrôle, '
 'indus) et sur la fin du délai des PF. Le recueil passe avant : la BI le suit par défaut, et '
 "propose la formule des modèles comme autre valeur d'un réglage. Deux défauts des modèles sont à "
 'signaler à leur auteur : la ligne vide de la feuille 03, le stock oublié de la feuille 04.')

PROPOSITIONS = ["Garder les règles que l'annuaire confirme, sans rien y changer : D4 (liquidé = notifié, pour les "
 'allocations de vieillesse et les IJ de maternité), T32-T41 R6 (le stock au 31/12, payé ou non), '
 "R5 (l'ayant droit, l'orphelin à son âge), T42-T49 R7 (la première fois : une rente recalculée "
 "n'est pas une entrée), R1 (maternité dans les PF), A1 (l'accessoire suit la pension), R8 pour "
 'les grilles de T32, T33, T37 et les tranches de T42. La DR du dossier (T32-T41 N3, T25-T27 N2, '
 'T42-T49 N5) est seulement précisée (tableau 22).',
 "Faire de ce que l'annuaire tranche la valeur par défaut des réglages : date_entree = SAISIE "
 '(T42-T49) ; coordination gardée dans T42 (T42-T49 N6) ; retraite_anticipee = LIGNE_A_PART dans '
 'T32 (T32-T41 N4) ; ages_impossibles = DERNIERE_TRANCHE (T32-T41 R8) ; rachats de rente dans T56 '
 '(T50-T57 N4) ; ligne « Autres » de T56 = ASS (T50-T57 N5), le montant restant à la CNSS (Q18). '
 'Source : Annuaire CNSS 2024 ; état : proposé, à valider par la CNSS.',
 'Tester R4 sans attendre la CNSS : comparer mois par mois nos paiements, au mois payé et à la '
 'saisie, aux tableaux 28 à 33 (vieillesse), 41 à 46 (PF) et 90 à 95 (AT/MP), DR par DR. Le profil '
 'publié suit déjà la saisie (corrélations 0,75, 0,92, 0,84) ; le niveau annuel reste partagé. '
 'Proposition BI.',
 'Expliquer les écarts par les contrôles déjà nommés : A06 et A07 (veuves et orphelins : décès et '
 "radiations saisis après l'arrêt de l'annuaire), A08 (orphelins par âge), lot 10 (rentes "
 'viagères, ascendants de rente, âge des nouveaux), P115 (date des paiements, IJ AT/MP), P116 '
 '(montant liquidé des IJ de maternité), P117 (rentiers décédés sans la condition « rente en cours '
 '»).',
 'Montrer à côté de chaque tableau le contrôle « stock + entrées − sorties » (T32-T41 N8), puisque '
 "l'annuaire lui-même ne le tient pas toujours.",
 "Présenter avec une réserve les montants AT/MP (rentes, soins, IJ) tant qu'on ne sait pas où ils "
 'sont payés ; à terme, lire les charges techniques dans la comptabilité (V_BI_BALANCE_COMPTABLE, '
 'T50-T57 N6).',
 "Signaler à la DPC, qui fait l'annuaire, ses chiffres qui ne tiennent pas entre eux (partie 2.2) "
 ": 89 décès d'ascendants, orphelins de 2024, rentes d'orphelins, ligne ASS du tableau 106, "
 'étiquettes et pourcentages des tableaux 49, 51, 57 et 60. Proposition BI.',
 "Les trois verdicts qui différaient pour la même comparaison (tableaux 22, 26, 35) ont été "
 "harmonisés dans les documents de famille le 08/11 (« précise ») ; les autres écarts de la "
 'partie 3.6 sont justifiés.']

QUESTIONS = [("L'année ou le mois d'un paiement : mois payé, saisie ou versement",
  'T32-T41 Q2 ; T50-T57 Q1 ; T28-T31 Q24',
  'DPC et DCP',
  "Même question dans trois familles, même formulation, posée une fois (DPC 1 de la liste du "
  "recueil). Le test de la proposition 3 peut y répondre d'abord"),
 ("La base de l'annuaire arrêtée vers fin mars N+1",
  'T32-T41 Q3 ; T50-T57 Q2',
  'DPC',
  'Posée une fois (DPC 2 de la liste du recueil). Indice fort : 444 décès de retraités pour 447'),
 ('Une pension versée après un décès déclaré en retard',
  'T28-T31 Q18',
  'DCP',
  'Repère donné par le tableau 26 (T28-T31 R13) : des décès de 2024 déclarés après mars 2025'),
 ('Un nouveau compte-t-il à la saisie ou à la notification ?',
  'T42-T49 Q2',
  'DCP',
  "L'annuaire tranche pour la saisie ; la question demande une confirmation (feuille 03)"),
 ("Le montant d'un nouveau : rappel des mois passés compris ou non",
  'T42-T49 Q3',
  'DCP',
  "L'annuaire ne publie aucun montant de nouveaux : il ne tranche pas"),
 ("Pour un orphelin, une veuve ou un parent de rentier, qui compter : l'ayant droit ou l'assuré "
  'décédé',
  'T42-T49 Q1',
  'DCP, avec les auteurs du recueil',
  "L'annuaire compte l'ayant droit (tableau 77) ; il ne peut pas contredire le recueil"),
 ("La retraite anticipée : dans T42, et dans les maquettes d'origine",
  'T42-T49 Q13 ; T25-T27 Q22 ; T28-T31 Q13',
  'DCP ; avec la DPC pour les maquettes',
  "L'annuaire la publie à part (tableaux 18 et 23) ; T32-T41 N4 la met à part dans T32"),
 ("Le montant d'un tableau : payé aux comptés, ou tout le payé de l'année",
  'T32-T41 Q1 ; T32-T41 Q9 (allocations validées non versées)',
  'Q1 : DCP (pensions), DES et DR (T41), DP et DR (rentes). Q9 : DPC, avec la DES',
  'Le publié tombe entre nos deux montants'),
 ('Où sont payés les rentes et les soins AT/MP absents de la base',
  'T32-T41 Q21 ; T42-T49 Q5 ; T50-T57 Q16 ; T25-T27 Q7 (les soins dans T26)',
  'DSI, avec la DFC et la DP ; DP et DR (T25-T27 Q7)',
  'Posée une fois (DSI 9 de la liste du recueil). Question déjà posée à la DSI (README)'),
 ('Où sont enregistrés les frais médicaux de maternité (25 030 869 FCFA en 2024)',
  'T25-T27 Q15 ; T50-T57 Q17',
  'DSI, avec la DFC et la DPC',
  'Doublon : les deux familles le disent déjà'),
 ('Le rachat de rente',
  "T25-T27 Q11 (dans T26 ?) ; T28-T31 Q11 (l'« achat de rente » de Kaya) ; T28-T31 Q22 (dans les "
  'payées de T31 ?)',
  'DP et DR ; DCP',
  "T50-T57 N4 tranche qu'il est une prestation servie de T56"),
 ('Les rappels de moins-perçus versés par ajustement : des prestations payées ou servies ?',
  'T28-T31 Q23 (T31) ; T50-T57 Q19 (T56)',
  'DCP, avec la DAI et la DPC',
  'Même question pour deux tableaux, posée une fois (DCP 15 de la liste du recueil)'),
 ('Le dossier payé de T28 à T30 : payé ou liquidé dans la période ; dossier ou prestation '
  'contrôlée',
  'T28-T31 Q7, Q4',
  'DAI, avec la DCP',
  "L'annuaire compte les IJ de maternité et les allocations de vieillesse à la liquidation, les "
  "allocataires en stock ; une pension n'est pas un dossier"),
 ('La source et la ligne « Autres » de T56 ; les années passées',
  'T50-T57 Q5, Q18, Q6',
  'DPC, avec la DFC, la DPASS et la DCG',
  "Deux montants d'ASS pour 2024 dans l'annuaire"),
 ('Les « départs à la retraite » du tableau 27',
  'T25-T27 Q21',
  'DPC',
  "Repris par T42-T49 (ne pas s'en servir pour T42)"),
 ('Les veuves de 2024', 'T32-T41 Q4', 'DCP', "A06 et A07 d'abord"),
 ('Les orphelins de pension et de rente',
  "T42-T49 Q20 ; T32-T41 Q12 (limite d'âge des rentes d'orphelins)",
  'DPC (Q20) ; DP et DR (Q12)',
  'Le stock publié de 2024 ne tient pas avec les entrées'),
 ("L'âge et le sexe : date de calcul de l'âge, enfants de 21 ans, sexe des nouveaux retraités",
  'T32-T41 Q10, Q11 ; T42-T49 Q8, Q9',
  'DPC',
  "T32-T41 Q10 et T42-T49 Q9 : la même date de calcul de l'âge, posée une fois (DPC 5)"),
 ('Les nouvelles rentes viagères (44) et les nouveaux ascendants de rente (13)',
  'T42-T49 Q6, Q7',
  'DP et DR',
  'Lot 10 avant de poser Q6'),
 ('Les décès : rentiers, ascendants, décès immédiats',
  'T50-T57 Q14, Q15, Q12, Q13',
  'DPC, avec la DCP ; DSI (Q13)',
  'Q15 rejoint points_a_valider n° 57'),
 ('Le « cumul » de T38',
  'T32-T41 Q14',
  'DP et DR',
  "L'« ensemble au 31-12 » du tableau 78 appuie la lecture « stock »"),
 ('Les modèles de ratios : fin du délai des PF, première demande, formule des restes à traiter',
  'T25-T27 Q16, Q18, Q19',
  "DSI, avec la DES (Q16) ; DCP, DES, DP (Q18) ; l'auteur de la grille, à défaut la DCP (Q19)",
  'Le recueil passe avant les modèles'),
 ("Les modèles de ratios : dénominateur du contrôle, taux d'indus, moins-perçus",
  'T28-T31 Q6, Q15, Q16',
  'DAI, avec la DCP',
  'Idem'),
 ('Le ratio des ressources : soins compris ou non',
  'T50-T57 Q20',
  'DAI et DPC',
  'Repère recalculé : 45,96 %, 25,91 %, 9,82 %'),
 ("L'agence d'un dossier",
  'T25-T27 Q20 ; T28-T31 Q28 ; T32-T41 Q20',
  'DSI',
  'Posée une fois (DSI 5) ; les tableaux 73 et 74 montrent que la CNSS sait ventiler par lieu')]

RESTE = [('Comparer mois par mois et par DR nos paiements aux tableaux 28 à 33, 41 à 46 et 90 à 95 (R4)',
  'Claude, puis toi',
  'à faire'),
 ('Exécuter A06, A07 et A08 (veuves, orphelins)', 'toi (exécution), puis Claude', 'à faire'),
 ('Lancer le lot 10 et P115 à P117 (rentes, ascendants, âges, date des paiements, rentiers '
  'décédés)',
  'toi (exécution), puis Claude',
  'à faire'),
 ('Poser les questions une seule fois, dans la liste du recueil (besoins_v7_prestations, partie 8)',
  'CNSS, via toi',
  'à poser'),
 ("Signaler à la DPC les incohérences de l'annuaire (partie 2.2)", 'toi', 'à faire'),
 ("Signaler à l'auteur des modèles les deux défauts (feuilles 03 et 04)", 'toi', 'à faire'),
 ('Harmoniser les verdicts de la partie 3.6 dans les documents de famille',
  'Claude',
  'fait le 08/11 (tableaux 22, 26, 35)')]


def decompte():
    """Verdicts par famille, calculés depuis les cartes ; contrôlés contre le md."""
    lignes = []
    total = Counter()
    for titre, cartes in FAMILLES:
        fam = cartes[0][0].split(" ")[0]
        c = Counter(CLE[carte[6]] for carte in cartes)
        assert all(carte[0].startswith(fam) for carte in cartes), fam
        vals = (len(cartes),) + tuple(c[k] for k in ORDRE)
        assert vals == COMPTES_MD[fam], (fam, vals)
        total += c
        lignes.append([fam] + [str(v) for v in vals])
    lignes.append(["Ensemble", str(sum(total.values()))] + [str(total[k]) for k in ORDRE])
    return lignes


def build():
    p = Page(2024)
    d = p.d

    def sous_titre(x, y, txt):
        d.text(x, y, txt, 24)
        return y + 44

    def grille(y, items, par_ligne, pose, dessine):
        """Cartes côte à côte, rangée par rangée ; renvoie le bas."""
        gap = 30
        w = (INNER - (par_ligne - 1) * gap) / par_ligne
        for debut in range(0, len(items), par_ligne):
            rang = items[debut:debut + par_ligne]
            poses = [pose(it, w - 44) for it in rang]
            h = max(ps[-1] for ps in poses)
            for i, (it, ps) in enumerate(zip(rang, poses)):
                dessine(X0 + i * (w + gap), y, w, h, it, ps)
            y += h + gap
        return y - gap

    # ------------------------------------------------------------------ titre et légende
    d.text(0, 0, TITRE, 48)
    st = wrap(SOUS_TITRE, 22, WIDTH - 40)
    d.text(0, 66, st, 22, C["doux"])
    y = 66 + height_of(st, 22) + 24
    y = p.en_bref(y, EN_BREF) + 30
    y = p.bloc(0, y, WIDTH, MOTS, 19, (C["gris"], C["gris_bord"])) + 30
    d.text(0, y, "Verdicts :", 17, C["doux"])
    p.legende(110, y, [(bg, bord, lab, "dashed" if k == "non publie" else "solid")
                       for k, (bg, bord, lab) in VERDICTS.items()])
    p.y = y + 64

    # ------------------------------------------------------------------ 1. inventaire
    cadre, top = p.section("1. L'inventaire : les tableaux de l'annuaire qui touchent les "
                           "prestations, par thème")
    top = p.bloc(X0, top, INNER, NOTE_INVENTAIRE, 19, (C["bleu_clair"], C["bleu"])) + 30
    colonnes = [("N°", 230), ("Ce qu'il compte", 900), ("Page", 140), ("Tableau CIPRES", 520),
                ("Ce qu'on en tire", INNER - 1790)]
    for titre, lignes in INVENTAIRE:
        top = sous_titre(X0, top, titre)
        top = p.tableau(X0, top, colonnes, [list(r) for r in lignes]) + 34
    p.fermer(cadre, top - 34)

    # ------------------------------------------------------------------ 2. contrôles internes
    cadre, top = p.section("2. Les contrôles internes : l'annuaire se recoupe-t-il ?")
    top = p.bloc(X0, top, INNER, NOTE_CONTROLES, 19, (C["bleu_clair"], C["bleu"])) + 30
    top = sous_titre(X0, top, "2.1 D'un tableau à l'autre : ce qui se recoupe")
    top = p.tableau(X0, top, [("Entre", 600), ("Ce qu'on trouve", 1650), ("Écart", INNER - 2250)],
                    [list(r) for r in RECOUPE]) + 34
    top = sous_titre(X0, top, "2.2 D'un tableau à l'autre : ce qui ne colle pas")
    top = p.tableau(X0, top, [("Entre", 520), ("Ce qui ne colle pas", 1250),
                              ("Ce qu'on en fait", INNER - 1770)],
                    [list(r) for r in NE_COLLE_PAS]) + 34
    top = sous_titre(X0, top, "2.3 D'une année à l'autre")
    top = p.bloc(X0, top, INNER, "\n".join("• " + a for a in ANNEES), 19,
                 (C["jaune_clair"], C["jaune_bord"])) + 24
    bas = p.bloc(X0, top, INNER, "En clair : " + CONTROLES_EN_CLAIR, 20,
                 (C["vert_clair"], C["vert"]), sw=3)
    p.fermer(cadre, bas)

    # ------------------------------------------------------------------ 3. verdicts
    cadre, top = p.section("3. Les verdicts : ce que l'annuaire confirme, tranche ou contredit "
                           "(les verdicts des familles)")
    top = p.bloc(X0, top, INNER, NOTE_VERDICTS, 19, (C["vert_clair"], C["vert"])) + 30
    courts = ["confirme", "tranche", "précise", "écart à expliquer", "contredit",
              "non comparable"]
    entete = [("Famille", 400), ("Cartes", 250)] + [(c, (INNER - 650) / len(ORDRE))
                                                     for c in courts]
    top = p.tableau(X0, top, entete, decompte()) + 40

    def pose_carte(c, inner):
        carte, tab, comp, nous, pub, ecart, _, effet = c
        t = wrap(f"{carte} · {tab}", 21, inner)
        tc = wrap(comp, 18, inner)
        tn = wrap("Nous : " + nous, 17, inner)
        tp = wrap("Publié : " + pub, 17, inner)
        te = wrap("Écart : " + ecart, 17, inner)
        tf = wrap("Ce que ça change : " + effet, 17, inner)
        h = (16 + height_of(t, 21) + 10 + height_of(tc, 18) + 12 + height_of(tn, 17) + 6
             + height_of(tp, 17) + 6 + height_of(te, 17) + 12 + height_of(tf, 17) + 16 + 30 + 16)
        return t, tc, tn, tp, te, tf, h

    def dessine_carte(x, y, w, h, c, ps):
        t, tc, tn, tp, te, tf, _ = ps
        cle = CLE[c[6]]
        bord = VERDICTS[cle][1]
        g = d.group()
        b = d.box(x, y, w, h, bg=C["blanc"], stroke=bord, stroke_width=2,
                  style="dashed" if cle == "non comparable" else "solid", groups=g)
        cy = y + 16
        d.text(x + 22, cy, t, 21, groups=g, inside=b)
        cy += height_of(t, 21) + 10
        d.text(x + 22, cy, tc, 18, groups=g, inside=b)
        cy += height_of(tc, 18) + 12
        for txt in (tn, tp, te):
            d.text(x + 22, cy, txt, 17, groups=g, inside=b)
            cy += height_of(txt, 17) + 6
        cy += 6
        d.text(x + 22, cy, tf, 17, C["doux"], groups=g, inside=b)
        chip_verdict(d, x + 22, y + h - 16 - 30, cle, groups=g)

    for titre, cartes in FAMILLES:
        top = sous_titre(X0, top, f"{titre} ({len(cartes)} cartes)")
        top = grille(top, cartes, 4, pose_carte, dessine_carte) + 40

    top = sous_titre(X0, top, "3.6 Les verdicts qui différaient d'une famille à l'autre")
    top = p.bloc(X0, top, INNER, NOTE_DIVERGENCES, 19, (C["jaune_clair"], C["jaune_bord"])) + 24

    def pose_div(it, inner):
        t = wrap(it[0], 21, inner)
        tx = wrap(it[1], 18, inner)
        return t, tx, 16 + height_of(t, 21) + 10 + height_of(tx, 18) + 18

    def dessine_div(x, y, w, h, it, ps):
        t, tx, _ = ps
        g = d.group()
        b = d.box(x, y, w, h, bg=C["blanc"], stroke=C["jaune_bord"], stroke_width=3, groups=g)
        d.text(x + 22, y + 16, t, 21, groups=g, inside=b)
        d.text(x + 22, y + 16 + height_of(t, 21) + 10, tx, 18, C["doux"], groups=g, inside=b)

    items = [(f"{i}. {titre}", texte) for i, (titre, texte) in enumerate(DIVERGENCES, 1)]
    top = grille(top, items, 2, pose_div, dessine_div) + 24
    top = p.bloc(X0, top, INNER, MOTS_DIFFERENTS, 19, (C["jaune_clair"], C["jaune_bord"])) + 24
    bas = p.bloc(X0, top, INNER, "S'y ajoute une " + TENSION, 19, (C["gris"], C["gris_bord"]))
    p.fermer(cadre, bas)

    # ------------------------------------------------------------------ 4. non publié
    cadre, top = p.section("4. Ce que l'annuaire ne publie pas : il ne peut ni le confirmer ni "
                           "le contredire")
    top = p.bloc(X0, top, INNER, NOTE_NON_PUBLIE, 19, (C["gris"], C["gris_bord"])) + 24
    top = p.tableau(X0, top, [("Tableau CIPRES", 300), ("Ce qui n'est pas publié", 850),
                              ("Ce que l'annuaire donne quand même", 900),
                              ("Où c'est suivi", INNER - 2050)],
                    [list(r) for r in NON_PUBLIE]) + 24
    bas = p.bloc(X0, top, INNER, "En clair : " + NON_PUBLIE_EN_CLAIR, 20,
                 (C["vert_clair"], C["vert"]), sw=3)
    p.fermer(cadre, bas)

    # ------------------------------------------------------------------ 5. modèles de ratios
    cadre, top = p.section("5. Les modèles de ratios de la CNSS (classeur 2025, feuilles 03 à "
                           "06)")
    top = p.bloc(X0, top, INNER, NOTE_MODELES, 19, (C["orange_clair"], C["orange"])) + 24
    top = p.tableau(X0, top, [("Feuille", 280), ("Ce qu'elle calcule", 650), ("Norme", 270),
                              ("Ce qui rejoint nos règles", 420),
                              ("Ce qui diffère, ou ne marche pas", 610),
                              ("Famille et question", INNER - 2230)],
                    [list(r) for r in MODELES]) + 24
    bas = p.bloc(X0, top, INNER, "En clair : " + MODELES_EN_CLAIR, 20,
                 (C["vert_clair"], C["vert"]), sw=3)
    p.fermer(cadre, bas)

    # ------------------------------------------------------------------ 6. propositions
    cadre, top = p.section("6. Ce qu'on propose (rien n'est appliqué), et les questions "
                           "ouvertes des familles")
    top = sous_titre(X0, top, "6.1 Ce qu'on propose")
    top = p.bloc(X0, top, INNER, "\n".join(f"{i}. {t}" for i, t in enumerate(PROPOSITIONS, 1)),
                 19, (C["vert_clair"], C["vert"])) + 34
    top = sous_titre(X0, top, "6.2 Les questions ouvertes, déjà posées dans les familles")
    top = p.bloc(X0, top, INNER, NOTE_QUESTIONS, 19, (C["bleu_clair"], C["bleu"])) + 24
    top = p.tableau(X0, top, [("Point", 620), ("Questions des familles", 640), ("À qui", 400),
                              ("Remarque", INNER - 1660)],
                    [list(r) for r in QUESTIONS]) + 34
    top = sous_titre(X0, top, "6.3 Ce qui reste à faire")
    top = p.tableau(X0, top, [("Tâche", 1500), ("Qui", 400), ("État", INNER - 1900)],
                    [list(r) for r in RESTE]) + 24
    bas = p.bloc(X0, top, INNER, SOURCES, 19)
    p.fermer(cadre, bas)

    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
