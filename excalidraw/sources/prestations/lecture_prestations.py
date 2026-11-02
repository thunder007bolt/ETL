"""
Contenu de la fiche de lecture des prestations sociales (T25 à T57), générée par
sources/fiche_de_lecture.py dans schemas/prestations/fiche_de_lecture.excalidraw.

Chaque tableau a ses termes, dans l'ordre où on les lit dans l'Annuaire ; chaque terme : ce que
dit l'Annuaire (avec la page), ce que j'en ai compris, un exemple, la question à valider, où je le
prends dans la base, et la requête pour vérifier. Les requêtes viennent des fiches de validation
(sources/prestations/t25_…), renumérotées d'après les termes.

Les tableaux suivent l'ordre de l'Annuaire ; une famille de tableaux qui se calcule de la même
façon (T32 à T40, T42 à T49, T50 à T57) partage une section.
"""

import t25_dossiers_de_pension as t25
import t26_dossiers_at_mp as t26
import t27_dossiers_pf as t27
import t31_indus as t31
import t32_pensionnes_vieillesse as t32
import t41_allocataires as t41
import t42_nouveaux_retraites as t42
import t50_t57_evolution as t50
from fiche_de_lecture import renvoi

TITRE = "Fiche de lecture : les prestations sociales"
SOUS_TITRE = ("Les tableaux CIPRES T25 à T57, terme par terme. À lire à côté de l'Annuaire "
              "statistique CIPRES (version du 11/09/2020).")
INTRO = [
    "Pour chaque tableau de l'Annuaire, je reprends chaque terme (déposé, liquidé, en instance, "
    "rejeté…) et je dis comment je l'ai compris, d'où je le prends dans la base et comment on "
    "refait le chiffre. Le but : que tu me dises si ma lecture est la bonne, terme par terme.",
]
TITRE_TABLES = "Les tables de la base des prestations (schéma USER_DWH)"
TABLES = [
    "DOSSIER : un dossier par assuré et par branche. TDOS_CODE donne la branche : V pensions "
    "(PVID), A accidents du travail (AT/MP), F prestations familiales, M maternité. DOS_STATUT : "
    "A actif, F fermé. DOS_CATEGORIE : vide pour un dossier ordinaire, COOR_AC coordination payée "
    "par une autre caisse, CNSS_AC transféré à une autre caisse.",
    "RECEPTION_DOSSIER : chaque passage au guichet. RD_DATE_RECEPTION (date du dépôt), "
    "RD_TYPE_RECEPTION (NOUVEAU : ouvre un dossier ; ANCIEN : arrive sur un dossier qui existe "
    "déjà). Le dossier garde le numéro de la réception qui l'a ouvert (DOSSIER.RD_ID).",
    "PRESTATION_ESP : chaque prestation en espèces d'un dossier. TPE_CODE (le type : RPV pension "
    "de vieillesse…), PE_DATE_DEMANDE, PE_DATE_NOTIFICATION (droits notifiés), PE_DATE_EFFET, "
    "PE_DATE_SUSPENSION, PE_DATE_RADIATION ; IND_ID_PREST, la personne qui la reçoit.",
    "DOSSIER_REJETE : les refus enregistrés, avec DOSRE_DATE_REJET et le motif.",
    "DEBOURS : les paiements (DEB_TYPE PM paiement, DEB_STATUT D ou P payé, DEB_MONTANT, "
    "DEB_DATE_EFFET le mois payé). AJUSTEMENT : trop-perçus et moins-perçus. INDIVIDU : la "
    "personne (date de naissance, sexe 1 homme ou 2 femme, date de décès).",
]
SUITE = [
    "Dans l'ordre de l'Annuaire : T25, T26 et T27 (les dossiers selon la durée de liquidation), "
    "T28 à T30 (les contrôles a posteriori), T31 (les prestations indues), T32 à T40 (pensionnés "
    "et crédirentiers au 31 décembre), T41 (allocataires), T42 à T49 (nouveaux bénéficiaires), "
    "T50 à T57 (évolution sur 5 ans, prestations servies et décédés par branche).",
    "Les chiffres sont ceux de la base au 02/02/2026 : 2025 pour T25 à T30, 2024 pour les "
    "autres, comme l'annuaire de la CNSS. Les tableaux d'une même famille se calculent de la "
    "même façon : la requête est donnée pour le premier, et on change les codes pour les autres.",
]


def sql(fiche, regle, decalage=None):
    """La requête de la règle d'une fiche de validation, avec les renvois aux termes."""
    return renvoi(fiche.SQL[regle], decalage)


# T25 à T27 : numéro de la règle de la fiche, numéro du terme ici (le terme 2, les lignes du
# tableau, n'a pas de règle)
DECALE = {n: n + 1 for n in range(2, 11)}


def t25_sql(regle):
    return sql(t25, regle, DECALE)


T25 = dict(
    code="T25",
    titre="Dossiers de prestations PVID de l'exercice selon la durée de liquidation",
    pages="Annuaire CIPRES : description p. 11, maquette p. 34, glossaire p. 62 et p. 64",
    annee="2025",
    resume=("Pour moi, ce tableau mesure la vitesse à laquelle la CNSS traite les demandes de "
            "pension : combien arrivent dans l'année, combien sont réglées et en combien de "
            "jours, combien restent à traiter au 31 décembre. Je le calcule sur 2025."),
    termes=[
        dict(terme="Dossier de prestations PVID",
             annuaire="PVID : pensions de vieillesse, d'invalidité et de décès (les survivants). "
                      "Le tableau porte sur les dossiers « de l'exercice » (p. 11).",
             compris="Un dossier de la branche des pensions, ouvert au nom de l'assuré. J'y "
                     "garde les dossiers de coordination : l'assuré a travaillé dans deux pays, "
                     "l'autre caisse paie, mais la CNSS reçoit et instruit la demande. Je retire "
                     "seulement les dossiers transférés à une autre caisse, que la CNSS ne traite "
                     "plus.",
             question="Tu valides : les dossiers de coordination comptent, les dossiers "
                      "transférés non ?",
             base=["Table DOSSIER, TDOS_CODE = 'V' (branche des pensions).",
                   "DOS_CATEGORIE vide : dossier ordinaire, gardé.",
                   "DOS_CATEGORIE = 'COOR_AC' : coordination payée par une autre caisse, "
                   "gardée (3 326 dossiers ; en 2025, 238 dépôts et 292 liquidations).",
                   "DOS_CATEGORIE = 'CNSS_AC' : transféré à une autre caisse, retiré (153 "
                   "dossiers)."],
             sql=t25_sql("1")),
        dict(terme="Type de prestation (les lignes du tableau)",
             annuaire="Six lignes : pension de vieillesse, pension d'invalidité, pension de "
                      "survivant, allocations de vieillesse, allocation de survivant, autres "
                      "prestations de la branche, puis le TOTAL (p. 34). L'allocation de "
                      "vieillesse est versée en une seule fois (glossaire, p. 62).",
             compris="Chaque prestation a un code, et le code dit la ligne. J'ajoute une ligne "
                     "« pension de retraite anticipée », demandée à l'atelier de Kaya en 2017 "
                     "mais absente de l'Annuaire. La veuve et les orphelins d'un même décès ne "
                     "font qu'une demande de pension de survivant. Un complément (bonification, "
                     "majoration pour enfant) suit la pension : il ne crée pas de ligne.",
             exemple="un dossier avec une pension de vieillesse et une majoration pour enfant "
                     "compte une fois, sur la ligne pension de vieillesse.",
             question="Tu valides ce classement des codes, avec une ligne à part pour la "
                      "retraite anticipée ?",
             base=["Table PRESTATION_ESP, champ TPE_CODE (libellés dans TYPE_PRESTATION_ESP).",
                   "Pension de vieillesse : RPV. Retraite anticipée : RPRA. Invalidité : RPI.",
                   "Pension de survivant : RPSV veuve, RPSF veuf, RPSE orphelin, RPA ascendant.",
                   "Allocation de vieillesse : RAV. Allocation de survivant : RASC conjoint, "
                   "RASO orphelin, RASA ascendant (RAS, l'ancien code, ne sert plus).",
                   "Tous les autres codes : autres prestations de la branche (remboursement de "
                   "cotisations, complément seul)."],
             sql="""-- les prestations de pension par code
SELECT pe.TPE_CODE, COUNT(*)
FROM   USER_DWH.PRESTATION_ESP pe
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = pe.DOS_CODE
WHERE  d.TDOS_CODE = 'V'
GROUP  BY pe.TPE_CODE
ORDER  BY 2 DESC
-- la ligne de chaque code : le CASE du
-- terme 4"""),
        dict(terme="Nombre de dossiers déposés",
             annuaire="Les dossiers « déposés » au cours de l'exercice (p. 11) ; la durée de "
                      "liquidation part de la date de dépôt (glossaire, p. 64).",
             compris="Une demande reçue au guichet entre le 1er janvier et le 31 décembre. "
                     "C'est soit une nouvelle demande, qui ouvre un dossier, soit une demande de "
                     "réversion déposée sur le dossier d'un pensionné décédé. Je compte des "
                     "demandes, pas des dossiers : un dossier peut en recevoir plusieurs au fil "
                     "des ans.",
             exemple="un pensionné meurt en 2023 ; sa veuve dépose sa demande le 3 mars 2025 "
                     "sur le dossier de son mari : c'est un dépôt de 2025, sur la ligne pension "
                     "de survivant.",
             question="Tu valides : déposé = reçu au guichet dans l'année, réversions sur un "
                      "dossier existant comprises ?",
             base=["Table RECEPTION_DOSSIER, une ligne par passage au guichet, TDOS_CODE = 'V'.",
                   "RD_DATE_RECEPTION : la date du dépôt, entre le 01/01 et le 31/12.",
                   "RD_TYPE_RECEPTION = 'NOUVEAU' : ouvre un dossier (4 055 en 2025).",
                   "RD_TYPE_RECEPTION = 'ANCIEN' : arrive sur un dossier existant, à 94 % une "
                   "réversion (616 en 2025).",
                   "Une réversion jamais passée au guichet prend la date de demande de sa "
                   "pension (PRESTATION_ESP.PE_DATE_DEMANDE)."],
             sql=t25_sql("2")),
        dict(terme="Nombre de dossiers liquidés",
             annuaire="La liquidation va jusqu'à la « notification des droits au bénéficiaire » "
                      "(glossaire, p. 64).",
             compris="Une demande dont les droits ont été notifiés à l'assuré entre le 1er "
                     "janvier et le 31 décembre, quelle que soit l'année de son dépôt. Je prends "
                     "la première notification de la demande : une deuxième, souvent des années "
                     "après, est une révision. Je ne regarde ni le paiement ni le statut du "
                     "dossier, qui reste « actif » pour 61 632 dossiers.",
             exemple="demande déposée le 20 novembre 2024, droits notifiés le 15 janvier 2025 : "
                     "liquidée en 2025, en 56 jours.",
             question="Tu valides : liquidé = droits notifiés, à la date de la première "
                      "notification ?",
             base=["Table PRESTATION_ESP : PE_DATE_NOTIFICATION, la date où les droits sont "
                   "notifiés.",
                   "Une demande = un dossier (DOS_CODE) et une ligne du tableau (TPE_CODE, "
                   "terme 2).",
                   "Sa date de liquidation : la plus petite PE_DATE_NOTIFICATION de la demande.",
                   "Une réversion est une demande à part : elle compte l'année de sa propre "
                   "notification (environ 900 en 2025)."],
             sql=t25_sql("3")),
        dict(terme="Durée de liquidation",
             annuaire="« Temps entre la date de dépôt d'un dossier de prestation et celle de "
                      "notification des droits au bénéficiaire » (glossaire, p. 64).",
             compris="Le nombre de jours entre le dépôt au guichet et la première notification. "
                     "Je prends la date de réception saisie au guichet comme date du dossier "
                     "complet : 96 % des pièces sont saisies le jour même. Une réversion part de "
                     "sa propre réception, pas de celle du dossier du défunt. Une durée négative, "
                     "erreur de saisie, compte 0 jour.",
             exemple="reçu le 17 novembre 2025, notifié le 2 décembre 2025 : 15 jours.",
             question="Tu valides : la date saisie au guichet est bien celle du dossier "
                      "complet ?",
             base=["Début : RECEPTION_DOSSIER.RD_DATE_RECEPTION de la réception qui a ouvert le "
                   "dossier (DOSSIER.RD_ID).",
                   "Pour une réversion : sa réception ANCIEN sur le dossier, la plus récente "
                   "avant la notification.",
                   "Sans réception : PRESTATION_ESP.PE_DATE_DEMANDE.",
                   "Fin : la première PE_DATE_NOTIFICATION de la demande (terme 4)."],
             sql=t25_sql("4")),
        dict(terme="Moins de 15 jours, 15 à 45 jours, plus de 45 jours",
             annuaire="Les trois colonnes de la durée de liquidation (p. 11 et p. 34).",
             compris="Chaque demande liquidée tombe dans une seule colonne : 0 à 14 jours, 15 à "
                     "45 jours, 46 jours et plus. 15 jours et 45 jours vont dans la colonne du "
                     "milieu ; un dossier notifié le jour du dépôt va dans « moins de 15 "
                     "jours ». Les trois colonnes ensemble font les dossiers liquidés.",
             question="Tu valides les bornes : 15 et 45 jours comptent dans « 15 à 45 jours » ?",
             base=["La durée du terme 5, classée en trois.",
                   "2025, toutes lignes : 18 % en moins de 15 jours, 22 % de 15 à 45 jours, 60 % "
                   "en plus de 45 jours."],
             sql=t25_sql("5")),
        dict(terme="Dossiers en instance",
             annuaire="« Le nombre de dossiers restants à traiter en fin d'exercice » (p. 11).",
             compris="Une demande reçue, ni notifiée ni rejetée au 31 décembre, quelle que soit "
                     "l'année de son dépôt. J'enlève deux choses : les dossiers fermés sans "
                     "avoir jamais été notifiés, et les vieilles réceptions d'avant 2016 jamais "
                     "reliées à un dossier. Sans ça, on aurait 23 218 demandes en instance fin "
                     "2025, cinq fois les dépôts d'une année.",
             question="Tu valides : une demande qui attend depuis des années sort du stock "
                      "(reçue avant 2016 sans dossier, ou dossier fermé) ?",
             base=["RECEPTION_DOSSIER : les réceptions jusqu'au 31/12/2025.",
                   "Aucune notification après la réception : PRESTATION_ESP."
                   "PE_DATE_NOTIFICATION.",
                   "Aucun refus : DOSSIER_REJETE.",
                   "Dossier fermé : DOSSIER.DOS_STATUT = 'F' (A = actif).",
                   "Pas de dossier : la réception n'est reliée à aucun DOSSIER."],
             sql=t25_sql("6")),
        dict(terme="Dossiers rejetés",
             annuaire="La colonne « Dossiers rejetés » de la maquette (p. 34).",
             compris="Un dossier dont le refus a été enregistré dans l'année, avec son motif : "
                     "prescription, pas de bénéficiaire, pièce non conforme, autre. Il n'y en a "
                     "que 14 en 2025 (146 depuis 2017) : d'après l'atelier de Kaya, le module des "
                     "rejets existe mais sert peu. Un refus non saisi reste donc en instance.",
             question="Tu valides : tous les refus sont saisis dans le module des rejets ? "
                      "Sinon, où les trouver ?",
             base=["Table DOSSIER_REJETE, TDOS_CODE = 'V'.",
                   "DOSRE_DATE_REJET : la date du refus, entre le 01/01 et le 31/12.",
                   "RD_ID : la réception refusée ; DOS_CODE : le dossier, s'il existe."],
             sql=t25_sql("7")),
        dict(terme="Durée moyenne de liquidation et norme CIPRES",
             annuaire="Le tableau permet de « déterminer pour chaque type de prestation la "
                      "durée moyenne de liquidation » et de la comparer à la norme CIPRES : "
                      "moins de 45 jours (p. 11).",
             compris="La moyenne des durées des demandes liquidées dans l'année, ligne par "
                     "ligne. J'ajoute la médiane, la durée de la demande du milieu, que quelques "
                     "dossiers très lents ne faussent pas. En 2025 : moyenne 82 jours, médiane 62 "
                     "jours, au-dessus de la norme.",
             question="Tu valides la moyenne simple des durées, et l'ajout de la médiane ?",
             base=["Les durées du terme 5, pour les demandes liquidées en 2025.",
                   "La norme CIPRES : moins de 45 jours."],
             sql=t25_sql("8")),
        dict(terme="TOTAL",
             annuaire="La ligne TOTAL de la maquette (p. 34).",
             compris="La somme des lignes : chaque demande compte une fois. Une demande reçue qui "
                     "n'a encore aucune pension créée n'a pas de ligne : elle compte seulement "
                     "dans le Total des déposés et des en instance. La durée moyenne du Total "
                     "est recalculée sur toutes les demandes, pas tirée des moyennes des lignes.",
             question="Tu valides : les demandes sans ligne comptent dans le Total ?",
             base=["Toutes les demandes des termes 3 à 9, sans regarder la ligne."],
             sql=t25_sql("9")),
        dict(terme="Dossiers restants à traiter en fin d'exercice (le taux)",
             annuaire="Le nombre de dossiers restants à traiter en fin d'exercice « rentre dans "
                      "le calcul des prestations à payer » (p. 11). L'atelier de Kaya en fait un "
                      "indicateur, le taux des dossiers restant à traiter, sans formule.",
             compris="Le taux : les demandes en instance au 31 décembre, divisées par celles en "
                     "instance au 1er janvier plus celles déposées dans l'année. Ni l'Annuaire "
                     "ni l'atelier ne donnent de formule : c'est la mienne.",
             question="Tu valides cette formule du taux ?",
             base=["En instance au 31/12/2025 et au 31/12/2024 : le terme 7, à ces deux dates.",
                   "Déposés en 2025 : le terme 3 (4 671)."],
             sql=t25_sql("10")),
    ],
)

def demande_rejet_total_taux(fiche, deposes):
    """Les termes 7 à 11 de T26 et T27, qui se lisent comme ceux de T25."""
    return [
        dict(terme="Dossiers en instance",
             annuaire="« Le nombre de dossiers restants à traiter en fin d'exercice » (p. 12).",
             compris="Une demande reçue, ni liquidée ni rejetée au 31 décembre. J'enlève les "
                     "dossiers fermés sans rien avoir donné, et les demandes toujours sans suite "
                     "24 mois après leur dépôt : beaucoup ne donnent jamais rien et ne sont jamais "
                     "fermées, elles resteraient en instance pour toujours.",
             question="Tu valides : une demande sans suite depuis 24 mois sort du stock ?",
             base=["RECEPTION_DOSSIER : les réceptions des 24 derniers mois.",
                   "Aucune notification après la réception (PRESTATION_ESP), aucun refus "
                   "(DOSSIER_REJETE).",
                   "Dossier fermé : DOSSIER.DOS_STATUT = 'F'."],
             sql=sql(fiche, "6", DECALE)),
        dict(terme="Dossiers rejetés",
             annuaire="La colonne « Dossiers rejetés » de la maquette (p. 34).",
             compris="Un dossier dont le refus a été enregistré dans l'année, dans le module "
                     "des rejets. Il sert peu (atelier de Kaya) : une demande sans suite n'est "
                     "ni fermée ni rejetée.",
             question="Tu valides : tous les refus sont saisis dans le module des rejets ?",
             base=["Table DOSSIER_REJETE : DOSRE_DATE_REJET, la date du refus ; TDOS_CODE, la "
                   "branche."],
             sql=sql(fiche, "7", DECALE)),
        dict(terme="Durée moyenne de liquidation et norme CIPRES",
             annuaire="La durée moyenne par type de prestation, comparée à la norme CIPRES : "
                      "moins de 45 jours (p. 12).",
             compris="La moyenne des durées des demandes liquidées dans l'année, ligne par "
                     "ligne, avec la médiane en plus, que quelques dossiers très lents ne "
                     "faussent pas.",
             question="Tu valides la moyenne simple, avec la médiane en plus ?",
             base=["Les durées du terme 5, pour les demandes liquidées dans l'année."],
             sql=sql(fiche, "8", DECALE)),
        dict(terme="TOTAL",
             annuaire="La ligne TOTAL de la maquette (p. 34).",
             compris="La somme des lignes : chaque demande compte une fois sur sa ligne. Les "
                     "réceptions qui n'ont encore donné aucune prestation comptent seulement "
                     "dans le Total des déposés et des en instance. La durée moyenne du Total est "
                     "recalculée sur toutes les demandes.",
             question="Tu valides : les réceptions sans prestation comptent dans le Total ?",
             base=["Toutes les demandes des termes 3 à 9, sans regarder la ligne."],
             sql=sql(fiche, "9", DECALE)),
        dict(terme="Dossiers restants à traiter en fin d'exercice (le taux)",
             annuaire="Le nombre de dossiers restants à traiter en fin d'exercice (p. 12) ; "
                      "l'atelier de Kaya en fait un taux, sans formule.",
             compris="Les demandes en instance au 31 décembre, divisées par celles en instance "
                     "au 1er janvier plus celles déposées dans l'année : la même formule que "
                     "pour T25.",
             question="Tu valides cette formule ?",
             base=["En instance : le terme 7 au 31/12/2025 et au 31/12/2024.",
                   f"Déposés : le terme 3 ({deposes} en 2025)."],
             sql=sql(fiche, "10", DECALE)),
    ]


T26 = dict(
    code="T26",
    titre="Dossiers de prestations AT/MP de l'exercice selon la durée de liquidation",
    pages="Annuaire CIPRES : description p. 12, maquette p. 34, glossaire p. 64 et p. 65",
    annee="2025",
    resume=("Le même tableau que T25, pour les accidents du travail et les maladies "
            "professionnelles : combien de demandes arrivent, combien sont réglées et en combien "
            "de jours, combien restent à traiter. La victime a un seul dossier, qui garde toutes "
            "ses prestations : IJ, soins, puis rente."),
    termes=[
        dict(terme="Dossier de prestations AT/MP",
             annuaire="AT/MP : accidents du travail et maladies professionnelles. Une rente est "
                      "« une prestation périodique versée au titre de la législation sur les "
                      "AT/MP à la victime ou à ses ayants droit » (glossaire, p. 65).",
             compris="Le dossier de la victime, ouvert par la déclaration de l'accident ou de "
                     "la maladie. Il garde ensuite toutes ses prestations : les indemnités "
                     "journalières de l'arrêt, les soins, puis la rente, parfois des années "
                     "après. Tous les dossiers AT/MP comptent.",
             question="Tu valides : un seul dossier par victime, qui porte toutes ses "
                      "prestations ?",
             base=["Table DOSSIER, TDOS_CODE = 'A' (branche des risques professionnels).",
                   "La déclaration est la réception qui a ouvert le dossier : DOSSIER.RD_ID.",
                   "9 106 dossiers ouverts de 2019 à 2025."],
             sql=sql(t26, "1", DECALE)),
        dict(terme="Type de prestation (les lignes du tableau)",
             annuaire="Cinq lignes : rentes viagères, rentes de survivants, indemnités "
                      "journalières, allocation AT/MP, autres prestations de la branche, puis le "
                      "TOTAL (p. 34).",
             compris="La rente viagère est celle de la victime, après la consolidation. Les "
                     "rentes de survivants vont à la veuve, au veuf, aux orphelins et aux "
                     "ascendants. L'allocation AT/MP est l'allocation d'incapacité, pour les "
                     "petits taux. Dans « autres prestations », je mets les remboursements de "
                     "soins : consultations, pharmacie, examens, radios.",
             question="Tu valides ce classement, avec les soins dans « autres prestations » ?",
             base=["PRESTATION_ESP.TPE_CODE : ARI rente viagère ; ARSV veuve, ARSF veuf, ARSE "
                   "orphelin, ARSA ascendant ; AIJ indemnités journalières ; AAI allocation "
                   "d'incapacité.",
                   "Les soins ne sont pas dans PRESTATION_ESP : ce sont des remboursements, "
                   "DEBOURS avec un TPN_CODE (le type de soin)."],
             sql="""-- les prestations AT/MP par code
SELECT pe.TPE_CODE, COUNT(*)
FROM   USER_DWH.PRESTATION_ESP pe
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = pe.DOS_CODE
WHERE  d.TDOS_CODE = 'A'
GROUP  BY pe.TPE_CODE
-- les soins remboursés, par type de soin
SELECT deb.TPN_CODE, COUNT(*)
FROM   USER_DWH.DEBOURS deb
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = deb.DOS_CODE
WHERE  d.TDOS_CODE = 'A'
AND    deb.TPN_CODE IS NOT NULL
GROUP  BY deb.TPN_CODE"""),
        dict(terme="Nombre de dossiers déposés",
             annuaire="Les dossiers « déposés » au cours de l'exercice (p. 12).",
             compris="Une demande reçue au guichet dans l'année. La déclaration de l'accident "
                     "est le dépôt des IJ et des soins. Une rente, une allocation ou une rente de "
                     "survivant est déposée à part, par sa propre demande sur le dossier qui "
                     "existe. Une rente sans demande au guichet prend la date du certificat pour "
                     "la rente, sinon celle de la déclaration.",
             exemple="accident déclaré en mars 2023 ; la victime demande sa rente au guichet le "
                     "10 juin 2025 : c'est un dépôt de 2025, sur la ligne rentes viagères.",
             question="Tu valides : la déclaration vaut dépôt des IJ et des soins, et la rente a "
                      "son propre dépôt ?",
             base=["RECEPTION_DOSSIER, TDOS_CODE = 'A' ; RD_DATE_RECEPTION, la date du dépôt.",
                   "NOUVEAU : la déclaration (1 345 en 2025). ANCIEN : une demande sur le "
                   "dossier, suivie à 70 % d'une rente ou d'une allocation, à 9 % d'une rente "
                   "de survivant (136 en 2025).",
                   "Certificat pour la rente : DOSSIER.DOS_DATE_REC_CM_RENTE."],
             sql=sql(t26, "2", DECALE)),
        dict(terme="Nombre de dossiers liquidés",
             annuaire="La liquidation va jusqu'à la notification des droits au bénéficiaire "
                      "(glossaire, p. 64).",
             compris="Une demande dont les droits ont été notifiés dans l'année : la première "
                     "notification de la demande. Une IJ prolongée donne plusieurs IJ : la "
                     "demande compte une fois, à la première. Les soins n'ont pas de "
                     "notification : je les compte liquidés au premier remboursement saisi.",
             question="Tu valides : les soins sont liquidés au premier remboursement saisi ?",
             base=["PRESTATION_ESP.PE_DATE_NOTIFICATION : la plus petite de la demande (97 % "
                   "des IJ en ont une ; l'allocation d'incapacité depuis 2019).",
                   "Soins : DEBOURS.DEB_DATE_INSERT du premier remboursement du dossier."],
             sql=sql(t26, "3", DECALE)),
        dict(terme="Durée de liquidation",
             annuaire="« Temps entre la date de dépôt d'un dossier de prestation et celle de "
                      "notification des droits au bénéficiaire » (glossaire, p. 64).",
             compris="Les jours entre le dépôt de la demande et sa liquidation. Pour les IJ et "
                     "les soins, je pars de la déclaration ; pour une rente, de sa demande au "
                     "guichet. L'atelier de Kaya partait du certificat de consolidation pour les "
                     "rentes, et du certificat du médecin pour les soins, une date que la base n'a "
                     "pas.",
             question="Tu valides : la durée part du dépôt au guichet, comme pour les pensions, "
                      "et pas du certificat ?",
             base=["IJ et soins : RD_DATE_RECEPTION de la déclaration (DOSSIER.RD_ID).",
                   "Rente, allocation : la réception ANCIEN du dossier, sinon "
                   "DOS_DATE_REC_CM_RENTE, sinon la déclaration.",
                   "2025, en médiane : rentes 14 jours, allocations 13, survivants 35, IJ 251 "
                   "(l'arrêt de travail compris), soins environ 155."],
             sql=sql(t26, "4", DECALE)),
        dict(terme="Moins de 15 jours, 15 à 45 jours, plus de 45 jours",
             annuaire="Les trois colonnes de la durée de liquidation (p. 12 et p. 34).",
             compris="0 à 14 jours, 15 à 45 jours, 46 jours et plus, comme pour T25. Les trois "
                     "colonnes ensemble font les liquidés.",
             question="Tu valides les mêmes bornes que pour T25 ?",
             base=["La durée du terme 5, classée en trois.",
                   "2025 : plus de 45 jours pour au moins 85 % des IJ, pour 30 % des rentes."],
             sql=sql(t26, "5", DECALE)),
    ] + demande_rejet_total_taux(t26, "1 481"),
)

T27 = dict(
    code="T27",
    titre="Dossiers de prestations PF de l'exercice selon la durée de liquidation",
    pages="Annuaire CIPRES : description p. 12, maquette p. 34, glossaire p. 62 et p. 64",
    annee="2025",
    resume=("Le même tableau que T25, pour les prestations familiales, maternité comprise : un "
            "dossier par allocataire pour les allocations familiales, un dossier par grossesse "
            "pour la maternité."),
    termes=[
        dict(terme="Dossier de prestations PF",
             annuaire="L'allocataire est la personne qui perçoit des prestations familiales du "
                      "chef de son activité professionnelle ou de celle d'autrui (glossaire, "
                      "p. 62).",
             compris="Deux sortes de dossiers dans la branche : celui de l'allocataire, pour "
                     "les allocations familiales de tous ses enfants, et celui d'une grossesse, "
                     "pour la maternité. La maternité est rattachée aux prestations familiales ; "
                     "ce n'est pas la maladie, aucun soin n'y est payé.",
             question="Tu valides : un dossier par allocataire, et un dossier par grossesse ?",
             base=["Table DOSSIER : TDOS_CODE = 'F' prestations familiales, 'M' maternité.",
                   "Une mère n'a qu'un dossier de maternité par an."],
             sql=sql(t27, "1", DECALE)),
        dict(terme="Type de prestation (les lignes du tableau)",
             annuaire="Les lignes : allocations familiales, indemnités journalières de "
                      "maternité, allocations prénatales, autres prestations de la branche, puis "
                      "le TOTAL (p. 34).",
             compris="Les allocations familiales sont celles de l'allocataire, pour ses enfants. "
                     "Les IJ et l'allocation prénatale viennent du dossier de la grossesse. "
                     "J'ajoute l'ancienne allocation prénatale des PF (33 en 2025). Aucune autre "
                     "prestation n'est payée aujourd'hui : des frais de maternité sont prévus "
                     "dans la base, jamais payés.",
             question="Tu valides ce classement des codes ?",
             base=["PRESTATION_ESP.TPE_CODE : FAF allocations familiales ; MIJ indemnités "
                   "journalières de maternité ; MAP allocation prénatale de la maternité, FAP "
                   "l'ancienne des PF."],
             sql="""-- les prestations PF par code
SELECT d.TDOS_CODE, pe.TPE_CODE, COUNT(*)
FROM   USER_DWH.PRESTATION_ESP pe
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = pe.DOS_CODE
WHERE  d.TDOS_CODE IN ('F', 'M')
GROUP  BY d.TDOS_CODE, pe.TPE_CODE"""),
        dict(terme="Nombre de dossiers déposés",
             annuaire="Les dossiers « déposés » au cours de l'exercice (p. 12) ; l'atelier de "
                      "Kaya : le dépôt, c'est la réception du dossier complet au guichet.",
             compris="Une demande reçue au guichet dans l'année : un nouvel allocataire, ou une "
                     "nouvelle grossesse, qui ouvre son dossier. Une grossesse est déposée une "
                     "fois sur chacune de ses deux lignes (IJ et prénatale). Un enfant ajouté "
                     "plus tard ne passe pas au guichet : je le compte déposé à la date de sa "
                     "demande.",
             question="Tu valides : un enfant ajouté est une demande de plus, déposée à sa date "
                      "de demande ?",
             base=["RECEPTION_DOSSIER, TDOS_CODE 'F' ou 'M' ; RD_DATE_RECEPTION.",
                   "2025 : 9 968 réceptions PF et 4 338 de maternité ; 6 seulement sur un "
                   "dossier existant (certificats de scolarité ou de vie).",
                   "Enfant ajouté : PRESTATION_ESP.PE_DATE_DEMANDE de sa prestation FAF."],
             sql=sql(t27, "2", DECALE)),
        dict(terme="Nombre de dossiers liquidés",
             annuaire="La liquidation va jusqu'à la notification des droits (glossaire, p. 64). "
                      "Kaya : pour T54 et T55, on compte les IJ et les prénatales liquidées.",
             compris="Une demande dont les droits ont été notifiés dans l'année, à la première "
                     "notification. Les deux lignes d'une grossesse sont notifiées ensemble, "
                     "environ 15 jours après la demande. Un enfant ajouté est liquidé à sa "
                     "propre notification.",
             question="Tu valides : l'enfant ajouté compte comme une demande liquidée à part ?",
             base=["PRESTATION_ESP.PE_DATE_NOTIFICATION, la première de chaque dossier et "
                   "ligne.",
                   "Enfant ajouté : chaque autre date de notification FAF du dossier.",
                   "2025 : 8 246 nouveaux allocataires, environ 8 100 dossiers avec un enfant "
                   "ajouté, 4 273 IJ de maternité, 4 245 allocations prénatales."],
             sql=sql(t27, "3", DECALE)),
        dict(terme="Durée de liquidation",
             annuaire="« Temps entre la date de dépôt d'un dossier de prestation et celle de "
                      "notification des droits au bénéficiaire » (glossaire, p. 64).",
             compris="Les jours entre le dépôt au guichet et la première notification. Une "
                     "grossesse est déposée environ 3 semaines avant l'accouchement : la durée "
                     "ne compte pas l'attente de la naissance. Un enfant ajouté part de sa date "
                     "de demande. Une durée négative compte 0.",
             question="Tu valides : la durée part de la réception, même pour une grossesse "
                      "déposée avant la naissance ?",
             base=["Début : RD_DATE_RECEPTION de la réception qui a ouvert le dossier ; à défaut "
                   "PE_DATE_DEMANDE.",
                   "2025, en médiane : 13,5 jours pour les allocataires, 17 pour la maternité, "
                   "environ 11 pour un enfant ajouté."],
             sql=sql(t27, "4", DECALE)),
        dict(terme="Moins de 15 jours, 15 à 45 jours, plus de 45 jours",
             annuaire="Les trois colonnes de la durée de liquidation (p. 12 et p. 34).",
             compris="0 à 14 jours, 15 à 45 jours, 46 jours et plus, comme pour T25.",
             question="Tu valides les mêmes bornes que pour T25 ?",
             base=["La durée du terme 5, classée en trois.",
                   "Dossiers ouverts en 2025 : allocataires 57 %, 24 %, 19 % ; maternité 44 %, "
                   "44 %, 12 %."],
             sql=sql(t27, "5", DECALE)),
    ] + demande_rejet_total_taux(t27, "14 306"),
)

T28 = dict(
    code="T28 à T30",
    titre="Dossiers PVID, AT/MP et PF contrôlés a posteriori, par type de prestation",
    pages="Annuaire CIPRES : description p. 12 et p. 13, maquettes p. 35 et p. 36, glossaire "
          "p. 64",
    annee="2025",
    resume=("Pour moi, ces trois tableaux disent si la CNSS vérifie après coup les dossiers "
            "qu'elle paie : combien de dossiers sont payés, combien sont contrôlés chaque "
            "trimestre, et si la part contrôlée atteint la norme de 5 %. T28 pour les pensions, "
            "T29 pour les AT/MP, T30 pour les prestations familiales."),
    termes=[
        dict(terme="Nombre de dossiers payés",
             annuaire="« Dossier de prestation dont les droits échus ont été payés au "
                      "bénéficiaire par l'organisme » (glossaire, p. 64).",
             compris="Un dossier qui a reçu au moins un paiement dans l'année. Je compte des "
                     "dossiers, pas des paiements : une pension payée douze fois est un dossier "
                     "payé.",
             question="Tu valides : un dossier payé = au moins un paiement dans l'année ?",
             base=["Table DEBOURS : DEB_TYPE = 'PM' (paiement), DEB_STATUT 'D' ou 'P' (payé), "
                   "DEB_DATE_EFFET dans l'année.",
                   "CODE_BRANCHE : V pensions, A AT/MP, F et M prestations familiales.",
                   "2025, tout débours : V 47 732 dossiers, A 2 134, F 100 113, M 5 227."],
             sql="""-- les dossiers payés en 2025, par branche
SELECT CODE_BRANCHE,
       COUNT(DISTINCT DOS_CODE) AS payes
FROM   USER_DWH.DEBOURS
WHERE  DEB_TYPE = 'PM'
AND    DEB_STATUT IN ('D', 'P')
AND    DEB_DATE_EFFET >= DATE '2025-01-01'
AND    DEB_DATE_EFFET <  DATE '2026-01-01'
GROUP  BY CODE_BRANCHE"""),
        dict(terme="Dossier contrôlé a posteriori",
             annuaire="« Ces contrôles portent sur les dossiers déjà liquidés aux fins de "
                      "s'assurer que les droits servis sont conformes aux éléments du dossier » "
                      "(p. 12).",
             compris="Un dossier déjà payé qu'un agent a revérifié, et dont le contrôle est "
                     "enregistré. La base a une table des contrôles, mais plus rien n'y est saisi "
                     "depuis juillet 2025. La case « vérifié » des paiements n'est pas un "
                     "contrôle a posteriori : c'est une coche de saisie. D'après l'atelier de "
                     "Kaya, les contrôles sont faits par l'audit interne, dans des rapports.",
             question="Tu valides : seuls les contrôles de la table des contrôles comptent ? Où "
                      "sont-ils enregistrés depuis juillet 2025 ?",
             base=["Table DOSSIERS_CONTROLES : DOS_CODE, TDOS_CODE, DC_DATE_CONTROLE.",
                   "1 542 contrôles en tout, le dernier le 09/07/2025 ; quelques dates "
                   "fausses (années 202, 203, 2055).",
                   "2025 : 115 contrôles (V 25, A 4, F 40, M 46).",
                   "DEBOURS.DEB_VERIFIE = 'O' : pas un contrôle."],
             sql="""-- les contrôles par année et branche
SELECT EXTRACT(YEAR FROM DC_DATE_CONTROLE)
         AS annee,
       TDOS_CODE,
       COUNT(DISTINCT DOS_CODE) AS controles
FROM   USER_DWH.DOSSIERS_CONTROLES
GROUP  BY EXTRACT(YEAR FROM DC_DATE_CONTROLE),
          TDOS_CODE
ORDER  BY 1, 2"""),
        dict(terme="1er, 2e, 3e et 4e trimestre",
             annuaire="Les dossiers contrôlés sont répartis par trimestre ; le tableau se "
                      "remplit chaque fin de trimestre (p. 12).",
             compris="Le trimestre de la date du contrôle : janvier à mars, avril à juin, "
                     "juillet à septembre, octobre à décembre. Un dossier contrôlé deux fois "
                     "dans le même trimestre compte une fois.",
             question="Tu valides : le trimestre est celui de la date du contrôle ?",
             base=["DOSSIERS_CONTROLES.DC_DATE_CONTROLE, découpée en trimestres."],
             sql="""SELECT TDOS_CODE,
       TO_CHAR(DC_DATE_CONTROLE, 'Q')
         AS trimestre,
       COUNT(DISTINCT DOS_CODE) AS controles
FROM   USER_DWH.DOSSIERS_CONTROLES
WHERE  DC_DATE_CONTROLE >= DATE '2025-01-01'
AND    DC_DATE_CONTROLE <  DATE '2026-01-01'
GROUP  BY TDOS_CODE,
          TO_CHAR(DC_DATE_CONTROLE, 'Q')"""),
        dict(terme="Type de prestation (les lignes)",
             annuaire="T28 : pensions de retraite, d'invalidité, de veuf ou veuve, d'orphelin, "
                      "de solidarité, d'ascendants ; allocations de retraite, de veuf ou veuve, "
                      "d'orphelin ; autres prestations (p. 35). T29 et T30 : les lignes de T26 "
                      "et T27.",
             compris="La ligne vient de la prestation principale du dossier contrôlé, avec les "
                     "mêmes codes que T25 à T27. La « pension de solidarité » n'a aucun code dans "
                     "la base : je laisse la ligne vide.",
             question="Qu'est-ce qu'une pension de solidarité à la CNSS, et où la trouver ?",
             base=["PRESTATION_ESP.TPE_CODE du dossier contrôlé, comme pour T25 (T28), T26 "
                   "(T29), T27 (T30).",
                   "Allocation de retraite : RAV ; allocations de veuf ou veuve et d'orphelin : "
                   "RASC, RASO."],
             sql="""-- les codes des dossiers contrôlés
SELECT c.TDOS_CODE, pe.TPE_CODE,
       COUNT(DISTINCT c.DOS_CODE)
FROM   USER_DWH.DOSSIERS_CONTROLES c
JOIN   USER_DWH.PRESTATION_ESP pe
       ON pe.DOS_CODE = c.DOS_CODE
GROUP  BY c.TDOS_CODE, pe.TPE_CODE"""),
        dict(terme="Ratio de contrôle a posteriori",
             annuaire="Le tableau permet de « déterminer le ratio de contrôle a posteriori dont "
                      "la norme est de 5 % au moins » (p. 12).",
             compris="Les dossiers contrôlés dans l'année divisés par les dossiers payés dans "
                     "l'année, par branche. En 2025 : 115 contrôles pour 155 206 dossiers payés, "
                     "0,07 %, très loin de la norme.",
             question="Tu valides la formule : contrôlés de l'année sur payés de l'année ?",
             base=["Contrôlés : le terme 2. Payés : le terme 1."],
             sql="""SELECT ROUND(100 * (
         SELECT COUNT(DISTINCT DOS_CODE)
         FROM   USER_DWH.DOSSIERS_CONTROLES
         WHERE  EXTRACT(YEAR FROM
                  DC_DATE_CONTROLE) = 2025)
       / (SELECT COUNT(DISTINCT DOS_CODE)
          FROM   USER_DWH.DEBOURS
          WHERE  DEB_TYPE = 'PM'
          AND    DEB_STATUT IN ('D', 'P')
          AND    EXTRACT(YEAR FROM
                   DEB_DATE_EFFET) = 2025), 2)
         AS ratio_controle_pct
FROM   DUAL
-- 2025 : 0,07 % (norme : au moins 5 %)"""),
    ],
)

T31 = dict(
    code="T31",
    titre="Prestations indues par branche et par mois",
    pages="Annuaire CIPRES : description p. 13, maquette p. 36, glossaire p. 64 à p. 66",
    annee="2024",
    resume=("Pour moi, ce tableau suit chaque mois ce que la CNSS a payé de trop, face à ce "
            "qu'elle a payé, par branche, et le compare à la norme : au plus 2 % d'indus."),
    termes=[
        dict(terme="Branche",
             annuaire="Trois branches : PVID (pensions), AT/MP, PF (prestations familiales).",
             compris="La branche vient du type de dossier. Je range la maternité avec les "
                     "prestations familiales, comme partout dans l'Annuaire.",
             question="Tu valides : la maternité compte dans la branche PF ?",
             base=["DOSSIER.TDOS_CODE : V PVID, A AT/MP, F et M PF.",
                   "Le DTM laissait la maternité de côté (15 trop-perçus en 2024)."],
             sql=sql(t31, "1")),
        dict(terme="Prestations payées",
             annuaire="L'atelier de Kaya : « la somme décaissée au profit de l'assuré ».",
             compris="Les paiements effectués dans le mois, toutes prestations de la branche. "
                     "Je prends le mois payé ; le mois du versement donne le même taux à l'année.",
             question="Tu valides : les payées sont les paiements du mois, au mois payé ?",
             base=["DEBOURS : DEB_TYPE 'PM', DEB_STATUT 'D' (payé par effet) ou 'P' (payé sans "
                   "effet), DEB_MONTANT.",
                   "Mois payé : DEB_DATE_EFFET ; mois du versement : DEB_DATE_INSERT.",
                   "2024 : 58,7 milliards."],
             sql=sql(t31, "2")),
        dict(terme="Trop perçu",
             annuaire="« Paiement de trop, au détriment de l'organisme » (glossaire, p. 66) ; "
                      "une prestation indue est une « prestation irrégulièrement versée » "
                      "(p. 65).",
             compris="Un ajustement « trop-perçu », ou « trop-perçu sur convention », établi "
                     "dans le mois. C'est lui, l'indu du tableau.",
             question="Tu valides : l'indu = les trop-perçus établis dans le mois ?",
             base=["Table AJUSTEMENT : TAJ_CODE 'TP' ou 'TP-CONV', AJ_MONTANT, "
                   "AJ_DATE_ETABLISSEMENT.",
                   "2024 : 684 trop-perçus, 229,5 millions."],
             sql=sql(t31, "3")),
        dict(terme="Moins perçu",
             annuaire="« Différence négative entre le montant du droit dû et le montant payé » "
                      "(glossaire, p. 64).",
             compris="Un ajustement « moins-perçu ou rappel », ou « rappel sur convention ». Il "
                     "est dans le tableau, mais ce n'est pas un indu : la CNSS a payé trop peu.",
             question="Tu valides : les moins-perçus sont montrés, mais n'entrent pas dans le "
                      "taux ?",
             base=["AJUSTEMENT.TAJ_CODE 'MP' ou 'RC'.", "2024 : 414 millions."],
             sql=sql(t31, "4")),
        dict(terme="Ce qui n'est pas un indu",
             annuaire="Rien dans l'Annuaire ; l'atelier de Kaya ne range dans les indus que les "
                      "trop-perçus.",
             compris="Le remboursement d'un trop-perçu, les avances et leur reprise, les mois "
                     "de pension gratuits, les dons et avances COVID, les rachats de rente : ce "
                     "sont des mouvements d'argent, pas des erreurs de paiement.",
             question="Tu valides cette liste de ce qui reste hors du tableau ?",
             base=["AJUSTEMENT.TAJ_CODE : REM-TP remboursement, AV avance, MPG mois gratuits, "
                   "et les autres types ; libellés dans TYPE_AJUSTEMENT."],
             sql=sql(t31, "5")),
        dict(terme="Le statut d'un ajustement",
             annuaire="Rien dans l'Annuaire.",
             compris="Je garde tous les ajustements sauf ceux annulés : un trop-perçu en cours "
                     "de reprise ou déjà soldé reste un indu du mois où il a été établi.",
             question="Tu valides : seul le statut « annulé » sort du tableau ?",
             base=["AJUSTEMENT.AJ_STATUT : C soldé, A en cours, R annulé."],
             sql=sql(t31, "6")),
        dict(terme="Le mois",
             annuaire="Le tableau est mensuel, de janvier à décembre (p. 36).",
             compris="Un trop-perçu compte le mois où il est établi, pas le mois où il a été "
                     "payé en trop, ni celui où il est repris.",
             exemple="une pension payée en trop de janvier à août 2024, trop-perçu établi en "
                     "novembre 2024 : il compte en novembre.",
             question="Tu valides : le mois d'un indu est celui où il est établi ?",
             base=["AJUSTEMENT.AJ_DATE_ETABLISSEMENT."],
             sql=sql(t31, "7")),
        dict(terme="Taux de prestations indues",
             annuaire="Suivre les indus par rapport aux prestations payées, « norme : 2 % au "
                      "plus » (p. 13).",
             compris="Les trop-perçus divisés par les payées, en pourcentage, par branche et "
                     "sur l'année. En 2024 : 0,39 %, très en dessous de la norme.",
             question="Tu valides la formule : trop-perçus sur payées ?",
             base=["Trop-perçus : le terme 3 ; payées : le terme 2.",
                   "2024 : PVID 0,47 %, AT/MP 0,47 %, PF 0,05 %."],
             sql=sql(t31, "8")),
    ],
)

T32 = dict(
    code="T32 à T40",
    titre="Pensionnés et crédirentiers au 31 décembre, montants par tranche d'âge et par sexe",
    pages="Annuaire CIPRES : description p. 13 et p. 14, maquettes p. 37 à p. 41, glossaire "
          "p. 62 et p. 65",
    annee="2024",
    resume=("Pour moi, ces neuf tableaux font la photo, au 31 décembre, des personnes qui "
            "touchent une pension (T32 à T36) ou une rente d'accident du travail (T37 à T40) : "
            "combien elles sont, par âge et par sexe, et ce qu'on leur a payé dans l'année. "
            "Ils se calculent tous de la même façon : la requête est donnée pour T32, on change "
            "les codes et la grille d'âge pour les autres."),
    termes=[
        dict(terme="Les tableaux de la famille",
             annuaire="Pensionnés : vieillesse (T32), invalidité (T33), veufs et veuves (T34), "
                      "orphelins (T35), ascendants (T36). Crédirentiers des AT/MP : rente "
                      "viagère (T37), veufs et veuves (T38), orphelins (T39), ascendants (T40). "
                      "Le pensionné ascendant est le père ou la mère d'un assuré décédé "
                      "(glossaire, p. 65).",
             compris="Chaque tableau a ses codes de prestation et sa grille d'âge. Je mets la "
                     "retraite anticipée avec la vieillesse. Un orphelin compte jusqu'à 24 ans.",
             question="Tu valides ce rattachement des codes, et la limite de 25 ans pour les "
                      "orphelins ?",
             base=["T32 RPV et RPRA, moins de 55 à 100 ans et plus ; T33 RPI, moins de 25 à 60 "
                   "et plus ; T34 RPSV et RPSF, moins de 15 à 100 et plus ; T35 RPSE, moins de "
                   "5 à 25 et plus ; T36 RPA, moins de 50 à 100 et plus.",
                   "T37 ARI, moins de 20 à 100 et plus ; T38 ARSV et ARSF, idem ; T39 ARSE, "
                   "moins de 5 à 25 et plus ; T40 ARSA, moins de 50 à 80 et plus.",
                   "Tous dans PRESTATION_ESP.TPE_CODE."],
             sql="""-- les pensions et rentes en cours au
-- 31/12/2024, par code (sans les autres
-- conditions du terme 2)
SELECT TPE_CODE, COUNT(*)
FROM   USER_DWH.PRESTATION_ESP
WHERE  TPE_CODE IN ('RPV', 'RPRA', 'RPI',
         'RPSV', 'RPSF', 'RPSE', 'RPA', 'ARI',
         'ARSV', 'ARSF', 'ARSE', 'ARSA')
AND    PE_DATE_EFFET <= DATE '2024-12-31'
AND   (PE_DATE_RADIATION IS NULL
       OR PE_DATE_RADIATION
            > DATE '2024-12-31')
GROUP  BY TPE_CODE"""),
        dict(terme="Pensionné au 31 décembre",
             annuaire="Les pensionnés sont l'« ensemble des bénéficiaires de pensions au titre "
                      "de l'assurance vieillesse, invalidité et décès » (glossaire, p. 65), comptés "
                      "à la fin de l'exercice (p. 13).",
             compris="Une pension en cours le 31 décembre : son effet a commencé, elle n'est ni "
                     "radiée ni suspendue, et son titulaire est en vie. Qu'elle ait été payée en "
                     "décembre ou non. C'est ce que fait l'annuaire de la CNSS : nos chiffres de "
                     "2020 à 2024 retombent dessus à moins de 1 % près.",
             exemple="une pension suspendue le 31 octobre 2024 et pas réactivée n'est pas "
                     "comptée au 31/12/2024.",
             question="Tu valides : en cours au 31/12, titulaire en vie, payé ou non ?",
             base=["PRESTATION_ESP : PE_DATE_EFFET avant le 31/12 ; PE_DATE_RADIATION vide ou "
                   "après ; PE_DATE_SUSPENSION vide, après, ou suivie d'une "
                   "PE_DATE_REACTIVATION.",
                   "INDIVIDU.IND_DATE_DECES vide ou après le 31/12."],
             sql=sql(t32, "2")),
        dict(terme="La personne comptée",
             annuaire="Le tableau compte des pensionnés, des personnes (p. 37).",
             compris="Le titulaire de la pension, une seule fois, même s'il a deux pensions "
                     "du même tableau après une révision. Son montant est celui de toutes ses "
                     "pensions du tableau.",
             question="Tu valides : une personne compte une fois par tableau ?",
             base=["PRESTATION_ESP.IND_ID_PREST : la personne qui reçoit la pension.",
                   "2024 : 31 267 pensions de vieillesse en cours pour 31 093 personnes."],
             sql=sql(t32, "3")),
        dict(terme="L'âge",
             annuaire="« Obtenu par la différence entre l'année de constatation et l'année de "
                      "naissance » (glossaire, p. 62).",
             compris="L'année de l'exercice moins l'année de naissance, sans regarder le jour "
                     "d'anniversaire. 120 ans ou plus, ou pas de date : âge inconnu. L'année "
                     "1900 est une date par défaut, pas un vrai âge.",
             exemple="né en 1960, il a 64 ans dans T32 de 2024.",
             question="Tu valides l'âge par différence des années, comme le dit le glossaire ?",
             base=["INDIVIDU.IND_DATE_NAISSANCE de la personne."],
             sql=sql(t32, "4")),
        dict(terme="Tranches d'âge",
             annuaire="Des tranches de 5 ans, avec une première et une dernière tranche ouvertes "
                      "propres à chaque tableau (p. 37 à p. 41).",
             compris="T32 : moins de 55 ans, 55-59, 60-64… 95-99, 100 ans et plus. Les autres "
                     "tableaux suivent leur propre grille (terme 1).",
             question="Tu valides les grilles de l'Annuaire telles quelles ?",
             base=["L'âge du terme 4, rangé dans la grille du tableau."],
             sql=sql(t32, "5")),
        dict(terme="Le sexe",
             annuaire="Hommes et femmes, en colonnes (p. 37).",
             compris="Le sexe de la fiche de la personne.",
             question="Tu valides : le sexe vient de la fiche de la personne ?",
             base=["INDIVIDU.IND_SEXE : 1 homme, 2 femme.",
                   "2024, T32 : 24 814 hommes, 6 279 femmes."],
             sql=sql(t32, "6")),
        dict(terme="Le montant",
             annuaire="Le montant des pensions, par tranche et par sexe (p. 37).",
             compris="Ce qui a été payé dans l'année aux pensionnés comptés, pour les mois de "
                     "l'année. Pas le net à payer d'un état de paiement : le montant de chaque "
                     "paiement fait. Les pensionnés sortis avant le 31/12 ne sont pas comptés, "
                     "leur montant non plus.",
             question="Tu valides : le montant = ce qui a été payé dans l'année aux personnes "
                      "comptées ?",
             base=["DEBOURS : DEB_TYPE 'PM', DEB_STATUT 'D' ou 'P', DEB_MONTANT, DEB_DATE_EFFET "
                   "dans l'année.",
                   "2024 : 37,67 milliards ; et 0,96 milliard à 1 105 pensionnés sortis dans "
                   "l'année."],
             sql=sql(t32, "7")),
        dict(terme="TOTAL et montant moyen",
             annuaire="La ligne TOTAL et le montant moyen (p. 37).",
             compris="Le Total est la somme des tranches. Le montant moyen est le montant divisé "
                     "par l'effectif, par tranche, par sexe et au total.",
             question="Tu valides : montant moyen = montant de l'année divisé par l'effectif "
                      "au 31/12 ?",
             base=["Les termes 3 et 7.", "2024 : environ 1,2 million de FCFA par retraité et "
                   "par an."],
             sql=sql(t32, "8")),
    ],
)

T41 = dict(
    code="T41",
    titre="Allocataires de la branche PF et montants par tranche d'âge et par sexe",
    pages="Annuaire CIPRES : description p. 14, maquette p. 41, glossaire p. 62",
    annee="2024",
    resume=("La même photo au 31 décembre que T32, pour les allocataires des prestations "
            "familiales : combien de familles touchent des allocations, l'âge et le sexe de "
            "l'allocataire, et ce qu'on leur a payé."),
    termes=[
        dict(terme="Les allocations comptées",
             annuaire="Les allocataires de la branche PF (p. 14).",
             compris="Les allocations familiales versées pour les enfants. Les allocations "
                     "prénatales ont leur tableau (T55), la maternité aussi (T54).",
             question="Tu valides : T41 ne compte que les allocations familiales ?",
             base=["PRESTATION_ESP.TPE_CODE = 'FAF', une prestation par enfant."],
             sql=sql(t41, "1")),
        dict(terme="L'enfant compté au 31 décembre",
             annuaire="Rien de plus dans l'Annuaire que « à la fin de l'exercice ».",
             compris="Une allocation en cours le 31 décembre, comme pour T32, pour un enfant en "
                     "vie de 21 ans au plus, et payée dans l'année. Sans la condition de "
                     "paiement, on compterait 5 % de trop.",
             question="Tu valides : un enfant compte seulement s'il a été payé dans l'année ?",
             base=["PRESTATION_ESP : en cours au 31/12 (terme 2 de T32).",
                   "INDIVIDU de l'enfant : IND_DATE_NAISSANCE, IND_DATE_DECES.",
                   "DEBOURS : au moins un paiement dans l'année.",
                   "2024 : 271 577 enfants (l'annuaire de la CNSS : 268 481)."],
             sql=sql(t41, "2")),
        dict(terme="Allocataire",
             annuaire="« Personne qui perçoit des prestations familiales du chef de son "
                      "activité professionnelle ou de celle d'autrui » (glossaire, p. 62).",
             compris="Le titulaire du dossier d'allocations familiales qui a au moins un enfant "
                     "compté, une seule fois, quel que soit le nombre de ses enfants.",
             question="Tu valides : l'allocataire est le titulaire du dossier ?",
             base=["DOSSIER.IND_ID : le titulaire du dossier.",
                   "2024 : 98 048 allocataires, 2,8 enfants chacun (l'annuaire de la CNSS : "
                   "97 378)."],
             sql=sql(t41, "3")),
        dict(terme="L'âge",
             annuaire="Année de constatation moins année de naissance (glossaire, p. 62).",
             compris="L'âge de l'allocataire, pas celui des enfants.",
             exemple="née en 1984, l'allocataire a 40 ans dans T41 de 2024.",
             question="Tu valides : l'âge est celui de l'allocataire ?",
             base=["INDIVIDU.IND_DATE_NAISSANCE du titulaire du dossier."],
             sql=sql(t41, "4")),
        dict(terme="Tranches d'âge",
             annuaire="Moins de 20 ans, 20-24… 75-79, 80 ans et plus (p. 41).",
             compris="La grille de l'Annuaire telle quelle.",
             question="Tu valides la grille de l'Annuaire ?",
             base=["L'âge du terme 4."],
             sql=sql(t41, "5")),
        dict(terme="Le sexe",
             annuaire="Hommes et femmes, en colonnes (p. 41).",
             compris="Le sexe de l'allocataire, d'après sa fiche.",
             question="Tu valides : le sexe est celui de l'allocataire ?",
             base=["INDIVIDU.IND_SEXE du titulaire : 1 homme, 2 femme."],
             sql=sql(t41, "6")),
        dict(terme="Le montant",
             annuaire="Le montant des allocations, par tranche et par sexe (p. 41).",
             compris="Ce qui a été payé dans l'année pour les enfants comptés du dossier.",
             question="Tu valides : le montant = le payé de l'année pour les enfants comptés ?",
             base=["DEBOURS : DEB_TYPE 'PM', DEB_STATUT 'D' ou 'P', DEB_DATE_EFFET dans "
                   "l'année.", "2024 : 7,69 milliards."],
             sql=sql(t41, "7")),
        dict(terme="TOTAL et allocation moyenne",
             annuaire="La ligne TOTAL et le montant moyen (p. 41).",
             compris="Le Total est la somme des tranches ; l'allocation moyenne, le montant "
                     "divisé par le nombre d'allocataires.",
             question="Tu valides : la moyenne se fait par allocataire, pas par enfant ?",
             base=["Les termes 3 et 7.",
                   "2024 : environ 78 400 FCFA par allocataire et par an, 28 300 par enfant."],
             sql=sql(t41, "8")),
    ],
)

T42 = dict(
    code="T42 à T49",
    titre="Nouveaux pensionnés et nouveaux crédirentiers de l'exercice",
    pages="Annuaire CIPRES : description p. 15 et p. 16, maquettes p. 42 à p. 45, glossaire "
          "p. 66",
    annee="2024",
    resume=("Pour moi, ces huit tableaux comptent les personnes qui entrent dans l'année : les "
            "nouveaux retraités, les nouvelles veuves, les nouveaux orphelins, les nouveaux "
            "rentiers des AT/MP, par âge et par sexe, avec ce qu'on leur a payé. T42 ajoute les "
            "services passés, T48 le degré moyen d'incapacité. La requête est donnée pour T42."),
    termes=[
        dict(terme="Les tableaux de la famille",
             annuaire="T42 nouveaux pensionnés de vieillesse, T43 veufs et veuves, T44 "
                      "orphelins ; T45 nouveaux crédirentiers des AT/MP, T46 orphelins, T47 "
                      "ascendants, T48 rente viagère, T49 veufs et veuves (p. 15 et p. 16).",
             compris="Chaque tableau a ses codes et sa grille d'âge. Je lis T45 comme le total "
                     "de toutes les nouvelles rentes AT/MP ; l'atelier de Kaya range T45 à T49 "
                     "avec les rentes.",
             question="Tu valides : T45 est le total des nouvelles rentes AT/MP (viagères et "
                      "ayants droit) ?",
             base=["T42 RPV et RPRA (moins de 50 à 100 et plus) ; T43 RPSV et RPSF (moins de 20 "
                   "à 60 et plus) ; T44 RPSE (moins de 5 à 20 et plus).",
                   "T45 ARI, ARSV, ARSF, ARSE, ARSA (moins de 15 à 100 et plus) ; T46 ARSE "
                   "(moins de 5 à 20 et plus) ; T47 ARSA (moins de 50 à 100 et plus) ; T48 ARI "
                   "et T49 ARSV, ARSF (moins de 20 à 65 et plus)."],
             sql="""-- les prestations saisies en 2024, par
-- code
SELECT TPE_CODE, COUNT(*)
FROM   USER_DWH.PRESTATION_ESP
WHERE  TPE_CODE IN ('RPV', 'RPRA', 'RPSV',
         'RPSF', 'RPSE', 'ARI', 'ARSV',
         'ARSF', 'ARSE', 'ARSA')
AND    PE_DATE_INSERT >= DATE '2024-01-01'
AND    PE_DATE_INSERT <  DATE '2025-01-01'
GROUP  BY TPE_CODE"""),
        dict(terme="Nouveau pensionné",
             annuaire="Les nouveaux pensionnés « de l'exercice » (p. 15), sans autre précision.",
             compris="La personne dont la première prestation du tableau est saisie dans "
                     "l'année. C'est ce qui retombe sur l'annuaire de la CNSS à 1 % près ; la "
                     "date de notification donne 4 à 5 % de moins.",
             question="Tu valides : nouveau = première pension saisie dans l'année ?",
             base=["PRESTATION_ESP.PE_DATE_INSERT, la date de saisie : la plus ancienne de la "
                   "personne pour les codes du tableau.",
                   "2024 : 2 463 nouveaux retraités (l'annuaire de la CNSS : 2 489) ; veuves "
                   "+0,7 %, orphelins +0,5 %."],
             sql=sql(t42, "2")),
        dict(terme="La personne comptée",
             annuaire="Le tableau compte des personnes (p. 42).",
             compris="Une seule fois, l'année de son entrée, même si elle meurt ou que sa "
                     "pension s'arrête avant le 31 décembre. Une révision ou une reliquidation "
                     "n'en fait pas un nouveau pensionné.",
             question="Tu valides : une révision n'est pas une entrée ?",
             base=["Une révision : une nouvelle pension pour une personne qui en avait déjà une "
                   "(18 à 47 par an jusqu'en 2024, 220 en 2025)."],
             sql=sql(t42, "3")),
        dict(terme="L'âge",
             annuaire="Année de constatation moins année de naissance (glossaire, p. 62).",
             compris="L'année de l'exercice moins l'année de naissance. Par tranche, l'âge "
                     "retombe sur l'annuaire de la CNSS ; âge par âge, il se décale : la CNSS "
                     "le calcule peut-être à la date d'effet.",
             question="À quelle date la CNSS calcule-t-elle l'âge d'un nouveau retraité ?",
             base=["INDIVIDU.IND_DATE_NAISSANCE."],
             sql=sql(t42, "4")),
        dict(terme="Tranches d'âge",
             annuaire="Des tranches de 5 ans, propres à chaque tableau (p. 42 à p. 45).",
             compris="T42 : moins de 50 ans, 50-54… 95-99, 100 ans et plus ; les autres "
                     "suivent leur grille (terme 1).",
             question="Tu valides les grilles de l'Annuaire telles quelles ?",
             base=["L'âge du terme 4.",
                   "2024 : 55-59 ans 1 450 (1 429 publiés), 60-64 ans 941 (956)."],
             sql=sql(t42, "5")),
        dict(terme="Le sexe",
             annuaire="Hommes et femmes, en colonnes (p. 42).",
             compris="Le sexe de la fiche de la personne. Pour les nouveaux retraités, il ne "
                     "retombe pas sur l'annuaire de la CNSS : 641 femmes dans la base pour 527 "
                     "publiées.",
             question="D'où vient le sexe publié dans l'annuaire de la CNSS ?",
             base=["INDIVIDU.IND_SEXE : 1 homme, 2 femme."],
             sql=sql(t42, "6")),
        dict(terme="Le montant",
             annuaire="Le montant des nouvelles pensions (p. 42).",
             compris="Ce qui a été payé pour les mois de l'année. Une nouvelle pension est "
                     "payée avec un rappel des mois d'avant : je le montre à part (8 % du montant "
                     "pour les retraités, 25 % pour les veuves). Une pension de coordination "
                     "payée par l'autre caisse compte sans montant.",
             question="Le montant d'un nouveau pensionné comprend-il le rappel des mois "
                      "d'avant ?",
             base=["DEBOURS : DEB_TYPE 'PM', DEB_STATUT 'D' ou 'P', DEB_DATE_EFFET dans "
                   "l'année ; le rappel : DEB_DATE_EFFET avant l'année.",
                   "2024 : 2,86 milliards."],
             sql=sql(t42, "7")),
        dict(terme="Services passés (T42)",
             annuaire="« Total des périodes d'activités salariées » (glossaire, p. 66). "
                      "L'atelier de Kaya : en mois, et la seule part CNSS pour la coordination.",
             compris="Les mois cotisés de la dernière pension de la personne, de 1 à 600 mois. "
                     "En coordination, j'enlève les mois cotisés dans l'autre caisse.",
             question="Tu valides : PE_COT_AUTRE est le nombre de mois cotisés dans l'autre "
                      "caisse, compris dans le total ?",
             base=["PRESTATION_ESP.PE_MOIS_COTISATION : les mois cotisés.",
                   "PRESTATION_ESP.PE_COT_AUTRE : les mois dans l'autre caisse, rempli pour les "
                   "pensions de coordination.",
                   "2024 : 270 mois en médiane (22 ans et demi) pour les dossiers ordinaires."],
             sql=sql(t42, "8")),
        dict(terme="Degré moyen d'IPP (T48)",
             annuaire="Les nouveaux bénéficiaires de rente viagère « et degré IPP » (p. 45) : le "
                      "taux d'incapacité permanente partielle.",
             compris="La moyenne des taux d'incapacité des nouvelles rentes viagères. En "
                     "dessous de 15 %, ce n'est pas une rente mais une allocation d'incapacité.",
             question="Tu valides : le degré moyen est la moyenne simple des taux des nouvelles "
                      "rentes ?",
             base=["PRESTATION_ESP.PE_TAUX_INCAPACITE, toujours rempli, de 15 à 100.",
                   "2024 : 39 nouvelles rentes viagères (44 publiées), taux médian 24."],
             sql="""-- T48 : le taux moyen des rentes
-- viagères (ARI) des nouveaux de 2024
SELECT COUNT(DISTINCT IND_ID_PREST)
         AS nouveaux,
       ROUND(AVG(PE_TAUX_INCAPACITE), 1)
         AS ipp_moyen
FROM   USER_DWH.PRESTATION_ESP
WHERE  TPE_CODE = 'ARI'
AND    IND_ID_PREST IN (
         SELECT IND_ID_PREST
         FROM   USER_DWH.PRESTATION_ESP
         WHERE  TPE_CODE = 'ARI'
         GROUP  BY IND_ID_PREST
         HAVING MIN(PE_DATE_INSERT)
                  >= DATE '2024-01-01'
         AND    MIN(PE_DATE_INSERT)
                  <  DATE '2025-01-01')
-- une rente révisée compte deux fois
-- dans la moyenne"""),
    ],
)

T50 = dict(
    code="T50 à T57",
    titre="Évolution sur 5 ans des bénéficiaires, prestations servies et décédés par branche",
    pages="Annuaire CIPRES : description p. 16, maquettes p. 46 à p. 48, glossaire p. 63",
    annee="2020 à 2024",
    resume=("Pour moi, T50 à T55 suivent sur cinq ans le nombre de bénéficiaires de six "
            "prestations et ce qu'on leur a payé ; T56 récapitule tout ce qui a été servi par "
            "branche, T57 les décès par branche."),
    termes=[
        dict(terme="Les tableaux de la famille",
             annuaire="T50 allocations de vieillesse, T51 allocations de survivants, T52 IJ des "
                      "AT/MP, T53 allocations d'incapacité, T54 indemnités de maternité, T55 "
                      "allocations prénatales ; T56 prestations servies, T57 décédés (p. 16).",
             compris="Chaque tableau a ses codes. L'allocation de survivants se liquide par ses "
                     "parts (conjoint, orphelin, ascendant). Les prénatales sont surtout celles "
                     "de la maternité.",
             question="Tu valides ce rattachement des codes ?",
             base=["T50 RAV ; T51 RASC, RASO, RASA (RAS ne sert plus) ; T52 AIJ ; T53 AAI ; "
                   "T54 MIJ ; T55 MAP et FAP.",
                   "2024 : allocation de survivants conjoint 81, orphelin 472, ascendant 18 ; "
                   "prénatales MAP 3 857, FAP 56."],
             sql=sql(t50, "1")),
        dict(terme="Bénéficiaire de l'année",
             annuaire="« Évolution du nombre des bénéficiaires » (p. 16) ; l'atelier de Kaya : "
                      "pour T54 et T55, on compte les liquidées, pas les payées.",
             compris="La personne dont la prestation est liquidée dans l'année : notifiée pour "
                     "la première fois, comme pour T25. Je l'applique à T50 à T55. Les payées "
                     "s'en écartent de 25 %.",
             question="Tu valides : un bénéficiaire de l'année = une prestation liquidée dans "
                      "l'année, pour T50 à T55 ?",
             base=["PRESTATION_ESP.PE_DATE_NOTIFICATION dans l'année.",
                   "2024 : allocations de vieillesse 831 (833 publiées) ; IJ de maternité 3 890 "
                   "(3 893), à moins de 1 % de 2020 à 2024 sauf 2022."],
             sql=sql(t50, "2")),
        dict(terme="La personne comptée",
             annuaire="Effectif par année et par sexe ; T54 et T55 : les femmes (p. 46 et "
                      "p. 47).",
             compris="Une fois par tableau et par année, avec son sexe. Une IJ des AT/MP se "
                     "liquide en plusieurs fois (371 IJ pour 195 victimes en 2024) : la victime "
                     "compte une fois.",
             question="Tu valides : on compte des personnes, pas des prestations ?",
             base=["PRESTATION_ESP.IND_ID_PREST ; INDIVIDU.IND_SEXE."],
             sql=sql(t50, "3")),
        dict(terme="Le montant de l'année",
             annuaire="Le montant par année (p. 46).",
             compris="Ce qui a été payé dans l'année pour la prestation. La date qui retombe le "
                     "mieux sur l'annuaire de la CNSS est celle du versement, rappels compris : "
                     "497,8 millions d'allocations de vieillesse versées en 2024 pour 497,7 "
                     "publiés, contre +3,9 % au mois payé. Le choix reste ouvert.",
             question="Le montant d'une année : les paiements versés dans l'année, ou les mois "
                      "de l'année payés ?",
             base=["DEBOURS : DEB_TYPE 'PM', DEB_STATUT 'D' ou 'P'.",
                   "Versé dans l'année : DEB_DATE_INSERT ; mois payé : DEB_DATE_EFFET."],
             sql=sql(t50, "4")),
        dict(terme="Nombre moyen de services passés (T50)",
             annuaire="T50 donne aussi le nombre moyen de services passés (p. 46) ; services "
                      "passés : total des périodes d'activité salariée (glossaire, p. 66).",
             compris="La moyenne des mois cotisés à la CNSS de l'allocation liquidée, par sexe ; "
                     "en coordination, moins les mois dans l'autre caisse.",
             question="Tu valides : en mois, et la seule part CNSS ?",
             base=["PRESTATION_ESP.PE_MOIS_COTISATION et PE_COT_AUTRE.",
                   "2024 : 96 mois en moyenne."],
             sql=sql(t50, "5")),
        dict(terme="Prestations servies par branche (T56)",
             annuaire="« Récapitule toutes les prestations servies par branche » : PF, AT/MP, "
                      "AVS, autres (p. 16).",
             compris="Les paiements et les remboursements de l'année, par branche. Pas les "
                     "ajustements, qui sont des corrections. Les soins des AT/MP ne sont pas "
                     "tous dans les remboursements : l'AT/MP reste 29 % sous le publié.",
             question="Tu valides : sans les ajustements ? Et les rachats de rente des AT/MP "
                      "(21,5 millions en 2024), on les ajoute ?",
             base=["DEBOURS : DEB_TYPE 'PM' paiement et 'RB' remboursement ; CODE_BRANCHE V "
                   "AVS, A AT/MP, F et M PF.",
                   "2024, au mois payé : AVS 47,24 milliards (46,63 publiés), PF 10,75 (11,18), "
                   "AT/MP 0,76 (1,07)."],
             sql=sql(t50, "6")),
        dict(terme="Décédés par branche (T57)",
             annuaire="Décédé : « personne dont la mort a été dûment constatée par un acte » "
                      "d'état civil (glossaire, p. 63) ; T57 récapitule les décédés par branche "
                      "(p. 16).",
             compris="AVS : les titulaires d'une pension directe morts dans l'année. AT/MP : les "
                     "rentiers, et les victimes mortes dans l'année de leur accident. PF : les "
                     "allocataires. À la date du décès : la base n'a pas de date de déclaration.",
             question="T57 compte-t-il les bénéficiaires décédés, ou les assurés dont le décès "
                      "ouvre des droits aux survivants ?",
             base=["INDIVIDU.IND_DATE_DECES ; PRESTATION_ESP pour la pension en cours ; "
                   "DOSSIER.DOS_DATE_ACCIDENT pour les sinistrés.",
                   "2024 : 619 retraités morts dans la base d'aujourd'hui, 444 connus fin mars "
                   "2025, 447 publiés."],
             sql=sql(t50, "7")),
        dict(terme="Les 5 années et la date du calcul",
             annuaire="« Au cours des 5 dernières années » : N-4 à N (p. 46 à p. 48).",
             compris="Les années N-4 à N, chacune avec les mêmes règles. La CNSS arrête son "
                     "annuaire fin mars de l'année suivante : je calcule l'année N sur la copie "
                     "de la base de fin mars N+1, sinon les décès et radiations saisis après "
                     "font bouger les chiffres.",
             question="Tu valides : l'année N se calcule sur la base de fin mars N+1 ?",
             base=["La requête complète lit la copie de fin mars N+1 dans l'entrepôt ; sur la "
                   "base d'aujourd'hui, les années passées ont bougé."],
             sql=sql(t50, "8")),
    ],
)

TABLEAUX = [T25, T26, T27, T28, T31, T32, T41, T42, T50]
