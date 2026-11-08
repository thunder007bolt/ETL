# L'effectif du personnel (T58, T59, T61, T65) : règles retenues

État au 05/11/2026, après la réunion du 03/11/2026 avec la CNSS (termes 1 à 9 de la fiche de lecture GRH) et le lot 24 (les décisions face aux situations).

La requête qui applique ces règles : [t58_t65_effectif_requete_proposee.sql](t58_t65_effectif_requete_proposee.sql). Chaque choix qui peut changer est un **réglage** en tête de la requête (partie 0). Changer un réglage ne demande pas de réécrire la requête.

**La structure ne change pas.** La requête garde la structure de la table actuelle DTM_GRH_EFFECTIF (requête du DTM [grh_effectif.sql](../../domaines/grh/dtm/sql/grh_effectif.sql)) : les mêmes colonnes, le même grain, le même paramètre (le cliché). Ce sont seulement les règles de calcul qui changent. Cinq colonnes s'ajoutent à la fin, et aucune n'est retirée. Les tableaux déjà construits dans OBIEE continuent de marcher. La mise en page des tableaux CIPRES (catégories et sexe en colonnes) se fait dans OBIEE, pas dans la table.

| Colonne ajoutée | Contenu | Pour |
|---|---|---|
| CATEGORIE | catégorie de la grille de salaire (01 à 06, HC) | G3 |
| GROUPE_CATEGORIE | employés, maîtrise, cadres moyens, cadres supérieurs, sans catégorie | G3 : colonnes de T58, T59, T61 |
| UA_CODE | code de l'unité administrative | la déclinaison par direction, agence et direction régionale |
| DOMAINE_ACTIVITE | domaine de l'unité, d'après la CNSS | G12 : lignes de T59 |
| NB_CADRES | nombre d'agents des catégories cadres | G6 : T65 = NB_CADRES ÷ NB_AGENTS |

La colonne CODE_DOMAINE garde son contenu d'avant (le code de la situation), pour ne rien casser. T59 se fait désormais avec DOMAINE_ACTIVITE.

**Ordre des sources.** Une décision de réunion avec la CNSS passe avant tout. Ensuite viennent le recueil des besoins de la CNSS (v7, ateliers de janvier 2026), l'atelier de Kaya (2017), puis l'Annuaire CIPRES. L'annuaire CNSS 2024 sert seulement à contrôler les chiffres.

---

## 1. Les règles

### G1. La nature de l'agent (terme 1)

Chaque agent est rangé selon la nature de sa **dernière situation** commencée au plus tard à la date d'arrêté (GRH_SITUATION.NATURE_EMPLOIE) : **Permanent** ou **Temporaire** (colonne NATURE_EMPLOI, déjà dans la table).

Dans OBIEE, la nature est un **filtre global** : on affiche le tableau des permanents, ou celui des temporaires. Le filtre s'applique à T58, T59, T61 et T65.

Les tableaux CIPRES ne comptent que les agents de la CNSS (qualité A). Comme avant, la table garde aussi les agents de la fonction publique (B) et les agents affectés (C), dans la colonne QUALITE_EMPLOYE. Les tableaux CIPRES se lisent donc avec le filtre QUALITE_EMPLOYE = A.

Un agent qui n'a aucune situation suit la règle des permanents (G2) et reste compté avec des axes vides, comme avant.

*Source : réunion du 03/11/2026, à la suite de l'atelier de Kaya (tableaux scindés selon la nature) ; recueil des besoins v7, RH ligne 1 (« par type employé : permanent, temporaire »).*

### G2. Le permanent présent à la date d'arrêté (terme 2)

La date d'arrêté est le 31 décembre pour l'annuaire, ou toute autre date (fin de mois, fin de trimestre). Un permanent est compté quand **les deux conditions** sont remplies.

1. **Il est entré au plus tard à la date d'arrêté.** La date d'entrée est la date d'embauche (GRH_PERSONNE.DATE_EMBAUCHE). Si elle est vide, c'est la date de prise de service.
2. **Il n'est pas encore parti.** C'est le cas si sa fiche n'est pas inactive (GRH_PERSONNE.ETAT_EMPLOYE différent de INACTIF), ou si sa première décision de départ prend effet **après** la date d'arrêté.

Une fiche **sans état** compte comme encore là. Ce sont des embauches récentes, dont la fiche n'a pas encore été passée à ACTIF : 67 fiches, jusqu'au 28/01/2026 (lot 2, Q10). Avant le 05/11/2026, la requête demandait une fiche ACTIF, et ces agents n'étaient comptés nulle part, ce qui risquait de sous-compter le 31/12/2025.

Les décisions de départ (GRH_DECISION.CDEC_CODE, liste réglable `decisions_depart`) :

| Code | Départ |
|---|---|
| 02 | admission à la retraite |
| 34 | liquidation des droits après décès |
| 35 | fin de contrat |
| 36 | démission |
| 52 | rupture du contrat |
| 54 | licenciement |

Une décision annulée par une décision **40** ne compte pas. Sans décision de départ, c'est la date de décès qui compte, si elle est saisie.

*Exemple : retraite au 15/12/2024, l'agent n'est pas compté au 31/12/2024. Retraite au 15/01/2025, il est compté.*

*Source : réunion du 03/11/2026 (« d'accord ») ; fiche sans état : proposition BI du 05/11/2026.*

### G7. Le temporaire présent à la date d'arrêté (terme 6)

Un temporaire est compté quand la date d'arrêté tombe **entre le début et la fin d'un de ses contrats**.

- **Début d'un contrat :** la date de l'engagement temporaire (décision **50**), sauf s'il est annulé par une décision 40.
- **Fin d'un contrat :**
  1. la **date de départ**, si elle est saisie : la première décision de départ après le début du contrat ;
  2. sinon, **la veille du début du contrat suivant** ;
  3. sinon, **aucune fin connue** : l'agent est **considéré encore sous contrat**, donc compté.

*Exemple : contrat commencé le 01/03/2024, nouveau contrat le 01/09/2024, aucun départ saisi entre les deux. Le premier contrat finit le 31/08/2024. Si le second n'a ni départ ni contrat suivant, l'agent est compté à toute date après le 01/09/2024.*

*Source : réunion du 03/11/2026. La BI ne fixe pas de durée de contrat à la place de la CNSS : l'ancien réglage de 18 mois est supprimé.*

### G3. La catégorie professionnelle (terme 3)

La catégorie est celle de la dernière situation (GRH_SITUATION.CATEGORIE), c'est-à-dire la catégorie de la grille de salaire. Elle est regroupée par la table réglable `regroupement_categorie` :

| Code | Pour la CNSS | Pour la CIPRES |
|---|---|---|
| 01, 02 | Agents d'exécution | Employés |
| 03 | Agents de maîtrise | Agents de maîtrise |
| 04, 05 | Cadres moyens | Cadres |
| 06 | Cadres supérieurs | Cadres |
| HC | Cadres supérieurs *(proposition BI)* | Cadres *(proposition BI)* |

La requête donne les colonnes de la CNSS (employés, maîtrise, cadres moyens, cadres supérieurs). Les cadres de la CIPRES sont la colonne CADRES (moyens + supérieurs).

*Source : recueil des besoins v7, RH ligne 1. Preuve : chaque case catégorie et sexe retombe à 4 agents près sur l'annuaire CNSS 2024 (lot 19, Q111).*

### G4. L'âge (terme 4)

L'âge est le nombre d'**années pleines** à la date d'arrêté. Les tranches sont celles de l'Annuaire CIPRES (table DIM_GRH_TRANCHE_AGE) : moins de 20, 20-24… 55-59, 60 et plus. Avec une date de naissance absente, ou un âge hors de 0 à 119 ans, l'agent est compté avec une tranche vide. Avant, il n'était pas compté du tout.

*Source : réunion du 03/11/2026 (« d'accord »).*

### Le sexe (terme 5)

Le sexe est lu sur la fiche de l'agent : M hommes, F femmes. Un autre code ne compte que dans le total.

*Source : réunion du 03/11/2026 (« d'accord »).*

### G5. L'ancienneté (terme 7, T61)

L'ancienneté est le **temps passé en activité à la CNSS**, en années pleines, entre la date de début et la date d'arrêté, **moins les périodes où l'agent était parti**.

- **La date de début** est la date de **prise de service**. Si elle est vide, c'est la date d'**embauche** (réglage `ordre_date_anciennete`).
- **Les périodes retirées** (réglage `retirer_interruptions` = OUI) :
  - mise en disponibilité (décision 07), jusqu'à la remise en activité (décision 18), ou jusqu'à la date d'arrêté si l'agent est encore en disponibilité ;
  - départ suivi d'un nouvel engagement (décision 50 ou 51), du départ jusqu'au nouvel engagement.
- **Les tranches de l'Annuaire CIPRES :** moins d'1 an, 1-4, 5-9… 35-39, 40 et plus.

*Exemple : prise de service le 01/01/2008, disponibilité du 01/01/2015 au 01/01/2018. Au 31/12/2024, l'agent a 13 ans d'ancienneté (17 ans moins 3) : tranche 10-14.*

*Sources : réunion du 03/11/2026 pour l'ordre des dates ; recueil des besoins v7, RH ligne 4, pour les interruptions (« prendre en compte les sorties et retours »).*

### G12. Le domaine d'activité (terme 8, T59)

Le domaine d'un agent est celui de **son unité administrative** à la date d'arrêté (GRH_SITUATION.UA_CODE, dernière situation). On le lit dans la **correspondance unité → domaine tenue par la CNSS** : le fichier [correspondance_unite_domaine.xlsx](correspondance_unite_domaine.xlsx), chargé dans l'entrepôt.

Il y a 5 domaines : recouvrement ; prestations ; prévention des risques professionnels ; action sanitaire et sociale ; gestion administrative et financière.

Une unité absente du fichier, ou sans domaine retenu, donne « **Domaine non renseigné** ». La BI ne choisit pas de domaine à la place de la CNSS.

Le code de domaine de la situation (CODE_DOMAINE_ACT) n'est **pas** utilisé : il n'est pas fiable. Dans 66 unités sur 139, les agents d'une même unité ont des codes différents (lot 23, Q129).

*Sources : réunion du 03/11/2026 ; recueil des besoins v7, RH ligne 2, pour les 5 domaines.*

### G6. Le taux d'encadrement (terme 9, T65)

Le taux d'encadrement est le nombre de **cadres** (catégories 04, 05, 06 et HC) divisé par l'effectif, à chaque date d'arrêté, sur les cinq derniers exercices. Dans la table : NB_CADRES ÷ NB_AGENTS. La norme CIPRES est de 10 % au plus.

La table DTM_GRH_RATIO (requête grh_ratio.sql) calcule aujourd'hui le même taux avec la qualification (QUAL_CODE), et non avec la catégorie. Elle sera alignée ou remplacée par la lecture de DTM_GRH_EFFECTIF : c'est à voir avec Jules (tâche 4).

*Source : réunion du 03/11/2026 (« on garde comme ça »).*

### G13. Les agents actifs et inactifs (colonnes NB_AGENTS_ACTIFS et NB_AGENTS_INACTIFS)

Ces deux colonnes existaient déjà dans la table. Les tableaux CIPRES ne les lisent pas (ils lisent NB_AGENTS et NB_CADRES), mais OBIEE peut les afficher.

Un agent compté est **inactif** quand il est **en disponibilité à la date d'arrêté** : sa dernière mise en disponibilité (décision **07**) n'est pas suivie d'une remise en activité (décision **18**) à cette date. Les autres agents comptés sont **actifs**. C'est la même lecture que pour la disponibilité (QG6) et pour l'ancienneté (G5).

**Pourquoi on ne lit plus le statut de la situation.** Avant, un agent était inactif si sa dernière situation avait le statut B (détachement), D (mise à disposition), E (disponibilité), F (suspendu) ou H (sous les drapeaux). Mais ce statut n'est pas daté. Quand un agent s'absente ou part, l'application modifie le statut de la situation en cours au lieu d'ouvrir une nouvelle situation. Le statut dit donc l'état d'aujourd'hui, pas celui de la date d'arrêté. Au 31/12/2024, les 6 permanents en disponibilité selon les décisions avaient le statut A et étaient comptés actifs (lot 24, Q137 à Q139).

**Limite.** Le détachement, la mise à disposition, la suspension et le service militaire n'ont pas de paire de décisions début et fin connue. Ils ne sont plus comptés inactifs. Au 31/12/2024, le statut n'en trouvait qu'un seul (F).

Le réglage `inactifs` = STATUT revient à l'ancienne lecture. Avec `disponibilite` = HORS_EFFECTIF, les agents en disponibilité sortent de l'effectif, et NB_AGENTS_INACTIFS vaut 0.

*Exemple : mise en disponibilité le 01/03/2023 (07), remise en activité le 01/06/2025 (18). L'agent est compté inactif au 31/12/2023 et au 31/12/2024, et actif au 31/12/2025.*

*Source : lot 24 (05/11/2026) ; proposition BI, à faire viser par la DRH avec les autres règles.*

### La déclinaison et la périodicité

- **Déclinaison :** la table reçoit le code de l'unité (colonne ajoutée UA_CODE). OBIEE en tire la direction, l'agence ou le bureau et la direction régionale grâce à la dimension des unités administratives (recueil des besoins v7, RH ligne 1 : « Direction, Agence »).
- **Périodicité :** comme avant, la table donne l'effectif au 31/12 de chaque exercice clos (ID_TEMPS = AAAA1231). Le réglage `periodicite` = MENSUELLE ajoute la fin de chaque mois clos de l'année passée et de l'année en cours (ID_TEMPS = AAAAMMJJ), comme le demande le recueil des besoins v7 (mensuelle, trimestrielle, annuelle ; une fin de trimestre est une fin de mois). **Avant de l'activer**, Jules doit régler NB_AGENTS en « dernière valeur » sur le temps dans OBIEE : sinon, une année additionnerait ses douze mois.

---

## 2. Les réglages de la requête

| Réglage | Valeur retenue | Autre valeur possible | D'où vient la valeur |
|---|---|---|---|
| `periodicite` | ANNUELLE (31/12 des exercices clos, comme avant) | MENSUELLE (fins de mois de l'année passée et de l'année en cours) | recueil des besoins v7 ; à activer avec Jules |
| `decisions_depart` | 02, 34, 35, 36, 52, 54 | ajouter ou retirer un code | réunion du 03/11 |
| `codes_decision` | annulation 40 ; engagements 50 et 51 ; disponibilité 07 et remise 18 | autres codes | lots 3, 13, 20 |
| `regroupement_categorie` | 01-02, 03, 04-05, 06-HC ; cadres : 04, 05, 06, HC | déplacer une catégorie (Kaya 2017 mettait la 5e en cadres supérieurs) | recueil des besoins v7 |
| `ordre_date_anciennete` | PRISE_SERVICE | EMBAUCHE | réunion du 03/11 |
| `retirer_interruptions` | OUI | NON | recueil des besoins v7 |
| `fin_contrat_inconnue` | PRESENT | FICHE (compté seulement si la fiche n'est pas inactive) | réunion du 03/11 |
| `disponibilite` | PRESENT (l'agent en disponibilité est compté, inactif) | HORS_EFFECTIF (il sort de l'effectif, comme le tableau 121 de l'annuaire CNSS 2024) | **pas encore tranché** (QG6) ; même valeur dans la requête de T60 |
| `inactifs` | DECISIONS (en disponibilité selon les décisions 07 et 18) | STATUT (statuts B, D, E, F, H de la situation, comme avant) | lot 24 |
| `correspondance_domaine` | *vide : à brancher* | la table chargée depuis le fichier Excel | réunion du 03/11 |

---

## 3. Les points susceptibles de changer

| Point | Ce qui est retenu | Ce qui pourrait changer | Qui décide |
|---|---|---|---|
| Temporaire sans fin de contrat connue | Compté présent | Le compter seulement si sa fiche est active (`fin_contrat_inconnue` = FICHE) | CNSS, au vu du chiffre |
| Agent en disponibilité à la date d'arrêté | Compté dans l'effectif | Le sortir de l'effectif, comme l'annuaire CNSS 2024 | DRH (QG6) |
| Agents inactifs | En disponibilité selon les décisions 07 et 18 | Revenir au statut de la situation (`inactifs` = STATUT) | DRH |
| Fiche sans état (embauche récente) | Comptée comme encore là | Ne la compter qu'une fois passée à ACTIF | DRH |
| Catégorie HC (hors catégorie) | Cadres supérieurs | Autre groupe | DRH |
| Ordre des dates de l'ancienneté | Prise de service, puis embauche | Embauche d'abord | CNSS |
| Interruptions de l'ancienneté | Retirées | Non retirées | CNSS |
| Domaine des agences, bureaux, guichets, directions régionales et DPASS | À décider dans le fichier (132 unités) | Le choix de la CNSS, ligne par ligne | CNSS |
| Liste des décisions de départ | 02, 34, 35, 36, 52, 54 | Un code en plus ou en moins | DRH |
| Passage aux fins de mois | Annuel seulement, comme avant | `periodicite` = MENSUELLE | Jules (réglage OBIEE d'abord) |
| Colonne utilisée pour T59 | DOMAINE_ACTIVITE (ajoutée) ; CODE_DOMAINE garde l'ancien code | Remplir directement CODE_DOMAINE avec le domaine de l'unité | Jules |
| Taux d'encadrement T65 | Lu dans DTM_GRH_EFFECTIF (NB_CADRES) | Garder DTM_GRH_RATIO, aligné sur la catégorie | Jules |

---

## 4. Les points d'attention à expliquer à la CNSS

1. **Les temporaires sans fin de contrat saisie restent comptés.** La base ne garde pas la fin prévue d'un contrat. Si ces agents sont nombreux, le nombre de temporaires dépassera nettement les 46 publiés en 2024. La solution est de saisir les fins de contrat (décision 35) dans l'application GRH.
2. **Les temporaires sans engagement (décision 50) ne sont pas comptés.** On ne connaît pas le début de leur contrat. Le lot 13 en trouvait 234, presque tous anciens.
3. **10 agents actifs sont rattachés à une unité fermée** (lot 23, Q130). Ils sont comptés, avec le domaine de l'ancienne unité. Leur situation est à corriger dans l'application GRH.
4. **Tant que la CNSS n'a pas rendu le fichier unité → domaine, tout T59 est en « Domaine non renseigné ».**
5. **Pour les années 2019 à 2023**, les 84 agents engagés en 2019 n'ont pas de date de départ. La base ne les compte jamais présents, alors que l'annuaire CNSS les compte jusqu'en 2023 (règle G11). Ce point est sans effet à partir de 2025.
6. **Les absences et les retours se lisent dans les décisions, pas dans les situations.** Une mise en disponibilité, une mise à disposition ou une remise en activité ne crée presque jamais de situation, et le statut de la situation est modifié au lieu d'être daté (lot 24). Pour que les agents inactifs et l'ancienneté soient justes, chaque disponibilité et chaque retour doivent avoir leur décision (07 et 18) dans l'application GRH.

---

## 5. Ce qui reste pour finaliser l'effectif

| N° | Tâche | Qui | État |
|---|---|---|---|
| 1 | Exécuter les deux requêtes de contrôle de l'en-tête (cliché 102026), jusqu'au 31/12/2025, et comparer à l'attendu ; puis une fois avec `periodicite` = MENSUELLE pour tester les fins de mois | toi, puis Claude | à faire |
| 2 | Remplir le fichier unité → domaine, d'abord les 132 lignes « À décider » | CNSS | à faire |
| 3 | Créer la table de correspondance (DTM.REF_GRH_UNITE_DOMAINE) et son chargement depuis le fichier Excel dans la chaîne Python et Jenkins ; brancher la partie 0 de la requête | Claude, puis toi pour le déploiement | à faire |
| 4 | Script d'ajout des cinq colonnes (CATEGORIE, GROUPE_CATEGORIE, UA_CODE, DOMAINE_ACTIVITE, NB_CADRES) dans DTM et ODS.DTM_GRH_EFFECTIF, puis remplacement de grh_effectif.sql par cette requête ; décider avec Jules du sort de grh_ratio.sql | Claude, après le visa de la DRH | à faire |
| 5 | Dans OBIEE : le filtre global de nature, le filtre qualité A, la déclinaison par direction, agence et direction régionale, T58 à T61 par GROUPE_CATEGORIE et sexe, le taux = SUM(NB_CADRES) / SUM(NB_AGENTS) | toi | à faire |
| 6 | Trancher la disponibilité (QG6) et la catégorie HC ; viser G13 (inactifs lus dans les décisions) et la fiche sans état (G2) | DRH | à faire |
| 7 | Reporter les décisions dans la fiche de lecture GRH (termes 1 à 9, en gardant tes notes) et dans la fiche de validation T58 | Claude | à faire |
