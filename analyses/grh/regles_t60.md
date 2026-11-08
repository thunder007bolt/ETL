# Le mouvement du personnel (T60) : règles proposées

État au 08/11/2026. Les termes de T60 de la fiche de lecture GRH n'ont pas encore été vus en réunion avec la CNSS. Ces règles sont des propositions, à faire viser par la DRH.

La requête qui applique ces règles : [t60_requete_proposee.sql](t60_requete_proposee.sql). Chaque choix qui peut changer est un **réglage** en tête de la requête, dans la CTE `parametres` (partie 0), comme dans la requête de l'effectif. Changer un réglage ne demande pas de réécrire la requête.

**La structure ne change pas.** La requête garde la structure de la table actuelle DTM_GRH_MOUVEMENT (requête du DTM [grh_mouvement.sql](../../domaines/grh/dtm/sql/grh_mouvement.sql)) : les mêmes colonnes, le même grain, le même paramètre (le cliché). Ce sont seulement les règles de calcul qui changent. Quatre colonnes s'ajoutent à la fin, et aucune n'est retirée. La mise en page de T60 (début, entrées, sorties, fin) se fait dans OBIEE, pas dans la table. Elle remplace la proposition du 01/11/2026, qui rendait directement une ligne par année à la façon de la maquette.

| Colonne ajoutée | Contenu | Pour |
|---|---|---|
| QUALITE_EMPLOYE | A agent CNSS, B fonction publique, C agent affecté | le filtre des tableaux CIPRES (agents A) |
| NATURE_EMPLOI | PERMANENT ou TEMPORAIRE | le filtre global de nature (réunion du 03/11/2026) |
| UA_CODE | code de l'unité administrative | la déclinaison par direction, agence et bureau |
| MOTIF_MVT | ce qui explique le mouvement : DECISION_02, DATE_DECES, EMBAUCHE, ENGAGEMENT_50, ETAIT_TEMPORAIRE… | G15 : le détail des « autres » |

**Pourquoi réécrire.** La requête actuelle lit la table des mouvements de l'application (GRH_MOUVEMENT). Cette table ne contient presque que des mouvements internes : affectations, mutations, nominations. Elle n'a ni recrutement, ni retraite, ni décès (lot 1). La CNSS l'a confirmé à la réunion du 03/11/2026 (M06). Les entrées se lisent dans la fiche de l'agent (la date d'embauche) et dans les engagements. Les sorties se lisent dans les décisions de départ.

---

## Ce que disent les sources

Ordre de priorité : une décision de réunion avec la CNSS, puis le recueil des besoins v7, l'atelier de Kaya (2017), l'Annuaire CIPRES. L'annuaire CNSS 2024 sert seulement à comparer les chiffres.

| Source | Ce qu'elle dit de T60 |
|---|---|
| Recueil des besoins v7, RH ligne 3 | « Autre : il faut le détailler (lister pour la CNSS) ». Source : application GRH et paie. Déclinaison : direction, agence, bureau. Périodicité : annuelle |
| Recueil des besoins v7, RH ligne 1 | Par type d'employé : permanent ou temporaire (pour la CNSS et la CIPRES) |
| Atelier de Kaya (2017) | La note de prise de service vaut en attendant la décision d'engagement ; le communiqué nécrologique vaut en attendant l'acte de décès ; « la DRH devra proposer et décrire les modalités des entrées et des sorties » |
| Annuaire CIPRES, p. 17 et p. 51 | « Permet de suivre les mouvements du personnel en entrée et sortie pendant les 5 dernières années ». Maquette : effectif début ; entrées (recrutement, autres) ; sorties (retraite, décès, autres) ; effectif fin ; années N-4 à N |
| Réunion du 03/11/2026 | GRH_MOUVEMENT est interne à 94 ou 95 % (M06). Restent ouverts : un temporaire est-il un recrutement (M09) ; prise de service ou engagement pour la date d'entrée (M10) ; le détail des autres sorties, et la formation longue (M11) ; la date de décès (M12) |
| Annuaire CNSS 2024, tableau 121 | Le mouvement des permanents de 2015 à 2024. Il détaille déjà les « autres » : remises en activité, détachements ; démissions, licenciements, disponibilités, fins de détachement |

**Les codes de mouvement de la CNSS.** L'application GRH a déjà une table des types de mouvement (GRH_TYPE_MOUVEMENT, copiée dans DTM.DIM_GRH_TYPE_MOUVEMENT). Ses codes du domaine ALL sont les colonnes de la maquette CIPRES. Ses codes du domaine CNSS en sont le détail, avec les mêmes rubriques que le tableau 121 de l'annuaire CNSS 2024. C'est la liste que demande le recueil des besoins : la requête la reprend telle quelle.

| Code | Libellé de la CNSS | Nature | Colonne de T60 | Rempli par |
|---|---|---|---|---|
| DEBUT | Début de l'année | INITIAL | Effectif début | G14 |
| RECRU | Recrutement | ENTREES | Recrutement | G8 |
| REM | Remise en activité | ENTREES | Autres entrées | G8, réglage `disponibilite` = HORS_EFFECTIF |
| DETAC | Détaché | ENTREES | Autres entrées | G8 (décision 37) |
| AUTRE_ENT | Autres entrées | ENTREES | Autres entrées | G8 |
| RET | Retraite | SORTIES | Retraite | G9 (décision 02) |
| DEC | Décès | SORTIES | Décès | G9 (date de décès, sinon décision 34) |
| LIC | Licenciement | SORTIES | Autres sorties | G9 (décision 54) |
| DEM | Démission | SORTIES | Autres sorties | G9 (décision 36) |
| DISP | Disponibilité (sans solde) | SORTIES | Autres sorties | G9, réglage `disponibilite` = HORS_EFFECTIF |
| MISE_DIS | Mise à disposition | SORTIES | Autres sorties | pas rempli (question 6) |
| FIN_DETACH | Fin de détachement | SORTIES | Autres sorties | pas rempli (question 5) |
| FIN_CTRAT | Fin de contrat | SORTIES | Autres sorties | G9 (décisions 35, 52) |
| AUTRE_SORT | Autres sorties | SORTIES | Autres sorties | G9 |
| AFF, MUT, PROMO, RETRO, NOM | Affectation, mutation, promotion, rétrogradation, nomination | INTERNE | aucune | G17 (comme avant) |

---

## 1. Les règles

### G14. Les deux photos de l'exercice

L'effectif **au début** de l'exercice N est celui des agents présents au **31/12 de N-1**. L'effectif **à la fin** est celui des présents au **31/12 de N**. Être présent suit **exactement les règles de T58** : G1 (la nature), G2 (les permanents), G7 (les temporaires), avec les mêmes réglages `disponibilite` et `fin_contrat_inconnue`. Les parties 0 à 7 de la requête reprennent celles de la requête de l'effectif. Une modification de ces règles se fait donc dans les deux requêtes.

À chaque date, un agent appartient à la population de sa nature : les permanents, les temporaires, ou les agents sans situation. Le filtre de nature d'OBIEE donne ainsi un tableau des permanents et un tableau des temporaires, chacun juste.

Conséquence : **début + entrées − sorties = fin**, toujours, et la fin est l'effectif de T58 au 31/12.

*Exemple : un agent présent au 31/12/2023 et au 31/12/2024 ne fait aucun mouvement en 2024, même s'il a changé d'unité, de qualification ou de contrat entre les deux.*

*Source : Annuaire CIPRES, p. 51 (effectif début et fin) ; règles de T58 vues en réunion le 03/11/2026.*

### G8. Les entrées

Un agent **entre** dans l'exercice quand il est présent à la fin dans une nature, et absent de cette nature au début. Le code de l'entrée se lit dans cet ordre.

Pour un permanent (et un agent sans situation) :

1. **DETAC** s'il a une décision **37** (classement d'un agent détaché) dans l'exercice ;
2. **RECRU** s'il est **entré dans l'exercice** : date d'embauche, à défaut date de prise de service (motif EMBAUCHE) ;
3. **REM** s'il était **en disponibilité au début** (décision 07 sans 18 après). Seulement avec le réglage `disponibilite` = HORS_EFFECTIF : sinon un agent en disponibilité ne quitte jamais l'effectif.

Pour un temporaire :

1. **RECRU** s'il n'était **sous aucun contrat au début** : c'est un nouvel engagement 50 (motif ENGAGEMENT_50).

Puis, pour tous :

- **AUTRE_ENT** s'il était présent au début dans **une autre nature**. Le motif dit laquelle : ETAIT_TEMPORAIRE (un temporaire devenu permanent), ETAIT_PERMANENT, ETAIT_SANS_SITUATION (un agent embauché en fin d'année, dont la première situation commence l'année suivante) ;
- sinon **AUTRE_ENT** avec le motif SANS_MOTIF.

*Exemple : temporaire engagé le 01/06/2023, devenu permanent le 01/07/2024. Dans le tableau des temporaires, il entre en 2023 (RECRU) et sort en 2024 (AUTRE_SORT, DEVIENT_PERMANENT). Dans celui des permanents, il entre en 2024 (AUTRE_ENT, ETAIT_TEMPORAIRE). Il n'est compté qu'une fois en recrutement.*

*Sources : Annuaire CIPRES, p. 51 ; atelier de Kaya pour la prise de service ; recrutements de 2024 retrouvés à l'unité près (104, lot 17, Q99).*

### G9. Les sorties

Un agent **sort** dans l'exercice quand il est présent au début dans une nature, et absent de cette nature à la fin. Le code de la sortie se lit dans cet ordre.

1. **Le départ qui l'a fait sortir**, daté dans l'exercice. Pour un permanent, c'est son **premier départ**, comme dans G2. Pour un temporaire, c'est le **dernier départ de l'exercice**, celui qui a fini son dernier contrat. Le code de la décision donne le type (réglage `decisions_depart`) :

   | Décision | Type |
   |---|---|
   | 02 admission à la retraite | RET |
   | 34 liquidation des droits après décès | DEC |
   | 35 fin de contrat | FIN_CTRAT |
   | 36 démission | DEM |
   | 52 rupture du contrat | FIN_CTRAT *(proposition BI : un seul cas dans la base)* |
   | 54 licenciement | LIC |
   | date de décès de la fiche | DEC |

2. **DISP** s'il est **en disponibilité à la fin**. Seulement avec le réglage `disponibilite` = HORS_EFFECTIF.
3. **AUTRE_SORT** s'il est présent à la fin dans **une autre nature** (motif DEVIENT_PERMANENT ou DEVIENT_TEMPORAIRE) ;
4. sinon **AUTRE_SORT** avec le motif SANS_MOTIF : un agent sorti sans départ daté.

*Exemple : retraite le 01/02/2024, puis décès et liquidation des droits le 01/11/2024. L'agent sort en 2024 en retraite : c'est son premier départ.*

*Sources : décisions de retraite (02) à 4 près par an, démissions (36) exactes, licenciements (54) à 1 près face à l'annuaire CNSS 2024 (lots 17 et 18, Q99, Q109). Décès : la date de décès n'est plus saisie depuis 2010 ; la liquidation des droits (34) vient environ 8 mois et demi après le décès (lot 7, Q42).*

### G15. Le détail des « autres »

Le recueil des besoins demande de détailler les « autres » et d'en donner la liste à la CNSS. La requête range chaque entrée et chaque sortie sous un code de la CNSS (tableau plus haut). La colonne MOTIF_MVT précise ce que le code ne dit pas : la décision qui a servi, ou le changement de nature.

Dans OBIEE, les colonnes « autres » de la maquette se détaillent par CODE_TYPE_MVT, puis par MOTIF_MVT.

*Source : recueil des besoins v7, RH ligne 3.*

### G16. Entré et sorti dans le même exercice (réglage `passages_annee`)

Un agent embauché en février et parti en octobre n'est présent à aucune des deux photos.

- **NON** (valeur retenue) : il n'est pas compté. C'est la proposition de la fiche T60 (question Q4).
- **OUI** : il est compté une fois en entrée (RECRU) et une fois en sortie (le code de son départ), pour un permanent comme pour un temporaire dont le contrat commence et finit dans l'exercice.

Dans les deux cas, début + entrées − sorties = fin reste vrai.

*Source : proposition BI.*

### G17. Les mouvements internes

Les mouvements internes (affectations, mutations, nominations) restent repris de GRH_MOUVEMENT **comme avant** : lignes validées (VALIDE = O), sur l'année de leur date de début. Ils n'entrent pas dans T60. Les codes d'entrée et de sortie de cette table (DIS, DET, REM) ne sont plus lus : les entrées et les sorties viennent de G8 et G9. Le réglage `mouvements_internes` = AUCUN les retire.

*Source : comme avant ; lot 1.*

---

## 2. T60 dans OBIEE

Filtres : QUALITE_EMPLOYE = A ; NATURE_EMPLOI en filtre global (permanents ou temporaires) ; les 5 derniers exercices (ID_TEMPS).

| Colonne de la maquette | Calcul sur NB_AGENTS |
|---|---|
| Effectif début | lignes DEBUT |
| Recrutement | RECRU |
| Autres entrées | nature ENTREES, sauf RECRU ; détail par CODE_TYPE_MVT et MOTIF_MVT |
| Retraite | RET |
| Décès | DEC |
| Autres sorties | nature SORTIES, sauf RET et DEC ; détail par CODE_TYPE_MVT et MOTIF_MVT |
| Effectif fin | début + entrées − sorties (= l'effectif de T58 au 31/12) |

**Par direction, agence ou bureau.** Un agent qui change d'unité dans l'année ne fait ni entrée ni sortie. L'égalité début + entrées − sorties = fin tient pour toute la CNSS, par nature et par sexe, mais pas pour une unité. Pour une unité, l'effectif de fin se lit dans DTM_GRH_EFFECTIF (colonne UA_CODE). L'écart entre cette fin et début + entrées − sorties donne les mutations nettes de l'unité.

---

## 3. Les réglages de la requête

Ils s'écrivent en tête de la requête, dans la CTE `parametres` (partie 0), comme dans la requête de l'effectif. Changer une valeur ne demande pas de réécrire le reste.

**La disponibilité** (réglage `disponibilite`, question QG6) :

| Valeur | Ce que fait la requête |
|---|---|
| **PRESENT** (valeur actuelle) | un permanent en disponibilité (décision 07, pas encore de retour 18) reste dans l'effectif : ni sortie, ni retour |
| **HORS_EFFECTIF** | il sort de l'effectif : une sortie DISP à la mise en disponibilité, une entrée REM au retour, comme le tableau 121 de l'annuaire CNSS 2024 |

La requête de l'effectif (T58) a le même réglage `disponibilite`, dans sa propre CTE `parametres`. Il faut lui donner **la même valeur dans les deux requêtes**, sinon la fin de T60 ne retombe plus sur l'effectif de T58. La DRH n'a pas encore tranché (QG6).

*Exemple : mise en disponibilité le 01/03/2023, remise en activité le 01/06/2025. Avec PRESENT, l'agent est compté au début et à la fin de chaque année, sans mouvement. Avec HORS_EFFECTIF, il sort en 2023 (DISP), n'est pas compté au 31/12/2023 ni au 31/12/2024, et rentre en 2025 (REM).*

**Les autres réglages :**

| Réglage | Valeur retenue | Autre valeur possible | D'où vient la valeur |
|---|---|---|---|
| `fin_contrat_inconnue` | PRESENT | FICHE | même valeur que l'effectif ; réunion du 03/11 |
| `passages_annee` | NON | OUI | proposition de la fiche T60 (Q4) |
| `mouvements_internes` | COMME_AVANT | AUCUN | comme avant |
| `decisions_depart` | 02 RET, 34 DEC, 35 FIN_CTRAT, 36 DEM, 52 FIN_CTRAT, 54 LIC | un code en plus ou en moins, un autre type | mêmes codes que l'effectif |
| `codes_decision` | annulation 40 ; engagements 50 et 51 ; disponibilité 07 et remise 18 ; détaché 37 | autres codes | lots 3, 13, 20 |

---

## 4. Les points susceptibles de changer

| Point | Ce qui est retenu | Ce qui pourrait changer | Qui décide |
|---|---|---|---|
| Disponibilité (réglage `disponibilite`) | PRESENT : l'agent reste dans l'effectif, ni sortie ni retour | HORS_EFFECTIF : sortie DISP et retour REM, dans T58 et T60 à la fois | DRH (QG6) |
| Date d'un décès | Date de décès, sinon liquidation des droits (34) | Une date saisie, ou celle du communiqué nécrologique | DRH (QG3, M12) |
| Date d'entrée | Embauche, sinon prise de service (comme T58) | Prise de service d'abord, comme l'ancienneté | CNSS (M10) |
| Entré et sorti la même année | Pas compté | Compté en entrée et en sortie | DRH (Q4) |
| Rupture du contrat (52) | Fin de contrat | Licenciement, ou autre sortie | DRH |
| Mise à disposition (14) | Pas une sortie | Une sortie MISE_DIS, avec un retour | DRH |
| Détachements | Décision 37 (aucune chez les permanents) | La source que donnera la DRH | DRH (QG7) |
| Mutations entre unités | Pas de mouvement ; fin d'une unité lue dans T58 | Des lignes d'arrivée et de départ par unité | CNSS |
| Mouvements internes | Repris de GRH_MOUVEMENT comme avant | Retirés (`mouvements_internes` = AUCUN) | Jules |

---

## 5. Les chiffres attendus, face à l'annuaire CNSS 2024

Permanents (agents A), réglages par défaut (disponibilité PRESENT). « Base » : ce que la requête devrait donner, d'après les lots 17, 18 et 25, à quelques agents près. « Annuaire » : tableau 121 de l'annuaire CNSS 2024.

| Année | | Début | Recrutement | Autres entrées | Retraite | Décès | Autres sorties | Fin |
|---|---|---|---|---|---|---|---|---|
| 2020 | Base | 823 | 1 | 0 | 39 | 3 | 0 | 782 |
| | Annuaire | 918 | 0 | 3 | 43 | 4 | 5 | 869 |
| 2021 | Base | 782 | 0 | 0 | 31 | 4 | 1 | 746 |
| | Annuaire | 869 | 0 | 2 | 31 | 2 | 4 | 834 |
| 2022 | Base | 746 | 1 | 0 | 29 | 4 | 2 | 712 |
| | Annuaire | 834 | 0 | 5 | 26 | 3 | 4 | 806 |
| 2023 | Base | 712 | 0 | 0 | 30 | 3 | 2 | 677 |
| | Annuaire | 806 | 0 | 7 | 32 | 5 | 9 | 767 |
| 2024 | Base | 677 | 104 | 0 | 25 | 5 | 4 | 747 |
| | Annuaire | 767 | 104 | 7 | 23 | 3 | 94 | 758 |

Le détail de la base : 2021, 1 licenciement ; 2022, 2 licenciements ; 2023, 1 démission et 1 fin de contrat ; 2024, 2 démissions, 1 licenciement et 1 agent sorti sans départ daté.

Le détail de l'annuaire, autres entrées : remises en activité 3, 2, 3, 5, 6 ; détachements 2 en 2023, 1 en 2024 ; 2 autres en 2022. Autres sorties : disponibilités 5, 3, 2, 4, 7 ; démissions 1 en 2023, 2 en 2024 ; licenciements 1 en 2022, 1 en 2024 ; fins de détachement 2 en 2023 ; autres 1, 1, 2 et 84.

Les écarts s'expliquent :

- **Début et fin** : les 84 agents engagés en 2019, sans date de départ, que la base ne compte jamais (G11). L'annuaire les sort tous en 2024 (les 84 « autres »). Sans eux, l'écart tombe à 8 à 16 agents par an.
- **Disponibilités et remises en activité** : la base les a (décisions 07 et 18, à 1 à 3 près par an), mais le réglage PRESENT les garde dans l'effectif. Avec HORS_EFFECTIF, elles apparaissent en DISP et REM.
- **Décès** : lus dans la liquidation des droits, environ 8 mois après le décès. Un décès de fin d'année glisse sur l'année suivante (19 sorties pour 17 décès de 2020 à 2024).
- **Recrutements, retraites, démissions, licenciements** : retrouvés à l'unité ou à quelques unités près.

---

## 6. Les points d'attention à expliquer à la CNSS

1. **Les 84 agents engagés en 2019** n'ont pas de date de départ : la base ne les compte jamais, ni en entrée ni en sortie. L'annuaire CNSS les compte de 2019 à 2023. Ce point est sans effet à partir de 2025.
2. **Aucune date de décès n'est saisie depuis 2010.** Un décès compte à la liquidation des droits, souvent l'année suivante. La solution est de saisir la date du décès dans l'application GRH.
3. **Les disponibilités et les remises en activité** ne sont des mouvements que si la DRH décide qu'un agent en disponibilité sort de l'effectif (QG6) : on passe alors le réglage `disponibilite` à HORS_EFFECTIF dans les deux requêtes. L'annuaire CNSS 2024 les compte, et l'interlocuteur de la CNSS l'a dit aussi à la réunion du 03/11 (M11).
4. **Les détachements et les fins de détachement** publiés par l'annuaire ne se trouvent pas dans la base : aucune décision 37 chez les permanents.
5. **Un agent embauché en fin d'année sans situation au 31/12** est compté dans T58 avec une nature vide, donc dans aucun des deux tableaux. L'année suivante, il apparaît chez les permanents en « autre entrée » (motif ETAIT_SANS_SITUATION), et non en recrutement.
6. **Par unité, la fin ne se calcule pas** : les mutations entre unités ne sont pas des entrées ni des sorties. La fin d'une unité se lit dans T58.
7. **Un temporaire parti puis réengagé dans la même année**, présent aux deux 31/12, ne fait aucun mouvement.
8. **La table des mouvements de l'application** (GRH_MOUVEMENT) ne sert plus aux entrées et aux sorties : elle n'en contient presque pas.

---

## 7. Les questions à la CNSS

Chacune a une valeur par défaut dans la requête ; la réponse change un réglage, pas le travail.

| N° | Question | En attendant |
|---|---|---|
| 1 | La liste des « autres » ci-dessus (remise en activité, détaché, démission, licenciement, disponibilité, mise à disposition, fin de détachement, fin de contrat, autres) vous convient-elle ? | La liste de la CNSS (GRH_TYPE_MOUVEMENT) |
| 2 | Une mise en disponibilité est-elle une sortie, et la remise en activité une entrée ? (QG6) | Non : l'agent reste dans l'effectif (réglage PRESENT) |
| 3 | Peut-on saisir la date du décès, ou celle du communiqué nécrologique ? (QG3, M12) | La liquidation des droits (34) |
| 4 | La date d'entrée est-elle la prise de service, comme pour l'ancienneté ? (M10) | L'embauche, sinon la prise de service |
| 5 | Où sont saisis les détachements et les fins de détachement ? (QG7) | La décision 37 |
| 6 | Une mise à disposition (décision 14) est-elle une sortie ? | Non |
| 7 | Un agent recruté puis parti dans la même année doit-il apparaître en entrée et en sortie ? (Q4) | Non |
| 8 | Pour une direction ou une agence, faut-il montrer les arrivées et les départs par mutation ? | Non : la fin de l'unité vient de T58 |

La question M09 (un temporaire compte-t-il comme recrutement ?) est réglée par le filtre de nature. Le tableau des permanents compte les permanents embauchés, celui des temporaires les nouveaux contrats. Un temporaire devenu permanent est une « autre entrée » des permanents.

---

## 8. Comment la requête a été vérifiée

- **Syntaxe Oracle** vérifiée avec sqlglot. Les identifiants font au plus 30 caractères, sans FETCH FIRST ni alias réservé.
- **Rejeu dans SQLite** sur 22 agents fictifs, un par cas (retraite, décès par la date ou la liquidation, démission, licenciement par décision collective, disponibilité, contrats renouvelés, titularisation, embauche sans situation au 31/12, fiche inactive sans date, agent B, retraite annulée, contrat sans fin, fiche sans état, mutation, détaché, rupture de contrat, entrés et sortis dans l'année…), pour les quatre combinaisons des réglages `disponibilite` et `passages_annee`. Chaque mouvement est celui attendu.
- **Début + entrées − sorties = début de l'exercice suivant**, pour chaque nature, qualité et sexe.
- **L'effectif de début est celui de la requête de l'effectif (T58)** à chaque 31/12 comparé.

Pas encore exécutée sur la base de la CNSS : c'est la tâche 1.

---

## 9. Ce qui reste pour finaliser T60

| N° | Tâche | Qui | État |
|---|---|---|---|
| 1 | Exécuter les deux requêtes de contrôle de l'en-tête (cliché 102026), de 2020 à 2025, avec le réglage `disponibilite` à PRESENT, puis à HORS_EFFECTIF, et comparer au tableau du point 5 | toi, puis Claude | à faire |
| 2 | Vérifier que DTM.DIM_GRH_TYPE_MOUVEMENT contient les 18 codes de la CNSS, plus NOM (`SELECT CODE_TYPE_MVT, CODE_NAT_MVT FROM DTM.DIM_GRH_TYPE_MOUVEMENT ORDER BY 2, 1`) | toi | à faire |
| 3 | Mettre à jour les termes de T60 de la fiche de lecture GRH et la fiche de validation T60 avec ces règles, puis les voir en réunion avec la CNSS | Claude, puis toi avec la CNSS | à faire |
| 4 | Faire trancher les questions du point 7, d'abord la disponibilité (elle change aussi T58) | DRH | à faire |
| 5 | Script d'ajout des quatre colonnes dans DTM et ODS.DTM_GRH_MOUVEMENT, puis remplacement de grh_mouvement.sql par cette requête | Claude, après le visa de la DRH | à faire |
| 6 | Dans OBIEE : la maquette de T60 (point 2), les filtres qualité A et nature, le détail des autres, la fin par unité lue dans T58 | toi, avec Jules | à faire |
