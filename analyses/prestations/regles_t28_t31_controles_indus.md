# T28 à T31 : les contrôles a posteriori et les prestations indues — les règles en clair

## En bref

- **Ce que comptent ces tableaux.** T28 (pensions), T29 (accidents du travail et maladies professionnelles) et T30 (prestations familiales) comptent, par type de prestation, les dossiers payés et les dossiers vérifiés après paiement (les « contrôles a posteriori »), trimestre par trimestre : au moins 5 % des dossiers payés doivent être vérifiés. T31 suit chaque mois, par branche, ce qui est payé, payé en trop (les trop-perçus, ou « indus ») et payé en moins (les moins-perçus) : au plus 2 % d'indus.
- **Ce que le recueil v7 demande de nouveau.** Une ligne « pension de retraite anticipée » dans T28 ; une colonne TOTAL qui additionne les quatre trimestres ; des dossiers payés, pas des personnes ; T28 à T30 au trimestre, au semestre et à l'année, T31 au mois, au trimestre et à l'année ; les taux par direction régionale et par agence. Il donne les fichiers Excel de la DAI (Direction de l'audit interne) comme source de T28 et de T31.
- **Ce que l'annuaire CNSS 2024 confirme.** La maternité est rangée avec les prestations familiales (tableaux 40 et 41) ; la CNSS ne sert aucune prestation de solidarité (tableaux 28 et 34), comme l'avait décidé Kaya 2017. Il donne aussi un indice fort, sans preuve, qu'un exercice se calcule sur la copie de la base arrêtée fin mars de l'année suivante (444 décès de retraités retrouvés pour 447 publiés, tableau 26).
- **Ce qui reste en écart.** L'annuaire ne publie ni indus ni contrôles. Le total des sommes payées en 2024 tombe juste (−0,26 %), mais seulement parce que les branches se compensent : pensions +1,3 %, prestations familiales −3,8 %, accidents du travail −20,3 % (argent versé) et −28,9 % (soins compris). Les rachats de rente sont à −15,2 %. L'annuaire compte les indemnités de maternité (3 893) et les allocations de vieillesse (833) l'année où elles sont accordées (liquidées), pas l'année où elles sont payées.
- **Les chiffres de la base.** Indus 2024 : 0,39 % des sommes payées, sous la norme de 2 % dans toutes les lectures. Contrôles 2025 : 0,07 % (115 contrôles pour 155 206 dossiers payés), loin des 5 %, et plus rien d'enregistré depuis le 09/07/2025.
- **Les décisions attendues de la CNSS.** 30 questions ; aucune ne bloque, chacune a une valeur par défaut. Les plus lourdes : où sont les contrôles, et pouvez-vous nous les donner (**Q1** à **Q3**) ; sur quoi divise-t-on le taux de contrôle (**Q6**) ; T31 prend-il tous les trop-perçus de l'application ou ceux de la DAI (**Q14**) ; sur quoi divise-t-on le taux d'indus (**Q15**) ; les moins-perçus comptent-ils dans ce taux (**Q16**).
- **Ce qui reste à faire.** Obtenir les contrôles de la DAI (sans eux, T28 à T30 restent vides) ; ajouter à la requête T31 un taux par branche, puis par direction régionale et par agence ; tester la date des paiements mois par mois ; un petit lot de requêtes de contrôle sur la base (numérotées P120 et suivantes).

---

**Ce que couvre la famille.** Quatre tableaux de l'Annuaire CIPRES. T28, T29 et T30 répartissent, par type de prestation, les dossiers payés et les dossiers contrôlés a posteriori au 1er, 2e, 3e et 4e trimestre : T28 pour les pensions (vieillesse, invalidité, décès : PVID), T29 pour les accidents du travail et maladies professionnelles (AT/MP), T30 pour les prestations familiales (PF), maternité comprise. Ils servent au ratio de contrôle a posteriori (5 % au moins). T31 donne, mois par mois et pour chaque branche, les prestations payées, les trop-perçus et les moins-perçus, et sert au ratio des prestations indues (2 % au plus). S'y ajoutent les indicateurs du recueil v7 qui en sortent : ratio de contrôle a posteriori et ratio des prestations indues pour chaque branche, taux de prestations indues de la CNSS.

**État au 08/11/2026.** Tout est **proposé**, rien n'est encore validé par la CNSS. Aucune réunion avec la CNSS n'a encore porté sur ces tableaux. T28 à T30 n'ont pas de requête : les contrôles manquent. La requête T31 est proposée, pas exécutée ; ses chiffres de 2024 viennent du lot 9 et sont **vus au 02/02/2026** (date de la copie de la base).

**D'où viennent les règles.** Dans l'ordre de force : une décision de réunion (aucune ici), le recueil des besoins v7, l'atelier de Kaya (2017), l'Annuaire CIPRES. L'annuaire statistique de la CNSS 2024 sert à comparer les chiffres, et tranche la pratique quand les sources plus fortes se taisent. Les modèles de ratios de la CNSS (classeur « Evaluation des Ratios CIPRES 2025 », feuilles 05 et 06) montrent comment la CNSS calcule elle-même ces deux ratios.

**Ce qui existe déjà, laissé tel quel.** Requête [t31_requete_proposee.sql](t31_requete_proposee.sql) ; règles R1, R3, R4, R13, I1 du [README](README.md) ; cartes I1, C1 et A1 du schéma `regles_prestations` ; hypothèses H11 et H12 de [reference_cipres_T25_T57.md](reference_cipres_T25_T57.md) ; fiche de validation T31 (8 règles, 5 questions, `excalidraw/schemas/prestations/tableaux cipres/`) ; fiche de lecture, termes de T28 à T30 et de T31 ; [points_a_valider](points_a_valider.md) ; schéma « vues cnss », planche 02 (vue `V_BI_CONTROLE_POSTERIORI` proposée le 07/11/2026). Ce document les reprend avec leurs identifiants, et **propose** les règles nouvelles N1 à N14 sans rien modifier.

**Le schéma associé :** `excalidraw/schemas/prestations/T28 T31 controles indus/regles_t28_t31_controles_indus.excalidraw`, fait par `excalidraw/sources/prestations/famille_t28_t31_controles_indus.py`. Il dit la même chose en plus court, avec les mêmes identifiants.

**Les mots de ce document.** *Contrôle a posteriori* : une vérification d'un dossier faite après le paiement, pour voir si ce qui est payé correspond au dossier. *Dossier payé* : un dossier dont les droits ont été payés au bénéficiaire. *Liquider* : calculer les droits d'un assuré et les lui notifier. *Débours* : une ligne de paiement due à une personne pour un mois ; un débours de paiement a le type PM ; son statut D veut dire payé par un titre de paiement (un effet), P payé sans effet (sens lu dans les données, à confirmer par la DSI). *Ajustement* : une correction de paiement enregistrée à part (table `AJUSTEMENT`) ; elle est *établie* à une date. *Trop-perçu* : une somme payée en trop, que la CNSS doit reprendre. *Moins-perçu* : une somme payée en moins, que la CNSS rattrape par un *rappel*. *Indu* : une prestation versée à tort ; pour Kaya 2017, seuls les trop-perçus en sont. *Payées* : les sommes versées dans le mois. *Convention* ou *coordination* : un dossier partagé entre la CNSS et la caisse d'un autre pays. *Rachat de rente* : un capital versé une fois à la place d'une rente d'accident. *Branche* : PVID (pensions), AT/MP (accidents du travail et maladies professionnelles, que la CNSS appelle aussi risques professionnels, RP), PF (prestations familiales). Le *cliché* : la copie de la base sur laquelle on calcule. Les *réglages* : les choix écrits dans le bloc `parametres` en tête de chaque requête (une « CTE » en langage SQL) ; on les change sans réécrire la requête. *Pnn*, *lot n* : le résultat numéroté d'une requête de contrôle déjà lancée sur la base. *Écart moyen* : la moyenne des écarts mois par mois, sans tenir compte du signe. *Corrélation* : un chiffre entre −1 et 1 qui dit si deux séries de mois montent et descendent ensemble (1 : exactement ensemble ; 0 : aucun lien). *Vue* : une table de lecture que la DSI prépare pour la BI. *DTM* : les tables de résultats du projet BI actuel, jugées fausses ; on n'en cite que le code, jamais les chiffres. Les sigles des directions sont expliqués en partie 6.

---

## Ce que disent les sources

| Source | Ce qu'elle dit pour ces tableaux |
|---|---|
| Recueil des besoins v7 (14/01/2026 et 20/01/2026) | T30 (l. 134 à 136) : « TOTAL : total des trimestres » ; « Nbre de bénéf. payés : nbre de dossiers payés » ; périodicité trimestrielle, semestrielle, annuelle. T29 (l. 182) et T28 (l. 257) : « Idem. Tab 30 ». T28 (l. 255 et 256) : une ligne « Pension de retraite anticipée » ; « Source : Fichiers Excel (DAI) ». T31 (l. 142 à 146) : les trois branches ; « Source : Fichiers Excel (DAI) » ; par mois, trimestre et année. Les indicateurs (l. 1111 à 1128, 1190 à 1209, 1267 à 1285, 1369 à 1376, 1608 à 1614) donnent le ratio de contrôle a posteriori et le ratio d'indus de chaque branche, avec leurs formules, par direction régionale et par agence |
| Réunions avec la CNSS | Aucune sur T28 à T31 à ce jour. Les questions de la fiche T31 et de points_a_valider (points 1, 17, 29 à 33) attendent leur réponse |
| Atelier de Kaya (2017) | Tableau 1 : T28, supprimer la « pension de solidarité », regrouper veuf/veuve et orphelin en « pension de survivant » et en « allocation de survivant », développer le module des contrôles (l. 281 à 284) ; T29, nouveau titre (« nombre de dossiers payés AT/MP contrôlés a posteriori »), ligne « achat de rente », « nombre de prestations payées », rentes de survivants regroupées, colonne total rattachée au « nombre de dossiers » (l. 172 à 177) ; T30, pas de module dans l'application, la DAI produit ces rapports à la main, détail par direction régionale et par agence, le total fait partie des dossiers contrôlés (l. 93 à 99) ; T31, « les moins perçus ne sont pas des prestations indues », « payées » = la somme décaissée au profit de l'assuré (l. 107 à 110). Tableau 2 : les mêmes formules que le recueil pour les deux ratios |
| Annuaire CIPRES | Description p. 12 et 13, maquettes p. 35 et 36 : dossiers payés (bénéficiaires payés pour T30) et contrôlés par trimestre, norme 5 % au moins, rempli en fin de trimestre ; T31 : payées, trop perçus, moins perçus par mois et par branche, norme 2 % au plus, rempli en fin d'année. Glossaire : contrôle a posteriori (p. 62), dossier payé et moins perçu (p. 64), prestation indue (p. 65), trop perçu (p. 66) |
| Annuaire CNSS 2024 | Ne publie ni contrôles a posteriori ni indus. Donne les sommes servies chaque mois, par branche et par direction : pensions (tableaux 28 à 34), prestations familiales (40 à 46), accidents du travail (82, 90 à 99), récapitulatif (106) ; les rachats de rente (90, 96, 97) ; la retraite anticipée à part dans les effectifs (18, 22, 23) ; les bénéficiaires d'indemnités de maternité (35, 39) et les allocations de vieillesse liquidées (24) ; les allocataires (37) et les rentes (79, 80) au 31/12 ; les décès de pensionnés (26). Les « contrôles » des tableaux 11, 100 à 102 et les « titres de paiement » des tableaux 107 à 113 parlent d'autre chose |
| Modèles de ratios de la CNSS (2025) | Feuille 05 : taux de contrôle = dossiers contrôlés ÷ « dossiers liquidés ou en portefeuille », norme 5 % au moins, par branche et par an. Feuille 06 : taux de trop-perçu et taux de moins-perçu, chacun rapporté au « montant des dossiers contrôlés », et un taux global (les deux additionnés), norme 2 % au plus. Ce sont des modèles vides. La feuille 04 (restes à traiter) concerne T25 à T27 |
| Données de la base | Lot 1 (P01, P06, P07, P11, P13), lot 3 (P34, P35, P41, P45 à P47), lot 5 (P78), lot 6 (P79), lot 8 (P93, P95, P98, P99), lot 9 (P102 à P106) ; comparaison A01 (dossier `comparaison_annuaire_cnss_2024`) |

---

## 0. Les tableaux de la famille

| Tableau | Ce qu'il compte (Annuaire CIPRES) | Page | Ce qu'on a déjà |
|---|---|---|---|
| T28 | Les dossiers de pension (PVID) payés, puis contrôlés a posteriori au 1er, 2e, 3e et 4e trimestre, et un TOTAL. Lignes de la maquette : pensions de retraite, d'invalidité, de veuf/veuve, d'orphelin, de solidarité, d'ascendants ; allocations de retraite, de veuf/veuve, d'orphelin ; autres prestations de la branche ; TOTAL | p. 12 ; maquette p. 35 ; glossaire p. 62 et 64 | Pas de requête (les contrôles manquent) ; fiche de lecture, termes de T28 à T30 ; P13 |
| T29 | La même grille pour les AT/MP. Lignes : indemnités journalières, rentes viagères, rentes de veuve/veuf, d'orphelins, d'ascendants, allocations d'AT/MP, autres prestations de la branche, TOTAL | p. 12 ; maquette p. 35 | Pas de requête ; fiche de lecture ; P13 |
| T30 | La même grille pour les prestations familiales, maternité comprise ; la première colonne s'appelle « nombre de bénéficiaires payés ». Lignes : allocations prénatales, indemnité journalière de maternité, allocations familiales, autres prestations de la branche, TOTAL | p. 13 ; maquette p. 36 | Pas de requête ; fiche de lecture ; P13, P79 |
| T31 | Une ligne par mois et une ligne TOTAL. Pour chaque branche (PVID, AT/MP, PF), puis pour le total : payées, trop perçus, moins perçus. Sert au ratio des prestations indues | p. 13 ; maquette p. 36 ; glossaire p. 64 à 66 | `t31_requete_proposee.sql` (01/11, pas exécutée) ; fiche T31 (8 règles, 5 questions) ; lot 9 |

La table des contrôles de l'application (`DOSSIERS_CONTROLES`) a 1 542 lignes en tout, la dernière saisie le 09/07/2025 (P01) : bien trop peu, puisqu'il faudrait environ 7 760 contrôles dans la seule année 2025 pour atteindre 5 % des 155 206 dossiers payés (P13). La vue `V_BI_CONTROLE_POSTERIORI` (une ligne par contrôle : dossier, branche, prestation contrôlée, date, résultat, montant de l'indu trouvé) est proposée à la DSI dans le schéma « vues cnss », planche 02.

---

## 1. Ce que demande le recueil des besoins v7

Toutes les lignes du recueil qui touchent T28 à T31. Les numéros de ligne sont ceux de `besoins_v7.txt`. Les formules des indicateurs sont des équations Word, absentes du texte : elles ont été lues dans le document Word, et ce sont les mêmes que dans le rapport de Kaya 2017.

| Réf. | Ce que dit le recueil | En clair | Où c'est pris en compte | Statut |
|---|---|---|---|---|
| Prestation #2 (T30), l. 134 | « TOTAL : total des trimestres » | La colonne TOTAL additionne les contrôles des quatre trimestres ; elle n'ajoute pas les dossiers payés. Kaya 2017 le disait déjà pour T30 | N5 ; la fiche de lecture parle des trimestres, pas du TOTAL. Aucune requête T30 | à intégrer |
| Prestation #2 (T30), l. 135 | « Nbre de bénéf. payés : nbre de dossiers payés » | La colonne compte des dossiers payés, pas des personnes (pas les enfants) | N4. Ordre de grandeur 2024 : environ 104 600 dossiers d'allocations familiales payés dans l'année (P79). Aucune requête, chiffre à recompter selon N4 ; la CNSS compte autrement (Q7) | à intégrer |
| Prestation #2 (T30), l. 136 | « Périodicité : Trimestrielle, Semestrielle, Annuelle » | T30 sort pour un trimestre, un semestre ou une année. Le semestre est nouveau (l'Annuaire CIPRES dit « fin de trimestre ») | N6 ; réglage `payes_depuis` (Q8) | à intégrer |
| Prestation #8 (T29), l. 182 (« Idem. Tab 30 », renvoie à l. 134) | TOTAL = total des trimestres | Même colonne TOTAL pour les AT/MP. Kaya 2017 écrivait pour T29 « intégrer la colonne total dans la colonne de modalité nombre de dossiers » | N5 | à intégrer |
| Prestation #8 (T29), l. 182 (renvoie à l. 135) | Bénéficiaires payés = dossiers payés | Le recueil compte des dossiers payés, là où Kaya 2017 avait écrit « nombre de prestations payées » pour T29. Lire que le recueil annule ce renommage est une lecture BI, à confirmer par la DCP | N4 | à intégrer |
| Prestation #8 (T29), l. 182 (renvoie à l. 136) | Périodicité trimestrielle, semestrielle, annuelle | T29 au trimestre, au semestre, à l'année | N6 | à intégrer |
| Prestation #19 (T28), l. 255 | « Ajouter une ligne “Pension de retraite anticipée” » | La retraite anticipée (code RPRA) a sa propre ligne dans T28, à côté de la vieillesse | N1. La correspondance du README range RPRA avec la vieillesse (« à confirmer ») : à reprendre après validation | à intégrer |
| Prestation #19 (T28), l. 256 | « Source : Fichiers Excel (DAI) » | Les contrôles a posteriori sont tenus par la DAI dans des fichiers Excel, pas dans l'application | N11 ; vue `V_BI_CONTROLE_POSTERIORI` proposée ; remarque B14 du recueil (qui étend cette source à T29 et T30) | hors base |
| Prestation #19 (T28), l. 257 (« Idem. Tab 30 », renvoie à l. 134) | TOTAL = total des trimestres | Même colonne TOTAL pour les pensions | N5 | à intégrer |
| Prestation #19 (T28), l. 257 (renvoie à l. 135) | Bénéficiaires payés = dossiers payés | Pour T28 aussi, on compte des dossiers payés | N4 | à intégrer |
| Prestation #19 (T28), l. 257 (renvoie à l. 136) | Périodicité trimestrielle, semestrielle, annuelle | T28 au trimestre, au semestre, à l'année | N6 | à intégrer |
| Prestation #3 (T31), l. 142 à 144 | « PVID : … (Branche pension) ; AT/MP : … (RP) ; PF : Prestation familiale » | Les trois branches de T31. La maternité n'y est pas citée | R1 (la maternité va avec les PF, comme dans l'annuaire CNSS 2024, tableaux 40 et 41) ; t31 le fait déjà | pris en compte |
| Prestation #3 (T31), l. 145 | « Source : Fichiers Excel (DAI) » | Aujourd'hui, la CNSS tire T31 des fichiers de la DAI. Sa grille (feuille 06) laisse penser que la DAI ne compte que les indus trouvés par ses contrôles | N10 : la base par défaut, le fichier de la DAI pour comparer ; remarque B13 du recueil ; question 4 de la planche 02 « vues cnss » | question (Q14) |
| Prestation #3 (T31), l. 146 | « Périodicité : Mensuelle, Trimestrielle, Annuelle » | T31 sort pour un mois, un trimestre ou une année | N6. La requête t31 ne tourne que sur une année entière et ne donne pas le taux d'un trimestre : réglage `periode` à créer | à intégrer |
| Prestation #29 (T51), l. 326 | « Allocation de survivants : veufs/veuves, orphelins et ascendants » | La CNSS met l'ascendant dans l'allocation de survivant ; Kaya 2017 ne regroupait que veuf/veuve et orphelin | N1 : la ligne « allocation de survivant » de T28 prend aussi l'allocation d'ascendant (RASA). Aucune requête T28 | à intégrer |
| Prestation #1 (T27), l. 127 | « Autres prestations de la branche : Renommer par “Frais médicaux de maternité” » | Écrit pour T27 ; T30 a la même dernière ligne pour la même branche | N3, par analogie seulement ; la ligne reste vide, faute de ces frais dans la base | question (Q12) |
| Indicateurs PF, l. 1111 et 1116 à 1119 | « Le ratio de contrôle a posteriori » : dossiers contrôlés après paiement ÷ dossiers liquidés et payés × 100 ; mensuel, trimestriel, annuel ; par type de prestations, DR et agence ; DAI, DR | La part des dossiers de PF vérifiés après paiement, chaque mois, trimestre et année. Kaya 2017 le voulait seulement annuel. Le mois va plus loin que T30, qui est trimestriel | C1 (la formule) ; N5, N6, N9. Aucune donnée depuis juillet 2025 | hors base |
| Indicateurs AT/MP, l. 1190 et 1196 à 1198 | Le même ratio ; annuel ; par type de prestations, DR et agences ; DR, DCP | La part des dossiers AT/MP vérifiés, chaque année | C1 ; contrôles à obtenir de la DAI | hors base |
| Indicateurs pensions, l. 1267, 1274 et 1275 | Le même ratio ; annuel ; par type de prestations, DR et agence ; sans structure responsable | La part des dossiers de pension vérifiés, chaque année | C1 ; contrôles à obtenir de la DAI | hors base |
| Indicateurs PF, l. 1120 et 1125 à 1128 | « Le ratio des prestations indues » : montant des prestations indues ÷ montant des prestations payées sur la période × 100 ; mensuel, trimestriel, annuel ; par type de prestations, DR et agence ; DAI, DR | Le taux d'indus des prestations familiales, chaque mois, trimestre et année | Fiche T31, règle 8. La requête t31 ne donne que le taux du total : le taux PF (0,045 % en 2024) vient du lot 9 (P105). Manquent le taux par branche, la DR et l'agence (N9). « Par type de prestation » est impossible : un trop-perçu n'est relié à aucune prestation (P106, Q27) | à intégrer |
| Indicateurs AT/MP, l. 1199 et 1207 à 1209 | Le même ratio ; annuel ; par type de prestations, DR et agences ; DR, DCP | Le taux d'indus des AT/MP, chaque année | Fiche T31, règle 8 (0,47 % en 2024, P105 ; payées AT/MP trop basses, N7) ; N9 | à intégrer |
| Indicateurs pensions, l. 1277, 1284 et 1285 | Le même ratio ; annuel ; par type de prestations, DR et agence | Le taux d'indus des pensions, chaque année | Fiche T31, règle 8 (0,47 % en 2024, P105) ; N9 | à intégrer |
| Indicateurs prestations, l. 1357, 1369 et 1373 à 1375 | « Le taux de prestations indues (CIPRES) » : même formule ; trimestriel, annuel ; par DR et par agence ; sans structure responsable | Le taux d'indus de toute la CNSS, par trimestre et par an | Taux total de t31 (0,39 % en 2024) ; le trimestre et la DR manquent (N6, N9) | à intégrer |
| Indicateurs du recouvrement, l. 1608 à 1614 | Le même taux, « annuel, par DR et par agences », DCR et DR | Le même indicateur, rangé aussi sous le recouvrement. Kaya 2017 l'y rangeait déjà (tableau 2, l. 1344) : ce n'est pas une erreur récente | Même calcul que l. 1369 ; reste à savoir qui en répond | question (Q29) |
| Structures responsables, l. 1119, 1128, 1198 et 1209 | « DAI, DR » pour les ratios des PF ; « DR, DCP » pour ceux des AT/MP | Les questions sur les deux ratios vont à la DAI, avec la DCP et les directions régionales | Destinataires des questions (partie 6) | pris en compte |
| Ratios CIPRES respectés, l. 1978 à 1989 | Nombre de ratios respectés ÷ ratios CIPRES à respecter × 100 ; « Source : DAI. Données non disponibles en base » ; DES/MQ | La part des normes CIPRES tenues | Pour la CNSS, hors base. T28 à T31 lui fourniront deux ratios avec leur norme (N12) : en 2024, l'indus est tenu ; en 2025, le contrôle ne l'est pas | hors base |
| Termes de référence, l. 4 à 10 | « cinq (5) Directions régionales : Ouagadougou, Bobo Dioulasso, Ouahigouya, Fada N'gourma, Dédougou » | « Par DR » veut dire ces cinq directions. La base en a cinq, plus un siège inactif (P45) | N9 | à intégrer |
| Indicateurs, l. 1065 | « De manière globale, donner la possibilité de désagréger l'indicateur » | Chaque taux doit pouvoir se découper : direction, agence, type de prestation | N9 | à intégrer |
| Attentes, l. 29 ; caractéristiques, l. 51 et 68 | Données « traquées jusqu'au niveau de granularité le plus élémentaires » ; analyses « y compris au niveau transactionnel » | Depuis le taux d'indus, descendre jusqu'à chaque trop-perçu et chaque paiement ; depuis le taux de contrôle, jusqu'à chaque contrôle | N13. La l. 29 est reprise d'un cahier bancaire (remarque B41 du recueil) : on s'appuie surtout sur les l. 51 et 68 | à intégrer |
| Attentes et caractéristiques, l. 31 et 74 | Présentation multidimensionnelle ; une dimension ajoutée vaut pour tous les indicateurs | Les mêmes axes (branche, ligne, DR, agence, mois) pour T28 à T31 et leurs taux | N9, N13 | à intégrer |
| Attentes, l. 32 | « Emission d'alertes sur les indicateurs de performances » | Prévenir quand le contrôle passe sous 5 % ou l'indu au-dessus de 2 % | N12 ; l'alerte elle-même se règle dans l'outil de restitution (OBIEE) | à intégrer |
| Attentes, l. 35 | Rapports « à des fréquences adaptées » | Chaque tableau à sa fréquence | N6 | à intégrer |
| Attentes et caractéristiques, l. 28, 34 et 75 | Peu d'intervention manuelle ; restitutions automatisées ; rafraîchissement automatique | Les chiffres sortent de requêtes, sans saisie à la main. Cela se heurte aux fichiers Excel de la DAI | N10 (T31 lu dans la base) ; N11 (un modèle de fichier fixe, chargé dans la vue) | à intégrer |
| Caractéristiques, l. 53 à 55 et 60 | Charger des fichiers locaux ou distants ; faire des indicateurs à partir de bases ou de documents | La BI peut prendre les fichiers de la DAI | N11 : la vue peut être alimentée par le fichier de la DAI | à intégrer |
| Caractéristiques, l. 58 | Règles de transcodage écrites et rejouables | La correspondance entre codes de prestation et lignes est écrite | N1 à N3 ; correspondance du README (R2) | pris en compte |
| Caractéristiques, l. 72 | « Permettre le contrôle par profil d'utilisateurs des conditions d'accès aux données (règles de confidentialité) » | Les contrôles de la DAI (résultat, anomalie, montant de l'indu de chaque dossier) sont sensibles : seuls certains profils les voient | N14 ; qui voit le détail : Q10 | à intégrer |
| Attentes et caractéristiques, l. 24 et 78 | Profilage de la qualité des données | Mesurer les trous de la base | Lots 1 et 9 ; points d'attention de la partie 7 | pris en compte |
| Général, l. 15 et 92 à 94 | La source : l'application de gestion des prestations | T31 se calcule sur la copie de l'application ; T28 à T30 en tirent les dossiers payés, pas les contrôles | Copie de la base (schéma `USER_DWH`) au 02/02/2026 | pris en compte |
| Prestation #30 (T76), l. 333 | « Ratio : en pourcentage (2 chiffres après la virgule) » | Écrit pour le tableau 76 (action sanitaire et sociale), pas pour T28 à T31 | N12 : par analogie seulement ; la requête t31 arrondit à 3 chiffres (Q30) | question (Q30) |

**Quels tableaux n'ont aucune ligne.** Les quatre tableaux ont leurs lignes. Mais T29 n'a qu'un renvoi (« Idem. Tab 30 ») ; T29 et T30 n'ont pas de ligne « Source » : la remarque B14 du recueil leur étend la source de la DAI, et on suppose la même source pour les trois tableaux de contrôle (N11).

**Les lignes générales qui ne changent rien ici :** l. 80 et 81 (document de référence, ni nommé ni joint : rangé dans la famille T50-T57, question T50-T57 Q25) ; indicateur de satisfaction (l. 1075 à 1084 : rangé dans la famille T50-T57, hors base) ; indicateur de paiement par virement (l. 1085 à 1091 : il porte sur le mode de paiement, pas sur T31) ; ratio des ressources de cotisations (l. 1129 à 1141, 1210 à 1214, 1287 à 1294 : des montants notifiés, hors famille) ; nouvelles prestations « à confirmer avec DAI et DPC » (l. 1338 à 1356 : rangé dans la famille T50-T57, question T50-T57 Q24) ; contrôles d'inspection des employeurs, source DAI (l. 1565 à 1590, recouvrement) ; délai de production des rapports d'audit, source DAI (l. 1991 à 1998) ; colonnes « Priorité » vides (l. 120 et 121).

---

## 2. Les règles, en clair

### R1 — Les branches

**Règle.** La branche vient du type du dossier : V (vieillesse, invalidité, décès) = PVID ; A = AT/MP ; F (prestations familiales) et M (maternité) = PF. Un trop-perçu n'est relié à aucune prestation : sa branche se lit aussi par le dossier.

**Exemple.** Un trop-perçu sur un dossier de maternité va dans la colonne PF de T31 ; une indemnité de maternité contrôlée va dans T30.

**Sources.** Recueil v7, l. 142 à 144 ; Annuaire CIPRES p. 36 ; Annuaire CNSS 2024, tableaux 40 et 41 (indemnités de maternité, allocations prénatales et frais médicaux dans les PF) ; README, R1 ; Données P02, P05, P102 (15 trop-perçus de maternité en 2024, 2 158 495 FCFA, que le DTM oubliait), P106.

**État.** Confirmée par l'annuaire CNSS 2024.

### R3 — Ce qu'est un paiement

**Règle.** Un paiement est un débours de type PM au statut D (payé par un titre de paiement) ou P (payé sans titre). On prend le montant du débours, jamais le net à payer du titre. C'est la base des payées de T31 et des dossiers payés de T28 à T30.

**Exemple.** Un paiement au statut V (sens à confirmer par la DSI) n'est pas compté : ni dans les payées de T31, ni dans les dossiers payés de T28.

**Sources.** Kaya 2017, tableau 1, l. 110 (« payées » = la somme décaissée au profit de l'assuré) ; Annuaire CIPRES, glossaire p. 64 ; README, R3 ; Données P20, P21, P105 (58 732 916 866 FCFA payés en 2024 dans les trois branches).

**État.** Confirmée pour les pensions (+1,3 % face à l'annuaire, tableau 28) ; à préciser pour les PF (−3,8 %, tableau 41) ; non confirmée pour les AT/MP (−20,3 % sur les espèces, tableau 90 ; −28,9 % soins compris, tableau 99). Le total des trois branches tombe à −0,26 % (tableau 106), mais seulement parce que ces écarts se compensent (partie 4). Le taux d'indus de l'année reste 0,39 % quelle que soit la lecture.

### R4 — La date des payées de T31

**Règle.** Aujourd'hui, un paiement compte dans le mois qu'il paie (`DEB_DATE_EFFET`). L'autre choix est le jour où il est saisi (`DEB_DATE_INSERT`), rappels compris. Un seul réglage, `date_montant`, passe de l'un à l'autre.

**Exemple.** La pension de janvier saisie le 5 février : en janvier avec 'MOIS', en février avec 'SAISIE'.

**Sources.** README, R4 (question posée au point du 02/11/2026) ; fiche T31, question Q4 ; points_a_valider n° 1 ; Annuaire CNSS 2024, tableaux 28, 41 et 90 (sommes servies mois par mois) ; Données P98, P105.

**État.** À décider par la CNSS (non bloquant). Au trimestre, la date de saisie suit la CNSS pour les trois branches (pensions de +1,5 à +2,1 % ; PF de −3,0 à −3,7 %) ; le mois payé non (pensions de −0,5 à +4,3 % ; PF de −10,5 à +3,0 %). Au mois, la saisie fait mieux pour les PF (écart moyen 3,4 % contre 6,6 %) et pour les AT/MP (19,6 % contre 26,6 %), pas pour les pensions (6,4 % contre 6,2 %), même si elle reproduit mieux leur rythme (partie 4). Une date de saisie n'est pas une date de versement : la date de décaissement est un autre champ, souvent vide (80 % des paiements PF de 2025 sans elle, P07). La date d'approbation, déjà testée sur l'année, est presque identique à la saisie (P98).

### R13 — Le cliché du calcul

**Règle.** L'exercice N se calcule sur la copie de la base de fin mars N+1, date à laquelle la CNSS arrête son annuaire.

**Exemple.** Un trop-perçu établi le 15/12/2024 mais saisi le 20/01/2025 entre dans T31 de 2024, mois de décembre.

**Sources.** README, R13 ; en-tête de `t31_requete_proposee.sql` ; Annuaire CNSS 2024, tableau 26 ; Données P99 (444 décès de retraités de 2024 connus fin mars 2025, pour 447 publiés).

**État.** Proposée ; appuyée par un indice fort de l'annuaire CNSS 2024 (tableau 26), à confirmer par la DPC (T32-T41 Q3). Aujourd'hui, le seul cliché disponible porte la copie arrêtée au 02/02/2026.

### I1 — Trop-perçus et moins-perçus

**Règle.** Un trop-perçu est un ajustement TP (trop-perçu) ou TP-CONV (trop-perçu sur convention entre caisses). Un moins-perçu est un ajustement MP (moins-perçu ou rappel) ou RC (rappel sur convention). Les deux sont montrés dans T31 ; seuls les trop-perçus sont des indus et entrent dans le taux. La maternité est comprise.

**Exemple.** Une pension payée deux fois en mars 2024, trop-perçu établi le 20/05/2024 : trop-perçu de mai, colonne PVID, dans le taux.

**Sources.** Annuaire CIPRES p. 13 et glossaire p. 64 à 66 ; Kaya 2017, tableau 1, l. 107 et 108 ; README, I1 ; fiche T31, règles 3 et 4 ; Données P11, P41, P102 (en 2024 : 684 trop-perçus pour 229 484 759 FCFA, et 414 442 709 FCFA de moins-perçus).

**État.** Proposée, à valider (DCP, DAI). La grille de la CNSS de 2025 met les moins-perçus dans un taux global (question Q16).

### Fiche T31 r. 5 — Ce qui n'est pas un indu

**Règle.** Restent hors de T31 : le remboursement d'un trop-perçu (REM-TP), les avances (AV, AV-COVID19) et leur reprise (REV-AV), les mois de pension gratuits (MPG), les dons COVID (DON-COVID19), les rachats de rente (RR), les oppositions (OI, OE).

**Exemple.** Une avance sur pension versée en avril, reprise en septembre : rien dans les colonnes d'indus de T31.

**Sources.** Kaya 2017, tableau 1, l. 108 ; fiche T31, règle 5 ; Données P41 (libellés des 13 types), P102 (5 384 869 208 FCFA d'avances établies en 2024, de loin les plus gros ajustements), P11 (le DTM comptait REM-TP comme un nouvel indu).

**État.** Proposée. Le rachat de rente et les avances ne sont pas des indus ; reste à savoir s'ils entrent dans les payées (N7, Q22, Q23). Le mois de pension gratuit est sans effet depuis 2016 (P11) ; s'il revenait, il irait dans les payées (l'annuaire en fait une prestation servie, tableau 28), à confirmer.

### Fiche T31 r. 6 — Les statuts des ajustements

**Règle.** Tous les ajustements comptent, sauf le statut R (rejeté ou annulé). A veut dire en cours, C soldé.

**Exemple.** Un trop-perçu en cours de reprise par retenues (statut A) compte le mois où il a été établi.

**Sources.** Fiche T31, règle 6 ; README, lot 9 ; Données P103 (moins-perçus C payés en totalité : 14 073 501 511 sur 14 084 842 402 ; moins-perçus R presque pas : 25 749 sur 178 832 920 ; plus aucun ajustement R depuis 2014 ; tous les trop-perçus au statut A depuis 2017).

**État.** Proposée ; sens des statuts à confirmer par la DSI (Q25) ; sans effet sur 2015 à 2025.

### Fiche T31 r. 7 — Le mois d'un indu

**Règle.** Un trop-perçu ou un moins-perçu compte une seule fois, le mois où il est établi (`AJ_DATE_ETABLISSEMENT`) : ni les mois payés en trop, ni les mois où il est repris.

**Exemple.** Trop-perçu établi en novembre 2024, repris par retenues jusqu'en juin 2025 : novembre 2024 seulement.

**Sources.** Fiche T31, règle 7 et question Q2 ; fiche de lecture, T31, terme « le mois » ; Données P103 (la date d'établissement est toujours remplie ; la période payée en trop n'est pas dans la base).

**État.** Proposée ; question Q17.

### Fiche T31 r. 8 — Le taux de prestations indues

**Règle.** Taux = trop-perçus ÷ prestations payées × 100, par branche et pour l'année. La norme (2 % au plus) se lit sur l'année, pas mois par mois.

**Exemple.** AT/MP en avril 2024 : 4,08 % à cause d'un seul trop-perçu de 2 306 520 FCFA, remboursé le même mois ; sur l'année : 0,47 %.

**Sources.** Annuaire CIPRES p. 13 ; Recueil v7, l. 1120, 1199, 1277 et 1369 (formule du document Word) ; Kaya 2017, tableau 2 (même formule) ; fiche T31, règle 8 ; Données P105 (2024 : pensions 0,47 %, AT/MP 0,47 %, PF 0,045 %, total 0,39 %).

**État.** Proposée ; formule conforme au recueil v7. La requête t31 ne calcule que le taux du total (par mois et sur l'année) : les taux par branche viennent du lot 9 (P105) et sont à ajouter à la requête (N9, partie 9).

### C1 — Ce qu'est un contrôle a posteriori

**Règle.** Un contrôle a posteriori est un contrôle enregistré par la DAI (dans sa vue ou son fichier, ou, pour le passé, dans la table des contrôles). La coche « vérifié » des paiements (`DEB_VERIFIE`) n'en est pas un : elle est faite à la saisie. Taux de contrôle = dossiers contrôlés après paiement ÷ dossiers payés × 100 ; norme : 5 % au moins.

**Exemple.** Une pension dont tous les paiements sont cochés « vérifié », sans contrôle enregistré, n'est pas contrôlée.

**Sources.** Annuaire CIPRES p. 12 et 13, glossaire p. 62 ; Kaya 2017, tableau 1 (T28, T30) ; Recueil v7, l. 256 et indicateurs l. 1111, 1190, 1267 (formule) ; Modèles CNSS feuille 05 (même norme ; autre dénominateur, Q6) ; carte C1 de `regles_prestations` ; fiche de lecture, T28 à T30 ; Données P06 (5 942 752 débours cochés « vérifié »), P13 (115 contrôles en 2025 pour 155 206 dossiers payés, soit 0,07 %), P01 (dernière saisie le 09/07/2025).

**État.** Ouverte : où sont les contrôles depuis juillet 2025 (Q1), et la table a-t-elle jamais été complète (Q2) ? La carte C1 de `regles_prestations` ne lit que la table des contrôles ; ce document y ajoute la vue ou le fichier de la DAI (N11), sans changer le reste de la règle.

### A1 — Les accessoires de pension (étendue à T28)

**Règle.** La bonification ou majoration pour enfant (RBEC, RME, RME-COL) et la contribution aux charges du ménage (RCCM) s'ajoutent à une pension : dans T28, elles suivent la ligne de cette pension et ne font pas compter le dossier une fois de plus.

**Exemple.** Un retraité payé en pension de vieillesse et en bonification pour enfant compte une fois, sur la ligne de la vieillesse.

**Sources.** Carte A1 de `regles_prestations` ; Annuaire CNSS 2024, tableaux 28 et 34 (majoration pour enfant rangée avec la pension de vieillesse) ; Données P34 (68 836 bonifications pour enfant).

**État.** Confirmée par l'annuaire CNSS 2024 pour les montants ; proposée pour T28. La correspondance du README range ces codes en « autres prestations de la branche » pour T25 et T28 : désaccord à reprendre après validation, comme pour RPRA (N1).

### H11 — Ce qui est faux dans les DTM des contrôles

**Règle.** Dans `DTM_CONTROLE_PRESTATION`, les dossiers payés ne sont comptés que parmi les dossiers contrôlés : le taux de contrôle ne peut pas être calculé. `DTM_INDICATEURS_PRESTATIONS` calcule ce taux sur les ajustements, pas sur les contrôles.

**Exemple.** Avec ces requêtes, le taux de contrôle ne mesure pas du tout les contrôles de la DAI.

**Sources.** `reference_cipres_T25_T57.md`, H11 ; code de `controle_prestation.sql` et `indicateurs_prestations.sql` (lu, non modifié) ; Données P06, P13.

**État.** Confirmée ; à corriger après visa (TODO, tâche 10). D'autres défauts du code sont notés au point d'attention 13.

### H12 — Ce qui est faux dans le DTM des indus

**Règle.** Dans `DTM_PRESTATION_INDUE`, tout ajustement qui n'est pas un trop-perçu est classé « moins-perçu », et la branche M (maternité) est exclue des indus.

**Exemple.** Un remboursement de trop-perçu (REM-TP) y devient un indu de plus ; un trop-perçu de maternité n'apparaît pas.

**Sources.** `reference_cipres_T25_T57.md`, H12 ; code de `prestation_indue.sql` (lu) ; Données P11, P103.

**État.** Confirmée en partie (le classement suit les libellés, sauf REM-TP) ; à corriger après visa (TODO, tâche 9).

### N1 — T28 : les lignes

**Règle.** Lignes : pension de vieillesse (RPV) ; pension de retraite anticipée (RPRA), sur sa propre ligne ; pension d'invalidité (RPI) ; pension de survivant (RPSV veuve, RPSF veuf, RPSE orphelin) ; pension d'ascendant (RPA) ; allocation de vieillesse (RAV) ; allocation de survivant (RAS, RASC, RASO et RASA, l'ascendant) ; autres prestations de la branche (remboursement de cotisations, RRC) ; TOTAL. Plus de ligne « pension de solidarité ». Les accessoires suivent leur pension (A1).

**Exemple.** Une pension de retraite anticipée contrôlée va sur sa propre ligne, pas avec la vieillesse.

**Sources.** Recueil v7, l. 255 (ligne anticipée) et l. 326 (l'ascendant dans l'allocation de survivant) ; Kaya 2017, tableau 1, l. 281 à 283 (plus de pension de solidarité ; regrouper en pension de survivant et allocation de survivant) ; Annuaire CIPRES p. 35 ; Annuaire CNSS 2024, tableaux 18, 22 et 23 (pension anticipée à part dans les effectifs), 28 et 34 (« Solidarité » à zéro, survivants regroupés) ; Données P34 (aucun code de solidarité ; RPRA = retraite anticipée, 176 prestations).

**État.** Nouvelle (proposée). La ligne anticipée est demandée par le recueil ; les regroupements et la fin de la pension de solidarité sont tranchés par Kaya 2017 (la question de la fiche de lecture sur la pension de solidarité est réglée). La correspondance du README (RPRA avec la vieillesse) est à reprendre après validation.

### N2 — T29 : les lignes

**Règle.** Lignes : indemnités journalières (AIJ) ; rentes viagères (ARI) ; rentes de survivants (ARSV, ARSF, ARSE, ARSA), regroupées ; allocations d'incapacité (AAI) ; achat de rente, lu comme le rachat de rente (ajustement RR) ; autres prestations de la branche (soins remboursés) ; TOTAL.

**Exemple.** Un dossier qui a reçu un rachat de rente en septembre 2024 est un dossier payé de la ligne « achat de rente ».

**Sources.** Annuaire CIPRES p. 35 ; Kaya 2017, tableau 1, l. 173 et 175 ; Annuaire CNSS 2024, tableaux 90, 96 et 97 (colonne « rachat de rente », rentes de survivants regroupées) ; Données P104 (11 rachats de rente payés pour des mois de 2024, 21 519 361 FCFA).

**État.** Nouvelle (proposée). Les regroupements et la ligne sont tranchés par Kaya 2017 ; que « achat de rente » veuille dire « rachat de rente » reste à confirmer (Q11). Les paiements de chaque ligne sont précisés par N4.

### N3 — T30 : les lignes

**Règle.** Lignes : allocations prénatales (FAP, MAP) ; indemnités journalières de maternité (MIJ) ; allocations familiales (FAF) ; autres prestations de la branche, renommées « frais médicaux de maternité » comme pour T27 ; TOTAL.

**Exemple.** Une allocation prénatale de la branche maternité (MAP) va sur la ligne des allocations prénatales.

**Sources.** Annuaire CIPRES p. 36 ; Recueil v7, l. 127 (le renommage, écrit pour T27) ; Annuaire CNSS 2024, tableaux 40 et 41 (frais médicaux : 25 030 869 FCFA en 2024) ; Données P35 (7 types de frais de maternité), P95 (aucun débours de soins dans la branche M en 2024), P66.

**État.** Nouvelle (proposée, par analogie avec T27 : question Q12). La dernière ligne reste vide tant que les frais médicaux ne sont pas dans la base (T25-T27 Q15).

### N4 — T28 à T30 : le dossier payé

**Règle.** Un dossier est payé dans la période s'il a au moins un paiement daté dans la période. Les paiements qui comptent dépendent de la ligne : en général, les débours de paiement (R3) ; pour les soins (« autres prestations » de T29), les débours de soins remboursés (type RB) ; pour le rachat de rente, les débours d'ajustement de type RR. Sans cela, ces deux lignes de T29 resteraient toujours vides. Un dossier compte une fois sur chaque ligne où il est payé, et une seule fois dans la ligne TOTAL. Dans T30, « bénéficiaires payés » veut dire dossiers payés. Un dossier payé par une autre caisse (coordination, catégorie `COOR_AC`) n'est pas un dossier payé de la CNSS.

**Exemple.** Un dossier AT/MP payé en indemnités journalières en mars et en rente en octobre : une fois sur chaque ligne, une fois dans le TOTAL.

**Sources.** Annuaire CIPRES, glossaire p. 64 ; Recueil v7, l. 135, repris pour T29 (l. 182) et T28 (l. 257) ; fiche de lecture, T28 à T30, terme 1 ; remarque B09 du recueil (bénéficiaires payés = dossiers payés, pour T29 et T30) ; Données P13 (en 2025, avec un débours de n'importe quel type : V 47 732, A 2 134, F 100 113, M 5 227 dossiers), P79 (104 611 allocataires payés dans l'année 2024), P93, P95, P104.

**État.** Nouvelle (proposée). L'annuaire CNSS 2024 compte autrement les prestations versées une fois ou sur quelques mois : 3 893 bénéficiaires d'indemnités de maternité, soit les 3 890 liquidées en 2024 et non les 4 881 payées (tableaux 35 et 39, P93) ; 833 allocations de vieillesse, soit les 832 liquidées et non les 753 payées (tableau 24). Question Q7, réglage `dossier_paye`.

### N5 — T28 à T30 : le contrôle, le trimestre et le TOTAL

**Règle.** Un contrôle compte dans le trimestre de sa date, sur la ligne de la prestation contrôlée. La colonne TOTAL additionne les quatre trimestres. Le taux, lui, divise les dossiers contrôlés, comptés une seule fois dans la période, par les dossiers payés ; l'écart entre les deux est montré. Ne comptent que les contrôles d'un dossier déjà payé ; les dates impossibles (années 202, 203, 2055) sont écartées.

**Exemple.** Un dossier contrôlé le 12/02/2025 et le 20/10/2025 compte au 1er et au 4e trimestre, donc deux fois dans le TOTAL, mais une fois dans le taux de l'année.

**Sources.** Annuaire CIPRES p. 35 et 36 (quatre trimestres), glossaire p. 62 ; Recueil v7, l. 134, et formule des indicateurs (l. 1111, 1190, 1267 : « dossiers contrôlés après paiement ») ; Kaya 2017, tableau 1 (T30 : le total fait partie des dossiers contrôlés ; T29 : rattaché au « nombre de dossiers ») ; fiche de lecture, T28 à T30, termes 2 et 3 (dossiers distincts) ; Données P13 (années fausses ; la colonne de code de prestation de la table des contrôles n'est pas encore mesurée).

**État.** Nouvelle (proposée). Question Q9, réglage `controle_compte`.

### N6 — Les périodes

**Règle.** T28 à T30 se produisent au trimestre, au semestre et à l'année ; T31 au mois, au trimestre et à l'année. Les dossiers payés se comptent du 1er janvier à la fin de la période. Un taux se recalcule toujours sur les sommes de la période, jamais comme une moyenne de taux mensuels.

**Exemple.** T31 du 1er trimestre 2024 : trop-perçus de janvier à mars ÷ payées de janvier à mars.

**Sources.** Recueil v7, l. 136, 146, et indicateurs l. 1116 et 1117, 1125 et 1126, 1373 et 1374 ; l. 35 ; Annuaire CIPRES p. 12 et 13 ; remarque B32 du recueil ; Données P105 (les 12 mois de 2024).

**État.** Nouvelle (proposée). Le mois du ratio de contrôle des PF (l. 1116) va plus loin que T30 : il se calcule comme un trimestre. Réglages `periode` et `payes_depuis` ; question Q8.

### N7 — T31 : les payées égales aux prestations servies de T56

**Règle.** Sur l'année, les payées d'une branche dans T31 devraient égaler ses prestations servies dans T56 (même règle, même date). Aujourd'hui T31 ne prend que les débours de paiement (PM), alors que T56 prend aussi les soins remboursés (RB). Restent à trancher : les rachats de rente, les rappels de moins-perçus (408 312 511 FCFA en 2024) et les avances (373 206 605 FCFA), versés par des débours d'ajustement ; les soins des AT/MP payés hors des débours ; les frais médicaux de maternité.

**Exemple.** AT/MP 2024 : 744 609 484 FCFA de paiements, pour 934 445 128 publiés en espèces (−20,3 %) ; 763 397 429 avec les soins remboursés, pour 1 074 113 823 publiés soins compris (−28,9 %).

**Sources.** Proposition BI (cohérence entre T31 et T56) ; `t50_t57_requete_proposee.sql` (T56 : PM et RB) ; Annuaire CNSS 2024, tableaux 90, 98, 99 et 106 ; points_a_valider n° 33 et 52 ; Données P95, P104, P105.

**État.** Nouvelle (proposée). Questions Q22 et Q23 ; réglage `payees_types`. Les rappels sont bien des sommes décaissées au profit de l'assuré, au sens de Kaya 2017 : 0,7 % des payées.

### N8 — T31 : les autres taux, à côté du taux d'indus

**Règle.** À côté du taux d'indus (trop-perçus seuls), afficher le taux des moins-perçus, et les trop-perçus et moins-perçus rapportés à toutes les payées. Seul le taux choisi par la CNSS se compare à la norme de 2 %. Le taux de la grille de la CNSS (feuille 06) divise par le montant des dossiers contrôlés : il ne se calcule pas sans ce montant.

**Exemple.** 2024 : trop-perçus 0,39 %, moins-perçus 0,71 %, les deux ensemble 1,10 % des payées : sous les 2 % dans toutes les lectures.

**Sources.** Modèles CNSS feuille 06 ; Annuaire CIPRES p. 13 (indus en trop ou en moins) ; Kaya 2017, tableau 1, l. 108 ; Données P105 (414 442 709 FCFA de moins-perçus en 2024).

**État.** Nouvelle (proposée). Questions Q15 et Q16 ; réglages `taux_numerateur` et `taux_denominateur`.

### N9 — Par branche, direction régionale, agence et type

**Règle.** Les taux de contrôle et d'indus se calculent pour chaque branche, et se déclinent par direction régionale et par agence, grâce au dossier. Les payées (dénominateur du taux d'indus) prennent aussi la direction du dossier, pas celle du titre de paiement. T28 à T30 se déclinent aussi par type de prestation (leurs lignes) ; T31 seulement par branche, car un trop-perçu n'a pas de prestation liée.

**Exemple.** Le taux d'indus de la direction régionale de Bobo-Dioulasso = ses trop-perçus ÷ ses payées, les deux rangés par la direction du dossier.

**Sources.** Recueil v7, l. 4 à 10, 1065, et indicateurs l. 1118, 1127, 1197, 1208, 1275, 1285, 1375 ; l. 31 et 74 ; Kaya 2017, tableau 1 (T30) ; Annuaire CNSS 2024, tableaux 29 à 34, 40, 42 à 46, 91 à 96 et 98 (sommes servies par direction : pour contrôler) ; comparaison A02 (la direction du dossier, validée sur le tableau 22) ; Données P07 (62 % des paiements PF de 2025 sans titre de paiement), P45 à P47, P106.

**État.** Nouvelle (proposée). Les DTM des indus et des contrôles lisent déjà la direction et le service sur le dossier ; celui des prestations, qui donne les payées, les lit sur le titre de paiement. L'agence est à confirmer (Q28) ; le type de prestation des indus aussi (Q27).

### N10 — T31 : la source des indus

**Règle.** Par défaut, T31 lit tous les trop-perçus et moins-perçus enregistrés dans l'application (table `AJUSTEMENT`). Le fichier Excel de la DAI, que cite le recueil, sert à comparer ; il ne remplace la base que si la CNSS le décide.

**Exemple.** On compare le total 2024 de la DAI aux 684 trop-perçus (229 484 759 FCFA) de la base avant de choisir.

**Sources.** Recueil v7, l. 145 ; l. 28, 34 et 75 (peu de saisie manuelle, restitutions automatisées) ; planche 02 « vues cnss », question 4 ; remarque B13 du recueil ; Données P102 à P106.

**État.** Nouvelle (proposée) ; écart à une source plus forte (le recueil), gardé en attendant la DAI. Question Q14 ; réglage `source_indus`.

### N11 — T28 à T30 : la source des contrôles

**Règle.** Les contrôles viennent de la DAI, par la vue `V_BI_CONTROLE_POSTERIORI` (une ligne par contrôle), que la DSI alimente depuis la table des contrôles ou depuis le fichier Excel de la DAI. La table des contrôles de l'application est bien trop maigre pour servir seule, même pour le passé.

**Exemple.** Un contrôle de 2026 noté dans le fichier de la DAI arrive dans T28 par la vue, sans passer par l'application.

**Sources.** Recueil v7, l. 256, et l. 53 à 55 et 60 (charger des fichiers) ; Kaya 2017, tableau 1 (T28 et T30 : la DAI produit les rapports à la main) ; planche 02 « vues cnss » (07/11/2026) ; Données P01 (1 542 lignes en tout, dernière saisie le 09/07/2025), P13 (de 1 à 212 contrôles par an et par branche ; pour 5 % des 155 206 dossiers payés de 2025, il en faudrait environ 7 760 dans l'année ; 212 contrôles de pensions ne feraient que 0,44 % des 47 732 dossiers de pension payés en 2025).

**État.** Nouvelle (proposée). Questions Q1 à Q3 ; réglage `source_controles`.

### N12 — Les normes, les alertes et l'arrondi

**Règle.** Chaque taux porte la mention « dans les normes » ou « hors normes » : contrôle a posteriori, 5 % au moins ; indus, 2 % au plus, lus sur l'année. Les normes sont des réglages de la CTE `parametres`, pas des chiffres écrits dans la requête. Les taux s'affichent avec 3 chiffres après la virgule, comme t31 aujourd'hui.

**Exemple.** 2024 : 0,391 % d'indus, « dans les normes ». 2025 : 0,07 % de contrôle, « hors normes ». Avec 2 chiffres, le taux PF de 2024 s'écrirait 0,04 % au lieu de 0,045 %.

**Sources.** Recueil v7, l. 32 (alertes) et l. 1978 (ratios CIPRES respectés) ; Annuaire CIPRES p. 12 et 13 (les normes) ; Modèles CNSS feuilles 05 et 06 (colonne « Observation ») ; Recueil v7, l. 333 (2 chiffres, pour le tableau 76 seulement) ; remarque B35 du recueil.

**État.** Nouvelle, à intégrer. L'arrondi est une question (Q30).

### N13 — Retrouver chaque chiffre

**Règle.** La BI garde le détail : une ligne par trop-perçu, par moins-perçu, par paiement et par contrôle, avec sa branche, sa date, sa direction et son agence. Chaque case de T28 à T31 se retrouve dans ces listes.

**Exemple.** Les 684 trop-perçus de 2024 se listent un à un, mois par mois.

**Sources.** Recueil v7, l. 51 et 68 (et l. 29) ; planche 02 « vues cnss » (une ligne par contrôle).

**État.** Nouvelle ; la requête t31 part des ajustements et des débours un par un, mais ne sort que des totaux.

### N14 — La confidentialité des contrôles

**Règle.** Le résultat d'un contrôle et le montant d'indu trouvé pour un dossier ne sont visibles que par les profils autorisés. La BI ne lit pas les textes libres des contrôles (`DC_REMARQUE`, `DC_RECOMMANDE`) ni des ajustements (`AJ_REMARQUE`), et la vue proposée ne demande ni nom ni numéro bancaire.

**Exemple.** Un agent d'une direction régionale voit le taux de contrôle de sa direction, pas l'anomalie trouvée sur un dossier précis.

**Sources.** Recueil v7, l. 72 ; planche 02 « vues cnss ».

**État.** Nouvelle (proposée). Question Q10.

---

## 3. Les cas

| Cas | Ce que fait la règle | Pourquoi |
|---|---|---|
| 1. Un retraité payé deux fois en mars 2024 ; trop-perçu établi le 20/05/2024, repris ensuite par retenues sur sa pension | T31 de 2024 : trop-perçu de mai, colonne PVID, dans le taux d'indus. Les retenues des mois suivants ne comptent pas une seconde fois | Compté (I1 ; fiche T31 r. 7) |
| 2. Une allocataire dont l'enfant n'avait plus droit ; trop-perçu établi en novembre 2024, repris jusqu'en juin 2025 | T31 de 2024, novembre, colonne PF. Rien dans T31 de 2025 | Compté (fiche T31 r. 7 ; R1) |
| 3. Une indemnité de maternité payée en trop ; trop-perçu établi en mars 2024 sur un dossier de maternité (type M) | T31, colonne PF : la maternité va avec les prestations familiales. Le DTM actuel l'oubliait | Compté (R1 ; H12) |
| 4. Une veuve sous-payée d'avril à juin 2024 ; moins-perçu établi en juin, payé en juillet | T31 de 2024 : colonne « moins perçus » de juin (PVID), hors du taux d'indus selon Kaya 2017 ; il entrerait dans le taux global de la grille de la CNSS | Selon un réglage (`taux_numerateur` ; I1, N8 ; Q16) |
| 5. Un retraité qui reçoit une avance sur sa pension en avril 2025, reprise sur le rappel en septembre | Ni trop-perçu ni moins-perçu : hors des colonnes d'indus. L'avance passe par un débours d'ajustement : elle n'est pas dans les payées aujourd'hui | Pas compté (fiche T31 r. 5 ; R3) ; payées : selon un réglage (`payees_types` ; Q23) |
| 6. Un retraité décédé en février 2025, décès déclaré en juin ; sa pension a continué d'être versée de mars à mai | Un trop-perçu de 2025 seulement si la CNSS l'établit. Dans la base, presque jamais : 7 trop-perçus après un décès en 2024, pour 175 décès de retraités de 2024 déclarés après mars 2025 | Question (I1 ; Q18) |
| 7. Un trop-perçu d'accident du travail de 2 306 520 FCFA en avril 2024, remboursé le même mois (REM-TP) | Il compte en avril, colonne AT/MP ; le remboursement ne l'efface pas. Le taux AT/MP d'avril monte à 4,08 %, celui de l'année reste à 0,47 % | À surveiller (fiche T31 r. 5 et r. 8 ; N12) : la norme se lit sur l'année |
| 8. Un trop-perçu sur une pension de coordination entre caisses (TP-CONV), établi en 2025 | Compté comme trop-perçu, colonne PVID. Mais ces trop-perçus bondissent : 758 en 2025 (54 771 312 FCFA) contre 15 en 2024 | À surveiller (I1 ; Q21) : à faire expliquer avant de publier 2025 |
| 9. Un rachat de rente d'accident du travail : capital versé en septembre 2024 à la place de la rente (ajustement RR, payé par un débours d'ajustement) | Pas un indu. T31 : dans les payées AT/MP seulement si la CNSS suit son annuaire. T29 : ligne « achat de rente » | Selon un réglage (`payees_types` ; N2, N4, N7 ; Q11, Q22) |
| 10. Un dossier de pension contrôlé par la DAI le 12/02/2025, sans anomalie | T28 de 2025 : un contrôle au 1er trimestre, sur la ligne de la prestation contrôlée. Il compte dans le taux, même sans anomalie | Compté (C1 ; N5) |
| 11. Un dossier contrôlé en mars, puis en octobre 2025 | Au 1er et au 4e trimestre ; la colonne TOTAL le compte deux fois, le taux de l'année une fois | Selon un réglage (`controle_compte` ; N5 ; Q9) |
| 12. Une pension dont tous les paiements sont cochés « vérifié » (`DEB_VERIFIE`), sans contrôle enregistré | Pas un contrôle a posteriori : ni dans T28, ni dans le taux. La coche est faite à la saisie | Pas compté (C1 ; H11) |
| 13. Un contrôle de prestations familiales fait en août 2025 | La table des contrôles n'a plus rien depuis le 09/07/2025 : invisible pour la BI tant que la DAI ne le fournit pas. T30 de 2025 serait faussement bas | À surveiller (C1 ; N11 ; Q1) |
| 14. Une pension de coordination payée par l'autre caisse (`COOR_AC`), que la DAI contrôle | Pas un dossier payé de T28 (la CNSS ne lui verse rien). Compter son contrôle mettrait au numérateur un dossier absent du dénominateur | Question (N4 ; Q6) : la grille de la CNSS compte les dossiers « en portefeuille » |
| 15. Une indemnité de maternité liquidée en novembre 2024, payée en novembre 2024 et en janvier 2025 | Avec N4 : un dossier payé en 2024 et un en 2025. Comme l'annuaire (tableaux 35 et 39) : compté en 2024 seulement, l'année de la liquidation | Selon un réglage (`dossier_paye` ; N4 ; Q7) |
| 16. Un retraité payé en pension de vieillesse (RPV) et en bonification pour enfant (RBEC) dans l'année | T28 : le dossier compte une fois, ligne « pension de vieillesse » ; la bonification ne le fait pas compter en plus dans « autres prestations » | Compté (A1 ; N1 ; N4) |
| 17. Une pension de retraite anticipée (RPRA) contrôlée en 2025 | T28, ligne « pension de retraite anticipée » demandée par le recueil, et non avec la vieillesse | Compté (N1) |

---

## 4. Face à l'annuaire CNSS 2024

| Tableau de l'annuaire | Ce qu'on compare | Nous | Publié | Écart | Verdict | Ce que ça change |
|---|---|---|---|---|---|---|
| Liste des tableaux 1 à 126 (p. vi à xi) | Trop-perçus, moins-perçus, indus, contrôles a posteriori des dossiers | Indus 2024 : 0,39 % ; contrôle 2025 : 0,07 % | rien | — | pas publié | Les indus et les contrôles ne se vérifient pas par l'annuaire. La CNSS les suit par la DAI (feuilles 05 et 06 ; « Fichiers Excel (DAI) » dans le recueil) |
| Tableaux 28 et 34 (p. 22 et 28) | Payées PVID de 2024 (T31), avec les deux dates du réglage `date_montant` | 47 237 434 040 au mois payé ; 47 486 010 786 à la saisie (P105) | 46 629 625 556 | +1,30 % (+607 808 484) ; +1,84 % (+856 385 230) | précise | R3 tient pour les pensions. Au trimestre, la saisie suit la CNSS (+1,5 à +2,1 %), le mois payé non (−0,5 à +4,3 %). Au mois, la saisie reproduit le rythme publié (corrélation 0,75 contre −0,12) sans réduire l'écart moyen (6,4 % contre 6,2 %). Taux d'indus PVID inchangé (0,47 %) |
| Tableaux 28 et 34, lignes à zéro (p. 22 et 28) | « Solidarité » (une allocation), « Pension gratuite », « Remboursements » | Aucun code de solidarité (P34) ; mois de pension gratuits seulement de 2010 à 2015 (P11) | 0, chaque mois et dans chaque direction | aucun | confirme | Conforte la décision de Kaya 2017 de supprimer la ligne « pension de solidarité » de T28 (N1). La pension gratuite, sans effet depuis 2016, irait dans les payées si elle revenait (à confirmer) |
| Tableaux 18, 22 et 23 (p. 16 à 19) | Pension de retraite anticipée au 31/12/2024 (ligne de T28) | 87 pensions en cours (A01) | 78 (66 à 78 de 2020 à 2024 ; 3 nouvelles en 2024) | +9 (+11,5 %) | précise | La ligne à part existe dans les effectifs publiés, comme le demande le recueil (N1). Notre chiffre a un écart chaque année (2022 : 78 pour 68). Dans les sommes servies (tableaux 28 et 34), l'anticipée est fondue dans la vieillesse |
| Tableaux 40 et 41 (p. 32 et 33) | Où va la maternité | Maternité (M) avec les PF (R1) | Indemnités de maternité, allocations prénatales et frais médicaux dans les PF ; tableau 106 : même total | aucun | confirme | R1 confirmée pour T31 et T30 |
| Tableaux 40 et 41 (p. 32 et 33) | Payées PF de 2024 (T31), maternité comprise | 10 750 873 342 au mois payé ; 10 802 063 700 à la saisie (P105) | 11 180 191 425 | −3,84 % (−429 318 083) ; −3,38 % (−378 127 725) | précise | Au mois payé, ligne par ligne (P98) : allocations familiales −3,4 %, indemnités de maternité −4,2 %, prénatales −4,0 %. Les frais médicaux (25 030 869, aucun dans la base en 2024, P95) n'expliquent que 0,2 point. Au mois, la saisie suit bien mieux (écart moyen 3,4 % contre 6,6 %) |
| Tableaux 90, 96 et 97 (p. 71 et 74) | Payées AT/MP de 2024 (T31) face aux espèces | 744 609 484 au mois payé ; 790 138 121 à la saisie (P105) | 934 445 128 en espèces | −20,3 % (−189 835 644) ; −15,4 % (−144 307 007) | écart à expliquer | Les payées AT/MP sont trop basses : le taux d'indus AT/MP vaut 0,47 % sur nos payées, 0,38 % sur les espèces publiées. Écart moyen au mois : 26,6 % (mois payé), 19,6 % (saisie) |
| Tableaux 82, 98, 99 et 106 (p. 63, 75, 80) | Payées AT/MP avec les soins remboursés (comme T56) face à l'ensemble | 763 397 429 (paiements + soins remboursés, P95) | 1 074 113 823 (espèces + soins) | −28,9 % (−310 716 394) | écart à expliquer | Taux d'indus AT/MP de 0,33 % sur l'ensemble publié. Savoir où sont payés les soins et une partie des rentes (question à la DSI déjà ouverte, README) ; N7, Q22 |
| Tableaux 90, 96 et 97, colonne « rachat de rente » | Rachats de rente de 2024 (ligne de T29, payées AT/MP) | 21 519 361 (11 débours de rachat, P104) | 25 366 909 | −15,2 % (−3 847 548) | écart à expliquer | La CNSS compte le rachat dans les prestations servies : ligne « achat de rente » de T29 (N2) ; dans les payées de T31 si la CNSS le confirme (Q22). Pas un indu. L'écart reste à expliquer (date du rachat ?) |
| Tableau 106 (et 103 à 105) (p. 79 et 80) | Total des payées de 2024, trois branches | 58 732 916 866 au mois payé ; 59 078 212 607 à la saisie (P105) | 58 883 930 804 (sans l'action sanitaire et sociale) | −0,26 % (−151 013 938) ; +0,33 % (+194 281 803) | précise | Juste seulement par compensation : pensions +1,3 %, PF −3,8 %, AT/MP −30,7 % face à la ligne RP, qui comprend les soins. Taux d'indus 0,391 % sur nos payées, 0,390 % sur le publié. L'annuaire met ses données en accord avec le compte de résultat (méthodologie, p. xiii et xiv) : ce que cela dit de la date d'un paiement est à vérifier avec la DPC et la DFC |
| Tableaux 35 (p. 30) et 39 (p. 32) | Bénéficiaires d'indemnités de maternité de 2024 (ligne de T30) | 3 890 personnes liquidées ; 4 881 personnes payées (P93) | 3 893 | −0,1 % (liquidées) ; +25 % (payées) | précise | La CNSS compte ces bénéficiaires l'année de la liquidation. Avec N4, la ligne des indemnités de maternité de T30 serait bien plus haute (Q7) |
| Tableau 24 (p. 19) | Allocations de vieillesse de 2024 (ligne « allocation de vieillesse » de T28) | 832 liquidées (831 personnes) ; 753 personnes payées (P93) | 833 | −1 (liquidées) ; −80 (payées) | précise | Même pratique : compté à la liquidation (Q7) |
| Tableaux 37 et 38 (p. 30 et 31) | Allocataires de 2024 (ligne « allocations familiales » de T30) | 104 611 payés dans l'année (N4) ; 98 048 avec un enfant en droit au 31/12 et payé dans l'année (P79) | 97 378 au 31/12 | +7,4 % (+7 233) ; +0,7 % (+670) | précise | La CNSS compte un stock au 31/12, qui diffère de N4. Avec N4, environ 104 600 dossiers payés en 2024 : il faudrait environ 5 230 contrôles dans l'année pour atteindre 5 % |
| Tableaux 18 et 22 (p. 16 et 18), lus en entier | Pensions au 31/12/2024 face aux dossiers de pension payés | 47 732 dossiers de pension payés en 2025 (P13) | 62 231 pensions | — | non comparable | Une pension n'est pas un dossier : veuve et orphelins sont payés sur le dossier du défunt, et 2 901 pensions de vieillesse de coordination ne sont pas payées par la CNSS (P78). Utile pour Q4 (dossier ou prestation contrôlée) et pour le dénominateur de T28 |
| Tableaux 79 et 80 (p. 61 et 62), avec 76 et 78 | Rentes AT/MP au 31/12/2024 face aux dossiers AT/MP payés | 2 134 dossiers AT/MP avec un débours en 2025 (P13) | 2 232 rentes (1 255 viagères, 977 de survivants : 283 veuves, 2 veufs, 485 orphelins, 207 ascendants) | — | non comparable | Un ordre de grandeur seulement pour les lignes de rentes de T29 : autre année, rentes et non dossiers |
| Tableaux 29 à 34, 40, 42 à 46, 91 à 96 et 98 | Sommes servies par direction régionale | pas encore calculé | publié pour les trois branches | — | précise | De quoi contrôler les payées par direction, dénominateur du taux d'indus par direction (N9) |
| Tableau 26 (p. 21) | Décès de pensionnés de 2024 | 444 décès de retraités connus fin mars 2025 (P99) | 447 décès de retraités de vieillesse ; 648 pensionnés décédés en tout | −3 (−0,7 %) | précise | Indice fort pour R13 (cliché de fin mars N+1), pas une preuve : la base n'a pas de date de déclaration du décès. Même verdict que la famille T50-T57 pour ce chiffre. Repère pour Q18 : 175 décès de retraités de 2024 déclarés après mars 2025, 7 trop-perçus après un décès |
| Tableaux 100 et 101 (p. 76) | « Entreprises contrôlées » en 2024 | 115 contrôles de dossiers en 2025 (P13) | 178 entreprises (DPASS 107, DRB 71) | sans objet | non comparable | Des contrôles de prévention des accidents dans les entreprises, pas des contrôles de dossiers : ne pas les confondre avec ceux de la DAI |
| Tableaux 102 (p. 77) et 11 (p. 11) | Comités de santé et de sécurité au travail ; employeurs contrôlés | — | 16 comités renouvelés, 23 nouveaux ; 8 794 employeurs contrôlés | sans objet | non comparable | Hors famille : le tableau 11 sert au recouvrement (contrôles d'employeurs, T14 et T15 de l'Annuaire CIPRES) |
| Tableaux 107 à 113 (p. 80 à 83), avec le tableau 8 (p. 8) | Délai de remise en banque des titres de paiement | — | 38 413 titres en 2024, dont 5 568 remis en moins d'un jour (14,5 %) | sans objet | non comparable | Un indicateur du recouvrement (modèle : T71 de l'Annuaire CIPRES) : le délai d'encaissement des chèques des cotisants. Leurs pics de janvier, avril, juillet et octobre sont ceux des cotisations trimestrielles (tableau 8 : 15,3 milliards FCFA en janvier). Rien pour cette famille |

**En clair, ce que l'annuaire confirme :**
- la maternité est dans la branche des prestations familiales (R1) ;
- la CNSS ne sert aucune prestation de solidarité : la ligne disparaît de T28, comme l'avait décidé Kaya 2017 (N1).

**Ce qu'il précise sans trancher :** le cliché de fin mars (R13, indice fort : tableau 26) ; la date des payées (R4 : la saisie suit ses trimestres et le rythme de ses mois, sans réduire l'écart moyen des pensions) ; la ligne à part de la retraite anticipée (effectifs seulement) ; ce qu'est un dossier payé (indemnités de maternité et allocations de vieillesse comptées à la liquidation ; allocataires en stock au 31/12) ; le total des payées (juste par compensation) ; le découpage par direction.

**Ce qu'il contredit :** rien dans nos règles. L'écart des payées AT/MP contredit seulement le fait de lire R3 comme confirmée pour toutes les branches.

**Ce qu'il faut expliquer :** les payées AT/MP (−20,3 % sur les espèces, −28,9 % soins compris) ; les rachats de rente (−15,2 %).

**Ce qu'il ne publie pas :** les contrôles a posteriori, les trop-perçus, les moins-perçus, les taux. Les tableaux 11, 100 à 102 et 107 à 113 parlent d'autres contrôles et d'autres paiements.

---

## 5. Les réglages

Ils s'écrivent dans la CTE `parametres`, le bloc de réglages en tête de chaque requête. Changer une valeur ne demande pas de réécrire la requête. Une décision attendue de la CNSS ne bloque rien : la valeur retenue s'applique en attendant. « À créer » : le choix est aujourd'hui écrit en dur dans la requête, ou n'existe pas. La requête de T28 à T30 reste à écrire : ses réglages sont proposés dès maintenant.

| Réglage | Requête | Valeur retenue | Autre valeur | Ce que ça change | Qui décide |
|---|---|---|---|---|---|
| `annee`, `debut`, `fin` | t31 (tirés de l'exercice donné au lancement) | l'année demandée, du 01/01 au 31/12 | — | Bornent les paiements et les ajustements de l'exercice | l'utilisateur, à chaque lancement |
| `periode` (à créer) | t31 ; requête T28 à T30 | MOIS pour T31 (12 mois et le TOTAL), TRIMESTRE pour T28 à T30 ; un trimestre ou un semestre additionne ses mois, taux recalculé sur les sommes | une période libre (date de début, date de fin) | Permet T31 au trimestre et T28 à T30 au semestre (N6) | tranché par le recueil (l. 136 et 146) |
| Le cliché (choisi au lancement de la requête, pas un réglage de `parametres`) | t31 | la copie de fin mars N+1 (R13) ; aujourd'hui, la seule copie, du 02/02/2026 | le dernier cliché chargé | Les ajustements établis en fin d'année mais saisis après coup comptent | BI, avec la DSI |
| `date_montant` | t31 (même réglage dans t32_t41, t42_t49, t50_t57) | 'MOIS' : le mois payé (`DEB_DATE_EFFET`) | 'SAISIE' : le jour de la saisie (`DEB_DATE_INSERT`), rappels compris | 2024 : payées 58 732 916 866 ou 59 078 212 607 FCFA ; taux d'indus 0,39 % dans les deux cas. Avec 'MOIS', des mois réguliers ; avec 'SAISIE', des pics en février, mai, août et novembre, comme l'annuaire | DCP, pour la règle retenue (R4, point du 02/11/2026) ; la DPC dit comment l'annuaire range un paiement (Q24) |
| `statut_ecarte` (à créer ; en dur : `AJ_STATUT <> 'R'`) | t31 | R (rejeté ou annulé) écarté | aucun statut écarté | Aucun effet sur 2015 à 2025 : le statut R n'apparaît plus (P103) | DSI (Q25) |
| `rem_tp_deduit` (à créer ; en dur : non) | t31 | NON : trop-perçus bruts | OUI : trop-perçus moins leurs remboursements (REM-TP) | 2024 : 13 024 760 FCFA de remboursements ; taux 0,39 % brut, 0,37 % net | DCP, avec la DAI (Q19) |
| `payees_types` (à créer ; en dur : 'PM') | t31 | PM : les débours de paiement seulement | PM et RB (soins remboursés), comme T56 ; en plus, les rachats de rente (RR) ; en plus, les rappels de moins-perçus et les avances versés par ajustement | AT/MP 2024 : 744 609 484, puis 763 397 429 avec les soins, puis 784 916 790 avec les rachats ; taux AT/MP 0,47 %, 0,46 %, 0,45 %. Rappels et avances : 781 519 116 FCFA de plus en 2024 | DCP, avec la DPC (Q22, Q23) |
| `taux_numerateur` (à créer) | t31 (N8) | 'TP' : trop-perçus seuls (Kaya 2017) | 'TP+MP' : trop-perçus et moins-perçus | 2024 : 0,39 % ou 1,10 % (pensions 0,47 % ou 1,04 % ; PF 0,045 % ou 1,35 % ; AT/MP 0,47 % ou 0,93 %) | DAI, avec la DCP (Q16) |
| `taux_denominateur` (à créer) | t31 (N8) | 'PAYEES' : toutes les prestations payées de la période (recueil v7) | 'CONTROLES' : le montant des dossiers contrôlés (feuille 06) | Pas calculable aujourd'hui : le montant des dossiers contrôlés n'est dans aucune source ; il faudrait l'ajouter à la vue des contrôles | DAI (Q15) |
| `source_indus` (à créer) | t31 (N10) | 'BASE' : table `AJUSTEMENT` | 'DAI' : fichier Excel de la DAI chargé dans une table | Avec la base, tous les trop-perçus établis par les services ; avec le fichier, peut-être les seuls indus trouvés par les contrôles | DAI, avec la DCP (Q14) |
| `source_controles` (à créer) | requête T28 à T30 (N11) | `V_BI_CONTROLE_POSTERIORI` (vue proposée le 07/11/2026) | `DOSSIERS_CONTROLES` (table de l'application, incomplète, arrêtée au 09/07/2025) | Sans la vue, T28 à T30 n'ont aucun contrôle après juillet 2025, et presque aucun avant | DSI et DAI (Q1 à Q3) |
| `denominateur_controle` (à créer) | requête T28 à T30 (C1) | 'PAYES' : dossiers payés dans la période (Annuaire CIPRES, recueil v7) | 'PORTEFEUILLE' : dossiers liquidés ou en portefeuille (feuille 05) | À mesurer ; l'écart vient des dossiers en cours non payés dans la période (suspendus, coordination) | DAI (Q6) |
| `dossier_paye` (à créer) | requête T28 à T30 (N4) | 'PAIEMENT' : au moins un paiement dans la période | 'LIQUIDATION' : pour les prestations versées une fois ou sur quelques mois, le dossier liquidé dans la période, comme l'annuaire | Indemnités de maternité 2024 : 4 881 payées, 3 890 liquidées | DAI, avec la DCP (Q7) |
| `payes_depuis` (à créer) | requête T28 à T30 (N6) | CUMUL : du 1er janvier à la fin de la période | TRIMESTRE : le seul trimestre | En cumul, le taux du 4e trimestre est celui de l'année ; sur le seul trimestre, chaque trimestre a son taux | DAI, avec la DCP (Q8) |
| `controle_compte` (à créer) | requête T28 à T30 (N5) | 'DOSSIERS' : dans le taux, un dossier contrôlé compte une fois dans la période (la colonne TOTAL reste la somme des trimestres) | 'CONTROLES' : chaque contrôle compte, comme la colonne TOTAL | Change le taux dès qu'un dossier est contrôlé plusieurs fois ; à mesurer | DAI (Q9) |
| `norme_controle`, `norme_indus` (à créer) | requête T28 à T30 ; t31 (N12) | 5 % au moins ; 2 % au plus | d'autres normes | Mention « dans les normes » ou « hors normes » | DAI |
| `decimales` (à créer ; en dur : 3) | t31 ; requête T28 à T30 (N12) | 3 chiffres après la virgule | 2, comme le tableau 76 | PF 2024 : 0,045 % ou 0,04 % | DAI (Q30) |
| `agence` (à créer) | t31 ; requête T28 à T30 (N9) | SERVICE : le service du dossier | LIEU_PAIEMENT : le lieu de paiement | Le découpage des taux par agence | DSI (Q28) |

---

## 6. Les questions ouvertes

Chaque question a une valeur par défaut dans la requête : la réponse change un réglage, pas le travail. Les destinataires suivent le recueil v7 : la DAI et les DR pour les ratios des PF (l. 1119, 1128) ; les DR et la DCP pour ceux des AT/MP (l. 1198, 1209) ; aucune structure n'est écrite pour les pensions, on y met la DAI avec la DCP. Sigles : DAI (Direction de l'audit interne) ; DCP (Direction centrale des prestations) ; DR (directions régionales) ; DSI (Direction des systèmes d'information) ; DPC (Direction de la prospective et de la coopération, qui fait l'annuaire ; son Service études et statistique) ; DCR (Direction centrale du recouvrement) ; DPASS (Direction de la prévention et de l'action sanitaire et sociale) ; DES (sigle du recueil, ou la direction qui l'a remplacée) ; DFC (sigle du recueil, cité pour la comptabilité).

### Q1 — Depuis juillet 2025, où sont enregistrés les contrôles a posteriori des dossiers de prestations ?
**À qui :** DAI, avec la DSI.
**En attendant :** T28 à T30 ne sont pas produits.
**Pourquoi on la pose :** la table des contrôles n'a plus de saisie depuis le 09/07/2025 : 115 contrôles en 2025 pour 155 206 dossiers payés (0,07 %, norme 5 %). Le recueil v7 cite les fichiers Excel de la DAI.
**Déjà posée :** README (questions à la DSI) ; points_a_valider n° 29 ; planche 02 « vues cnss », question 1 ; carte C1.

### Q2 — La table des contrôles de l'application a-t-elle un jour contenu tous vos contrôles ?
**À qui :** DAI.
**En attendant :** la table ne sert pas, même pour le passé.
**Pourquoi on la pose :** 1 542 lignes en tout, de 1 à 212 contrôles par an et par branche. Pour atteindre 5 % des 155 206 dossiers payés de 2025, il en faudrait environ 7 760 dans l'année.
**Déjà posée :** nouvelle.

### Q3 — Pouvez-vous nous donner vos contrôles des cinq derniers exercices, un par ligne : le dossier, la prestation contrôlée, la date, le résultat et le montant de l'indu trouvé ?
**À qui :** DAI, avec la DSI.
**En attendant :** rien : la vue `V_BI_CONTROLE_POSTERIORI` est seulement proposée.
**Pourquoi on la pose :** c'est la seule façon de remplir T28 à T30 et les trois ratios de contrôle du recueil.
**Déjà posée :** planche 02 « vues cnss » (proposition du 07/11/2026), étendue ici à cinq exercices.

### Q4 — Un contrôle porte-t-il sur tout le dossier, ou sur une seule prestation du dossier ?
**À qui :** DAI.
**En attendant :** la ligne du tableau est celle de la prestation contrôlée.
**Pourquoi on la pose :** un dossier peut porter plusieurs prestations, par exemple la pension du défunt et celle de sa veuve (62 231 pensions au 31/12/2024 pour 47 732 dossiers de pension payés en 2025). La table des contrôles a une colonne de code de prestation, jamais mesurée.
**Déjà posée :** planche 02 « vues cnss », question 2.

### Q5 — Quels résultats peut avoir un contrôle (conforme, anomalie, en cours…) ?
**À qui :** DAI.
**En attendant :** tout contrôle enregistré compte, quel que soit son résultat.
**Pourquoi on la pose :** le taux de contrôle compte des contrôles ; le résultat sert à la grille des indus (feuille 06) et à comparer avec T31.
**Déjà posée :** planche 02 « vues cnss », question 3.

### Q6 — Pour le taux de contrôle, divise-t-on par les dossiers payés dans la période, ou par les dossiers « liquidés ou en portefeuille » de votre grille de 2025 ?
**À qui :** DAI, avec la DCP et les DR.
**En attendant :** les dossiers payés (`denominateur_controle` = 'PAYES').
**Pourquoi on la pose :** trois mots différents : « dossiers payés » (Annuaire CIPRES ; recueil, T30), « dossiers liquidés et payés » (formule du recueil), « liquidés ou en portefeuille » (feuille 05). Un dossier de coordination payé par l'autre caisse est en portefeuille, pas payé.
**Déjà posée :** nouvelle (la fiche de lecture, terme « ratio », pose déjà « dossiers payés »).

### Q7 — Pour les prestations versées une fois ou sur quelques mois (indemnités journalières, allocations prénatales, allocations de vieillesse), le dossier payé de T28 à T30 est-il celui payé dans la période, ou celui liquidé dans la période ?
**À qui :** DAI, avec la DCP.
**En attendant :** payé dans la période (`dossier_paye` = 'PAIEMENT').
**Pourquoi on la pose :** votre annuaire compte 3 893 bénéficiaires d'indemnités de maternité en 2024 : ce sont les 3 890 liquidées, pas les 4 881 payées ; de même 833 allocations de vieillesse (832 liquidées, 753 payées).
**Déjà posée :** nouvelle.

### Q8 — Quand on produit T28 à T30 pour un trimestre ou un semestre, compte-t-on les dossiers payés depuis le 1er janvier ?
**À qui :** DAI, avec la DCP.
**En attendant :** oui, depuis le 1er janvier ; les contrôles, eux, par trimestre.
**Pourquoi on la pose :** le recueil demande T28 à T30 au trimestre, au semestre et à l'année, mais la maquette n'a qu'une colonne de dossiers payés.
**Déjà posée :** nouvelle.

### Q9 — Dans le taux de contrôle, un dossier contrôlé deux fois dans l'année compte-t-il deux fois ?
**À qui :** DAI.
**En attendant :** une fois dans le taux ; deux fois dans la colonne TOTAL, qui additionne les trimestres (`controle_compte` = 'DOSSIERS').
**Pourquoi on la pose :** le recueil dit « TOTAL : total des trimestres », mais sa formule compte des « dossiers contrôlés après paiement ».
**Déjà posée :** nouvelle (la fiche de lecture compte une fois un dossier contrôlé deux fois dans le même trimestre).

### Q10 — Qui peut voir, dans la BI, le résultat des contrôles et le montant d'indu trouvé pour chaque dossier ?
**À qui :** DAI, avec la DSI.
**En attendant :** seuls les taux sont diffusés ; le détail des contrôles reste réservé (N14).
**Pourquoi on la pose :** le recueil demande un accès par profil (l. 72) ; ces données touchent des assurés nommément.
**Déjà posée :** nouvelle.

### Q11 — L'« achat de rente » ajouté à T29 par l'atelier de Kaya est-il le rachat de rente, un capital versé à la place de la rente ?
**À qui :** DCP, avec les DR.
**En attendant :** oui : la ligne compte les dossiers qui ont reçu un rachat de rente.
**Pourquoi on la pose :** la base et l'annuaire parlent de « rachat » (11 rachats et 21 519 361 FCFA dans la base en 2024 ; 25 366 909 publiés) ; personne n'a vérifié que c'est ce que Kaya appelait « achat ».
**Déjà posée :** nouvelle (voisine de T25-T27 Q11, sur le rachat dans T26).

### Q12 — Dans T30, la ligne « Autres prestations de la branche » devient-elle « Frais médicaux de maternité », comme dans T27 ?
**À qui :** DCP, avec la DES.
**En attendant :** oui (N3) ; ligne vide, faute de ces frais dans la base.
**Pourquoi on la pose :** le recueil le demande pour T27 seulement ; l'annuaire publie 25 030 869 FCFA de frais médicaux de maternité en 2024.
**Déjà posée :** nouvelle pour T30 (pour T27 : recueil l. 127 ; remarque B27 du recueil ; T25-T27 Q15 pour la source des données).

### Q13 — Pour la CIPRES, faut-il aussi garder la maquette d'origine de T28 à T30 (avec la pension de solidarité, sans la ligne de retraite anticipée) ?
**À qui :** DCP, avec la DPC.
**En attendant :** la version de Kaya 2017 et du recueil ; la maquette d'origine se refait en additionnant les lignes.
**Pourquoi on la pose :** Kaya et le recueil changent les tableaux CIPRES eux-mêmes ; rien ne dit si la CIPRES accepte ces changements.
**Déjà posée :** T25-T27 Q22 (même question pour T25 et T27).

### Q14 — Le tableau des indus doit-il prendre tous les trop-perçus enregistrés dans l'application des prestations, ou seulement ceux trouvés par les contrôles de la DAI ?
**À qui :** DAI, avec la DCP.
**En attendant :** tous les trop-perçus de l'application (`source_indus` = 'BASE' ; 0,39 % d'indus en 2024).
**Pourquoi on la pose :** le recueil donne les fichiers Excel de la DAI comme source de T31 ; la base a pourtant tous les trop-perçus, mois par mois et par branche.
**Déjà posée :** planche 02 « vues cnss », question 4 ; remarque B13 du recueil.

### Q15 — Le taux d'indus se calcule-t-il sur toutes les prestations payées de la période, ou sur le seul montant des dossiers contrôlés ?
**À qui :** DAI.
**En attendant :** sur toutes les prestations payées (`taux_denominateur` = 'PAYEES'), formule du recueil.
**Pourquoi on la pose :** votre grille de 2025 (feuille 06) divise par le « montant des dossiers contrôlés » ; le recueil et Kaya 2017 par les « prestations payées sur la période ».
**Déjà posée :** nouvelle.

### Q16 — Les moins-perçus (sommes payées en moins, rattrapées par un rappel) entrent-ils dans le taux d'indus ?
**À qui :** DAI, avec la DCP.
**En attendant :** non : ils sont montrés à part, avec le taux des deux ensemble à côté (2024 : 0,39 % sans eux, 1,10 % avec).
**Pourquoi on la pose :** Kaya 2017 dit que les moins-perçus ne sont pas des indus ; votre grille de 2025 additionne trop-perçus et moins-perçus ; l'Annuaire CIPRES parle d'indus en trop ou en moins.
**Déjà posée :** fiche T31 Q1 ; README (questions à la direction des prestations) ; points_a_valider n° 17.

### Q17 — Un trop-perçu se compte-t-il le mois où il est établi ?
**À qui :** DCP.
**En attendant :** oui, le mois d'établissement.
**Pourquoi on la pose :** la base n'a pas la période payée en trop ; seule la date d'établissement est toujours remplie.
**Déjà posée :** fiche T31 Q2 ; README (questions à la direction des prestations).

### Q18 — Une pension versée après un décès déclaré en retard devient-elle un trop-perçu ?
**À qui :** DCP.
**En attendant :** on ne compte que les trop-perçus enregistrés.
**Pourquoi on la pose :** 7 trop-perçus après un décès en 2024, pour 175 décès de retraités de 2024 déclarés après mars 2025.
**Déjà posée :** fiche T31 Q3 ; README ; points_a_valider n° 30.

### Q19 — Comment voit-on qu'un trop-perçu a été remboursé ?
**À qui :** DCP, avec la DSI.
**En attendant :** le remboursement n'entre pas dans T31 (`rem_tp_deduit` = NON).
**Pourquoi on la pose :** aucun trop-perçu n'est marqué « soldé » depuis 2017 : ils restent « en cours », repris par retenues. Le DTM lit pourtant « soldé » comme « recouvré ».
**Déjà posée :** fiche T31 Q5 ; points_a_valider n° 31.

### Q20 — Pourquoi les moins-perçus des prestations familiales ont-ils presque disparu ?
**À qui :** DCP.
**En attendant :** on les compte tels qu'ils sont.
**Pourquoi on la pose :** 56 362 en 2023, 17 126 en 2024, 1 384 en 2025.
**Déjà posée :** README (questions à la direction des prestations) ; points_a_valider n° 32.

### Q21 — Pourquoi les trop-perçus sur convention entre caisses passent-ils de 15 en 2024 à 758 en 2025 ?
**À qui :** DCP.
**En attendant :** comptés comme les autres trop-perçus, colonne PVID.
**Pourquoi on la pose :** 54 771 312 FCFA en 2025 contre 12 135 453 en 2024 ; à comprendre avant de publier T31 de 2025.
**Déjà posée :** nouvelle (le constat est au README, lot 9).

### Q22 — Les « prestations payées » de T31 doivent-elles égaler les prestations servies de votre annuaire, soins des accidents du travail, rachats de rente et frais médicaux de maternité compris ?
**À qui :** DCP, avec la DPC.
**En attendant :** les débours de paiement seulement (`payees_types` = PM).
**Pourquoi on la pose :** AT/MP 2024 : 744 609 484 FCFA de paiements dans la base, 934 445 128 publiés en espèces, 1 074 113 823 avec les soins (tableaux 90 et 99).
**Déjà posée :** points_a_valider n° 52 (rachats et rappels, pour T56).

### Q23 — Les rappels de moins-perçus et les avances, versés par un ajustement et non par un débours de paiement, font-ils partie des prestations payées de T31 ?
**À qui :** DCP, avec la DAI.
**En attendant :** non.
**Pourquoi on la pose :** 408 312 511 FCFA de rappels et 373 206 605 FCFA d'avances en 2024 (P104) ; les rappels sont bien des sommes décaissées au profit de l'assuré, au sens de Kaya 2017.
**Déjà posée :** fiche T31 Q4 (deuxième partie) ; points_a_valider n° 33.

### Q24 — Dans vos tableaux de prestations servies mois par mois, un paiement est-il rangé dans le mois qu'il paie, le mois où il est saisi, ou le mois où il est réellement versé ?
**À qui :** DPC (Service études et statistique).
**En attendant :** le mois payé (`date_montant` = 'MOIS').
**Pourquoi on la pose :** les mois de l'annuaire 2024 ont un rythme par trimestre (janvier, avril, juillet, octobre plus bas) que le mois payé ne donne pas ; la saisie le donne presque.
**Déjà posée :** points_a_valider n° 1 ; README, R4 (point du 02/11/2026) ; la partie « mois par mois » est nouvelle. La même question est reprise par T32-T41 Q2 et T50-T57 Q1 : une seule réponse vaut pour tout le domaine.

### Q25 — Le statut R d'un ajustement veut-il dire « annulé » ?
**À qui :** DSI.
**En attendant :** écarté ; sans effet depuis 2015.
**Pourquoi on la pose :** les statuts n'ont pas de libellé dans la base ; R n'apparaît plus depuis 2014.
**Déjà posée :** fiche T31 Q5 ; points_a_valider n° 17 et 58.

### Q26 — Parmi les paiements liés à un trop-perçu, lesquels sont des retenues, lesquels des versements ?
**À qui :** DSI.
**En attendant :** T31 lit le montant établi du trop-perçu, pas ces paiements.
**Pourquoi on la pose :** les débours liés à un trop-perçu ont les deux signes, de −6,1 à +6,6 millions FCFA en 2024 (P104).
**Déjà posée :** README (questions à la DSI) ; points_a_valider n° 61.

### Q27 — Le recueil demande les taux d'indus par type de prestation, mais un trop-perçu n'est relié à aucune prestation dans la base : la branche suffit-elle ?
**À qui :** DCP, avec la DSI.
**En attendant :** par branche seulement.
**Pourquoi on la pose :** les 684 trop-perçus de 2024 n'ont aucune prestation liée (P106).
**Déjà posée :** nouvelle.

### Q28 — L'agence d'un dossier est-elle bien son service dans l'application ?
**À qui :** DSI, avec la DPC.
**En attendant :** direction régionale et service du dossier (`agence` = SERVICE).
**Pourquoi on la pose :** le recueil veut les ratios par direction régionale et par agence ; la base a 5 directions régionales (et un siège inactif), 20 services et 350 lieux de paiement (P45 à P47).
**Déjà posée :** T25-T27 Q20 ; remarque B34 du recueil.

### Q29 — Le taux de prestations indues rangé aussi sous le recouvrement (recueil, l. 1608) est-il suivi par la DCR ou par la DAI ?
**À qui :** DCR, avec la DAI.
**En attendant :** un seul calcul, celui de la l. 1369.
**Pourquoi on la pose :** le même indicateur figure sous deux objectifs, avec deux responsables ; Kaya 2017 le rangeait déjà sous le recouvrement.
**Déjà posée :** nouvelle.

### Q30 — Les taux de contrôle et d'indus s'affichent-ils avec 2 chiffres après la virgule, comme le tableau 76 ?
**À qui :** DAI.
**En attendant :** 3 chiffres (`decimales` = 3), comme la requête t31.
**Pourquoi on la pose :** le recueil ne fixe l'arrondi que pour le tableau 76 (l. 333) ; avec 2 chiffres, le taux PF de 2024 s'écrirait 0,04 % au lieu de 0,045 %.
**Déjà posée :** nouvelle (remarque B35 du recueil, pour T69, T70, T76 et SP1).

---

## 7. Les points d'attention

1. **L'annuaire CNSS 2024 ne publie ni contrôles a posteriori ni indus.** Ces chiffres ne se valident qu'avec la DAI. L'annuaire sert à contrôler les payées (tableaux 28 à 34, 40 à 46, 82, 90 à 99, 106) et à voir comment la CNSS compte ses bénéficiaires (tableaux 24, 35, 37, 39).
2. **Le conflit le plus important porte sur le taux d'indus.** La grille de la CNSS (feuille 06) le calcule sur les seuls dossiers contrôlés et y met les moins-perçus ; le recueil divise par toutes les prestations payées ; Kaya 2017 exclut les moins-perçus. La requête t31 suit le recueil et Kaya : à faire trancher par la DAI (Q15, Q16).
3. **Le recueil donne les fichiers de la DAI comme source de T28 et de T31.** T31 lit la base : c'est un écart à une source plus forte, gardé en attendant la DAI (N10, Q14). T29 et T30 n'ont pas de ligne « Source » : on suppose la même source pour les trois tableaux de contrôle.
4. **La table des contrôles est bien trop maigre** (1 542 lignes en tout, 212 contrôles par an et par branche au plus, quand 5 % des dossiers payés de 2025 en demanderaient environ 7 760) et ne reçoit plus rien depuis le 09/07/2025. Sans les contrôles de la DAI, T28 à T30 restent vides, même pour le passé (Q1 à Q3).
5. **Le dénominateur de 0,07 % est à recompter.** Les 155 206 dossiers payés de 2025 comptent les dossiers qui ont un débours de n'importe quel type et statut (P13), alors que la requête de la fiche de lecture dit paiement PM, statut D ou P. On ne sait pas non plus si les 115 contrôles sont des lignes ou des dossiers distincts, et 2025 n'a des contrôles que jusqu'au 9 juillet. Le taux restera très loin des 5 %.
6. **Le total des payées tombe juste par compensation.** −0,26 % sur l'ensemble, mais pensions +1,3 %, PF −3,8 %, AT/MP −20,3 % (espèces) et −28,9 % (soins compris). T31 applique R3 branche par branche : le taux d'indus AT/MP est gonflé (0,47 % sur nos payées, 0,38 % sur les espèces publiées, 0,33 % sur l'ensemble publié).
7. **T31 et T56 ne prennent pas les mêmes paiements** : T31 les seuls débours PM, T56 aussi les soins remboursés (RB). S'y ajoutent les rachats, les rappels et les avances versés par ajustement (N7, Q22, Q23).
8. **Mois par mois, aucune date ne retombe sur l'annuaire** (écart moyen de 6 % pour les pensions, quelle que soit la date) : pour un tableau mensuel comme T31, ne pas trancher R4 sur les seuls totaux de l'année. Une date de saisie n'est pas une date de versement.
9. **La norme de 2 % est tenue en 2024 dans toutes les lectures calculables** : 0,39 % (trop-perçus seuls), 1,10 % (avec les moins-perçus), 0,39 % sur les prestations servies publiées. Seule la lecture de la grille, sur les dossiers contrôlés, pourrait donner bien plus ; elle ne se calcule pas aujourd'hui. Un mois peut dépasser 2 % à cause d'un seul trop-perçu (AT/MP, avril 2024 : 4,08 %) : la norme se lit sur l'année.
10. **Les trop-perçus sur convention passent de 15 en 2024 à 758 en 2025** : à faire expliquer avant de publier T31 de 2025 (Q21).
11. **Les statuts des ajustements ne disent plus rien du recouvrement** : aucun trop-perçu soldé depuis 2017, aucun statut R depuis 2014. Les colonnes « recouvré » et « irrécupérable » du DTM actuel sont donc fausses.
12. **Le dossier payé ne se compte pas comme l'annuaire.** La CNSS compte à la liquidation les indemnités de maternité et les allocations de vieillesse, et en stock au 31/12 les allocataires. N4 compte un paiement dans la période : la ligne des indemnités de maternité de T30 serait 25 % plus haute (Q7).
13. **Les DTM actuels sont faux pour cette famille et ne servent pas à valider.** H11 et H12, et en plus, à la lecture du code : le DTM des contrôles prend le statut F (fermé) pour « payé » ; le DTM des indicateurs met les moins-perçus et les remboursements dans le taux d'indus et divise par des débours de tous statuts, payés ou non ; le DTM des prestations compte la coche « vérifié » comme un contrôle et prend la direction sur le titre de paiement, alors que 62 % des paiements PF de 2025 n'en ont pas (P07).
14. **Les données sensibles.** La table des contrôles et les ajustements ont des textes libres que la BI ne lit pas ; la vue proposée ne demande ni nom ni numéro bancaire ; les feuilles 04 à 06 de la grille n'en contiennent pas (N14).
15. **La feuille 04 de la grille (restes à traiter) concerne T25 à T27.** Elle est déjà reprise par cette famille (T25-T27 N4 et Q19), avec son oubli du stock de début d'année dans la ligne des risques professionnels.
16. **Les documents existants, laissés tels quels :** la fiche de lecture garde la « pension de solidarité » et la demande ; la correspondance du README range RPRA avec la vieillesse et les accessoires en « autres prestations » ; la fiche de lecture compte des dossiers distincts par trimestre sans dire ce qu'est le TOTAL. À reprendre après validation.
17. **R13 et les faits du DWH :** si les faits du DWH ne gardent que le dernier cliché, comme pour la GRH, T31 de l'année N devra être calculé et gardé fin mars N+1 (à vérifier pour les faits des prestations).

---

## 8. Comment c'est vérifié

- **Sources relues pour ce document :** `besoins_v7.txt`, l. 4 à 10, 24 à 35, 51 à 78, 125 à 146, 178 à 184, 252 à 258, 326, 333, 1065, 1108 à 1128, 1188 à 1212, 1267 à 1286, 1357 à 1376, 1560 à 1614, 1978 à 2000 ; `kaya.txt`, l. 89 à 110, 164 à 177, 273 à 284, 875 à 884, 1340 à 1350 ; `annuaire_cipres.txt` (descriptions et glossaire) ; l'annuaire CNSS 2024, tableau 8, liste des tableaux 107 à 113 et méthodologie (p. xiii et xiv) ; `t31_requete_proposee.sql` (en-tête, CTE `parametres`, arrondi) ; fiche T31, fiche de lecture (T28 à T31), cartes I1, C1, A1, H11, H12 de `regles_prestations` ; `reference_cipres_T25_T57.md` (H11, H12) ; points_a_valider n° 1, 17, 29 à 33, 52, 58, 61 ; planche 02 « vues cnss ».
- **Recalculs depuis les résultats des lots :** P105 (payées, trop-perçus, moins-perçus et taux de chaque branche ; taux AT/MP sur les espèces et sur l'ensemble publiés : 0,38 % et 0,33 % ; taux global 1,10 %) ; P102 (684 trop-perçus, 229 484 759 FCFA ; 414 442 709 de moins-perçus ; 13 024 760 de remboursements ; trop-perçus sur convention 2025) ; P104 (rappels 408 312 511, avances 373 206 605) ; P93 (3 890 indemnités de maternité liquidées, 4 881 payées ; 832 allocations de vieillesse liquidées, 753 payées) ; P79 (104 611 et 98 048 allocataires) ; P98 (écarts ligne par ligne des PF) ; P78 (2 901 pensions de coordination) ; comparaison A01 (retraite anticipée, 2020 à 2024). Les autres chiffres de l'annuaire et des lots viennent du dossier de la famille, recalculés par sa contre-vérification (sommes, totaux et écarts justes).
- **Lots utilisés :** 1 (P01, P06, P07, P11, P13), 3 (P34, P35, P41, P45 à P47), 5 (P78), 6 (P79), 8 (P93, P95, P98, P99), 9 (P102 à P106) ; comparaison A01. Aucune requête nouvelle n'a été lancée ; aucune table DTM n'a été lue (seul leur code est cité).
- **Corrections reprises de la contre-vérification :**
  - *erreur* : tableaux 107 à 113 décrits comme un indicateur du recouvrement (encaissement des chèques des cotisants), sans lien avec R4 ; retraite anticipée, verdict « précise » et non « confirme » (+11,5 %, série publiée de 66 à 78, montants fondus dans la vieillesse), N1 fondée sur le recueil ; R3 « confirmée pour les pensions, à préciser pour les PF, non confirmée pour les AT/MP », tableau 106 « précise » (juste par compensation, AT/MP −30,7 %) ; T30 : 104 611 allocataires payés dans l'année (+7,4 %), et non 98 048, soit environ 5 230 contrôles pour 5 % ; recueil l. 135 « à intégrer » ; paiements comparés aux espèces (−20,3 %), paiements et soins à l'ensemble (−28,9 %), taux AT/MP 0,38 % et 0,33 % ; N4 dit quels paiements comptent pour chaque ligne de T29, question Q11 sur « achat de rente ».
  - *manque* : taux par branche absent de la requête t31 (fiche T31 r. 8, N9, partie 9) ; table des contrôles jamais complète (N11, Q2) ; bénéficiaires comptés à la liquidation (N4, Q7, réglage `dossier_paye`, cas 15) ; rappels et avances dans N7 et Q23, N7 marquée « Proposition BI ».
  - *imprécision* : taux de contrôle sur des dossiers distincts par défaut, TOTAL = somme des trimestres (N5, Q9) ; direction des payées prise sur le dossier, pas sur le titre de paiement (N9) ; « trop-perçus et moins-perçus rapportés à toutes les payées » au lieu de « taux de la grille » (N8) ; lignes à zéro des tableaux 28 et 34 : « confirme (décision de Kaya 2017) », pension gratuite sans effet depuis 2016 ; argument comptable retiré, méthodologie p. xiii et xiv, « à vérifier avec la DPC et la DFC » ; R4 réécrite (trimestre, mois, « saisi » et non « versé ») ; deux rôles pour `date_montant` (DCP décide, DPC renseigne) ; rachats « écart à expliquer » ; statuts « à intégrer » pour les l. 134 à 136, 146, 28, 34, 75, 53 à 55 et 60 ; lecture BI sur le renommage de Kaya pour T29, citée mot pour mot avec son titre ; écart ligne par ligne des PF et P95 pour 2024 ; statut V « sens à confirmer » ; arrondi « par analogie avec le tableau 76 », décidé par la DAI (Q30) ; l. 1608 « déjà dans Kaya 2017 » ; Prestation #29 (l. 326) citée pour RASA, désaccord sur les accessoires signalé (A1) ; termes expliqués (débours, cliché, écart moyen) ; « 5 directions régionales (et un siège inactif) ».
  - *demandes du recueil oubliées*, reprises en partie 1 : l. 72 (N14, Q10) ; structures responsables l. 1119, 1128, 1198, 1209 (destinataires de la partie 6).
  - *tableaux de l'annuaire oubliés*, repris en partie 4 : 35 et 39 ; 24 ; 76, 78, 79, 80 ; 18 et 22 lus en entier ; tableaux par direction (29 à 34, 40, 42 à 46, 91 à 96, 98) ; 8 ; 26.
- **Un point où ce document garde une nuance que la contre-vérification n'avait pas :** pour la date des payées des pensions, la contre-vérification note que la saisie ne réduit pas l'écart moyen mois par mois (6,4 % contre 6,2 %) ; l'inventaire de l'annuaire montre qu'elle en reproduit pourtant le rythme (corrélation 0,75 contre −0,12). Les deux sont justes et sont dits ensemble (R4, partie 4).
- **Relecture (exactitude et clarté), 08/11/2026 :**
  - *Recueil* : liste refaite à la main (grep de `besoins_v7.txt`, demandes F2 et transverses du relevé, parties 5 et 6 de la note de reprise, oublis de la contre-vérification). Toutes les lignes y étaient ; ajout de la l. 127 (T27, renommage en « frais médicaux de maternité »), sur laquelle N3 et Q12 s'appuient par analogie ; l. 326 passée de « pris en compte » à « à intégrer » (N1 n'est dans aucune requête, comme la l. 255) ; « l. 1978 à 1988 » corrigé en « 1978 à 1989 » (la structure DES/MQ est à la l. 1989).
  - *Chiffres* : recalculés depuis les sources, tous justes : écarts de la partie 4 (annuaire CNSS 2024, tableaux 28, 41, 90, 99, 106, 24, 35, 37) ; taux de P105 par branche (0,47 %, 0,47 %, 0,045 %, total 0,39 % ; avec les moins-perçus 1,04 %, 0,93 %, 1,35 %, 1,10 % ; net des remboursements 0,37 %) ; P102 (684 trop-perçus, 229 484 759 FCFA ; 414 442 709 de moins-perçus ; 13 024 760 de remboursements ; avances établies 5 384 869 208) ; P104 (rappels 408 312 511, avances 373 206 605) ; P93, P79, P95, P99 (444 = 619 − 175), P13, P34, A01.
  - *Table des contrôles* : « n'a jamais été complète » était affirmé alors que c'est la question Q2, et le chiffre de 0,44 % suppose que les 212 contrôles portent sur des pensions (la branche de ce maximum n'est pas connue). Remplacé par un repère sûr, recalculé : environ 7 760 contrôles par an seraient nécessaires pour 5 % des 155 206 dossiers payés de 2025, face à 1 542 lignes en tout (partie 0, N11, Q2, point d'attention 4).
  - *Identifiants* : comparés au README, à `reference_cipres_T25_T57.md`, à `regles_prestations` et à la fiche T31 : sens gardés. C1 dit maintenant qu'il est étendu à la vue de la DAI (la carte ne lit que la table) ; Q23 renvoie aussi à la fiche T31 Q4, qui posait déjà la question des rappels et des avances.
  - *Clarté* : « En bref » simplifié (« vérifiés après paiement », « indus » expliqué, « copie de la base arrêtée fin mars », norme de 5 % rappelée, nombre de questions et « aucune ne bloque ») ; mots ajoutés au lexique (liquider, corrélation, vue, DTM ; statuts D et P « à confirmer ») ; `:1` retiré du réglage du cliché ; « Md » écrit en toutes lettres. Aucun statut, verdict ni source n'a changé, sauf la l. 326 ci-dessus.
- **Deux choix de forme :**
  - H11 et H12 gardent leur sens de `reference_cipres_T25_T57.md` ; les autres défauts du code des DTM, relevés par le dossier, vont au point d'attention 13 au lieu d'élargir ces deux identifiants.
  - Les questions du dossier sur les regroupements de T28 (pension et allocation de survivant, plus de pension de solidarité) et de T29 (rentes de survivants, achat de rente) ne sont pas reposées : Kaya 2017, source plus forte que l'Annuaire CIPRES, les tranche (N1, N2). Restent la question du sens de « achat de rente » (Q11) et celle de la maquette d'origine (Q13, déjà posée pour T25 et T27).
- **Critique d'exhaustivité et de cohérence entre les cinq familles, 08/11/2026.** Le recueil v7 (l. 115 à 335 et indicateurs l. 1018 à 1378) a été relu à nouveau face aux cinq documents de famille, à la liste du recueil (`besoins_v7_prestations.md`) et au document sur l'annuaire (`annuaire_cnss_2024_prestations.md`). Changements ici : tableau 26, verdict « précise » au lieu de « confirme » (un indice fort, puisque la base n'a pas de date de déclaration du décès), comme la famille T50-T57 pour le même chiffre ; R13, l'« En bref » et « En clair » suivent. La ligne l. 72 cite Q10. La satisfaction, les nouvelles prestations et le document de référence sont rangés dans la famille T50-T57 (hors base ; questions T50-T57 Q24 et Q25). Q24 dit que T32-T41 Q2 et T50-T57 Q1 la reprennent. Aucun statut n'a changé.
- **Numéros des contrôles à écrire, 08/11/2026.** Trois familles réservaient P107 pour des contrôles différents. Les numéros sont maintenant distincts : P107 à P114 pour le lot 10 de T42-T49, contrôles (a) à (h) ; P115 à P119 pour T50-T57 (avant : P107 à P111) ; P120 et suivantes pour T28-T31 (avant : P107 et suivantes). Le document de l'annuaire suit (P115 à P117).

---

## 9. Ce qui reste à faire

| Tâche | Qui | État |
|---|---|---|
| Dire où sont les contrôles depuis juillet 2025, et fournir ceux des cinq derniers exercices dans la vue `V_BI_CONTROLE_POSTERIORI` (Q1 à Q3). Sans cela, T28 à T30 restent vides | DAI et DSI, via toi | à poser |
| Présenter les lectures du taux d'indus (recueil, Kaya 2017, feuille 06) et du taux de contrôle (feuille 05), et recueillir la décision (Q6, Q14 à Q16) | toi, avec la DAI et la DCP | à faire |
| Ajouter à t31 un taux par branche, puis la direction régionale et l'agence par le dossier, les payées comprises (N9) | Claude | à faire |
| Ajouter à t31 la période libre, le taux des moins-perçus et des deux ensemble, la mention de norme et les réglages « à créer » (N6, N8, N12, partie 5) | Claude | proposé, à faire après visa |
| Lot de contrôle P120 et suivantes, sans lire les textes libres : la colonne de code de prestation de la table des contrôles est-elle remplie ; lignes et dossiers distincts par année, branche et trimestre ; dénominateur 2025 recompté avec les seuls paiements PM au statut D ou P ; trop-perçus établis dans les 6 mois après un contrôle | Claude, puis toi (exécution) | à faire |
| Tester la date des payées mois par mois avec ce qui ne l'a pas encore été (la date de l'effet, là où elle existe), face aux tableaux 28, 41 et 90 | Claude, puis toi | à faire |
| Comparer les payées par direction aux tableaux 29 à 34, 40, 42 à 46, 91 à 96 et 98 (N9) | Claude, après exécution | à faire |
| Fournir le fichier des indus de 2024 de la DAI, pour le comparer aux 684 trop-perçus (229 484 759 FCFA) de la base (N10) | DAI, via toi | à poser |
| Dire dans quel mois l'annuaire range un paiement (Q24) | DPC, via toi | à poser |
| Quand les contrôles arriveront : écrire la requête T28 à T30 (N1 à N5, C1), puis proposer les corrections de `DTM_CONTROLE_PRESTATION` et `DTM_INDICATEURS_PRESTATIONS` (TODO, tâche 10) | Claude | en attente des contrôles |
| Après validation : proposer la requête `DTM_PRESTATION_INDUE` corrigée (TODO, tâche 9) | Claude | après visa |
| Répondre aux questions Q1 à Q30 (DAI, DCP, DR, DSI, DPC, DCR) | CNSS, via toi | à poser |
| Si l'utilisateur le décide : reporter ces règles dans le README, la fiche T31, la fiche de lecture, `regles_prestations` et points_a_valider | Claude, sur demande | non commencé (rien n'a été modifié) |
