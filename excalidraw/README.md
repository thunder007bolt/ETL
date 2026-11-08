# Schémas Excalidraw du projet BI CNSS

Les documents et les explications du projet sont faits sous forme de schémas Excalidraw. Ce dossier les range par domaine, avec une visionneuse locale pour les lire, les retoucher et les enregistrer.

## Contenu

| Chemin | Rôle |
| --- | --- |
| `schemas/<dossier>/…/*.excalidraw` | Les schémas, rangés par dossier et sous-dossier : `prestations`, `grh`, `recouvrement`, `projet`… |
| `viewer/` | La visionneuse : liste des schémas, édition, enregistrement (source : `app.js`) |
| `viewer/vendor/` | Excalidraw, React et les polices, servis par le projet : la visionneuse marche sans Internet. Construit par `viewer/build/`, suivi par git |
| `viewer/build/` | La construction de `vendor/` (`package.json`, `construire.mjs`) ; ses `node_modules` restent hors de git |
| `server.py` | Petit serveur local de la visionneuse (Python seul, aucune dépendance) |
| `lib/excalidraw_builder.py` | Bibliothèque pour générer un schéma en Python |
| `lib/historique.py` | Historique des versions, partagé par la visionneuse et le builder |
| `sources/<domaine>/*.py` | Scripts qui ont produit le premier jet de chaque schéma |
| `historique/<dossier>/<schéma>/` | Les versions gardées de chaque schéma (pas suivi par git) |
| `historique/deplacements.json` | L'ancien et le nouveau chemin des schémas déplacés dans la visionneuse |

## Ouvrir la visionneuse

- **Dans l'app Claude** : demander « ouvre la visionneuse Excalidraw ». Elle s'ouvre dans le panneau du navigateur (configuration `excalidraw` de `.claude/launch.json`).
- **Dans un terminal**, depuis le dossier `ETL` :

  ```bash
  python3 excalidraw/server.py
  ```

  puis ouvrir http://localhost:8765 dans un navigateur.

L'adresse d'un schéma précis est `http://localhost:8765/#prestations/domaine_prestations.excalidraw`.

La visionneuse marche sans Internet : Excalidraw, React et les polices sont dans `viewer/vendor/`, servis par `server.py`. Seule la police Xiaolai (chinois, japonais, 12 Mo) n'y est pas copiée.

Après une modification de `viewer/app.js`, reconstruire `vendor/` (il faut Internet la première fois, pour `npm install`) :

```bash
cd excalidraw/viewer/build && npm install && npm run build
```

## Utiliser la visionneuse

- **Ouvrir** : cliquer sur un schéma dans l'arborescence de gauche. Le bouton « Liste » la masque pour avoir tout l'écran.
- **Dossiers** : un clic sur un dossier le replie ou le déplie ; replié, il affiche son nombre de schémas. La visionneuse retient les dossiers repliés.
- **Modifier** : comme sur excalidraw.com. La barre du haut indique « Modifications non enregistrées » dès qu'on change quelque chose.
- **Enregistrer** : bouton « Enregistrer » ou Cmd+S. Le fichier `.excalidraw` est mis à jour sur le disque.
- **Nouveau schéma** : « + Schéma », puis un nom avec son dossier, par exemple `grh/effectif`. Au survol d'un dossier, l'icône de fichier crée un schéma dedans.
- **Nouveau dossier** : « + Dossier », puis un nom avec son dossier parent, par exemple `prestations/archives`. Au survol d'un dossier, l'icône de dossier crée un sous-dossier ; la corbeille supprime un dossier vide (un dossier qui contient quelque chose ne se supprime pas).
- **Ranger** : glisser un schéma ou un dossier sur un autre dossier, ou sur un schéma pour rejoindre son dossier. Pendant le glisser, une zone « racine » apparaît en bas de la liste. Un dossier replié s'ouvre si on le survole un instant. Les versions du schéma le suivent, et un schéma ouvert reste ouvert, même avec des modifications non enregistrées.
- **Exporter** : menu ☰ d'Excalidraw, puis « Exporter l'image » (PNG ou SVG), par exemple pour un courriel ou une présentation.
- **Suivi du disque** : quand Claude régénère le schéma ouvert, la visionneuse le recharge seule. Si des modifications ne sont pas enregistrées, elle affiche « Modifié sur le disque » et ne recharge qu'à la demande. À l'enregistrement, elle prévient si le fichier a changé entre-temps.
- **Versions** : le bouton « Versions » ouvre l'historique du schéma (voir ci-dessous).

Les fichiers s'ouvrent aussi sans la visionneuse : en les glissant sur excalidraw.com, ou dans VS Code avec l'extension Excalidraw. Une modification faite par ce chemin est gardée dans l'historique à la prochaine écriture par la visionneuse ou par un script.

## Historique des versions

Chaque écriture d'un schéma garde une version dans `historique/<domaine>/<schéma>/` : un enregistrement dans la visionneuse, une restauration, ou une régénération par un script (y compris avec `--force`). Avant d'écrire, l'état trouvé sur le disque est aussi gardé s'il n'était pas encore dans l'historique. Un enregistrement identique à la dernière version n'en crée pas de nouvelle.

Dans la visionneuse, le panneau « Versions » liste les versions, de la plus récente à la plus ancienne, avec leur date et leur origine (visionneuse, script, restauration, trouvée sur le disque). La version identique au fichier porte la pastille « actuelle ».

- **Voir** : affiche la version en lecture seule, au même cadrage, pour la comparer. « Revenir à l'actuelle » la referme.
- **Restaurer** : remplace le schéma par cette version. L'état actuel reste dans l'historique, donc une restauration s'annule en restaurant la version d'avant.

L'historique est local : il n'est pas suivi par git, pour ne pas alourdir le dépôt. Git sert à figer les étapes importantes (schéma validé, version remise à la CNSS) ; l'historique garde tout le reste. Une version est un fichier `.excalidraw` ordinaire : on peut aussi l'ouvrir directement sur excalidraw.com.

Après une mise à jour de la visionneuse, relancer `server.py` : un serveur lancé avant ne garde pas d'historique.

## Conventions

- Un dossier par domaine, et des noms en minuscules avec `_` : `schemas/prestations/domaine_prestations.excalidraw`.
- Police manuscrite Excalifont et fond blanc.
- Couleurs, toujours avec le même sens (`C` dans `lib/excalidraw_builder.py`) :

  | Couleur | Sens |
  | --- | --- |
  | Bleu | PVID (vieillesse, invalidité, décès) |
  | Orange | AT/MP (accidents du travail, maladies professionnelles) |
  | Vert | PF (prestations familiales, maternité) ; étape faite |
  | Rouge | Normes CIPRES, problèmes constatés |
  | Jaune | Notes, points à valider, étapes à faire |
  | Violet | La CNSS |
  | Gris | Neutre, contexte |

- Les chiffres provisoires et les règles qui ne sont que des propositions sont signalés comme tels dans le schéma.
- Chaque schéma porte sa date d'état dans son sous-titre.

## Générer un schéma en Python

Un script de `sources/<domaine>/` construit le schéma avec `lib/excalidraw_builder.py`, puis l'écrit dans `schemas/<domaine>/` :

```bash
python3 excalidraw/sources/prestations/domaine_prestations.py
```

Le script refuse d'écraser un schéma qui existe déjà, car il a peut-être été retouché dans la visionneuse. Il refuse aussi d'en recréer un à l'ancienne place d'un schéma déplacé dans la visionneuse, et donne la nouvelle : relancer avec `--output <nouveau chemin> --force`, et mettre à jour `OUTPUT` dans le script. Avec `--force`, il l'écrase ; les retouches ne sont plus dans le fichier, mais restent dans l'historique, d'où on peut les restaurer. Une fois un schéma retouché à la main, c'est le fichier `.excalidraw` qui fait foi, pas le script.

## Journal de bord

`schemas/journal/` est la mémoire du travail : `00 tableau de bord` (où on en est, qui doit agir, la suite), `01 frise` (chaque jour, semaine par semaine), `02 decisions` (ce qui a été décidé, pourquoi, et ce qui a été remplacé), `03 ou trouver quoi` (analyses, requêtes, schémas, documents) et `jours/AAAA-MM-JJ` (une fiche par jour).

Tout vient d'un seul fichier de données, `sources/journal/journal.py`, mis à jour à la fin de chaque étape ; puis :

```bash
python3 excalidraw/sources/journal/generer.py --force
```

## Fiches de maîtrise

`schemas/maitrise/` rassemble des fiches de révision pour s'approprier tout le travail et pouvoir le défendre. Commencer par `00 commencer ici`.

- **Organisation.** Un sous-dossier par domaine de l'Annuaire CIPRES, numéroté comme lui (`1 immatriculation`, `2 recouvrement`, `3 prestations`, `4 gestion administrative`, `5 gestion financière`), plus `0 projet` pour ce qui est commun.
- **Même plan pour chaque domaine** : 01 le domaine en bref, 02 le vocabulaire, 03 les données, 04 la démarche, 05 les découvertes, 06–07 les règles, 08 les erreurs des DTM, 09 les questions ouvertes, 10 où on en est, 11 se préparer aux questions.
- **Forme d'une fiche.** Une suite de parties, chacune une grille de blocs séparés : un bloc par idée, avec sa couleur, sa pastille (R6, B12, P25…) et son état.
- **Générer.** Le contenu est dans `sources/maitrise/` : `fiches.py` (le générateur), un module par domaine (`projet.py`, `prestations.py`), `generer.py` (le lancement).

  ```bash
  python3 excalidraw/sources/maitrise/generer.py --force --seulement "3 prestations"
  ```

- **Ajouter un domaine** : écrire son module sur le modèle de `prestations.py` et l'ajouter à `DOMAINES` dans `generer.py`.

État au 30/10/2026 : le sommaire, `0 projet` (3 fiches) et `3 prestations` (11 fiches) ; les autres domaines restent à faire.

## Schémas

Rangement du dossier `grh` (08/11/2026) : un dossier par famille de tableaux, avec ses schémas d'explication — `T58 effectif` (T58, T59, T61, T65 : la requête pas à pas, les cas d'agents, simple et SQL), `T60 mouvements` (la requête pas à pas, les cas de mouvement, simple et SQL), `T63 T64 finances` (la structure à demander à la CNSS) ; les fiches de validation dans `tableaux cipres` ; à la racine, ce qui vaut pour toute la GRH (règles, fiche de lecture, points à valider).

| Schéma | Domaine | État au |
| --- | --- | --- |
| [regles_prestations](schemas/prestations/regles_prestations.excalidraw) — toutes les règles du domaine trouvées dans les données, avec leurs preuves : l'essentiel en bref, les 5 lots de requêtes, ce que la base nous a appris (B1–B25), les règles de calcul (D1–D14 pour les dossiers T25 à T27, R1–R9, A1, I1, C1 ; R1 à R7 confrontées à l'annuaire de la CNSS 2024, R6 corrigée et R7 revue le 30/10) et la correspondance code → ligne CIPRES, les 14 hypothèses sur les DTM, les questions ouvertes, l'index des 73 requêtes P01–P73 avec leur fichier | Prestations | 30/10/2026 |
| [T25_dossiers_de_pension](<schemas/prestations/tableaux cipres/T25_dossiers_de_pension.excalidraw>) — fiche de validation de T25 pour le métier : maquette avec le numéro de règle de chaque colonne, six dossiers fictifs sur un calendrier (dont une réversion), les lignes, dix règles avec leurs sources et les chiffres du lot 3 B, leur SQL à droite et des cases de réponse, cinq questions. Gabarit des fiches suivantes : `sources/prestations/fiche_cipres.py` | Prestations | 30/10/2026 |
| [T26_dossiers_at_mp](<schemas/prestations/tableaux cipres/T26_dossiers_at_mp.excalidraw>) — fiche de validation de T26 (accidents du travail et maladies professionnelles), même gabarit que T25 : maquette, premiers chiffres de 2025, six dossiers fictifs (IJ, soins, rente après consolidation, rente de survivant, déclaration sans suite), cinq lignes, dix règles avec leur SQL, quatre questions (déclarations sans suite, soins, départ de la durée d'une rente, attente des IJ) | Prestations | 30/10/2026 |
| [T27_dossiers_pf](<schemas/prestations/tableaux cipres/T27_dossiers_pf.excalidraw>) — fiche de validation de T27 (prestations familiales et maternité), même gabarit : six dossiers fictifs (nouvel allocataire, enfant ajouté, grossesse, refus, réception sans dossier), quatre lignes, dix règles avec leur SQL, quatre questions (enfant ajouté, grossesse sur deux lignes, demandes sans suite, refus) | Prestations | 30/10/2026 |
| [T32_pensionnes_vieillesse](<schemas/prestations/tableaux cipres/T32_pensionnes_vieillesse.excalidraw>) — fiche pilote de T32 à T41 (bénéficiaires au 31/12 par âge et sexe) : maquette, premiers chiffres de 2024 et répartition par âge publiée par la CNSS, six pensionnés fictifs (compté ou non au 31/12), les tranches d'âge, huit règles avec leur SQL, cinq questions ; mise à jour le 01/11 avec les lots 5 et 6 (répartition par âge de la base face à celle de la CNSS) | Prestations | 31/10/2026 |
| [T41_allocataires](<schemas/prestations/tableaux cipres/T41_allocataires.excalidraw>) — fiche de T41 (allocataires des prestations familiales au 31/12) : suit la fiche pilote T32, sauf l'allocataire (un dossier), l'enfant payé dans l'année et l'âge de l'allocataire ; allocataires de 2020 à 2024 face à l'annuaire de la CNSS, six allocataires fictifs, huit règles avec leur SQL, cinq questions | Prestations | 01/11/2026 |
| [T42_nouveaux_retraites](<schemas/prestations/tableaux cipres/T42_nouveaux_retraites.excalidraw>) — fiche pilote de T42 à T49 (nouveaux pensionnés et crédirentiers de l'exercice) : maquette avec services passés, nouveaux retraités de 2024 par âge face à l'annuaire de la CNSS, six retraités fictifs (saisie, révision, rappel, coordination), huit règles avec leur SQL, cinq questions | Prestations | 01/11/2026 |
| [T50_T57_evolution_5_ans](<schemas/prestations/tableaux cipres/T50_T57_evolution_5_ans.excalidraw>) — fiche pilote de T50 à T57 (évolution sur 5 ans, récapitulatifs par branche) : trois maquettes (T50, T56, T57), IJ de maternité de 2020 à 2024 face à l'annuaire de la CNSS, six cas fictifs (liquidation, paiement l'année suivante, décès déclaré tard, accident mortel), huit règles avec leur SQL, cinq questions dont la date des montants | Prestations | 01/11/2026 |
| [T31_prestations_indues](<schemas/prestations/tableaux cipres/T31_prestations_indues.excalidraw>) — fiche de T31 (prestations indues par branche et par mois) : maquette à 12 colonnes, taux d'indus de 2024 face à la norme de 2 %, six cas fictifs (trop-perçu, rappel, avance, reprise, annulation, décès), huit règles avec leur SQL, cinq questions | Prestations | 01/11/2026 |
| [regles_grh](schemas/grh/regles_grh.excalidraw) — les règles du domaine GRH (T58 à T65) trouvées dans les données et contrôlées par l'annuaire de la CNSS 2024 : l'essentiel en bref, les lots 1 à 20, l'annuaire face à la base (tableaux 119 à 124, ligne par ligne, avec un verdict), les règles G1 à G11 avec leurs preuves, les questions QG1 à QG7 à la DRH et la suite, l'index des requêtes Q97 à Q117. Source : `sources/grh/regles_grh.py` | GRH | 01/11/2026 |
| [T58_effectif_31_12](<schemas/grh/tableaux cipres/T58_effectif_31_12.excalidraw>) — fiche pilote de l'effectif (T58, T59, T61, T65), sur le gabarit des fiches des prestations : maquette (quatre catégories par sexe), tranches d'âge de 2024 face à l'annuaire de la CNSS, six agents fictifs (recruté, retraité en décembre, en disponibilité, contractuel, parti sans date), les lignes des quatre tableaux, neuf règles avec leur SQL, cinq questions à la DRH. Sources : `sources/grh/t58_effectif.py`, gabarit réglé pour la GRH dans `sources/grh/fiche_grh.py` | GRH | 01/11/2026 |
| [T60_mouvement_personnel](<schemas/grh/tableaux cipres/T60_mouvement_personnel.excalidraw>) — fiche de T60 (mouvement du personnel sur cinq ans), mise à jour le 08/11/2026 avec la requête réécrite : maquette, retraites de 2020 à 2024 face à l'annuaire de la CNSS, six agents fictifs (recrutement, retraite, décès daté par la liquidation, disponibilité, temporaire devenu permanent, entré et sorti la même année), les colonnes, neuf règles avec leur SQL (dont le détail des « autres » par les types de mouvement de la CNSS), neuf questions numérotées comme `analyses/grh/regles_t60.md`. Étiquette de source « Recueil v7 » en plus | GRH | 08/11/2026 |
| [T62_personnel_forme](<schemas/grh/tableaux cipres/T62_personnel_forme.excalidraw>) — fiche de T62 (permanents formés, avec ou sans promotion) : maquette, formés de 2024 par catégorie face à l'annuaire de la CNSS (qui ne retombe pas), six agents fictifs, les catégories, huit règles avec leur SQL, cinq questions dont l'origine des 480 formés publiés | GRH | 01/11/2026 |
| [requete_effectif_pas_a_pas](<schemas/grh/T58 effectif/requete_effectif_pas_a_pas.excalidraw>) — la requête de l'effectif (T58, T59, T61, T65) expliquée : une seule photo mais des dates, la requête partie par partie, trois agents fictifs rejoués au 31/12 de 2018 à 2025, ce que la base ne permet pas de rejouer. Source : `sources/grh/requete_effectif_pas_a_pas.py` | GRH | 07/11/2026 |
| [cas_agents_effectif](<schemas/grh/T58 effectif/cas_agents_effectif.excalidraw>) et [cas_agents_effectif_sql](<schemas/grh/T58 effectif/cas_agents_effectif_sql.excalidraw>) — tous les cas d'agents de l'effectif (permanents, temporaires, entre les deux) et ce que la requête en fait : la version simple (compté, inactif, pas compté), puis la version technique reliée au SQL (parties et CTE, WHERE annoté, SQL qui décide chaque cas). Sources : `sources/grh/cas_agents_effectif.py` et `cas_agents_effectif_sql.py` | GRH | 07/11/2026 |
| [cas_mouvements_t60](<schemas/grh/T60 mouvements/cas_mouvements_t60.excalidraw>) et [cas_mouvements_t60_sql](<schemas/grh/T60 mouvements/cas_mouvements_t60_sql.excalidraw>) — T60, le mouvement du personnel, cas par cas. La version simple : les deux photos (31/12 d'avant, 31/12), la maquette CIPRES avec les codes de mouvement de la CNSS sous chaque colonne (le détail des « autres » du recueil v7), les entrées, les sorties, les cas sans mouvement, 2024 face à l'annuaire CNSS. La version technique reliée au SQL : parties et CTE, le WHERE et le CASE des entrées et des sorties (repères E1 à E8, S1 à S7), le SQL qui décide chaque cas, le rangement et le comptage ; les numéros de ligne sont lus dans le fichier SQL et chaque bout de SQL y est vérifié à la génération. Règles : `analyses/grh/regles_t60.md`. Sources : `sources/grh/cas_mouvements_t60.py` et `cas_mouvements_t60_sql.py` | GRH | 08/11/2026 |
| [requete_mouvement_pas_a_pas](<schemas/grh/T60 mouvements/requete_mouvement_pas_a_pas.excalidraw>) — la requête de T60 expliquée, sur le modèle de celle de l'effectif : l'idée des deux photos de l'effectif, la requête partie par partie avec ses numéros de ligne (lus dans le fichier SQL), six agents fictifs d'un 31/12 à l'autre (retraite, recrutement, titularisation, disponibilité, décès lu à la liquidation, entré et sorti la même année), les lignes de 2024 qu'ils donnent, le contrôle attendu et les limites. Source : `sources/grh/requete_mouvement_pas_a_pas.py` | GRH | 08/11/2026 |
| [t63_t64_donnees_a_fournir](<schemas/grh/T63 T64 finances/t63_t64_donnees_a_fournir.excalidraw>) — T63 et T64 : les données à demander à la CNSS, sans SQL ; c'est la vue commune V_BI_BALANCE_COMPTABLE, vue depuis la GRH. Source : `sources/grh/t63_t64_donnees_a_fournir.py` | GRH | 07/11/2026 |
| [vues cnss](<schemas/vues cnss/00 sommaire.excalidraw>) — dossier « vues cnss » : les vues à demander à la CNSS pour les données qui manquent, sans SQL. 00 sommaire (le principe, les trois vues, ce qui est déjà dans nos sources), 01 balance comptable (V_BI_BALANCE_COMPTABLE : T63, T64, T66 à T76, SP1, SP2), 02 contrôles a posteriori (T28 à T30), 03 réserves (T75). Source : `sources/vues_cnss/vues_cnss.py` ; les colonnes de la vue commune sont dans `commun.py`, partagé avec le schéma T63-T64 | Prestations, Finance, GRH | 07/11/2026 |
| [points_a_valider (prestations)](schemas/prestations/points_a_valider.excalidraw) et [points_a_valider (GRH)](schemas/grh/points_a_valider.excalidraw) — pour la réunion du 02/11 : les règles et les questions à valider de chaque domaine, une carte par point (cases D'accord, Pas d'accord, À discuter, et réponse), les 5 points prioritaires en orange, puis tous les tableaux (T25 à T57, T58 à T65) avec leurs codes et leurs points. Faits à partir de `analyses/<domaine>/points_a_valider.md` par `sources/points_a_valider.py` ; la version Word est à côté du .md | Prestations, GRH | 01/11/2026 |
| [pensions_2024](<schemas/annuaire cnss/pensions_2024.excalidraw>) — dossier « annuaire cnss », qui regroupe les analyses sur l'annuaire statistique de la CNSS. Les pensions de 2024 : notre base face aux chiffres publiés (tableau 18 comparé case par case, contrôles des tableaux 19 à 23, ce que la comparaison dit de nos règles, écarts et suite). Requêtes dans `analyses/comparaison_annuaire_cnss_2024` | Annuaire CNSS | 30/10/2026 |
| [gestion_administrative_2024](<schemas/annuaire cnss/gestion_administrative_2024.excalidraw>) — la partie IX de l'annuaire (tableaux 119 à 126, le personnel au format CIPRES) face à nos règles GRH : le tableau 121 recopié avec ses contrôles (la remise en activité est une entrée, la disponibilité une sortie : réponse à QG6), neuf cartes de règles tableau par tableau (T58 à T65, par direction), ce qui est entré dans nos règles et ce qui reste ouvert. Conclusions intégrées le 08/11/2026 (disponibilité hors effectif dans T58 et T60, règles, questions, fiches et schémas GRH). Source : `sources/annuaire_cnss/gestion_administrative_2024.py` | Annuaire CNSS | 08/11/2026 |
| [remarques_besoins_v7](<schemas/besoins cnss/remarques_besoins_v7.excalidraw>) — dossier « besoins cnss », qui regroupe ce qu'on tire du recueil des besoins de la CNSS (cnss_files/2566BF_CNSS_Analytics_Description_Besoins_v7.docx, ateliers de janvier 2026). Les remarques B01 à B41 face à notre travail : ce qui confirme nos règles, ce qui les contredit, les comptes qui ne concordent pas avec la grille des ratios, les réponses à nos questions, ce qui est nouveau pour la BI, ce que le document ne dit pas, et le hors-périmètre. Rien n'est encore reporté dans les règles | Besoins CNSS | 04/11/2026 |
| [reunion_grh_2026-11-03](schemas/reunions/reunion_grh_2026-11-03.excalidraw) — dossier « reunions », un schéma par réunion. La réunion GRH du 03/11/2026, tirée de ses deux enregistrements (cnss_files : opt.m4a et Voix 261103_144325.m4a, transcrits avec whisper.cpp) : ce qui est dit ou validé (M01 à M08), ce qui reste ouvert (M09 à M12), ce qu'on a vu dans les données, qui fait quoi. Chaque point renvoie à la minute de l'enregistrement. Rien n'est encore reporté dans les règles | GRH | 04/11/2026 |
| [domaine_prestations](schemas/prestations/domaine_prestations.excalidraw) — le domaine Prestations expliqué simplement (branches, bénéficiaires, vie d'un dossier, tableaux T25–T57, chiffres 2025) | Prestations | 29/10/2026 |
| [prestation_esp](schemas/prestations/prestation_esp.excalidraw) — la table PRESTATION_ESP expliquée simplement : espèces et nature, place entre dossier et débours, deux exemples, colonnes, vie d'un droit, règles R5 à R7 | Prestations | 29/10/2026 |
| [tables_prestations](schemas/prestations/tables_prestations.excalidraw) — les tables du domaine : base source et colonnes de liaison, toutes les listes de codes lues dans la base (lot 3 : prestations en espèces et en nature, débours, effets, modes, étapes, ajustements, pièces, statuts…), géographie (directions, services, 350 lieux de paiement), tables DTM et tableaux qu'elles alimentent | Prestations | 29/10/2026 |
| [vie_dossier](schemas/prestations/vie_dossier/vie_dossier.excalidraw) — la partie 2 de tables_prestations développée : le parcours en 7 étapes, ce que chaque étape écrit dans la base (tables, colonnes, chiffres, pièges ; demandes NOUVEAU et ANCIEN, réversion), un exemple de bout en bout jusqu'à la réversion, le lien avec les tableaux CIPRES (règles D3, D4, D6), ce que le lot 3 B a vérifié et ce qui reste ouvert | Prestations | 30/10/2026 |
| [modele_conceptuel_dossier](schemas/prestations/vie_dossier/modele_conceptuel_dossier.excalidraw) — le parcours d'un dossier en modèle conceptuel (à la manière de Merise) : tables du parcours, détails, personnes, associations et cardinalités (« ? » : à vérifier), dont la demande ANCIEN qui porte sur un dossier existant, et ce que le modèle dit en clair | Prestations | 30/10/2026 |
| [cheminement_rente](schemas/prestations/vie_dossier/cheminement_rente.excalidraw) — une rente AT/MP de la déclaration de l'accident au paiement, puis aux rentes de survivants : le parcours en 7 étapes, ce que chaque étape écrit (tables, colonnes, chiffres du lot 4, pièges), les clés d'une table à l'autre, un exemple de bout en bout, les tableaux CIPRES où la rente compte | Prestations | 03/11/2026 |
