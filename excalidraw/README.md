# Schémas Excalidraw du projet BI CNSS

Les documents et les explications du projet sont faits sous forme de schémas Excalidraw. Ce dossier les range par domaine, avec une visionneuse locale pour les lire, les retoucher et les enregistrer.

## Contenu

| Chemin | Rôle |
| --- | --- |
| `schemas/<dossier>/…/*.excalidraw` | Les schémas, rangés par dossier et sous-dossier : `prestations`, `grh`, `recouvrement`, `projet`… |
| `viewer/` | La visionneuse : liste des schémas, édition, enregistrement |
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

La bibliothèque Excalidraw est chargée depuis esm.sh : il faut Internet au premier chargement, ensuite le navigateur la garde en cache.

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

| Schéma | Domaine | État au |
| --- | --- | --- |
| [regles_prestations](schemas/prestations/regles_prestations.excalidraw) — toutes les règles du domaine trouvées dans les données, avec leurs preuves : l'essentiel en bref, les 5 lots de requêtes, ce que la base nous a appris (B1–B25), les règles de calcul (D1–D14 pour les dossiers T25 à T27, R1–R9, A1, I1, C1 ; R1 à R7 confrontées à l'annuaire de la CNSS 2024, R6 corrigée et R7 revue le 30/10) et la correspondance code → ligne CIPRES, les 14 hypothèses sur les DTM, les questions ouvertes, l'index des 73 requêtes P01–P73 avec leur fichier | Prestations | 30/10/2026 |
| [T25_dossiers_de_pension](<schemas/prestations/tableaux cipres/T25_dossiers_de_pension.excalidraw>) — fiche de validation de T25 pour le métier : maquette avec le numéro de règle de chaque colonne, six dossiers fictifs sur un calendrier (dont une réversion), les lignes, dix règles avec leurs sources et les chiffres du lot 3 B, leur SQL à droite et des cases de réponse, cinq questions. Gabarit des fiches suivantes : `sources/prestations/fiche_cipres.py` | Prestations | 30/10/2026 |
| [T26_dossiers_at_mp](<schemas/prestations/tableaux cipres/T26_dossiers_at_mp.excalidraw>) — fiche de validation de T26 (accidents du travail et maladies professionnelles), même gabarit que T25 : maquette, premiers chiffres de 2025, six dossiers fictifs (IJ, soins, rente après consolidation, rente de survivant, déclaration sans suite), cinq lignes, dix règles avec leur SQL, quatre questions (déclarations sans suite, soins, départ de la durée d'une rente, attente des IJ) | Prestations | 30/10/2026 |
| [T27_dossiers_pf](<schemas/prestations/tableaux cipres/T27_dossiers_pf.excalidraw>) — fiche de validation de T27 (prestations familiales et maternité), même gabarit : six dossiers fictifs (nouvel allocataire, enfant ajouté, grossesse, refus, réception sans dossier), quatre lignes, dix règles avec leur SQL, quatre questions (enfant ajouté, grossesse sur deux lignes, demandes sans suite, refus) | Prestations | 30/10/2026 |
| [pensions_2024](<schemas/annuaire cnss/pensions_2024.excalidraw>) — dossier « annuaire cnss », qui regroupe les analyses sur l'annuaire statistique de la CNSS. Les pensions de 2024 : notre base face aux chiffres publiés (tableau 18 comparé case par case, contrôles des tableaux 19 à 23, ce que la comparaison dit de nos règles, écarts et suite). Requêtes dans `analyses/comparaison_annuaire_cnss_2024` | Annuaire CNSS | 30/10/2026 |
| [domaine_prestations](schemas/prestations/domaine_prestations.excalidraw) — le domaine Prestations expliqué simplement (branches, bénéficiaires, vie d'un dossier, tableaux T25–T57, chiffres 2025) | Prestations | 29/10/2026 |
| [prestation_esp](schemas/prestations/prestation_esp.excalidraw) — la table PRESTATION_ESP expliquée simplement : espèces et nature, place entre dossier et débours, deux exemples, colonnes, vie d'un droit, règles R5 à R7 | Prestations | 29/10/2026 |
| [tables_prestations](schemas/prestations/tables_prestations.excalidraw) — les tables du domaine : base source et colonnes de liaison, toutes les listes de codes lues dans la base (lot 3 : prestations en espèces et en nature, débours, effets, modes, étapes, ajustements, pièces, statuts…), géographie (directions, services, 350 lieux de paiement), tables DTM et tableaux qu'elles alimentent | Prestations | 29/10/2026 |
| [vie_dossier](schemas/prestations/vie_dossier/vie_dossier.excalidraw) — la partie 2 de tables_prestations développée : le parcours en 7 étapes, ce que chaque étape écrit dans la base (tables, colonnes, chiffres, pièges ; demandes NOUVEAU et ANCIEN, réversion), un exemple de bout en bout jusqu'à la réversion, le lien avec les tableaux CIPRES (règles D3, D4, D6), ce que le lot 3 B a vérifié et ce qui reste ouvert | Prestations | 30/10/2026 |
| [modele_conceptuel_dossier](schemas/prestations/vie_dossier/modele_conceptuel_dossier.excalidraw) — le parcours d'un dossier en modèle conceptuel (à la manière de Merise) : tables du parcours, détails, personnes, associations et cardinalités (« ? » : à vérifier), dont la demande ANCIEN qui porte sur un dossier existant, et ce que le modèle dit en clair | Prestations | 30/10/2026 |
