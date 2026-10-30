# Comparaison : la base face à l'annuaire CNSS 2024

Comparaison isolée, faite le 30/10/2026 à la demande de l'utilisateur. Elle ne fait pas partie du suivi des prestations (`analyses/prestations`) : elle n'en change ni les règles, ni les requêtes proposées, ni les schémas.

**Question posée.** L'utilisateur a retrouvé l'annuaire statistique de la CNSS n° 32, année 2024 (`cnss_files/Annuaire_2024_CNSS_BURKINA-FASO.pdf`). Notre base correspond-elle à la sienne ? Peut-on reconstituer son tableau 18 « Évolution des bénéficiaires de pension selon le type de pension de 2020 à 2024 » ?

## Fichiers

| Fichier | Contenu |
| --- | --- |
| [requetes_comparaison.sql](requetes_comparaison.sql) | A01 à A08, avec les chiffres publiés dans l'en-tête, puis le résultat et le constat de chaque requête |
| [tableau18_reconstitue.sql](tableau18_reconstitue.sql) | Le tableau 18 dans la forme de l'annuaire, avec la définition retrouvée (pas encore exécutée) |
| [resultats/](resultats/) | A01 et A02 en entier, avec les chiffres publiés à côté |

| Requête | Compare à | État |
| --- | --- | --- |
| A01 | Tableau 18 (2020 à 2024), selon plusieurs définitions du pensionné | Exécutée |
| A02 | Tableau 22 (par direction régionale, 2024) | Exécutée |
| A03 | Tableau 23 (nouveaux pensionnés de 2024) | Exécutée |
| A04 | Tableaux 19 à 21 (par sexe, avec les montants) | Échec ORA-01722 (le sexe est un nombre), corrigée, à relancer |
| A05 | Montant de la branche des pensions en 2024 | Exécutée |
| A06 | La base telle qu'elle était à la publication | À exécuter |
| A07 | Dates de saisie des radiations | À exécuter |
| A08 | Orphelins de 2024 par âge (tableau 20) | À exécuter |

## Le tableau 18 comparé

Notre chiffre : pensions en cours au 31/12 dont le titulaire est en vie ; orphelins de moins de 25 ans. Entre parenthèses, le chiffre publié par la CNSS.

| Type de pension | 2020 | 2021 | 2022 | 2023 | 2024 |
| --- | --- | --- | --- | --- | --- |
| Retraite vieillesse | 25 246 (25 161) | 26 558 (26 415) | 28 037 (27 873) | 29 230 (29 042) | 31 180 (30 730) |
| Veuves | 19 643 (19 309) | 20 300 (19 964) | 20 861 (20 734) | 19 945 (20 832) | 20 369 (21 438) |
| Veufs | 236 (225) | 276 (249) | 304 (274) | 324 (298) | 356 (329) |
| Orphelins | 9 705 (8 982) | 9 558 (8 789) | 9 296 (8 667) | 8 123 (8 687) | 7 995 (9 478) |
| Invalidité | 111 (114) | 109 (111) | 119 (115) | 121 (116) | 125 (119) |
| Anticipée | 70 (69) | 70 (66) | 78 (68) | 78 (75) | 87 (78) |
| Ascendants | 58 (48) | 60 (53) | 65 (59) | 59 (54) | 59 (59) |
| **Total** | **55 069 (53 908)** | **56 931 (55 647)** | **58 760 (57 790)** | **57 880 (59 104)** | **60 171 (62 231)** |

Écart sur le total : +2,2 %, +2,3 %, +1,7 %, −2,1 %, −3,3 %.

Les autres définitions testées (pension payée dans l'année, au 4e trimestre, en décembre) s'éloignent davantage : environ 2 000 retraités de vieillesse de moins que l'annuaire chaque année (A01).

## Autres contrôles sur 2024

- **Par direction (tableau 22).** Les ascendants tombent juste dans les cinq directions ; la retraite vieillesse à 0 à 4 % près (DRO 20 352 pour 20 067, DRD 818 pour 817). Il nous manque surtout des veuves à la DRO (−403) et à la DRB (−617) (A02).
- **Nouveaux pensionnés (tableau 23).** Comptés à la saisie de la pension : 4 090 pour 4 037 publiés (+1,3 %). À la notification : 3 888 (−3,7 %) (A03).
- **Montants.** Retraite vieillesse : 38,50 Md payés pour 38,24 Md publiés (+0,7 %). Branche des pensions : 47,24 Md pour 46,63 Md (+1,3 %) (A01, A05).

## Ce qu'on en déduit

1. **La base est bien celle qui a servi à l'annuaire.** Mêmes types de pension, mêmes directions, mêmes montants à 1 ou 2 % près. Les chiffres de 2020 à 2024 sont exploitables.
2. **La CNSS compte un stock au 31 décembre** : les pensions en cours ce jour-là, titulaire en vie, qu'elles aient été payées ou non. Pour les orphelins, seulement les moins de 25 ans (tableau 20).
3. **Un nouveau pensionné est compté l'année où sa pension est saisie**, pas celle de sa notification.
4. **Les montants publiés sont les montants payés** (débours de paiement au statut D ou P).
5. **On ne retombera jamais à l'unité près.** Notre base est une copie au 02/02/2026 ; la CNSS a compté sur la base de l'époque. Depuis, des pensions ont été créées avec un effet rétroactif (vieillesse : +85 pour 2020, +450 pour 2024).
6. **Les veuves et les orphelins décrochent depuis 2023.** Nos pensions actives perdent 1 088 veuves et 2 012 orphelins en 2023, alors que la CNSS publie une hausse. Hypothèse : des radiations datées de 2023, saisies après la publication. Non vérifiée : c'est l'objet de A06 à A08.

## Ce que la comparaison dit de nos règles

Remarques du 30/10/2026. Les règles R3 à R7 ont été mises à jour en conséquence, à la demande de l'utilisateur, dans `analyses/prestations/README.md` et le schéma Excalidraw `regles_prestations`.

| Règle | Verdict | Ce qui le montre |
| --- | --- | --- |
| R1, R2 : branches, code → ligne | Confirmées | Les 7 lignes du tableau 18 sont nos 7 codes de pension ; les petites lignes tombent à quelques unités près |
| R3 : un paiement = débours PM au statut D ou P, montant du débours | Confirmée | Montants à 0,7–2,5 % près (vieillesse : 38,50 Md pour 38,24) ; branche à 1,3 % près |
| R4 : un paiement compte au mois payé | Confirmée indirectement | Les montants annuels calculés ainsi retombent juste |
| R5 : on compte l'ayant droit, l'orphelin et non le tuteur | Confirmée | Le tableau 20 compte des orphelins de 0 à 24 ans |
| A1 : un accessoire ne fait pas un pensionné de plus | Confirmée | Le tableau 18 ne compte que les 7 types de pension ; les accessoires sont dans le montant de la branche |
| Direction régionale = celle du dossier | Confirmée | Ascendants justes dans les 5 directions ; vieillesse à 0–4 % près |
| R6 : pensionné au 31/12 = pension active et payée au 4e trimestre | Corrigée | La CNSS compte la pension en cours au 31/12, titulaire en vie, payée ou non ; orphelins de moins de 25 ans. Avec le paiement, environ 2 500 retraités de moins en 2024 (−8 %) ; sans, +1,5 % |
| R7 : nouveau pensionné = pension notifiée | Revue | La CNSS compte à la saisie de la pension : 4 090 pour 4 037 publiés (+1,3 %) ; la notification en donne 3 888 (−3,7 %) |
| R8 (âge), R9 (services passés) | Pas encore testées | A04 à relancer |
| D1–D14 (dossiers T25 à T27), I1 (indus), C1 (contrôles) | Rien à en tirer | Le tableau 18 ne parle que de pensionnés |

Autre remarque : nos 8 447 pensions d'orphelin actives sans paiement (P25) sont sans doute des orphelins de 25 ans et plus jamais radiés, que la CNSS ne compte pas.

D'autres chiffres de l'annuaire pourraient tester d'autres règles de la même façon : rentes AT/MP (2 232 bénéficiaires), victimes d'AT/MP déclarées (1 171), enfants d'allocataires (268 481), bénéficiaires d'IJ (3 893).

Schéma Excalidraw de la comparaison : `excalidraw/schemas/annuaire cnss/pensions_2024.excalidraw`.
