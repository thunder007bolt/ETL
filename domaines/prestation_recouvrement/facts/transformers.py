"""
Transformations métier spécifiques aux tables de faits.

Chaque fonction :
    - reçoit un DataFrame extrait de la source
    - retourne un DataFrame prêt pour le chargement
    - ne doit pas modifier les colonnes clés déclarées dans fact_config

Convention de nommage : transform_<nom_court_fait>
"""

import pandas as pd
