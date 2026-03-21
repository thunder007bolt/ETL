-- DIM_GRH_CAT_FRAIS — Source : GRH_CAT_FRAIS (full reload)
-- Colonnes avec # requièrent des guillemets doubles en Oracle
SELECT
    CODE_CFR,
    DESCRIPTION,
    MODE_CALCUL,
    "NO#",
    "CAT#",
    "TARIF#",
    "MONTANT#",
    "TAUX#",
    TYPE_CFR
FROM GRH_CAT_FRAIS
