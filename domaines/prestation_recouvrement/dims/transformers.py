import pandas as pd

def transform_assure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les colonnes dérivées de DIM_ASSURE :
      - ID_ASSURE          : clé surrogate = IND_ID (naturelle, unique NUMBER(10))
      - LIBELLE_SEXE       : 1 → Masculin | 2 → Féminin
      - TRANCHE_AGE        : 9 tranches (<20 … 75+) ; None si AGE indisponible
      - LIBELLE_SIT_MATRI  : C/M/V/D → libellé français
      - LIBELLE_TYPE_ENFANT: L/N/A/R → libellé français
      - LIBELLE_ROLE       : rôle bénéficiaire en clair
                             NULL ROLE_BENEFICIAIRE → "Assuré principal"

    Préconditions (garanties par le pipeline) :
      - colonnes SEXE, AGE, SITUATION_MATRIMONIALE, TYPE_ENFANT,
        ROLE_BENEFICIAIRE, IND_ID présentes dans df.
    """


    # 1. Clé surrogate : IND_ID est unique NUMBER(10), pas besoin de séquence
    df["ID_ASSURE"] = df["IND_ID"]

    # 2. Libellé sexe
    df["LIBELLE_SEXE"] = df["SEXE"].map({1: "Masculin", 2: "Féminin"})

    # 3. Tranche d'âge
    #    pd.cut requiert un numérique flottant ; Int64 nullable → float via to_numeric
    _bins   = [-1,  19,  29,  39,  49,  59,  64,  69,  74, float("inf")]
    _labels = ["<20", "20-29", "30-39", "40-49", "50-59",
               "60-64", "65-69", "70-74", "75+"]
    age_f = pd.to_numeric(df["AGE"], errors="coerce")
    df["TRANCHE_AGE"] = (
        pd.cut(age_f, bins=_bins, labels=_labels, right=True)
        .astype(object)                        # Categorical → object pour Oracle
    )
    df.loc[age_f.isna(), "TRANCHE_AGE"] = None  # âge inconnu → NULL

    # 4. Libellé situation matrimoniale
    _sit_matri = {
        "C": "Célibataire",
        "M": "Marié(e)",
        "V": "Veuf(ve)",
        "D": "Divorcé(e)",
    }
    df["LIBELLE_SIT_MATRI"] = df["SITUATION_MATRIMONIALE"].map(_sit_matri)

    # 5. Libellé type enfant (NULL = adulte, ~45 % des cas)
    _type_enf = {
        "L": "Légitime",
        "N": "Naturel",
        "A": "Adoptif",
        "R": "Recueilli",
    }
    df["LIBELLE_TYPE_ENFANT"] = df["TYPE_ENFANT"].map(_type_enf)

    # 6. Libellé rôle bénéficiaire
    #    NULL ROLE_BENEFICIAIRE = assuré principal (BEN_LN_TYPE='M' exclu à l'extraction)
    _roles = {
        "C": "Conjoint",
        "E": "Enfant",
        "A": "Ascendant",
        "R": "Recours",
    }
    df["LIBELLE_ROLE"] = df["ROLE_BENEFICIAIRE"].map(_roles).fillna("Assuré principal")

    return df
