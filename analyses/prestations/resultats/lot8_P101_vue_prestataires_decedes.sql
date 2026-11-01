-- Texte de la vue USER_DWH.V_PRESTATAIRES_DECEDES_BI (ALL_VIEWS, TEXT_LENGTH 684),
-- relevé le 01/11/2026 sur une capture d'écran de SQL Developer (lot 8, P101).
-- Les colonnes nominatives (IND_NOM, IND_PRENOM, IND_ADRESSE, IND_TELEPHONE,
-- IND_SITUATION_MATRI) ne sont jamais lues par l'équipe BI.
SELECT PE_ID,
       PE_STATUT,
       PE_DATE_EFFET,
       PE_DATE_RADIATION,
       TPE_CODE,
       DOS_CODE,
       IND_ID_PREST,
       IND_ID_BENEF,
       PE_SAL_MOYEN,
       PE_MOIS_COTISATION,
       PE_DATE_INSERT,
       PE_USAGER_INSERT,
       PE_TAUX_INCAPACITE,
       PE_FLAG,
       PE_COT_AUTRE,
       PE_DATE_NOTIFICATION,
       TR_ID,
       IND_NOM,
       IND_PRENOM,
       IND_DATE_NAISSANCE,
       IND_SEXE,
       IND_ADRESSE,
       IND_SITUATION_MATRI,
       IND_TELEPHONE,
       IND_DATE_DECES,
       IND_VERIFIE,
       IND_DATE_INSERT,
       IND_USAGER_INSERT,
       IND_DATE_UPDATE,
       IND_USAGER_UPDATE,
       IND_ACTIF
FROM Prestation_Esp pe, Individu Indi
WHERE pe.ind_id_prest = indi.ind_id
  AND ind_date_deces IS NOT NULL
  AND pe_date_notification IS NOT NULL
