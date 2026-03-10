-- Donner les droits sur toutes les tables du schema DTM
BEGIN
    FOR t IN (
        SELECT table_name 
        FROM all_tables 
        WHERE owner = 'DTM'
    )
    LOOP
        EXECUTE IMMEDIATE 
        'GRANT SELECT, INSERT, UPDATE, DELETE ON DTM.' 
        || t.table_name || 
        ' TO DWH';
    END LOOP;
END;
/
BEGIN
    FOR s IN (
        SELECT sequence_name 
        FROM all_sequences 
        WHERE sequence_owner = 'DTM'
    )
    LOOP
        EXECUTE IMMEDIATE 
        'GRANT SELECT ON DTM.' 
        || s.sequence_name || 
        ' TO DWH';
    END LOOP;
END;
/