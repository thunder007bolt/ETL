with req  as (
SELECT  EMP_ID
from FAIT_EMPLOYEUR 
where 
EMP_ETAT = 'A' AND 
EMP_ID NOT IN (
    select EMP_ID from FAIT_CONTROLE
    WHERE  EXTRACT(YEAR FROM TO_DATE(CTL_DATE,'DD/MM/YY')) = (select to_char(sysdate, 'YYYY') from dual)
) ),
req1 as (
SELECT EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE,'DD/MM/YY')) AS ANNEE, fc.CTL_TYPE, fe.EMP_ID, COUNT(*) as nbre_controle
from FAIT_EMPLOYEUR fe 
LEFT JOIN FAIT_CONTROLE fc on fc.EMP_ID = fe.EMP_ID
WHERE EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE,'DD/MM/YY')) = (select to_char(sysdate, 'YYYY') from dual)   and fe.EMP_ETAT = 'A'   
GROUP BY EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE,'DD/MM/YY')), fc.CTL_TYPE, fe.EMP_ID )

select to_char (annee), nbre_controle, count(EMP_ID)as nbre from req1
group by annee, nbre_controle
union all 
select to_char(sysdate, 'YYYY') as annee, 0 as nbre_controle , count(*) as nbre from req

order by nbre_controle


SELECT 
    EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE,'DD/MM/YY')) AS annee,
    TO_CHAR(TO_DATE(fc.CTL_DATE,'DD/MM/YY'), 'Q') AS trimestre,
    fc.CTL_TYPE,
    COUNT(fc.EMP_ID)
FROM FAIT_CONTROLE fc
LEFT JOIN FAIT_EMPLOYEUR fe on fc.EMP_ID = fe.EMP_ID
WHERE fe.EMP_ETAT = 'A' AND EXTRACT(YEAR FROM  TO_DATE(fc.CTL_DATE,'DD/MM/YY')) = 2025
GROUP BY 
    EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE,'DD/MM/YY')),
    TO_CHAR(TO_DATE(fc.CTL_DATE,'DD/MM/YY'), 'Q'),
    fc.CTL_TYPE
ORDER BY annee, trimestre;