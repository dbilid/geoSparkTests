 SELECT 
  temp2.h4subj,  u_s1.value,   temp1.h0subj,  u_s2.value 
FROM temp1 INNER JOIN temp2 on ((ST_Intersects(temp1.l2geo,temp2.l1geo))) 
     INNER JOIN uri_values u_s1 ON (u_s1.id = temp2.h4subj) 
     INNER JOIN uri_values u_s2 ON (u_s2.id = temp1.h0subj) 
