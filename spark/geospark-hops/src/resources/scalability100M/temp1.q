SELECT       h0.subj as h0subj, l_o2.strdfgeo as l2geo  
    FROM hasgeometry_79 h0
     INNER JOIN aswkt_3 a1 ON (a1.subj = h0.obj)
     INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)
     INNER JOIN has_code_73 h6 ON (h6.subj = h0.subj)
     INNER JOIN label_values l_code2 ON (l_code2.id = h6.obj) 
    WHERE ( l_code2.value IN ('5622', '5601', '5641', '5621', '5661') )

