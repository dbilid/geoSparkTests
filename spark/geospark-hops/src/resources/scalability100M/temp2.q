 SELECT h4.subj as h4subj, l_o1.strdfgeo as l1geo FROM 
      geo_values l_o1  
     INNER JOIN aswkt_3 a3 ON (a3.obj = l_o1.id)
     INNER JOIN hasgeometry_79 h4 ON (h4.obj = a3.subj)
     INNER JOIN has_code_73 h5 ON (h5.obj =  '1879048216'
     AND h5.subj = h4.subj) 
