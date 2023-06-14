DO $$
DECLARE
   N INTEGER := 1000; 
   step INTEGER := 0;
   rows_count INTEGER;
BEGIN
   -- Получение общего количества строк в таблице vtbfha
   SELECT COUNT(*) INTO rows_count FROM core.vtbfha;
   
   -- Цикл по выборке строк vtbfha с шагом N
   WHILE step <= rows_count LOOP
      WITH selected_rows AS (SELECT * FROM core.vtbfha ORDER BY bukrs, rfha, dcrdat_ha, tcrtim_ha 
         LIMIT N OFFSET step
      )

      INSERT INTO cdm.metrica_4 (bukrs, rfha, dcrdat, tcrtim, zuond, 
                                 zz_num_reg, kontrh, butxt, sgsart, 
                                 sfhaart, wgschft, ssign, dzterm, bzbetr,
                                 rfhazu, rfhazb, zz_begda, sfhazba,
                                 sbewebe, dberbis, khwkurs, dbervon, wzbetr, 
								 name_org1, name_org2, name_org3, name_org4)
      SELECT DISTINCT ON (vtbfha.bukrs, vtbfha.rfha, vtbfhapo.dcrdat, vtbfhapo.tcrtim,
                          vtbfhapo.rfhazu, vtbfhapo.rfhazb)
         vtbfha.bukrs, vtbfha.rfha, vtbfhapo.dcrdat, vtbfhapo.tcrtim, 
         vtbfha.zuond, draw.zz_num_reg, vtbfha.kontrh, t001.butxt, 
         vtbfha.sgsart, vtbfha.sfhaart, vtbfha.wgschft, vtbfhapo.ssign, 
         vtbfhapo.dzterm, vtbfhapo.bzbetr, vtbfhapo.rfhazu, vtbfhapo.rfhazb, 
         draw.zz_begda, vtbfhapo.sfhazba, vtbfhapo.sbewebe, vtbfhapo.dberbis, 
         vtbfhapo.khwkurs, vtbfhapo.dbervon, vtbfhapo.wzbetr, but000.name_org1, 
		 but000.name_org2, but000.name_org3, but000.name_org4
      FROM selected_rows AS vtbfha
      LEFT OUTER JOIN core.vtbfhapo ON vtbfha.bukrs = core.vtbfhapo.bukrs
                                   AND vtbfha.rfha = core.vtbfhapo.rfha
      LEFT OUTER JOIN core.draw ON vtbfha.zuond = core.draw.zz_doknr
      LEFT OUTER JOIN core.t001 ON core.vtbfhapo.bukrs = core.t001.bukrs
	  LEFT OUTER JOIN core.but000 ON draw.zz_bu_partner = core.but000.partner
      WHERE core.vtbfhapo.dcrdat IS NOT NULL
      AND core.vtbfhapo.tcrtim IS NOT NULL
	  ORDER BY vtbfhapo.dcrdat DESC, vtbfhapo.tcrtim DESC; 
      step := step + N;
   END LOOP;
END $$;