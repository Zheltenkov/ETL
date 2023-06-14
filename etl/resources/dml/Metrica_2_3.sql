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
      WITH selected_rows AS (SELECT * FROM core.vtbfha 
							 WHERE core.vtbfha.sgsart IN ('A30', 'A31', 'A20')
							 ORDER BY bukrs, rfha, dcrdat_ha, tcrtim_ha 
         					 LIMIT N OFFSET step)
      INSERT INTO cdm.metrica_2_3(bukrs, rfha, dcrdat, tcrtim, zz_hbkid, zuond, banks, 
		             zz_num_reg, nordext, sgsart, sfhaart, zz_bankname, wgschft, kontrh, 
					 dbuchung, ssign, bzbetr, dis_flowtype, posting_key, position_amt, 
					 position_curr, sfhazba, limit_date, rfha_relat_obj, limit_currency, 
					 limit_pos_amount, rfhazu, rfhazb, dzterm, saktiv, sbewebe, acpostingdate, 
					 facilitynr, name_org1, name_org2, name_org3, name_org4, sgsart_name)
      SELECT DISTINCT ON (vtbfha.bukrs, vtbfha.rfha, vtbfhapo.rfhazu, vtbfhapo.rfhazb, 
		                  vtbfhapo.dcrdat, vtbfhapo.tcrtim)
                          vtbfha.bukrs, vtbfha.rfha, vtbfhapo.dcrdat, vtbfhapo.tcrtim, 
						  draw.zz_hbkid, vtbfha.zuond, t012.banks, draw.zz_num_reg, 
						  vtbfhazu.nordext, vtbfha.sgsart, vtbfha.sfhaart, draw.zz_bankname, 
						  vtbfha.wgschft, vtbfha.kontrh, vtbfhapo.dbuchung, vtbfhapo.ssign, 
						  vtbfhapo.bzbetr, tracv_accitem.dis_flowtype, tracv_accitem.posting_key, 
						  tracv_accitem.position_amt, tracv_accitem.position_curr, vtbfhapo.sfhazba, 
						  vtb_asgn_limit.limit_date, vtb_asgn_limit.rfha_relat_obj, 
						  vtb_asgn_limit.limit_currency, vtb_asgn_limit.limit_pos_amount, 
						  vtbfhapo.rfhazu, vtbfhapo.rfhazb, vtbfhapo.dzterm, vtbfha.saktiv, 
						  vtbfhapo.sbewebe, tracv_accitem.acpostingdate, vtbfha.facilitynr,
						  but000.name_org1, but000.name_org2, but000.name_org3, but000.name_org4,
						  CASE
							WHEN vtbfha.sgsart = 'A20' THEN 'Коммерческий займ'
							WHEN vtbfha.sgsart = 'A30' THEN 'Внутригрупповой займ'
							WHEN vtbfha.sgsart = 'A31' THEN 'Cash-pooling'
							ELSE NULL
						  END AS sgsart_name
      FROM selected_rows AS vtbfha
	  LEFT JOIN core.vtbfhapo ON vtbfha.bukrs = vtbfhapo.bukrs 
		                      AND vtbfha.rfha = vtbfhapo.rfha
      LEFT JOIN core.vtbfhazu ON vtbfha.rfhazul = vtbfhazu.rfhazu 
		                      AND vtbfha.bukrs = vtbfhazu.bukrs 
							  AND vtbfha.rfha = vtbfhazu.rfha
      LEFT JOIN core.tracv_accitem ON vtbfha.bukrs = tracv_accitem.company_code 
		                           AND vtbfha.rfha = tracv_accitem.deal_number
      LEFT JOIN core.vtb_asgn_limit ON vtbfha.facilitybukrs = vtb_asgn_limit.bukrs_relat_obj 
		                            AND vtbfha.facilitynr = vtb_asgn_limit.rfha_relat_obj
      LEFT JOIN core.draw ON vtbfha.zuond = draw.zz_doknr
      LEFT JOIN core.t012 ON vtbfha.bukrs = t012.bukrs 
		                  AND vtbfha.sfhaart = t012.hbkid
	  LEFT OUTER JOIN core.but000 ON draw.zz_bu_partner = core.but000.partner
      WHERE core.vtbfhapo.dcrdat IS NOT NULL
      AND core.vtbfhapo.tcrtim IS NOT NULL
	  AND vtb_asgn_limit.limit_date IS NOT NULL
	  ORDER BY vtbfhapo.dcrdat DESC, vtbfhapo.tcrtim DESC; 
   
      step := step + N;
   END LOOP;
END $$;