DO $$
DECLARE
   N INTEGER := 2000; 
   step INTEGER := 0;
   rows_count INTEGER;
BEGIN
   -- Получение общего количества строк в таблице reguh
   SELECT COUNT(*) INTO rows_count FROM core.reguh;
   
   -- Цикл по выборке строк reguh с шагом N
   WHILE step <= rows_count LOOP
      WITH selected_rows AS (
         SELECT *
         FROM core.reguh
         ORDER BY pyord, laufd 
         LIMIT N OFFSET step
      )
      INSERT INTO cdm.metrica_1(bukrs, pyord, belnr, gjahr, hbkid, zbukr, butxt, laufd, text1, waers_b, bankn, 
								rbetr, zz_bezakc, zz_pyord_r, zz_zh2h, kblnr, waers_t, zz_vblnr, awkey, zz_rfha, 
								partner, name_first, namemiddle, name_last, zz_bukrs, zz_fdatk, zz_hbkid, bkont, 
								zz_rfhazu, zz_rfhazb, zz_dcrdat, zz_tcrtim, wtges, banka)
      SELECT DISTINCT ON (regup.bukrs, reguh.pyord, kblk.belnr, regup.gjahr, reguh.hbkid, reguh.zbukr)
         regup.bukrs AS bukrs, reguh.pyord AS pyord, kblk.belnr AS belnr, regup.gjahr AS gjahr, reguh.hbkid AS hbkid, 
		 reguh.zbukr AS zbukr, t001.butxt AS butxt, reguh.laufd AS laufd, t012.text1 AS text1, bkpf.waers_b AS waers_b, 
		 t012.numsfh AS numsfh, reguh.rbetr AS rbetr, kblk.zz_bezakc AS zz_bezakc, kblk.zz_pyord_r AS zz_pyord_r, 
		 kblk.zz_zh2h AS zz_zh2h, regup.kblnr AS kblnr, t012.waers_t AS waers_t, kblk.zz_vblnr AS zz_vblnr, bkpf.awkey AS awkey, 
		 kblp.zz_rfha AS zz_rfha, but000.partner AS partner, but000.name_first AS name_first, but000.namemiddle AS namemiddle, 
		 but000.name_last AS name_last, kblk.zz_bukrs AS zz_bukrs, kblk.zz_fdatk AS zz_fdatk, kblk.zz_hbkid AS zz_hbkid, 
		 t012.bkont AS bkont, kblp.zz_rfhazu AS zz_rfhazu, kblp.zz_rfhazb AS zz_rfhazb, kblp.zz_dcrdat AS zz_dcrdat, 
		 kblp.zz_tcrtim AS zz_tcrtim, kblp.wtges AS wtges, bnka.banka AS banka
      FROM core.reguh 
      LEFT JOIN core.regup ON reguh.pyord = regup.pyord
                           AND reguh.laufi = regup.laufi
                           AND reguh.zbukr = regup.zbukr 
			               AND reguh.laufd = regup.laufd							
      LEFT JOIN core.kblk ON regup.kblnr = kblk.belnr
      LEFT JOIN core.bkpf ON regup.belnr = bkpf.belnr
                          AND regup.gjahr = bkpf.gjahr
                          AND regup.bukrs = bkpf.bukrs
      LEFT JOIN core.kblp ON kblk.belnr = kblp.belnr
      LEFT JOIN core.t012 ON regup.bukrs = t012.bukrs
                          AND reguh.hbkid = t012.hbkid
      LEFT JOIN core.t001 ON regup.bukrs = t001.bukrs
      LEFT JOIN core.bnka ON t012.banks = bnka.banks
                          AND t012.bankl = bnka.bankl
      LEFT JOIN core.but000 ON regup.zz_partner = but000.partner
      WHERE regup.bukrs IS NOT NULL 
         AND reguh.pyord IS NOT NULL
         AND kblk.belnr IS NOT NULL
         AND regup.gjahr IS NOT NULL            
         AND reguh.hbkid IS NOT NULL
         AND reguh.zbukr IS NOT NULL
         AND reguh.laufd IS NOT NULL
		 AND reguh.laufd >= '2022-01-01 00:00:00'
         AND NOT EXISTS (SELECT 1 FROM cdm.metrica_1
         WHERE cdm.metrica_1.bukrs = regup.bukrs
           AND cdm.metrica_1.pyord = reguh.pyord
           AND cdm.metrica_1.belnr = kblk.belnr
           AND cdm.metrica_1.gjahr = regup.gjahr
           AND cdm.metrica_1.hbkid = reguh.hbkid
           AND cdm.metrica_1.zbukr = reguh.zbukr)
         ORDER BY regup.bukrs, reguh.pyord, kblk.belnr, regup.gjahr, reguh.hbkid, reguh.zbukr, reguh.pyord DESC, regup.laufd DESC; 
   
      step := step + N;
   END LOOP;
END $$;