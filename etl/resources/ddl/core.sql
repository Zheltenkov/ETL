DROP TABLE IF EXISTS core.pyordh;
DROP TABLE IF EXISTS core.pyordp;
DROP TABLE IF EXISTS core.vtbfhazu;
DROP TABLE IF EXISTS core.kblk;
DROP TABLE IF EXISTS core.bseg;
DROP TABLE IF EXISTS core.bkpf;
DROP TABLE IF EXISTS core.vtbfha;
DROP TABLE IF EXISTS core.vtbfhapo;
DROP TABLE IF EXISTS core.t012;
DROP TABLE IF EXISTS core.t001;
DROP TABLE IF EXISTS core.but000;
DROP TABLE IF EXISTS core.draw;
DROP TABLE IF EXISTS core.vtb_asgn_limit;
DROP TABLE IF EXISTS core.tracv_accitem;
--DROP TABLE IF EXISTS core.kblp;

CREATE TABLE core.reguh(
"pyord" Varchar(10) NOT NULL,
"laufi" Varchar(6) NOT NULL,
"xvorl" Varchar(1) NOT NULL,
"vblnr" Varchar(20) NOT NULL,
"laufd" timestamp(6) NOT NULL,
"gjahr" INT,
"waers"	Varchar(10),
"zbukr"	Varchar(10),
"hbkid" Varchar(10),
"rbetr" FLOAT(4),
CONSTRAINT pk_raw_reguh PRIMARY KEY (pyord,laufi,xvorl,vblnr,laufd)
);


CREATE TABLE core.regup(
"pyord" Varchar(20) NOT NULL,
"bukrs"	Varchar(8) NOT NULL,
"belnr"	Varchar(20) NOT NULL,
"zbukr" Varchar(10),
"gjahr" INT NOT NULL,
"buzei" INT NOT NULL,
"kblnr" Varchar(20),
"laufi" Varchar(6) NOT NULL,
"zz_partner" Varchar(20),
CONSTRAINT pk_raw_regup PRIMARY KEY (pyord,bukrs,belnr,gjahr,buzei,laufi)
);

CREATE TABLE core.kblk(
"belnr" Varchar(20) NOT NULL,
"kerdat" timestamp(6),
"gjahr" INT,
"zz_bezakc" Varchar(2),
"zz_pyord_r" Varchar(20),
"zz_zh2h" Varchar(2),
"zz_vblnr" Varchar(20),
"zz_bukrs" VARCHAR(8),
"zz_hbkid" VARCHAR(10),
CONSTRAINT pk_core_kblk PRIMARY KEY (belnr)
);


CREATE TABLE core.bseg(
"bukrs" Varchar(8) NOT NULL,
"belnr" Varchar(20) NOT NULL,
"gjahr" INT NOT NULL,
"kblnr" Varchar(20),
"buzei" INT NOT NULL,
"wrbtr" FLOAT,
CONSTRAINT pk_core_bseg PRIMARY KEY (bukrs,belnr,gjahr,buzei)
);


CREATE TABLE core.bkpf(
"bukrs" Varchar(8) NOT NULL,
"belnr" Varchar(20) NOT NULL,
"gjahr" INT NOT NULL,
"waers_b" Varchar(20),
"awkey" Varchar(20),
"xreversal" VARCHAR(1),
CONSTRAINT pk_core_bkpf PRIMARY KEY (bukrs,belnr,gjahr)
);


CREATE TABLE core.vtbfha(
"bukrs" Varchar(8) NOT NULL,
"rfha" Varchar(26) NOT NULL,
"dcrdat_ha" date NOT NULL,
"tcrtim_ha" time(3) NOT NULL,
"sgsart" Varchar(6),
"sfhaart" Varchar(6),
"zuond" Varchar(36),
"saktiv" INT,
"wgschft" Varchar(20),
"kontrh" Varchar(20),
"rfhazul" INT,
CONSTRAINT pk_core_vtbfha PRIMARY KEY (bukrs,rfha)
);

CREATE TABLE core.vtbfhapo(
"bukrs" Varchar(8) NOT NULL,
"rfhazu" INT,
"rfha" Varchar(26) NOT NULL,
"dcrdat" date NOT NULL,
"gjahr" INT NOT NULL,
"tcrtim" time(3),
"rfhazb" INT,
"sfhazba" Varchar(8),
"ssign" Varchar(2),
"dzterm" date,
"bzbetr" Float,
"dbuchung" date,
"sbewebe" Varchar(1),
"dberbis" date,
"khwkurs" Float,
"dbervon" date,
"wzbetr" Varchar(5),
CONSTRAINT pk_raw_vtbfhapo PRIMARY KEY (bukrs,rfhazu,rfha,dcrdat,gjahr,tcrtim,rfhazb)
);

CREATE TABLE core.vtbfhazu(
"bukrs" Varchar(8) NOT NULL,
"rfhazu" INT,
"rfha" Varchar(26) NOT NULL,
"dcrdat_zu" date NOT NULL,
"tcrtim_zu" time(3) NOT NULL,
"nordext" Varchar(32),
CONSTRAINT pk_core_vtbfhazu PRIMARY KEY (bukrs,rfhazu,rfha)
);

CREATE TABLE core.t012(
"bukrs" Varchar(8) NOT NULL,
"hbkid" Varchar(10) NOT NULL,
"banks" Varchar(6),
"text1" Varchar(80),
"numsfh" Varchar(80),
"waers_t" Varchar(50),
CONSTRAINT pk_core_t012 PRIMARY KEY (bukrs,hbkid)
);

CREATE TABLE core.t001(
"bukrs" Varchar(8) NOT NULL,
"butxt" Varchar(50),
CONSTRAINT pk_core_t001 PRIMARY KEY (bukrs)
);

CREATE TABLE core.but000(
"partner" Varchar(20) NOT NULL,
"name_org1" Varchar(80),
"name_org2" Varchar(80),
"name_org3" Varchar(80),
"name_org4" Varchar(80),
CONSTRAINT pk_core_but000 PRIMARY KEY (partner)
);

CREATE TABLE core.draw(
"dokar" Varchar(6) NOT NULL,
"doknr" Varchar(50) NOT NULL,
"zz_num_reg" Varchar(50),
"zz_hbkid" Varchar(10),
"zz_vvsart" Varchar(6),
"zz_sfhaart" Varchar(6),
"zz_bankname" Varchar(120),
"zz_begda" Date,
"zz_doknr" Varchar(50),
CONSTRAINT pk_core_draw PRIMARY KEY (dokar,doknr)
);

CREATE TABLE core.vtb_asgn_limit(
"relat_obj" VARCHAR(22) not null,
"limit_date" TIMESTAMP(6) not null,
"bukrs_relat_obj" VARCHAR(8),
"rfha_relat_obj" VARCHAR(26) not null,
"limit_currency" VARCHAR(10) not null,
"limit_pos_amount" FLOAT,
CONSTRAINT pk_core_vtb_asgn_limit PRIMARY KEY (relat_obj,limit_date,rfha_relat_obj,limit_currency)
);

CREATE TABLE core.tracv_accitem(
"company_code" VARCHAR(6),
"deal_number" VARCHAR(26),
"acpostingdate" DATE,
"dis_flowtype" VARCHAR(8),
"posting_key" VARCHAR(2),
"position_amt" FLOAT,
"position_curr" VARCHAR(10),
"document_guid" VARCHAR(32),
"item_number" INTEGER,
"os_guid_pi" VARCHAR(32),
CONSTRAINT pk_core_tracv_accitem PRIMARY KEY (document_guid,item_number,os_guid_pi)
);

CREATE TABLE core.kblp(
"belnr" VARCHAR(20),
"blpos" INT,
"zz_rfha" VARCHAR(26),
"zz_rfhazu" INT,
"zz_rfhazb" INT,
"zz_dcrdat" DATE,
"zz_tcrtim" TIME(3),
"wtges" FLOAT(4),
CONSTRAINT pk_core_kblp PRIMARY KEY (belnr,blpos)
);
