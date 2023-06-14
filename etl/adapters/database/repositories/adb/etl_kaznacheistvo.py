from typing import List

from sqlalchemy import select, and_, null
from sqlalchemy.orm import Session, aliased

from adapters.database.tables.adb.cdm import (Metrica1, Metrica23, Metrica4)
from adapters.database.tables.adb.core import (But000)
from adapters.database.tables.adb.raw import (RawReguh, RawRegup, RawKblk, RawBseg, RawBkpf, RawVtbfha, RawVtbfhapo,
                                              RawVtbfhazu,RawKblp,
                                              RawT012, RawT001, RawDraw, RawVtbAsgnLimit, RawTracvAccitem)


class EtlKaznacheistvoRepo:
    def processMetrica1(self, session: Session, rawMetrica1: List[Metrica1]):
        print(f'Processing Metrica1')
        metrica_1: List[Metrica1] = session.query(Metrica1).all()
        metrica_1_dict = {(rawRow.bukrs, rawRow.pyord, rawRow.belnr, rawRow.gjahr, rawRow.hbkid, rawRow.zbukr): rawRow
                          for rawRow in metrica_1}
        updated_objects = []
        new_objects = []
        raw_metrica_1_dict = {
            (rawRow.bukrs, rawRow.pyord, rawRow.belnr, rawRow.gjahr, rawRow.hbkid, rawRow.zbukr): rawRow
            for rawRow in rawMetrica1}
        for key, rawRow in raw_metrica_1_dict.items():
            if (rawRow.bukrs, rawRow.pyord, rawRow.belnr, rawRow.gjahr, rawRow.hbkid, rawRow.zbukr) in metrica_1_dict:
                if metrica_1_dict[(rawRow.bukrs, rawRow.pyord,
                                   rawRow.belnr, rawRow.gjahr, rawRow.hbkid, rawRow.zbukr)] != rawRow:
                    updated_obj: Metrica1 = metrica_1_dict[
                        (rawRow.bukrs, rawRow.pyord, rawRow.belnr, rawRow.gjahr, rawRow.hbkid, rawRow.zbukr)]
                    updated_obj.bukrs = rawRow.bukrs
                    updated_obj.pyord = rawRow.pyord
                    updated_obj.belnr = rawRow.belnr
                    updated_obj.gjahr = rawRow.gjahr
                    updated_obj.hbkid = rawRow.hbkid
                    updated_obj.zbukr = rawRow.zbukr
                    updated_obj.butxt = rawRow.butxt
                    updated_obj.laufd = rawRow.laufd
                    updated_obj.text1 = rawRow.text1
                    updated_obj.waers_b = rawRow.waers_b
                    updated_obj.numsfh = rawRow.numsfh
                    updated_obj.rbetr = rawRow.rbetr
                    updated_obj.zz_bezakc = rawRow.zz_bezakc
                    updated_obj.zz_pyord_r = rawRow.zz_pyord_r
                    updated_obj.zz_zh2h = rawRow.zz_zh2h
                    updated_obj.kblnr = rawRow.kblnr
                    updated_obj.waers_t = rawRow.waers_t
                    updated_obj.zz_vblnr = rawRow.zz_vblnr
                    updated_obj.awkey = rawRow.awkey
                    updated_obj.zz_rfha = rawRow.zz_rfha
                    updated_obj.partner = rawRow.partner
                    updated_obj.name_org1 = rawRow.name_org1
                    updated_obj.name_org2 = rawRow.name_org2
                    updated_obj.name_org3 = rawRow.name_org3
                    updated_obj.name_org4 = rawRow.name_org4
                    updated_objects.append(updated_obj)
                else:
                    pass
            else:
                obj = Metrica1(bukrs=rawRow.bukrs,
                               pyord=rawRow.pyord,
                               belnr=rawRow.belnr,
                               gjahr=rawRow.gjahr,
                               hbkid=rawRow.hbkid,
                               zbukr=rawRow.zbukr,
                               butxt=rawRow.butxt,
                               laufd=rawRow.laufd,
                               text1=rawRow.text1,
                               waers_b=rawRow.waers_b,
                               numsfh=rawRow.numsfh,
                               rbetr=rawRow.rbetr,
                               zz_bezakc=rawRow.zz_bezakc,
                               zz_pyord_r=rawRow.zz_pyord_r,
                               zz_zh2h=rawRow.zz_zh2h,
                               kblnr=rawRow.kblnr,
                               waers_t=rawRow.waers_t,
                               zz_vblnr=rawRow.zz_vblnr,
                               awkey=rawRow.awkey,
                               zz_rfha=rawRow.zz_rfha,
                               partner=rawRow.partner,
                               name_org1=rawRow.name_org1,
                               name_org2=rawRow.name_org2,
                               name_org3=rawRow.name_org3,
                               name_org4=rawRow.name_org4
                               )
                new_objects.append(obj)
        for obj in updated_objects:
            session.add(obj)
        session.flush()
        session.commit()
        print(f'Total updated rows: {len(updated_objects)}')
        session.add_all(new_objects)
        session.flush()
        session.commit()
        print(f'Total new inserted rows: {len(new_objects)}')

    def processMetrica23(self, session: Session, rawMetrica23: List[Metrica23]):
        print(f'Processing Metrica23')
        metrica_2_3: List[Metrica23] = session.query(Metrica23).all()
        metrica_2_3_dict = {(rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb,
                             rawRow.dokar, rawRow.doknr, rawRow.document_guid, rawRow.item_number,
                             rawRow.os_guid_pi): rawRow
                            for rawRow in metrica_2_3}
        updated_objects = []
        new_objects = []
        raw_metrica_2_3_dict = {
            (rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb, rawRow.dokar,
             rawRow.doknr, rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi): rawRow
            for rawRow in rawMetrica23}
        for key, rawRow in raw_metrica_2_3_dict.items():
            if (rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb, rawRow.dokar,
                rawRow.doknr, rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi) in metrica_2_3_dict:
                if metrica_2_3_dict[(
                        rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb,
                        rawRow.dokar,
                        rawRow.doknr, rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi)] != rawRow:
                    updated_obj: Metrica23 = metrica_2_3_dict[(
                        rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb,
                        rawRow.dokar,
                        rawRow.doknr, rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi)]
                    updated_obj.bukrs = rawRow.bukrs
                    updated_obj.rfha = rawRow.rfha
                    updated_obj.dcrdat = rawRow.dcrdat
                    updated_obj.tcrtim = rawRow.tcrtim
                    updated_obj.zz_hbkid = rawRow.zz_hbkid
                    updated_obj.zuond = rawRow.zuond
                    updated_obj.banks = rawRow.banks
                    updated_obj.zz_num_reg = rawRow.zz_num_reg
                    updated_obj.nordext = rawRow.nordext
                    updated_obj.sgsart = rawRow.sgsart
                    updated_obj.sfhaart = rawRow.sfhaart
                    updated_obj.zz_bankname = rawRow.zz_bankname
                    updated_obj.wgschft = rawRow.wgschft
                    updated_obj.kontrh = rawRow.kontrh
                    updated_obj.dbuchung = rawRow.dbuchung
                    updated_obj.ssign = rawRow.ssign
                    updated_obj.bzbetr = rawRow.bzbetr
                    updated_obj.dis_flowtype = rawRow.dis_flowtype
                    updated_obj.posting_key = rawRow.posting_key
                    updated_obj.position_amt = rawRow.position_amt
                    updated_obj.position_curr = rawRow.position_curr
                    updated_obj.sfhazba = rawRow.sfhazba
                    updated_obj.limit_date = rawRow.limit_date
                    updated_obj.rfha_relat_obj = rawRow.rfha_relat_obj
                    updated_obj.limit_currency = rawRow.limit_currency
                    updated_obj.limit_pos_amount = rawRow.limit_pos_amount
                    updated_obj.dokar = rawRow.dokar,
                    updated_obj.doknr = rawRow.doknr,
                    updated_obj.document_guid = rawRow.document_guid,
                    updated_obj.item_number = rawRow.item_number,
                    updated_obj.os_guid_pi = rawRow.os_guid_pi
                    updated_objects.append(updated_obj)
                else:
                    pass
            else:
                obj = Metrica23(bukrs=rawRow.bukrs,
                                rfha=rawRow.rfha,
                                dcrdat=rawRow.dcrdat,
                                tcrtim=rawRow.tcrtim,
                                zz_hbkid=rawRow.zz_hbkid,
                                zuond=rawRow.zuond,
                                banks=rawRow.banks,
                                zz_num_reg=rawRow.zz_num_reg,
                                nordext=rawRow.nordext,
                                sgsart=rawRow.sgsart,
                                sfhaart=rawRow.sfhaart,
                                zz_bankname=rawRow.zz_bankname,
                                wgschft=rawRow.wgschft,
                                kontrh=rawRow.kontrh,
                                dbuchung=rawRow.dbuchung,
                                ssign=rawRow.ssign,
                                bzbetr=rawRow.bzbetr,
                                dis_flowtype=rawRow.dis_flowtype,
                                posting_key=rawRow.posting_key,
                                position_amt=rawRow.position_amt,
                                position_curr=rawRow.position_curr,
                                sfhazba=rawRow.sfhazba,
                                limit_date=rawRow.limit_date,
                                rfha_relat_obj=rawRow.rfha_relat_obj,
                                limit_currency=rawRow.limit_currency,
                                limit_pos_amount=rawRow.limit_pos_amount,
                                dokar=rawRow.dokar,
                                doknr=rawRow.doknr,
                                document_guid=rawRow.document_guid,
                                item_number=rawRow.item_number,
                                os_guid_pi=rawRow.os_guid_pi
                                )
                new_objects.append(obj)
        for obj in updated_objects:
            session.add(obj)
        session.flush()
        session.commit()
        print(f'Total updated rows: {len(updated_objects)}')
        session.add_all(new_objects)
        session.flush()
        session.commit()
        print(f'Total new inserted rows: {len(new_objects)}')

    def processMetrica4(self, session: Session, rawMetrica4: List[Metrica4]):
        print(f'Processing Metrica4')
        metrica_4: List[Metrica4] = session.query(Metrica4).all()
        metrica_4_dict = {
            (rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb): rawRow
            for rawRow in metrica_4}
        updated_objects = []
        new_objects = []
        raw_metrica_4_dict = {(rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu,
                               rawRow.rfhazb): rawRow
                              for rawRow in rawMetrica4}
        for key, rawRow in raw_metrica_4_dict.items():
            if (
                    (rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu,
                     rawRow.rfhazb)) in metrica_4_dict:
                if metrica_4_dict[
                    rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb] != rawRow:
                    updated_obj: Metrica4 = metrica_4_dict[
                        (rawRow.bukrs, rawRow.rfha, rawRow.dcrdat, rawRow.tcrtim, rawRow.rfhazu, rawRow.rfhazb)]
                    updated_obj.bukrs = rawRow.bukrs
                    updated_obj.rfha = rawRow.rfha
                    updated_obj.dcrdat = rawRow.dcrdat
                    updated_obj.tcrtim = rawRow.tcrtim
                    updated_obj.zuond = rawRow.zuond
                    updated_obj.zz_num_reg = rawRow.zz_num_reg
                    updated_obj.zz_bankname = rawRow.zz_bankname
                    updated_obj.butxt = rawRow.butxt
                    updated_obj.sgsart = rawRow.sgsart
                    updated_obj.sfhaart = rawRow.sfhaart
                    updated_obj.wgschft = rawRow.wgschft
                    updated_obj.ssign = rawRow.ssign
                    updated_obj.dzterm = rawRow.dzterm
                    updated_obj.bzbetr = rawRow.bzbetr
                    updated_obj.rfhazu = rawRow.rfhazu
                    updated_obj.rfhazb = rawRow.rfhazb
                    updated_obj.zz_begda = rawRow.zz_begda
                    updated_obj.sfhazba = rawRow.sfhazba
                    updated_obj.sbewebe = rawRow.sbewebe
                    updated_obj.dberbis = rawRow.dberbis
                    updated_obj.khwkurs = rawRow.khwkurs
                    updated_obj.dbervon = rawRow.dbervon
                    updated_obj.wzbetr = rawRow.wzbetr   
                    updated_objects.append(updated_obj)
                else:
                    pass
            else:
                obj = Metrica4(bukrs=rawRow.bukrs,
                               rfha=rawRow.rfha,
                               dcrdat=rawRow.dcrdat,
                               tcrtim=rawRow.tcrtim,
                               zuond=rawRow.zuond,
                               zz_num_reg=rawRow.zz_num_reg,
                               zz_bankname=rawRow.zz_bankname,
                               butxt=rawRow.butxt,
                               sgsart=rawRow.sgsart,
                               sfhaart=rawRow.sfhaart,
                               wgschft=rawRow.wgschft,
                               ssign=rawRow.ssign,
                               dzterm=rawRow.dzterm,
                               bzbetr=rawRow.bzbetr,
                               rfhazu=rawRow.rfhazu,
                               rfhazb=rawRow.rfhazb,
                               zz_begda=rawRow.zz_begda,
                               sfhazba = rawRow.sfhazba,
                               sbewebe = rawRow.sbewebe, 
                               dberbis = rawRow.dberbis, 
                               khwkurs = rawRow.khwkurs, 
                               dbervon = rawRow.dbervon, 
                               wzbetr  = rawRow.wzbetr  
                               )
                new_objects.append(obj)
        for obj in updated_objects:
            session.add(obj)
        session.flush()
        session.commit()
        print(f'Total updated rows: {len(updated_objects)}')
        session.add_all(new_objects)
        session.flush()
        session.commit()
        print(f'Total new inserted rows: {len(new_objects)}')

    def getMetrica1(self, session: Session) -> List[Metrica1]:
        # bukrs, pyord, belnr, gjahr, hbkid, zbukr, butxt, laufd, text1, waers_b,
        # numsfh, rbetr, zz_bezakc, zz_pyord_r, zz_zh2h, kblnr, waers_t, zz_vblnr, awkey
        # zz_rfha, partner, name_org1, name_org2, name_org3, name_org4

        metrica_1_select = session.query(RawRegup.bukrs, RawRegup.belnr, RawRegup.gjahr,
                                         RawRegup.pyord, RawReguh.hbkid, RawRegup.zbukr,
                                         RawT001.butxt, RawReguh.laufd, RawT012.text1, RawBkpf.waers_b,
                                         RawT012.numsfh, RawReguh.rbetr, RawKblk.zz_bezakc, RawKblk.zz_pyord_r,
                                         RawKblk.zz_zh2h, RawRegup.kblnr, RawT012.waers_t, RawKblk.zz_vblnr,
                                         RawBkpf.awkey, RawKblp.zz_rfha, But000.partner, But000.name_org1,
                                         But000.name_org2, But000.name_org3, But000.name_org4) \
            .join(RawRegup, and_(RawReguh.pyord == RawRegup.pyord,
                                 RawReguh.laufi == RawRegup.laufi,
                                 RawReguh.zbukr == RawRegup.zbukr,
                                 RawReguh.laufd == RawRegup.laufd), isouter=True) \
            .join(RawBkpf, and_(RawBkpf.bukrs == RawRegup.bukrs,
                                RawBkpf.belnr == RawRegup.belnr,
                                RawBkpf.gjahr == RawRegup.gjahr), isouter=True) \
            .join(RawKblk, and_(RawKblk.belnr == RawRegup.kblnr), isouter=True) \
            .join(RawKblp, and_(RawKblp.belnr == RawKblk.belnr), isouter=True) \
            .join(RawT012, and_(RawT012.bukrs == RawRegup.bukrs, RawT012.hbkid == RawReguh.hbkid), isouter=True) \
            .join(RawT001, and_(RawT001.bukrs == RawRegup.bukrs), isouter=True) \
            .join(But000, and_(But000.partner == RawRegup.zz_partner), isouter=True).where(RawRegup.pyord != null(),
                                                                                           RawRegup.zbukr != null(),
                                                                                           RawRegup.laufi != null(),
                                                                                           RawRegup.kblnr != null(),
                                                                                           RawBkpf.bukrs != null(),
                                                                                           RawBkpf.belnr != null(),
                                                                                           RawBkpf.gjahr != null(),
                                                                                           RawT012.hbkid != null(),
                                                                                           But000.partner != null(),
                                                                                           RawKblk.belnr != null(),
                                                                                           RawT001.bukrs != null(),
                                                                                           RawKblp.belnr != null())
        print(metrica_1_select)
        metrica_1_extracted = session.execute(metrica_1_select).all()
        if len(metrica_1_extracted) > 0:
            metrica_1_list = [Metrica1(bukrs=record[0],
                                       pyord=record[1],
                                       belnr=record[2],
                                       gjahr=record[3],
                                       hbkid=record[4],
                                       zbukr=record[5],
                                       butxt=record[6],
                                       laufd=record[7],
                                       text1=record[8],
                                       waers_b=record[9],
                                       numsfh=record[10],
                                       rbetr=record[11],
                                       zz_bezakc=record[12],
                                       zz_pyord_r=record[13],
                                       zz_zh2h=record[14],
                                       kblnr=record[15],
                                       waers_t=record[16],
                                       zz_vblnr=record[17],
                                       awkey=record[18],
                                       zz_rfha=record[19],
                                       partner=record[20],
                                       name_org1=record[21],
                                       name_org2=record[22],
                                       name_org3=record[23],
                                       name_org4=record[24]) for record in metrica_1_extracted]
            return metrica_1_list
        else:
            return []

    def getMetrica23(self, session: Session) -> List[Metrica23]:
        metrica_23_select = session.query(RawVtbfha.bukrs, RawVtbfha.rfha, RawVtbfhapo.dcrdat, RawVtbfhapo.tcrtim,
                                          RawDraw.zz_hbkid, RawVtbfha.zuond, RawT012.banks,
                                          RawDraw.zz_num_reg, RawVtbfhazu.nordext,
                                          RawVtbfha.sgsart, RawVtbfha.sfhaart,
                                          RawDraw.zz_bankname, RawVtbfha.wgschft, RawVtbfha.kontrh,
                                          RawVtbfhapo.dbuchung,
                                          RawVtbfhapo.ssign, RawVtbfhapo.bzbetr, RawTracvAccitem.dis_flowtype,
                                          RawTracvAccitem.posting_key, RawTracvAccitem.position_amt,
                                          RawTracvAccitem.position_curr, RawVtbfhapo.sfhazba,
                                          RawVtbAsgnLimit.limit_date,
                                          RawVtbAsgnLimit.limit_currency, RawVtbAsgnLimit.rfha_relat_obj,
                                          RawVtbAsgnLimit.limit_pos_amount, RawDraw.dokar, RawDraw.doknr,
                                          RawTracvAccitem.document_guid, RawTracvAccitem.item_number,
                                          RawTracvAccitem.os_guid_pi) \
            .join(RawVtbfha, and_(RawVtbfhapo.bukrs == RawVtbfha.bukrs,
                                  RawVtbfhapo.rfha == RawVtbfha.rfha), isouter=True) \
            .join(RawVtbfhazu, and_(RawVtbfhapo.bukrs == RawVtbfhazu.bukrs,
                                    RawVtbfhapo.rfha == RawVtbfhazu.rfha,
                                    RawVtbfhapo.rfhazu == RawVtbfhazu.rfhazu), isouter=True) \
            .join(RawVtbAsgnLimit, and_(RawVtbfhapo.bukrs == RawVtbAsgnLimit.bukrs_relat_obj,
                                        RawVtbfhapo.rfha == RawVtbAsgnLimit.rfha_relat_obj), isouter=True) \
            .join(RawTracvAccitem, and_(RawVtbfhapo.bukrs == RawTracvAccitem.company_code,
                                        RawVtbfhapo.rfha == RawTracvAccitem.deal_number,
                                        RawVtbfhapo.dcrdat == RawTracvAccitem.acpostingdate), isouter=True) \
            .join(RawDraw, RawVtbfha.zuond == RawDraw.zz_doknr, isouter=True) \
            .join(RawT012, and_(RawVtbfhapo.bukrs == RawT012.bukrs,
                                RawDraw.zz_hbkid == RawT012.hbkid), isouter=True).where(RawVtbfha.bukrs != null(),
                                                                               RawVtbfha.rfha != null(),
                                                                               RawVtbfhapo.dcrdat != null(),
                                                                               RawVtbfhapo.tcrtim != null(),
                                                                               RawVtbfhapo.rfhazu != null(),
                                                                               RawVtbfhapo.rfhazb != null(),
                                                                               RawDraw.dokar != null(),
                                                                               RawDraw.doknr != null(),
                                                                               RawTracvAccitem.document_guid != null(),
                                                                               RawTracvAccitem.item_number != null(),
                                                                               RawTracvAccitem.os_guid_pi != null())

        print(metrica_23_select)
        metrica_2_3_extracted = session.execute(metrica_23_select).all()
        # print(metrica_2_3_extracted)
        if len(metrica_2_3_extracted) > 0:
            metrica_2_3_list = [Metrica23(bukrs=record[0],
                                          rfha=record[1],
                                          dcrdat=record[2],
                                          tcrtim=record[3],
                                          zz_hbkid=record[4],
                                          zuond=record[5],
                                          banks=record[6],
                                          zz_num_reg=record[7],
                                          nordext=record[8],
                                          sgsart=record[9],
                                          sfhaart=record[10],
                                          zz_bankname=record[11],
                                          wgschft=record[12],
                                          kontrh=record[13],
                                          dbuchung=record[14],
                                          ssign=record[15],
                                          bzbetr=record[16],
                                          dis_flowtype=record[17],
                                          posting_key=record[18],
                                          position_amt=record[19],
                                          position_curr=record[20],
                                          sfhazba=record[21],
                                          limit_date=record[22],
                                          rfha_relat_obj=record[23],
                                          limit_currency=record[24],
                                          limit_pos_amount=record[25],
                                          rfhazu=record[26],
                                          rfhazb=record[27],
                                          dzterm=record[28],
                                          saktiv=record[29],
                                          sbewebe=record[30],
                                          acpostingdate=record[31],
                                          dokar=record[32],
                                          doknr=record[33],
                                          document_guid=record[34],
                                          item_number=record[35],
                                          os_guid_pi=record[36]) for record in metrica_2_3_extracted]
            return metrica_2_3_list
        else:
            return []

    def getMetrica4(self, session: Session) -> List[Metrica4]:
        # bukrs, rfha, dcrdat, tcrtim, zuond, zz_num_reg,
        # zz_bankname, butxt, sgsart, sfhaart, wgschft,
        # ssign, dzterm, bzbetr

        metrica_4_select = session.query(RawVtbfhapo.bukrs, RawVtbfhapo.rfha, RawVtbfhapo.dcrdat, RawVtbfhapo.tcrtim,
                                         RawVtbfha.zuond, RawDraw.zz_num_reg, RawDraw.zz_bankname, RawT001.butxt,
                                         RawVtbfha.sgsart, RawVtbfha.sfhaart, RawVtbfha.wgschft, RawVtbfhapo.ssign,
                                         RawVtbfhapo.dzterm, RawVtbfhapo.bzbetr,
                                         RawVtbfhapo.rfhazu, RawVtbfhapo.rfhazb, RawDraw.zz_begda,
                                         RawVtbfhapo.sfhazba, RawVtbfhapo.sbewebe, RawVtbfhapo.dberbis,
                                         RawVtbfhapo.khwkurs, RawVtbfhapo.dbervon, RawVtbfhapo.wzbetr) \
            .join(RawVtbfha,
                  and_(RawVtbfhapo.bukrs == RawVtbfha.bukrs, RawVtbfhapo.rfha == RawVtbfha.rfha),
                  isouter=True) \
            .join(RawDraw,
                  RawVtbfha.zuond == RawDraw.zz_doknr, isouter=True) \
            .join(RawT001, RawVtbfhapo.bukrs == RawT001.bukrs, isouter=True) \
            .where(RawVtbfhapo.bukrs != null(), RawVtbfhapo.rfha != null(),
                   RawVtbfhapo.dcrdat != null(), RawVtbfhapo.tcrtim != null(),
                   RawVtbfhapo.rfhazb != null(), RawVtbfhapo.rfhazu != null())

        print(metrica_4_select)
        metrica_4_extracted = session.execute(metrica_4_select).all()
        # print(metrica_4_extracted)
        if len(metrica_4_extracted) > 0:
            metrica_4_list = [Metrica4(bukrs=record[0],
                                       rfha=record[1],
                                       dcrdat=record[2],
                                       tcrtim=record[3],
                                       zuond=record[4],
                                       zz_num_reg=record[5],
                                       zz_bankname=record[6],
                                       butxt=record[7],
                                       sgsart=record[8],
                                       sfhaart=record[9],
                                       wgschft=record[10],
                                       ssign=record[11],
                                       dzterm=record[12],
                                       bzbetr=record[13],
                                       rfhazu=record[14],
                                       rfhazb=record[15],
                                       zz_begda=record[16]
                                       sfhazba=record[17]
                                       sbewebe=record[18]
                                       dberbis=record[19]
                                       khwkurs=record[20]
                                       dbervon=record[21]
                                       wzbetr=record[22]
                                       ) for record in metrica_4_extracted]
            return metrica_4_list
        else:
            return []
