from typing import List
from sqlalchemy.orm import Session
from adapters.database.tables.adb.core import (Reguh, Regup, Kblk, Bseg, Bkpf, Vtbfha, Vtbfhapo, Vtbfhazu, T012, T001,
                                               But000, Draw, VtbAsgnLimit, TracvAccitem, Kblp)
from adapters.database.tables.adb.raw import (RawReguh, RawRegup, RawKblk, RawBseg, RawBkpf, RawVtbfha, RawVtbfhapo,
                                              RawVtbfhazu,
                                              RawT012, RawT001, RawBut000, RawDraw, RawVtbAsgnLimit, RawTracvAccitem,
                                              RawKblp)


class RawToCoreRepo:
    def processReguh(self, session: Session):
        print(f'Processing Reguh')
        rawData: List[RawReguh] = session.query(RawReguh).all()
        if len(rawData) > 0:
            reguh: List[Reguh] = session.query(Reguh).all()
            reguh_dict = {(x.pyord, x.laufi, x.xvorl, x.vblnr, x.laufd): x for x in reguh}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.pyord, rawRow.laufi, rawRow.xvorl, rawRow.vblnr, rawRow.laufd) in reguh_dict:
                    if reguh_dict[(rawRow.pyord, rawRow.laufi, rawRow.xvorl, rawRow.vblnr, rawRow.laufd)] != rawRow:
                        updated_obj: Reguh = reguh_dict[(rawRow.pyord, rawRow.laufi, rawRow.xvorl,
                                                         rawRow.vblnr, rawRow.laufd)]
                        updated_obj.pyord = rawRow.pyord
                        updated_obj.laufi = rawRow.laufi
                        updated_obj.xvorl = rawRow.xvorl
                        updated_obj.vblnr = rawRow.vblnr
                        updated_obj.laufd = rawRow.laufd
                        updated_obj.gjahr = rawRow.gjahr
                        updated_obj.waers = rawRow.waers
                        updated_obj.zbukr = rawRow.zbukr
                        updated_obj.hbkid = rawRow.hbkid
                        updated_obj.rbetr = rawRow.rbetr
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Reguh(pyord=rawRow.pyord,
                                 laufi=rawRow.laufi,
                                 xvorl=rawRow.xvorl,
                                 vblnr=rawRow.vblnr,
                                 laufd=rawRow.laufd,
                                 gjahr=rawRow.gjahr,
                                 waers=rawRow.waers,
                                 zbukr=rawRow.zbukr,
                                 hbkid=rawRow.hbkid,
                                 rbetr=rawRow.rbetr)
                    new_objects.append(obj)
            session.add_all(updated_objects)
            session.flush()
            session.commit()
            print(f'Total updated rows: {len(updated_objects)}')
            session.add_all(new_objects)
            session.flush()
            session.commit()
            print(f'Total new inserted rows: {len(new_objects)}')
            deleted_num_rows = session.query(RawReguh).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processRegup(self, session: Session):
        print(f'Processing Regup')
        rawData: List[RawRegup] = session.query(RawRegup).all()
        if len(rawData) > 0:
            regup: List[Regup] = session.query(Regup).all()
            regup_dict = {(x.pyord, x.bukrs, x.belnr, x.gjahr, x.buzei, x.laufi): x for x in regup}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.pyord, rawRow.bukrs, rawRow.belnr, rawRow.gjahr, rawRow.buzei, rawRow.laufi) in regup_dict:
                    if regup_dict[(rawRow.pyord, rawRow.bukrs, rawRow.belnr, rawRow.gjahr, rawRow.buzei,
                                   rawRow.laufi)] != rawRow:
                        updated_obj: Regup = regup_dict[
                            (rawRow.pyord, rawRow.bukrs, rawRow.belnr, rawRow.gjahr, rawRow.buzei, rawRow.laufi)]
                        updated_obj.pyord = rawRow.pyord
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.belnr = rawRow.belnr
                        updated_obj.zbukr = rawRow.zbukr
                        updated_obj.gjahr = rawRow.gjahr
                        updated_obj.buzei = rawRow.buzei
                        updated_obj.kblnr = rawRow.kblnr
                        updated_obj.laufi = rawRow.laufi
                        updated_obj.zz_partner = rawRow.zz_partner
                        updated_obj.laufd = rawRow.laufd
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Regup(pyord=rawRow.pyord,
                                 bukrs=rawRow.bukrs,
                                 belnr=rawRow.belnr,
                                 zbukr=rawRow.zbukr,
                                 gjahr=rawRow.gjahr,
                                 buzei=rawRow.buzei,
                                 kblnr=rawRow.kblnr,
                                 laufi=rawRow.laufi,
                                 zz_partner=rawRow.zz_partner,
                                 laufd=rawRow.laufd)
                    new_objects.append(obj)
            session.add_all(updated_objects)
            session.flush()
            session.commit()
            print(f'Total updated rows: {len(updated_objects)}')
            session.add_all(new_objects)
            session.flush()
            session.commit()
            print(f'Total new inserted rows: {len(new_objects)}')
            deleted_num_rows = session.query(RawReguh).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processKblk(self, session: Session):
        print(f'Processing Kblk')
        rawData: List[RawKblk] = session.query(RawKblk).all()
        if len(rawData) > 0:
            kblk: List[Kblk] = session.query(Kblk).all()
            kblk_dict = {x.belnr: x for x in kblk}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if rawRow.belnr in kblk_dict:
                    if kblk_dict[rawRow.belnr] != rawRow:
                        updated_obj: Kblk = kblk_dict[rawRow.belnr]
                        updated_obj.belnr = rawRow.belnr
                        updated_obj.kerdat = rawRow.kerdat
                        updated_obj.gjahr = rawRow.gjahr
                        updated_obj.zz_bezakc = rawRow.zz_bezakc
                        updated_obj.zz_pyord_r = rawRow.zz_pyord_r
                        updated_obj.zz_zh2h = rawRow.zz_zh2h
                        updated_obj.zz_vblnr = rawRow.zz_vblnr
                        updated_obj.zz_bukrs = rawRow.zz_bukrs
                        updated_obj.zz_hbkid = rawRow.zz_hbkid
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Kblk(belnr=rawRow.belnr,
                               kerdat=rawRow.kerdat,
                               gjahr=rawRow.gjahr,
                               zz_bezakc=rawRow.zz_bezakc,
                               zz_pyord_r=rawRow.zz_pyord_r,
                               zz_zh2h=rawRow.zz_zh2h,
                               zz_vblnr=rawRow.zz_vblnr,
                               zz_bukrs=rawRow.zz_bukrs,
                               zz_hbkid=rawRow.zz_hbkid)
                    new_objects.append(obj)
            session.add_all(updated_objects)
            session.flush()
            session.commit()
            print(f'Total updated rows: {len(updated_objects)}')
            session.add_all(new_objects)
            session.flush()
            session.commit()
            print(f'Total new inserted rows: {len(new_objects)}')
            deleted_num_rows = session.query(RawKblk).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processBseg(self, session: Session):
        print(f'Processing Bseg')
        rawData: List[RawBseg] = session.query(RawBseg).all()
        if len(rawData) > 0:
            bseg: List[Bseg] = session.query(Bseg).all()
            bseg_dict = {(x.bukrs, x.belnr, x.gjahr, x.buzei): x for x in bseg}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs, rawRow.belnr, rawRow.gjahr, rawRow.buzei) in bseg_dict:
                    if bseg_dict[(rawRow.bukrs, rawRow.belnr, rawRow.gjahr, rawRow.buzei)] != rawRow:
                        updated_obj: Bseg = bseg_dict[(rawRow.bukrs, rawRow.belnr, rawRow.gjahr, rawRow.buzei)]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.belnr = rawRow.belnr
                        updated_obj.gjahr = rawRow.gjahr
                        updated_obj.kblnr = rawRow.kblnr
                        updated_obj.buzei = rawRow.buzei
                        updated_obj.wrbtr = rawRow.wrbtr
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Bseg(bukrs=rawRow.bukrs,
                               belnr=rawRow.belnr,
                               gjahr=rawRow.gjahr,
                               kblnr=rawRow.kblnr,
                               buzei=rawRow.buzei,
                               wrbtr=rawRow.wrbtr
                               )
                    new_objects.append(obj)
            session.add_all(updated_objects)
            session.flush()
            session.commit()
            print(f'Total updated rows: {len(updated_objects)}')
            session.add_all(new_objects)
            session.flush()
            session.commit()
            print(f'Total new inserted rows: {len(new_objects)}')
            deleted_num_rows = session.query(RawBseg).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processBkpf(self, session: Session):
        print(f'Processing Bkpf')
        rawData: List[RawBkpf] = session.query(RawBkpf).all()
        if len(rawData) > 0:
            bkpf: List[Bkpf] = session.query(Bkpf).all()
            bkpf_dict = {(x.bukrs, x.belnr, x.gjahr): x for x in bkpf}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs, rawRow.belnr, rawRow.gjahr) in bkpf_dict:
                    if bkpf_dict[(rawRow.bukrs, rawRow.belnr, rawRow.gjahr)] != rawRow:
                        updated_obj: Bkpf = bkpf_dict[(rawRow.bukrs, rawRow.belnr, rawRow.gjahr)]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.belnr = rawRow.belnr
                        updated_obj.gjahr = rawRow.gjahr
                        updated_obj.waers_b = rawRow.waers_b
                        updated_obj.awkey = rawRow.awkey
                        updated_obj.xreversal = rawRow.xreversal
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Bkpf(bukrs=rawRow.bukrs,
                               belnr=rawRow.belnr,
                               gjahr=rawRow.gjahr,
                               waers_b=rawRow.waers_b,
                               awkey=rawRow.awkey,
                               xreversal=rawRow.xreversal
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
            deleted_num_rows = session.query(RawBkpf).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processVtbfha(self, session: Session):
        print(f'Processing Vtbfha')
        rawData: List[RawVtbfha] = session.query(RawVtbfha).all()
        if len(rawData) > 0:
            vtbfha: List[Vtbfha] = session.query(Vtbfha).all()
            vtbfha_dict = {(x.bukrs, x.rfha): x for x in vtbfha}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs, rawRow.rfha) in vtbfha_dict:
                    if vtbfha_dict[(rawRow.bukrs, rawRow.rfha)] != rawRow:
                        updated_obj: Vtbfha = vtbfha_dict[(rawRow.bukrs, rawRow.rfha)]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.rfha = rawRow.rfha
                        updated_obj.dcrdat_ha = rawRow.dcrdat_ha
                        updated_obj.tcrtim_ha = rawRow.tcrtim_ha
                        updated_obj.sgsart = rawRow.sgsart
                        updated_obj.sfhaart = rawRow.sfhaart
                        updated_obj.zuond = rawRow.zuond
                        updated_obj.saktiv = rawRow.saktiv
                        updated_obj.wgschft = rawRow.wgschft
                        updated_obj.kontrh = rawRow.kontrh
                        updated_obj.rfhazul = rawRow.rfhazul
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Vtbfha(bukrs=rawRow.bukrs,
                                 rfha=rawRow.rfha,
                                 dcrdat_ha=rawRow.dcrdat_ha,
                                 tcrtim_ha=rawRow.tcrtim_ha,
                                 sgsart=rawRow.sgsart,
                                 sfhaart=rawRow.sfhaart,
                                 zuond=rawRow.zuond,
                                 saktiv=rawRow.saktiv,
                                 wgschft=rawRow.wgschft,
                                 kontrh=rawRow.kontrh,
                                 rfhazul=rawRow.rfhazul
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
            deleted_num_rows = session.query(RawVtbfha).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processVtbfhapo(self, session: Session):
        print(f'Processing Vtbfhapo')
        rawData: List[RawVtbfhapo] = session.query(RawVtbfhapo).all()
        if len(rawData) > 0:
            vtbfhapo: List[Vtbfhapo] = session.query(Vtbfhapo).all()
            vtbfhapo_dict = {(rawRow.bukrs, rawRow.rfhazu, rawRow.rfha, rawRow.dcrdat, rawRow.gjahr, rawRow.tcrtim,
                              rawRow.rfhazb): rawRow for rawRow in vtbfhapo}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs, rawRow.rfhazu, rawRow.rfha, rawRow.dcrdat, rawRow.gjahr, rawRow.tcrtim,
                    rawRow.rfhazb) in vtbfhapo_dict:
                    if vtbfhapo_dict[
                        (rawRow.bukrs, rawRow.rfhazu, rawRow.rfha, rawRow.dcrdat, rawRow.gjahr, rawRow.tcrtim,
                         rawRow.rfhazb)] != rawRow:
                        updated_obj: Vtbfhapo = vtbfhapo_dict[(
                            rawRow.bukrs, rawRow.rfhazu, rawRow.rfha, rawRow.dcrdat, rawRow.gjahr, rawRow.tcrtim,
                            rawRow.rfhazb)]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.rfhazu = rawRow.rfhazu
                        updated_obj.rfha = rawRow.rfha
                        updated_obj.dcrdat = rawRow.dcrdat
                        updated_obj.gjahr = rawRow.gjahr
                        updated_obj.tcrtim = rawRow.tcrtim
                        updated_obj.rfhazb = rawRow.rfhazb
                        updated_obj.sfhazba = rawRow.sfhazba
                        updated_obj.ssign = rawRow.ssign
                        updated_obj.dzterm = rawRow.dzterm
                        updated_obj.bzbetr = rawRow.bzbetr
                        updated_obj.dbuchung = rawRow.dbuchung
                        updated_obj.sbewebe = rawRow.sbewebe
                        updated_obj.dberbis = rawRow.dberbis
                        updated_obj.khwkurs = rawRow.khwkurs
                        updated_obj.dbervon = rawRow.dbervon
                        updated_obj.wzbetr = rawRow.wzbetr
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Vtbfhapo(bukrs=rawRow.bukrs,
                                   rfhazu=rawRow.rfhazu,
                                   rfha=rawRow.rfha,
                                   dcrdat=rawRow.dcrdat,
                                   gjahr=rawRow.gjahr,
                                   tcrtim=rawRow.tcrtim,
                                   rfhazb=rawRow.rfhazb,
                                   sfhazba=rawRow.sfhazba,
                                   ssign=rawRow.ssign,
                                   dzterm=rawRow.dzterm,
                                   bzbetr=rawRow.bzbetr,
                                   dbuchung=rawRow.dbuchung,
                                   sbewebe=rawRow.sbewebe,
                                  dberbis=rawRow.dberbis,
                                  khwkurs=rawRow.khwkurs,
                                  dbervon=rawRow.dbervon,
                                  wzbetr=rawRow.wzbetr
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
            deleted_num_rows = session.query(RawVtbfhapo).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processVtbfhazu(self, session: Session):
        print(f'Processing Vtbfhazu')
        rawData: List[RawVtbfhazu] = session.query(RawVtbfhazu).all()
        if len(rawData) > 0:
            vtbfhazu: List[Vtbfhazu] = session.query(Vtbfhazu).all()
            vtbfhazu_dict = {(rawRow.bukrs, rawRow.rfhazu, rawRow.rfha): rawRow for rawRow in vtbfhazu}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs, rawRow.rfhazu, rawRow.rfha) in vtbfhazu_dict:
                    if vtbfhazu_dict[(rawRow.bukrs, rawRow.rfhazu, rawRow.rfha)] != rawRow:
                        updated_obj: Vtbfhazu = vtbfhazu_dict[(rawRow.bukrs, rawRow.rfhazu, rawRow.rfha)]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.rfhazu = rawRow.rfhazu
                        updated_obj.rfha = rawRow.rfha
                        updated_obj.dcrdat_zu = rawRow.dcrdat_zu
                        updated_obj.tcrtim_zu = rawRow.tcrtim_zu
                        updated_obj.nordext = rawRow.nordext
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Vtbfhazu(bukrs=rawRow.bukrs,
                                   rfhazu=rawRow.rfhazu,
                                   rfha=rawRow.rfha,
                                   dcrdat_zu=rawRow.dcrdat_zu,
                                   tcrtim_zu=rawRow.tcrtim_zu,
                                   nordext=rawRow.nordext
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
            deleted_num_rows = session.query(RawVtbfhazu).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processT012(self, session: Session):
        print(f'Processing T012')
        rawData: List[RawT012] = session.query(RawT012).all()
        if len(rawData) > 0:
            t012: List[T012] = session.query(T012).all()
            t012_dict = {(rawRow.bukrs, rawRow.hbkid): rawRow for rawRow in t012}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs, rawRow.hbkid) in t012_dict:
                    if t012_dict[(rawRow.bukrs, rawRow.hbkid)] != rawRow:
                        updated_obj: T012 = t012_dict[(rawRow.bukrs, rawRow.hbkid)]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.hbkid = rawRow.hbkid
                        updated_obj.banks = rawRow.banks
                        updated_obj.text1 = rawRow.text1
                        updated_obj.numsfh = rawRow.numsfh
                        updated_obj.waers_t = rawRow.waers_t
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = T012(bukrs=rawRow.bukrs,
                               hbkid=rawRow.hbkid,
                               banks=rawRow.banks,
                               text1=rawRow.text1,
                               numsfh=rawRow.numsfh,
                               waers_t=rawRow.waers_t
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
            deleted_num_rows = session.query(RawT012).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processT001(self, session: Session):
        print(f'Processing T001')
        rawData: List[RawT001] = session.query(RawT001).all()
        if len(rawData) > 0:
            t001: List[T001] = session.query(T001).all()
            t001_dict = {rawRow.bukrs: rawRow for rawRow in t001}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if rawRow.bukrs in t001_dict:
                    if t001_dict[rawRow.bukrs] != rawRow:
                        updated_obj: T001 = t001_dict[rawRow.bukrs]
                        updated_obj.bukrs = rawRow.bukrs
                        updated_obj.butxt = rawRow.butxt
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = T001(bukrs=rawRow.bukrs,
                               butxt=rawRow.butxt
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
            deleted_num_rows = session.query(RawT001).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processBut000(self, session: Session):
        print(f'Processing But000')
        rawData: List[RawBut000] = session.query(RawBut000).all()
        if len(rawData) > 0:
            but000: List[But000] = session.query(But000).all()
            but000_dict = {rawRow.partner: rawRow for rawRow in but000}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if rawRow.partner in but000_dict:
                    if but000_dict[rawRow.partner] != rawRow:
                        updated_obj: But000 = but000_dict[rawRow.partner]
                        updated_obj.partner = rawRow.partner
                        updated_obj.name_org1 = rawRow.name_org1
                        updated_obj.name_org2 = rawRow.name_org2
                        updated_obj.name_org3 = rawRow.name_org3
                        updated_obj.name_org4 = rawRow.name_org4
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = But000(partner=rawRow.partner,
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
            deleted_num_rows = session.query(RawBut000).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processDraw(self, session: Session):
        print(f'Processing Draw')
        rawData: List[RawDraw] = session.query(RawDraw).all()
        if len(rawData) > 0:
            draw: List[Draw] = session.query(Draw).all()
            draw_dict = {(rawRow.dokar, rawRow.doknr): rawRow for rawRow in draw}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.dokar, rawRow.doknr) in draw_dict:
                    if draw_dict[(rawRow.dokar, rawRow.doknr)] != rawRow:
                        updated_obj: Draw = draw_dict[(rawRow.dokar, rawRow.doknr)]
                        updated_obj.dokar = rawRow.dokar
                        updated_obj.doknr = rawRow.doknr
                        updated_obj.zz_num_reg = rawRow.zz_num_reg
                        updated_obj.zz_hbkid = rawRow.zz_hbkid
                        updated_obj.zz_vvsart = rawRow.zz_vvsart
                        updated_obj.zz_sfhaart = rawRow.zz_sfhaart
                        updated_obj.zz_bankname = rawRow.zz_bankname
                        updated_obj.zz_begda = rawRow.zz_begda
                        updated_obj.zz_doknr = rawRow.zz_doknr
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Draw(dokar=rawRow.dokar,
                               doknr=rawRow.doknr,
                               zz_num_reg=rawRow.zz_num_reg,
                               zz_hbkid=rawRow.zz_hbkid,
                               zz_vvsart=rawRow.zz_vvsart,
                               zz_sfhaart=rawRow.zz_sfhaart,
                               zz_bankname=rawRow.zz_bankname,
                               zz_begda=rawRow.zz_begda,
                               zz_doknr=rawRow.zz_doknr
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
            deleted_num_rows = session.query(RawDraw).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processVtbAsgnLimit(self, session: Session):
        print(f'Processing VtbAsgnLimit')
        rawData: List[RawVtbAsgnLimit] = session.query(RawVtbAsgnLimit).all()
        if len(rawData) > 0:
            vtb_asgn_limit: List[VtbAsgnLimit] = session.query(VtbAsgnLimit).all()
            vtb_asgn_limit_dict = {(rawRow.bukrs_relat_obj, rawRow.limit_date, rawRow.rfha_relat_obj, rawRow.limit_currency): rawRow for rawRow in vtb_asgn_limit}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.bukrs_relat_obj, rawRow.limit_date, rawRow.rfha_relat_obj, rawRow.limit_currency) in vtb_asgn_limit_dict:
                    if vtb_asgn_limit_dict[(rawRow.bukrs_relat_obj, rawRow.limit_date, rawRow.rfha_relat_obj, rawRow.limit_currency)] != rawRow:
                        updated_obj: VtbAsgnLimit = vtb_asgn_limit_dict[(rawRow.bukrs_relat_obj, rawRow.limit_date, rawRow.rfha_relat_obj, rawRow.limit_currency)]
                        updated_obj.relat_obj = rawRow.relat_obj
                        updated_obj.limit_date = rawRow.limit_date
                        updated_obj.bukrs_relat_obj = rawRow.bukrs_relat_obj
                        updated_obj.rfha_relat_obj = rawRow.rfha_relat_obj
                        updated_obj.limit_currency = rawRow.limit_currency
                        updated_obj.limit_pos_amount = rawRow.limit_pos_amount
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = VtbAsgnLimit(relat_obj=rawRow.relat_obj,
                                       limit_date=rawRow.limit_date,
                                       bukrs_relat_obj=rawRow.bukrs_relat_obj,
                                       rfha_relat_obj=rawRow.rfha_relat_obj,
                                       limit_currency=rawRow.limit_currency,
                                       limit_pos_amount=rawRow.limit_pos_amount
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
            deleted_num_rows = session.query(RawVtbAsgnLimit).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processTracvAccitem(self, session: Session):
        print(f'Processing TracvAccitem')
        rawData: List[RawTracvAccitem] = session.query(RawTracvAccitem).all()
        if len(rawData) > 0:
            tracv_accitem: List[TracvAccitem] = session.query(TracvAccitem).all()
            tracv_accitem_dict = {(rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi): rawRow for rawRow in
                                  tracv_accitem}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi) in tracv_accitem_dict:
                    if tracv_accitem_dict[(rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi)] != rawRow:
                        updated_obj: TracvAccitem = tracv_accitem_dict[
                            (rawRow.document_guid, rawRow.item_number, rawRow.os_guid_pi)]
                        updated_obj.company_code = rawRow.company_code
                        updated_obj.deal_number = rawRow.deal_number
                        updated_obj.acpostingdate = rawRow.acpostingdate
                        updated_obj.dis_flowtype = rawRow.dis_flowtype
                        updated_obj.posting_key = rawRow.posting_key
                        updated_obj.position_amt = rawRow.position_amt
                        updated_obj.position_curr = rawRow.position_curr
                        updated_obj.document_guid = rawRow.document_guid
                        updated_obj.item_number = rawRow.item_number
                        updated_obj.os_guid_pi = rawRow.os_guid_pi
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = TracvAccitem(company_code=rawRow.company_code,
                                       deal_number=rawRow.deal_number,
                                       acpostingdate=rawRow.acpostingdate,
                                       dis_flowtype=rawRow.dis_flowtype,
                                       posting_key=rawRow.posting_key,
                                       position_amt=rawRow.position_amt,
                                       position_curr=rawRow.position_curr,
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
            deleted_num_rows = session.query(RawTracvAccitem).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')

    def processKblp(self, session: Session):
        print(f'Processing Kblp')
        rawData: List[RawKblp] = session.query(RawKblp).all()
        if len(rawData) > 0:
            kblp: List[Kblp] = session.query(Kblp).all()
            kblp_dict = {(rawRow.belnr, rawRow.blpos): rawRow for rawRow in
                         kblp}
            updated_objects = []
            new_objects = []
            for rawRow in rawData:
                if (rawRow.belnr, rawRow.blpos) in kblp_dict:
                    if kblp_dict[(rawRow.belnr, rawRow.blpos)] != rawRow:
                        updated_obj: Kblp = kblp_dict[
                            (rawRow.belnr, rawRow.blpos)]
                        updated_obj.belnr = rawRow.belnr
                        updated_obj.blpos = rawRow.blpos
                        updated_obj.zz_rfha = rawRow.zz_rfha
                        updated_obj.zz_rfhazu = rawRow.zz_rfhazu
                        updated_obj.zz_rfhazb = rawRow.zz_rfhazb
                        updated_obj.zz_dcrdat = rawRow.zz_dcrdat
                        updated_obj.zz_tcrtim = rawRow.zz_tcrtim
                        updated_obj.wtges = rawRow.wtges
                        updated_objects.append(updated_obj)
                    else:
                        pass
                else:
                    obj = Kblp(belnr=rawRow.belnr,
                                       blpos=rawRow.blpos,
                                       zz_rfha=rawRow.zz_rfha,
                                       zz_rfhazu=rawRow.zz_rfhazu,
                                       zz_rfhazb=rawRow.zz_rfhazb,
                                       zz_dcrdat=rawRow.zz_dcrdat,
                                       zz_tcrtim=rawRow.zz_tcrtim,
                                       wtges=rawRow.wtges
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
            deleted_num_rows = session.query(RawTracvAccitem).delete()
            session.flush()
            session.commit()
            print(f'Total deleted raw rows: {deleted_num_rows}')
        else:
            print(f'Empty input.')
