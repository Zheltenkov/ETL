from datetime import date, time, datetime
from typing import Optional

from attrs import define
from sqlalchemy import Column
from sqlalchemy import (
    Date,
    DateTime,
    Float,
    VARCHAR,
    Time
)
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import registry

mapper_registry = registry()

metadata = MetaData(schema='raw',
                    naming_convention={
                        'ix': 'ix_%(column_0_label)s',
                        'uq': 'uq_%(table_name)s_%(column_0_name)s',
                        'ck': 'ck_%(table_name)s_%(constraint_name)s',
                        'fk': 'fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s',
                        'pk': 'pk_%(table_name)s'
                    })


@mapper_registry.mapped
@define(slots=False)
class RawReguh:
    def __eq__(self, other) -> bool:
        return self.pyord == other.pyord \
               and self.laufi == other.laufi \
               and self.xvorl == other.xvorl \
               and self.vblnr == other.vblnr \
               and self.laufd == other.laufd \
               and self.gjahr == other.gjahr \
               and self.waers == other.waers \
               and self.zbukr == other.zbukr \
               and self.hbkid == other.hbkid \
               and self.rbetr == other.rbetr

    __table__ = Table(
        'reguh', metadata,
        Column('pyord', VARCHAR(10), nullable=False, primary_key=True),
        Column('laufi', VARCHAR(6), nullable=False, primary_key=True),
        Column('xvorl', VARCHAR(1), nullable=False, primary_key=True),
        Column('vblnr', VARCHAR(20), nullable=False, primary_key=True),
        Column('laufd', DateTime, nullable=False, primary_key=True),
        Column('gjahr', Integer),
        Column('waers', VARCHAR(10)),
        Column('zbukr', VARCHAR(10)),
        Column('hbkid', VARCHAR(10)),
        Column('rbetr', Float(4))
    )

    pyord: str
    laufi: str
    xvorl: str
    vblnr: str
    laufd: datetime
    gjahr: Optional[int]
    waers: Optional[str]
    zbukr: Optional[str]
    hbkid: Optional[str]
    rbetr: Optional[float]


@mapper_registry.mapped
@define(slots=False)
class RawRegup:
    def __eq__(self, other) -> bool:
        return self.pyord == other.pyord \
               and self.bukrs == other.bukrs \
               and self.belnr == other.belnr \
               and self.zbukr == other.zbukr \
               and self.gjahr == other.gjahr \
               and self.buzei == other.buzei \
               and self.kblnr == other.kblnr \
               and self.laufi == other.laufi \
               and self.zz_partner == other.zz_partner \
               and self.laufd == other.laufd

    __table__ = Table(
        'regup', metadata,
        Column('pyord', VARCHAR(20), nullable=False, primary_key=True),
        Column('bukrs', VARCHAR(8), nullable=False, primary_key=True),
        Column('belnr', VARCHAR(20), nullable=False, primary_key=True),
        Column('zbukr', VARCHAR(10)),
        Column('gjahr', Integer, nullable=False, primary_key=True),
        Column('buzei', Integer, nullable=False, primary_key=True),
        Column('kblnr', VARCHAR(20)),
        Column('laufi', VARCHAR(6), nullable=False, primary_key=True),
        Column('zz_partner', VARCHAR(20)),
        Column('laufd', DateTime, nullable=False)
    )
    pyord: str
    bukrs: str
    belnr: str
    zbukr: Optional[str]
    gjahr: int
    buzei: int
    kblnr: Optional[str]
    laufi: str
    zz_partner: Optional[str]
    laufd: datetime


@mapper_registry.mapped
@define(slots=False)
class RawKblk:
    def __eq__(self, other) -> bool:
        return self.belnr == other.belnr \
               and self.kerdat == other.kerdat \
               and self.gjahr == other.gjahr \
               and self.zz_bezakc == other.zz_bezakc \
               and self.zz_pyord_r == other.zz_pyord_r \
               and self.zz_zh2h == other.zz_zh2h \
               and self.zz_vblnr == other.zz_vblnr \
               and self.zz_bukrs == other.zz_bukrs \
               and self.zz_hbkid == other.zz_hbkid

    __table__ = Table(
        'kblk', metadata,
        Column('belnr', VARCHAR(20), primary_key=True),
        Column('kerdat', DateTime),
        Column('gjahr', Integer),
        Column('zz_bezakc', VARCHAR(2)),
        Column('zz_pyord_r', VARCHAR(20)),
        Column('zz_zh2h', VARCHAR(2)),
        Column('zz_vblnr', VARCHAR(20)),
        Column('zz_bukrs', VARCHAR(8)),
        Column('zz_hbkid', VARCHAR(20))
    )
    belnr: str
    kerdat: Optional[datetime]
    gjahr: Optional[int]
    zz_bezakc: Optional[str]
    zz_pyord_r: Optional[str]
    zz_zh2h: Optional[str]
    zz_vblnr: Optional[str]
    zz_bukrs: Optional[str]
    zz_hbkid: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawBseg:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.belnr == other.belnr \
               and self.gjahr == other.gjahr \
               and self.kblnr == other.kblnr \
               and self.buzei == other.buzei \
               and self.wrbtr == other.wrbtr

    __table__ = Table(
        'bseg', metadata,
        Column('bukrs', VARCHAR, primary_key=True),
        Column('belnr', VARCHAR, primary_key=True),
        Column('gjahr', Integer, primary_key=True),
        Column('kblnr', VARCHAR),
        Column('buzei', Integer, primary_key=True),
        Column('wrbtr', Float)
    )
    bukrs: Optional[str]
    belnr: Optional[str]
    gjahr: Optional[int]
    kblnr: Optional[str]
    buzei: Optional[int]
    wrbtr: Optional[float]


@mapper_registry.mapped
@define(slots=False)
class RawBkpf:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.belnr == other.belnr \
               and self.gjahr == other.gjahr \
               and self.waers_b == other.waers_b \
               and self.awkey == other.awkey \
               and self.xreversal == other.xreversal

    __table__ = Table(
        'bkpf', metadata,
        Column('bukrs', VARCHAR(8), primary_key=True),
        Column('belnr', VARCHAR(20), primary_key=True),
        Column('gjahr', Integer, primary_key=True),
        Column('waers_b', VARCHAR(20)),
        Column('awkey', VARCHAR(20)),
        Column('xreversal', VARCHAR(1))
    )
    bukrs: Optional[str]
    belnr: Optional[str]
    gjahr: Optional[int]
    waers_b: Optional[str]
    awkey: Optional[str]
    xreversal: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawVtbfha:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.rfha == other.rfha \
               and self.dcrdat_ha == other.dcrdat_ha \
               and self.tcrtim_ha == other.tcrtim_ha \
               and self.sgsart == other.sgsart \
               and self.sfhaart == other.sfhaart \
               and self.zuond == other.zuond \
               and self.saktiv == other.saktiv \
               and self.wgschft == other.wgschft \
               and self.kontrh == other.kontrh \
               and self.rfhazul == other.rfhazul

    __table__ = Table(
        'vtbfha', metadata,
        Column('bukrs', VARCHAR(8), primary_key=True),
        Column('rfha', VARCHAR(26), primary_key=True),
        Column('dcrdat_ha', Date),
        Column('tcrtim_ha', Time),
        Column('sgsart', VARCHAR(6)),
        Column('sfhaart', VARCHAR(6)),
        Column('zuond', VARCHAR(36)),
        Column('saktiv', Integer),
        Column('wgschft', VARCHAR(20)),
        Column('kontrh', VARCHAR(20)),
        Column('rfhazul', Integer)
    )
    bukrs: Optional[str]
    rfha: Optional[str]
    dcrdat_ha: Optional[date]
    tcrtim_ha: Optional[time]
    sgsart: Optional[str]
    sfhaart: Optional[str]
    zuond: Optional[str]
    saktiv: Optional[int]
    wgschft: Optional[str]
    kontrh: Optional[str]
    rfhazul: Optional[int]


@mapper_registry.mapped
@define(slots=False)
class RawVtbfhapo:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.rfhazu == other.rfhazu \
               and self.rfha == other.rfha \
               and self.dcrdat == other.dcrdat \
               and self.gjahr == other.gjahr \
               and self.tcrtim == other.tcrtim \
               and self.rfhazb == other.rfhazb \
               and self.sfhazba == other.sfhazba \
               and self.ssign == other.ssign \
               and self.dzterm == other.dzterm \
               and self.bzbetr == other.bzbetr \
               and self.dbuchung == other.dbuchung \
               and self.sbewebe == other.sbewebe \
               and self.dberbis == other.dberbis \
               and self.khwkurs == other.khwkurs \
               and self.dbervon == other.dbervon \
               and self.wzbetr == other.wzbetr  

    __table__ = Table(
        'vtbfhapo', metadata,
        Column('bukrs', VARCHAR(8), primary_key=True),
        Column('rfhazu', Integer, primary_key=True),
        Column('rfha', VARCHAR(26), primary_key=True),
        Column('dcrdat', Date, primary_key=True),
        Column('gjahr', Integer, primary_key=True),
        Column('tcrtim', Time, primary_key=True),
        Column('rfhazb', Integer, primary_key=True),
        Column('sfhazba', VARCHAR(8)),
        Column('ssign', VARCHAR(2)),
        Column('dzterm', Date),
        Column('bzbetr', Float),
        Column('dbuchung', Date),
        Column('sbewebe', VARCHAR(1)),
        Column('dberbis', Date),
        Column('khwkurs', Float),
        Column('dbervon', Date),
        Column('wzbetr', VARCHAR(5))
        )
    bukrs: Optional[str]
    rfhazu: Optional[int]
    rfha: Optional[str]
    dcrdat: Optional[date]
    gjahr: Optional[int]
    tcrtim: Optional[time]
    rfhazb: Optional[int]
    sfhazba: Optional[str]
    ssign: Optional[str]
    dzterm: Optional[date]
    bzbetr: Optional[float]
    dbuchung: Optional[date]
    sbewebe: Optional[str]
    dberbis: Optional[date]
    khwkurs: Optional[float]
    dbervon: Optional[date]
    wzbetr: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawVtbfhazu:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.rfhazu == other.rfhazu \
               and self.rfha == other.rfha \
               and self.dcrdat_zu == other.dcrdat_zu \
               and self.tcrtim_zu == other.tcrtim_zu \
               and self.nordext == other.nordext

    __table__ = Table(
        'vtbfhazu', metadata,
        Column('bukrs', VARCHAR(8), primary_key=True),
        Column('rfhazu', Integer, primary_key=True),
        Column('rfha', VARCHAR(26), primary_key=True),
        Column('dcrdat_zu', Date),
        Column('tcrtim_zu', Time),
        Column('nordext', VARCHAR(32))
    )
    bukrs: Optional[str]
    rfhazu: Optional[int]
    rfha: Optional[str]
    dcrdat_zu: Optional[date]
    tcrtim_zu: Optional[time]
    nordext: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawT012:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.hbkid == other.hbkid \
               and self.banks == other.banks \
               and self.text1 == other.text1 \
               and self.numsfh == other.numsfh \
               and self.waers_t == other.waers_t

    __table__ = Table(
        't012', metadata,
        Column('bukrs', VARCHAR(8), primary_key=True),
        Column('hbkid', VARCHAR(10), primary_key=True),
        Column('banks', VARCHAR(6)),
        Column('text1', VARCHAR(80)),
        Column('numsfh', VARCHAR(80)),
        Column('waers_t', VARCHAR(50))
    )
    bukrs: Optional[str]
    hbkid: Optional[str]
    banks: Optional[str]
    text1: Optional[str]
    numsfh: Optional[str]
    waers_t: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawT001:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.butxt == other.butxt

    __table__ = Table(
        't001', metadata,
        Column('bukrs', VARCHAR(8), primary_key=True),
        Column('butxt', VARCHAR(50))
    )
    bukrs: Optional[str]
    butxt: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawBut000:
    def __eq__(self, other) -> bool:
        return self.partner == other.partner \
               and self.name_org1 == other.name_org1 \
               and self.name_org2 == other.name_org2 \
               and self.name_org3 == other.name_org3 \
               and self.name_org4 == other.name_org4

    __table__ = Table(
        'but000', metadata,
        Column('partner', VARCHAR(20), primary_key=True),
        Column('name_org1', VARCHAR(80)),
        Column('name_org2', VARCHAR(80)),
        Column('name_org3', VARCHAR(80)),
        Column('name_org4', VARCHAR(80))
    )
    partner: Optional[str]
    name_org1: Optional[str]
    name_org2: Optional[str]
    name_org3: Optional[str]
    name_org4: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawDraw:
    def __eq__(self, other) -> bool:
        return self.dokar == other.dokar \
               and self.doknr == other.doknr \
               and self.zz_num_reg == other.zz_num_reg \
               and self.zz_hbkid == other.zz_hbkid \
               and self.zz_vvsart == other.zz_vvsart \
               and self.zz_sfhaart == other.zz_sfhaart \
               and self.zz_bankname == other.zz_bankname \
               and self.zz_begda == other.zz_begda \
               and self.zz_doknr == other.zz_doknr

    __table__ = Table(
        'draw', metadata,
        Column('dokar', VARCHAR(6), primary_key=True),
        Column('doknr', VARCHAR(50), primary_key=True),
        Column('zz_num_reg', VARCHAR(50)),
        Column('zz_hbkid', VARCHAR(10)),
        Column('zz_vvsart', VARCHAR(6)),
        Column('zz_sfhaart', VARCHAR(6)),
        Column('zz_bankname', VARCHAR(120)),
        Column('zz_begda', Date),
        Column('zz_doknr', VARCHAR(50))
    )
    dokar: Optional[str]
    doknr: Optional[str]
    zz_num_reg: Optional[str]
    zz_hbkid: Optional[str]
    zz_vvsart: Optional[str]
    zz_sfhaart: Optional[str]
    zz_bankname: Optional[str]
    zz_begda: Optional[date]
    zz_doknr: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawVtbAsgnLimit:
    def __eq__(self, other) -> bool:
        return self.relat_obj == other.relat_obj \
               and self.limit_date == other.limit_date \
               and self.bukrs_relat_obj == other.bukrs_relat_obj \
               and self.rfha_relat_obj == other.rfha_relat_obj \
               and self.limit_currency == other.limit_curreny \
               and self.limit_pos_amount == other.limit_pos_amount

    __table__ = Table(
        'vtb_asgn_limit', metadata,
        Column('relat_obj', VARCHAR(22), primary_key=True),
        Column('limit_date', DateTime, primary_key=True),
        Column('bukrs_relat_obj', VARCHAR),
        Column('rfha_relat_obj', VARCHAR(26), primary_key=True),
        Column('limit_currency', VARCHAR(10), primary_key=True),
        Column('limit_pos_amount', Float)
    )
    relat_obj: Optional[str]
    limit_date: Optional[datetime]
    bukrs_relat_obj: Optional[str]
    rfha_relat_obj: Optional[str]
    limit_currency: Optional[str]
    limit_pos_amount: Optional[float]


@mapper_registry.mapped
@define(slots=False)
class RawTracvAccitem:
    def __eq__(self, other) -> bool:
        return self.company_code == other.company_code \
               and self.deal_number == other.deal_number \
               and self.acpostingdate == other.acpostingdate \
               and self.dis_flowtype == other.dis_flowtype \
               and self.posting_key == other.posting_key \
               and self.position_amt == other.position_amt \
               and self.position_curr == other.position_curr \
               and self.document_guid == other.document_guid \
               and self.item_number == other.item_number \
               and self.os_guid_pi == other.os_guid_pi

    __table__ = Table(
        'tracv_accitem', metadata,
        Column('company_code', VARCHAR(6)),
        Column('deal_number', VARCHAR(26)),
        Column('acpostingdate', Date),
        Column('dis_flowtype', VARCHAR(8)),
        Column('posting_key', VARCHAR(2)),
        Column('position_amt', Float),
        Column('position_curr', VARCHAR(10)),
        Column('document_guid', VARCHAR(32), primary_key=True),
        Column('item_number', Integer, primary_key=True),
        Column('os_guid_pi', VARCHAR(32), primary_key=True)
    )
    company_code: Optional[str]
    deal_number: Optional[str]
    acpostingdate: Optional[date]
    dis_flowtype: Optional[str]
    posting_key: Optional[str]
    position_amt: Optional[float]
    position_curr: Optional[str]
    document_guid: Optional[str]
    item_number: Optional[int]
    os_guid_pi: Optional[str]


@mapper_registry.mapped
@define(slots=False)
class RawKblp:
    def __eq__(self, other) -> bool:
        return self.belnr == other.belnr \
               and self.blpos == other.blpos \
               and self.zz_rfha == other.zz_rfha \
               and self.zz_rfhazu == other.zz_rfhazu \
               and self.zz_rfhazb == other.zz_rfhazb \
               and self.zz_dcrdat == other.zz_dcrdat \
               and self.zz_tcrtim == other.zz_tcrtim \
               and self.wtges == other.wtges

    __table__ = Table(
        'kblp', metadata,
        Column('belnr', VARCHAR(20), primary_key=True),
        Column('blpos', Integer, primary_key=True),
        Column('zz_rfha', VARCHAR(26)),
        Column('zz_rfhazu', Integer),
        Column('zz_rfhazb', Integer),
        Column('zz_dcrdat', Date),
        Column('zz_tcrtim', Time),
        Column('wtges', Float)
    )
    belnr: Optional[str]
    blpos: Optional[int]
    zz_rfha: Optional[str]
    zz_rfhazu: Optional[int]
    zz_rfhazb: Optional[int]
    zz_dcrdat: Optional[date]
    zz_tcrtim: Optional[time]
    wtges: Optional[float]
