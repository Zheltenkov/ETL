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

metadata = MetaData(schema='cdm',
                    naming_convention={
                        'ix': 'ix_%(column_0_label)s',
                        'uq': 'uq_%(table_name)s_%(column_0_name)s',
                        'ck': 'ck_%(table_name)s_%(constraint_name)s',
                        'fk': 'fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s',
                        'pk': 'pk_%(table_name)s'
                    })


@mapper_registry.mapped
@define(slots=False)
class Metrica1:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs \
               and self.pyord == other.pyord \
               and self.belnr == other.belnr \
               and self.gjahr == other.gjahr \
               and self.hbkid == other.hbkid \
               and self.zbukr == other.zbukr \
               and self.butxt == other.butxt \
               and self.laufd == other.laufd \
               and self.text1 == other.text1 \
               and self.waers_b == other.waers_b \
               and self.numsfh == other.numsfh \
               and self.rbetr == other.rbetr \
               and self.zz_bezakc == other.zz_bezakc \
               and self.zz_pyord_r == other.zz_pyord_r \
               and self.zz_zh2h == other.zz_zh2h \
               and self.kblnr == other.kblnr \
               and self.waers_t == other.waers_t \
               and self.zz_vblnr == other.zz_vblnr \
               and self.awkey == other.awkey \
               and self.zz_rfha == other.zz_rfha \
               and self.partner == other.partner \
               and self.name_org1 == other.name_org1 \
               and self.name_org2 == other.name_org2 \
               and self.name_org3 == other.name_org3 \
               and self.name_org4 == other.name_org4

    __table__ = Table(
        'metrica_1', metadata,
        Column('bukrs', VARCHAR, nullable=False, primary_key=True),
        Column('pyord', VARCHAR, nullable=False, primary_key=True),
        Column('belnr', VARCHAR, nullable=False, primary_key=True),
        Column('gjahr', Integer, nullable=False, primary_key=True),
        Column('hbkid', VARCHAR, nullable=False, primary_key=True),
        Column('zbukr', VARCHAR, nullable=False, primary_key=True),
        Column('butxt', VARCHAR),
        Column('laufd', DateTime),
        Column('text1', VARCHAR),
        Column('waers_b', VARCHAR),
        Column('numsfh', VARCHAR),
        Column('rbetr', Float),
        Column('zz_bezakc', VARCHAR),
        Column('zz_pyord_r', VARCHAR),
        Column('zz_zh2h', VARCHAR),
        Column('kblnr', VARCHAR),
        Column('waers_t', VARCHAR),
        Column('zz_vblnr', VARCHAR),
        Column('awkey', VARCHAR),
        Column('zz_rfha', VARCHAR),
        Column('partner', VARCHAR),
        Column( 'name_org1', VARCHAR),
        Column('name_org2', VARCHAR),
        Column('name_org3', VARCHAR),
        Column('name_org4', VARCHAR)
    )
    bukrs: str
    pyord: str
    belnr: str
    gjahr: int
    hbkid: str
    zbukr: str
    butxt: Optional[str]
    laufd: Optional[datetime]
    text1: Optional[str]
    waers_b: Optional[str]
    numsfh: Optional[str]
    rbetr: Optional[float]
    zz_bezakc: Optional[str]
    zz_pyord_r: Optional[str]
    zz_zh2h: Optional[str]
    kblnr: Optional[str]
    waers_t: Optional[str]
    zz_vblnr: Optional[str]
    awkey: Optional[str]
    zz_rfha: Optional[str]
    partner: Optional[str]
    name_org1: Optional[str]
    name_org2: Optional[str]
    name_org3: Optional[str]
    name_org4: Optional[str]

@mapper_registry.mapped
@define(slots=False)
class Metrica23:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs and \
               self.rfha == other.rfha and \
               self.dcrdat == other.dcrdat and \
               self.tcrtim == other.tcrtim and \
               self.zz_hbkid == other.zz_hbkid and \
               self.zuond == other.zuond and \
               self.banks == other.banks and \
               self.zz_num_reg == other.zz_num_reg and \
               self.nordext == other.nordext and \
               self.sgsart == other.sgsart and \
               self.sfhaart == other.sfhaart and \
               self.zz_bankname == other.zz_bankname and \
               self.wgschft == other.wgschft and \
               self.kontrh == other.kontrh and \
               self.dbuchung == other.dbuchung and \
               self.ssign == other.ssign and \
               self.bzbetr == other.bzbetr and \
               self.dis_flowtype == other.dis_flowtype and \
               self.posting_key == other.posting_key and \
               self.position_amt == other.position_amt and \
               self.position_curr == other.position_curr and \
               self.sfhazba == other.sfhazba and \
               self.limit_date == other.limit_date and \
               self.rfha_relat_obj == other.rfha_relat_obj and \
               self.limit_currency == other.limit_currency and \
               self.limit_pos_amount == other.limit_pos_amount and \
               self.rfhazu == other.rfhazu and \
               self.rfhazb == other.rfhazb and \
               self.dzterm == other.dzterm and \
               self.saktiv == other.saktiv and \
               self.sbewebe == other.sbewebe and \
               self.acpostingdate == other.acpostingdate and \
               self.dokar == other.dokar and \
               self.doknr == other.doknr and \
               self.document_guid == other.document_guid and \
               self.item_number == other.item_number and \
               self.os_guid_pi == other.os_guid_pi

    __table__ = Table(
        'metrica_2_3', metadata,
        Column('bukrs', VARCHAR, nullable=False, primary_key=True),
        Column('rfha', VARCHAR, nullable=False, primary_key=True),
        Column('dcrdat', Date, nullable=False, primary_key=True),
        Column('tcrtim', Time, nullable=False, primary_key=True),
        Column('zz_hbkid', VARCHAR),
        Column('zuond', VARCHAR),
        Column('banks', VARCHAR),
        Column('zz_num_reg', DateTime),
        Column('nordext', VARCHAR),
        Column('sgsart', VARCHAR),
        Column('sfhaart', VARCHAR),
        Column('zz_bankname', VARCHAR),
        Column('wgschft', VARCHAR),
        Column('kontrh', VARCHAR),
        Column('dbuchung', Date),
        Column('ssign', VARCHAR),
        Column('bzbetr', Float),
        Column('dis_flowtype', VARCHAR),
        Column('posting_key', VARCHAR),
        Column('position_amt', Float),
        Column('position_curr', VARCHAR),
        Column('sfhazba', VARCHAR),
        Column('limit_date', DateTime),
        Column('rfha_relat_obj', VARCHAR),
        Column('limit_currency', VARCHAR),
        Column('limit_pos_amount', Float),
        Column('rfhazu', Integer, primary_key=True),
        Column('rfhazb', Integer, primary_key=True),
        Column('dzterm', Date),
        Column('saktiv', Integer),
        Column('sbewebe', VARCHAR(1)),
        Column('acpostingdate', Date),
        Column('dokar', VARCHAR(6), primary_key=True),
        Column('doknr', VARCHAR(50), primary_key=True),
        Column('document_guid', VARCHAR(32), primary_key=True),
        Column('item_number', Integer, primary_key=True),
        Column('os_guid_pi', VARCHAR(32), primary_key=True)
    )

    bukrs: str
    rfha: str
    dcrdat: date
    tcrtim: time
    zz_hbkid: Optional[str]
    zuond: Optional[str]
    banks: Optional[str]
    zz_num_reg: Optional[str]
    nordext: Optional[str]
    sgsart: Optional[str]
    sfhaart: Optional[str]
    zz_bankname: Optional[str]
    wgschft: Optional[str]
    kontrh: Optional[str]
    dbuchung: Optional[date]
    ssign: Optional[str]
    bzbetr: Optional[float]
    dis_flowtype: Optional[str]
    posting_key: Optional[str]
    position_amt: Optional[float]
    position_curr: Optional[str]
    sfhazba: Optional[str]
    limit_date: Optional[datetime]
    rfha_relat_obj: Optional[str]
    limit_currency: Optional[str]
    limit_pos_amount: Optional[float]
    rfhazu: int
    rfhazb: int
    dzterm: Optional[date]
    saktiv: Optional[int]
    sbewebe: Optional[str]
    acpostingdate: Optional[date]
    dokar: str
    doknr: str
    document_guid: str
    item_number: int
    os_guid_pi: str


@mapper_registry.mapped
@define(slots=False)
class Metrica4:
    def __eq__(self, other) -> bool:
        return self.bukrs == other.bukrs and \
               self.rfha == other.rfha and \
               self.dcrdat == other.dcrdat and \
               self.tcrtim == other.tcrtim and \
               self.zuond == other.zuond and \
               self.zz_num_reg == other.zz_num_reg and \
               self.zz_bankname == other.zz_bankname and \
               self.butxt == other.butxt and \
               self.sgsart == other.sgsart and \
               self.sfhaart == other.sfhaart and \
               self.wgschft == other.wgschft and \
               self.ssign == other.ssign and \
               self.dzterm == other.dzterm and \
               self.bzbetr == other.bzbetr and \
               self.rfhazu == other.rfhazu and \
               self.rfhazb == other.rfhazb and \
               self.zz_begda == other.zz_begda and \
               self.sfhazba == other.sfhazba and \
               self.sbewebe == other.sbewebe and \
               self.dberbis == other.dberbis and \
               self.khwkurs == other.khwkurs and \
               self.dbervon == other.dbervon and \
               self.wzbetr == other.wzbetr              

    __table__ = Table(
        'metrica_4', metadata,
        Column('bukrs', VARCHAR, nullable=False, primary_key=True),
        Column('rfha', VARCHAR, nullable=False, primary_key=True),
        Column('dcrdat', Date, nullable=False, primary_key=True),
        Column('tcrtim', Time, nullable=False, primary_key=True),
        Column('zuond', VARCHAR),
        Column('zz_num_reg', VARCHAR),
        Column('zz_bankname', VARCHAR),
        Column('butxt', VARCHAR),
        Column('sgsart', VARCHAR),
        Column('sfhaart', VARCHAR),
        Column('wgschft', VARCHAR),
        Column('ssign', VARCHAR),
        Column('dzterm', Date),
        Column('bzbetr', Float),
        Column('rfhazu', Integer, nullable=False, primary_key=True),
        Column('rfhazb', Integer, nullable=False, primary_key=True),
        Column('zz_begda', Date),
        Column('sfhazba', VARCHAR),
        Column('sbewebe', VARCHAR),
        Column('dberbis', Date),
        Column('khwkurs', Float),
        Column('dbervon', Date),
        Column('wzbetr', VARCHAR)
    )
    bukrs: str
    rfha: str
    dcrdat: date
    tcrtim: time
    zuond: str
    zz_num_reg: Optional[str]
    zz_bankname: Optional[str]
    butxt: Optional[str]
    sgsart: Optional[str]
    sfhaart: Optional[str]
    wgschft: Optional[str]
    ssign: Optional[str]
    dzterm: Optional[date]
    bzbetr: Optional[float]
    rfhazu: int
    rfhazb: int
    zz_begda: Optional[date]
    sfhazba: Optional[str]
    sbewebe: Optional[str]
    dberbis: Optional[date]
    khwkurs: Optional[float]
    dbervon: Optional[date]
    wzbetr: Optional[str]

