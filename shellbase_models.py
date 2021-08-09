from sqlalchemy import Table, Column, Integer, Float, String, MetaData, DateTime, Boolean, func, Text, SmallInteger, REAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

Base = declarative_base()

class Lkp_Area_Classification(Base):
  __tablename__ = "lkp_area_classification"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))

class Lkp_Tide(Base):
  __tablename__ = "lkp_tide"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))

class Lkp_Sample_Strategy(Base):
  __tablename__ = "lkp_sample_strategy"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))
  description = Column(String(400))

class Lkp_Sample_Reason(Base):
  __tablename__ = "lkp_sample_reason"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))

class Lkp_Fc_Analysis_Method(Base):
  __tablename__ = "lkp_fc_analysis_method"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))

class Lkp_Sample_Type(Base):
  __tablename__ = "lkp_sample_type"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))

class Lkp_Sample_Units(Base):
  __tablename__ = "lkp_sample_units"
  id = Column(Integer, primary_key=True)
  name = Column(String(50))
  long_name = Column(String(100))

class Areas(Base):
  __tablename__ = "areas"
  id = Column(Integer, primary_key=True)
  row_update_date = Column(String(32))

  name = Column(String(50))
  state = Column(String(2))

  classification = Column(Integer, ForeignKey("lkp_area_classification.id"))
  #classification = relationship("Lkp_Area_Classification", backref="areas")

  def as_dict(self):
    ret_dict = {}
    ret_dict['name'] = self.name
    ret_dict['state'] = self.state
    return ret_dict

class History_Areas_Closure(Base):
  __tablename__ = "history_areas_closure"
  id = Column(Integer, primary_key=True)

  current = Column(Boolean, nullable=True)

  start_date = Column(String(32))
  end_date = Column(String(32))

  comments = Column(String(400))


  area_id = Column(Integer, ForeignKey("areas.id"))
  #area = relationship("Areas", backref="history_areas_closure")

class History_Areas_Classification(Base):
  __tablename__ = "history_areas_classification"
  id = Column(Integer, primary_key=True)

  current = Column(Boolean, nullable=True)

  start_date = Column(String(32))
  end_date = Column(String(32))

  comments = Column(String(400))


  area_id = Column(Integer, ForeignKey("areas.id"))
  #area = relationship("Areas", backref="history_areas_classification")

  classification_id = Column(Integer, ForeignKey("lkp_area_classification.id"))
  #classification = relationship("Lkp_Area_Classification", backref="history_areas_classification")


class Stations(Base):
  __tablename__ = "stations"
  id = Column(Integer, primary_key=True)
  row_update_date = Column(String(32))

  name = Column(String(50))
  state = Column(String(2))

  area_id = Column(Integer, ForeignKey("areas.id"))
  area = relationship("Areas", lazy="select")

  lat = Column(Float, nullable=True)
  long = Column(Float, nullable=True)

  sample_depth_type = Column(String(2))
  sample_depth = Column(Float, nullable=True)

  active = Column(Boolean, nullable=True)


  def as_dict(self):
    ret_dict = {}
    ret_dict['name'] = self.name
    ret_dict['state'] = self.state
    ret_dict['sample_depth_type'] = self.sample_depth_type
    ret_dict['sample_depth'] = self.sample_depth
    ret_dict['active'] = self.active
    if self.area_id:
      ret_dict['area'] = self.area.as_dict()
    return ret_dict


class Samples(Base):
  __tablename__ = "samples"
  id = Column(Integer, primary_key=True)
  row_update_date = Column(String(32))

  sample_datetime = Column(String(32))
  date_only = Column(Boolean, nullable=True)

  station_id = Column("station_id", Integer, ForeignKey("stations.id"))
  #station = relationship("Stations", backref="samples")

  tide_id = Column("tide_id", Integer, ForeignKey("lkp_tide.id"))
  #tide = relationship("Lkp_Tide", backref="samples")

  strategy_id = Column("strategy_id", Integer, ForeignKey("lkp_sample_strategy.id"))
  #strategy = relationship("Lkp_Sample_Strategy", backref="samples")

  reason_id = Column("reason_id", Integer, ForeignKey("lkp_sample_reason.id"))
  reason = relationship("Lkp_Sample_Reason", backref="samples")

  fc_analysis_method_id = Column("fc_analysis_method_id", Integer, ForeignKey("lkp_fc_analysis_method.id"))
  #fc_analysis_method = relationship("Lkp_Fc_Analysis_Method", backref="samples")

  type_id = Column("type_id", Integer, ForeignKey("lkp_sample_type.id"))
  #type = relationship("Lkp_Sample_Type", backref="samples")

  units_id = Column("units_id", Integer, ForeignKey("lkp_sample_units.id"))
  #units = relationship("Lkp_Sample_Units", backref="samples")

  value = Column(Float, nullable=False)
  flag = Column(String(50))
