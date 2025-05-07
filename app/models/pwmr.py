# app/models/pwmr.py
from sqlalchemy import Column, Integer, String, DECIMAL, ForeignKey, Index
from sqlalchemy.orm import relationship
from app.db import Base

class PwmrJob(Base):
    __tablename__ = "pwmr_jobs"

    id            = Column(Integer, primary_key=True, index=True)
    created_at    = Column(String(64))   # if you have timestamp
    Type_of_entity  = Column(String(64))
    Entity_Name     = Column(String(128))
    Email           = Column(String(256))

    # backrefs
    next_targets = relationship("NextTarget", back_populates="job", cascade="all, delete")

class NextTarget(Base):
    __tablename__ = "next_target"

    id                = Column(Integer, primary_key=True, index=True)
    job_id            = Column(Integer, ForeignKey("pwmr_jobs.id", ondelete="CASCADE"), index=True)
    next_year         = Column(Integer)  # Changed from YEAR to Integer
    projected_amount  = Column(DECIMAL(15,3))
    Type_of_entity    = Column(String(64))
    Entity_Name       = Column(String(128))
    Email             = Column(String(256))

    job = relationship("PwmrJob", back_populates="next_targets")
