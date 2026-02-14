from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel

class Region(Document):
  name: str
  
  class Settings:
    name = "region"
    indexes = [
      IndexModel(["name"], unique=True),
    ]