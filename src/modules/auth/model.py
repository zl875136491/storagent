from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
  
class Role(Document):
  name: str
  
  class Settings:
    name = "role"
    indexes = [
      IndexModel(["name"], unique=True),
    ]
  
class User(Document):
  itcode: str
  
  class Settings:
    name = "user"
    indexes = [
      IndexModel(["itcode"], unique=True),
    ]
  
class TempCode(Document):
  user: Link[User]
  code: str
  expired_at: datetime
  
  class Settings:
    name = "temp_code"
    indexes = [
      IndexModel(["expired_at"]),
    ]
  
class DestoryedToken(Document):
  token: str
  expired_at: datetime
  
  class Settings:
    name = "destoryed_token"
    indexes = [
      IndexModel(["token"], unique=True),
      IndexModel(["expired_at"]),
    ]