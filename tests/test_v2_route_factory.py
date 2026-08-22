from types import SimpleNamespace

from bson import ObjectId
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from src.core.v2_route_factory import clone_router
from src.modules.public.schema import SimpleApplicationListResponse


def test_clone_router_validates_beanie_style_attributes():
  source = APIRouter()

  @source.get("/application/enabled", response_model=SimpleApplicationListResponse)
  async def source_endpoint():
    return {
      "data": [SimpleNamespace(
        id=ObjectId("507f1f77bcf86cd799439011"),
        name="demo",
        shown_name="Demo",
      )]
    }

  async def delegate(endpoint, *args, **kwargs):
    return await endpoint(*args, **kwargs)

  app = FastAPI()
  app.include_router(clone_router(source, delegate), prefix="/api/v2")
  response = TestClient(app).get("/api/v2/application/enabled")

  assert response.status_code == 200
  assert response.json()["data"] == [{
    "id": "507f1f77bcf86cd799439011",
    "name": "demo",
    "shown_name": "Demo",
  }]
