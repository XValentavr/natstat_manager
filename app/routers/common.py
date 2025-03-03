from fastapi import APIRouter


router = APIRouter()


@router.get("/")
def default():
    return {"status": "ok", "service_name": "Boilerplate fastapi project"}
