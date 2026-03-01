import os
from pathlib import Path
from fastapi import APIRouter, Request, Depends, Form, Response
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import select, Session
from fastapi.responses import FileResponse

# Internal Imports
from auth import verify_password
from database import get_session
from models import User

router = APIRouter()

# --- PATH CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLIENT_DIR = BASE_DIR / "client"

# --- TEMPLATES ---
templates = {
    "home": Jinja2Templates(directory=str(CLIENT_DIR / "HomePage")),
    "login": Jinja2Templates(directory=str(CLIENT_DIR / "LoginPage")),
}

# --- PAGE ROUTES ---
@router.get("/")
async def read_root(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates["home"].TemplateResponse("homepage.html", {"request": request, "user": user})

@router.get("/login")
async def login_page(request: Request):
    if request.session.get("user"):
        return RedirectResponse(url="/", status_code=303)
    return templates["login"].TemplateResponse("login.html", {"request": request})

@router.post("/login")
async def login_user(request: Request, username: str = Form(...), password: str = Form(...), session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.username == username)).first()
    if user and verify_password(password, user.password_hash):
        request.session["user"] = user.username
        return RedirectResponse(url="/", status_code=303)
    return templates["login"].TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")

@router.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@router.get("/audit/gps")
async def serve_gps_corner(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
        
    return FileResponse(CLIENT_DIR / "GPSCorner" / "gps_corner.html")

# Add this to the bottom of page_route_api.py
@router.get("/b2b-maker")
async def serve_b2b_maker(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(CLIENT_DIR / "B2BCorner" / "b2b_corner.html")
# --- INDIVIDUAL AUDIT PAGES (From Sidebar) ---

@router.get("/audit/b2b")
async def serve_b2b_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
    # Points to the new B2B Maker UI we just created
    return FileResponse(CLIENT_DIR / "B2BCorner" / "b2b_corner.html")


@router.get("/audit/incomplete")
async def serve_incomplete_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
    # Since you haven't built this HTML yet, we return a temporary placeholder
    return HTMLResponse(
        "<div style='font-family: sans-serif; padding: 40px; text-align: center;'>"
        "<h2>🚧 Incomplete Trips Page is Under Construction 🚧</h2>"
        "<p>You will design this page next!</p>"
        "</div>"
    )
@router.get("/audit/toll")
async def serve_toll_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(CLIENT_DIR / "AuditCorner" / "audit_toll.html")