from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend
from fastapi import Request
from sqlmodel import Session, select

# Internal Imports
from database import engine
# ✅ FIX: Imported the updated model names and added the new ones
from models import (
    User, 
    AppTripData, 
    ManualTripData, 
    ClientData, 
    RawTripData, 
    OperationTripData,
    BARowData,
    T3AddressLocality,
    TollData,
    TollRouteRule
)
from auth import verify_password, get_password_hash
from config import get_settings

cfg = get_settings()


# --- 1. AUTHENTICATION BACKEND ---
class AdminAuth(AuthenticationBackend):
    async def login(self, request: Request) -> bool:
        form = await request.form()
        username, password = form.get("username"), form.get("password")
        with Session(engine) as session:
            user = session.exec(select(User).where(User.username == username)).first()
            if user and verify_password(password, user.password_hash):
                request.session.update({"user": user.username})
                return True
        return False

    async def logout(self, request: Request) -> bool:
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        return request.session.get("user") in ["admin", "chickenman"]


# --- 2. ADMIN VIEWS ---
class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.username]

    async def on_model_change(self, data, model, is_created, request):
        password = data.get("password_hash")
        if password and not (len(password) == 60 and password.startswith("$")):
            hashed = get_password_hash(password)
            model.password_hash = hashed
            data["password_hash"] = hashed


# ✅ FIX: Updated to AppTripData
class AppTripDataAdmin(ModelView, model=AppTripData):
    name = "App Trip Data"
    column_list = [AppTripData.shift_date, AppTripData.unique_id, AppTripData.employee_name]

# ✅ NEW: Added view for Manual Trip Data
class ManualTripDataAdmin(ModelView, model=ManualTripData):
    name = "Manual Trip Data"
    column_list = [ManualTripData.shift_date, ManualTripData.unique_id, ManualTripData.employee_name]

# ✅ NEW: Added view for Operation Trip Data
class OperationTripDataAdmin(ModelView, model=OperationTripData):
    name = "Operation Trip Data"
    column_list = [OperationTripData.shift_date, OperationTripData.unique_id, OperationTripData.employee_name]

class ClientDataAdmin(ModelView, model=ClientData):
    name = "Client Data"
    column_list = [ClientData.id, ClientData.unique_id]

class RawTripDataAdmin(ModelView, model=RawTripData):
    name = "Raw Trip Data"
    column_list = [RawTripData.id, RawTripData.unique_id]

class BARowDataAdmin(ModelView, model=BARowData):
    name = "BA Row Data"
    column_list = [BARowData.id, BARowData.unique_id]

class AddressLocalityAdmin(ModelView, model=T3AddressLocality):
    name = "Address Master"
    column_list = [T3AddressLocality.id, T3AddressLocality.address, T3AddressLocality.locality]

# ✅ NEW: Added Toll Data Admin
class TollDataAdmin(ModelView, model=TollData):
    name = "Toll Data"
    # Using the new toll_name / una_toll columns
    column_list = [TollData.id, TollData.toll_name, TollData.una_toll, TollData.amount, TollData.travel_date_time]

# ✅ NEW: Added Toll Route Rules Admin
class TollRouteRuleAdmin(ModelView, model=TollRouteRule):
    name = "Toll Route Rules"
    column_list = [TollRouteRule.landmark, TollRouteRule.office, TollRouteRule.toll_name, TollRouteRule.is_toll_route]


# --- 3. THE SETUP FUNCTION ---
def setup_admin(app):
    """
    This function is called by main.py.
    It attaches the Admin interface to the main FastAPI app.

    FIX: Use cfg.secret_key instead of a hardcoded string.
         A different key from the session key previously caused session
         conflicts on HTTPS platforms like Render.
    """
    admin = Admin(app, engine, authentication_backend=AdminAuth(secret_key=cfg.secret_key))

    # Add Views
    admin.add_view(UserAdmin)
    admin.add_view(AppTripDataAdmin)
    admin.add_view(ManualTripDataAdmin)
    admin.add_view(OperationTripDataAdmin)
    admin.add_view(ClientDataAdmin)
    admin.add_view(RawTripDataAdmin)
    admin.add_view(BARowDataAdmin)
    admin.add_view(AddressLocalityAdmin)
    admin.add_view(TollDataAdmin)
    admin.add_view(TollRouteRuleAdmin)

    return admin