import os
import os.path
import sys

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

import ds

app = FastAPI()


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    print(exc, file=sys.stderr)
    return JSONResponse(content={"detail": exc.detail}, status_code=exc.status_code)


@app.middleware("http")
async def exception_handling_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return JSONResponse(content=str(e), status_code=500)


ds_host = os.environ.get("IRODS_HOST")
if ds_host is None:
    print("Environment variable IRODS_HOST is not set.", file=sys.stderr)
    sys.exit(1)

ds_port = os.environ.get("IRODS_PORT")
if ds_port is None:
    print("Environment variable IRODS_PORT is not set.", file=sys.stderr)
    sys.exit(1)

ds_user = os.environ.get("IRODS_USER")
if ds_user is None:
    print("Environment variable IRODS_USER is not set.", file=sys.stderr)
    sys.exit(1)

ds_password = os.environ.get("IRODS_PASSWORD")
if ds_password is None:
    print("Environment variable IRODS_PASSWORD is not set.", file=sys.stderr)
    sys.exit(1)

ds_zone = os.environ.get("IRODS_ZONE")
if ds_zone is None:
    print("Environment variable IRODS_ZONE is not set.", file=sys.stderr)
    sys.exit(1)

server = ds.DataStoreAPI(ds_host, ds_port, ds_user, ds_password, ds_zone)


@app.get("/", status_code=200)
def hello():
    return "Hello from portal-datastore."


@app.get("/path/exists", status_code=200)
def path_exists(path: str):
    if path == "":
        raise HTTPException(400, "path query parameter is not set")
    exists = server.path_exists(path)
    return {"path": path, "exists": exists}


@app.get("/users/{username}/exists", status_code=200)
def user_exists(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    exists = server.user_exists(username)
    return {"user": username, "exists": exists}


@app.get("/permissions/available", status_code=200)
def list_available_permissions():
    list = server.list_available_permissions()
    return {"permissions": list}


@app.get("/path/permissions", status_code=200)
def path_permissions(path: str):
    if path == "":
        raise HTTPException(400, "path query parameter is not set")
    perms = server.get_permissions(path)
    return {"permissions": perms}


@app.post("/users/{username}", status_code=200)
def create_user(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    if server.user_exists(username):
        raise HTTPException(400, "user exists")
    irods_user = server.create_user(username)
    return {
        "user": irods_user.name,
        "type": irods_user.type,
        "zone": irods_user.zone,
    }


@app.delete("/users/{username}", status_code=200)
def delete_user(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    server.delete_user(username)
    return {"user": username}


@app.get("/users/{username}/home", status_code=200)
def get_home_dir(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    home_dir = server.home_directory(username)
    return {"user": username, "home": home_dir}


@app.delete("/users/{username}/home", status_code=200)
def delete_home(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    server.delete_home(username)
    return {"user": username, "home": server.home_directory(username)}


class PasswordChange(BaseModel):
    password: str


@app.post("/users/{username}/password", status_code=200)
def change_password(username: str, password_change: PasswordChange):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    server.change_password(username, password_change.password)
    return {"user": username}


class PathPermission(BaseModel):
    username: str
    path: str
    permission: str


@app.post("/path/chmod", status_code=200)
def chmod(perm_change: PathPermission):
    if perm_change.username == "":
        raise HTTPException(400, "username must be set in request body")
    if perm_change.path == "":
        raise HTTPException(400, "path must be set in request body")
    if perm_change.permission == "":
        raise HTTPException(400, "permission must be set in request body")
    if not server.user_exists(perm_change.username):
        raise HTTPException(400, f"username {perm_change.username} does not exist")
    if perm_change.permission not in server.list_available_permissions():
        raise HTTPException(400, f"permission {perm_change.permission} does not exist")
    if not server.path_exists(perm_change.path):
        raise HTTPException(400, f"path {perm_change.path} does not exist")
    server.chmod(perm_change.username, perm_change.permission, perm_change.path)
    return perm_change


class ServiceRegistration(BaseModel):
    username: str
    irods_path: str
    irods_user: str | None = None


@app.post("/services/register", status_code=200)
def service_registration(registration: ServiceRegistration):
    if registration.username == "":
        raise HTTPException(400, "username must not be empty")
    if registration.irods_path == "":
        raise HTTPException(400, "irods_path must not be empty")

    # Ensure user exists (create if necessary)
    try:
        user = server.ensure_user_exists(registration.username)
        print(f"User {registration.username} is ready for service registration", file=sys.stderr)
    except Exception as e:
        print(f"Failed to ensure user {registration.username} exists: {str(e)}", file=sys.stderr)
        raise HTTPException(500, f"Failed to prepare user {registration.username}: {str(e)}")

    home_dir = server.home_directory(registration.username)

    full_path = os.path.join(home_dir, registration.irods_path)
    if not server.path_exists(full_path):
        server.session.collections.create(full_path)

    server.chmod(username="", permission="inherit", path=full_path)
    server.chmod(username=registration.username, permission="own", path=full_path)
    if registration.irods_user is not None:
        server.chmod(username=registration.irods_user, permission="own", path=full_path)

    return {
        "user": registration.username,
        "irods_path": full_path,
        "irods_user": registration.irods_user,
    }
