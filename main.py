import os
import sys

import ds

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

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
    try:
        exists = server.path_exists(path)
    except Exception as e:
        raise HTTPException(500, e)
    return {"path" : path, "exists" : exists}

@app.get("/users/{username}/exists", status_code=200)
def user_exists(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    try:
        exists = server.user_exists(username)
    except Exception as e:
        raise HTTPException(500, e)
    return {"user":username, "exists": exists}

@app.get("/permissions/available", status_code=200)
def list_available_permissions():
    try:
        list = server.list_available_permissions()
    except Exception as e:
        raise HTTPException(500, e)
    return {"permissions" : list}

@app.get("/path/permissions", status_code=200)
def path_permissions(path: str):
    if path == "":
        raise HTTPException(400, "path query parameter is not set")
    try:
        perms = server.get_permissions(path)
    except Exception as e:
        raise HTTPException(500, e)
    return {"permissions" : perms}

@app.post("/users/{username}", status_code=200)
def create_user(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    if server.user_exists(username):
        raise HTTPException(400, "user exists")
    try:
        irods_user = server.create_user(username)
    except Exception as e:
        raise HTTPException(500, e)
    return {
        "user" : irods_user.name,
        "type" : irods_user.type,
        "zone" : irods_user.zone,
    }

@app.delete("/users/{username}", status_code=200)
def delete_user(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    try:
        server.delete_user(username)
    except Exception as e:
        raise HTTPException(500, e)
    return {"user" : username}

@app.delete("/users/{username}/home", status_code=200)
def delete_home(username: str):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    try:
        server.delete_home(username)
    except Exception as e:
        raise HTTPException(500, e)
    return {
        "user" : username,
        "home" : server.home_directory(username)
    }

class PasswordChange(BaseModel):
    password: str

@app.post("/users/{username}/password", status_code=200)
def change_password(username: str, password_change: PasswordChange):
    if username == "":
        raise HTTPException(400, "username must not be empty")
    try:
        server.change_password(username, password_change.password)
    except Exception as e:
        raise HTTPException(500, e)
    return {"user" : username}


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
    try:
        server.chmod(perm_change.username, perm_change.permission, perm_change.path)
    except Exception as e:
        raise HTTPException(500, e)
    return perm_change
