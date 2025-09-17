from irods.access import iRODSAccess
from irods.exception import UserDoesNotExist
from irods.models import User
from irods.path import iRODSPath
from irods.session import iRODSSession
from irods.user import iRODSUser


class DataStoreAPI(object):
    _user_type = "rodsuser"

    def __init__(self, host: str, port: str, user: str, password: str, zone: str):
        self.session = iRODSSession(
            host=host, port=port, user=user, password=password, zone=zone
        )
        self.session.connection_timeout = None
        self.host = host
        self.port = port
        self.user = user
        self.zone = zone

    def path_exists(self, a_path: str) -> bool:
        fixed_path = iRODSPath(a_path)
        return self.session.data_objects.exists(
            fixed_path
        ) or self.session.collections.exists(fixed_path)

    def user_exists(self, username: str) -> bool:
        user_exists = False

        try:
            user = self.session.users.get(username, self.zone)
            user_exists = user is not None
        except UserDoesNotExist:
            user_exists = False

        return user_exists

    def list_users_by_username(self, username: str) -> list[iRODSUser]:
        return [
            self.session.users.get(u[User.name], u[User.zone])
            for u in self.session.query(User).filter(
                User.name == username and User.zone == self.zone
            )
        ]

    def delete_home(self, username: str) -> None:
        homedir = self.home_directory(username)
        if self.session.collections.exists(homedir):
            self.session.collections.remove(homedir, force=True, recurse=True)

    def create_user(self, username: str) -> iRODSUser:
        return self.session.users.create(username, DataStoreAPI._user_type)

    def get_user(self, username: str) -> iRODSUser:
        return self.session.users.get(username, self.zone)

    def delete_user(self, username: str) -> None:
        self.session.users.get(username, self.zone).remove()

    def change_password(self, username: str, password: str) -> None:
        self.session.users.modify(username, "password", password)

    def chmod(self, username: str, permission: str, path: str) -> None:
        access = iRODSAccess(permission, iRODSPath(path), username)
        self.session.acls.set(access)

    def list_available_permissions(self) -> list[str]:
        return self.session.available_permissions.keys()

    def get_permissions(self, path: str) -> list[iRODSAccess]:
        clean_path = iRODSPath(path)

        obj = None
        if self.session.data_objects.exists(clean_path):
            obj = self.session.data_objects.get(clean_path)
        else:
            obj = self.session.collections.get(clean_path)

        return self.session.acls.get(obj)

    def home_directory(self, username: str) -> str:
        return iRODSPath(f"/{self.zone}/home/{username}")

    def ensure_user_exists(self, username: str) -> iRODSUser:
        """
        Ensure a user exists in iRODS, creating them and their home directory if necessary.

        Args:
            username: The username to ensure exists

        Returns:
            iRODSUser: The user object (either existing or newly created)

        Raises:
            Exception: If user or home directory creation fails
        """
        # Check if user already exists
        if self.user_exists(username):
            return self.get_user(username)

        # Create the user
        user = self.create_user(username)

        # Ensure home directory exists
        home_dir = self.home_directory(username)
        if not self.path_exists(home_dir):
            # Create home directory
            self.session.collections.create(home_dir)
            # Set ownership of home directory to the user
            self.chmod(username=username, permission="own", path=home_dir)

        return user
