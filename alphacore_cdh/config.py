import os, yaml, toml

from alphacore_cdh.services.azure_keyvault import get_azure_vault_client


class Config:

    ENV = os.getenv("SERVER_ENVIRONMENT", "default")
    CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

    config = None
    poetry_config = None

    @classmethod
    def init(cls):
        """[OPTIONAL] Load config files into class variables"""
        cls.load_configuration()
        cls.load_poetry_configuration()

    @classmethod
    def get_item(cls, section, key):
        """ Retrieve configuration key """
        if not cls.config:
            cls.load_configuration()
        return cls.config[section][key]

    @classmethod
    def get_poetry_item(cls, section, key):
        """ Retrieve poetry config key """
        if not cls.poetry_config:
            cls.load_poetry_configuration()
        return cls.poetry_config[section][key]

    # --------- [Configuration file with Azure Key] ---------
    @classmethod
    def load_configuration(cls) -> dict:
        file_path = os.path.join(cls.CURRENT_DIR, "..", "configuration", f"configuration_{cls.ENV}.yml")
        yaml.add_constructor("!AZURE_VAULT", cls._get_azure_vault_from_conf)
        if os.path.isfile(file_path):
            with open(file_path, "r") as config_file:
                cls.config = yaml.load(config_file, Loader=yaml.FullLoader)

    @classmethod
    def _get_azure_vault_from_conf(cls, loader, node):
        values = loader.construct_sequence(node)
        azure_key = values[0]
        return get_azure_vault_client().get_secret(azure_key).value

    # --------- [Poetry config file] ---------
    @classmethod
    def load_poetry_configuration(cls) -> dict:
        poetry_file_path = os.path.join(cls.CURRENT_DIR, "..", "pyproject.toml")
        cls.poetry_config = toml.load(poetry_file_path)


if __name__ == "__main__":
    print(Config.get_item('azure-storage', 'container'))
