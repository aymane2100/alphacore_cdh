import os

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


def get_azure_vault_client(client_secret=os.getenv("AZURE_KEYVAULT_SECRET")):
    VAULT_URL = "https://cylon.vault.azure.net"

    vault_credential = ClientSecretCredential(
        tenant_id="24139d14-c62c-4c47-8bdd-ce71ea1d50cf",
        client_id="2bef733d-75be-4159-b280-672e054938c3",
        client_secret=client_secret,
    )

    return SecretClient(vault_url=VAULT_URL, credential=vault_credential)
