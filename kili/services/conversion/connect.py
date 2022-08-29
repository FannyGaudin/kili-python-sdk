# """
# Connect to kili api
# """

# import os

# from kili.client import Kili


# def sign_in():
#     """
#     Sign in with Kili Python SDK
#     """
#     url = os.getenv("ENDPOINT__BACKEND_URL_FROM_SERVICES")
#     endpoint = f'{os.getenv("ENDPOINT__API_V2")}{os.getenv("ENDPOINT__GRAPHQL")}'
#     api_endpoint = f"{url}{endpoint}"
#     api_key = os.getenv("KILI__API_KEY")
#     verify = os.getenv("KILI__VERIFY_SSL") != "False"
#     return Kili(api_endpoint=api_endpoint, api_key=api_key, verify=verify)
