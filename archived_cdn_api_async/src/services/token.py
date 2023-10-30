import http

import aiohttp
from fastapi import HTTPException, Request, status as st
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

import core.config as conf


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super().__init__(auto_error=auto_error)

    async def __call__(self, request: Request) -> str:
        """
        Returns access token for request taken from header Authorization
        :param request:
        :return:
        """
        creds: HTTPAuthorizationCredentials = await super().__call__(request)
        if not creds:
            raise HTTPException(status_code=http.HTTPStatus.FORBIDDEN,
                                detail='Invalid authorization code.')
        if not creds.scheme == 'Bearer':
            raise HTTPException(status_code=http.HTTPStatus.UNAUTHORIZED,
                                detail='Only Bearer token might be accepted')
        return creds.credentials


security_jwt = JWTBearer()


async def check_roles(token: str, roles: str) -> None:
    try:
        headers = {'Authorization': f'Bearer {token}'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url=f'http://{conf.settings.host_auth}:'
                                        f'{conf.settings.port_auth}'
                                        f'/api/v1/users/check_roles',
                                    json={
                                        'roles': f'{roles}'
                                    }) as status:
                detail = await status.json()
                status_code = status.status
                if status_code != st.HTTP_200_OK:
                    raise HTTPException(
                        status_code=status_code,
                        detail=detail['detail'],
                        headers={"WWW-Authenticate": "Bearer"},
                    )
    except aiohttp.ServerTimeoutError as err:
        raise HTTPException(status_code=st.HTTP_504_GATEWAY_TIMEOUT,
                            detail=err.strerror)
    except aiohttp.TooManyRedirects as err:
        raise HTTPException(status_code=st.HTTP_502_BAD_GATEWAY,
                            detail=err.strerror)
    except aiohttp.ClientError as err:
        raise HTTPException(status_code=st.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=err.strerror)
