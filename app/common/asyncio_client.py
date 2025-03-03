import asyncio
import json
import traceback
from datetime import datetime
from time import time
from typing import Union, Optional

from aiohttp import TCPConnector, ClientSession, ClientTimeout
from fastapi import HTTPException

from app.common.config import ASYNC_CONCURENT_BY_HOST
from app.common.header_sign import add_agent_details
from app.common.logging import LogMixin
from app.common.logging import get_logger, get_took_status
from app.common.utils import get_london_now, get_utc_now


class AsyncAiohttpSessionClient(LogMixin):
    def __init__(self):
        super().__init__()
        self._session = None
        self.limit_per_host = ASYNC_CONCURENT_BY_HOST
        self.processing_now: bool = False  # have a live call now.
        self.recreate_session_now: bool = False  # recreate session is in progress
        self.recreated_at: Optional[datetime] = None  # when session was recreated last time

    async def recreate_session(self):
        self.log("Recreating session...")
        # if session was created less then 5s ago - return
        need_skip = await self.is_need_to_skip_recreate()
        if need_skip:
            return

        self.recreate_session_now = True
        await self.close_session_with_timeout()
        await self._create_session()
        self.recreate_session_now = False

    async def is_need_to_skip_recreate(self):
        from app.common.utils import get_london_now

        recreated_ago_is_ok = (
            self.recreated_at is not None
            and (get_london_now() - self.recreated_at).total_seconds() < 60
        )
        skip = False
        if recreated_ago_is_ok:
            self.log("Session was created less then 5s ago - return")
            skip = True
        if self.recreate_session_now:
            self.log("Session is already recreating - return")
            skip = True
        return skip

    async def close_session_with_timeout(self):
        try:
            # wait before self.processing_now is False
            while self.processing_now:
                self.log("Waiting for processing_now to be False...")
                await asyncio.sleep(0.1)

            self.log("Attempting to close old session...")
            await asyncio.wait_for(self.close_session(), timeout=5)
            self.log("Session closed.")
        except asyncio.TimeoutError:
            self.log("Session creation timed out.")

    @property
    async def session(self):
        while self.recreate_session_now:
            self.log("Waiting for session to be recreated...")
            await asyncio.sleep(0.1)

        if self._session is None:
            await self._create_session()
        return self._session

    async def _create_session(self):
        self.log("Creating session...")
        timeout = ClientTimeout(total=25)
        conn = TCPConnector(
            limit_per_host=self.limit_per_host,  # max number of connections per host
            ttl_dns_cache=600,  # DNS cache TTL in seconds
            verify_ssl=False,  # disable ssl verification, because all servers internal
        )
        self._session = ClientSession(connector=conn, timeout=timeout, connector_owner=True)
        self.recreated_at = get_london_now()
        self.log("Session created")

    async def _get(
        self,
        url,
        params=None,
        headers=None,
        timeout=10,
        ok_statuses: list[int] | tuple[int] = (200,),
        **kwargs,
    ):
        session = await self.session
        t1 = time()
        client_resp = await session.get(url, params=params, timeout=timeout, headers=headers)

        return await self.prepare_response(client_resp, url, t1, "GET", ok_statuses)

    async def _post(
        self,
        url,
        params=None,
        payload=None,
        headers=None,
        timeout=10,
        ok_statuses: list[int] | tuple[int] = (200,),
        **kwargs,
    ):
        get_logger().info(
            f"POST started {url} - , payload: {payload}, kwargs: {kwargs}, "
            f"headers: {headers}, timeout: {timeout}"
        )
        t1 = time()

        session = await self.session

        client_resp = await session.post(
            url, params=params, timeout=timeout, json=payload, headers=headers
        )
        return await self.prepare_response(client_resp, url, t1, "POST", ok_statuses)

    async def prepare_response(
        self,
        response,
        url,
        time_start: float,
        method: str,
        ok_statuses: list[int] | tuple[int],
    ) -> Union[dict, list, str]:
        status = response.status
        if status not in ok_statuses:
            raise Exception(f"{status} {method} {url}")

        data = await response.text()

        duration = int((time() - time_start) * 1000)
        took_status = get_took_status(duration)
        self.log(f"{method}  took {duration}ms  ({took_status}) {url}")

        try:
            data = json.loads(data)
        except:  # noqa: 722
            pass
        return data

    # on class destruction - close session
    async def __aexit__(self, exc_type, exc, tb):
        await self.close_session()

    # close session
    async def close_session(self):
        if self._session is not None:
            self.log("Closing session")
            await self._session.close()

    async def get(self, url, timeout: int = 30, **kwargs) -> dict | list[dict]:
        start_time = get_utc_now()
        try:
            result = await self._get(url, timeout=timeout, **kwargs)
            return result
        except TimeoutError:
            get_logger().info(f"TimeoutError error get {url}, timeout: {timeout}")
            await self.recreate_session()
            result = await self._get(url, timeout=timeout * 2, **kwargs)
            return result
        except HTTPException as e:
            self.log(f"GET {str(e)[:60]}, start time - {start_time}, url - {url}")
            raise e
        except Exception as e:
            self.log(f"GET {e}, start time - {start_time}, url - {url}")
            traceback.print_exception(e)
            await self.recreate_session()
            get_logger().info(f"try to get again {url}, timeout: {timeout * 2}")
            result = await self._get(url, timeout=timeout * 2, **kwargs)
            return result

    async def post(
        self,
        url,
        params: Optional[dict] = None,
        payload: Optional[Union[dict, list, str]] = None,
        headers: Optional[dict] = None,
        timeout: int = 30,
        **kwargs,
    ):
        start_time = get_utc_now()
        headers = add_agent_details(headers, kwargs.get("source"))

        try:
            return await self._post(
                url,
                params=params,
                timeout=timeout,
                payload=payload,
                headers=headers,
                **kwargs,
            )
        except TimeoutError:
            get_logger().info(f"TimeoutError error post {url}, timeout: {timeout}")
            await self.recreate_session()
            return await self._post(
                url,
                payload=payload,
                params=params,
                headers=headers,
                timeout=timeout * 2,
                **kwargs,
            )
        except HTTPException as e:
            self.log(f"POST {str(e)[:60]}, start time - {start_time}, url - {url}")
            raise e

        except Exception as e:
            self.log(f"POST {e}, start time - {start_time}, url - {url}")
            await self.recreate_session()
            get_logger().info(f"try to post again {url}, timeout: {timeout * 2}")
            return await self._post(
                url,
                params=params,
                timeout=timeout * 2,
                payload=payload,
                headers=headers,
                **kwargs,
            )


if __name__ == "__main__":
    client = AsyncAiohttpSessionClient()

    async def get():
        await client.get("http://python.org")
        await client.get("http://python.org")
        await client.get("http://python.org")
        # await client.close_session()

    asyncio.run(get())
