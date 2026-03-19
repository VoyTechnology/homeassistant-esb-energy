"""Downloader for ESB Energy data via the ESB web portal."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from urllib.parse import quote
from yarl import URL
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from aiohttp import ClientResponseError, ClientSession, CookieJar
from bs4 import BeautifulSoup

from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .csv_utils import InvalidCsvFile, merge_csv_content, validate_csv_header

_LOGGER = logging.getLogger(__name__)

_STORE_VERSION = 1


@dataclass
class FetchResult:
    """Result of a fetch attempt."""

    success: bool
    message: str


class ESBDownloader:
    """Handle scheduled downloads and CSV merging."""

    def __init__(
        self,
        hass,
        entry_id: str,
        csv_file: str,
        mprn: str,
        username: str,
        password: str,
        interval_hours: int,
    ) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._csv_file = csv_file
        self._mprn = mprn
        self._username = username
        self._password = password
        self._interval = timedelta(hours=max(1, interval_hours))
        # Use a dedicated session with an unsafe cookie jar so Azure B2C cookies
        # from the login domain are preserved across redirects.
        self._session: ClientSession = ClientSession(cookie_jar=CookieJar(unsafe=True))
        self._store = Store(hass, _STORE_VERSION, f"{DOMAIN}.{entry_id}")
        self._last_fetch: Optional[datetime] = None
        self._unsub = None

    @property
    def last_fetch(self) -> Optional[datetime]:
        """Return the last successful fetch timestamp."""
        return self._last_fetch

    async def async_start(self) -> None:
        """Start the scheduled downloader."""
        await self._load_state()
        self._unsub = async_track_time_interval(
            self._hass, self._handle_interval, self._interval
        )
        self._hass.async_create_task(self.async_maybe_fetch("startup"))

    async def async_stop(self) -> None:
        """Stop the scheduled downloader."""
        if self._unsub:
            self._unsub()
            self._unsub = None
        if not self._session.closed:
            await self._session.close()

    async def async_maybe_fetch(self, reason: str) -> FetchResult:
        """Fetch if due and configured."""
        if not self._credentials_configured():
            return FetchResult(False, "Credentials not configured; skipping download.")
        if not self._mprn:
            return FetchResult(False, "MPRN not configured; skipping download.")
        if not self._csv_file:
            return FetchResult(False, "CSV file path not configured; skipping download.")

        now = dt_util.utcnow()
        if self._last_fetch:
            next_due = self._last_fetch + self._interval
            if now < next_due:
                return FetchResult(False, "Not due yet; skipping download.")

        try:
            csv_text = await self._download_csv()
            if not csv_text:
                return FetchResult(False, "No CSV content returned.")
            validate_csv_header(csv_text)
            target_path = Path(self._csv_file)
            await asyncio.to_thread(target_path.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(merge_csv_content, target_path, csv_text)
            self._last_fetch = now
            await self._save_state()
            return FetchResult(True, "Download succeeded.")
        except (ClientResponseError, InvalidCsvFile, ValueError) as exc:
            _LOGGER.error("Download failed: %s", exc)
            return FetchResult(False, str(exc))
        except Exception as exc:  # pragma: no cover - safety net
            _LOGGER.exception("Unexpected download failure: %s", exc)
            return FetchResult(False, str(exc))

    async def _handle_interval(self, _now) -> None:
        """Handle interval tick."""
        result = await self.async_maybe_fetch("interval")
        if result.success:
            _LOGGER.info("ESB download completed.")
        elif "Not due yet" not in result.message:
            _LOGGER.debug("ESB download skipped: %s", result.message)

    def _credentials_configured(self) -> bool:
        return bool(self._username and self._password)

    async def _load_state(self) -> None:
        data = await self._store.async_load() or {}
        last_fetch_raw = data.get("last_fetch")
        if last_fetch_raw:
            parsed = dt_util.parse_datetime(last_fetch_raw)
            if parsed:
                self._last_fetch = dt_util.as_utc(parsed)

    async def _save_state(self) -> None:
        if not self._last_fetch:
            return
        await self._store.async_save(
            {"last_fetch": dt_util.as_utc(self._last_fetch).isoformat()}
        )

    async def _download_csv(self) -> str:
        """Log in and download the CSV content."""
        login_page, authorize_url = await self._request_text_with_url(
            "GET",
            "https://myaccount.esbnetworks.ie/",
            allow_redirects=True,
            headers=_browser_headers(),
            log_label="Authorize",
        )
        _LOGGER.debug("Authorize URL resolved to: %s", authorize_url)
        _LOGGER.debug(
            "Login cookie names before SelfAsserted: %s",
            self._cookie_names_for_domain("login.esbnetworks.ie"),
        )
        settings = _extract_settings(login_page)
        csrf = settings.get("csrf")
        trans_id = settings.get("transId")
        if not csrf or not trans_id:
            raise ValueError("Unable to extract login flow settings.")
        _LOGGER.debug(
            "Extracted login settings (csrf len=%s, transId present=%s)",
            len(csrf) if csrf else 0,
            bool(trans_id),
        )
        csrf_cookie = self._cookie_value_for_domain("login.esbnetworks.ie", "x-ms-cpim-csrf")
        _LOGGER.debug(
            "CSRF cookie present=%s, matches header=%s",
            bool(csrf_cookie),
            bool(csrf_cookie) and csrf_cookie == csrf,
        )

        self_asserted_url = _build_self_asserted_url(trans_id)
        _LOGGER.debug("SelfAsserted URL: %s", self_asserted_url)
        self_asserted_headers = {
            **_browser_headers(),
            "X-CSRF-TOKEN": csrf,
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://login.esbnetworks.ie",
            "Referer": authorize_url,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "*/*",
        }
        _LOGGER.debug(
            "SelfAsserted headers (redacted): %s",
            _redact_headers(self_asserted_headers),
        )
        await self._request_text(
            "POST",
            _raw_url(self_asserted_url),
            data={
                "signInName": self._username,
                "password": self._password,
                "request_type": "RESPONSE",
            },
            headers=self_asserted_headers,
            log_label="SelfAsserted",
        )

        confirmed_url = _build_confirmed_url(csrf, trans_id)
        _LOGGER.debug("Confirmed URL: %s", confirmed_url)
        confirmed = await self._request_text(
            "GET",
            _raw_url(confirmed_url),
            headers={
                "Referer": authorize_url,
                **_browser_headers(),
            },
            log_label="Confirmed",
        )

        form_data = _extract_auto_form(confirmed)
        await self._request_text(
            "POST",
            form_data["action"],
            data={
                "state": form_data["state"],
                "client_info": form_data["client_info"],
                "code": form_data["code"],
            },
            headers=_browser_headers(),
            allow_redirects=False,
            log_label="SigninOidc",
        )

        await self._request_text("GET", "https://myaccount.esbnetworks.ie/")
        await self._request_text(
            "GET", "https://myaccount.esbnetworks.ie/Api/HistoricConsumption"
        )

        token_payload = await self._request_json(
            "GET",
            "https://myaccount.esbnetworks.ie/af/t",
            headers={
                "x-ReturnUrl": "https://myaccount.esbnetworks.ie/Api/HistoricConsumption"
            },
        )
        token = token_payload.get("token")
        if not token:
            raise ValueError("Unable to obtain download token.")

        search_type = self._select_search_type()
        _LOGGER.debug(
            "ESB download using searchType=%s (interval=%s, last_fetch=%s)",
            search_type,
            self._interval,
            self._last_fetch,
        )
        download = await self._request_text(
            "POST",
            "https://myaccount.esbnetworks.ie/DataHub/DownloadHdfPeriodic",
            json_payload={"mprn": self._mprn, "searchType": search_type},
            headers={
                "x-xsrf-token": token,
                "x-ReturnUrl": "https://myaccount.esbnetworks.ie/Api/HistoricConsumption",
                "Origin": "https://myaccount.esbnetworks.ie",
                "Referer": "https://myaccount.esbnetworks.ie/Api/HistoricConsumption",
            },
        )
        return download

    def _select_search_type(self) -> str:
        """Choose the CSV search type based on interval and first-run behavior."""
        if self._last_fetch is None:
            return "day"
        if self._interval > timedelta(hours=24):
            return "day"
        return "intervalkw"

    async def _request_text(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
        json_payload: dict | None = None,
        headers: dict | None = None,
        allow_redirects: bool = True,
        log_label: str | None = None,
    ) -> str:
        async with self._session.request(
            method,
            url,
            params=params,
            data=data,
            json=json_payload,
            headers=headers,
            allow_redirects=allow_redirects,
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                label = f"{log_label}: " if log_label else ""
                _LOGGER.error("%sHTTP %s response body: %s", label, resp.status, text)
                raise ValueError(f"{label}HTTP {resp.status} response body: {text[:800]}")
            return text

    async def _request_text_with_url(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
        json_payload: dict | None = None,
        headers: dict | None = None,
        allow_redirects: bool = True,
        log_label: str | None = None,
    ) -> tuple[str, str]:
        async with self._session.request(
            method,
            url,
            params=params,
            data=data,
            json=json_payload,
            headers=headers,
            allow_redirects=allow_redirects,
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                label = f"{log_label}: " if log_label else ""
                _LOGGER.error("%sHTTP %s response body: %s", label, resp.status, text)
                raise ValueError(f"{label}HTTP {resp.status} response body: {text[:800]}")
            return text, str(resp.url)

    def _cookie_names_for_domain(self, domain: str) -> list[str]:
        jar = self._session.cookie_jar.filter_cookies(f"https://{domain}/")
        return sorted(jar.keys())

    def _cookie_value_for_domain(self, domain: str, name: str) -> str | None:
        jar = self._session.cookie_jar.filter_cookies(f"https://{domain}/")
        morsel = jar.get(name)
        return morsel.value if morsel else None


def _redact_headers(headers: dict[str, str]) -> dict[str, str]:
    redacted = {}
    for key, value in headers.items():
        if key.lower() in {"authorization", "cookie"}:
            redacted[key] = "<redacted>"
        elif key.lower() in {"x-csrf-token"}:
            redacted[key] = f"<len:{len(value)}>"
        else:
            redacted[key] = value
    return redacted

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
    ) -> dict:
        async with self._session.request(
            method,
            url,
            params=params,
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()
            return json.loads(text)


def _extract_settings(html: str) -> dict:
    match = re.search(r"var SETTINGS = (\{.*?\});", html, re.S)
    if not match:
        raise ValueError("Login settings not found.")
    return json.loads(match.group(1))


def _extract_auto_form(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", {"id": "auto"})
    if not form:
        raise ValueError("Login form not found.")
    action = form.get("action")
    state = form.find("input", {"name": "state"})
    client_info = form.find("input", {"name": "client_info"})
    code = form.find("input", {"name": "code"})
    if not (action and state and client_info and code):
        raise ValueError("Login form fields missing.")
    return {
        "action": action,
        "state": state.get("value", ""),
        "client_info": client_info.get("value", ""),
        "code": code.get("value", ""),
    }


def _build_self_asserted_url(trans_id: str) -> str:
    return (
        "https://login.esbnetworks.ie/"
        "esbntwkscustportalprdb2c01.onmicrosoft.com/"
        "B2C_1A_signup_signin/SelfAsserted"
        f"?tx={trans_id}&p=B2C_1A_signup_signin"
    )


def _build_confirmed_url(csrf: str, trans_id: str) -> str:
    csrf_safe = quote(csrf, safe="")
    return (
        "https://login.esbnetworks.ie/"
        "esbntwkscustportalprdb2c01.onmicrosoft.com/"
        "B2C_1A_signup_signin/api/CombinedSigninAndSignup/confirmed"
        f"?rememberMe=false&csrf_token={csrf_safe}&tx={trans_id}&p=B2C_1A_signup_signin"
    )


def _raw_url(url: str) -> URL:
    # Preserve raw query params (notably tx=StateProperties=...) without encoding.
    return URL(url, encoded=True)


def _browser_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) "
            "Gecko/20100101 Firefox/148.0"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
