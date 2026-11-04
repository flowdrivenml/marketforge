"""Async archive discovery migrated from the old acquisition sources.

No current instrument listing is consulted for explicit historical instruments.
The web-only Bybit/Bitget interfaces are intentionally validated fail-closed.
"""

import re
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import quote, unquote, urljoin, urlsplit

import httpx

from forge.historical.archives.config import (
    AcquisitionInterval,
    ArchiveRequest,
    ArchiveScope,
)
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.sources.base import (
    DiscoveryError,
    ProbeEvent,
    UnsupportedRequestError,
    archive,
    check_code,
    daily,
    mapping,
    milliseconds,
    overlaps,
    records,
    sequence,
    text,
    windows,
)
from forge.shared.async_work import bounded_map
from forge.shared.http.client import HttpClient


class Links(HTMLParser):
    """Read archive links and directory-listing headings."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.listing = False
        self.headings: list[str] = []
        self._heading_parts: list[str] | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag in {"pre", "table"}:
            self.listing = True

        if tag in {"title", "h1"}:
            self._heading_parts = []

        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)

    def handle_data(self, data: str) -> None:
        if self._heading_parts is not None:
            self._heading_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"title", "h1"} and self._heading_parts is not None:
            heading = " ".join("".join(self._heading_parts).split())
            self.headings.append(heading)
            self._heading_parts = None


class ExchangeSource:
    """A single upstream's archive adapter, sharing its HTTP client and budget."""

    version = "archive-v4-okx-futureschain"

    def __init__(
        self,
        exchange: str,
        http: HttpClient,
        *,
        on_activity: Callable[[str], None] | None = None,
        on_probe: Callable[[ProbeEvent], None] | None = None,
    ) -> None:
        self.exchange = exchange
        self.http = http
        self.on_activity = on_activity
        self.on_probe = on_probe

    async def _get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        if self.on_activity is not None:
            self.on_activity("Fetching discovery listing")
        return await self.http.get(
            url,
            params=params,
            headers=headers,
            max_response_bytes=16 * 1024 * 1024,
            follow_redirects=True,
        )

    async def _post(self, url: str, *, json: object, retry_safe: bool) -> httpx.Response:
        if self.on_activity is not None:
            self.on_activity("Reading historical availability")
        return await self.http.post(
            url, json=json, retry_safe=retry_safe, max_response_bytes=16 * 1024 * 1024
        )

    def validate(self, request: ArchiveRequest) -> None:
        if request.exchange != self.exchange or self.exchange not in {
            "binance",
            "bybit",
            "bitget",
            "okx",
            "gateio",
        }:
            raise UnsupportedRequestError("Unsupported historical exchange.")
        if not re.fullmatch(r"[A-Za-z0-9_.:/-]+", request.scope.identifier):
            raise UnsupportedRequestError("Unsupported source scope identifier.")
        if request.scope.identifier in {".", "..", "ANY"}:
            raise UnsupportedRequestError("Use a specific historical scope.")
        if request.instrument_type == "option":
            if request.market_category != "option":
                raise UnsupportedRequestError("Options require market_category = 'option'.")
            if self.exchange not in {"bybit", "okx"}:
                raise UnsupportedRequestError("This exchange has no supported option archives.")
        elif request.instrument_type == "spot":
            if request.market_category != "spot":
                raise UnsupportedRequestError("Spot instruments require the spot market category.")
        elif request.market_category not in {"linear", "inverse"}:
            raise UnsupportedRequestError("Derivatives require linear or inverse market category.")
        if self.exchange == "binance" and request.data_type != "trades":
            raise UnsupportedRequestError("Binance supports historical trades only.")
        if (
            self.exchange == "bitget"
            and request.instrument_type == "future"
            and request.market_category != "inverse"
        ):
            raise UnsupportedRequestError("Bitget linear dated futures are not supported.")
        if self.exchange == "okx":
            permitted = {"instrument"} if request.instrument_type == "spot" else {"family"}
            if request.instrument_type == "option":
                permitted.add("base_coin")
            if request.scope.kind not in permitted:
                raise UnsupportedRequestError(
                    "OKX derivatives require an explicit family; options also accept base_coin."
                )
        elif self.exchange == "bybit" and request.instrument_type == "option":
            if request.scope.kind != "base_coin":
                raise UnsupportedRequestError(
                    "Bybit option archives require base_coin scope, not a contract."
                )
        elif request.scope.kind != "instrument":
            raise UnsupportedRequestError("This archive source requires instrument scope.")

    async def discover(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        self.validate(request)
        interval = AcquisitionInterval(
            start=interval.start.astimezone(UTC), end=interval.end.astimezone(UTC)
        )
        method = {
            "binance": self._binance,
            "bybit": self._bybit,
            "bitget": self._bitget,
            "okx": self._okx,
            "gateio": self._gateio,
        }[self.exchange]
        async for item in method(request, interval):
            if overlaps(item, interval):
                yield item

    async def _binance(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        prefix = (
            {
                "spot": "data/spot/daily/trades/",
                "linear": "data/futures/um/daily/trades/",
                "inverse": "data/futures/cm/daily/trades/",
            }[request.market_category]
            + request.scope.identifier
            + "/"
        )
        marker: str | None = None
        namespace = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        while True:
            params = {"delimiter": "/", "prefix": prefix}
            if marker is not None:
                params["marker"] = marker
            response = await self._get(
                "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
                params=params,
            )
            try:
                root = ET.fromstring(response.text)
            except ET.ParseError:
                raise DiscoveryError("Malformed Binance S3 XML.") from None
            if root.tag != namespace + "ListBucketResult":
                raise DiscoveryError("Unexpected Binance S3 response.")
            truncated = root.findtext(namespace + "IsTruncated")
            if truncated not in {"true", "false"}:
                raise DiscoveryError("Missing S3 pagination status.")
            for entry in root.findall(namespace + "Contents"):
                key = text(entry.findtext(namespace + "Key"))
                if not key.startswith(prefix):
                    raise DiscoveryError("S3 returned an object outside the requested prefix.")
                if key.endswith(".zip"):
                    filename = key.rsplit("/", 1)[-1]
                    observed = archive(
                        request,
                        filename,
                        "https://data.binance.vision/" + quote(key, safe="/"),
                        daily(filename),
                        key=key,
                    )
                    etag = entry.findtext(namespace + "ETag")
                    modified = entry.findtext(namespace + "LastModified")
                    if etag is not None:
                        observed = observed.model_copy(update={"source_etag": etag})
                    if modified is not None:
                        try:
                            timestamp = datetime.fromisoformat(modified)
                        except ValueError:
                            raise DiscoveryError("Invalid Binance object timestamp.") from None
                        if timestamp.tzinfo is None:
                            raise DiscoveryError("Binance object timestamp has no timezone.")
                        observed = observed.model_copy(update={"source_last_modified": timestamp})
                    yield observed
            if truncated == "false":
                return
            next_marker = text(root.findtext(namespace + "NextMarker"))
            if marker is not None and next_marker <= marker:
                raise DiscoveryError("Repeated or non-advancing Binance pagination cursor.")
            marker = next_marker

    async def _bybit(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        if request.instrument_type == "option":
            calendar_interval = interval.model_copy(
                update={"start": interval.start.replace(hour=0, minute=0, second=0, microsecond=0)}
            )
            for window in windows(calendar_interval, 7):
                response = await self._get(
                    "https://www.bybit.com/x-api/quote/public/support/download/list-files",
                    params={
                        "bizType": "option",
                        "productId": ("trade" if request.data_type == "trades" else "orderbook"),
                        "symbols": request.scope.identifier,
                        "interval": "daily",
                        "periods": "",
                        "startDay": window.start.date().isoformat(),
                        "endDay": (window.end - timedelta(microseconds=1)).date().isoformat(),
                    },
                    headers={"Referer": "https://www.bybit.com/derivatives/en/history-data"},
                )
                body = mapping(response.json())
                check_code(body, "retCode", "0")
                for value in records(body, "result", ("files", "list")):
                    item = mapping(value)
                    filename = text(item.get("filename") or item.get("fileName"))
                    yield archive(
                        request,
                        filename,
                        text(item.get("url")),
                        daily(filename),
                        key="option/" + request.data_type + "/" + filename,
                    )
            return
        if request.data_type == "trades":
            root = "https://public.bybit.com/" + (
                "spot/" if request.market_category == "spot" else "trading/"
            )
        else:
            root = f"https://quote-saver.bycsi.com/orderbook/{request.market_category}/"
        url = root + quote(request.scope.identifier, safe="") + "/"
        response = await self._get(url)
        if any(
            token in response.text.casefold()
            for token in ("access denied", "accessdenied", "request blocked", "<error>")
        ):
            raise DiscoveryError("Bybit returned an access/error page instead of a listing.")
        parser = Links()
        parser.feed(response.text)
        resolved = urlsplit(str(response.url))
        requested = urlsplit(url)
        expected_path = unquote(requested.path).rstrip("/")
        if (
            resolved.hostname != requested.hostname
            or unquote(resolved.path).rstrip("/") != expected_path
        ):
            raise DiscoveryError(
                "Bybit listing redirected outside the requested instrument directory."
            )
        expected_headings = {
            f"directory listing for {expected_path}".casefold(),
            f"index of {expected_path}".casefold(),
        }
        named_listing = any(
            heading.casefold().rstrip("/") in expected_headings for heading in parser.headings
        )
        legacy_listing = parser.listing and any(href in {"../", ".."} for href in parser.links)
        if not (named_listing or (legacy_listing and not parser.headings)):
            raise DiscoveryError("Bybit response is not a directory listing.")
        base = str(response.url).split("?", 1)[0].rstrip("/") + "/"
        seen: set[str] = set()
        symbol = re.escape(request.scope.identifier)
        for href in parser.links:
            target = urlsplit(urljoin(base, href))
            filename = unquote(target.path.rsplit("/", 1)[-1])
            if not filename.endswith((".csv.gz", ".zip", ".csv")):
                continue
            if not re.search(r"\d{4}-\d{2}-\d{2}", filename):
                continue
            if (
                target.scheme != "https"
                or target.netloc != resolved.netloc
                or unquote(target.path).rsplit("/", 1)[0] != expected_path
                or target.username
                or target.password
                or target.fragment
            ):
                raise DiscoveryError("Bybit archive link escaped the requested directory.")
            name_pattern = (
                symbol + r"(?:[_-])?(?P<day>\d{4}-\d{2}-\d{2})(?:_[A-Za-z0-9_-]+)?\.csv(?:\.gz)?"
                if request.data_type == "trades"
                else r"(?P<day>\d{4}-\d{2}-\d{2})_" + symbol + r"_ob\d+\.data\.zip"
            )
            name_match = re.fullmatch(name_pattern, filename)
            if name_match is None:
                raise DiscoveryError(
                    "Bybit archive filename does not match the requested instrument."
                )
            if filename in seen:
                continue
            seen.add(filename)
            yield archive(request, filename, urljoin(base, href), daily(name_match["day"]))

    async def _bitget(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        base = "https://www.bitget.com/v1/statistics/public/download/"
        symbol = request.scope.identifier
        business = 2 if request.data_type == "trades" else 3
        if request.instrument_type == "spot" and "/" not in symbol:
            response = await self._post(
                base + "getSymbolList",
                json={
                    "displaySymbol": symbol,
                    "businessLine": 1,
                    "businessType": business,
                    "languageType": 12,
                },
                retry_safe=True,
            )
            body = mapping(response.json())
            check_code(body, "code", "200")
            matches: set[str] = set()
            for value in records(body, "data", ("list", "data", "symbols")):
                if isinstance(value, str):
                    candidate = value
                else:
                    item = mapping(value)
                    candidate = text(
                        item.get("displaySymbol")
                        or item.get("symbol")
                        or item.get("symbolName")
                        or item.get("instId")
                    )
                if candidate.replace("/", "") == symbol:
                    matches.add(candidate)
            if len(matches) != 1:
                raise DiscoveryError(
                    "No unambiguous Bitget historical display symbol; supply BASE/QUOTE explicitly."
                )
            symbol = matches.pop()
        offset = timedelta(hours=8)
        calendar_interval = AcquisitionInterval(
            start=(interval.start + offset).replace(hour=0, minute=0, second=0, microsecond=0),
            end=interval.end + offset,
        )
        for window in windows(calendar_interval, 7):
            payload: dict[str, object] = {
                "displaySymbol": [symbol],
                "businessLine": 1 if request.instrument_type == "spot" else 2,
                "businessType": business,
                "dateType": 1,
                "beginTimeStr": window.start.date().isoformat(),
                "endTimeStr": (window.end - timedelta(microseconds=1)).date().isoformat(),
            }
            if request.data_type == "order_book_l2":
                payload["deptType"] = 2
            response = await self._post(base + "getPublicDataV2", json=payload, retry_safe=True)
            body = mapping(response.json())
            check_code(body, "code", "200")
            seen: dict[str, datetime] = {}
            for value in records(body, "data", ("list", "data", "files", "records")):
                item = mapping(value)
                filename = text(item.get("fileName"))
                if "displayName" in item and item["displayName"] != symbol:
                    raise DiscoveryError("Bitget returned an unrelated display symbol.")
                prefix = re.escape(symbol + "-" + symbol.replace("/", "") + "_")
                suffix = (
                    r"(\d{8})_\d+\.zip"
                    if request.data_type == "trades"
                    else (
                        r"1_(\d{8})\.zip"
                        if request.instrument_type == "spot"
                        else r"3_(\d{8})\.zip"
                    )
                )
                match = re.fullmatch(prefix + suffix, filename)
                if match is None:
                    raise DiscoveryError(
                        "Bitget archive filename does not match instrument/data type."
                    )
                file_day = daily(match[1])
                if (
                    item.get("dateTimeStr") is not None
                    and daily(text(item["dateTimeStr"])) != file_day
                ):
                    raise DiscoveryError("Bitget filename date conflicts with source metadata.")
                start = milliseconds(item["dateTime"]) if "dateTime" in item else file_day - offset
                if (
                    start.date() != file_day.date()
                    and (start + timedelta(hours=8)).date() != file_day.date()
                ):
                    raise DiscoveryError("Bitget timestamp conflicts with archive date.")
                observed = archive(
                    request,
                    filename,
                    text(item.get("fileUrl")),
                    start,
                    key="history/" + request.data_type + "/" + filename,
                )
                if not overlaps(
                    observed,
                    AcquisitionInterval(start=window.start - offset, end=window.end - offset),
                ):
                    continue
                if filename in seen:
                    if seen[filename] != start:
                        raise DiscoveryError("Bitget duplicate identity has conflicting intervals.")
                    continue
                seen[filename] = start
                yield observed

    async def _okx(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        if request.scope.kind == "base_coin":
            response = await self._get(
                "https://www.okx.com/api/v5/public/underlying",
                params={"instType": "OPTION"},
            )
            body = mapping(response.json())
            check_code(body, "code", "0")
            families: set[str] = set()
            for group in sequence(body.get("data")):
                for value in sequence(group):
                    family = text(value)
                    if family.startswith(request.scope.identifier + "-"):
                        families.add(family)
            if not families:
                raise DiscoveryError(
                    "No advertised OKX option family; use an explicit historical family."
                )
            for family in sorted(families):
                concrete = request.model_copy(
                    update={"scope": ArchiveScope(kind="family", identifier=family)}
                )
                async for item in self._okx(concrete, interval):
                    yield item
            return
        # Query one local calendar day per call to avoid upstream record truncation.
        # Trade dates use UTC+8, books UTC, as documented by OKX.
        offset = timedelta(hours=8) if request.data_type == "trades" else timedelta(0)
        current = (interval.start + offset).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - offset
        while current < interval.end:
            end = current + timedelta(days=1)
            params = {
                "module": "1" if request.data_type == "trades" else "5",
                "instType": {
                    "spot": "SPOT",
                    "perpetual": "SWAP",
                    "future": "FUTURES",
                    "option": "OPTION",
                }[request.instrument_type],
                "dateAggrType": "daily",
                "begin": str(int(current.timestamp() * 1000)),
                "end": str(int((end - timedelta(milliseconds=1)).timestamp() * 1000)),
                (
                    "instIdList" if request.instrument_type == "spot" else "instFamilyList"
                ): request.scope.identifier,
            }
            response = await self._get(
                "https://www.okx.com/api/v5/public/market-data-history", params=params
            )
            body = mapping(response.json())
            check_code(body, "code", "0")
            seen_files: dict[str, datetime] = {}
            for value in sequence(body.get("data")):
                for detail_value in sequence(mapping(value).get("details")):
                    detail = mapping(detail_value)
                    inst_type = {
                        "spot": "SPOT",
                        "perpetual": "SWAP",
                        "future": "FUTURES",
                        "option": "OPTION",
                    }[request.instrument_type]
                    if detail.get("instType") not in {None, "", inst_type}:
                        raise DiscoveryError("OKX returned the wrong instrument type.")
                    family = request.scope.identifier
                    if request.instrument_type == "spot" and detail.get("instFamily") not in {
                        None,
                        "",
                        family,
                    }:
                        raise DiscoveryError("OKX returned an unexpected spot family.")
                    if request.instrument_type != "spot" and detail.get("instFamily") != family:
                        raise DiscoveryError("OKX returned an unexpected family.")
                    returned_id = detail.get("instId")
                    if returned_id:
                        self._validate_okx_id(request, text(returned_id))
                    for file_value in sequence(detail.get("groupDetails")):
                        item = mapping(file_value)
                        filename = text(item.get("filename"))
                        marker = "-trades-" if request.data_type == "trades" else "-L2orderbook-"
                        # Accept the legacy fixture alias as well as the official L2 filename.
                        if marker not in filename and request.data_type == "order_book_l2":
                            marker = "-books-"
                        if marker not in filename:
                            raise DiscoveryError("Unrecognized OKX archive filename.")
                        file_id = filename.split(marker, 1)[0]
                        if file_id.endswith("-futureschain"):
                            # Historical dated-futures bundles identify a family, not a contract.
                            suffix = (
                                r"-trades-\d{4}-\d{2}-\d{2}\.zip"
                                if request.data_type == "trades"
                                else r"-L2orderbook-5000lv-\d{4}-\d{2}-\d{2}\.tar\.gz"
                            )
                            if not (
                                request.instrument_type == "future"
                                and request.scope.kind == "family"
                                and detail.get("instType") == "FUTURES"
                                and detail.get("instFamily") == request.scope.identifier
                                and returned_id == ""
                                and re.fullmatch(
                                    re.escape(request.scope.identifier + "-futureschain") + suffix,
                                    filename,
                                )
                            ):
                                raise DiscoveryError(
                                    "Invalid OKX futures family bundle metadata or filename."
                                )
                        else:
                            self._validate_okx_id(request, file_id)
                        if returned_id and file_id != returned_id:
                            raise DiscoveryError(
                                "OKX archive filename conflicts with returned instrument."
                            )
                        # Keep family-scoped object identity stable; validate concrete content explicitly.
                        concrete = (
                            request
                            if not returned_id
                            else request.model_copy(
                                update={
                                    "scope": ArchiveScope(kind="instrument", identifier=file_id)
                                }
                            )
                        )
                        start = milliseconds(item.get("dateTs") or item.get("dataTs"))
                        if (start + offset).date() != daily(filename).date():
                            raise DiscoveryError(
                                "OKX filename date conflicts with source timestamp."
                            )
                        observed = archive(
                            concrete,
                            filename,
                            text(item.get("url")),
                            start,
                            key="history/" + request.data_type + "/" + filename,
                        )
                        if not overlaps(observed, AcquisitionInterval(start=current, end=end)):
                            continue
                        if filename in seen_files:
                            if seen_files[filename] != start:
                                raise DiscoveryError(
                                    "OKX duplicate filename has conflicting intervals."
                                )
                            continue
                        seen_files[filename] = start
                        yield observed
            current = end

    @staticmethod
    def _validate_okx_id(request: ArchiveRequest, instrument: str) -> None:
        scope = request.scope.identifier
        if request.instrument_type == "spot":
            valid = instrument == scope
        elif request.instrument_type == "perpetual":
            valid = instrument == scope + "-SWAP"
        elif request.instrument_type == "future":
            valid = re.fullmatch(re.escape(scope) + r"-\d{6}", instrument) is not None
        else:
            valid = re.fullmatch(re.escape(scope) + r"-\d{6}-[0-9.]+-[CP]", instrument) is not None
        if not valid:
            raise DiscoveryError(
                "OKX instrument does not belong to the requested market and scope."
            )

    async def _gateio(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        market = (
            "spot"
            if request.instrument_type == "spot"
            else ("futures_usdt" if request.market_category == "linear" else "futures_btc")
        )
        snapshot = (
            request.data_type == "order_book_l2"
            and request.source_options is not None
            and request.source_options.feed == "snapshot"
        )
        dataset = (
            ("orderbooks_slice" if snapshot else "orderbooks")
            if request.data_type == "order_book_l2"
            else "deals"
            if request.instrument_type == "spot"
            else "trades"
        )
        current = interval.start.replace(minute=0, second=0, microsecond=0)
        if request.data_type == "trades":
            current = current.replace(day=1, hour=0)

        async def candidates() -> AsyncIterator[DiscoveredArchive]:
            nonlocal current
            while current < interval.end:
                if request.data_type == "order_book_l2":
                    end = current + timedelta(hours=1)
                    stamp = current.strftime("%Y%m%d%H")
                else:
                    end = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
                    stamp = current.strftime("%Y%m")
                suffix = ".gz" if snapshot else ".csv.gz"
                filename = request.scope.identifier + "-" + stamp + suffix
                path = f"{market}/{dataset}/{current:%Y%m}/{quote(filename, safe='')}"
                url = "https://download.gatedata.org/" + path
                yield archive(request, filename, url, current, end=end, key=path)
                current = end

        async def probe(candidate: DiscoveredArchive) -> DiscoveredArchive | bool:
            if self.on_activity is not None:
                self.on_activity(f"Checking possible archive: {candidate.filename}")
            if self.on_probe is not None:
                self.on_probe(ProbeEvent("checking", candidate.filename))
            try:
                response = await self.http.head(
                    text(candidate.download_url), allowed_statuses=frozenset({404})
                )
                content_type = response.headers.get("content-type", "").lower()
                if any(kind in content_type for kind in ("html", "json")):
                    raise DiscoveryError("Gate.io probe returned an unrecognized document.")
                if response.status_code == 200:
                    if self.on_activity is not None:
                        self.on_activity(f"Archive found: {candidate.filename}")
                    if self.on_probe is not None:
                        self.on_probe(ProbeEvent("found", candidate.filename))
                    return candidate
                if response.status_code != 404:
                    raise DiscoveryError("Unexpected Gate.io existence response.")
                if self.on_activity is not None:
                    self.on_activity(f"Candidate absent: {candidate.filename}")
                if self.on_probe is not None:
                    self.on_probe(ProbeEvent("absent", candidate.filename))
                return False
            except Exception:
                if self.on_probe is not None:
                    self.on_probe(ProbeEvent("failed", candidate.filename))
                if self.on_activity is not None:
                    self.on_activity(f"Probe failed: {candidate.filename}")
                raise

        async for result in bounded_map(candidates(), probe, workers=4):
            if isinstance(result, DiscoveredArchive):
                yield result
