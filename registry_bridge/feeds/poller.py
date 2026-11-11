"""分页变更接口采集：从持久化游标续采，按签发方维护连续 sequence。

游标只代表网络页位置；各签发方连续生效的 sequence 仍由账本接收与
drain 机制独立核对。每页在一个事务内核清（接收、拒收、冲突全部记录）
后才确认下一游标；坏行连同原页与原文写入 dead_letters 供重放。429 按
Retry-After 有界重试，410 按服务给定起点重放且不清空登记，同源并发
采集通过租约代际 fencing，旧进程的迟到提交无法覆盖新持有者的位置。
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

from registry_bridge.errors import (
    Conflict, FeedUnavailable, InvalidMessage, LeaseLost, NotFound, RegistryError,
)
from registry_bridge.feeds.importer import dead_letter
from registry_bridge.feeds.message import (
    IDENTIFIER, canonical, digest, identifier, integer, parse_message, text,
)
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import audit, issuers as issuer_ledger, leases
from registry_bridge.reconcile import resolution

DDL = '''
CREATE TABLE IF NOT EXISTS feed_checkpoints(
 source TEXT PRIMARY KEY, url TEXT NOT NULL, cursor TEXT NOT NULL,
 updated_at TEXT NOT NULL);
'''

DEFAULT_CURSOR = 'start'
MAX_PAGES = 1000
MAX_PAGE_BYTES = 16 * 1024 * 1024
MAX_REPLAYS = 3
STATUSES = {'caught_up', 'paused', 'rate_limited', 'stalled', 'replay_exhausted'}


def lease_name(source):
    candidate = 'feed:' + source
    if len(candidate) <= 128 and IDENTIFIER.fullmatch(candidate):
        return candidate
    return 'feed:' + digest(source)


def page_source(source, cursor):
    return f'{source}@page[{cursor}]'


def raw_text(item):
    try:
        return canonical(item)
    except RegistryError:
        return json.dumps(item, ensure_ascii=False)


def retry_after(value):
    if isinstance(value, str) and value.isdigit():
        return min(int(value), 60)
    return 1


def page_json(body):
    try:
        value = json.loads(body.decode('utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise FeedUnavailable('Feed page is not valid JSON') from exc
    if not isinstance(value, dict):
        raise FeedUnavailable('Feed page must be a JSON object')
    return value


def opaque_cursor(value, field):
    if not isinstance(value, str) or not value or len(value) > 256:
        raise FeedUnavailable('Feed returned an invalid cursor', context={'field': field})
    return value


def fetch_page(url, cursor, *, timeout=10):
    """GET one change page; returns ('page'|'rate_limited'|'expired', payload)."""
    separator = '&' if '?' in url else '?'
    target = url + separator + urllib.parse.urlencode({'cursor': cursor})
    request = urllib.request.Request(target, headers={'Accept': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, headers = response.status, response.headers
            body = response.read(MAX_PAGE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        status, headers = exc.code, exc.headers
        body = exc.read(MAX_PAGE_BYTES + 1)
    except (urllib.error.URLError, OSError) as exc:
        raise FeedUnavailable('Change feed is unreachable', context={'reason': str(exc)}) from exc
    if len(body) > MAX_PAGE_BYTES:
        raise FeedUnavailable('Feed page exceeds size limit')
    if status == 429:
        return 'rate_limited', retry_after(headers.get('Retry-After'))
    if status == 410:
        return 'expired', opaque_cursor(page_json(body).get('restart'), 'restart')
    if status != 200:
        raise FeedUnavailable('Change feed returned an unexpected status', context={'status': status})
    payload = page_json(body)
    items = payload.get('items')
    if not isinstance(items, list) or len(items) > 10000:
        raise FeedUnavailable('Feed page items must be a bounded list')
    following = payload.get('next')
    if following is not None:
        opaque_cursor(following, 'next')
    return 'page', {'items': items, 'next': following}


class ChangePoller:
    def __init__(self, store):
        self.store = store
        if store._depth:
            raise InvalidMessage('Change poller must be initialized outside a transaction')
        store.connection.executescript(DDL)

    def checkpoint(self, source):
        source = text(source, 'source', 128)
        row = self.store.one('SELECT * FROM feed_checkpoints WHERE source=?', (source,))
        if row is None:
            raise NotFound('Feed checkpoint does not exist', context={'source': source})
        return row

    def checkpoints(self):
        return self.store.all('SELECT * FROM feed_checkpoints ORDER BY source')

    def poll(self, source, *, url=None, cursor=None, pages=1, issuers=None,
             owner='poller', ttl=30, fetch=None, sleep=time.sleep, retries=3):
        source = text(source, 'source', 128)
        integer(pages, 'pages', 1, MAX_PAGES)
        integer(ttl, 'ttl', 1, 86400)
        integer(retries, 'retries', 0, 10)
        identifier(owner, 'owner')
        allow = None
        if issuers is not None:
            if not isinstance(issuers, list) or not 1 <= len(issuers) <= 100:
                raise InvalidMessage('issuers must be a nonempty bounded list')
            allow = {identifier(item, 'issuer') for item in issuers}
        if url is not None:
            url = text(url, 'url', 1024)
        if cursor is not None:
            cursor = text(cursor, 'cursor', 256)
        fetch = fetch or fetch_page
        row = self.store.one('SELECT * FROM feed_checkpoints WHERE source=?', (source,))
        if row is None:
            if url is None:
                raise InvalidMessage('First poll for a source requires the feed URL')
            position = cursor or DEFAULT_CURSOR
        else:
            if url is not None and url != row['url']:
                raise Conflict('Persisted feed URL differs', context={'source': source})
            if cursor is not None and cursor != row['cursor']:
                raise Conflict('Persisted feed cursor differs; resume from the checkpoint', context={
                    'source': source, 'persisted': row['cursor'],
                })
            url = row['url']
            position = row['cursor']
        token = leases.acquire(self.store, lease_name(source), owner, ttl)
        origin = position
        totals = {'accepted': 0, 'duplicate': 0, 'conflicts': 0, 'rejected': 0, 'skipped': 0}
        dead = []
        touched = set()
        fetched = 0
        replays = 0
        status = 'paused'
        detail = None
        try:
            while fetched < pages:
                kind, value = self._fetch(fetch, url, position, retries, sleep)
                if kind == 'rate_limited':
                    status = 'rate_limited'
                    detail = {'retry_after': value}
                    break
                if kind == 'expired':
                    replays += 1
                    if replays > MAX_REPLAYS:
                        status = 'replay_exhausted'
                        detail = {'restart': value}
                        break
                    with self.store.transaction():
                        leases.assert_held(self.store, token)
                        self._confirm(source, url, value)
                        audit.append(self.store, 'feed.cursor_reset', owner, {
                            'source': source, 'from': position, 'restart': value,
                        })
                    position = value
                    continue
                items, following = value['items'], value['next']
                stalled = following is not None and following == position
                with self.store.transaction():
                    leases.assert_held(self.store, token)
                    counts, dead_ids, seen = self._apply(source, position, items, allow)
                    for key in totals:
                        totals[key] += counts[key]
                    self._confirm(source, url, following if following is not None else position)
                    leases.renew(self.store, token, ttl)
                    audit.append(self.store, 'feed.page_committed', owner, {
                        'source': source, 'cursor': position, 'next': following, 'counts': counts,
                    })
                fetched += 1
                dead.extend(dead_ids)
                touched |= seen
                if following is None:
                    status = 'caught_up'
                    break
                if stalled:
                    status = 'stalled'
                    detail = {'cursor': position}
                    break
                position = following
        finally:
            try:
                leases.release(self.store, token)
            except LeaseLost:
                pass
        persisted = self.store.one('SELECT * FROM feed_checkpoints WHERE source=?', (source,))
        summary = {
            'source': source,
            'url': url,
            'status': status,
            'owner': owner,
            'from_cursor': origin,
            'cursor': persisted['cursor'] if persisted else position,
            'pages': fetched,
            'replays': replays,
            **totals,
            'dead_letters': dead,
            'progress': {},
            'gaps': {},
        }
        if detail is not None:
            summary['detail'] = detail
        for name in sorted(touched):
            if issuer_ledger.get(self.store, name, required=False):
                summary['progress'][name] = issuer_ledger.progress(self.store, name)
                summary['gaps'][name] = resolution.gaps(self.store, name)['gaps']
        return summary

    @staticmethod
    def _fetch(fetch, url, position, retries, sleep):
        attempts = 0
        while True:
            kind, value = fetch(url, position)
            if kind != 'rate_limited':
                return kind, value
            attempts += 1
            if attempts > retries:
                return 'rate_limited', value
            sleep(min(value, 60))

    def _confirm(self, source, url, cursor):
        self.store.execute(
            'INSERT INTO feed_checkpoints(source,url,cursor,updated_at) VALUES(?,?,?,?) '
            'ON CONFLICT(source) DO UPDATE SET url=excluded.url,cursor=excluded.cursor,'
            'updated_at=excluded.updated_at',
            (source, url, cursor, self.store.clock()),
        )

    def _apply(self, source, cursor, items, allow):
        counts = {'accepted': 0, 'duplicate': 0, 'conflicts': 0, 'rejected': 0, 'skipped': 0}
        dead = []
        touched = set()
        label = page_source(source, cursor)
        for index, item in enumerate(items, 1):
            if allow is not None:
                issuer = item.get('issuer') if isinstance(item, dict) else None
                if issuer not in allow:
                    counts['skipped'] += 1
                    continue
            try:
                message = parse_message(item)
            except RegistryError as exc:
                dead.append(dead_letter(self.store, label, index, raw_text(item), exc))
                counts['rejected'] += 1
                continue
            touched.add(message.issuer)
            try:
                result = receive(self.store, message)
            except RegistryError as exc:
                dead.append(dead_letter(self.store, label, index, raw_text(item), exc))
                counts['rejected'] += 1
                continue
            if result['status'] == 'conflict':
                counts['conflicts'] += 1
            else:
                counts[result['status']] += 1
        return counts, dead, touched
