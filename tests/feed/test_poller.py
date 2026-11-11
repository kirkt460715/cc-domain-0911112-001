import io
import json
import threading
from pathlib import Path

from registry_bridge.cli import main as cli_main
from registry_bridge.errors import Conflict, FeedUnavailable, InvalidMessage, LeaseLost
from registry_bridge.feeds import importer
from registry_bridge.feeds.poller import ChangePoller
from registry_bridge.ledger import issuers
from registry_bridge.service import RegistryService
from synthetic_exchange.server import make_server
from tests.support import LedgerCase, registration, transition


def page(*items, nxt=None):
    return 'page', {'items': list(items), 'next': nxt}


def no_sleep(_seconds):
    return None


class PollerCase(LedgerCase):
    def poller(self):
        return ChangePoller(self.store)

    def test_pages_limit_and_resume_from_persisted_cursor(self):
        feed = {
            'start': page(registration(), nxt='p2'),
            'p2': page(registration(2, asset='second'), nxt=None),
        }
        calls = []

        def fetch(url, cursor):
            calls.append(cursor)
            return feed[cursor]

        first = self.poller().poll('registry', url='http://feed/changes', pages=1, fetch=fetch, owner='alpha')
        self.assertEqual(first['status'], 'paused')
        self.assertEqual(first['pages'], 1)
        self.assertEqual(calls, ['start'])
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'p2')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 1)
        second = self.poller().poll('registry', pages=5, fetch=fetch, owner='alpha')
        self.assertEqual(second['status'], 'caught_up')
        self.assertEqual(second['from_cursor'], 'p2')
        self.assertEqual(calls, ['start', 'p2'])
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)

    def test_mixed_page_out_of_order_and_bad_line_is_replayable(self):
        feed = {'start': page(transition(2, 2, 'suspend'), {'bad': '合成损坏行'}, registration(), nxt=None)}
        result = self.poller().poll('registry', url='http://feed', pages=3, fetch=lambda u, c: feed[c], owner='alpha')
        self.assertEqual(result['status'], 'caught_up')
        self.assertEqual(result['accepted'], 2)
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)
        self.assertEqual(result['progress']['north']['last_sequence'], 2)
        self.assertEqual(result['gaps']['north'], [])
        dead = importer.list_dead_letters(self.store)
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]['source'], 'registry@page[start]')
        self.assertEqual(dead[0]['line_number'], 2)
        self.assertIn('合成损坏行', dead[0]['raw'])
        replay = importer.replay_dead_letter(self.store, dead[0]['dead_id'], registration(3, asset='third'))
        self.assertEqual(replay['status'], 'accepted')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 3)

    def test_failed_first_page_leaves_no_checkpoint(self):
        def fetch(url, cursor):
            raise FeedUnavailable('Feed is down')

        with self.assertRaises(FeedUnavailable):
            self.poller().poll('registry', url='http://feed', fetch=fetch, owner='alpha')
        self.assertIsNone(self.store.one('SELECT * FROM feed_checkpoints WHERE source=?', ('registry',)))

    def test_mid_run_failure_keeps_only_cleared_pages(self):
        def flaky(url, cursor):
            if cursor == 'p2':
                raise FeedUnavailable('Feed is down')
            return page(registration(), nxt='p2')

        with self.assertRaises(FeedUnavailable):
            self.poller().poll('registry', url='http://feed', pages=5, fetch=flaky, owner='alpha')
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'p2')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 1)
        recovered = self.poller().poll(
            'registry', pages=5, owner='alpha',
            fetch=lambda u, c: page(registration(2, asset='second'), nxt=None),
        )
        self.assertEqual(recovered['status'], 'caught_up')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)

    def test_rate_limit_retries_then_recovers(self):
        attempts = []

        def fetch(url, cursor):
            attempts.append(cursor)
            if len(attempts) <= 2:
                return 'rate_limited', 0
            return page(registration(), nxt=None)

        result = self.poller().poll('registry', url='http://feed', fetch=fetch, sleep=no_sleep, owner='alpha')
        self.assertEqual(result['status'], 'caught_up')
        self.assertEqual(len(attempts), 3)
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'start')

    def test_rate_limit_exhaustion_keeps_checkpoint_for_resume(self):
        result = self.poller().poll(
            'registry', url='http://feed', retries=2, sleep=no_sleep, owner='alpha',
            fetch=lambda u, c: ('rate_limited', 5),
        )
        self.assertEqual(result['status'], 'rate_limited')
        self.assertEqual(result['detail'], {'retry_after': 5})
        self.assertEqual(result['pages'], 0)
        self.assertIsNone(self.store.one('SELECT * FROM feed_checkpoints WHERE source=?', ('registry',)))
        recovered = self.poller().poll(
            'registry', url='http://feed', fetch=lambda u, c: page(registration(), nxt=None), owner='alpha',
        )
        self.assertEqual(recovered['status'], 'caught_up')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 1)

    def test_expired_cursor_replays_from_restart_without_clearing(self):
        self.poller().poll(
            'registry', url='http://feed', owner='alpha',
            fetch=lambda u, c: page(registration(), nxt='p2'),
        )
        self.store.execute("UPDATE feed_checkpoints SET cursor='retired' WHERE source='registry'")
        feed = {
            'retired': ('expired', 'start'),
            'start': page(registration(), nxt='p2'),
            'p2': page(registration(2, asset='second'), nxt=None),
        }
        result = self.poller().poll('registry', pages=5, fetch=lambda u, c: feed[c], owner='alpha')
        self.assertEqual(result['status'], 'caught_up')
        self.assertEqual(result['replays'], 1)
        self.assertEqual(result['duplicate'], 1)
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 2)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM assets'), 2)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'p2')

    def test_replay_loop_is_bounded(self):
        self.poller().poll('registry', url='http://feed', fetch=lambda u, c: page(nxt='p2'), owner='alpha')
        self.store.execute("UPDATE feed_checkpoints SET cursor='retired' WHERE source='registry'")
        result = self.poller().poll(
            'registry', pages=5, owner='alpha', fetch=lambda u, c: ('expired', 'again'),
        )
        self.assertEqual(result['status'], 'replay_exhausted')
        self.assertEqual(result['replays'], 4)

    def test_duplicate_cursor_stalls_without_spinning(self):
        calls = []

        def fetch(url, cursor):
            calls.append(cursor)
            return page(nxt=cursor)

        result = self.poller().poll('registry', url='http://feed', pages=10, fetch=fetch, owner='alpha')
        self.assertEqual(result['status'], 'stalled')
        self.assertEqual(result['detail'], {'cursor': 'start'})
        self.assertEqual(calls, ['start'])
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'start')

    def test_late_commit_from_previous_owner_cannot_overwrite_checkpoint(self):
        def takeover(url, cursor):
            self.clock.tick(31)
            self.poller().poll(
                'registry', url='http://feed', owner='beta', pages=1,
                fetch=lambda u, c: page(registration(), nxt='beta-cursor'),
            )
            return page(registration(9, asset='late'), nxt='alpha-cursor')

        with self.assertRaises(LeaseLost):
            self.poller().poll('registry', url='http://feed', owner='alpha', ttl=30, fetch=takeover)
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'beta-cursor')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 1)
        self.assertIsNone(self.store.one("SELECT * FROM events WHERE asset_id='late'"))

    def test_concurrent_source_is_refused_while_lease_held(self):
        outcome = []

        def fetch(url, cursor):
            try:
                self.poller().poll('registry', url='http://feed', owner='beta',
                                   fetch=lambda u, c: page(nxt=None))
            except Conflict as exc:
                outcome.append(exc)
            return page(nxt=None)

        self.poller().poll('registry', url='http://feed', owner='alpha', ttl=30, fetch=fetch)
        self.assertEqual(len(outcome), 1)
        self.assertEqual(outcome[0].code, 'conflict')
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'start')

    def test_issuer_filter_limits_whose_events_are_ingested(self):
        feed = {'start': page(
            registration(),
            registration(issuer='south', asset='south-asset', event='south:e1'),
            nxt=None,
        )}
        result = self.poller().poll(
            'registry', url='http://feed', issuers=['south'], fetch=lambda u, c: feed[c], owner='alpha',
        )
        self.assertEqual(result['status'], 'caught_up')
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(result['skipped'], 1)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 0)
        self.assertEqual(issuers.progress(self.store, 'south')['last_sequence'], 1)
        self.assertEqual(sorted(result['progress']), ['south'])

    def test_first_poll_requires_url_and_persisted_settings_win(self):
        with self.assertRaises(InvalidMessage):
            self.poller().poll('registry', fetch=lambda u, c: page(nxt=None), owner='alpha')
        self.poller().poll('registry', url='http://feed', fetch=lambda u, c: page(nxt=None), owner='alpha')
        with self.assertRaises(Conflict):
            self.poller().poll('registry', url='http://other', fetch=lambda u, c: page(nxt=None), owner='alpha')
        with self.assertRaises(Conflict):
            self.poller().poll('registry', cursor='elsewhere', fetch=lambda u, c: page(nxt=None), owner='alpha')
        resumed = self.poller().poll('registry', fetch=lambda u, c: page(nxt=None), owner='alpha')
        self.assertEqual(resumed['url'], 'http://feed')

    def test_service_operations_validate_before_polling(self):
        from registry_bridge.errors import NotFound
        service = RegistryService(self.store)
        with self.assertRaises(InvalidMessage):
            service.call('feed.poll', {'source': 'registry'})
        with self.assertRaises(NotFound):
            service.call('feed.checkpoint', {'source': 'registry'})
        self.assertEqual(service.call('feed.checkpoints'), [])


class SyntheticServerCase(LedgerCase):
    def setUp(self):
        super().setUp()
        scenario = json.loads((Path(__file__).parents[2] / 'synthetic_exchange' / 'scenario.json').read_text())
        self.server = make_server(scenario)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/changes'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)
        super().tearDown()

    def test_poll_against_synthetic_exchange(self):
        result = self.poller_run(pages=5)
        self.assertEqual(result['status'], 'caught_up')
        self.assertEqual(result['accepted'], 3)
        self.assertEqual(result['duplicate'], 1)
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)
        self.assertEqual(issuers.progress(self.store, 'south')['last_sequence'], 1)
        dead = importer.list_dead_letters(self.store)
        self.assertEqual(len(dead), 1)
        self.assertIn('p2', dead[0]['source'])
        self.assertIn('合成损坏行', dead[0]['raw'])
        replay = importer.replay_dead_letter(
            self.store, dead[0]['dead_id'], registration(2, issuer='south', asset='south-asset2', event='south:e2'),
        )
        self.assertEqual(replay['status'], 'accepted')

    def test_expired_and_loop_cursors_against_synthetic_exchange(self):
        self.poller_run(pages=5)
        self.store.execute("UPDATE feed_checkpoints SET cursor='expired' WHERE source='registry'")
        replayed = self.poller_run(pages=5)
        self.assertEqual(replayed['status'], 'caught_up')
        self.assertEqual(replayed['replays'], 1)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 3)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)
        self.store.execute("UPDATE feed_checkpoints SET cursor='loop' WHERE source='registry'")
        stalled = self.poller_run(pages=5)
        self.assertEqual(stalled['status'], 'stalled')
        self.assertEqual(self.poller().checkpoint('registry')['cursor'], 'loop')

    def test_service_and_cli_entries(self):
        service = RegistryService(self.store)
        result = service.call('feed.poll', {'source': 'registry', 'url': self.url, 'pages': 5, 'owner': 'svc'})
        self.assertEqual(result['status'], 'caught_up')
        checkpoint = service.call('feed.checkpoint', {'source': 'registry'})
        self.assertEqual(checkpoint['cursor'], 'p2')
        self.assertEqual(len(service.call('feed.checkpoints')), 1)

    def poller_run(self, **kwargs):
        return self.poller().poll('registry', url=self.url, sleep=no_sleep, owner='alpha', **kwargs)

    def poller(self):
        return ChangePoller(self.store)


class CliCase(LedgerCase):
    def test_cli_poll_and_checkpoints(self):
        scenario = json.loads((Path(__file__).parents[2] / 'synthetic_exchange' / 'scenario.json').read_text())
        server = make_server(scenario)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f'http://127.0.0.1:{server.server_port}/changes'
            db = str(self.path.parent / 'cli.db')
            for issuer in ('north', 'south'):
                out = io.StringIO()
                code = cli_main(['--db', db, 'call', 'issuer.add', '--json',
                                 json.dumps({'issuer': issuer, 'display_name': issuer})], stdout=out)
                self.assertEqual(code, 0, out.getvalue())
            out = io.StringIO()
            code = cli_main(
                ['--db', db, 'poll', '--source', 'registry', '--url', url, '--pages', '5'],
                stdout=out,
            )
            self.assertEqual(code, 0)
            summary = json.loads(out.getvalue())['result']
            self.assertEqual(summary['status'], 'caught_up')
            self.assertEqual(summary['accepted'], 3)
            out = io.StringIO()
            code = cli_main(['--db', db, 'checkpoints', '--source', 'registry'], stdout=out)
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(out.getvalue())['result']['cursor'], 'p2')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)
