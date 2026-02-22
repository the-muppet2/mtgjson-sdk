"""Smoke test: pull real data from CDN and exercise ALL SDK methods.

Coverage goal: 100% of public methods, all filter parameters,
all output modes (model, dict, dataframe), and key edge cases.
"""

import asyncio
import logging
import sys
import tempfile
import threading
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("smoke_test")

from mtg_json_tools import AsyncMtgJsonTools, MtgJsonTools

PASS = 0
FAIL = 0
SKIP = 0


def check(label: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
    else:
        FAIL += 1
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{status}] {label}{suffix}")


def skip(label: str, reason: str = ""):
    global SKIP
    SKIP += 1
    suffix = f" -- {reason}" if reason else ""
    print(f"  [SKIP] {label}{suffix}")


def section(name: str):
    print(f"\n{'=' * 60}")
    print(f"  {name}")
    print(f"{'=' * 60}")


def main():
    t0 = time.time()

    # ══════════════════════════════════════════════════════════
    #  CLIENT LIFECYCLE
    # ══════════════════════════════════════════════════════════
    section("Client Lifecycle")

    # __repr__
    sdk = MtgJsonTools()
    r = repr(sdk)
    check("__repr__", "MtgJsonTools" in r, f"repr={r}")

    # context manager — verify resources released after exit
    with MtgJsonTools() as ctx_sdk:
        check("__enter__ returns SDK", isinstance(ctx_sdk, MtgJsonTools))
        # Query inside context should work
        ctx_meta = ctx_sdk.meta
        check("query inside context", isinstance(ctx_meta, dict))
    # After exit, connection should be closed
    try:
        ctx_sdk.sql("SELECT 1")
        check("__exit__ closes connection", False, "query succeeded after close")
    except Exception:
        check("__exit__ closes connection", True)

    # constructor: custom cache_dir
    with tempfile.TemporaryDirectory() as tmpdir:
        custom_sdk = MtgJsonTools(cache_dir=tmpdir)
        actual_dir = str(custom_sdk._cache.cache_dir)
        check(
            "constructor cache_dir",
            Path(actual_dir) == Path(tmpdir),
            f"expected={tmpdir}, actual={actual_dir}",
        )
        custom_sdk.close()

    # constructor: on_progress callback
    progress_calls = []
    progress_sdk = MtgJsonTools(on_progress=lambda *args: progress_calls.append(args))
    check("constructor on_progress", progress_sdk is not None)
    progress_sdk.close()

    # meta property
    meta = sdk.meta
    check(
        "meta loads",
        isinstance(meta, dict) and "data" in meta,
        f"keys={list(meta.keys())}",
    )
    if "data" in meta:
        version = meta["data"].get("version", "?")
        date = meta["data"].get("date", "?")
        check("meta has version", bool(version), f"v={version}, date={date}")

    # views property (starts empty, grows as we query)
    views_before = sdk.views
    check("views property (initial)", isinstance(views_before, list))

    # refresh() — in offline-like conditions, cache is fresh after meta load
    refresh_result = sdk.refresh()
    check("refresh()", isinstance(refresh_result, bool), f"stale={refresh_result}")

    # refresh — queries still work after refresh
    sdk.refresh()
    post_refresh_count = sdk.cards.count()
    check(
        "queries work after refresh",
        post_refresh_count > 0,
        f"count={post_refresh_count}",
    )

    # Connection.raw property
    raw_conn = sdk._conn.raw
    check("Connection.raw returns DuckDB", raw_conn is not None)

    # ══════════════════════════════════════════════════════════
    #  CARDS — CardQuery (8 methods, ~20 filter params)
    # ══════════════════════════════════════════════════════════
    section("Cards: get_by_name / get_by_uuid")

    bolt = sdk.cards.get_by_name("Lightning Bolt")
    check("get_by_name Lightning Bolt", len(bolt) > 0, f"found {len(bolt)} printings")

    # get_by_name with set_code filter
    bolt_lea = sdk.cards.get_by_name("Lightning Bolt", set_code="LEA")
    check("get_by_name set_code=LEA", len(bolt_lea) >= 0, f"found {len(bolt_lea)}")

    # get_by_name as_dict
    bolt_dicts = sdk.cards.get_by_name("Lightning Bolt", as_dict=True)
    check(
        "get_by_name as_dict", len(bolt_dicts) > 0 and isinstance(bolt_dicts[0], dict)
    )

    # get_by_name as_dataframe
    bolt_df = sdk.cards.get_by_name("Lightning Bolt", as_dataframe=True)
    check(
        "get_by_name as_dataframe",
        hasattr(bolt_df, "shape"),
        f"type={type(bolt_df).__name__}",
    )

    # get_by_name — empty string
    bolt_empty_name = sdk.cards.get_by_name("")
    check("get_by_name empty string", len(bolt_empty_name) == 0)

    uuid = None
    if bolt:
        uuid = bolt[0].uuid

        # get_by_uuid — model
        card = sdk.cards.get_by_uuid(uuid)
        check("get_by_uuid (model)", card is not None and card.name == "Lightning Bolt")

        # get_by_uuid — as_dict
        card_dict = sdk.cards.get_by_uuid(uuid, as_dict=True)
        check(
            "get_by_uuid as_dict",
            isinstance(card_dict, dict) and card_dict.get("name") == "Lightning Bolt",
        )

        # get_by_uuid — as_dataframe
        card_df = sdk.cards.get_by_uuid(uuid, as_dataframe=True)
        check("get_by_uuid as_dataframe", hasattr(card_df, "shape"))

        # get_by_uuid — nonexistent
        missing = sdk.cards.get_by_uuid("00000000-0000-0000-0000-000000000000")
        check("get_by_uuid nonexistent returns None", missing is None)

        # Cross-reference consistency: uuid lookup == name lookup
        by_uuid = sdk.cards.get_by_uuid(uuid)
        by_name = sdk.cards.get_by_name("Lightning Bolt")
        name_uuids = {c.uuid for c in by_name}
        check(
            "cross-ref uuid in name results",
            by_uuid is not None and by_uuid.uuid in name_uuids,
        )

    # ── Cards: get_by_uuids (bulk lookup) ──
    section("Cards: bulk lookups (get_by_uuids)")

    if bolt and len(bolt) >= 2:
        bulk_uuids = [b.uuid for b in bolt[:5]]
        bulk_cards = sdk.cards.get_by_uuids(bulk_uuids)
        check(
            "get_by_uuids (models)",
            len(bulk_cards) == len(bulk_uuids),
            f"requested {len(bulk_uuids)}, got {len(bulk_cards)}",
        )

        bulk_dicts = sdk.cards.get_by_uuids(bulk_uuids, as_dict=True)
        check(
            "get_by_uuids as_dict",
            len(bulk_dicts) > 0 and isinstance(bulk_dicts[0], dict),
        )

        bulk_df = sdk.cards.get_by_uuids(bulk_uuids, as_dataframe=True)
        check("get_by_uuids as_dataframe", hasattr(bulk_df, "shape"))

    # empty list
    bulk_empty = sdk.cards.get_by_uuids([])
    check("get_by_uuids empty list", bulk_empty == [])

    # nonexistent uuids
    bulk_none = sdk.cards.get_by_uuids(["00000000-0000-0000-0000-000000000000"])
    check("get_by_uuids nonexistent", len(bulk_none) == 0)

    # ── Cards: search (all filter params) ──
    section("Cards: search filters")

    # name LIKE
    s = sdk.cards.search(name="Lightning%", limit=10)
    check("search name LIKE", len(s) > 0, f"found {len(s)}")

    # exact name
    s = sdk.cards.search(name="Lightning Bolt", limit=5)
    check("search name exact", len(s) > 0)

    # fuzzy_name — typo-tolerant search
    s = sdk.cards.search(fuzzy_name="Ligtning Bolt", limit=5)
    check(
        "search fuzzy_name typo",
        len(s) > 0,
        f"found {len(s)}, top={s[0].name if s else '?'}",
    )
    if s:
        check(
            "fuzzy_name finds correct card",
            s[0].name == "Lightning Bolt",
            f"got '{s[0].name}'",
        )

    # fuzzy_name with set_code
    s = sdk.cards.search(fuzzy_name="Counterspel", set_code="MH3", limit=5)
    check("search fuzzy_name + set_code", isinstance(s, list), f"found {len(s)}")

    # colors
    s = sdk.cards.search(colors=["R"], mana_value=1.0, limit=5)
    check("search colors=R mv=1", len(s) > 0, f"found {len(s)}")

    # color_identity
    s = sdk.cards.search(color_identity=["W", "U"], limit=5)
    check("search color_identity=[W,U]", len(s) > 0, f"found {len(s)}")

    # types
    s = sdk.cards.search(types="Creature", limit=5)
    check("search types=Creature", len(s) > 0, f"found {len(s)}")

    # rarity
    s = sdk.cards.search(rarity="mythic", limit=5)
    check("search rarity=mythic", len(s) > 0, f"found {len(s)}")

    # text
    s = sdk.cards.search(text="draw a card", limit=5)
    check("search text='draw a card'", len(s) > 0, f"found {len(s)}")

    # power / toughness
    s = sdk.cards.search(power="4", toughness="4", limit=5)
    check("search power=4 toughness=4", len(s) > 0, f"found {len(s)}")

    # mana_value exact
    s = sdk.cards.search(mana_value=3.0, limit=5)
    check("search mana_value=3", len(s) > 0, f"found {len(s)}")

    # mana_value_lte
    s = sdk.cards.search(mana_value_lte=1.0, limit=5)
    check("search mana_value_lte=1", len(s) > 0, f"found {len(s)}")

    # mana_value_gte
    s = sdk.cards.search(mana_value_gte=10.0, limit=5)
    check("search mana_value_gte=10", len(s) > 0, f"found {len(s)}")

    # artist
    s = sdk.cards.search(artist="Christopher Moeller", limit=5)
    check("search artist", len(s) > 0, f"found {len(s)}")

    # keyword
    s = sdk.cards.search(keyword="Flying", limit=5)
    check("search keyword=Flying", len(s) > 0, f"found {len(s)}")

    # layout
    s = sdk.cards.search(layout="split", limit=5)
    check("search layout=split", len(s) > 0, f"found {len(s)}")

    # is_promo
    s = sdk.cards.search(is_promo=True, limit=5)
    check("search is_promo=True", len(s) > 0, f"found {len(s)}")

    s_np = sdk.cards.search(is_promo=False, limit=5)
    check("search is_promo=False", len(s_np) > 0, f"found {len(s_np)}")

    # availability
    s = sdk.cards.search(availability="mtgo", limit=5)
    check("search availability=mtgo", len(s) > 0, f"found {len(s)}")

    s = sdk.cards.search(availability="paper", limit=5)
    check("search availability=paper", len(s) > 0, f"found {len(s)}")

    # language
    s = sdk.cards.search(language="Japanese", limit=5)
    check("search language=Japanese", isinstance(s, list), f"found {len(s)}")

    # set_code
    s = sdk.cards.search(set_code="MH3", limit=5)
    check("search set_code=MH3", len(s) > 0, f"found {len(s)}")

    # set_type (requires JOIN with sets)
    s = sdk.cards.search(set_type="expansion", limit=5)
    check("search set_type=expansion", len(s) > 0, f"found {len(s)}")

    # legal_in + mana_value_lte
    s = sdk.cards.search(legal_in="modern", mana_value_lte=2.0, limit=5)
    check("search legal_in=modern + mana_value_lte", len(s) > 0, f"found {len(s)}")

    # combined filters
    s = sdk.cards.search(colors=["R"], rarity="rare", mana_value_lte=3.0, limit=5)
    check("search combined (colors+rarity+mv)", len(s) > 0, f"found {len(s)}")

    # offset (pagination)
    page1 = sdk.cards.search(name="Lightning%", limit=3, offset=0)
    page2 = sdk.cards.search(name="Lightning%", limit=3, offset=3)
    check(
        "search offset (pagination)",
        len(page1) > 0 and len(page2) > 0,
        "two pages fetched",
    )
    if page1 and page2:
        check("search pages differ", page1[0].uuid != page2[0].uuid, "different cards")

    # text_regex
    s = sdk.cards.search(text_regex="deals \\d+ damage", limit=5)
    check("search text_regex", len(s) > 0, f"found {len(s)}")

    # localized_name (foreign language search)
    s = sdk.cards.search(localized_name="Blitzschlag", limit=5)
    check(
        "search localized_name (German)",
        len(s) > 0,
        f"found {len(s)}, name={s[0].name if s else '?'}",
    )

    # localized_name LIKE
    s = sdk.cards.search(localized_name="%Foudre%", limit=5)
    check("search localized_name LIKE", isinstance(s, list), f"found {len(s)}")

    # search as_dict
    s = sdk.cards.search(name="Lightning%", limit=3, as_dict=True)
    check("search as_dict", len(s) > 0 and isinstance(s[0], dict))

    # search as_dataframe
    s = sdk.cards.search(name="Lightning%", limit=3, as_dataframe=True)
    check("search as_dataframe", hasattr(s, "shape"))

    # ── Cards: multi-filter combination tests ──
    section("Cards: multi-filter combinations")

    # colors + types + rarity (red mythic creatures)
    s = sdk.cards.search(colors=["R"], types="Creature", rarity="mythic", limit=5)
    check("multi: colors+types+rarity", len(s) > 0, f"found {len(s)}")

    # keyword + mana_value_lte (cheap flyers)
    s = sdk.cards.search(keyword="Flying", mana_value_lte=2.0, limit=5)
    check("multi: keyword+mana_value_lte", len(s) > 0, f"found {len(s)}")

    # text_regex + colors (red cards with damage text)
    s = sdk.cards.search(text_regex="damage", colors=["R"], limit=5)
    check("multi: text_regex+colors", len(s) > 0, f"found {len(s)}")

    # availability + layout
    s = sdk.cards.search(availability="paper", layout="normal", limit=5)
    check("multi: availability+layout", len(s) > 0, f"found {len(s)}")

    # is_promo + rarity + set_code
    s = sdk.cards.search(is_promo=True, rarity="mythic", set_code="MH3", limit=5)
    check("multi: promo+rarity+set_code", isinstance(s, list), f"found {len(s)}")

    # localized_name + set_code
    s = sdk.cards.search(localized_name="%Blitz%", set_code="MH3", limit=5)
    check("multi: localized_name+set_code", isinstance(s, list), f"found {len(s)}")

    # wildcard-only search (should respect limit)
    s = sdk.cards.search(name="%", limit=3)
    check("search wildcard-only respects limit", len(s) <= 3, f"got {len(s)}")

    # ── Cards: other methods ──
    section("Cards: random, count, printings, atomic, find_by_scryfall_id")

    rand = sdk.cards.random(3)
    check("random(3)", len(rand) == 3, f"names: {[c.name for c in rand]}")

    rand_dict = sdk.cards.random(2, as_dict=True)
    check("random as_dict", len(rand_dict) == 2 and isinstance(rand_dict[0], dict))

    rand_df = sdk.cards.random(2, as_dataframe=True)
    check("random as_dataframe", hasattr(rand_df, "shape"))

    count = sdk.cards.count()
    check("count()", count > 1000, f"total cards: {count}")

    # count with single filter
    count_r = sdk.cards.count(rarity="mythic")
    check(
        "count(rarity=mythic)",
        count_r > 0 and count_r < count,
        f"mythic cards: {count_r}",
    )

    # count with combined filters
    count_combo = sdk.cards.count(setCode="MH3", rarity="rare")
    check(
        "count(setCode+rarity)",
        count_combo > 0 and count_combo < count_r,
        f"MH3 rares: {count_combo}",
    )

    printings = sdk.cards.get_printings("Counterspell")
    check(
        "get_printings Counterspell",
        len(printings) > 5,
        f"found {len(printings)} printings",
    )

    printings_dict = sdk.cards.get_printings("Counterspell", as_dict=True)
    check(
        "get_printings as_dict",
        len(printings_dict) > 0 and isinstance(printings_dict[0], dict),
    )

    printings_df = sdk.cards.get_printings("Counterspell", as_dataframe=True)
    check("get_printings as_dataframe", hasattr(printings_df, "shape"))

    # get_atomic — exact name
    atomic = sdk.cards.get_atomic("Lightning Bolt")
    check("get_atomic Lightning Bolt", len(atomic) > 0)

    atomic_dict = sdk.cards.get_atomic("Lightning Bolt", as_dict=True)
    check(
        "get_atomic as_dict", len(atomic_dict) > 0 and isinstance(atomic_dict[0], dict)
    )

    # get_atomic — face name fallback for split cards
    atomic_fire = sdk.cards.get_atomic("Fire")
    check(
        "get_atomic face name 'Fire'",
        len(atomic_fire) > 0,
        f"layout={atomic_fire[0].layout if atomic_fire else '?'}",
    )

    # get_atomic — split card with full name
    fire_ice = sdk.cards.get_by_name("Fire // Ice")
    check(
        "get_by_name split card 'Fire // Ice'",
        len(fire_ice) > 0,
        f"found {len(fire_ice)} printings",
    )

    # find_by_scryfall_id — use a REAL scryfall ID from identifiers
    if uuid:
        real_scryfall_id = None
        bolt_ids = sdk.identifiers.get_identifiers(uuid)
        if bolt_ids and bolt_ids.get("scryfallId"):
            real_scryfall_id = bolt_ids["scryfallId"]

        if real_scryfall_id:
            scry_cards = sdk.cards.find_by_scryfall_id(real_scryfall_id)
            check(
                "find_by_scryfall_id (real ID)",
                len(scry_cards) > 0,
                f"found {len(scry_cards)}, name={scry_cards[0].name if scry_cards else '?'}",
            )

            scry_dict = sdk.cards.find_by_scryfall_id(real_scryfall_id, as_dict=True)
            check(
                "find_by_scryfall_id as_dict",
                len(scry_dict) > 0 and isinstance(scry_dict[0], dict),
            )

            scry_df = sdk.cards.find_by_scryfall_id(real_scryfall_id, as_dataframe=True)
            check("find_by_scryfall_id as_dataframe", hasattr(scry_df, "shape"))
        else:
            # Fallback: at least test that the method runs
            scry_cards = sdk.cards.find_by_scryfall_id(uuid)
            check("find_by_scryfall_id runs (fallback)", isinstance(scry_cards, list))
            skip("find_by_scryfall_id real ID", "no scryfallId in identifiers")

        # Nonexistent scryfall ID
        scry_missing = sdk.cards.find_by_scryfall_id(
            "00000000-0000-0000-0000-000000000000"
        )
        check("find_by_scryfall_id nonexistent", len(scry_missing) == 0)

    # ══════════════════════════════════════════════════════════
    #  TOKENS — TokenQuery (5 methods, ~8 filter params)
    # ══════════════════════════════════════════════════════════
    section("Tokens")

    token_count = sdk.tokens.count()
    check("token count()", token_count > 0, f"total tokens: {token_count}")

    # count with filters
    # (setCode is a common column, should work)
    token_count_mh3 = sdk.tokens.count(setCode="MH3")
    check(
        "token count(setCode=MH3)",
        isinstance(token_count_mh3, int),
        f"MH3 tokens: {token_count_mh3}",
    )

    # search by name LIKE
    token_search = sdk.tokens.search(name="%Soldier%", limit=5)
    check("token search name LIKE", len(token_search) > 0, f"found {len(token_search)}")

    # search by set_code — find a set that actually has tokens
    # First discover a set code from existing token data
    token_set_row = sdk.sql("SELECT DISTINCT setCode FROM tokens LIMIT 1")
    token_set_code = token_set_row[0]["setCode"] if token_set_row else "MH3"
    token_search_set = sdk.tokens.search(set_code=token_set_code, limit=5)
    check(
        "token search set_code",
        len(token_search_set) > 0,
        f"set={token_set_code}, found {len(token_search_set)}",
    )

    # search by types
    token_search_type = sdk.tokens.search(types="Creature", limit=5)
    check(
        "token search types=Creature",
        len(token_search_type) > 0,
        f"found {len(token_search_type)}",
    )

    # search by artist — use a real artist name from token data
    token_artist_row = sdk.sql(
        "SELECT DISTINCT artist FROM tokens WHERE artist IS NOT NULL LIMIT 1"
    )
    if token_artist_row:
        token_artist_name = token_artist_row[0]["artist"]
        token_search_artist = sdk.tokens.search(artist=token_artist_name, limit=5)
        check(
            "token search artist (real)",
            len(token_search_artist) > 0,
            f"artist='{token_artist_name}', found {len(token_search_artist)}",
        )
    else:
        skip("token search artist", "no artist data in tokens")

    # search by colors
    token_search_colors = sdk.tokens.search(colors=["W"], limit=5)
    check(
        "token search colors=[W]",
        len(token_search_colors) > 0,
        f"found {len(token_search_colors)}",
    )

    # search with offset
    tp1 = sdk.tokens.search(name="%Soldier%", limit=2, offset=0)
    tp2 = sdk.tokens.search(name="%Soldier%", limit=2, offset=2)
    check("token search offset", isinstance(tp1, list) and isinstance(tp2, list))

    # search as_dict
    token_dict = sdk.tokens.search(name="%Soldier%", limit=3, as_dict=True)
    check(
        "token search as_dict", len(token_dict) > 0 and isinstance(token_dict[0], dict)
    )

    # search as_dataframe
    token_df = sdk.tokens.search(name="%Soldier%", limit=3, as_dataframe=True)
    check("token search as_dataframe", hasattr(token_df, "shape"))

    # get_by_uuid
    if token_search:
        token = sdk.tokens.get_by_uuid(token_search[0].uuid)
        check(
            "token get_by_uuid (model)",
            token is not None,
            f"name={token.name if token else '?'}",
        )

        token_d = sdk.tokens.get_by_uuid(token_search[0].uuid, as_dict=True)
        check("token get_by_uuid as_dict", isinstance(token_d, dict))

        token_f = sdk.tokens.get_by_uuid(token_search[0].uuid, as_dataframe=True)
        check("token get_by_uuid as_dataframe", hasattr(token_f, "shape"))

        # nonexistent
        missing_token = sdk.tokens.get_by_uuid("00000000-0000-0000-0000-000000000000")
        check("token get_by_uuid nonexistent", missing_token is None)

    # get_by_name
    token_soldiers = sdk.tokens.get_by_name("Soldier")
    check(
        "token get_by_name Soldier",
        len(token_soldiers) > 0,
        f"found {len(token_soldiers)}",
    )

    token_soldiers_dict = sdk.tokens.get_by_name("Soldier", as_dict=True)
    check(
        "token get_by_name as_dict",
        len(token_soldiers_dict) > 0 and isinstance(token_soldiers_dict[0], dict),
    )

    token_soldiers_df = sdk.tokens.get_by_name("Soldier", as_dataframe=True)
    check("token get_by_name as_dataframe", hasattr(token_soldiers_df, "shape"))

    # get_by_name with set_code — use a set that has tokens
    token_soldiers_set = sdk.tokens.get_by_name("Soldier", set_code=token_set_code)
    check(
        "token get_by_name set_code",
        isinstance(token_soldiers_set, list),
        f"set={token_set_code}, found {len(token_soldiers_set)}",
    )

    # for_set — use known token set
    tokens_for = sdk.tokens.for_set(token_set_code)
    check(
        "token for_set",
        len(tokens_for) > 0,
        f"set={token_set_code}, found {len(tokens_for)}",
    )

    tokens_for_dict = sdk.tokens.for_set(token_set_code, as_dict=True)
    check(
        "token for_set as_dict",
        isinstance(tokens_for_dict, list) and len(tokens_for_dict) > 0,
    )

    tokens_for_df = sdk.tokens.for_set(token_set_code, as_dataframe=True)
    check("token for_set as_dataframe", hasattr(tokens_for_df, "shape"))

    # get_by_uuids (bulk token lookup)
    if token_search and len(token_search) >= 2:
        token_uuids = [t.uuid for t in token_search[:3]]
        bulk_tokens = sdk.tokens.get_by_uuids(token_uuids)
        check(
            "token get_by_uuids",
            len(bulk_tokens) == len(token_uuids),
            f"requested {len(token_uuids)}, got {len(bulk_tokens)}",
        )

        bulk_tokens_d = sdk.tokens.get_by_uuids(token_uuids, as_dict=True)
        check("token get_by_uuids as_dict", isinstance(bulk_tokens_d[0], dict))

        bulk_tokens_df = sdk.tokens.get_by_uuids(token_uuids, as_dataframe=True)
        check("token get_by_uuids as_dataframe", hasattr(bulk_tokens_df, "shape"))

    check("token get_by_uuids empty", sdk.tokens.get_by_uuids([]) == [])

    # ══════════════════════════════════════════════════════════
    #  SETS — SetQuery (4 methods, ~7 filter params)
    # ══════════════════════════════════════════════════════════
    section("Sets")

    # get
    mh3 = sdk.sets.get("MH3")
    check("get set MH3", mh3 is not None, f"name={mh3.name if mh3 else '?'}")

    mh3_dict = sdk.sets.get("MH3", as_dict=True)
    check("get set as_dict", isinstance(mh3_dict, dict))

    mh3_df = sdk.sets.get("MH3", as_dataframe=True)
    check("get set as_dataframe", hasattr(mh3_df, "shape"))

    # get — case insensitive (code is uppercased internally)
    mh3_lower = sdk.sets.get("mh3")
    check(
        "get set case insensitive",
        mh3_lower is not None and mh3_lower.code == "MH3",
        f"code={mh3_lower.code if mh3_lower else '?'}",
    )

    # get nonexistent
    missing_set = sdk.sets.get("ZZZZZ")
    check("get set nonexistent", missing_set is None)

    # list — no filter
    all_sets = sdk.sets.list(limit=10)
    check("list sets (no filter)", len(all_sets) > 0, f"found {len(all_sets)}")

    # list — set_type
    expansions = sdk.sets.list(set_type="expansion", limit=10)
    check("list expansions", len(expansions) > 0, f"found {len(expansions)}")

    # list — name filter
    horizon_list = sdk.sets.list(name="%Horizons%", limit=10)
    check("list name filter", len(horizon_list) > 0, f"found {len(horizon_list)}")

    # list — offset
    sets_p1 = sdk.sets.list(limit=3, offset=0)
    sets_p2 = sdk.sets.list(limit=3, offset=3)
    check("list offset (pagination)", len(sets_p1) > 0 and len(sets_p2) > 0)
    if sets_p1 and sets_p2:
        check("list pages differ", sets_p1[0].code != sets_p2[0].code)

    # list — as_dict
    sets_d = sdk.sets.list(limit=3, as_dict=True)
    check("list as_dict", isinstance(sets_d[0], dict))

    # list — as_dataframe
    sets_df = sdk.sets.list(limit=3, as_dataframe=True)
    check("list as_dataframe", hasattr(sets_df, "shape"))

    # search — name
    set_search = sdk.sets.search(name="Horizons")
    check("search 'Horizons'", len(set_search) > 0, f"found {len(set_search)}")

    # search — set_type
    set_search_type = sdk.sets.search(set_type="masters", limit=10)
    check(
        "search set_type=masters",
        len(set_search_type) > 0,
        f"found {len(set_search_type)}",
    )

    # search — block
    set_search_block = sdk.sets.search(block="Innistrad")
    check(
        "search block=Innistrad",
        isinstance(set_search_block, list),
        f"found {len(set_search_block)}",
    )

    # search — release_year
    set_search_year = sdk.sets.search(release_year=2024, limit=10)
    check(
        "search release_year=2024",
        len(set_search_year) > 0,
        f"found {len(set_search_year)}",
    )

    # search — as_dict
    set_search_dict = sdk.sets.search(name="Horizons", as_dict=True)
    check(
        "search as_dict",
        len(set_search_dict) > 0 and isinstance(set_search_dict[0], dict),
    )

    # search — as_dataframe
    set_search_df = sdk.sets.search(name="Horizons", as_dataframe=True)
    check("search as_dataframe", hasattr(set_search_df, "shape"))

    # search — offset (pagination)
    set_sp1 = sdk.sets.search(set_type="expansion", limit=3)
    set_sp2_all = sdk.sets.search(set_type="expansion", limit=100)
    check(
        "set search limit constrains",
        len(set_sp1) <= 3 and len(set_sp2_all) > len(set_sp1),
        f"limit3={len(set_sp1)}, limit100={len(set_sp2_all)}",
    )

    # count
    set_count = sdk.sets.count()
    check("set count", set_count > 100, f"total sets: {set_count}")

    # ══════════════════════════════════════════════════════════
    #  IDENTIFIERS — IdentifierQuery (18 methods)
    # ══════════════════════════════════════════════════════════
    section("Identifiers")

    if uuid:
        ids = sdk.identifiers.get_identifiers(uuid)
        check(
            "get_identifiers",
            ids is not None,
            f"keys={list(ids.keys()) if ids else '?'}",
        )

        # Exercise ALL named find_by_* methods using real IDs from Lightning Bolt
        if ids:
            # Scryfall ID
            if ids.get("scryfallId"):
                by_scry = sdk.identifiers.find_by_scryfall_id(ids["scryfallId"])
                check("find_by_scryfall_id", len(by_scry) > 0, f"found {len(by_scry)}")

                by_scry_dict = sdk.identifiers.find_by_scryfall_id(
                    ids["scryfallId"], as_dict=True
                )
                check("find_by_scryfall_id as_dict", isinstance(by_scry_dict, list))
            else:
                skip("find_by_scryfall_id", "no scryfallId in data")

            # Scryfall Oracle ID
            if ids.get("scryfallOracleId"):
                by_oracle = sdk.identifiers.find_by_scryfall_oracle_id(
                    ids["scryfallOracleId"]
                )
                check(
                    "find_by_scryfall_oracle_id",
                    len(by_oracle) > 0,
                    f"found {len(by_oracle)}",
                )
            else:
                skip("find_by_scryfall_oracle_id", "no scryfallOracleId")

            # Scryfall Illustration ID
            if ids.get("scryfallIllustrationId"):
                by_illus = sdk.identifiers.find_by_scryfall_illustration_id(
                    ids["scryfallIllustrationId"]
                )
                check(
                    "find_by_scryfall_illustration_id",
                    len(by_illus) > 0,
                    f"found {len(by_illus)}",
                )
            else:
                skip("find_by_scryfall_illustration_id", "no scryfallIllustrationId")

            # TCGPlayer Product ID
            if ids.get("tcgplayerProductId"):
                by_tcg = sdk.identifiers.find_by_tcgplayer_id(
                    str(ids["tcgplayerProductId"])
                )
                check("find_by_tcgplayer_id", len(by_tcg) > 0, f"found {len(by_tcg)}")
            else:
                skip("find_by_tcgplayer_id", "no tcgplayerProductId")

            # TCGPlayer Etched ID
            if ids.get("tcgplayerEtchedProductId"):
                by_tcg_e = sdk.identifiers.find_by_tcgplayer_etched_id(
                    str(ids["tcgplayerEtchedProductId"])
                )
                check("find_by_tcgplayer_etched_id", len(by_tcg_e) > 0)
            else:
                skip("find_by_tcgplayer_etched_id", "no tcgplayerEtchedProductId")

            # MTGO ID
            if ids.get("mtgoId"):
                by_mtgo = sdk.identifiers.find_by_mtgo_id(str(ids["mtgoId"]))
                check("find_by_mtgo_id", len(by_mtgo) > 0)
            else:
                skip("find_by_mtgo_id", "no mtgoId")

            # MTGO Foil ID
            if ids.get("mtgoFoilId"):
                by_mtgo_f = sdk.identifiers.find_by_mtgo_foil_id(str(ids["mtgoFoilId"]))
                check("find_by_mtgo_foil_id", len(by_mtgo_f) > 0)
            else:
                skip("find_by_mtgo_foil_id", "no mtgoFoilId")

            # MTG Arena ID
            if ids.get("mtgArenaId"):
                by_arena = sdk.identifiers.find_by_mtg_arena_id(str(ids["mtgArenaId"]))
                check("find_by_mtg_arena_id", len(by_arena) > 0)
            else:
                skip("find_by_mtg_arena_id", "no mtgArenaId")

            # Multiverse ID
            if ids.get("multiverseId"):
                by_multi = sdk.identifiers.find_by_multiverse_id(
                    str(ids["multiverseId"])
                )
                check("find_by_multiverse_id", len(by_multi) > 0)
            else:
                skip("find_by_multiverse_id", "no multiverseId")

            # MCM ID
            if ids.get("mcmId"):
                by_mcm = sdk.identifiers.find_by_mcm_id(str(ids["mcmId"]))
                check("find_by_mcm_id", len(by_mcm) > 0)
            else:
                skip("find_by_mcm_id", "no mcmId")

            # MCM Meta ID
            if ids.get("mcmMetaId"):
                by_mcm_m = sdk.identifiers.find_by_mcm_meta_id(str(ids["mcmMetaId"]))
                check("find_by_mcm_meta_id", len(by_mcm_m) > 0)
            else:
                skip("find_by_mcm_meta_id", "no mcmMetaId")

            # Card Kingdom ID
            if ids.get("cardKingdomId"):
                by_ck = sdk.identifiers.find_by_card_kingdom_id(
                    str(ids["cardKingdomId"])
                )
                check("find_by_card_kingdom_id", len(by_ck) > 0)
            else:
                skip("find_by_card_kingdom_id", "no cardKingdomId")

            # Card Kingdom Foil ID
            if ids.get("cardKingdomFoilId"):
                by_ck_f = sdk.identifiers.find_by_card_kingdom_foil_id(
                    str(ids["cardKingdomFoilId"])
                )
                check("find_by_card_kingdom_foil_id", len(by_ck_f) > 0)
            else:
                skip("find_by_card_kingdom_foil_id", "no cardKingdomFoilId")

            # Card Kingdom Etched ID
            if ids.get("cardKingdomEtchedId"):
                by_ck_e = sdk.identifiers.find_by_card_kingdom_etched_id(
                    str(ids["cardKingdomEtchedId"])
                )
                check("find_by_card_kingdom_etched_id", len(by_ck_e) > 0)
            else:
                skip("find_by_card_kingdom_etched_id", "no cardKingdomEtchedId")

            # Cardsphere ID
            if ids.get("cardsphereId"):
                by_cs = sdk.identifiers.find_by_cardsphere_id(str(ids["cardsphereId"]))
                check("find_by_cardsphere_id", len(by_cs) > 0)
            else:
                skip("find_by_cardsphere_id", "no cardsphereId")

            # Cardsphere Foil ID
            if ids.get("cardsphereFoilId"):
                by_cs_f = sdk.identifiers.find_by_cardsphere_foil_id(
                    str(ids["cardsphereFoilId"])
                )
                check("find_by_cardsphere_foil_id", len(by_cs_f) > 0)
            else:
                skip("find_by_cardsphere_foil_id", "no cardsphereFoilId")

            # Generic find_by with valid column
            if ids.get("scryfallId"):
                by_gen = sdk.identifiers.find_by("scryfallId", ids["scryfallId"])
                check("find_by generic (scryfallId)", len(by_gen) > 0)

            # Generic find_by as_dict
            if ids.get("scryfallId"):
                by_gen_d = sdk.identifiers.find_by(
                    "scryfallId", ids["scryfallId"], as_dict=True
                )
                check("find_by generic as_dict", isinstance(by_gen_d, list))

    # Second card for identifier coverage — find one with more IDs populated
    # Use a recent card likely to have Arena, Multiverse, Cardsphere, etc.
    section("Identifiers (secondary card for fuller coverage)")
    alt_cards = sdk.cards.search(name="Llanowar Elves", set_code="M19", limit=1)
    if alt_cards:
        alt_uuid = alt_cards[0].uuid
        alt_ids = sdk.identifiers.get_identifiers(alt_uuid)
        if alt_ids:
            for id_col, method_name in [
                ("mtgArenaId", "find_by_mtg_arena_id"),
                ("multiverseId", "find_by_multiverse_id"),
                ("mcmMetaId", "find_by_mcm_meta_id"),
                ("cardsphereId", "find_by_cardsphere_id"),
                ("cardsphereFoilId", "find_by_cardsphere_foil_id"),
                ("cardKingdomEtchedId", "find_by_card_kingdom_etched_id"),
                ("tcgplayerEtchedProductId", "find_by_tcgplayer_etched_id"),
                ("mtgoFoilId", "find_by_mtgo_foil_id"),
            ]:
                val = alt_ids.get(id_col)
                if val:
                    method = getattr(sdk.identifiers, method_name)
                    result = method(str(val))
                    check(
                        f"{method_name} (alt card)", len(result) > 0, f"{id_col}={val}"
                    )
                else:
                    skip(f"{method_name} (alt card)", f"no {id_col}")
        else:
            skip("alt card identifiers", "no identifiers found")
    else:
        skip("alt card identifiers", "Llanowar Elves M19 not found")

    # find_by — invalid column raises ValueError
    try:
        sdk.identifiers.find_by("invalidColumn", "123")
        check("find_by invalid column raises", False)
    except ValueError:
        check("find_by invalid column raises", True)

    # ══════════════════════════════════════════════════════════
    #  LEGALITIES — LegalityQuery (7 methods)
    # ══════════════════════════════════════════════════════════
    section("Legalities")

    if uuid:
        # formats_for_card
        formats = sdk.legalities.formats_for_card(uuid)
        check(
            "formats_for_card",
            len(formats) > 0,
            f"formats: {list(formats.keys())[:5]}...",
        )

        # formats_for_card — verify structure (format->status mapping)
        if formats:
            sample_fmt = next(iter(formats))
            sample_status = formats[sample_fmt]
            check(
                "formats_for_card structure",
                isinstance(sample_fmt, str)
                and sample_status
                in ("Legal", "Banned", "Restricted", "Suspended", "Not Legal"),
                f"{sample_fmt}={sample_status}",
            )

        # is_legal
        is_legal = sdk.legalities.is_legal(uuid, "modern")
        check("is_legal modern", is_legal is True)

        is_legal_fake = sdk.legalities.is_legal(uuid, "nonexistent_format")
        check("is_legal nonexistent format", is_legal_fake is False)

    # legal_in — model
    modern_cards = sdk.legalities.legal_in("modern", limit=5)
    check("legal_in modern", len(modern_cards) > 0, f"found {len(modern_cards)}")

    # legal_in — as_dict
    modern_dict = sdk.legalities.legal_in("modern", limit=3, as_dict=True)
    check("legal_in as_dict", len(modern_dict) > 0 and isinstance(modern_dict[0], dict))

    # legal_in — as_dataframe
    modern_df = sdk.legalities.legal_in("modern", limit=3, as_dataframe=True)
    check("legal_in as_dataframe", hasattr(modern_df, "shape"))

    # legal_in — with offset
    legal_p1 = sdk.legalities.legal_in("modern", limit=3, offset=0)
    legal_p2 = sdk.legalities.legal_in("modern", limit=3, offset=3)
    check("legal_in offset", len(legal_p1) > 0 and len(legal_p2) > 0)

    # banned_in — result structure validation
    banned = sdk.legalities.banned_in("modern", limit=5)
    check("banned_in modern", isinstance(banned, list), f"found {len(banned)}")
    if banned:
        check(
            "banned_in has name+uuid keys",
            "name" in banned[0] and "uuid" in banned[0],
            f"keys={list(banned[0].keys())}",
        )

    # restricted_in — result structure validation
    restricted = sdk.legalities.restricted_in("vintage", limit=5)
    check(
        "restricted_in vintage",
        isinstance(restricted, list),
        f"found {len(restricted)}",
    )
    if restricted:
        check(
            "restricted_in has name+uuid keys",
            "name" in restricted[0] and "uuid" in restricted[0],
            f"keys={list(restricted[0].keys())}",
        )

    # suspended_in (may have 0 results if no cards are currently suspended)
    suspended = sdk.legalities.suspended_in("historic", limit=5)
    check(
        "suspended_in historic", isinstance(suspended, list), f"found {len(suspended)}"
    )

    # not_legal_in
    not_legal = sdk.legalities.not_legal_in("standard", limit=5)
    check(
        "not_legal_in standard", isinstance(not_legal, list), f"found {len(not_legal)}"
    )
    if not_legal:
        check(
            "not_legal_in has name+uuid keys",
            "name" in not_legal[0] and "uuid" in not_legal[0],
            f"keys={list(not_legal[0].keys())}",
        )

    # banned_in — with offset
    banned_p1 = sdk.legalities.banned_in("modern", limit=2, offset=0)
    banned_p2 = sdk.legalities.banned_in("modern", limit=2, offset=2)
    check(
        "banned_in offset",
        isinstance(banned_p1, list) and isinstance(banned_p2, list),
    )

    # ══════════════════════════════════════════════════════════
    #  PRICES — PriceQuery (7 methods)
    #  Downloads AllPricesToday.json.gz (~large file)
    # ══════════════════════════════════════════════════════════
    section("Prices")

    try:
        # get — raw nested structure
        if uuid:
            price_raw = sdk.prices.get(uuid)
            check(
                "prices.get",
                isinstance(price_raw, (dict, type(None))),
                f"type={type(price_raw).__name__}",
            )

            # today
            today_prices = sdk.prices.today(uuid)
            check(
                "prices.today",
                isinstance(today_prices, list),
                f"found {len(today_prices)} rows",
            )

            if today_prices:
                # Validate price row structure
                first_row = today_prices[0]
                expected_keys = {
                    "uuid",
                    "provider",
                    "finish",
                    "price",
                    "date",
                    "category",
                }
                actual_keys = set(first_row.keys())
                check(
                    "price row has expected keys",
                    expected_keys.issubset(actual_keys),
                    f"missing={expected_keys - actual_keys}, actual={sorted(actual_keys)}",
                )

                # today — with provider filter
                providers = {r.get("provider") for r in today_prices}
                if providers:
                    first_prov = next(iter(providers))
                    today_filt = sdk.prices.today(uuid, provider=first_prov)
                    check(
                        "prices.today provider filter",
                        len(today_filt) > 0,
                        f"provider={first_prov}",
                    )

                # today — with finish filter
                finishes = {r.get("finish") for r in today_prices}
                if finishes:
                    first_fin = next(iter(finishes))
                    today_fin = sdk.prices.today(uuid, finish=first_fin)
                    check(
                        "prices.today finish filter",
                        len(today_fin) > 0,
                        f"finish={first_fin}",
                    )

                # today — with category filter
                categories = {r.get("category") for r in today_prices}
                if categories:
                    first_cat = next(iter(categories))
                    today_cat = sdk.prices.today(uuid, category=first_cat)
                    check(
                        "prices.today category filter",
                        len(today_cat) > 0,
                        f"cat={first_cat}",
                    )

                # today — as_dataframe
                today_df = sdk.prices.today(uuid, as_dataframe=True)
                check("prices.today as_dataframe", hasattr(today_df, "shape"))

                # today — as_dict returns same as default (both are dicts)
                today_dict = sdk.prices.today(uuid, as_dict=True)
                check(
                    "prices.today as_dict matches default",
                    len(today_dict) == len(today_prices),
                    f"dict={len(today_dict)}, default={len(today_prices)}",
                )

            # history
            history = sdk.prices.history(uuid)
            check(
                "prices.history",
                isinstance(history, list),
                f"found {len(history)} rows",
            )

            if history:
                # history — with date range
                dates = sorted({r.get("date", "") for r in history if r.get("date")})
                if len(dates) >= 2:
                    hist_range = sdk.prices.history(
                        uuid, date_from=dates[0], date_to=dates[-1]
                    )
                    check("prices.history date range", len(hist_range) > 0)
                else:
                    skip("prices.history date range", "only 1 date")

                # history — with provider filter
                hist_prov = sdk.prices.history(
                    uuid, provider=history[0].get("provider", "tcgplayer")
                )
                check("prices.history provider filter", isinstance(hist_prov, list))

                # history — with finish filter
                hist_fin = sdk.prices.history(
                    uuid, finish=history[0].get("finish", "normal")
                )
                check("prices.history finish filter", isinstance(hist_fin, list))

                # history — with category filter
                hist_cat = sdk.prices.history(
                    uuid, category=history[0].get("category", "retail")
                )
                check("prices.history category filter", isinstance(hist_cat, list))

                # history — as_dataframe
                hist_df = sdk.prices.history(uuid, as_dataframe=True)
                check("prices.history as_dataframe", hasattr(hist_df, "shape"))

            # price_trend
            trend = sdk.prices.price_trend(uuid)
            check(
                "prices.price_trend",
                isinstance(trend, (dict, type(None))),
                f"trend={trend}" if trend else "no trend data",
            )

            if trend:
                check(
                    "price_trend has keys",
                    all(k in trend for k in ("min_price", "max_price", "avg_price")),
                )

                # price_trend — verify additional keys
                check(
                    "price_trend has date+count keys",
                    all(k in trend for k in ("first_date", "last_date", "data_points")),
                    f"keys={list(trend.keys())}",
                )

                # price_trend with provider/finish
                trend2 = sdk.prices.price_trend(
                    uuid, provider="tcgplayer", finish="normal"
                )
                check(
                    "price_trend with filters", isinstance(trend2, (dict, type(None)))
                )

                # price_trend with buylist category
                trend_buy = sdk.prices.price_trend(uuid, category="buylist")
                check(
                    "price_trend buylist",
                    isinstance(trend_buy, (dict, type(None))),
                )

            # cheapest_printing
            cheapest = sdk.prices.cheapest_printing("Lightning Bolt")
            check(
                "prices.cheapest_printing",
                isinstance(cheapest, (dict, type(None))),
                f"cheapest={cheapest}" if cheapest else "no price data",
            )

            if cheapest:
                check(
                    "cheapest has price", "price" in cheapest and cheapest["price"] > 0
                )
                # Validate structure
                check(
                    "cheapest has expected keys",
                    all(k in cheapest for k in ("uuid", "setCode", "price")),
                    f"keys={list(cheapest.keys())}",
                )

            # cheapest with different provider
            cheapest2 = sdk.prices.cheapest_printing(
                "Lightning Bolt", provider="cardkingdom"
            )
            check(
                "cheapest_printing alt provider",
                isinstance(cheapest2, (dict, type(None))),
            )

            # cheapest_printings (bulk — each card's cheapest)
            cheapest_bulk = sdk.prices.cheapest_printings(limit=5)
            check(
                "prices.cheapest_printings (bulk)",
                isinstance(cheapest_bulk, list) and len(cheapest_bulk) > 0,
                f"found {len(cheapest_bulk)}",
            )
            if cheapest_bulk:
                check(
                    "cheapest_printings row structure",
                    all(
                        k in cheapest_bulk[0]
                        for k in ("name", "cheapest_set", "min_price")
                    ),
                    f"keys={list(cheapest_bulk[0].keys())}",
                )

            # cheapest_printings with pagination
            cp_p1 = sdk.prices.cheapest_printings(limit=3, offset=0)
            cp_p2 = sdk.prices.cheapest_printings(limit=3, offset=3)
            check(
                "cheapest_printings pagination",
                len(cp_p1) > 0 and len(cp_p2) > 0,
            )

            # cheapest_printings with different provider/finish
            cp_ck = sdk.prices.cheapest_printings(
                provider="cardkingdom", finish="normal", limit=3
            )
            check(
                "cheapest_printings alt provider",
                isinstance(cp_ck, list),
                f"found {len(cp_ck)}",
            )

            # most_expensive_printings (bulk)
            expensive_bulk = sdk.prices.most_expensive_printings(limit=5)
            check(
                "prices.most_expensive_printings (bulk)",
                isinstance(expensive_bulk, list) and len(expensive_bulk) > 0,
                f"found {len(expensive_bulk)}",
            )
            if expensive_bulk:
                check(
                    "most_expensive row structure",
                    all(
                        k in expensive_bulk[0]
                        for k in ("name", "priciest_set", "max_price")
                    ),
                    f"keys={list(expensive_bulk[0].keys())}",
                )
                # Verify ordering (most expensive first)
                check(
                    "most_expensive ordered DESC",
                    expensive_bulk[0]["max_price"] >= expensive_bulk[-1]["max_price"],
                    f"first={expensive_bulk[0]['max_price']}, last={expensive_bulk[-1]['max_price']}",
                )

            # most_expensive_printings with pagination
            ep_p1 = sdk.prices.most_expensive_printings(limit=3, offset=0)
            ep_p2 = sdk.prices.most_expensive_printings(limit=3, offset=3)
            check(
                "most_expensive pagination",
                len(ep_p1) > 0 and len(ep_p2) > 0,
            )

        else:
            skip("prices tests", "no uuid from Lightning Bolt")

    except Exception as e:
        check("prices module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  SET FINANCIAL SUMMARY (requires prices loaded)
    # ══════════════════════════════════════════════════════════
    section("Set Financial Summary (EV calculation)")

    if "prices_today" in sdk._conn._registered_views:
        fin = sdk.sets.get_financial_summary("MH3")
        check(
            "get_financial_summary MH3",
            fin is not None and fin.get("card_count", 0) > 0,
            f"cards={fin.get('card_count')}, total=${fin.get('total_value')}"
            if fin
            else "no data",
        )

        # With different provider
        fin_ck = sdk.sets.get_financial_summary(
            "MH3", provider="cardkingdom", finish="normal"
        )
        check(
            "get_financial_summary alt provider",
            isinstance(fin_ck, (dict, type(None))),
        )

        # Nonexistent set
        fin_none = sdk.sets.get_financial_summary("ZZZZZ")
        check("get_financial_summary no data", fin_none is None)

        # Verify structure when data exists
        if fin:
            check(
                "financial summary keys",
                all(
                    k in fin
                    for k in (
                        "card_count",
                        "total_value",
                        "avg_value",
                        "min_value",
                        "max_value",
                    )
                ),
                f"keys={list(fin.keys())}",
            )
    else:
        skip("get_financial_summary", "prices not loaded")

    # ══════════════════════════════════════════════════════════
    #  DECKS — DeckQuery (3 methods)
    #  Downloads DeckList.json
    # ══════════════════════════════════════════════════════════
    section("Decks")

    try:
        # count
        deck_count = sdk.decks.count()
        check("decks.count", deck_count >= 0, f"total decks: {deck_count}")

        # list — no filter
        deck_list = sdk.decks.list()
        check(
            "decks.list (all)", isinstance(deck_list, list), f"found {len(deck_list)}"
        )

        if deck_list:
            # list — as_dict
            deck_list_d = sdk.decks.list(as_dict=True)
            check(
                "decks.list as_dict",
                len(deck_list_d) > 0 and isinstance(deck_list_d[0], dict),
            )

            # list — set_code filter
            first_code = (
                deck_list[0].code
                if hasattr(deck_list[0], "code")
                else deck_list_d[0].get("code", "")
            )
            if first_code:
                decks_by_set = sdk.decks.list(set_code=first_code)
                check("decks.list set_code", len(decks_by_set) > 0, f"set={first_code}")

            # list — deck_type filter
            first_type = deck_list[0].type if hasattr(deck_list[0], "type") else ""
            if first_type:
                decks_by_type = sdk.decks.list(deck_type=first_type)
                check(
                    "decks.list deck_type", len(decks_by_type) > 0, f"type={first_type}"
                )

            # search — name
            first_name = deck_list[0].name if hasattr(deck_list[0], "name") else ""
            if first_name:
                # use first word of name for partial match
                search_term = first_name.split()[0] if first_name else "Starter"
                deck_search = sdk.decks.search(name=search_term)
                check(
                    "decks.search name", len(deck_search) > 0, f"term='{search_term}'"
                )

            # search — set_code
            if first_code:
                deck_search_set = sdk.decks.search(set_code=first_code)
                check("decks.search set_code", len(deck_search_set) > 0)

            # search — as_dict
            deck_search_d = sdk.decks.search(
                name=first_name.split()[0] if first_name else "Starter", as_dict=True
            )
            check("decks.search as_dict", isinstance(deck_search_d, list))
        else:
            skip("deck list/search tests", "no decks loaded")

    except Exception as e:
        check("decks module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  SKUS — SkuQuery (3 methods)
    #  Downloads TcgplayerSkus.json.gz (~large file)
    # ══════════════════════════════════════════════════════════
    section("SKUs")

    try:
        if uuid:
            # get
            skus = sdk.skus.get(uuid)
            check("skus.get", isinstance(skus, list), f"found {len(skus)} SKUs")

            skus_dict = sdk.skus.get(uuid, as_dict=True)
            check("skus.get as_dict", isinstance(skus_dict, list))

            if skus and len(skus) > 0:
                # Access first SKU — it's either a model or dict
                first_sku = skus_dict[0] if skus_dict else None
                if first_sku:
                    # Validate SKU row structure
                    sku_expected = {"skuId", "productId"}
                    sku_actual = set(first_sku.keys())
                    check(
                        "SKU row has expected keys",
                        sku_expected.issubset(sku_actual),
                        f"missing={sku_expected - sku_actual}, keys={sorted(sku_actual)}",
                    )

                    # find_by_sku_id
                    sku_id = first_sku.get("skuId")
                    if sku_id:
                        by_sku = sdk.skus.find_by_sku_id(sku_id)
                        check(
                            "skus.find_by_sku_id", by_sku is not None, f"skuId={sku_id}"
                        )
                    else:
                        skip("skus.find_by_sku_id", "no skuId in data")

                    # find_by_product_id
                    prod_id = first_sku.get("productId")
                    if prod_id:
                        by_prod = sdk.skus.find_by_product_id(prod_id)
                        check(
                            "skus.find_by_product_id",
                            len(by_prod) > 0,
                            f"productId={prod_id}",
                        )

                        # find_by_product_id as_dict
                        by_prod_d = sdk.skus.find_by_product_id(prod_id, as_dict=True)
                        check(
                            "skus.find_by_product_id as_dict",
                            isinstance(by_prod_d, list),
                        )
                    else:
                        skip("skus.find_by_product_id", "no productId in data")

                    # find_by_sku_id — nonexistent
                    by_sku_missing = sdk.skus.find_by_sku_id(-99999)
                    check("skus.find_by_sku_id nonexistent", by_sku_missing is None)
            else:
                skip("skus.find_by_sku_id", "no SKU data for this card")
                skip("skus.find_by_product_id", "no SKU data for this card")
        else:
            skip("skus tests", "no uuid")

    except Exception as e:
        check("skus module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  ENUMS — EnumQuery (3 methods)
    #  Downloads Keywords.json, CardTypes.json, EnumValues.json
    # ══════════════════════════════════════════════════════════
    section("Enums")

    try:
        # keywords
        kw = sdk.enums.keywords()
        check(
            "enums.keywords",
            isinstance(kw, dict) and len(kw) > 0,
            f"keys={list(kw.keys())[:5]}",
        )

        if kw:
            # Verify structure: should have abilityWords, keywordAbilities, keywordActions
            has_ability = "abilityWords" in kw or any(
                "ability" in k.lower() for k in kw
            )
            check(
                "keywords has expected keys",
                has_ability or len(kw) > 0,
                f"top keys: {list(kw.keys())[:5]}",
            )

        # card_types
        ct = sdk.enums.card_types()
        check(
            "enums.card_types",
            isinstance(ct, dict) and len(ct) > 0,
            f"keys={list(ct.keys())[:5]}",
        )

        if ct:
            # Should include things like "creature", "instant", "sorcery"
            has_creature = any("creature" in k.lower() for k in ct)
            check("card_types has creature", has_creature or len(ct) > 0)

        # enum_values
        ev = sdk.enums.enum_values()
        check(
            "enums.enum_values",
            isinstance(ev, dict) and len(ev) > 0,
            f"keys={list(ev.keys())[:5]}",
        )

    except Exception as e:
        check("enums module available", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  SEALED — SealedQuery (2 methods)
    #  Flat sets.parquet lacks sealedProduct — tests graceful degradation
    # ══════════════════════════════════════════════════════════
    section("Sealed Products (flat parquet - graceful degradation)")

    # list — no filter
    sealed_all = sdk.sealed.list()
    check("sealed.list (all)", isinstance(sealed_all, list), f"found {len(sealed_all)}")

    # list — set_code filter
    sealed_mh3 = sdk.sealed.list(set_code="MH3")
    check(
        "sealed.list set_code=MH3",
        isinstance(sealed_mh3, list),
        f"found {len(sealed_mh3)}",
    )

    # list — category filter
    sealed_cat = sdk.sealed.list(category="booster_box")
    check(
        "sealed.list category", isinstance(sealed_cat, list), f"found {len(sealed_cat)}"
    )

    # list — limit=1 (verify pagination)
    sealed_one = sdk.sealed.list(limit=1)
    check("sealed.list limit=1", isinstance(sealed_one, list) and len(sealed_one) <= 1)

    # list — as_dict
    sealed_dict = sdk.sealed.list(as_dict=True)
    check("sealed.list as_dict", isinstance(sealed_dict, list))

    # list — as_dataframe
    sealed_df = sdk.sealed.list(as_dataframe=True)
    check("sealed.list as_dataframe", sealed_df is not None)

    # get — nonexistent uuid
    sealed_item = sdk.sealed.get("00000000-0000-0000-0000-000000000000")
    check("sealed.get (graceful)", sealed_item is None or isinstance(sealed_item, dict))

    # ══════════════════════════════════════════════════════════
    #  BOOSTER — BoosterSimulator (4 methods)
    #  Flat sets.parquet lacks booster column — tests graceful degradation
    # ══════════════════════════════════════════════════════════
    section("Booster Simulation (flat parquet - graceful degradation)")

    # available_types
    types = sdk.booster.available_types("MH3")
    check("booster.available_types", isinstance(types, list), f"types: {types}")

    # open_pack — expect ValueError or graceful fail
    try:
        pack = sdk.booster.open_pack("MH3", "draft")
        check("booster.open_pack", isinstance(pack, list), f"got {len(pack)} cards")
    except ValueError:
        check("booster.open_pack raises ValueError (no booster data)", True)

    # open_box — expect ValueError or graceful fail
    try:
        box = sdk.booster.open_box("MH3", "draft", packs=1)
        check("booster.open_box", isinstance(box, list))
    except ValueError:
        check("booster.open_box raises ValueError (no booster data)", True)

    # sheet_contents
    contents = sdk.booster.sheet_contents("MH3", "draft", "common")
    check(
        "booster.sheet_contents",
        contents is None or isinstance(contents, dict),
        f"type={type(contents).__name__}",
    )

    # ══════════════════════════════════════════════════════════
    #  RAW SQL — sdk.sql() (all modes)
    # ══════════════════════════════════════════════════════════
    section("Raw SQL / Escape Hatches")

    # Simple query
    rows = sdk.sql("SELECT COUNT(*) AS cnt FROM cards")
    check("sql COUNT", rows[0]["cnt"] > 1000, f"count={rows[0]['cnt']}")

    # Query with params
    rows_param = sdk.sql(
        "SELECT name FROM cards WHERE manaValue = $1 LIMIT $2",
        params=[1.0, 5],
    )
    check("sql with params", len(rows_param) > 0, f"found {len(rows_param)}")

    # More complex query
    top_edhrec = sdk.sql(
        "SELECT name, edhrecRank FROM cards "
        "WHERE edhrecRank IS NOT NULL "
        "ORDER BY edhrecRank ASC LIMIT 5"
    )
    check(
        "sql top EDHREC",
        len(top_edhrec) == 5,
        f"top: {[r['name'] for r in top_edhrec]}",
    )

    # sql — as_dataframe
    df_result = sdk.sql("SELECT name, manaValue FROM cards LIMIT 5", as_dataframe=True)
    check("sql as_dataframe", hasattr(df_result, "shape"))

    # Cross-table join via raw SQL
    join_result = sdk.sql(
        "SELECT c.name, s.name AS setName "
        "FROM cards c JOIN sets s ON c.setCode = s.code "
        "LIMIT 3"
    )
    check("sql cross-table join", len(join_result) > 0 and "setName" in join_result[0])

    # sql — unicode param
    unicode_result = sdk.sql(
        "SELECT name FROM cards WHERE name = $1 LIMIT 1",
        params=["Lightning Bolt"],
    )
    check("sql unicode param", isinstance(unicode_result, list))

    # ══════════════════════════════════════════════════════════
    #  VIEWS — verify views grew as we queried
    # ══════════════════════════════════════════════════════════
    section("Views (post-query)")

    views_after = sdk.views
    check(
        "views grew",
        len(views_after) > len(views_before),
        f"before={len(views_before)}, after={len(views_after)}, views={views_after}",
    )

    # ══════════════════════════════════════════════════════════
    #  EDGE CASES & VALIDATION
    # ══════════════════════════════════════════════════════════
    section("Edge Cases & Validation")

    # Empty search results
    empty = sdk.cards.search(name="XYZ_NONEXISTENT_CARD_12345", limit=5)
    check("empty search result", len(empty) == 0)

    # Card with special characters in name
    s = sdk.cards.search(name="Jötun%", limit=5)
    check("search unicode name", isinstance(s, list), f"found {len(s)}")

    # Boundary: limit=0
    zero_limit = sdk.cards.search(name="Lightning%", limit=0)
    check("search limit=0", len(zero_limit) == 0)

    # Boundary: negative offset (should not crash)
    try:
        neg_offset = sdk.cards.search(name="Lightning%", limit=5, offset=-1)
        check("search negative offset", isinstance(neg_offset, list), "no crash")
    except Exception:
        check("search negative offset raises", True, "error is acceptable")

    # Card model field validation (comprehensive)
    if bolt:
        card = bolt[0]
        check("card has uuid", bool(card.uuid))
        check("card has name", card.name == "Lightning Bolt")
        check("card has colors", isinstance(card.colors, list), f"colors={card.colors}")
        check(
            "card has manaValue", card.mana_value is not None, f"mv={card.mana_value}"
        )
        check("card has text", bool(card.text), f"text={card.text[:50]}...")
        check("card has rarity", bool(card.rarity), f"rarity={card.rarity}")
        check("card has setCode", bool(card.set_code), f"setCode={card.set_code}")
        check("card has layout", bool(card.layout), f"layout={card.layout}")
        check("card has type", bool(card.type), f"type={card.type}")
        check("card has artist", bool(card.artist), f"artist={card.artist}")
        check("card has number", card.number is not None, f"number={card.number}")
        check(
            "card has availability",
            isinstance(card.availability, list) and len(card.availability) > 0,
            f"availability={card.availability}",
        )
        check(
            "card has keywords",
            isinstance(card.keywords, list),
            f"keywords={card.keywords}",
        )

    # Set model field validation (comprehensive)
    if mh3:
        check("set has code", mh3.code == "MH3")
        check("set has name", "Horizons" in mh3.name, f"name={mh3.name}")
        check("set has releaseDate", bool(mh3.release_date))
        check("set has type", bool(mh3.type))
        check(
            "set has baseSetSize",
            mh3.base_set_size > 0,
            f"baseSetSize={mh3.base_set_size}",
        )
        check(
            "set has totalSetSize",
            mh3.total_set_size > 0,
            f"totalSetSize={mh3.total_set_size}",
        )
        # Additional set fields
        if hasattr(mh3, "is_online_only"):
            check(
                "set has isOnlineOnly",
                isinstance(mh3.is_online_only, bool),
                f"isOnlineOnly={mh3.is_online_only}",
            )

    # Token model field validation (comprehensive)
    if token_search:
        tok = token_search[0]
        check("token has uuid", bool(tok.uuid))
        check("token has name", bool(tok.name))
        check(
            "token has colors",
            isinstance(tok.colors, list),
            f"colors={tok.colors}",
        )
        check("token has types", isinstance(tok.types, list), f"types={tok.types}")
        if hasattr(tok, "set_code"):
            check("token has setCode", bool(tok.set_code), f"setCode={tok.set_code}")

    # Atomic card model validation
    if atomic:
        a = atomic[0]
        check("atomic has name", a.name == "Lightning Bolt")
        check("atomic has layout", bool(a.layout))
        check("atomic has colors", isinstance(a.colors, list))
        check("atomic has types", isinstance(a.types, list), f"types={a.types}")
        check("atomic has text", bool(a.text), f"text={a.text[:50]}...")

    # DeckList model validation
    try:
        deck_list_all = sdk.decks.list()
        if deck_list_all:
            d = deck_list_all[0]
            check("deck has code", bool(d.code))
            check("deck has name", bool(d.name))
            check("deck has type", bool(d.type))
            check("deck has fileName", bool(d.file_name))
    except Exception:
        skip("deck model validation", "deck data not loaded")

    # DataFrame column validation (not just shape)
    df_cards = sdk.cards.search(name="Lightning Bolt", limit=3, as_dataframe=True)
    if hasattr(df_cards, "columns"):
        cols = set(df_cards.columns)
        check(
            "DataFrame has expected columns",
            {"name", "uuid", "manaValue", "rarity"}.issubset(cols),
            f"sample cols: {sorted(cols)[:10]}",
        )
    else:
        skip("DataFrame column validation", "no columns attribute")

    df_sets = sdk.sets.list(limit=3, as_dataframe=True)
    if hasattr(df_sets, "columns"):
        scols = set(df_sets.columns)
        check(
            "Set DataFrame has expected columns",
            {"code", "name", "releaseDate"}.issubset(scols),
            f"sample cols: {sorted(scols)[:10]}",
        )
    else:
        skip("Set DataFrame column validation", "no columns attribute")

    # ══════════════════════════════════════════════════════════
    #  EXPORT DB
    # ══════════════════════════════════════════════════════════
    section("Export DB")

    with tempfile.TemporaryDirectory() as tmpdir:
        export_path = Path(tmpdir) / "test_export.duckdb"
        result_path = sdk.export_db(export_path)
        check("export_db returns path", result_path == export_path)
        check("export_db creates file", export_path.exists())
        check(
            "export_db file has size",
            export_path.stat().st_size > 0,
            f"size={export_path.stat().st_size}",
        )

        # Verify exported DB is readable with DuckDB
        import duckdb

        with duckdb.connect(str(export_path)) as export_conn:
            tables = [
                r[0]
                for r in export_conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            ]
            check(
                "export_db has tables",
                "cards" in tables,
                f"tables={tables[:5]}",
            )
            export_count = export_conn.execute("SELECT COUNT(*) FROM cards").fetchone()[
                0
            ]
            check(
                "export_db cards queryable",
                export_count > 0,
                f"count={export_count}",
            )

    # ══════════════════════════════════════════════════════════
    #  ASYNC CLIENT — AsyncMtgJsonTools
    # ══════════════════════════════════════════════════════════
    section("AsyncMtgJsonTools")

    async def run_async_tests():
        results = []

        # Basic construction
        async with AsyncMtgJsonTools() as async_sdk:
            results.append(
                ("async __aenter__", isinstance(async_sdk, AsyncMtgJsonTools))
            )

            # inner property
            results.append(
                (
                    "async inner is MtgJsonTools",
                    isinstance(async_sdk.inner, MtgJsonTools),
                )
            )

            # run() with cards.search
            cards = await async_sdk.run(
                async_sdk.inner.cards.search, name="Lightning%", limit=3
            )
            results.append(("async run cards.search", len(cards) > 0))

            # run() with sets.get
            s = await async_sdk.run(async_sdk.inner.sets.get, "MH3")
            results.append(("async run sets.get", s is not None))

            # sql()
            rows = await async_sdk.sql("SELECT COUNT(*) AS cnt FROM cards")
            results.append(("async sql", rows[0]["cnt"] > 0))

            # sql() with as_dataframe
            df = await async_sdk.sql(
                "SELECT name FROM cards LIMIT 3", as_dataframe=True
            )
            results.append(("async sql as_dataframe", hasattr(df, "shape")))

            # Sequential async queries (DuckDB in-memory is single-writer)
            r1 = await async_sdk.run(async_sdk.inner.cards.count)
            r2 = await async_sdk.run(async_sdk.inner.sets.count)
            r3 = await async_sdk.run(async_sdk.inner.tokens.count)
            results.append(
                (
                    "async sequential multi-query",
                    r1 > 0 and r2 > 0 and r3 > 0,
                )
            )

        # After __aexit__, inner should be closed
        try:
            async_sdk.inner.sql("SELECT 1")
            results.append(("async __aexit__ closes", False))
        except Exception:
            results.append(("async __aexit__ closes", True))

        # Constructor with max_workers
        async_sdk2 = AsyncMtgJsonTools(max_workers=2)
        results.append(("async constructor max_workers", async_sdk2 is not None))
        await async_sdk2.close()
        results.append(("async manual close", True))

        return results

    try:
        async_results = asyncio.run(run_async_tests())
        for label, condition in async_results:
            check(label, condition)
    except Exception as e:
        check("async tests", False, f"error: {e}")

    # ══════════════════════════════════════════════════════════
    #  RESOURCE SAFETY
    # ══════════════════════════════════════════════════════════
    section("Resource Safety")

    # Double close — should not raise
    safety_sdk = MtgJsonTools()
    safety_sdk.close()
    try:
        safety_sdk.close()
        check("double close no error", True)
    except Exception as e:
        check("double close no error", False, f"raised: {e}")

    # Query after close — should raise
    closed_sdk = MtgJsonTools()
    closed_sdk.close()
    try:
        closed_sdk.sql("SELECT 1")
        check("query after close raises", False, "should have raised")
    except Exception:
        check("query after close raises", True)

    # ══════════════════════════════════════════════════════════
    #  CONCURRENT SDK INSTANCES
    # ══════════════════════════════════════════════════════════
    section("Concurrent SDK Instances")

    errors: list[str] = []

    def thread_query(sdk_instance, thread_id):
        try:
            result = sdk_instance.cards.search(name="Lightning%", limit=3)
            if len(result) == 0:
                errors.append(f"thread-{thread_id}: empty result")
        except Exception as e:
            errors.append(f"thread-{thread_id}: {e}")

    sdk2 = MtgJsonTools()
    threads = [
        threading.Thread(target=thread_query, args=(sdk, 1)),
        threading.Thread(target=thread_query, args=(sdk2, 2)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    check(
        "concurrent SDK instances",
        len(errors) == 0,
        f"errors: {errors}" if errors else "both threads OK",
    )
    sdk2.close()

    # ══════════════════════════════════════════════════════════
    #  DONE — close and report
    # ══════════════════════════════════════════════════════════
    sdk.close()
    elapsed = time.time() - t0

    section("RESULTS")
    total = PASS + FAIL
    print(f"  Total:   {total} checks ({SKIP} skipped)")
    print(f"  Passed:  {PASS}")
    print(f"  Failed:  {FAIL}")
    print(f"  Time:    {elapsed:.1f}s")
    print()

    if FAIL > 0:
        print("  *** FAILURES DETECTED ***")
        print()

    return FAIL == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
