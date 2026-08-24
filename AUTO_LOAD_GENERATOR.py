#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║               ChitChat Load Tester v1.0                            ║
║   Full-stack load testing: HTTP REST + WebSocket chat traffic       ║
║   Targets the load balancer → backend (chitchat) architecture      ║
╚══════════════════════════════════════════════════════════════════════╝

Modes:
  rooms       - N rooms x M users per room (e.g. 10 rooms, 100 users each)
  concurrent  - flat N concurrent users, auto-distributed across rooms
  ramp        - ramp from 0 -> N users over T seconds

Workflow per simulated user:
  1. Register (POST /user/create)
  2. Login    (POST /auth/login)  -> stores JWT
  3. Create / Join rooms (POST /room/create, POST /room/join/{id})
  4. Connect WebSocket (ws:///ws?token=JWT)
  5. Join room via WS  ({"type":"JOIN_ROOM","roomId":"..."})
  6. Send messages      ({"type":"SEND_MESSAGE","content":"...","roomId":"..."})
  7. Fetch messages     (GET /rooms/{roomId}/messages/recent)
  8. Fetch rooms list   (GET /room/all)

Requires:  pip install aiohttp websockets
"""

import argparse
import asyncio
import json
import logging
import random
import string
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

try:
    import aiohttp
except ImportError:
    print("ERROR: 'aiohttp' is required.  Install with:  pip install aiohttp")
    sys.exit(1)

try:
    import websockets
except ImportError:
    print("ERROR: 'websockets' is required.  Install with:  pip install websockets")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────
# Configuration & data classes
# ─────────────────────────────────────────────────────────────────────

@dataclass
class LoadTestConfig:
    """Central configuration for a load test run."""
    base_url: str = "http://10.1.75.51:3285"      # load balancer URL
    ws_url: str = "ws://10.1.75.51:3285"           # WebSocket via LB
    mode: str = "rooms"                           # rooms | concurrent | ramp
    num_rooms: int = 10
    users_per_room: int = 100
    total_users: int = 1000                       # for concurrent / ramp mode
    auto_rooms: int = 10                          # rooms to auto-create in concurrent mode
    ramp_duration: int = 60                       # seconds for ramp mode
    messages_per_user: int = 5                    # messages each user sends
    message_delay: float = 0.5                    # seconds between messages
    think_time: float = 0.2                       # pause between HTTP calls
    timeout: int = 30                             # HTTP timeout in seconds
    ws_listen_duration: int = 10                  # how long WS stays open (seconds)
    password: str = "loadtest123"                 # password for all test users
    user_prefix: str = "lt_user_"                 # prefix for generated usernames
    room_prefix: str = "lt_room_"                 # prefix for generated room names
    max_room_capacity: int = 2000                 # maximumCapacity for rooms
    max_concurrency: int = 500                    # semaphore limit
    verbose: bool = False


@dataclass
class UserContext:
    """Per-user state maintained across the test lifecycle."""
    username: str
    password: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    room_ids: list = field(default_factory=list)   # rooms this user belongs to
    ip_address: str = field(default_factory=lambda: f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}")


@dataclass
class Stats:
    """Aggregated statistics."""
    users_registered: int = 0
    users_logged_in: int = 0
    rooms_created: int = 0
    rooms_joined: int = 0
    ws_connected: int = 0
    ws_messages_sent: int = 0
    ws_messages_received: int = 0
    http_requests: int = 0
    http_errors: int = 0
    ws_errors: int = 0
    register_errors: int = 0
    login_errors: int = 0
    latencies: list = field(default_factory=list)  # (label, seconds)
    start_time: float = 0.0
    end_time: float = 0.0

    def summary(self) -> str:
        elapsed = self.end_time - self.start_time if self.end_time else 0
        avg_lat = (
            sum(l for _, l in self.latencies) / len(self.latencies)
            if self.latencies else 0
        )
        p50 = p95 = p99 = 0.0
        if self.latencies:
            sorted_lats = sorted(l for _, l in self.latencies)
            n = len(sorted_lats)
            p50 = sorted_lats[int(n * 0.50)]
            p95 = sorted_lats[int(n * 0.95)]
            p99 = sorted_lats[int(n * 0.99)]

        rps = self.http_requests / elapsed if elapsed > 0 else 0
        ws_mps = self.ws_messages_sent / elapsed if elapsed > 0 else 0

        return (
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║                    LOAD TEST RESULTS                       ║\n"
            "╠══════════════════════════════════════════════════════════════╣\n"
            f"║  Duration               : {elapsed:>10.2f} s                    ║\n"
            f"║  Users registered        : {self.users_registered:>10}                     ║\n"
            f"║  Users logged in         : {self.users_logged_in:>10}                     ║\n"
            f"║  Rooms created           : {self.rooms_created:>10}                     ║\n"
            f"║  Rooms joined            : {self.rooms_joined:>10}                     ║\n"
            f"║  WebSocket connections   : {self.ws_connected:>10}                     ║\n"
            f"║  WS messages sent        : {self.ws_messages_sent:>10}                     ║\n"
            f"║  WS messages received    : {self.ws_messages_received:>10}                     ║\n"
            f"║  HTTP requests           : {self.http_requests:>10}                     ║\n"
            f"║  HTTP errors             : {self.http_errors:>10}                     ║\n"
            f"║  WS errors               : {self.ws_errors:>10}                     ║\n"
            f"║  Register errors         : {self.register_errors:>10}                     ║\n"
            f"║  Login errors            : {self.login_errors:>10}                     ║\n"
            "╠══════════════════════════════════════════════════════════════╣\n"
            f"║  HTTP req/s              : {rps:>10.2f}                     ║\n"
            f"║  WS msg/s sent           : {ws_mps:>10.2f}                     ║\n"
            f"║  Avg latency             : {avg_lat * 1000:>10.2f} ms                   ║\n"
            f"║  P50 latency             : {p50 * 1000:>10.2f} ms                   ║\n"
            f"║  P95 latency             : {p95 * 1000:>10.2f} ms                   ║\n"
            f"║  P99 latency             : {p99 * 1000:>10.2f} ms                   ║\n"
            f"║  Total latency samples   : {len(self.latencies):>10}                     ║\n"
            "╚══════════════════════════════════════════════════════════════╝\n"
        )


# ─────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────

logger = logging.getLogger("loadtest")


# ─────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────

def random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def make_username(prefix: str, index: int) -> str:
    return f"{prefix}{index}_{random_string(4)}"


# ─────────────────────────────────────────────────────────────────────
# HTTP helpers (with latency tracking)
# ─────────────────────────────────────────────────────────────────────

async def timed_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    stats: Stats,
    label: str = "",
    **kwargs,
) -> Optional[aiohttp.ClientResponse]:
    """Fire an HTTP request, record latency, return the response."""
    t0 = time.perf_counter()
    try:
        resp = await session.request(method, url, **kwargs)
        elapsed = time.perf_counter() - t0
        stats.http_requests += 1
        stats.latencies.append((label or url, elapsed))
        return resp
    except Exception as e:
        elapsed = time.perf_counter() - t0
        stats.http_errors += 1
        stats.latencies.append((f"ERR:{label or url}", elapsed))
        logger.warning("HTTP %s %s failed: %s", method, url, e)
        return None


# ─────────────────────────────────────────────────────────────────────
# Core API interactions
# ─────────────────────────────────────────────────────────────────────

async def register_user(
    session: aiohttp.ClientSession,
    cfg: LoadTestConfig,
    user: UserContext,
    stats: Stats,
) -> bool:
    """POST /user/create"""
    payload = {
        "username": user.username,
        "password": user.password,
        "tagline": f"Load test user {user.username}",
        "profilePicture": "",
    }
    headers = {"X-Forwarded-For": user.ip_address}
    resp = await timed_request(
        session, "POST", f"{cfg.base_url}/user/create",
        stats, label="register",
        json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=cfg.timeout),
    )
    if resp and resp.status in (200, 201):
        stats.users_registered += 1
        logger.debug("Registered %s", user.username)
        return True
    else:
        status = resp.status if resp else "no response"
        stats.register_errors += 1
        logger.warning("Register failed for %s (status=%s)", user.username, status)
        return False


async def login_user(
    session: aiohttp.ClientSession,
    cfg: LoadTestConfig,
    user: UserContext,
    stats: Stats,
) -> bool:
    """POST /auth/login -> stores JWT tokens."""
    payload = {"username": user.username, "password": user.password}
    headers = {"X-Forwarded-For": user.ip_address}
    resp = await timed_request(
        session, "POST", f"{cfg.base_url}/auth/login",
        stats, label="login",
        json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=cfg.timeout),
    )
    if resp and resp.status == 200:
        body = await resp.json()
        user.access_token = body.get("accessToken")
        user.refresh_token = body.get("refreshToken")
        if user.access_token:
            stats.users_logged_in += 1
            logger.debug("Logged in %s", user.username)
            return True
    status = resp.status if resp else "no response"
    stats.login_errors += 1
    logger.warning("Login failed for %s (status=%s)", user.username, status)
    return False


async def create_room(
    session: aiohttp.ClientSession,
    cfg: LoadTestConfig,
    user: UserContext,
    room_name: str,
    participants: list,
    stats: Stats,
) -> Optional[str]:
    """POST /room/create -> returns room ID (UUID string)."""
    headers = {"Authorization": f"Bearer {user.access_token}"}
    payload = {
        "roomname": room_name,
        "participants": participants,
        "maximumCapacity": cfg.max_room_capacity,
    }
    resp = await timed_request(
        session, "POST", f"{cfg.base_url}/room/create",
        stats, label="create_room",
        json=payload, headers=headers,
        timeout=aiohttp.ClientTimeout(total=cfg.timeout),
    )
    if resp and resp.status in (200, 201):
        # The backend just returns a success message string, NOT the UUID.
        # We must fetch the rooms to find the newly created room's UUID.
        stats.rooms_created += 1
        
        rooms_data = await get_all_rooms(session, cfg, user, stats)
        if rooms_data and "rooms" in rooms_data:
            for key_str in rooms_data["rooms"].keys():
                if room_name in key_str:
                    import re
                    match = re.search(r'roomId=([0-9a-fA-F-]+)', key_str)
                    if match:
                        room_id = match.group(1)
                        logger.info("Created room %s -> %s", room_name, room_id)
                        return room_id
        
        logger.error("Could not find UUID for room %s after creation!", room_name)
        return None
    else:
        status = resp.status if resp else "no response"
        body = ""
        if resp:
            try:
                body = await resp.text()
            except Exception:
                pass
        logger.warning("Room creation failed for %s (status=%s, body=%s)", room_name, status, body[:200])
        return None


async def join_room_http(
    session: aiohttp.ClientSession,
    cfg: LoadTestConfig,
    user: UserContext,
    room_id: str,
    stats: Stats,
) -> bool:
    """POST /room/join/{roomId}"""
    headers = {"Authorization": f"Bearer {user.access_token}"}
    resp = await timed_request(
        session, "POST", f"{cfg.base_url}/room/join/{room_id}",
        stats, label="join_room",
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=cfg.timeout),
    )
    if resp and resp.status == 200:
        stats.rooms_joined += 1
        return True
    return False


async def get_all_rooms(
    session: aiohttp.ClientSession,
    cfg: LoadTestConfig,
    user: UserContext,
    stats: Stats,
):
    """GET /room/all"""
    headers = {"Authorization": f"Bearer {user.access_token}"}
    resp = await timed_request(
        session, "GET", f"{cfg.base_url}/room/all",
        stats, label="get_rooms",
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=cfg.timeout),
    )
    if resp and resp.status == 200:
        return await resp.json()
    return None


async def get_recent_messages(
    session: aiohttp.ClientSession,
    cfg: LoadTestConfig,
    user: UserContext,
    room_id: str,
    stats: Stats,
):
    """GET /rooms/{roomId}/messages/recent"""
    headers = {"Authorization": f"Bearer {user.access_token}"}
    resp = await timed_request(
        session, "GET", f"{cfg.base_url}/rooms/{room_id}/messages/recent",
        stats, label="get_messages",
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=cfg.timeout),
    )
    if resp and resp.status == 200:
        return await resp.json()
    return None


# ─────────────────────────────────────────────────────────────────────
# WebSocket interactions
# ─────────────────────────────────────────────────────────────────────

async def ws_user_session(
    cfg: LoadTestConfig,
    user: UserContext,
    room_ids: list,
    stats: Stats,
):
    """
    Connect via WebSocket, join rooms, send messages, listen for
    incoming messages for ws_listen_duration seconds.
    """
    ws_uri = f"{cfg.ws_url}/ws?token={user.access_token}"
    try:
        async with websockets.connect(
            ws_uri,
            open_timeout=cfg.timeout,
            close_timeout=cfg.timeout,
            max_size=2**20,
        ) as ws:
            stats.ws_connected += 1
            logger.debug("WS connected: %s", user.username)

            # Join all assigned rooms via WebSocket
            for rid in room_ids:
                join_msg = json.dumps({
                    "type": "JOIN_ROOM",
                    "roomId": rid,
                })
                await ws.send(join_msg)
                logger.debug("%s joined room %s via WS", user.username, rid)

            # Send messages
            for i in range(cfg.messages_per_user):
                target_room = random.choice(room_ids)
                chat_msg = json.dumps({
                    "type": "SEND_MESSAGE",
                    "content": f"[loadtest] {user.username} msg#{i} @ {time.time():.3f}",
                    "roomId": target_room,
                })
                t0 = time.perf_counter()
                await ws.send(chat_msg)
                elapsed = time.perf_counter() - t0
                stats.ws_messages_sent += 1
                stats.latencies.append(("ws_send", elapsed))

                await asyncio.sleep(cfg.message_delay)

            # Listen for incoming messages
            listen_until = time.time() + cfg.ws_listen_duration
            try:
                while time.time() < listen_until:
                    try:
                        msg = await asyncio.wait_for(
                            ws.recv(), timeout=min(2.0, max(0.1, listen_until - time.time()))
                        )
                        stats.ws_messages_received += 1
                        logger.debug("%s received: %s", user.username, msg[:80] if isinstance(msg, str) else str(msg)[:80])
                    except asyncio.TimeoutError:
                        continue
            except websockets.exceptions.ConnectionClosed:
                pass

    except Exception as e:
        stats.ws_errors += 1
        logger.warning("WS error for %s: %s", user.username, e)


# ─────────────────────────────────────────────────────────────────────
# Per-user complete lifecycle
# ─────────────────────────────────────────────────────────────────────

async def user_lifecycle(
    cfg: LoadTestConfig,
    user: UserContext,
    room_ids: list,
    stats: Stats,
    semaphore: asyncio.Semaphore,
):
    """
    Full lifecycle for one simulated user:
      register → login → join rooms (HTTP) → WS connect → chat → poll HTTP
    """
    async with semaphore:
        connector = aiohttp.TCPConnector(limit=0, force_close=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # 1. Register
            registered = await register_user(session, cfg, user, stats)
            if not registered:
                return
            await asyncio.sleep(cfg.think_time)

            # 2. Login
            logged_in = await login_user(session, cfg, user, stats)
            if not logged_in:
                return
            await asyncio.sleep(cfg.think_time)

            # 3. Join rooms via HTTP REST
            for rid in room_ids:
                await join_room_http(session, cfg, user, rid, stats)
                await asyncio.sleep(cfg.think_time * 0.5)

            # 4. Fetch room list (HTTP load)
            await get_all_rooms(session, cfg, user, stats)
            await asyncio.sleep(cfg.think_time)

            # 5. WebSocket session: join rooms, send & receive messages
            await ws_user_session(cfg, user, room_ids, stats)

            # 6. Post-WS: poll recent messages for each room (HTTP load)
            for rid in room_ids:
                await get_recent_messages(session, cfg, user, rid, stats)
                await asyncio.sleep(cfg.think_time * 0.5)


# ─────────────────────────────────────────────────────────────────────
# Mode orchestrators
# ─────────────────────────────────────────────────────────────────────

async def run_rooms_mode(cfg: LoadTestConfig, stats: Stats):
    """
    Create N rooms, assign M users per room.
    Total users = num_rooms × users_per_room.
    """
    total_users = cfg.num_rooms * cfg.users_per_room
    logger.info(
        "ROOMS MODE: %d rooms × %d users/room = %d total users",
        cfg.num_rooms, cfg.users_per_room, total_users,
    )

    # Generate users
    users = [
        UserContext(
            username=make_username(cfg.user_prefix, i),
            password=cfg.password,
        )
        for i in range(total_users)
    ]

    # Coordinator user creates rooms
    coordinator = UserContext(
        username=make_username(cfg.user_prefix + "coord_", 0),
        password=cfg.password,
    )
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        await register_user(session, cfg, coordinator, stats)
        await login_user(session, cfg, coordinator, stats)

        room_ids = []
        for r in range(cfg.num_rooms):
            room_name = f"{cfg.room_prefix}{r}_{random_string(4)}"
            # Create room with just the coordinator; users will join individually
            rid = await create_room(
                session, cfg, coordinator, room_name, [coordinator.username], stats,
            )
            if rid:
                room_ids.append(rid)
            else:
                logger.error("Failed to create room %s -- skipping", room_name)
                room_ids.append(None)

    # Assign rooms to users & run lifecycles
    semaphore = asyncio.Semaphore(cfg.max_concurrency)
    tasks = []
    for r in range(cfg.num_rooms):
        if room_ids[r] is None:
            continue
        start = r * cfg.users_per_room
        end = start + cfg.users_per_room
        for user in users[start:end]:
            user.room_ids = [room_ids[r]]
            tasks.append(
                user_lifecycle(cfg, user, user.room_ids, stats, semaphore)
            )

    logger.info("Launching %d user tasks...", len(tasks))
    await asyncio.gather(*tasks, return_exceptions=True)


async def run_concurrent_mode(cfg: LoadTestConfig, stats: Stats):
    """
    Flat N concurrent users, auto-distributed across auto_rooms rooms.
    """
    total = cfg.total_users
    num_rooms = cfg.auto_rooms
    logger.info(
        "CONCURRENT MODE: %d users across %d auto-created rooms",
        total, num_rooms,
    )

    users = [
        UserContext(
            username=make_username(cfg.user_prefix, i),
            password=cfg.password,
        )
        for i in range(total)
    ]

    # Coordinator creates rooms
    coordinator = UserContext(
        username=make_username(cfg.user_prefix + "coord_", 0),
        password=cfg.password,
    )
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        await register_user(session, cfg, coordinator, stats)
        await login_user(session, cfg, coordinator, stats)

        room_ids = []
        for r in range(num_rooms):
            room_name = f"{cfg.room_prefix}{r}_{random_string(4)}"
            # Create room with just the coordinator; users will join individually
            rid = await create_room(
                session, cfg, coordinator, room_name, [coordinator.username], stats,
            )
            if rid:
                room_ids.append(rid)

    if not room_ids:
        logger.error("No rooms created — aborting")
        return

    # Distribute users round-robin across rooms
    semaphore = asyncio.Semaphore(cfg.max_concurrency)
    tasks = []
    for i, user in enumerate(users):
        assigned = [room_ids[i % len(room_ids)]]
        tasks.append(user_lifecycle(cfg, user, assigned, stats, semaphore))

    logger.info("Launching %d user tasks...", len(tasks))
    await asyncio.gather(*tasks, return_exceptions=True)


async def run_ramp_mode(cfg: LoadTestConfig, stats: Stats):
    """
    Gradually ramp from 0 to total_users over ramp_duration seconds.
    """
    total = cfg.total_users
    duration = cfg.ramp_duration
    num_rooms = cfg.auto_rooms
    logger.info(
        "RAMP MODE: 0 -> %d users over %d seconds (%d rooms)",
        total, duration, num_rooms,
    )

    users = [
        UserContext(
            username=make_username(cfg.user_prefix, i),
            password=cfg.password,
        )
        for i in range(total)
    ]

    # Coordinator creates rooms up front
    coordinator = UserContext(
        username=make_username(cfg.user_prefix + "coord_", 0),
        password=cfg.password,
    )
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        await register_user(session, cfg, coordinator, stats)
        await login_user(session, cfg, coordinator, stats)

        room_ids = []
        for r in range(num_rooms):
            room_name = f"{cfg.room_prefix}{r}_{random_string(4)}"
            # Create room with just the coordinator; users will join individually
            rid = await create_room(
                session, cfg, coordinator, room_name, [coordinator.username], stats,
            )
            if rid:
                room_ids.append(rid)

    if not room_ids:
        logger.error("No rooms created — aborting")
        return

    semaphore = asyncio.Semaphore(cfg.max_concurrency)
    interval = duration / total if total > 0 else 0
    tasks = []

    for i, user in enumerate(users):
        assigned = [room_ids[i % len(room_ids)]]
        tasks.append(
            asyncio.ensure_future(
                _delayed_lifecycle(
                    i * interval, cfg, user, assigned, stats, semaphore
                )
            )
        )

    logger.info("Ramping %d users over %d seconds...", total, duration)
    await asyncio.gather(*tasks, return_exceptions=True)


async def _delayed_lifecycle(
    delay: float,
    cfg: LoadTestConfig,
    user: UserContext,
    room_ids: list,
    stats: Stats,
    semaphore: asyncio.Semaphore,
):
    await asyncio.sleep(delay)
    await user_lifecycle(cfg, user, room_ids, stats, semaphore)


# ─────────────────────────────────────────────────────────────────────
# CLI argument parsing
# ─────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="ChitChat Load Tester — stress test load balancer + backend",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # 10 rooms, 100 users per room (1,000 total users)
  python load_test.py rooms --num-rooms 10 --users-per-room 100

  # 1 room, 1000 users
  python load_test.py rooms --num-rooms 1 --users-per-room 1000

  # 1000 concurrent users, auto-distributed across 20 rooms
  python load_test.py concurrent --total-users 1000 --auto-rooms 20

  # 100000 concurrent users (extreme), 50 rooms
  python load_test.py concurrent --total-users 100000 --auto-rooms 50

  # Ramp from 0 to 5000 users over 120 seconds
  python load_test.py ramp --total-users 5000 --ramp-duration 120

  # Custom server URL
  python load_test.py concurrent --total-users 500 --base-url http://10.1.75.51:3285
        """,
    )

    # ── Sub-commands (modes) ──
    sub = p.add_subparsers(dest="mode", required=True, help="Load test mode")

    # -- rooms mode --
    rooms_p = sub.add_parser("rooms", help="N rooms x M users per room")
    rooms_p.add_argument("--num-rooms", type=int, default=10,
                         help="Number of rooms to create (default: 10)")
    rooms_p.add_argument("--users-per-room", type=int, default=100,
                         help="Users per room (default: 100)")

    # -- concurrent mode --
    conc_p = sub.add_parser("concurrent", help="Flat N concurrent users")
    conc_p.add_argument("--total-users", type=int, default=1000,
                        help="Total concurrent users (default: 1000)")
    conc_p.add_argument("--auto-rooms", type=int, default=10,
                        help="Number of rooms to auto-create (default: 10)")

    # -- ramp mode --
    ramp_p = sub.add_parser("ramp", help="Ramp 0->N users over T seconds")
    ramp_p.add_argument("--total-users", type=int, default=1000,
                        help="Target user count (default: 1000)")
    ramp_p.add_argument("--ramp-duration", type=int, default=60,
                        help="Ramp-up duration in seconds (default: 60)")
    ramp_p.add_argument("--auto-rooms", type=int, default=10,
                        help="Number of rooms to auto-create (default: 10)")

    # ── Global options (apply to all modes) ──
    for sub_parser in [rooms_p, conc_p, ramp_p]:
        sub_parser.add_argument("--base-url", type=str, default="http://10.1.75.51:3285",
                                help="Load balancer HTTP base URL")
        sub_parser.add_argument("--ws-url", type=str, default=None,
                                help="WebSocket base URL (default: derived from --base-url)")
        sub_parser.add_argument("--messages-per-user", type=int, default=5,
                                help="Messages each user sends (default: 5)")
        sub_parser.add_argument("--message-delay", type=float, default=0.5,
                                help="Delay between messages in seconds (default: 0.5)")
        sub_parser.add_argument("--think-time", type=float, default=0.2,
                                help="Pause between HTTP calls in seconds (default: 0.2)")
        sub_parser.add_argument("--ws-listen", type=int, default=10,
                                help="WebSocket listen duration in seconds (default: 10)")
        sub_parser.add_argument("--timeout", type=int, default=30,
                                help="HTTP/WS timeout in seconds (default: 30)")
        sub_parser.add_argument("--max-concurrency", type=int, default=500,
                                help="Max concurrent user coroutines (default: 500)")
        sub_parser.add_argument("--password", type=str, default="loadtest123",
                                help="Password for all test users (default: loadtest123)")
        sub_parser.add_argument("--verbose", "-v", action="store_true",
                                help="Enable debug logging")

    return p


def args_to_config(args) -> LoadTestConfig:
    ws_url = args.ws_url
    if ws_url is None:
        ws_url = args.base_url.replace("http://", "ws://").replace("https://", "wss://")

    cfg = LoadTestConfig(
        base_url=args.base_url,
        ws_url=ws_url,
        mode=args.mode,
        messages_per_user=args.messages_per_user,
        message_delay=args.message_delay,
        think_time=args.think_time,
        ws_listen_duration=args.ws_listen,
        timeout=args.timeout,
        password=args.password,
        max_concurrency=args.max_concurrency,
        verbose=args.verbose,
    )

    if args.mode == "rooms":
        cfg.num_rooms = args.num_rooms
        cfg.users_per_room = args.users_per_room
    elif args.mode == "concurrent":
        cfg.total_users = args.total_users
        cfg.auto_rooms = args.auto_rooms
    elif args.mode == "ramp":
        cfg.total_users = args.total_users
        cfg.ramp_duration = args.ramp_duration
        cfg.auto_rooms = args.auto_rooms

    return cfg


# ─────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────

async def main():
    parser = build_parser()
    args = parser.parse_args()
    cfg = args_to_config(args)

    # Logging
    level = logging.DEBUG if cfg.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    stats = Stats()

    banner_total = (
        cfg.num_rooms * cfg.users_per_room if cfg.mode == "rooms"
        else cfg.total_users
    )

    print(
        "\n"
        "╔══════════════════════════════════════════════════════════════╗\n"
        "║               ChitChat Load Tester v1.0                    ║\n"
        "╠══════════════════════════════════════════════════════════════╣\n"
        f"║  Mode          : {cfg.mode:<42} ║\n"
        f"║  Target        : {cfg.base_url:<42} ║\n"
        f"║  Total users   : {banner_total:<42} ║\n"
        f"║  Msgs/user     : {cfg.messages_per_user:<42} ║\n"
        "╚══════════════════════════════════════════════════════════════╝\n"
    )

    stats.start_time = time.time()

    if cfg.mode == "rooms":
        await run_rooms_mode(cfg, stats)
    elif cfg.mode == "concurrent":
        await run_concurrent_mode(cfg, stats)
    elif cfg.mode == "ramp":
        await run_ramp_mode(cfg, stats)

    stats.end_time = time.time()
    print(stats.summary())


if __name__ == "__main__":
    # On Windows, use SelectorEventLoop for compatibility with asyncio
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
