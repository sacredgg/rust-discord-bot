import asyncio
import os
import socket
import time
from urllib.parse import quote

import a2s
import aiohttp
import discord
from a2s.info import SourceInfo
from aiohttp import web
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
BATTLEMETRICS_TOKEN = os.getenv("BATTLEMETRICS_TOKEN", "")
STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")
QUERY_TIMEOUT = float(os.getenv("QUERY_TIMEOUT", "1"))
QUERY_ANSWER_LIMIT = float(os.getenv("QUERY_ANSWER_LIMIT", "2"))
CACHE_TTL = float(os.getenv("CACHE_TTL", "8"))
WEB_PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")))

DEFAULT_PORT = 28015
RUST_QUERY_PORT = 28016

GREEN = 0x57F287
RED = 0xED4245
GRAY = 0x99AAB5

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


def parse_address(raw: str) -> tuple[str, int]:
    text = raw.strip()
    if text.lower().startswith("connect "):
        text = text[8:]
    text = text.split(";")[0].strip()
    if ":" in text:
        host, _, port_str = text.rpartition(":")
    else:
        host, port_str = text, ""
    if not host:
        raise ValueError("Пустой адрес")
    if port_str:
        if not port_str.isdigit() or not (1 <= int(port_str) <= 65535):
            raise ValueError(f"Неверный порт: {port_str}")
        port = int(port_str)
    else:
        port = DEFAULT_PORT
    return host, port


def _a2s_one(host: str, port: int):
    return a2s.info((host, port), timeout=QUERY_TIMEOUT)


async def first_success(tasks) -> object:
    """Возвращает результат первой успешной корутины, иначе бросает последнюю ошибку."""
    pending = [asyncio.ensure_future(t) for t in tasks]
    last_error = None
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                if fut.cancelled():
                    continue
                try:
                    result = fut.result()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_error = exc
                    continue
                for p in pending:
                    p.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                return result
            if not pending:
                break
        raise last_error if last_error else TimeoutError("нет ответа")
    except BaseException:
        for p in pending:
            p.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        raise


async def _a2s_attempt(host: str, port: int):
    """Параллельно пробует несколько query-портов, возвращает первый ответ."""
    ports = dict.fromkeys([port, port + 1, port + 2, port + 10])

    async def one(p):
        info = await asyncio.to_thread(_a2s_one, host, p)
        info.used_port = p
        return info

    return await first_success([one(p) for p in ports])


def _source_info_from_server(server: dict, game_port: int) -> SourceInfo:
    info = SourceInfo(
        protocol=17,
        server_name=server.get("name", "") or "",
        map_name=server.get("map", "") or "",
        folder="rust",
        game="Rust",
        app_id=252490,
        player_count=int(server.get("players") or 0),
        max_players=int(server.get("max_players") or 0),
        bot_count=int(server.get("bots") or 0),
        server_type="d",
        platform=server.get("os", "") or "",
        password_protected=bool(int(server.get("password") or 0)),
        vac_enabled=bool(int(server.get("secure") or 0)),
        version=server.get("version", "") or "",
        edf=0,
        ping=0.0,
    )
    info.used_port = int(server.get("gameport") or 0) or game_port
    info.real_addr = server.get("addr", "")
    return info


async def _steam_addr_query(addr: str, game_port: int):
    import aiohttp

    filter_str = f"\\appid\\252490\\addr\\{addr}"
    url = (
        "https://api.steampowered.com/IGameServersService/GetServerList/v1/"
        f"?key={STEAM_API_KEY}&limit=5&filter={quote(filter_str)}"
    )
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    servers = (data.get("response", {}) or {}).get("servers") or []
    if not servers:
        raise LookupError(f"Сервер {addr} не найден в Steam API")
    server = None
    for s in servers:
        if int(s.get("gameport") or 0) == game_port:
            server = s
            break
    if server is None:
        server = servers[0]
    return _source_info_from_server(server, game_port)


async def fetch_steam_by_ip(host: str, port: int):
    """Последняя попытка: редирект-хост не отвечает напрямую.

    Сначала ищем сервер по IP (DNS хоста совпадает с реальным адресом).
    Если IP не совпал (типично для редиректов вида «3.magicrust.gg»),
    ищем по имени: слова из домена хоста + номер, например
    «3.magicrust.gg» -> сервер, в названии которого есть «magicrust» и «#3».
    """
    if not STEAM_API_KEY:
        return None
    try:
        import ipaddress
        import re

        raw_host = host
        host_is_hostname = False
        num = None
        try:
            ipaddress.ip_address(host)
        except ValueError:
            host_is_hostname = True
            num = re.search(r"\d+", host.split(".")[0])
            try:
                host = socket.gethostbyname(host)
            except Exception:
                host = None

        servers = await fetch_rust_snapshot()
        matches = (
            [s for s in servers if (s.get("addr") or "").split(":")[0] == host]
            if host else []
        )

        if not matches and host_is_hostname:
            labels = raw_host.lower().split(".")[:-1]
            words = set()
            for lbl in labels:
                for w in re.split(r"\W+", lbl):
                    if w and not w.isdigit():
                        words.add(w)
            if words:
                matches = []
                for s in servers:
                    name = (s.get("name") or "").lower()
                    flat = name.replace(" ", "")
                    if all((w in name) or (w in flat) for w in words):
                        matches.append(s)

        if not matches:
            return None

        def _rank(s: dict) -> tuple:
            name = (s.get("name") or "").lower()
            score = 0
            if host_is_hostname and num:
                n = int(num.group())
                if re.search(rf"#\s*{n}\b", name):
                    score += 2
                elif re.search(rf"(?<!\d){n}(?!\d)", name):
                    score += 1
            return (score, s.get("players") or 0)

        server = max(matches, key=_rank)
        return _source_info_from_server(server, port)
    except Exception:
        return None


async def fetch_steam_server(host: str, port: int):
    """Запрос статистики через Steam Web API (HTTPS). Работает, даже если UDP заблокирован."""
    if not STEAM_API_KEY:
        return None
    ports = dict.fromkeys([port, port + 1, port + 2, port + 10])
    try:
        return await first_success(
            [_steam_addr_query(f"{host}:{p}", port) for p in ports]
        )
    except Exception:
        return None


_SNAPSHOT_TTL = 120
_snapshot_cache: dict = {"ts": 0.0, "servers": []}


async def fetch_rust_snapshot() -> list:
    """Список Rust-серверов из Steam API (кэшируется)."""
    now = time.monotonic()
    if _snapshot_cache["servers"] and now - _snapshot_cache["ts"] < _SNAPSHOT_TTL:
        return _snapshot_cache["servers"]
    try:
        import aiohttp

        url = (
            "https://api.steampowered.com/IGameServersService/GetServerList/v1/"
            f"?key={STEAM_API_KEY}&limit=10000&filter=%5Cappid%5C252490"
        )
        timeout = aiohttp.ClientTimeout(total=25)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return _snapshot_cache["servers"]
                data = await resp.json()
        servers = (data.get("response", {}) or {}).get("servers") or []
        _snapshot_cache["ts"] = now
        _snapshot_cache["servers"] = servers
        return servers
    except Exception:
        return _snapshot_cache["servers"]


async def fetch_queue(host: str, port: int) -> int | None:
    if not BATTLEMETRICS_TOKEN:
        return None
    try:
        import aiohttp

        params = {"filter[search]": f"{host}:{port}", "page[size]": "1"}
        headers = {
            "Authorization": f"Bearer {BATTLEMETRICS_TOKEN}",
            "User-Agent": "rust-discord-bot/1.0",
        }
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                "https://api.battlemetrics.com/servers", params=params, headers=headers
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        server = (data.get("data") or [None])[0]
        if not server:
            return None
        attrs = server.get("attributes", {})
        details = attrs.get("details", {}) or {}
        for key in ("queue", "queueLength", "rust_queue", "playersInQueue"):
            value = details.get(key, attrs.get(key))
            if isinstance(value, int) and value > 0:
                return value
        return None
    except Exception:
        return None


_result_cache: dict = {}


async def query_server(host: str, port: int) -> tuple[object, int | None, str]:
    """Параллельно спрашивает A2S и Steam API, возвращает первый успешный ответ.

    Результат кэшируется на CACHE_TTL секунд — повторные запросы и кнопка
    «Обновить» отвечают мгновенно. Общий лимит ожидания — QUERY_ANSWER_LIMIT.
    Если адрес не отвечает (редирект), ищет реальный сервер по IP в Steam.
    """
    key = (host, port)
    now = time.monotonic()
    cached = _result_cache.get(key)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1], None, cached[2]

    tasks = {"a2s": asyncio.ensure_future(_a2s_attempt(host, port))}
    snapshot_fut = None
    if STEAM_API_KEY:
        tasks["steam"] = asyncio.ensure_future(fetch_steam_server(host, port))
        snapshot_fut = asyncio.ensure_future(fetch_rust_snapshot())

    pending = set(tasks.values())
    last_error = None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + QUERY_ANSWER_LIMIT

    def finish(info, source):
        if snapshot_fut and not snapshot_fut.done():
            snapshot_fut.cancel()
        return info, source

    while pending:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        done, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED, timeout=remaining
        )
        for fut in done:
            try:
                info = fut.result()
            except Exception as exc:
                last_error = exc
                continue
            if info is not None:
                for p in pending:
                    p.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                source = next(name for name, t in tasks.items() if t is fut)
                _result_cache[key] = (now, info, source)
                finish(info, source)
                queue = await fetch_queue(host, port)
                return info, queue, source
    for p in pending:
        p.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    if snapshot_fut and not snapshot_fut.done():
        try:
            await asyncio.wait_for(asyncio.shield(snapshot_fut), timeout=QUERY_ANSWER_LIMIT)
        except (asyncio.TimeoutError, Exception):
            pass

    info = await fetch_steam_by_ip(host, port)
    if info is not None:
        _result_cache[key] = (time.monotonic(), info, "redirect")
        queue = await fetch_queue(host, port)
        return info, queue, "redirect"

    raise last_error if last_error else TimeoutError("нет ответа")


def build_online_embed(host: str, port: int, info, queue: int | None, source: str = "a2s") -> discord.Embed:
    embed = discord.Embed(title=info.server_name or "Rust-сервер", color=GREEN)
    players = f"**{info.player_count} / {info.max_players}**"
    if queue:
        players += f"\nОчередь: **{queue}** 👥"
    embed.add_field(name="Статус", value="🟢 Онлайн", inline=True)
    embed.add_field(name="Игроки", value=players, inline=True)
    ping = getattr(info, "ping", None)
    embed.add_field(
        name="Пинг", value=f"{ping * 1000:.0f} мс" if ping else "—", inline=True
    )
    embed.add_field(name="Карта", value=info.map_name or "—", inline=True)
    embed.add_field(name="Версия", value=info.version or "—", inline=True)
    embed.add_field(name="Пароль", value="🔒 Да" if info.password_protected else "🔓 Нет", inline=True)
    connect_value = f"`connect {host}:{port}`\nЗапрос по порту **{info.used_port}**"
    real_addr = getattr(info, "real_addr", "")
    if source == "redirect" and real_addr:
        real_host, _, real_port = real_addr.rpartition(":")
        connect_value += f"\nРеальный сервер: `connect {real_host}:{info.used_port or real_port}`"
    embed.add_field(name="Подключение", value=connect_value, inline=False)
    embed.set_footer(
        text={
            "a2s": "Данные: A2S-запрос",
            "steam": "Данные: Steam Web API",
            "redirect": "Данные: Steam Web API (найден по IP редиректа)",
        }.get(source, "Данные: Steam Web API")
    )
    embed.timestamp = discord.utils.utcnow()
    return embed


def build_offline_embed(host: str, port: int) -> discord.Embed:
    embed = discord.Embed(title="Сервер оффлайн", color=RED)
    embed.description = f"**{host}:{port}** не отвечает на запрос.\nПроверь адрес или то, что сервер запущен."
    embed.add_field(name="Подключение", value=f"`connect {host}:{port}`", inline=False)
    embed.set_footer(text="Данные получены прямым A2S-запросом")
    embed.timestamp = discord.utils.utcnow()
    return embed


def build_error_embed(host: str, port: int, detail: str) -> discord.Embed:
    embed = discord.Embed(title="Не удалось обработать адрес", color=RED)
    embed.description = f"**{host}:{port}** — {detail}"
    return embed


class ServerView(discord.ui.View):
    def __init__(self, host: str, port: int):
        super().__init__(timeout=300)
        self.host = host
        self.port = port

    @discord.ui.button(label="Обновить", style=discord.ButtonStyle.primary, emoji="🔄")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        button.disabled = True
        await interaction.edit_original_response(
            content="Обновляю данные…", embed=None, view=self
        )
        try:
            info, queue, source = await query_server(self.host, self.port)
            embed = build_online_embed(self.host, self.port, info, queue, source)
        except Exception:
            embed = build_offline_embed(self.host, self.port)
        button.disabled = False
        await interaction.edit_original_response(
            content=None, embed=embed, view=self
        )


@bot.tree.command(name="server", description="Показать статистику Rust-сервера")
@app_commands.describe(
    address="Адрес сервера, например: 1.2.3.4:28015 или connect 1.2.3.4:28015"
)
async def server(interaction: discord.Interaction, address: str):
    await interaction.response.defer()
    try:
        host, port = parse_address(address)
    except ValueError as exc:
        embed = discord.Embed(title="Неверный адрес", color=RED)
        embed.description = (
            f"Не удалось распознать адрес **{address}** ({exc}).\n"
            "Примеры: `1.2.3.4:28015`, `my-server.ru:28015`, `connect 1.2.3.4:28015`"
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return

    try:
        info, queue, source = await query_server(host, port)
        embed = build_online_embed(host, port, info, queue, source)
    except Exception:
        embed = build_offline_embed(host, port)

    view = ServerView(host, port)
    await interaction.followup.send(embed=embed, view=view)


@bot.tree.command(name="search", description="Найти Rust-сервер по названию")
@app_commands.describe(query="Часть названия, например: Magic Rust")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()
    servers = await fetch_rust_snapshot()
    q = query.lower()
    matches = [s for s in servers if q in (s.get("name") or "").lower()][:10]
    if not matches:
        embed = discord.Embed(title="Ничего не найдено", color=RED)
        embed.description = f"По запросу **{query}** серверов не найдено."
        await interaction.followup.send(embed=embed)
        return
    lines = []
    for i, s in enumerate(matches, 1):
        name = (s.get("name") or "").strip()[:80]
        gameport = int(s.get("gameport") or 0)
        host = (s.get("addr") or "").split(":")[0]
        connect = f"{host}:{gameport}" if gameport else s.get("addr", "")
        players = f"{s.get('players')}/{s.get('max_players')}"
        lines.append(f"**{i}.** `{connect}` — {name} — *{players}*")
    embed = discord.Embed(title=f"Результаты: «{query}»", color=GREEN)
    embed.description = "\n".join(lines)
    embed.set_footer(text="Указан игровой адрес: connect адрес:порт")
    await interaction.followup.send(embed=embed)


@bot.event
async def on_ready():
    print(f"Бот запущен как {bot.user} (ID: {bot.user.id})")
    try:
        await bot.tree.sync()
        print("Слэш-команды синхронизированы")
    except Exception as exc:
        print(f"Не удалось синхронизировать команды: {exc}")


async def handle_root(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "bot": str(bot.user)})


async def keep_awake():
    """Пингует собственный домен, чтобы бесплатный хостинг не «усыплял» бота."""
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if not domain:
        return
    url = domain if domain.startswith("http") else f"https://{domain}"
    print(f"Keep-awake: пингую {url} каждые 5 минут")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    print(f"Keep-awake: пинг {resp.status}")
        except Exception as exc:
            print(f"Keep-awake: ошибка {exc}")
        await asyncio.sleep(300)


async def main():
    web_app = web.Application()
    web_app.router.add_get("/", handle_root)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEB_PORT)
    await site.start()
    print(f"Web-сервер запущен на порту {WEB_PORT}")

    asyncio.create_task(keep_awake())

    await bot.start(DISCORD_TOKEN)


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Ошибка: DISCORD_TOKEN не задан. Скопируй .env.example в .env и укажи токен.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
