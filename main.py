"""
Instagram Auto-Reply Bot (Direct messages + Comments)
Работает через официальный Instagram Graph API (Meta for Developers).

Требования:
- Instagram Business/Creator аккаунт, привязанный к Facebook-странице
- Приложение в Meta for Developers с продуктами: Instagram Graph API, Webhooks
- Page Access Token с правами: instagram_manage_messages, instagram_manage_comments

Запуск:
    uvicorn main:app --host 0.0.0.0 --port 8000

За HTTPS и проксирование запросов на этот порт отвечает ваш веб-сервер
(nginx / Beget панель) — см. README.md.
"""

import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, Response

from config import (
    VERIFY_TOKEN,
    PAGE_ACCESS_TOKEN,
    IG_USER_ID,
    GRAPH_API_VERSION,
    get_dm_text,
    COMMENT_REPLY_VARIANTS,
    REPLY_COOLDOWN_SECONDS,
    TELEGRAM_ALERT_BOT_TOKEN,
    TELEGRAM_ALERT_CHAT_ID,
    UPTIMEROBOT_API_KEY,
    CRON_SECRET,
    UPTIME_ALERT_COOLDOWN_SECONDS,
    RENDER_DEPLOY_HOOK_URL,
    match_trigger_group,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("instagram_bot")

app = FastAPI(title="Instagram Auto-Reply Bot")

# Временная диагностика — покажет в логах длину и начало/конец токена,
# чтобы проверить, не обрезался ли он при вставке в переменные окружения.
# Не раскрывает токен целиком.
if PAGE_ACCESS_TOKEN:
    _masked = f"{PAGE_ACCESS_TOKEN[:8]}...{PAGE_ACCESS_TOKEN[-6:]}"
    logger.info(
        "Диагностика токена: длина=%d символов, вид=%s",
        len(PAGE_ACCESS_TOKEN),
        _masked,
    )
else:
    logger.warning("IG_PAGE_ACCESS_TOKEN пустой!")

# Новый Instagram API с бизнес-логином работает через graph.instagram.com,
# а не через graph.facebook.com — это важно, иначе запросы будут падать
# с ошибками авторизации даже с правильным токеном.
GRAPH_API_BASE = f"https://graph.instagram.com/{GRAPH_API_VERSION}"
AUTH_HEADERS = {"Authorization": f"Bearer {PAGE_ACCESS_TOKEN}"}


# ---------------------------------------------------------------------------
# Защита от спама: не отвечаем одному и тому же человеку чаще, чем раз в
# REPLY_COOLDOWN_SECONDS (по умолчанию — 1 час), даже если он присылает
# триггерное слово много раз подряд. Хранится в памяти процесса — это
# сбрасывается при перезапуске сервиса, но для защиты от спама в течение
# дня этого достаточно и не требует внешней базы данных.
# ---------------------------------------------------------------------------
_last_reply_at: dict[str, float] = {}

# ---------------------------------------------------------------------------
# Дедупликация по ID события (mid сообщения / ID комментария). Meta может
# присылать одно и то же событие webhook повторно, если не получила
# быстрый ответ 200 OK вовремя — без этой проверки два одинаковых
# события, пришедшие почти одновременно, могли бы оба пройти проверку
# кулдауна (гонка) и оба отправить ответ (с разными случайными вариантами
# текста, отсюда и "два ответа на один и тот же триггер").
# ---------------------------------------------------------------------------
_processed_event_ids: dict[str, float] = {}

# ---------------------------------------------------------------------------
# Сохранение состояния (кулдаунов и обработанных ID) на диск — без этого
# перезапуск процесса (например, из-за временного сбоя авторизации у
# Meta) обнулял бы всю защиту от повторов, и накопленные повторные
# доставки webhook могли бы прорваться разом как "новые" сообщения.
# ---------------------------------------------------------------------------
STATE_FILE_PATH = Path(__file__).parent / "bot_state.json"
# Храним записи не дольше 30 дней, чтобы файл не рос бесконечно.
STATE_MAX_AGE_SECONDS = 30 * 24 * 3600


def _load_state() -> None:
    global _last_reply_at, _processed_event_ids
    if not STATE_FILE_PATH.exists():
        return
    try:
        with open(STATE_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        _last_reply_at.update(data.get("last_reply_at", {}))
        _processed_event_ids.update(data.get("processed_event_ids", {}))
        logging.getLogger("instagram_bot").info(
            "Состояние загружено с диска: %d кулдаунов, %d обработанных событий",
            len(_last_reply_at),
            len(_processed_event_ids),
        )
    except Exception as e:
        logging.getLogger("instagram_bot").warning("Не удалось загрузить состояние с диска: %s", e)


def _save_state() -> None:
    now = time.time()
    # Чистим устаревшие записи перед сохранением, чтобы файл не рос вечно.
    _last_reply_at_pruned = {k: v for k, v in _last_reply_at.items() if now - v < STATE_MAX_AGE_SECONDS}
    _processed_event_ids_pruned = {k: v for k, v in _processed_event_ids.items() if now - v < STATE_MAX_AGE_SECONDS}
    _last_reply_at.clear()
    _last_reply_at.update(_last_reply_at_pruned)
    _processed_event_ids.clear()
    _processed_event_ids.update(_processed_event_ids_pruned)

    try:
        with open(STATE_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_reply_at": _last_reply_at,
                    "processed_event_ids": _processed_event_ids,
                },
                f,
            )
    except Exception as e:
        logging.getLogger("instagram_bot").warning("Не удалось сохранить состояние на диск: %s", e)


_load_state()


def already_processed(event_id: str) -> bool:
    return event_id in _processed_event_ids


def mark_processed(event_id: str) -> None:
    _processed_event_ids[event_id] = time.time()
    _save_state()


def is_on_cooldown(user_id: str, cooldown_seconds: int = REPLY_COOLDOWN_SECONDS) -> bool:
    last = _last_reply_at.get(user_id)
    if last is None:
        return False
    return (time.time() - last) < cooldown_seconds


def mark_replied(user_id: str) -> None:
    _last_reply_at[user_id] = time.time()
    _save_state()


# ---------------------------------------------------------------------------
# 1. Верификация webhook (Meta делает GET-запрос при подключении в кабинете)
# ---------------------------------------------------------------------------
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        logger.info("Webhook успешно верифицирован")
        return Response(content=challenge, media_type="text/plain")

    logger.warning("Верификация webhook не пройдена: неверный токен")
    return Response(status_code=403)


# ---------------------------------------------------------------------------
# 2. Приём событий (новые сообщения Direct и комментарии)
# ---------------------------------------------------------------------------
@app.post("/webhook")
async def receive_webhook(request: Request):
    body = await request.json()
    logger.info("Получено событие: %s", body)

    try:
        for entry in body.get("entry", []):
            # --- Direct-сообщения ---
            for messaging_event in entry.get("messaging", []):
                await handle_direct_message(messaging_event)

            # --- Комментарии под постами / рилс ---
            for change in entry.get("changes", []):
                if change.get("field") == "comments":
                    await handle_comment(change.get("value", {}))
    except Exception as e:
        # Любая непредвиденная ошибка при обработке НЕ должна приводить
        # к ответу 500 — иначе Meta сочтёт доставку неудачной и пришлёт
        # то же самое событие повторно, что может привести к повторной
        # отправке ответов пользователю. Логируем и всё равно отвечаем 200.
        logger.error("Необработанная ошибка при обработке webhook-события: %s", e, exc_info=True)

    # Meta требует ответ 200 в течение нескольких секунд
    return Response(status_code=200)


# ---------------------------------------------------------------------------
# Обработка Direct-сообщений
# ---------------------------------------------------------------------------
async def handle_direct_message(event: dict):
    sender_id = event.get("sender", {}).get("id")
    message = event.get("message", {})

    # Игнорируем служебные события (эхо собственных сообщений и т.п.)
    if not sender_id or message.get("is_echo"):
        return

    # Защита от повторной доставки одного и того же webhook-события от
    # Meta — проверяем сразу, до любой другой логики.
    mid = message.get("mid")
    if mid and already_processed(mid):
        logger.info("Сообщение %s уже обработано (повторная доставка), пропускаем", mid)
        return

    text = message.get("text")
    if not text:
        return

    logger.info("Новое сообщение в Direct от %s: %s", sender_id, text)

    # Определяем, к какой группе триггеров относится текст (послание /
    # способности / чакры), и отвечаем только если сработала хотя бы одна.
    # Это защищает от того, чтобы бот отвечал шаблоном на любое сообщение.
    group = match_trigger_group(text)
    if not group:
        logger.info("Сообщение не совпало с триггерами, пропускаем: %s", text)
        return

    if mid:
        mark_processed(mid)

    # Кулдаун считается отдельно для каждой темы (послание/способности/
    # чакры) — если человек уже получил ответ на "послание", это не
    # мешает ему сразу же получить ответ на "чакры" или "способности".
    cooldown_key = f"{sender_id}:{group}"
    if is_on_cooldown(cooldown_key):
        logger.info("Пользователь %s на кулдауне по теме %s, повторный ответ пропущен", sender_id, group)
        return

    await send_typing_indicator(sender_id)
    await send_direct_message(sender_id, build_dm_message(group))
    mark_replied(cooldown_key)


def build_dm_message(group: str) -> dict:
    """
    Собирает тело сообщения для Direct под конкретную группу триггера —
    случайный вариант текста из DM_TEXT_VARIANTS в config.py, чтобы при
    пересылках между друзьями сообщения не были дословно одинаковыми.
    Button Template (кнопка вместо текстовой ссылки) не рендерится для
    Instagram API с Instagram Login — Instagram показывает его как
    обычный текст без кнопки. Поэтому используется простой текст со
    ссылкой — Instagram сам делает такие ссылки кликабельными в Direct.
    """
    return {"text": get_dm_text(group)}


async def send_typing_indicator(recipient_id: str):
    """
    Отправляет статус "печатает..." и делает небольшую паузу перед
    реальным ответом — так ответ выглядит более похожим на живого
    человека, а не на мгновенный автоответ бота.
    """
    url = f"{GRAPH_API_BASE}/{IG_USER_ID}/messages"
    payload = {
        "recipient": {"id": recipient_id},
        "sender_action": "typing_on",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
            if resp.status_code != 200:
                # Не критично, если индикатор не отправился — просто продолжаем
                # без него, не блокируя основной ответ.
                logger.warning("Не удалось отправить индикатор печати: %s", resp.text)
    except httpx.HTTPError as e:
        # Сетевая ошибка (таймаут и т.п.) не должна ронять весь обработчик
        # webhook — иначе Meta получит 500 и будет повторно слать то же
        # событие, что приводит к дублирующим ответам.
        logger.warning("Сетевая ошибка при отправке индикатора печати: %s", e)

    await asyncio.sleep(random.uniform(1.5, 2.5))


async def send_direct_message(recipient_id: str, message: dict):
    url = f"{GRAPH_API_BASE}/{IG_USER_ID}/messages"
    payload = {
        "recipient": {"id": recipient_id},
        "message": message,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
            if resp.status_code != 200:
                logger.error("Ошибка отправки Direct-сообщения: %s", resp.text)
            else:
                logger.info("Ответ в Direct отправлен пользователю %s", recipient_id)
    except httpx.HTTPError as e:
        # Сетевая ошибка не должна ронять обработчик webhook — иначе Meta
        # получит 500 и повторно пришлёт то же событие, вызывая дубли.
        logger.error("Сетевая ошибка при отправке Direct-сообщения: %s", e)


# ---------------------------------------------------------------------------
# Обработка комментариев
# ---------------------------------------------------------------------------
async def handle_comment(value: dict):
    comment_id = value.get("id")
    text = value.get("text")
    from_data = value.get("from", {})
    from_user = from_data.get("id")
    from_username = from_data.get("username")

    if not comment_id or not text:
        return

    # Защита от повторной доставки одного и того же webhook-события от
    # Meta — проверяем сразу, до остальной логики.
    if already_processed(comment_id):
        logger.info("Комментарий %s уже обработан (повторная доставка), пропускаем", comment_id)
        return

    # Явная защита от зацикливания: если комментарий оставлен самим
    # аккаунтом бота (например, это наш же ответ), не обрабатываем его,
    # независимо от того, что там написано.
    if from_user == IG_USER_ID:
        logger.info("Комментарий от самого бота, пропускаем: %s", text)
        return

    logger.info("Новый комментарий %s: %s", comment_id, text)

    # Как и в Direct — отвечаем только на комментарии, похожие на запрос
    # послания/способностей/чакр (включая нечёткие формулировки), а не на
    # любой комментарий подряд.
    group = match_trigger_group(text)
    if not group:
        logger.info("Комментарий не совпал с триггерами, пропускаем: %s", text)
        return

    mark_processed(comment_id)

    # Кулдаун считается отдельно для каждой темы — комментарий про "чакры"
    # не блокируется предыдущим ответом на "послание" от того же человека.
    cooldown_key = f"{from_user}:{group}"
    if is_on_cooldown(cooldown_key):
        logger.info("Пользователь %s на кулдауне по теме %s, повторный ответ на комментарий пропущен", from_user, group)
        return

    reply_text = build_comment_reply(text, from_username)
    await reply_to_comment(comment_id, reply_text)
    mark_replied(cooldown_key)

    # Кроме публичного ответа под комментарием, отправляем тому же
    # человеку полный текст в Direct — через специальный механизм Private
    # Reply (Meta позволяет написать в личку по ID комментария, даже если
    # переписки с этим человеком раньше не было).
    await send_private_reply_to_comment(comment_id, build_dm_message(group))


def build_comment_reply(incoming_text: str, username: str | None = None) -> str:
    """
    Случайно выбирает один из нескольких вариантов ответа (см.
    COMMENT_REPLY_VARIANTS в config.py), чтобы под разными постами не
    повторялся один и тот же текст — это не выглядит как спам-бот.
    Если известен username автора комментария, добавляет упоминание
    @username в начало — тогда человек получает уведомление, что ему
    ответили, а не просто видит безадресный комментарий.
    """
    reply = random.choice(COMMENT_REPLY_VARIANTS)
    if username:
        reply = f"@{username} {reply}"
    return reply


async def reply_to_comment(comment_id: str, text: str):
    url = f"{GRAPH_API_BASE}/{comment_id}/replies"
    payload = {"message": text}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
            if resp.status_code != 200:
                logger.error("Ошибка ответа на комментарий: %s", resp.text)
            else:
                logger.info("Ответ на комментарий %s отправлен", comment_id)
    except httpx.HTTPError as e:
        logger.error("Сетевая ошибка при ответе на комментарий %s: %s", comment_id, e)


async def send_private_reply_to_comment(comment_id: str, message: dict):
    """
    Private Reply — отправляет сообщение в Direct тому, кто оставил
    комментарий, используя ID комментария вместо ID пользователя.
    Meta разрешает это даже без предварительной переписки, но только
    один раз на комментарий и в течение 7 дней с момента комментария.
    """
    url = f"{GRAPH_API_BASE}/{IG_USER_ID}/messages"
    payload = {
        "recipient": {"comment_id": comment_id},
        "message": message,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
            if resp.status_code != 200:
                logger.error("Ошибка Private Reply на комментарий %s: %s", comment_id, resp.text)
            else:
                logger.info("Private Reply отправлен по комментарию %s", comment_id)
    except httpx.HTTPError as e:
        logger.error("Сетевая ошибка при Private Reply на комментарий %s: %s", comment_id, e)


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    # Временный переключатель для тестирования мониторинга — если
    # включён (переменная окружения FORCE_HEALTH_DOWN=true), health-check
    # намеренно отдаёт ошибку 500, имитируя реальное падение сервиса,
    # хотя сам процесс продолжает работать (и может отправить Telegram-
    # алерт). Не забудьте выключить после теста.
    if os.getenv("FORCE_HEALTH_DOWN", "").lower() == "true":
        return Response(status_code=500, content='{"status":"forced_down_for_testing"}')
    return {"status": "ok"}


async def send_telegram_alert(text: str):
    if not TELEGRAM_ALERT_BOT_TOKEN or not TELEGRAM_ALERT_CHAT_ID:
        logger.warning("TELEGRAM_ALERT_BOT_TOKEN/TELEGRAM_ALERT_CHAT_ID не настроены, алерт не отправлен")
        return

    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_ALERT_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_ALERT_CHAT_ID,
        "text": f"⚠️ {text}",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(telegram_url, json=payload)
        if resp.status_code != 200:
            logger.error("Не удалось отправить алерт в Telegram: %s", resp.text)
        else:
            logger.info("Алерт отправлен в Telegram")


async def check_token_health() -> bool:
    """
    Проверяет, что IG_PAGE_ACCESS_TOKEN всё ещё рабочий — делает лёгкий
    запрос к Graph API, не влияющий ни на что (просто читает базовую
    информацию об аккаунте). Возвращает True, если токен в порядке.
    """
    url = f"{GRAPH_API_BASE}/{IG_USER_ID}"
    params = {"fields": "id"}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params, headers=AUTH_HEADERS)
    except httpx.HTTPError as e:
        logger.warning("Сетевая ошибка при проверке токена: %s", e)
        return True  # Не считаем сетевой сбой поводом слать алерт про токен

    if resp.status_code == 200:
        return True

    logger.error("Проверка токена не прошла: %s %s", resp.status_code, resp.text)
    return False


@app.get("/alerts/check-uptime")
async def check_uptime(request: Request):
    """
    Вызывается по расписанию внешним cron-сервисом (например,
    cron-job.org каждые 5-10 минут). Сам спрашивает статус монитора
    через бесплатный UptimeRobot API — и если монитор "упал", шлёт
    алерт в Telegram. Не зависит от платных Alert Contacts UptimeRobot.
    Заодно проверяет, не протух ли IG_PAGE_ACCESS_TOKEN — эта проблема
    не показывается в обычном /health, но реально ломает ответы бота.
    """
    secret = request.query_params.get("secret")
    if not CRON_SECRET or secret != CRON_SECRET:
        logger.warning("Отклонён запрос на /alerts/check-uptime — неверный или отсутствующий secret")
        return Response(status_code=403)

    # --- Проверка токена Instagram ---
    token_ok = await check_token_health()
    if not token_ok:
        if not is_on_cooldown("token_alert", cooldown_seconds=UPTIME_ALERT_COOLDOWN_SECONDS):
            await send_telegram_alert(
                "Токен Instagram (IG_PAGE_ACCESS_TOKEN) больше не работает!\n\n"
                "Сгенерируйте новый: Meta for Developers → Instagram business login → "
                "Generate token, затем обновите переменную IG_PAGE_ACCESS_TOKEN на Render."
            )
            mark_replied("token_alert")

    if not UPTIMEROBOT_API_KEY:
        logger.warning("UPTIMEROBOT_API_KEY не настроен, проверка пропущена")
        return {"status": "skipped", "reason": "no api key", "token_ok": token_ok}

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            "https://api.uptimerobot.com/v2/getMonitors",
            data={
                "api_key": UPTIMEROBOT_API_KEY,
                "format": "json",
            },
        )

    if resp.status_code != 200:
        logger.error("Ошибка запроса к UptimeRobot API: %s", resp.text)
        return {"status": "error", "detail": "uptimerobot api request failed", "token_ok": token_ok}

    data = resp.json()
    monitors = data.get("monitors", [])

    # Статусы UptimeRobot: 2 = up, 8 = seems down, 9 = down.
    down_monitors = [m for m in monitors if m.get("status") in (8, 9)]

    if not down_monitors:
        return {"status": "ok", "monitors_checked": len(monitors), "token_ok": token_ok}

    if is_on_cooldown("uptime_alert", cooldown_seconds=UPTIME_ALERT_COOLDOWN_SECONDS):
        logger.info("Алерт об аптайме на кулдауне, повторно не отправляем")
        return {"status": "down_but_on_cooldown", "token_ok": token_ok}

    names = ", ".join(m.get("friendly_name", "monitor") for m in down_monitors)
    alert_text = f"Сервис недоступен: {names}"
    if RENDER_DEPLOY_HOOK_URL:
        alert_text += f"\n\n🔄 Перезапустить:\n{RENDER_DEPLOY_HOOK_URL}"
    await send_telegram_alert(alert_text)
    mark_replied("uptime_alert")

    return {"status": "alert_sent", "down_monitors": names}
