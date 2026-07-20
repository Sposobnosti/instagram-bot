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

import logging
import os
import random

import httpx
from fastapi import FastAPI, Request, Response

from config import (
    VERIFY_TOKEN,
    PAGE_ACCESS_TOKEN,
    IG_USER_ID,
    GRAPH_API_VERSION,
    AUTO_REPLY_DM_TEXT,
    COMMENT_REPLY_VARIANTS,
    text_matches_trigger,
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

    for entry in body.get("entry", []):
        # --- Direct-сообщения ---
        for messaging_event in entry.get("messaging", []):
            await handle_direct_message(messaging_event)

        # --- Комментарии под постами / рилс ---
        for change in entry.get("changes", []):
            if change.get("field") == "comments":
                await handle_comment(change.get("value", {}))

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

    text = message.get("text")
    if not text:
        return

    logger.info("Новое сообщение в Direct от %s: %s", sender_id, text)

    # Отвечаем только если текст похож на запрос "послания" / связанные
    # фразы — см. TRIGGER_KEYWORDS в config.py. Это защищает от того,
    # чтобы бот отвечал шаблоном на вообще любое сообщение в Direct.
    if not text_matches_trigger(text):
        logger.info("Сообщение не совпало с триггерами, пропускаем: %s", text)
        return

    reply_text = build_dm_reply(text)
    await send_direct_message(sender_id, reply_text)


def build_dm_reply(incoming_text: str) -> str:
    """
    В Direct используется один финальный текст (см. AUTO_REPLY_DM_TEXT
    в config.py) — там важна точная формулировка про очередь и ссылку.
    """
    return AUTO_REPLY_DM_TEXT


async def send_direct_message(recipient_id: str, text: str):
    url = f"{GRAPH_API_BASE}/{IG_USER_ID}/messages"
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": text},
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
        if resp.status_code != 200:
            logger.error("Ошибка отправки Direct-сообщения: %s", resp.text)
        else:
            logger.info("Ответ в Direct отправлен пользователю %s", recipient_id)


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

    # Явная защита от зацикливания: если комментарий оставлен самим
    # аккаунтом бота (например, это наш же ответ), не обрабатываем его,
    # независимо от того, что там написано.
    if from_user == IG_USER_ID:
        logger.info("Комментарий от самого бота, пропускаем: %s", text)
        return

    logger.info("Новый комментарий %s: %s", comment_id, text)

    # Как и в Direct — отвечаем только на комментарии, похожие на запрос
    # послания (включая нечёткие формулировки вроде "жду" или "что там
    # про меня"), а не на любой комментарий подряд.
    if not text_matches_trigger(text):
        logger.info("Комментарий не совпал с триггерами, пропускаем: %s", text)
        return

    reply_text = build_comment_reply(text, from_username)
    await reply_to_comment(comment_id, reply_text)

    # Кроме публичного ответа под комментарием, отправляем тому же
    # человеку полный текст в Direct — через специальный механизм Private
    # Reply (Meta позволяет написать в личку по ID комментария, даже если
    # переписки с этим человеком раньше не было).
    await send_private_reply_to_comment(comment_id, AUTO_REPLY_DM_TEXT)


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

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
        if resp.status_code != 200:
            logger.error("Ошибка ответа на комментарий: %s", resp.text)
        else:
            logger.info("Ответ на комментарий %s отправлен", comment_id)


async def send_private_reply_to_comment(comment_id: str, text: str):
    """
    Private Reply — отправляет сообщение в Direct тому, кто оставил
    комментарий, используя ID комментария вместо ID пользователя.
    Meta разрешает это даже без предварительной переписки, но только
    один раз на комментарий и в течение 7 дней с момента комментария.
    """
    url = f"{GRAPH_API_BASE}/{IG_USER_ID}/messages"
    payload = {
        "recipient": {"comment_id": comment_id},
        "message": {"text": text},
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload, headers=AUTH_HEADERS)
        if resp.status_code != 200:
            logger.error("Ошибка Private Reply на комментарий %s: %s", comment_id, resp.text)
        else:
            logger.info("Private Reply отправлен по комментарию %s", comment_id)


@app.get("/health")
async def health():
    return {"status": "ok"}
