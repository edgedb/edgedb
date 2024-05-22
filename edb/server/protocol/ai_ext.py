#
# This source file is part of the EdgeDB open source project.
#
# Copyright MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    NoReturn,
    Optional,
    TYPE_CHECKING,
)

import asyncio
import contextlib
import contextvars
import http
import itertools
import json
import logging

import httpx
import httpx_sse

from edb import errors
from edb.common import asyncutil
from edb.common import debug
from edb.common import markup
from edb.common import uuidgen

from edb.server import compiler
from edb.server.compiler import sertypes
from edb.server.protocol import execute

if TYPE_CHECKING:
    from edb.server import dbview
    from edb.server import tenant as srv_tenant
    from edb.server import pgcon
    from edb.server.protocol import protocol


logger = logging.getLogger("edb.server.ai_ext")


class AIExtError(Exception):
    http_status: ClassVar[http.HTTPStatus] = (
        http.HTTPStatus.INTERNAL_SERVER_ERROR)

    def __init__(
        self,
        *args: object,
        json: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(*args)
        self._json = json

    def get_http_status(self) -> http.HTTPStatus:
        return self.__class__.http_status

    def json(self) -> dict[str, Any]:
        if self._json is not None:
            return self._json
        else:
            return {
                "message": str(self.args[0]),
                "type": self.__class__.__name__,
            }


class AIProviderError(AIExtError):
    pass


class ConfigurationError(AIExtError):
    pass


class InternalError(AIExtError):
    pass


class BadRequestError(AIExtError):
    http_status = http.HTTPStatus.BAD_REQUEST


def start_extension(
    tenant: srv_tenant.Tenant,
    dbname: str,
) -> None:
    task_name = _get_builder_task_name(dbname)
    task = tenant.get_task(task_name)
    if task is None:
        logger.info(f"starting AI extension tasks on database {dbname!r}")
        tenant.create_task(
            _ext_ai_index_builder_controller_loop(tenant, dbname),
            interruptable=True,
            name=task_name,
        )


def stop_extension(
    tenant: srv_tenant.Tenant,
    dbname: str,
) -> None:
    task_name = _get_builder_task_name(dbname)
    task = tenant.get_task(task_name)
    if task is not None:
        logger.info(f"stopping AI extension tasks on database {dbname!r}")
        task.cancel()


def _get_builder_task_name(dbname: str) -> str:
    return f"ext::ai::index builder on database {dbname!r}"


_task_name = contextvars.ContextVar(
    "ext_ai_index_builder_task_name", default="-")


async def _ext_ai_index_builder_controller_loop(
    tenant: srv_tenant.Tenant,
    dbname: str,
) -> None:
    task_name = _get_builder_task_name(dbname)
    _task_name.set(task_name)
    logger.info(f"started {task_name}")
    db = tenant.get_db(dbname=dbname)
    holding_lock = False

    await db.introspection()
    naptime_cfg = db.lookup_config("ext::ai::Config::indexer_naptime")
    naptime = naptime_cfg.to_microseconds() / 1000000

    try:
        while True:
            try:
                pgconn = await tenant.acquire_pgcon(dbname)
                models = []
                processed = 0
                errors = 0
                try:
                    models = await _ext_ai_fetch_active_models(pgconn)
                    if models:
                        if not holding_lock:
                            holding_lock = await _ext_ai_lock(pgconn)
                        if holding_lock:
                            try:
                                processed, errors = (
                                    await _ext_ai_index_builder_work(
                                        db, pgconn, models))
                            finally:
                                if processed == 0 or errors != 0:
                                    await asyncutil.deferred_shield(
                                        _ext_ai_unlock(pgconn))
                                    holding_lock = False
                finally:
                    tenant.release_pgcon(dbname, pgconn)
            except Exception:
                logger.exception(f"caught error in {task_name}")

            if processed == 0:
                # No work, sleep for a bit.
                logger.debug(
                    f"{task_name} napping for {naptime:.2f} seconds: no work")
                await asyncio.sleep(naptime)
            elif errors != 0:
                # No work, sleep for a bit.
                logger.debug(
                    f"{task_name} napping for {naptime:.2f} seconds: there "
                    f"were {errors} error(s) during last run.")
                await asyncio.sleep(naptime)
    finally:
        logger.info(f"stopped {task_name}")


async def _ext_ai_fetch_active_models(
    pgconn: pgcon.PGConnection,
) -> list[tuple[int, str, str]]:
    models = await pgconn.sql_fetch(
        b"""
            SELECT
                id,
                name,
                provider
            FROM
                edgedbext.ai_active_embedding_models
        """,
    )

    result = []
    if models:
        for model in models:
            result.append((
                int.from_bytes(model[0], byteorder="big", signed=True),
                model[1].decode("utf-8"),
                model[2].decode("utf-8"),
            ))

    return result


_EXT_AI_ADVISORY_LOCK = b"3987734540"


async def _ext_ai_lock(
    pgconn: pgcon.PGConnection,
) -> bool:
    b = await pgconn.sql_fetch_val(
        b"SELECT pg_try_advisory_lock(" + _EXT_AI_ADVISORY_LOCK + b")")
    return b == b'\x01'


async def _ext_ai_unlock(
    pgconn: pgcon.PGConnection,
) -> None:
    await pgconn.sql_fetch_val(
        b"SELECT pg_advisory_unlock(" + _EXT_AI_ADVISORY_LOCK + b")")


async def _ext_ai_index_builder_work(
    db: dbview.Database,
    pgconn: pgcon.PGConnection,
    models: list[tuple[int, str, str]],
) -> tuple[int, int]:
    task_name = _task_name.get()

    models_by_provider: dict[str, list[str]] = {}
    for entry in models:
        model_name = entry[1]
        provider_name = entry[2]
        try:
            models_by_provider[provider_name].append(model_name)
        except KeyError:
            m = models_by_provider[provider_name] = []
            m.append(model_name)

    submit_list: dict[str, dict[str, list[tuple[bytes, ...]]]] = {}

    for provider_name, provider_models in models_by_provider.items():
        for model_name in provider_models:
            logger.debug(
                f"{task_name} considering {model_name!r} "
                f"indexes via {provider_name!r}"
            )

            entries = await pgconn.sql_fetch(
                f"""
                SELECT
                    *
                FROM
                    (
                        SELECT
                            "id",
                            "text",
                            "target_rel",
                            "target_attr",
                            "target_dims_shortening"
                        FROM
                            edgedbext."ai_pending_embeddings_{model_name}"
                        LIMIT
                            500
                    ) AS q
                ORDER BY
                    q."target_dims_shortening",
                    q."target_rel"
                """.encode()
            )

            if not entries:
                continue

            logger.debug(f"{task_name} found {len(entries)} entries to index")

            try:
                provider_list = submit_list[provider_name]
            except KeyError:
                provider_list = submit_list[provider_name] = {}

            try:
                model_list = provider_list[model_name]
            except KeyError:
                model_list = provider_list[model_name] = []

            model_list.extend(entries)

    errors = 0
    if submit_list:
        cfg = db.lookup_config("ext::ai::Config::providers")

        providers_cfg = {}
        for provider in cfg:
            providers_cfg[provider.name] = provider

        tasks = {}
        async with asyncio.TaskGroup() as g:
            for provider_name, provider_list in submit_list.items():
                try:
                    provider_cfg = _get_provider_config(
                        db=db, provider_name=provider_name)
                except LookupError as e:
                    logger.error(f"{task_name}: {e}")
                    errors += 1
                    continue

                for model_name, entries in provider_list.items():
                    groups = itertools.groupby(entries, key=lambda e: e[4])
                    for shortening_datum, part_iter in groups:
                        if shortening_datum is not None:
                            shortening = int.from_bytes(
                                shortening_datum,
                                byteorder="big",
                                signed=False,
                            )
                        else:
                            shortening = None
                        part = list(part_iter)
                        task = g.create_task(
                            _generate_embeddings_task(
                                provider_cfg,
                                model_name,
                                [entry[1].decode("utf-8") for entry in part],
                                shortening,
                            ),
                        )
                        tasks[task] = part

        for task, entries in tasks.items():
            embeddings = task.result()
            if embeddings is None:
                # error
                errors += 1
            else:
                groups = itertools.groupby(entries, key=lambda e: e[2:])
                offset = 0
                for (rel, attr, *_), items in groups:
                    ids = [item[0] for item in items]
                    await _update_embeddings_in_db(
                        pgconn, rel, attr, ids, embeddings, offset)
                    offset += len(ids)

    return len(submit_list), errors


async def _update_embeddings_in_db(
    pgconn: pgcon.PGConnection,
    rel: bytes,
    attr: bytes,
    ids: list[bytes],
    embeddings: bytes,
    offset: int,
) -> int:
    id_array = '", "'.join(uuidgen.from_bytes(ub).hex for ub in ids)
    entries = await pgconn.sql_fetch_val(
        f"""
        WITH upd AS (
            UPDATE {rel.decode()} AS target
            SET
                {attr.decode()} = (
                    (embeddings.data ->> 'embedding')::edgedb.vector)
            FROM
                (
                    SELECT
                        row_number() over () AS n,
                        j.data
                    FROM
                        (SELECT
                            data
                        FROM
                            json_array_elements(($1::json) -> 'data') AS data
                        OFFSET
                            $3::text::int
                        ) AS j
                ) AS embeddings,
                unnest($2::text::text[]) WITH ORDINALITY AS ids(id, n)
            WHERE
                embeddings."n" = ids."n"
                AND target."id" = ids."id"::uuid
            RETURNING
                target."id"
        )
        SELECT count(*)::text FROM upd
        """.encode(),
        args=(
            embeddings,
            f'{{"{id_array}"}}'.encode(),
            str(offset).encode(),
        ),
    )

    return int(entries.decode())


async def _generate_embeddings_task(
    provider,
    model_name: str,
    inputs: list[str],
    shortening: Optional[int],
) -> Optional[bytes]:
    task_name = _task_name.get()

    try:
        return await _generate_embeddings(
            provider, model_name, inputs, shortening,
        )
    except AIExtError as e:
        logger.error(f"{task_name}: {e}")
        return None
    except Exception as e:
        logger.error(
            f"{task_name}: could not generate embeddings "
            f"due to an internal error: {e}"
        )
        return None


async def _generate_embeddings(
    provider,
    model_name: str,
    inputs: list[str],
    shortening: Optional[int],
) -> bytes:
    task_name = _task_name.get()
    count = len(inputs)
    suf = "s" if count > 1 else ""
    logger.debug(
        f"{task_name} generating embeddings via {model_name!r} "
        f"of {provider.name!r} for {len(inputs)} object{suf}"
    )

    if provider.api_style == "OpenAI":
        return await _generate_openai_embeddings(
            provider, model_name, inputs, shortening)
    else:
        raise RuntimeError(
            f"unsupported model provider API style: {provider.api_style}, "
            f"provider: {provider.name}"
        )


async def _generate_openai_embeddings(
    provider,
    model_name: str,
    inputs: list[str],
    shortening: Optional[int],
) -> bytes:
    headers = {
        "Authorization": f"Bearer {provider.secret}",
    }
    if provider.name == "builtin::openai" and provider.client_id:
        headers["OpenAI-Organization"] = provider.client_id
    client = httpx.AsyncClient(
        headers=headers,
        base_url=provider.api_url,
    )

    params: dict[str, Any] = {
        "model": model_name,
        "encoding_format": "float",
        "input": inputs,
    }
    if shortening is not None:
        params["dimensions"] = shortening

    result = await client.post(
        "/embeddings",
        json=params,
    )

    if result.status_code >= 400:
        raise AIProviderError(
            f"API call to generate embeddings failed with status "
            f"{result.status_code}: {result.text}"
        )
    else:
        return result.content


async def _start_chat(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    provider,
    model_name: str,
    messages: list[dict],
    stream: bool,
) -> None:
    if provider.api_style == "OpenAI":
        await _start_openai_chat(
            protocol, request, response,
            provider, model_name, messages, stream)
    elif provider.api_style == "Anthropic":
        await _start_anthropic_chat(
            protocol, request, response,
            provider, model_name, messages, stream)
    else:
        raise RuntimeError(
            f"unsupported model provider API style: {provider.api_style}, "
            f"provider: {provider.name}"
        )


@contextlib.asynccontextmanager
async def aconnect_sse(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> AsyncIterator[httpx_sse.EventSource]:
    headers = kwargs.pop("headers", {})
    headers["Accept"] = "text/event-stream"
    headers["Cache-Control"] = "no-store"

    stream = client.stream(method, url, headers=headers, **kwargs)
    async with stream as response:
        if response.status_code >= 400:
            await response.aread()
            raise AIProviderError(
                f"API call to generate chat completions failed with status "
                f"{response.status_code}: {response.text}"
            )
        else:
            yield httpx_sse.EventSource(response)


async def _start_openai_like_chat(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    client: httpx.AsyncClient,
    model_name: str,
    messages: list[dict],
    stream: bool,
) -> None:
    if stream:
        async with aconnect_sse(
            client,
            method="POST",
            url="/chat/completions",
            json={
                "model": model_name,
                "messages": messages,
                "stream": True,
            }
        ) as event_source:
            async for sse in event_source.aiter_sse():
                if not response.sent:
                    response.status = http.HTTPStatus.OK
                    response.content_type = b'text/event-stream'
                    response.close_connection = False
                    response.custom_headers["Cache-Control"] = "no-cache"
                    protocol.write(request, response)

                if sse.event != "message":
                    continue

                if sse.data == "[DONE]":
                    event = (
                        b'event: message_stop\n'
                        + b'data: {"type": "message_stop"}\n\n'
                    )
                    protocol.write_raw(event)
                    continue

                message = sse.json()
                if message.get("object") == "chat.completion.chunk":
                    data = message.get("choices")[0]
                    delta = data.get("delta")
                    role = delta.get("role")
                    if role:
                        event_data = json.dumps({
                            "type": "message_start",
                            "message": {
                                "id": message["id"],
                                "role": role,
                                "model": message["model"],
                            }
                        }).encode("utf-8")
                        event = (
                            b'event: message_start\n'
                            + b'data: ' + event_data + b'\n\n'
                        )
                        protocol.write_raw(event)

                        event = (
                            b'event: content_block_start\n'
                            + b'data: {"type": "content_block_start",'
                            + b'"index":0,'
                            + b'"content_block":{"type":"text","text":""}}\n\n'
                        )
                        protocol.write_raw(event)
                    elif finish_reason := data.get("finish_reason"):
                        event = (
                            b'event: content_block_stop\n'
                            + b'data: {"type": "content_block_stop",'
                            + b'"index":0}\n\n'
                        )
                        protocol.write_raw(event)

                        event_data = json.dumps({
                            "type": "message_delta",
                            "delta": {
                                "stop_reason": finish_reason,
                            }
                        }).encode("utf-8")
                        event = (
                            b'event: message_delta\n'
                            + b'data: ' + event_data + b'\n\n'
                        )
                        protocol.write_raw(event)

                    else:
                        event_data = json.dumps({
                            "type": "text_delta",
                            "text": delta.get("content"),
                        }).encode("utf-8")
                        event = (
                            b'event: content_block_delta\n'
                            + b'data: {"type": "content_block_delta",'
                            + b'"index":0,'
                            + b'"delta":' + event_data + b'}\n\n'
                        )
                        protocol.write_raw(event)

            protocol.close()
    else:
        result = await client.post(
            "/chat/completions",
            json={
                "model": model_name,
                "messages": messages,
            }
        )

        response.status = http.HTTPStatus.OK
        response_text = result.json()["choices"][0]["message"]["content"]
        response.content_type = b'application/json'
        response.body = json.dumps({
            "response": response_text,
        }).encode("utf-8")


async def _start_openai_chat(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    provider,
    model_name: str,
    messages: list[dict],
    stream: bool,
) -> None:
    headers = {
        "Authorization": f"Bearer {provider.secret}",
    }

    if provider.name == "builtin::openai" and provider.client_id:
        headers["OpenAI-Organization"] = provider.client_id

    client = httpx.AsyncClient(
        base_url=provider.api_url,
        headers=headers,
    )

    await _start_openai_like_chat(
        protocol,
        request,
        response,
        client,
        model_name,
        messages,
        stream,
    )


async def _start_anthropic_chat(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    provider,
    model_name: str,
    messages: list[dict],
    stream: bool,
) -> None:
    headers = {
        "x-api-key": f"{provider.secret}",
    }

    if provider.name == "builtin::anthropic":
        headers["anthropic-version"] = "2023-06-01"
        headers["anthropic-beta"] = "messages-2023-12-15"

    client = httpx.AsyncClient(
        headers={
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "messages-2023-12-15",
            "x-api-key": f"{provider.secret}",
        },
        base_url=provider.api_url,
    )

    anthropic_messages = []
    system_prompt_parts = []
    for message in messages:
        if message["role"] == "system":
            system_prompt_parts.append(message["content"])
        else:
            anthropic_messages.append(message)

    system_prompt = "\n".join(system_prompt_parts)

    if stream:
        async with aconnect_sse(
            client,
            method="POST",
            url="/messages",
            json={
                "model": model_name,
                "messages": anthropic_messages,
                "stream": True,
                "system": system_prompt,
                "max_tokens": 4096,
            }
        ) as event_source:
            async for sse in event_source.aiter_sse():
                if not response.sent:
                    response.status = http.HTTPStatus.OK
                    response.content_type = b'text/event-stream'
                    response.close_connection = False
                    response.custom_headers["Cache-Control"] = "no-cache"
                    protocol.write(request, response)

                if sse.event == "message_start":
                    message = sse.json()["message"]
                    for k in tuple(message):
                        if k not in {"id", "type", "role", "model"}:
                            del message[k]
                    message_data = json.dumps(message).encode("utf-8")
                    event = (
                        b'event: message_start\n'
                        + b'data: {"type": "message_start",'
                        + b'"message":' + message_data + b'}\n\n'
                    )
                    protocol.write_raw(event)

                elif sse.event == "content_block_start":
                    protocol.write_raw(
                        b'event: content_block_start\n'
                        + b'data: ' + sse.data.encode("utf-8") + b'\n\n'
                    )
                elif sse.event == "content_block_delta":
                    protocol.write_raw(
                        b'event: content_block_start\n'
                        + b'data: ' + sse.data.encode("utf-8") + b'\n\n'
                    )
                elif sse.event == "message_delta":
                    delta = sse.json()["delta"]
                    delta_data = json.dumps(delta).encode("utf-8")
                    event = (
                        b'event: message_delta\n'
                        + b'data: {"type": "message_delta",'
                        + b"delta:" + delta_data + b'}\n\n'
                    )
                    protocol.write_raw(event)
                elif sse.event == "message_stop":
                    event = (
                        b'event: message_stop\n'
                        + b'data: {"type": "message_stop"}\n\n'
                    )
                    protocol.write_raw(event)

            protocol.close()

    else:
        result = await client.post(
            "/messages",
            json={
                "model": model_name,
                "messages": anthropic_messages,
                "system": system_prompt,
                "max_tokens": 4096,
            }
        )

        response.status = http.HTTPStatus.OK
        response.content_type = b'application/json'
        response_text = result.json()["content"][0]["text"]
        response.body = json.dumps({
            "response": response_text,
        }).encode("utf-8")


#
# HTTP API
#

async def handle_request(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    db: dbview.Database,
    args: list[str],
    tenant: srv_tenant.Tenant,
):
    if len(args) != 1 or args[0] not in {"rag", "embeddings"}:
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True
        return
    if request.method != b"POST":
        response.body = b"Invalid request method"
        response.status = http.HTTPStatus.METHOD_NOT_ALLOWED
        response.close_connection = True
        return
    if request.content_type != b"application/json":
        response.body = b"Expected application/json input"
        response.status = http.HTTPStatus.BAD_REQUEST
        response.close_connection = True
        return

    await db.introspection()

    try:
        if args[0] == "rag":
            await _handle_rag_request(protocol, request, response, db, tenant)
        elif args[0] == "embeddings":
            await _handle_embeddings_request(request, response, db, tenant)
        else:
            response.body = b'Unknown path'
            response.status = http.HTTPStatus.NOT_FOUND
            response.close_connection = True
            return
    except Exception as ex:
        if not isinstance(ex, AIExtError):
            ex = InternalError(str(ex))

        response.status = ex.get_http_status()
        response.content_type = b'application/json'
        response.body = json.dumps(ex.json()).encode("utf-8")
        response.close_connection = True
        return


async def _handle_rag_request(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    db: dbview.Database,
    tenant: srv_tenant.Tenant,
) -> None:
    try:
        body = json.loads(request.body)
        if not isinstance(body, dict):
            raise TypeError(
                'the body of the request must be a JSON object')

        context = body.get('context')
        if context is None:
            raise TypeError(
                'missing required "context" object in request')
        if not isinstance(context, dict):
            raise TypeError(
                '"context" value in request is not a valid JSON object')

        ctx_query = context.get("query")
        ctx_variables = context.get("variables")
        ctx_globals = context.get("globals")
        ctx_max_obj_count = context.get("max_object_count")

        if not ctx_query:
            raise TypeError(
                'missing required "query" in request "context" object')

        if ctx_variables is not None and not isinstance(ctx_variables, dict):
            raise TypeError('"variables" must be a JSON object')

        if ctx_globals is not None and not isinstance(ctx_globals, dict):
            raise TypeError('"globals" must be a JSON object')

        model = body.get('model')
        if not model:
            raise TypeError(
                'missing required "model" in request')

        query = body.get('query')
        if not query:
            raise TypeError(
                'missing required "query" in request')

        stream = body.get('stream')
        if stream is None:
            stream = False
        elif not isinstance(stream, bool):
            raise TypeError('"stream" must be a boolean')

        if ctx_max_obj_count is None:
            ctx_max_obj_count = 5
        elif not isinstance(ctx_max_obj_count, int) or ctx_max_obj_count <= 0:
            raise TypeError(
                '"context.max_object_count" must be an postitive integer')

        prompt_id = None
        prompt_name = None
        custom_prompt = None
        custom_prompt_messages: dict[str, list[dict[str, Any]]] = {}

        prompt = body.get("prompt")
        if prompt is None:
            prompt_name = "builtin::rag-default"
        else:
            if not isinstance(prompt, dict):
                raise TypeError(
                    '"prompt" value in request must be a JSON object')
            prompt_name = prompt.get("name")
            prompt_id = prompt.get("id")
            custom_prompt = prompt.get("custom")

            if prompt_name and prompt_id:
                raise TypeError(
                    "prompt.id and prompt.name are mutually exclusive"
                )

            if custom_prompt:
                if not isinstance(custom_prompt, list):
                    raise TypeError(
                        "prompt.custom must be a list of {role, content} "
                        "objects"
                    )
                for entry in custom_prompt:
                    if (
                        not isinstance(entry, dict)
                        or not entry.get("role")
                        or not entry.get("content")
                        or len(entry) > 2
                    ):
                        raise TypeError(
                            "prompt.custom must be a list of {role, content} "
                            "objects"
                        )

                    try:
                        by_role = custom_prompt_messages[entry["role"]]
                    except KeyError:
                        by_role = custom_prompt_messages[entry["role"]] = []

                    by_role.append(entry)

    except Exception as ex:
        raise BadRequestError(ex.args[0])

    provider_name = await _get_model_provider(
        db,
        base_model_type="ext::ai::TextGenerationModel",
        model_name=model,
    )

    provider = _get_provider_config(db, provider_name)

    vector_query = await _generate_embeddings_for_type(
        db,
        ctx_query,
        content=query,
    )

    ctx_query = f"""
        WITH
            __query := <array<float32>>(
                to_json(<str>$input)["data"][0]["embedding"]
            ),
            search := ext::ai::search(({ctx_query}), __query),
        SELECT
            ext::ai::to_context(search.object)
        ORDER BY
            search.distance ASC EMPTY LAST
        LIMIT
            <int64>$limit
    """

    if ctx_variables is None:
        ctx_variables = {}

    ctx_variables["input"] = vector_query.decode("utf-8")
    ctx_variables["limit"] = ctx_max_obj_count

    context = await _edgeql_query_json(
        db=db,
        query=ctx_query,
        variables=ctx_variables,
        globals_=ctx_globals,
    )
    if len(context) == 0:
        raise BadRequestError(
            'query did not match any data in specified context',
        )

    prompt_query = """
        SELECT
            ext::ai::ChatPrompt {
                messages: {
                    participant_role,
                    content,
                } ORDER BY .participant_role,
            }
        FILTER
    """

    if prompt_id or prompt_name:
        prompt_variables = {}
        if prompt_name:
            prompt_query += ".name = <str>$prompt_name"
            prompt_variables["prompt_name"] = prompt_name
        elif prompt_id:
            prompt_query += ".id = <uuid><str>$prompt_id"
            prompt_variables["prompt_id"] = prompt_id

        prompts = await _edgeql_query_json(
            db=db,
            query=prompt_query,
            variables=prompt_variables,
        )
        if len(prompts) == 0:
            raise BadRequestError("could not find the specified chat prompt")

        prompt = prompts[0]
    else:
        prompt = {
            "messages": [],
        }

    messages: dict[str, list[dict]] = {}
    for message in prompt["messages"]:
        if message["participant_role"] == "User":
            content = message["content"].format(
                context="\n".join(context),
                query=query,
            )
        elif message["participant_role"] == "System":
            content = message["content"].format(
                context="\n".join(context),
            )
        else:
            content = message["content"]

        role = message["participant_role"].lower()

        try:
            by_role = messages[role]
        except KeyError:
            by_role = messages[role] = []

        by_role.append((dict(role=role, content=content)))

    for role, role_messages in custom_prompt_messages.items():
        try:
            by_role = messages[role]
        except KeyError:
            by_role = messages[role] = []

        by_role.extend(role_messages)

    await _start_chat(
        protocol,
        request,
        response,
        provider,
        model,
        list(itertools.chain.from_iterable(messages.values())),
        stream,
    )


async def _handle_embeddings_request(
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    db: dbview.Database,
    tenant: srv_tenant.Tenant,
) -> None:
    try:
        body = json.loads(request.body)
        if not isinstance(body, dict):
            raise TypeError(
                'the body of the request must be a JSON object')

        inputs = body.get("input")
        if not inputs:
            raise TypeError(
                'missing or empty required "input" value in request')

        model_name = body.get("model")
        if not model_name:
            raise TypeError(
                'missing or empty required "model" value in request')

    except Exception as ex:
        raise BadRequestError(str(ex)) from None

    provider_name = await _get_model_provider(
        db,
        base_model_type="ext::ai::EmbeddingModel",
        model_name=model_name,
    )
    if provider_name is None:
        # Error
        return

    provider = _get_provider_config(db, provider_name)

    if not isinstance(inputs, list):
        inputs = [inputs]

    result = await _generate_embeddings(
        provider,
        model_name,
        inputs,
        shortening=None,
    )

    response.status = http.HTTPStatus.OK
    response.content_type = b'application/json'
    response.body = result


async def _edgeql_query_json(
    *,
    db: dbview.Database,
    query: str,
    variables: Optional[dict[str, Any]] = None,
    globals_: Optional[dict[str, Any]] = None,
) -> list[Any]:
    try:
        result = await execute.parse_execute_json(
            db,
            query,
            variables=variables or {},
            globals_=globals_,
        )

        content = json.loads(result)
    except Exception as ex:
        try:
            await _db_error(db, ex)
        except Exception as iex:
            raise iex from None
    else:
        return content


async def _db_error(
    db: dbview.Database,
    ex: Exception,
    *,
    errcls: Optional[type[AIExtError]] = None,
    context: Optional[str] = None,
) -> NoReturn:
    if debug.flags.server:
        markup.dump(ex)

    iex = await execute.interpret_error(ex, db)

    if context:
        msg = f'{context}: {iex}'
    else:
        msg = str(iex)

    err_dct = {
        'message': msg,
        'type': str(type(iex).__name__),
        'code': iex.get_code(),
    }

    if errcls is None:
        if isinstance(iex, errors.QueryError):
            errcls = BadRequestError
        else:
            errcls = InternalError

    raise errcls(json=err_dct) from iex


def _get_provider_config(
    db: dbview.Database,
    provider_name: str,
) -> Any:
    cfg = db.lookup_config("ext::ai::Config::providers")

    for provider in cfg:
        if provider.name == provider_name:
            return provider
    else:
        raise ConfigurationError(
            f"provider {provider_name!r} has not been configured"
        )


async def _get_model_provider(
    db: dbview.Database,
    base_model_type: str,
    model_name: str,
) -> str:
    models = await _edgeql_query_json(
        db=db,
        query="""
        WITH
            Parent := (
                SELECT
                    schema::ObjectType
                FILTER
                    .name = <str>$base_model_type
            ),
            Models := Parent.<ancestors[IS schema::ObjectType],
        SELECT
            Models {
                provider := (
                    SELECT
                        (.annotations@value, .annotations.name)
                    FILTER
                        .1 = "ext::ai::model_provider"
                    LIMIT
                        1
                ).0,
            }
        FILTER
            .annotations.name = "ext::ai::model_name"
            AND .annotations@value = <str>$model_name
        """,
        variables={
            "base_model_type": base_model_type,
            "model_name": model_name,
        },
    )
    if len(models) == 0:
        raise BadRequestError("invalid model name")
    elif len(models) > 1:
        raise InternalError("multiple models defined as requested model")

    return models[0]["provider"]


async def _generate_embeddings_for_type(
    db: dbview.Database,
    type_query: str,
    content: str,
) -> bytes:
    try:
        type_desc = await execute.describe(
            db,
            f"SELECT ({type_query})",
            allow_capabilities=compiler.Capability.NONE,
        )
        if (
            not isinstance(type_desc, sertypes.ShapeDesc)
            or not isinstance(type_desc.type, sertypes.ObjectDesc)
        ):
            raise errors.InvalidReferenceError(
                'context query does not return an '
                'object type indexed with an `ext::ai::index`'
            )

        indexes = await _edgeql_query_json(
            db=db,
            query="""
            WITH
                ObjectType := (
                    SELECT
                        schema::ObjectType
                    FILTER
                        .id = <uuid>$type_id
                ),
            SELECT
                ObjectType.indexes {
                    model := (
                        SELECT
                            (.annotations@value, .annotations.name)
                        FILTER
                            .1 = "ext::ai::model_name"
                        LIMIT
                            1
                    ).0,
                    provider := (
                        SELECT
                            (.annotations@value, .annotations.name)
                        FILTER
                            .1 = "ext::ai::model_provider"
                        LIMIT
                            1
                    ).0,
                    model_embedding_dimensions := <int64>(
                        SELECT
                            (.annotations@value, .annotations.name)
                        FILTER
                            .1 =
                            "ext::ai::embedding_model_max_output_dimensions"
                        LIMIT
                            1
                    ).0,
                    index_embedding_dimensions := <int64>(
                        SELECT
                            (.annotations@value, .annotations.name)
                        FILTER
                            .1 = "ext::ai::embedding_dimensions"
                        LIMIT
                            1
                    ).0,
                }
            FILTER
                .ancestors.name = 'ext::ai::index'
            """,
            variables={"type_id": str(type_desc.type.tid)},
        )
        if len(indexes) == 0:
            raise errors.InvalidReferenceError(
                'context query does not return an '
                'object type indexed with an `ext::ai::index`'
            )
        elif len(indexes) > 1:
            raise errors.InvalidReferenceError(
                'context query returns an object '
                'indexed with multiple `ext::ai::index` indexes'
            )

    except Exception as ex:
        await _db_error(db, ex, context="context.query")

    index = indexes[0]
    provider = _get_provider_config(db=db, provider_name=index["provider"])
    if (
        index["index_embedding_dimensions"]
        < index["model_embedding_dimensions"]
    ):
        shortening = index["index_embedding_dimensions"]
    else:
        shortening = None
    return await _generate_embeddings(
        provider, index["model"], [content], shortening=shortening)
