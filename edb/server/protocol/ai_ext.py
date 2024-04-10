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
    ClassVar,
    NoReturn,
    Optional,
    TYPE_CHECKING,
)

import asyncio
import contextvars
import http
import itertools
import json
import logging

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
    raise RuntimeError(f"unsupported model provider: {provider.name}")


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
        if args[0] == "embeddings":
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
        response.body = json.dumps(ex.json()).encode("utf-8")
        response.close_connection = True
        return


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
