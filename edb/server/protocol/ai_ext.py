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
from dataclasses import dataclass, field
from typing import (
    cast,
    Any,
    AsyncIterator,
    ClassVar,
    NoReturn,
    Optional,
    Sequence,
    TYPE_CHECKING,
)

import abc
import asyncio
import contextlib
import contextvars
import http
import itertools
import json
import logging

import httpx
import httpx_sse

import tiktoken
from mistral_common.tokens.tokenizers import mistral as mistral_tokenizer


from edb import errors
from edb.common import asyncutil
from edb.common import debug
from edb.common import enum as s_enum
from edb.common import markup
from edb.common import uuidgen

from edb.server import compiler
from edb.server.compiler import sertypes
from edb.server.protocol import execute
from edb.server.protocol import request_scheduler as rs

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


class ApiStyle(s_enum.StrEnum):
    OpenAI = 'OpenAI'
    Anthropic = 'Anthropic'


class Tokenizer(abc.ABC):

    @abc.abstractmethod
    def encode(self, text: str) -> list[int]:
        """Encode text into tokens."""
        raise NotImplementedError

    @abc.abstractmethod
    def encode_padding(self) -> int:
        """How many special characters are added to encodings?"""
        raise NotImplementedError

    @abc.abstractmethod
    def decode(self, tokens: list[int]) -> str:
        """Decode tokens into text."""
        raise NotImplementedError

    def shorten_to_token_length(self, text: str, token_length: int) -> str:
        """Truncate text to a maximum token length."""
        encoded = self.encode(text)
        if len(encoded) > token_length:
            encoded = encoded[:token_length]
        return self.decode(encoded)


class OpenAITokenizer(Tokenizer):

    _instances: dict[str, OpenAITokenizer] = {}

    encoding: Any

    @classmethod
    def for_model(cls, model_name: str) -> OpenAITokenizer:
        if model_name in cls._instances:
            return cls._instances[model_name]

        tokenizer = OpenAITokenizer()
        tokenizer.encoding = tiktoken.encoding_for_model(model_name)
        cls._instances[model_name] = tokenizer

        return tokenizer

    def encode(self, text: str) -> list[int]:
        return cast(list[int], self.encoding.encode(text))

    def encode_padding(self) -> int:
        return 0

    def decode(self, tokens: list[int]) -> str:
        return cast(str, self.encoding.decode(tokens))


class MistralTokenizer(Tokenizer):

    _instances: dict[str, MistralTokenizer] = {}

    tokenizer: Any

    @classmethod
    def for_model(cls, model_name: str) -> MistralTokenizer:
        if model_name in cls._instances:
            return cls._instances[model_name]

        assert model_name == 'mistral-embed'

        tokenizer = MistralTokenizer()
        tokenizer.tokenizer = mistral_tokenizer.MistralTokenizer.v1()
        cls._instances[model_name] = tokenizer

        return tokenizer

    def encode(self, text: str) -> list[int]:
        # V1 tokenizer wraps input text with control tokens [INST] [/INST].
        #
        # While these count towards the overal token limit, how special tokens
        # are applied to embedding requests is not documented. For now, directly
        # pass the text into the inner tokenizer.
        tokenized = self.tokenizer.instruct_tokenizer.tokenizer.encode(
            text, bos=False, eos=False
        )
        return cast(list[int], tokenized)

    def encode_padding(self) -> int:
        # V1 tokenizer wraps input text with control tokens [INST] [/INST].
        #
        # This is only 2 tokens, and testing shows that mistral-embed does add
        # two tokens to embeddings inputs. However, this is not documented, so
        # add some extra leeway in case things change.
        #
        # Note, other models may use significantly more control tokens.
        return 16

    def decode(self, tokens: list[int]) -> str:
        return cast(str, self.tokenizer.decode(tokens))


@dataclass
class ProviderConfig:
    name: str
    display_name: str
    api_url: str
    client_id: str
    secret: str
    api_style: ApiStyle


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

    provider_schedulers: dict[str, ProviderScheduler] = {}

    try:
        while True:
            try:
                models = []
                sleep_timer: rs.Timer = rs.Timer(None, False)
                async with tenant.with_pgcon(dbname) as pgconn:
                    models = await _ext_ai_fetch_active_models(pgconn)
                    if models:
                        if not holding_lock:
                            holding_lock = await _ext_ai_lock(pgconn)
                        if holding_lock:
                            provider_contexts = _prepare_provider_contexts(
                                db,
                                pgconn,
                                models,
                                provider_schedulers,
                                naptime,
                            )
                            try:
                                sleep_timer = (
                                    await _ext_ai_index_builder_work(
                                        provider_schedulers,
                                        provider_contexts,
                                    )
                                )
                            finally:
                                if not sleep_timer.is_ready_and_urgent():
                                    await asyncutil.deferred_shield(
                                        _ext_ai_unlock(pgconn))
                                    holding_lock = False
            except Exception:
                logger.exception(f"caught error in {task_name}")

            if not sleep_timer.is_ready_and_urgent():
                delay = sleep_timer.remaining_time(naptime)
                if delay == naptime:
                    logger.debug(
                        f"{task_name}: "
                        f"No work. Napping for {naptime:.2f} seconds."
                    )
                await asyncio.sleep(delay)

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


def _prepare_provider_contexts(
    db: dbview.Database,
    pgconn: pgcon.PGConnection,
    models: list[tuple[int, str, str]],
    provider_schedulers: dict[str, ProviderScheduler],
    naptime: float,
) -> dict[str, ProviderContext]:

    models_by_provider: dict[str, list[str]] = {}
    for entry in models:
        model_name = entry[1]
        provider_name = entry[2]
        try:
            models_by_provider[provider_name].append(model_name)
        except KeyError:
            m = models_by_provider[provider_name] = []
            m.append(model_name)

    # Drop any extra providers, they were probably deleted.
    unused_provider_names = {
        provider_name
        for provider_name in provider_schedulers.keys()
        if provider_name not in models_by_provider
    }
    for unused_provider_name in unused_provider_names:
        provider_schedulers.pop(unused_provider_name, None)

    # Create contexts
    provider_contexts = {}

    for provider_name, provider_models in models_by_provider.items():
        if provider_name not in provider_schedulers:
            # Create new schedulers if necessary
            provider_schedulers[provider_name] = ProviderScheduler(
                provider_name=provider_name
            )
        provider_scheduler = provider_schedulers[provider_name]

        if not provider_scheduler.timer.is_ready():
            continue

        provider_contexts[provider_name] = ProviderContext(
            naptime=naptime,
            db=db,
            pgconn=pgconn,
            provider_models=provider_models,
        )

    return provider_contexts


async def _ext_ai_index_builder_work(
    provider_schedulers: dict[str, ProviderScheduler],
    provider_contexts: dict[str, ProviderContext],
) -> rs.Timer:

    async with asyncio.TaskGroup() as g:
        for provider_name, provider_scheduler in provider_schedulers.items():
            if provider_name not in provider_contexts:
                continue

            provider_context = provider_contexts[provider_name]
            g.create_task(provider_scheduler.process(provider_context))

    sleep_timer = rs.Timer.combine(
        provider_scheduler.timer
        for provider_scheduler in provider_schedulers.values()
    )
    if sleep_timer is not None:
        return sleep_timer
    else:
        # Return any non-urgent timer
        return rs.Timer(None, False)


@dataclass(frozen=True)
class EmbeddingsData:
    embeddings: bytes


@dataclass
class ProviderContext(rs.Context):

    db: dbview.Database
    pgconn: pgcon.PGConnection
    provider_models: list[str]


@dataclass
class ProviderScheduler(rs.Scheduler[EmbeddingsData]):

    provider_name: str = ''

    # If a text is too long for a model, it will be excluded from embeddings
    # to prevent pointlessly wasting requests and tokens.
    # An embedding index may have its `truncate_to_max` flag switched. If the
    # flag is on, previously excluded inputs will be truncated and processed.
    model_excluded_ids: dict[str, list[str]] = field(default_factory=dict)

    async def get_params(
        self, context: rs.Context,
    ) -> Optional[Sequence[EmbeddingsParams]]:
        assert isinstance(context, ProviderContext)
        return await _generate_embeddings_params(
            context.db,
            context.pgconn,
            self.provider_name,
            context.provider_models,
            self.model_excluded_ids,
        )

    def finalize(self, execution_report: rs.ExecutionReport) -> None:
        task_name = _task_name.get()

        for message in execution_report.known_error_messages:
            logger.error(
                f"{task_name}: "
                f"Could not generate embeddings for {self.provider_name} "
                f"due to an internal error: {message}"
            )


@dataclass(frozen=True, kw_only=True)
class EmbeddingsParams(rs.Params[EmbeddingsData]):
    pgconn: pgcon.PGConnection
    provider: ProviderConfig
    model_name: str
    inputs: list[str]
    shortening: Optional[int]

    entries: list[tuple[bytes, ...]]

    def cost(self) -> int:
        return 1

    def create_request(self) -> EmbeddingsRequest:
        return EmbeddingsRequest(self)


class EmbeddingsRequest(rs.Request[EmbeddingsData]):

    async def run(self) -> Optional[rs.Result[EmbeddingsData]]:
        task_name = _task_name.get()

        try:
            assert isinstance(self.params, EmbeddingsParams)
            result = await _generate_embeddings(
                self.params.provider,
                self.params.model_name,
                self.params.inputs,
                self.params.shortening,
            )
            result.pgconn = self.params.pgconn
            result.entries = self.params.entries
            return result
        except AIExtError as e:
            logger.error(f"{task_name}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"{task_name}: could not generate embeddings "
                f"due to an internal error: {e}"
            )
            return None


class EmbeddingsResult(rs.Result[EmbeddingsData]):

    pgconn: Optional[Any] = None
    entries: Optional[list[tuple[bytes, ...]]] = None

    async def finalize(self) -> None:
        if isinstance(self.data, rs.Error):
            return
        if self.pgconn is None or self.entries is None:
            return

        groups = itertools.groupby(
            self.entries, key=lambda e: e[2:],
        )
        offset = 0
        for (rel, attr, *_), items in groups:
            ids = [item[0] for item in items]
            await _update_embeddings_in_db(
                self.pgconn,
                rel,
                attr,
                ids,
                self.data.embeddings,
                offset,
            )
            offset += len(ids)


async def _generate_embeddings_params(
    db: dbview.Database,
    pgconn: pgcon.PGConnection,
    provider_name: str,
    provider_models: list[str],
    model_excluded_ids: dict[str, list[str]],
) -> Optional[list[EmbeddingsParams]]:
    task_name = _task_name.get()

    try:
        provider_cfg = _get_provider_config(
            db=db, provider_name=provider_name)
    except LookupError as e:
        logger.error(f"{task_name}: {e}")
        return None

    model_tokenizers: dict[str, Tokenizer] = {}
    if provider_name == 'builtin::openai':
        model_tokenizers = {
            model_name: OpenAITokenizer.for_model(model_name)
            for model_name in provider_models
        }
    elif provider_name == 'builtin::mistral':
        model_tokenizers = {
            model_name: MistralTokenizer.for_model(model_name)
            for model_name in provider_models
        }

    model_max_input_tokens: dict[str, int] = {
        model_name: await _get_model_annotation_as_int(
            db,
            base_model_type="ext::ai::EmbeddingModel",
            model_name=model_name,
            annotation_name="ext::ai::embedding_model_max_input_tokens",
        )
        for model_name in provider_models
    }

    model_max_batch_tokens: dict[str, int] = {
        model_name: await _get_model_annotation_as_int(
            db,
            base_model_type="ext::ai::EmbeddingModel",
            model_name=model_name,
            annotation_name="ext::ai::embedding_model_max_batch_tokens",
        )
        for model_name in provider_models
    }

    model_entries: dict[str, list[tuple[bytes, ...]]] = {}

    for model_name in provider_models:
        logger.debug(
            f"{task_name} considering {model_name!r} "
            f"indexes via {provider_name!r}"
        )

        where_clause = ""
        if (
            model_name in model_excluded_ids
            and (excluded_ids := model_excluded_ids[model_name])
        ):
            # Only exclude long text if it won't be auto-truncated.
            logger.debug(
                f"{task_name} skipping {len(excluded_ids)} indexes "
                f"for {model_name!r}"
            )
            where_clause = (f"""
                WHERE
                    q."id" not in ({','.join(
                        "'" + excluded_id + "'"
                        for excluded_id in excluded_ids
                    )})
                    OR q."truncate_to_max"
            """)

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
                        "target_dims_shortening",
                        "truncate_to_max"
                    FROM
                        edgedbext."ai_pending_embeddings_{model_name}"
                    LIMIT
                        500
                ) AS q
            {where_clause}
            ORDER BY
                q."target_dims_shortening",
                q."target_rel"
            """.encode()
        )

        if not entries:
            continue

        logger.debug(f"{task_name} found {len(entries)} entries to index")

        try:
            model_list = model_entries[model_name]
        except KeyError:
            model_list = model_entries[model_name] = []

        model_list.extend(entries)

    embeddings_params: list[EmbeddingsParams] = []

    for model_name, entries in model_entries.items():
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

            inputs: list[str] = []
            for entry in part:
                input = entry[1].decode("utf-8")
                truncate_to_max = bool.from_bytes(entry[5])

                if model_name in model_tokenizers:
                    tokenizer = model_tokenizers[model_name]
                    truncate_length = (
                        model_max_input_tokens[model_name]
                        - tokenizer.encode_padding()
                    )

                    if truncate_to_max:
                        input = tokenizer.shorten_to_token_length(
                            input, truncate_length
                        )
                    elif len(tokenizer.encode(input)) > truncate_length:
                        # If the text is too long, mark it as excluded and skip.
                        if model_name not in model_excluded_ids:
                            model_excluded_ids[model_name] = []
                        model_excluded_ids[model_name].append(
                            uuidgen.from_bytes(entry[0]).hex
                        )
                        continue

                inputs.append(input)

            if model_name in model_tokenizers:
                # Trim the inputs so that they fit into the batch size
                batches = _batch_embeddings_inputs(
                    tokenizer, inputs, model_max_batch_tokens[model_name]
                )
                if batches:
                    # The request scheduler is not designed to handle multiple
                    # batches for a single provider. Just take the first one,
                    # and the rest can be processed next time. If the limits are
                    # not exceeded, this will happen "immediately".
                    inputs = batches[0]
                else:
                    inputs = []

            if not inputs:
                continue

            embeddings_params.append(EmbeddingsParams(
                pgconn=pgconn,
                provider=provider_cfg,
                model_name=model_name,
                inputs=inputs,
                shortening=shortening,
                entries=part,
            ))

    return embeddings_params


def _batch_embeddings_inputs(
    tokenizer: Tokenizer,
    inputs: list[str],
    max_batch_tokens: int,
) -> list[list[str]]:
    """Create batches of embeddings inputs."""

    # Get token counts
    input_token_counts = [
        len(tokenizer.encode(input))
        for input in inputs
    ]

    # Get indexes of inputs, sorted from shortest to longest by token count
    unbatched_input_indexes = list(range(len(inputs)))
    unbatched_input_indexes.sort(
        key=lambda index: input_token_counts[index],
        reverse=False,
    )

    def unbatched_input(unbatched_index: int) -> str:
        return inputs[unbatched_input_indexes[unbatched_index]]

    def unbatched_token_count(unbatched_index: int) -> int:
        return input_token_counts[unbatched_input_indexes[unbatched_index]]

    # Remove any inputs that are larger than the maximum
    while (
        unbatched_input_indexes
        and unbatched_token_count(-1) > max_batch_tokens
    ):
        unbatched_input_indexes.pop()

    batches: list[list[str]] = []
    while unbatched_input_indexes:
        # Start with the largest available input
        batch = [unbatched_input(-1)]
        batch_token_count = unbatched_token_count(-1)
        unbatched_input_indexes.pop()

        if batch_token_count < max_batch_tokens:
            # Then add the smallest available input as long as long as the
            # max batch token count isn't exceeded
            unbatched_index = 0
            while unbatched_index < len(unbatched_input_indexes):
                if (
                    batch_token_count + unbatched_token_count(unbatched_index)
                    <= max_batch_tokens
                ):
                    batch.append(unbatched_input(unbatched_index))
                    batch_token_count += unbatched_token_count(unbatched_index)
                    unbatched_input_indexes.pop(unbatched_index)
                else:
                    unbatched_index += 1

        batches.append(batch)

    return batches


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


async def _generate_embeddings(
    provider: ProviderConfig,
    model_name: str,
    inputs: list[str],
    shortening: Optional[int],
) -> EmbeddingsResult:
    task_name = _task_name.get()
    count = len(inputs)
    suf = "s" if count > 1 else ""
    logger.debug(
        f"{task_name} generating embeddings via {model_name!r} "
        f"of {provider.name!r} for {len(inputs)} object{suf}"
    )

    if provider.api_style == ApiStyle.OpenAI:
        return await _generate_openai_embeddings(
            provider, model_name, inputs, shortening,
        )
    else:
        raise RuntimeError(
            f"unsupported model provider API style: {provider.api_style}, "
            f"provider: {provider.name}"
        )


async def _generate_openai_embeddings(
    provider: ProviderConfig,
    model_name: str,
    inputs: list[str],
    shortening: Optional[int],
) -> EmbeddingsResult:

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

    error = None
    if result.status_code >= 400:
        error = rs.Error(
            message=(
                f"API call to generate embeddings failed with status "
                f"{result.status_code}: {result.text}"
            ),
            retry=(
                # If the request fails with 429 - too many requests, it can be
                # retried
                result.status_code == 429
            ),
        )

    return EmbeddingsResult(
        data=(error if error else EmbeddingsData(result.content)),
        request_limits=_read_openai_limits(result),
    )


def _read_openai_limits(
    result: Any,
) -> rs.Limits:
    try:
        header = (
            result.headers['x-ratelimit-limit-project-requests']
            if 'x-ratelimit-limit-project-requests' in result.headers else
            result.headers['x-ratelimit-limit-requests']
            if 'x-ratelimit-limit-requests' in result.headers else
            None
        )
        request_limit = int(header) if header is not None else None
    except Exception:
        request_limit = None

    try:
        header = (
            result.headers['x-ratelimit-remaining-project-requests']
            if 'x-ratelimit-remaining-project-requests' in result.headers else
            result.headers['x-ratelimit-remaining-requests']
            if 'x-ratelimit-remaining-requests' in result.headers else
            None
        )
        request_remaining = int(header) if header is not None else None
    except Exception:
        request_remaining = None

    return rs.Limits(
        total=request_limit,
        remaining=request_remaining
    )


async def _start_chat(
    protocol: protocol.HttpProtocol,
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    provider: ProviderConfig,
    model_name: str,
    messages: list[dict[str, Any]],
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
    messages: list[dict[str, Any]],
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

        if result.status_code >= 400:
            await result.aread()
            raise AIProviderError(
                f"API call to generate chat completions failed with status "
                f"{result.status_code}: {result.text}"
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
    provider: ProviderConfig,
    model_name: str,
    messages: list[dict[str, Any]],
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
    provider: ProviderConfig,
    model_name: str,
    messages: list[dict[str, Any]],
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

        if result.status_code >= 400:
            await result.aread()
            raise AIProviderError(
                f"API call to generate chat completions failed with status "
                f"{result.status_code}: {result.text}"
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
) -> None:
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

        if not isinstance(ex, BadRequestError):
            logger.error(f"error while handling a /{args[0]} request: {ex}")

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

    messages: dict[str, list[dict[str, Any]]] = {}
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
    if isinstance(result.data, rs.Error):
        raise AIProviderError(result.data.message)

    response.status = http.HTTPStatus.OK
    response.content_type = b'application/json'
    response.body = result.data.embeddings


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
        return cast(list[Any], content)


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
) -> ProviderConfig:
    cfg = db.lookup_config("ext::ai::Config::providers")

    for provider in cfg:
        if provider.name == provider_name:
            provider = cast(ProviderConfig, provider)
            return ProviderConfig(
                name=provider.name,
                display_name=provider.display_name,
                api_url=provider.api_url,
                client_id=provider.client_id,
                secret=provider.secret,
                api_style=provider.api_style,
            )
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

    return cast(str, models[0]["provider"])


async def _get_model_annotation_as_int(
    db: dbview.Database,
    base_model_type: str,
    model_name: str,
    annotation_name: str,
) -> int:
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
                value := (
                    SELECT
                        (.annotations@value, .annotations.name)
                    FILTER
                        .1 = <str>$annotation_name
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
            "annotation_name": annotation_name,
        },
    )
    if len(models) == 0:
        raise BadRequestError("invalid model name")
    elif len(models) > 1:
        raise InternalError("multiple models defined as requested model")

    return int(models[0]["value"])


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
    result = await _generate_embeddings(
        provider, index["model"], [content], shortening=shortening)
    if isinstance(result.data, rs.Error):
        raise AIProviderError(result.data.message)
    return result.data.embeddings
