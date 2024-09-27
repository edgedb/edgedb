/*
 *
 * This source file is part of the EdgeDB open source project.
 *
 * Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"

#include "fmgr.h"
#include "common/jsonapi.h"
#include "mb/pg_wchar.h"
#include "parser/analyze.h"

PG_MODULE_MAGIC;

#define EDB_STMT_MAGIC_PREFIX "-- {\"query\""

typedef enum EdbStmtInfoParseState {
    EDB_STMT_INFO_PARSE_NOOP,
    EDB_STMT_INFO_PARSE_QUERY,
    EDB_STMT_INFO_PARSE_QUERYID,
} EdbStmtInfoParseState;

typedef struct EdbStmtInfoSemState {
    int nested_level;
    EdbStmtInfoParseState state;
    char *query;
    uint64 queryId;
} EdbStmtInfoSemState;

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

static void
edbss_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);

static bool
edbss_overwrite_stmt_info(ParseState *pstate, Query *query);

static JsonParseErrorType
edbss_json_struct_start(void *semstate);

static JsonParseErrorType
edbss_json_struct_end(void *semstate);

static JsonParseErrorType
edbss_json_ofield_start(void *semstate, char *fname, bool isnull);

static JsonParseErrorType
edbss_json_scalar(void *semstate, char *token, JsonTokenType tokenType);

void
_PG_init(void) {
    prev_post_parse_analyze_hook = post_parse_analyze_hook;
    post_parse_analyze_hook = edbss_post_parse_analyze;
}

/*
 * Post-parse-analysis hook: mark query with a queryId
 */
static void
edbss_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate) {
    const char *orig_sourcetext = pstate->p_sourcetext;

    if (!edbss_overwrite_stmt_info(pstate, query))
        jstate = NULL;

    if (prev_post_parse_analyze_hook)
        prev_post_parse_analyze_hook(pstate, query, jstate);

    pstate->p_sourcetext = orig_sourcetext;
}

/*
 * Extract EdgeDB statement info and overwrite source text and queryId
 */
static bool
edbss_overwrite_stmt_info(ParseState *pstate, Query *query) {
    if (strncmp(pstate->p_sourcetext, EDB_STMT_MAGIC_PREFIX, strlen(EDB_STMT_MAGIC_PREFIX)) == 0) {
        EdbStmtInfoSemState state;
        JsonSemAction sem;
        char *info_str = (char *) pstate->p_sourcetext + 3;
        JsonLexContext *lex = makeJsonLexContextCstringLen(info_str, (int) (strchr(info_str, '\n') - info_str), PG_UTF8,
                                                           true);
        memset(&state, 0, sizeof(state));
        memset(&sem, 0, sizeof(sem));
        state.state = EDB_STMT_INFO_PARSE_NOOP;
        sem.semstate = (void *) &state;
        sem.object_start = edbss_json_struct_start;
        sem.object_end = edbss_json_struct_end;
        sem.array_start = edbss_json_struct_start;
        sem.array_end = edbss_json_struct_end;
        sem.object_field_start = edbss_json_ofield_start;
        sem.scalar = edbss_json_scalar;

        if (pg_parse_json(lex, &sem) == JSON_SUCCESS && state.query!= NULL) {
            pstate->p_sourcetext = state.query;
            if (state.queryId != 0) {
                query->queryId = state.queryId;
            }
            return true;
        }
    }

    // Don't track non-EdgeDB statements
    query->queryId = UINT64CONST(0);
    return false;
}

static JsonParseErrorType
edbss_json_struct_start(void *semstate) {
    EdbStmtInfoSemState *state = (EdbStmtInfoSemState *) semstate;
    state->nested_level += 1;
    return JSON_SUCCESS;
}

static JsonParseErrorType
edbss_json_struct_end(void *semstate) {
    EdbStmtInfoSemState *state = (EdbStmtInfoSemState *) semstate;
    state->nested_level -= 1;
    return JSON_SUCCESS;
}

static JsonParseErrorType
edbss_json_ofield_start(void *semstate, char *fname, bool isnull) {
    EdbStmtInfoSemState *state = (EdbStmtInfoSemState *) semstate;
    Assert(fname != NULL);
    if (strcmp(fname, "query") == 0) {
        state->state = EDB_STMT_INFO_PARSE_QUERY;
    } else if (strcmp(fname, "queryId") == 0) {
        state->state = EDB_STMT_INFO_PARSE_QUERYID;
    } else {
        state->state = EDB_STMT_INFO_PARSE_NOOP;
    }
    return JSON_SUCCESS;
}

static JsonParseErrorType
edbss_json_scalar(void *semstate, char *token, JsonTokenType tokenType) {
    EdbStmtInfoSemState *state = (EdbStmtInfoSemState *) semstate;
    switch (state->state) {
        case EDB_STMT_INFO_PARSE_QUERY:
            if (tokenType == JSON_TOKEN_STRING) {
                // DEBUG: copy?
                // state->query = palloc(strlen(token) + 1);
                // strcpy(state->query, token);
                state->query = token;
                state->state = EDB_STMT_INFO_PARSE_NOOP;
                break;
            } else {
                return JSON_SEM_ACTION_FAILED;
            }
        case EDB_STMT_INFO_PARSE_QUERYID:
            if (tokenType == JSON_TOKEN_NUMBER) {
                char *endptr;
                uint64 queryId = strtoull(token, &endptr, 10);
                if (*endptr == '\0' && queryId != UINT64_MAX) {
                    state->queryId = queryId;
                    state->state = EDB_STMT_INFO_PARSE_NOOP;
                    break;
                } else {
                    return JSON_SEM_ACTION_FAILED;
                }
            } else {
                return JSON_SEM_ACTION_FAILED;
            }
        case EDB_STMT_INFO_PARSE_NOOP:
            break;
    }
    return JSON_SUCCESS;
}
