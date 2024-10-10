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
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "optimizer/planner.h"
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
static planner_hook_type prev_planner_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void
edbss_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);

static PlannedStmt *
edbss_planner(Query *parse,
              const char *query_string,
              int cursorOptions,
              ParamListInfo boundParams);

static void
edbss_ExecutorEnd(QueryDesc *queryDesc);

static uint64
edbss_extract_stmt_info(char **query_str);

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
    prev_planner_hook = planner_hook;
    planner_hook = edbss_planner;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = edbss_ExecutorEnd;
}

/*
 * Post-parse-analysis hook: mark query with custom queryId
 */
static void
edbss_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate) {
    // Parse EdgeDB query info JSON and overwrite query->queryId
    char *query_str = (char *) pstate->p_sourcetext;
    query->queryId = edbss_extract_stmt_info(&query_str);

    // But skip pgss_store() as we don't need the early entry
    if (prev_post_parse_analyze_hook)
        prev_post_parse_analyze_hook(pstate, query, NULL);
}

static PlannedStmt *
edbss_planner(Query *parse,
              const char *query_string,
              int cursorOptions,
              ParamListInfo boundParams) {
    char *query_str = (char *) query_string;
    edbss_extract_stmt_info(&query_str);
    if (prev_planner_hook)
        return prev_planner_hook(parse, query_str, cursorOptions,
                                 boundParams);
    else
        return standard_planner(parse, query_str, cursorOptions,
                                boundParams);
}

static void
edbss_ExecutorEnd(QueryDesc *queryDesc) {
    const char *orig_sourceText = queryDesc->sourceText;
    edbss_extract_stmt_info((char **) &queryDesc->sourceText);
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    queryDesc->sourceText = orig_sourceText;
}

/*
 * Extract EdgeDB statement info and overwrite source text and queryId
 */
static uint64
edbss_extract_stmt_info(char **query_str) {
    if (strncmp(*query_str, EDB_STMT_MAGIC_PREFIX, strlen(EDB_STMT_MAGIC_PREFIX)) == 0) {
        EdbStmtInfoSemState state;
        JsonSemAction sem;
        char *info_str = *query_str + 3;
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

        if (pg_parse_json(lex, &sem) == JSON_SUCCESS && state.query != NULL) {
            *query_str = state.query;
            if (state.queryId != UINT64CONST(0)) {
                return state.queryId;
            }
        }
    }

    // Don't track non-EdgeDB cacheable statements
    return UINT64CONST(0);
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
    if (state->nested_level == 1) {
        if (strcmp(fname, "query") == 0) {
            state->state = EDB_STMT_INFO_PARSE_QUERY;
        } else if (strcmp(fname, "queryId") == 0) {
            state->state = EDB_STMT_INFO_PARSE_QUERYID;
        }
    }
    return JSON_SUCCESS;
}

static JsonParseErrorType
edbss_json_scalar(void *semstate, char *token, JsonTokenType tokenType) {
    EdbStmtInfoSemState *state = (EdbStmtInfoSemState *) semstate;
    switch (state->state) {
        case EDB_STMT_INFO_PARSE_QUERY:
            if (tokenType == JSON_TOKEN_STRING) {
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
