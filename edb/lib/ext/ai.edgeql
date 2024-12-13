#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


CREATE EXTENSION PACKAGE ai VERSION '1.0' {
    set ext_module := "ext::ai";
    set dependencies := ["pgvector>=0.7"];

    create module ext::ai;

    create scalar type ext::ai::ProviderAPIStyle
        extending enum<OpenAI, Anthropic>;

    create abstract type ext::ai::ProviderConfig extending cfg::ConfigObject {
        create required property name: std::str {
            set readonly := true;
            create constraint exclusive;
            create annotation std::description :=
                "Unique provider name.";
        };

        create required property display_name: std::str {
            set readonly := true;
            create annotation std::description :=
                "Human-friendly provider name.";
        };

        create required property api_url: std::str {
            set readonly := true;
            create annotation std::description := "Provider API URL.";
        };

        create property client_id: std::str {
            set readonly := true;
            create annotation std::description :=
                "ID for client provided by model API vendor.";
        };

        create required property secret: std::str {
            set readonly := true;
            set secret := true;
            create annotation std::description :=
                "Secret provided by model API vendor.";
        };

        create required property api_style: ext::ai::ProviderAPIStyle {
            create annotation std::description :=
                "The API style exposed by this provider.";
        };
    };

    create type ext::ai::CustomProviderConfig extending ext::ai::ProviderConfig {
        alter property display_name {
            set default := 'Custom';
        };

        alter property api_style {
            set default := ext::ai::ProviderAPIStyle.OpenAI;
        };
    };

    create type ext::ai::OpenAIProviderConfig extending ext::ai::ProviderConfig {
        alter property name {
            set protected := true;
            set default := 'builtin::openai';
        };

        alter property display_name {
            set protected := true;
            set default := 'OpenAI';
        };

        alter property api_url {
            set default := 'https://api.openai.com/v1'
        };

        alter property api_style {
            set protected := true;
            set default := ext::ai::ProviderAPIStyle.OpenAI;
        };
    };

    create type ext::ai::MistralProviderConfig extending ext::ai::ProviderConfig {
        alter property name {
            set protected := true;
            set default := 'builtin::mistral';
        };

        alter property display_name {
            set protected := true;
            set default := 'Mistral';
        };

        alter property api_url {
            set default := 'https://api.mistral.ai/v1'
        };

        alter property api_style {
            set protected := true;
            set default := ext::ai::ProviderAPIStyle.OpenAI;
        };
    };

    create type ext::ai::AnthropicProviderConfig extending ext::ai::ProviderConfig {
        alter property name {
            set protected := true;
            set default := 'builtin::anthropic';
        };

        alter property display_name {
            set protected := true;
            set default := 'Anthropic';
        };

        alter property api_url {
            set default := 'https://api.anthropic.com/v1'
        };

        alter property api_style {
            set protected := true;
            set default := ext::ai::ProviderAPIStyle.Anthropic;
        };
    };

    create type ext::ai::Config extending cfg::ExtensionConfig {
        create required property indexer_naptime: std::duration {
            set default := <std::duration>'10s';
            create annotation std::description := '
                Specifies the minimum delay between runs of the
                deferred ext::ai::index indexer on any given branch.
            ';
        };

        create multi link providers: ext::ai::ProviderConfig {
            create annotation std::description :=
                "AI model provider configurations.";
        };
    };

    create abstract inheritable annotation
        ext::ai::model_name;
    create abstract inheritable annotation
        ext::ai::model_provider;

    create abstract type ext::ai::Model extending std::BaseObject {
        create annotation ext::ai::model_name := "<must override>";
        create annotation ext::ai::model_provider := "<must override>";
    };

    create abstract inheritable annotation
        ext::ai::embedding_model_max_input_tokens;

    create abstract inheritable annotation
        ext::ai::embedding_model_max_batch_tokens;

    create abstract inheritable annotation
        ext::ai::embedding_model_max_output_dimensions;

    create abstract inheritable annotation
        ext::ai::embedding_model_supports_shortening;

    create abstract type ext::ai::EmbeddingModel
        extending ext::ai::Model
    {
        create annotation
            ext::ai::embedding_model_max_input_tokens := "<must override>";
        create annotation
            ext::ai::embedding_model_max_batch_tokens := "<must override>";
        create annotation
            ext::ai::embedding_model_max_output_dimensions := "<must override>";
        create annotation
            ext::ai::embedding_model_supports_shortening := "false";
    };

    create abstract inheritable annotation
        ext::ai::text_gen_model_context_window;

    create abstract type ext::ai::TextGenerationModel
        extending ext::ai::Model
    {
        create annotation
            ext::ai::text_gen_model_context_window := "<must override>";
    };

    # OpenAI models
    create abstract type ext::ai::OpenAITextEmbedding3SmallModel
        extending ext::ai::EmbeddingModel
    {
        alter annotation
            ext::ai::model_name := "text-embedding-3-small";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::embedding_model_max_input_tokens := "8191";
        alter annotation
            ext::ai::embedding_model_max_batch_tokens := "8191";
        alter annotation
            ext::ai::embedding_model_max_output_dimensions := "1536";
        alter annotation
            ext::ai::embedding_model_supports_shortening := "true";
    };

    create abstract type ext::ai::OpenAITextEmbedding3LargeModel
        extending ext::ai::EmbeddingModel
    {
        alter annotation
            ext::ai::model_name := "text-embedding-3-large";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::embedding_model_max_input_tokens := "8191";
        alter annotation
            ext::ai::embedding_model_max_batch_tokens := "8191";
        # Note: ext::pgvector is currently limited to 2000 dimensions,
        # so returned embeddings will be automatically truncated if
        # pgvector is used as the index implementation.
        alter annotation
            ext::ai::embedding_model_max_output_dimensions := "3072";
        alter annotation
            ext::ai::embedding_model_supports_shortening := "true";
    };

    create abstract type ext::ai::OpenAITextEmbeddingAda002Model
        extending ext::ai::EmbeddingModel
    {
        alter annotation
            ext::ai::model_name := "text-embedding-ada-002";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::embedding_model_max_input_tokens := "8191";
        alter annotation
            ext::ai::embedding_model_max_batch_tokens := "8191";
        alter annotation
            ext::ai::embedding_model_max_output_dimensions := "1536";
    };

    create abstract type ext::ai::OpenAIGPT_3_5_TurboModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "gpt-3.5-turbo";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "16385";
    };

    create abstract type ext::ai::OpenAIGPT_4_TurboPreviewModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "gpt-4-turbo-preview";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::OpenAIGPT_4_TurboModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "gpt-4-turbo";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::OpenAIGPT_4o_Model
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "gpt-4o";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::OpenAIGPT_4o_MiniModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "gpt-4o-mini";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::OpenAIGPT_4_Model
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "gpt-4";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::OpenAI_O1_PreviewModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "o1-preview";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::OpenAI_O1_MiniModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "o1-mini";
        alter annotation
            ext::ai::model_provider := "builtin::openai";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    # Mistral models
    create abstract type ext::ai::MistralEmbedModel
        extending ext::ai::EmbeddingModel
    {
        alter annotation
            ext::ai::model_name := "mistral-embed";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::embedding_model_max_input_tokens := "8192";
        alter annotation
            ext::ai::embedding_model_max_batch_tokens := "16384";
        alter annotation
            ext::ai::embedding_model_max_output_dimensions := "1024";
    };

    create abstract type ext::ai::MistralSmallModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "mistral-small-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "32000";
    };

    # Mistral legacy model
    create abstract type ext::ai::MistralMediumModel
        extending ext::ai::TextGenerationModel
    {
        create annotation std::deprecated :=
        "This model is noted as a legacy model in the Mistral docs.";
        alter annotation
            ext::ai::model_name := "mistral-medium-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "32000";
    };

    create abstract type ext::ai::MistralLargeModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "mistral-large-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::PixtralLargeModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "pixtral-large-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::Ministral_3B_Model
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "ministral-3b-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::Ministral_8B_Model
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "ministral-8b-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::CodestralModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "codestral-latest";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "32000";
    };

    # Mistral free models
    create abstract type ext::ai::PixtralModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "pixtral-12b-2409";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::MistralNemo
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "open-mistral-nemo";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "128000";
    };

    create abstract type ext::ai::CodestralMamba
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "open-codestral-mamba";
        alter annotation
            ext::ai::model_provider := "builtin::mistral";
        alter annotation
            ext::ai::text_gen_model_context_window := "256000";
    };

    # Anthropic models
    # Anthropic most intelligent model
    create abstract type ext::ai::AnthropicClaude_3_5_SonnetModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "claude-3-5-sonnet-latest";
        alter annotation
            ext::ai::model_provider := "builtin::anthropic";
        alter annotation
            ext::ai::text_gen_model_context_window := "200000";
    };

   # Anthropic fastest model
    create abstract type ext::ai::AnthropicClaude_3_5_HaikuModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "claude-3-5-haiku-latest";
        alter annotation
            ext::ai::model_provider := "builtin::anthropic";
        alter annotation
            ext::ai::text_gen_model_context_window := "200000";
    };

    create abstract type ext::ai::AnthropicClaude3HaikuModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "claude-3-haiku-20240307";
        alter annotation
            ext::ai::model_provider := "builtin::anthropic";
        alter annotation
            ext::ai::text_gen_model_context_window := "200000";
    };

    create abstract type ext::ai::AnthropicClaude3SonnetModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "claude-3-sonnet-20240229";
        alter annotation
            ext::ai::model_provider := "builtin::anthropic";
        alter annotation
            ext::ai::text_gen_model_context_window := "200000";
    };

    create abstract type ext::ai::AnthropicClaude3OpusModel
        extending ext::ai::TextGenerationModel
    {
        alter annotation
            ext::ai::model_name := "claude-3-opus-latest";
        alter annotation
            ext::ai::model_provider := "builtin::anthropic";
        alter annotation
            ext::ai::text_gen_model_context_window := "200000";
    };

    create scalar type ext::ai::DistanceFunction
        extending enum<Cosine, InnerProduct, L2>;

    create scalar type ext::ai::IndexType
        extending enum<HNSW>;

    create abstract inheritable annotation
        ext::ai::embedding_dimensions;

    create abstract index ext::ai::index (
        named only embedding_model: str,
        named only dimensions: optional int64 = {},
        named only distance_function: ext::ai::DistanceFunction
            = ext::ai::DistanceFunction.Cosine,
        named only index_type: ext::ai::IndexType
            = ext::ai::IndexType.HNSW,
        named only index_parameters: tuple<m: int64, ef_construction: int64>
            = (m := 32, ef_construction := 100),
        named only truncate_to_max: bool = False,
    ) {
        create annotation std::description :=
            "Semantic similarity index.";
        create annotation ext::ai::embedding_dimensions := "";
        set deferrability := 'Required';
    };

    create function ext::ai::to_context(
        object: anyobject,
    ) -> std::str
    {
        create annotation std::description :=
            "Evaluate the expression of an ai::index defined on the passed "
            ++ "object type and return it.";
        set volatility := 'Stable';
        using sql expression;
    };

    create function ext::ai::search(
        object: anyobject,
        query: array<std::float32>,
    ) -> optional tuple<object: anyobject, distance: float64>
    {
        create annotation std::description := '
            Search an object using its ext::ai::index index.
            Returns objects that match the specified semantic query and the
            similarity score.
        ';
        set volatility := 'Stable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql expression;
    };

    create scalar type ext::ai::ChatParticipantRole
        extending enum<System, User, Assistant, Tool>;

    create type ext::ai::ChatPromptMessage extending std::BaseObject {
        create required property participant_role:
            ext::ai::ChatParticipantRole
        {
            create annotation std::description :=
                'The role of the messages author.'
        };

        create property participant_name: str {
            create annotation std::description :=
                'Optional name for the participant.'
        };

        create required property content: str {
            create annotation std::description :=
                'Prompt message contenxt.'
        };
    };

    create type ext::ai::ChatPrompt extending std::BaseObject {
        create required property name: str {
            create constraint exclusive;
            create annotation std::description :=
                'Unique name for the prompt configuration';
        };

        create required multi link messages: ext::ai::ChatPromptMessage {
            create constraint exclusive;
            create annotation std::description :=
                'Messages in this prompt configuration';
        };
    };

    insert ext::ai::ChatPrompt {
        name := 'builtin::rag-default',
        messages := {
            (insert ext::ai::ChatPromptMessage {
                participant_role := ext::ai::ChatParticipantRole.System,
                content := (
                    "You are an expert Q&A system.\n" ++
                    "Always answer questions based on the provided \
                     context information. Never use prior knowledge.\n" ++
                    "Follow these additional rules:\n\
                     1. Never directly reference the given context in your \
                        answer.\n\
                     2. Never include phrases like 'Based on the context, ...' \
                        or any similar phrases in your responses. \
                     3. When the context does not provide information about \
                        the question, answer with \
                        'No information available.'.\n\
                     Context information is below:\n{context}\n\
                     Given the context information above and not prior \
                     knowledge, answer the user query."
                ),
            }),
        }
    };

    create index match for std::str using ext::ai::index;
};
