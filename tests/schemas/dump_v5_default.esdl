#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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


scalar type v3 extending ext::pgvector::vector<3>;

type L2 {
    required vec: v3;
    index ext::pgvector::hnsw_cosine(m := 2, ef_construction := 4) on (.vec);
}

type TestEmbeddingModel
    extending ext::ai::EmbeddingModel
{
    annotation ext::ai::model_name := "text-embedding-test";
    annotation ext::ai::model_provider := "custom::test";
    annotation ext::ai::embedding_model_max_input_tokens := "8191";
    annotation ext::ai::embedding_model_max_batch_tokens := "16384";
    annotation ext::ai::embedding_model_max_output_dimensions := "10";
    annotation ext::ai::embedding_model_supports_shortening := "true";
};

type Astronomy {
    content: str;
    deferred index ext::ai::index(embedding_model := 'text-embedding-test')
        on (.content);

    # N.B: This was added late, for 5.5, so won't appear in 5.0 dumps.
    # Test that having both AI and FTS indexes works.
    index fts::index on (
      fts::with_options(.content, language := fts::Language.eng));
};
