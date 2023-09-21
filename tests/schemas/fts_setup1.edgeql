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


insert Text {text := 'hello world'};
insert Text {text := 'running and jumping fox'};
insert FancyText {text := 'fancy hello', style := 0};
insert FancyText {text := 'elaborate and foxy', style := 10};
insert QuotedText {text := 'the world is big', author := 'Alice'};
insert QuotedText {text := 'this is simple', author := 'Bob'};
insert FancyQuotedText {
    text := 'the fox chases the rabbit', author := 'Cameron', style := 1,
};
insert FancyQuotedText {
    text := 'the rabbit is fast', author := 'Cameron', style := 2,
};

insert Post {
    title := 'first post',
    body := 'The sky is so red.',
};
insert Post {
    title := 'angry reply',
    body := "No! Wrong! It's blue!",
};
insert Post {
    title := 'helpful reply',
    body := "That's Rayleigh scattering for you",
};
insert Post {
    title := 'random stuff',
    body := 'angry giraffes',
};
insert Post {
    title := 'no body',
};

insert Description {
    num := 1,
    raw := 'red umbrella',
};
insert Description {
    num := 2,
    raw := 'red and white candy cane',
};
insert Description {
    num := 3,
    raw := 'fancy pants',
};