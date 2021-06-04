#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


INSERT Item {
    name := 'table',

    tag_set1 := {'wood', 'rectangle'},
    tag_set2 := {'wood', 'rectangle'},
    tag_array := ['wood', 'rectangle'],
};

INSERT Item {
    name := 'floor lamp',

    tag_set1 := {'metal', 'plastic'},
    tag_set2 := {'metal', 'plastic'},
    tag_array := ['metal', 'plastic'],
};

# some items with incomplete data
INSERT Item {
    name := 'chair',

    tag_set1 := {'wood', 'rectangle'},
    tag_array := ['wood', 'rectangle'],
};


INSERT Item {
    name := 'tv',

    tag_set2 := {'plastic', 'rectangle'},
    tag_array := ['plastic', 'rectangle'],
};


INSERT Item {
    name := 'ball',

    tag_set1 := {'plastic', 'round'},
    tag_set2 := {'plastic', 'round'},
};


INSERT Item {
    name := 'teapot',

    tag_array := ['ceramic', 'round'],
};

INSERT Item {
    name := 'mystery toy',
};

# no known properties
INSERT Item {
    name := 'ectoplasm',
};
