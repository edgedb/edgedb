#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

abstract type R {
    required property name -> str {
        delegated constraint exclusive;
    }
}

type A extending R;

type S extending R {
    required property s -> str;
    multi link l_a -> A;
}

type T extending R {
    required property t -> str;
    multi link l_a -> A;
}

abstract type U {
    required property u -> str;
}

type V extending U, S, T;

type W {
    required property name -> str {
        constraint exclusive;
    }
    link w -> W;
}

type X extending W, U;

type Z {
    required property name -> str {
        constraint exclusive;
    };

    # have 'name' in common
    multi link stw0 -> S | T | W;
}

# 3 abstract base types and their concrete permutations
abstract type Ba {
    required property ba -> str;
}

abstract type Bb {
    required property bb -> int64;
}

abstract type Bc {
    required property bc -> float64;
}

type CBa extending Ba;

type CBb extending Bb;

type CBc extending Bc;

type CBaBb extending Ba, Bb;

type CBaBc extending Ba, Bc;

type CBbBc extending Bb, Bc;

type CBaBbBc extending Ba, Bb, Bc;

# 3 types which resemble the base types

type XBa {
    required property ba -> str;
}

type XBb {
    required property bb -> int64;
}

type XBc {
    required property bc -> float64;
}

# Objects which all have a `numbers` property and `siblings` link

# non-computed single

type SoloNonCompSinglePropA {
    single property numbers -> int64;
}
type SoloNonCompSinglePropB {
    single property numbers -> int64;
}
type SoloNonCompSingleLinkA {
    single link siblings -> SoloNonCompSingleLinkA;
}
type SoloNonCompSingleLinkB {
    single link siblings -> SoloNonCompSingleLinkB;
}

# non-computed multi

type SoloNonCompMultiPropA {
    multi property numbers -> int64;
}
type SoloNonCompMultiPropB {
    multi property numbers -> int64;
}
type SoloNonCompMultiLinkA {
    multi link siblings -> SoloNonCompMultiLinkA;
}
type SoloNonCompMultiLinkB {
    multi link siblings -> SoloNonCompMultiLinkB;
}

# computed single

type SoloCompSinglePropA {
    single property numbers := 1;
}
type SoloCompSinglePropB {
    single property numbers := 1;
}
type SoloCompSingleLinkA {
    single link siblings := (select detached SoloCompSingleLinkA limit 1);
}
type SoloCompSingleLinkB {
    single link siblings := (select detached SoloCompSingleLinkB limit 1);
}

# computed multi

type SoloCompMultiPropA {
    multi property numbers := {1, 2, 3};
}
type SoloCompMultiPropB {
    multi property numbers := {1, 2, 3};
}
type SoloCompMultiLinkA {
    multi link siblings := (select detached SoloCompMultiLinkA);
}
type SoloCompMultiLinkB {
    multi link siblings := (select detached SoloCompMultiLinkB);
}

# non-computed single from base class

abstract type BaseNonCompSingleProp {
    single property numbers -> int64;
}
type DerivedNonCompSinglePropA extending BaseNonCompSingleProp;
type DerivedNonCompSinglePropB extending BaseNonCompSingleProp;

abstract type BaseNonCompSingleLink {
    single link siblings -> BaseNonCompSingleLink;
}
type DerivedNonCompSingleLinkA extending BaseNonCompSingleLink;
type DerivedNonCompSingleLinkB extending BaseNonCompSingleLink;

# non-computed multi from base class

abstract type BaseNonCompMultiProp {
    multi property numbers -> int64;
}
type DerivedNonCompMultiPropA extending BaseNonCompMultiProp;
type DerivedNonCompMultiPropB extending BaseNonCompMultiProp;

abstract type BaseNonCompMultiLink {
    multi link siblings -> BaseNonCompMultiLink;
}
type DerivedNonCompMultiLinkA extending BaseNonCompMultiLink;
type DerivedNonCompMultiLinkB extending BaseNonCompMultiLink;

# computed single from base class

abstract type BaseCompSingleProp {
    single property numbers := 1;
}
type DerivedCompSinglePropA extending BaseCompSingleProp;
type DerivedCompSinglePropB extending BaseCompSingleProp;

abstract type BaseCompSingleLink {
    single link siblings := (select detached BaseCompSingleLink limit 1);
}
type DerivedCompSingleLinkA extending BaseCompSingleLink;
type DerivedCompSingleLinkB extending BaseCompSingleLink;

# computed multi from base class

abstract type BaseCompMultiProp {
    multi property numbers := {1, 2, 3};
}
type DerivedCompMultiPropA extending BaseCompMultiProp;
type DerivedCompMultiPropB extending BaseCompMultiProp;

abstract type BaseCompMultiLink {
    multi link siblings := (select detached BaseCompMultiLink);
}
type DerivedCompMultiLinkA extending BaseCompMultiLink;
type DerivedCompMultiLinkB extending BaseCompMultiLink;

# Objects with links to a target type

type Destination {
    required property name -> str;
}

# independent types with compatible pointers

type SoloOriginA {
    single link dest -> Destination;
}
type SoloOriginB {
    single link dest -> Destination;
}

# independent types with compatible pointers and common derived type

type BaseOriginA {
    single link dest -> Destination;
}
type BaseOriginB {
    single link dest -> Destination;
}
type DerivedOriginC extending BaseOriginA, BaseOriginB;
