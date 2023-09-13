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


# Schema for bug #5791

scalar type autoIncrementFileLegacyId extending sequence;
scalar type autoIncrementCollectionLegacyId extending sequence;
scalar type autoIncrementCommentMappingId extending sequence;

type File {
    required property legacyId -> int32{
        default := (
            select sequence_next(introspect autoIncrementFileLegacyId)+1000000)
    };

    required property name -> str {
        constraint expression on (
            __subject__ = str_trim(__subject__)
        );
    }

    required property slug -> str;

    required property hash -> str {
        readonly := true;
        constraint exclusive;
    }

    property description -> str;

    property workflowId -> uuid {
        readonly := true;
    }

    required property userTags -> json;

    required property createdAt -> datetime{
        default := std::datetime_current();
        readonly := true;
    }
    property updatedAt -> datetime;
    property publishedAt -> datetime;

    required property userId -> uuid {
        readonly := true;
    }

    required property status -> str {
        constraint one_of('PENDING', 'PUBLISHED', 'REJECTED', 'DELETED');
        default := 'PENDING';
    }

    property bgColor -> str;

    required property isSticker -> bool {
        default := false;
    }
    required property isPremium -> bool {
        default := false;
    }

    property downloadCount -> int32{
        default := 0;
    };

    multi link fileVariations := .<file[is FileVariation];

    property lottieSource := (
        select .fileVariations
        filter .type = "LOTTIE" and .isOptimized = false
        order by .createdAt desc
        limit 1
    ).path;

    property jsonSource := (
        select .fileVariations
        filter .type = "JSON" and .isOptimized = false
        order by .createdAt desc
        limit 1
    ).path;

    property imageSource := (
        select .fileVariations
        filter .type = "PNG"
        order by .createdAt desc
        limit 1
    ).path;

    index on ((.hash, .status, .isPremium));
    index on ((.slug, .status));
    index on ((.id, .isSticker, .isPremium, .status));
    index on ((.isSticker, .isPremium, .status));
    index on ((.userId, .isPremium, .status));
    index on ((.userId, .status));
    index on ((.hash, .status));
    index on ((.userId, .isPremium, .status, .publishedAt));
    index on (.hash);
    index on (.isSticker);
    index on (.isPremium);
    index on (.status);
    index on (.userId);
    index on (.publishedAt);
    index on (.downloadCount);
}

type FileVariation {
    required property path -> str;

    required property createdAt -> datetime {
        default := std::datetime_current();
        readonly := true;
    }

    property updatedAt -> datetime;

    required property type -> str {
        constraint one_of(
            'LOTTIE', 'JSON', 'MP4', 'GIF', 'PNG', 'ZIP', 'WEBP', 'WEBM',
            'MOV', 'AEP'
        );
        default := 'LOTTIE';
    }

    property size -> int32 {
        default := 0;
    }

    link file -> File;

    link fileDimension -> FileDimension;

    property isOptimized -> bool {
        default := false;
    }

    property isTransparent -> bool {
        default := false;
    }

    index on ((.type, .isOptimized));
    index on ((.type, .isTransparent));
    index on (.type);
    index on (.isOptimized);
    index on (.isTransparent);
}

type UserPreference {
    required property userId -> uuid{
        constraint exclusive;
    };

    property isHireable -> bool {
        default := false;
    }
    index on (.isHireable);
}

type FileDimension {
    required property name -> str;
    required property width -> int32 {
        default := 0;
    }
    required property height -> int32 {
        default := 0;
    }
}
