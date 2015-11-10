##
# Copyright (c) 2012, 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importkit.import_ import get_object
from metamagic.utils.shell import CommandGroup
from metamagic.utils.cache.nodesystem import CacheSystem
from metamagic.node.shell import base


class CacheCommand(base.NodeCommand, name=None):
    build_stage = False


class CacheResetCommand(CacheCommand, name='reset'):
    def get_parser(self, subparsers):
        parser = super().get_parser(subparsers)

        parser.add_argument('-b', '--bucket', default=None, type=str, dest='bucket',
                            help='Bucket class to reset i.e. "metamagic.utils.cache.Bucket"')

        return parser

    def run(self, args):
        from metamagic.utils.cache import Bucket

        bucket_cls = Bucket
        if args.bucket:
            try:
                bucket_cls = get_object(args.bucket)
            except Exception as ex:
                raise RuntimeError('unable to load bucket class {!r}'.format(args.bucket)) from ex

        self.log('resetting cache bucket {!r}'.format(bucket_cls))
        bucket_cls.reset()
        self.log('done.')


class NodeCacheCommands(CommandGroup, name='cache', expose=True,
                        commands=(CacheResetCommand,)):
    pass

