#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


import asyncio
import contextlib
import pickle
import signal
import subprocess
import sys
import textwrap

from edb.testbase import server as tb


TIMEOUT = 5


@contextlib.asynccontextmanager
async def spawn(test_prog, global_prog=""):
    p = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "edb.testbase.proc",
        textwrap.dedent(global_prog)
        + "\n"
        + textwrap.dedent(
            """\
            import signal
            from edb.common import signalctl
            """
        ),
        textwrap.dedent(test_prog),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        yield p
    except Exception:
        stdout, stderr = await asyncio.wait_for(p.communicate(), TIMEOUT)
        if p.returncode <= 0:
            raise
        else:
            raise ChildProcessError("\n\n" + stderr.decode())
    else:
        stdout, stderr = await asyncio.wait_for(p.communicate(), TIMEOUT)
        if p.returncode > 0:
            raise ChildProcessError("\n\n" + stderr.decode())

    finally:
        try:
            p.kill()
            await asyncio.wait_for(p.wait(), TIMEOUT)
        except OSError:
            pass


class TestSignalctl(tb.TestCase):
    def notify_child(self, p, mark):
        p.stdin.write(str(mark).encode() + b"\n")

    async def wait_for_child(self, p, mark):
        line = await asyncio.wait_for(p.stdout.readline(), TIMEOUT)
        if not line:
            self.fail("Child process exited unexpectedly.")
        elif len(line) <= 3:
            self.assertEqual(line.strip(), str(mark).encode())
        else:
            line += await asyncio.wait_for(p.stdout.read(), TIMEOUT)
            ex, traceback = pickle.loads(line)
            raise ex from ChildProcessError("\n\n" + traceback.strip())

    async def test_signalctl_wait_for_01(self):
        test_prog = """\
        async def test_signalctl_wait_for_01_child(self):
            async def task():
                self.notify_parent(1)
                await asyncio.sleep(1)

            with signalctl.SignalController(signal.SIGTERM) as sc:
                with self.assertRaisesRegex(signalctl.SignalError, "SIGTERM"):
                    await sc.wait_for(task(), cancel_on={signal.SIGTERM})

            self.notify_parent(2)
            await self.wait_for_parent(3)
        """
        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()

            await self.wait_for_child(p, 2)
            p.terminate()
            self.assertEqual(
                await asyncio.wait_for(p.wait(), TIMEOUT), -signal.SIGTERM
            )

    async def test_signalctl_wait_for_02(self):
        test_prog = """\
        async def test_signalctl_wait_for_02_child(self):
            async def task():
                self.notify_parent(1)
                await asyncio.sleep(1)

            with signalctl.SignalController(signal.SIGINT) as sc:
                with self.assertRaisesRegex(signalctl.SignalError, "SIGINT"):
                    await sc.wait_for(task(), cancel_on={signal.SIGINT})

            self.notify_parent(2)
            await self.wait_for_parent(3)
        """
        end_reached = False
        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.send_signal(signal.SIGINT)

            await self.wait_for_child(p, 2)
            p.send_signal(signal.SIGINT)
            self.assertEqual(
                await asyncio.wait_for(p.wait(), TIMEOUT), -signal.SIGINT
            )
            end_reached = True

        self.assertTrue(end_reached)

    async def test_signalctl_wait_for_03(self):
        test_prog = """\
        async def test_signalctl_wait_for_03_child(self):
            async def task():
                self.notify_parent(1)
                await asyncio.sleep(1)

            with signalctl.SignalController(
                signal.SIGTERM, signal.SIGINT
            ) as sc:
                with self.assertRaisesRegex(signalctl.SignalError, "SIGTERM"):
                    await sc.wait_for(task(), cancel_on={signal.SIGTERM})

            self.notify_parent(2)
            await self.wait_for_parent(3)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)

            p.send_signal(signal.SIGINT)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(p.wait(), p.stdout.read(1)), 0.2
                )

            p.terminate()
            await self.wait_for_child(p, 2)

            p.terminate()
            self.assertEqual(
                await asyncio.wait_for(p.wait(), TIMEOUT), -signal.SIGTERM
            )

    async def test_signalctl_wait_for_04(self):
        test_prog = """\
        async def test_signalctl_wait_for_04_child(self):
            async def task():
                self.notify_parent(1)
                await asyncio.sleep(1)

            with signalctl.SignalController(
                signal.SIGTERM, signal.SIGINT
            ) as sc:
                with self.assertRaisesRegex(signalctl.SignalError, "SIGINT"):
                    await sc.wait_for(task(), cancel_on={signal.SIGINT})

            self.notify_parent(2)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)

            p.terminate()
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(p.wait(), p.stdout.read(1)), 0.2
                )

            p.send_signal(signal.SIGINT)
            await self.wait_for_child(p, 2)

    async def test_signalctl_wait_for_05(self):
        test_prog = """\
        async def test_signalctl_wait_for_05_child(self):
            async def task():
                self.notify_parent(2)
                await self.wait_for_parent(3)

            with signalctl.SignalController(signal.SIGTERM) as sc:
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        sc.wait_for(self.wait_for_parent(1)), 0.1
                    )

                with self.assertRaisesRegex(signalctl.SignalError, "SIGTERM"):
                    await sc.wait_for(task(), cancel_on={signal.SIGTERM})

            self.notify_parent(4)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 2)
            p.terminate()
            await self.wait_for_child(p, 4)

    async def test_signalctl_wait_for_06(self):
        test_prog = """\
        async def test_signalctl_wait_for_06_child(self):
            fut = self.loop.create_future()
            waiter = self.loop.create_future()

            async def _task():
                waiter.set_result(None)
                return await fut

            with signalctl.SignalController(signal.SIGTERM) as sc:
                task = self.loop.create_task(
                    sc.wait_for(_task(), cancel_on={signal.SIGTERM})
                )
                await waiter

                # The task is cancelled (not by signal) at the moment the
                # result is ready - return the result instead of the error
                fut.set_result(123)
                task.cancel()
                self.assertEqual(await task, 123)
        """

        async with spawn(test_prog):
            pass

    async def test_signalctl_wait_for_07(self):
        test_prog = """\
        async def test_signalctl_wait_for_07_child(self):
            fut = self.loop.create_future()

            class Waiter:
                def done(self):
                    return False

                def set_result(self, result):
                    # Simulates a completed task at the moment signal arrives
                    fut.set_result(123)

            with signalctl.SignalController(signal.SIGTERM) as sc:
                task = self.loop.create_task(
                    sc.wait_for(fut, cancel_on={signal.SIGTERM})
                )
                waiter = Waiter()
                sc._register_waiter(signal.SIGTERM, waiter)
                try:
                    os.kill(os.getpid(), signal.SIGTERM)
                    self.assertEqual(await task, 123)
                finally:
                    sc._discard_waiter(signal.SIGTERM, waiter)
        """

        async with spawn(test_prog, global_prog="import os"):
            pass

    async def test_signalctl_wait_for_08(self):
        test_prog = """\
        async def test_signalctl_wait_for_08_child(self):
            async def task():
                self.notify_parent(1)
                try:
                    await self.wait_for_parent(2)
                except asyncio.CancelledError:
                    # In case the task cancellation is intercepted, ..
                    return 123

            with signalctl.SignalController(signal.SIGTERM) as sc:
                # .. the SignalError is not raised
                await sc.wait_for(task(), cancel_on={signal.SIGTERM})
                self.notify_parent(3)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()
            await self.wait_for_child(p, 3)

    async def test_signalctl_wait_for_09(self):
        test_prog = """\
        async def test_signalctl_wait_for_09_child(self):
            fut = self.loop.create_future()

            async def _task():
                self.notify_parent(1)
                try:
                    await self.wait_for_parent(2)  # cancelled by signal
                except asyncio.CancelledError:
                    # In case the task cancellation is hanging, ..
                    fut.set_result(None)
                    await self.wait_for_parent(2)

            with signalctl.SignalController(signal.SIGTERM) as sc:
                task = self.loop.create_task(
                    sc.wait_for(_task(), cancel_on={signal.SIGTERM})
                )
                await fut

                # .. we should still be able to reliably cancel the task
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(task, 0.2)
                self.assertTrue(task.done())
                self.assertTrue(task.cancelled())
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()

    async def test_signalctl_wait_for_10(self):
        test_prog = """\
        async def test_signalctl_wait_for_10_child(self):
            async def _subtask1():
                with signalctl.SignalController(signal.SIGTERM) as sc2:
                    self.notify_parent(1)
                    try:
                        with self.assertRaisesRegex(
                            signalctl.SignalError, "SIGTERM"
                        ):  # not actually caught; task cancelled
                            await sc2.wait_for(self.wait_for_parent(2))
                    finally:
                        self.notify_parent(3)

            async def _subtask2():
                self.notify_parent(1)
                try:
                    await asyncio.sleep(10)
                finally:
                    self.notify_parent(3)

            async def _task():
                async with taskgroup.TaskGroup() as tg:
                    tg.create_task(_subtask1())
                    tg.create_task(_subtask2())

            with signalctl.SignalController(signal.SIGTERM) as sc:
                with self.assertRaisesRegex(signalctl.SignalError, "SIGTERM"):
                    await sc.wait_for(_task())
        """

        async with spawn(
            test_prog, global_prog="from edb.common import taskgroup"
        ) as p:
            await self.wait_for_child(p, 1)
            await self.wait_for_child(p, 1)
            p.terminate()
            await self.wait_for_child(p, 3)
            await self.wait_for_child(p, 3)

    async def test_signalctl_wait_for_11(self):
        test_prog = """\
        async def test_signalctl_wait_for_11_child(self):
            async def task():
                self.notify_parent(1)
                try:
                    await self.wait_for_parent(2)
                finally:
                    self.notify_parent(3)

            with signalctl.SignalController(signal.SIGTERM) as sc:
                with self.assertRaisesRegex(signalctl.SignalError, "SIGTERM"):
                    await sc.wait_for(task())
            self.notify_parent(4)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()
            await self.wait_for_child(p, 3)
            await self.wait_for_child(p, 4)

    async def test_signalctl_wait_for_12(self):
        test_prog = """\
        async def test_signalctl_wait_for_12_child(self):
            async def task():
                self.notify_parent(1)
                try:
                    await self.wait_for_parent(2)
                finally:
                    self.notify_parent(3)
                    try:
                        await self.wait_for_parent(4)
                    finally:
                        self.notify_parent(5)
                        await self.wait_for_parent(6)

            with signalctl.SignalController(
                signal.SIGTERM, signal.SIGINT, signal.SIGUSR1
            ) as sc:
                with self.assertRaises(signalctl.SignalError) as ctx:
                    await sc.wait_for(task())
            ex = ctx.exception
            self.assertEqual(ex.signo, signal.SIGUSR1)
            self.assertEqual(ex.__context__.signo, signal.SIGINT)
            self.assertEqual(ex.__context__.__context__.signo, signal.SIGTERM)
            self.notify_parent(7)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()
            await self.wait_for_child(p, 3)
            p.send_signal(signal.SIGINT)
            await self.wait_for_child(p, 5)
            p.send_signal(signal.SIGUSR1)
            await self.wait_for_child(p, 7)

    async def test_signalctl_wait_for_13(self):
        test_prog = """\
        async def test_signalctl_wait_for_13_child(self):
            fut = self.loop.create_future()
            async def _task():
                self.notify_parent(1)
                try:
                    await self.wait_for_parent(2)
                finally:
                    try:
                        self.notify_parent(3)
                        await self.wait_for_parent(4)
                    finally:
                        fut.set_result(None)
                        await asyncio.sleep(10)

            with signalctl.SignalController(
                signal.SIGTERM, signal.SIGINT
            ) as sc:
                with self.assertRaises(asyncio.TimeoutError) as ctx:
                    task = self.loop.create_task(sc.wait_for(_task()))
                    await fut
                    await asyncio.wait_for(task, 0.1)
            ex = ctx.exception
            while not isinstance(ex, signalctl.SignalError):
                ex = ex.__context__
            self.assertEqual(ex.signo, signal.SIGINT)
            self.assertEqual(ex.__context__.signo, signal.SIGTERM)
            self.notify_parent(5)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()
            await self.wait_for_child(p, 3)
            p.send_signal(signal.SIGINT)
            await self.wait_for_child(p, 5)

    async def test_signalctl_wait_for_14(self):
        test_prog = """\
        async def test_signalctl_wait_for_14_child(self):
            fut = self.loop.create_future()
            async def _task():
                self.notify_parent(1)
                try:
                    await self.wait_for_parent(2)
                finally:
                    fut.set_result(None)
                    try:
                        await asyncio.sleep(10)
                    finally:
                        self.notify_parent(3)
                        await self.wait_for_parent(4)

            with signalctl.SignalController(
                signal.SIGTERM, signal.SIGINT
            ) as sc:
                with self.assertRaises(signalctl.SignalError) as ctx:
                    task = self.loop.create_task(sc.wait_for(_task()))
                    await fut
                    await asyncio.wait_for(task, 0.1)
            ex = ctx.exception
            self.assertEqual(ex.signo, signal.SIGINT)
            self.assertEqual(ex.__context__.signo, signal.SIGTERM)
            self.notify_parent(5)
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.terminate()
            await self.wait_for_child(p, 3)
            p.send_signal(signal.SIGINT)
            await self.wait_for_child(p, 5)

    async def test_signalctl_add_handler(self):
        test_prog = """\
        async def test_signalctl_wait_for_signal_child(self):
            done = asyncio.Future()
            expectation = [
                signal.SIGTERM,
                signal.SIGTERM,
                signal.SIGUSR1,
                signal.SIGINT,
                signal.SIGINT,
            ]
            n_len = len(expectation) + 1

            def handler(s):
                try:
                    self.assertEqual(s, expectation.pop(0))
                    self.notify_parent(n_len - len(expectation))
                    if not expectation:
                        done.set_result(None)
                except Exception as e:
                    done.set_exception(e)

            with signalctl.SignalController(*set(expectation)) as sc:
                sc.add_handler(handler)
                self.notify_parent(1)
                await done
        """

        async with spawn(test_prog) as p:
            await self.wait_for_child(p, 1)
            p.send_signal(signal.SIGTERM)
            await self.wait_for_child(p, 2)
            p.send_signal(signal.SIGTERM)
            await self.wait_for_child(p, 3)
            p.send_signal(signal.SIGUSR1)
            await self.wait_for_child(p, 4)
            p.send_signal(signal.SIGINT)
            await self.wait_for_child(p, 5)
            p.send_signal(signal.SIGINT)
            await self.wait_for_child(p, 6)
