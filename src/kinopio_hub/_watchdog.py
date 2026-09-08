"""Private broker owner; stdin EOF also handles an uncatchable SDK parent exit."""
import json
import shutil
import signal
import subprocess
import sys
import threading


def main() -> None:
    directory = sys.argv[1]
    stopped = threading.Event()
    broker: subprocess.Popen[bytes] | None = None

    def stop(*_: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def read_parent() -> None:
        try:
            for _ in sys.stdin.buffer:
                pass
        finally:
            stopped.set()

    try:
        # The parent sends the start command only after retaining the pipe handle.
        line = sys.stdin.buffer.readline()
        if not line:
            return
        command = json.loads(line)
        threading.Thread(target=read_parent, daemon=True).start()
        if stopped.is_set():
            return
        broker = subprocess.Popen([command['binary'], '-c', command['config']],
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL)
        print(json.dumps({'pid': broker.pid}), flush=True)
        while not stopped.wait(0.05) and broker.poll() is None:
            pass
    finally:
        if broker is not None and broker.poll() is None:
            broker.terminate()
            try:
                broker.wait(timeout=2)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait()
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == '__main__':
    main()
