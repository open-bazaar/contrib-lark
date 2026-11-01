"""Send through bcc what each new marked message asks for."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

COMMAND = re.compile(r"\b(e2e-[0-9a-f]{12}) (reply|long|attach|quote|echo|approve)\b")
SENDER = r"(?:@(?P<handle>[^\s(:]+)(?:\([^)\n]*\))?: )?"
# A body runs to the next message, since a Lark post puts text and images on
# separate lines.
BODY = r"(?P<body>.*?)(?=\n\[target=|\n\[\d+/\d+ seq=|\Z)"
MESSAGE = re.compile(
    r"^\[target=(?P<target>.+?) msg=(?P<id>[0-9a-f-]+) [^\]\n]*?"
    r"(?: reply_to=(?P<reply>[0-9a-f-]+))?(?: edit_of=(?P<edit>[0-9a-f-]+))?\] "
    + SENDER
    + BODY,
    re.MULTILINE | re.DOTALL,
)
REFERENCE = re.compile(
    r"^\[\d+/\d+ seq=\d+ msg=(?P<id>[0-9a-f-]+) [^\]\n]*\] " + SENDER + BODY,
    re.MULTILINE | re.DOTALL,
)
ATTACHMENTS = re.compile(r" \[\d+ attachments?: .*\]$")
PATH = re.compile(r"path:([^,)\]]+)")
SECTIONS = 80


def send(bcc: str, target: str, body: str, *options: str) -> None:
    subprocess.run(
        [bcc, "message", "send", "--target", target, *options],
        input=body,
        text=True,
        check=True,
    )


def main() -> None:
    bcc = shutil.which("bcc")
    if bcc is None:
        raise FileNotFoundError("The harness session has no bcc command in PATH")
    inbox = subprocess.run(
        [bcc, "message", "check"], capture_output=True, text=True, check=True
    ).stdout
    quoted = {
        match["id"]: match["body"]
        for pattern in (REFERENCE, MESSAGE)
        for match in pattern.finditer(inbox)
    }
    for message in MESSAGE.finditer(inbox):
        command = COMMAND.search(message["body"])
        if command is None:
            continue
        marker, action = command.groups()
        target = message["target"]
        if action == "reply":
            send(bcc, target, f"{marker} reply", "--reply-to", message["id"])
        elif action == "long":
            sections = [f"# {marker} long"]
            for number in range(1, SECTIONS + 1):
                sections.append(
                    f"## {marker}-{number:04d}\n\n"
                    + "\n".join(
                        f"- item {item} of section {number} belongs to a checklist "
                        "that needs more than one Lark post"
                        for item in range(1, 7)
                    )
                )
                if number == SECTIONS // 2:
                    sections.append(
                        "```text\n"
                        + "\n".join(f"{marker} fenced {line}" for line in range(1, 21))
                        + "\n```"
                    )
            sections.append(f"END {marker} {SECTIONS}")
            send(bcc, target, "\n\n".join(sections))
        elif action == "attach":
            path = Path("attachment.txt")
            path.write_bytes(
                files("bcn_lark_test_harness").joinpath("attachment.txt").read_bytes()
            )
            send(bcc, target, "", "--attachment", str(path))
        elif action == "approve":
            print(json.dumps({"approval": {"command": f"touch {marker}"}}), flush=True)
            decision = json.loads(sys.stdin.readline())["decision"]
            send(bcc, target, f"{marker} {decision}", "--reply-to", message["id"])
        elif action == "quote":
            send(bcc, target, ATTACHMENTS.sub("", quoted[message["reply"]]))
        else:
            path = PATH.search(message["body"])
            if path is None:
                raise ValueError(f"{marker} echo arrived without an attachment path")
            send(bcc, target, "", "--attachment", path[1])


if __name__ == "__main__":
    main()
