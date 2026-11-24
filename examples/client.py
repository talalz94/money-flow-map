"""How another app reads the money-flow service (start it first:  python -m moneyflow run).

    python examples/client.py            # one REST read
    python examples/client.py --stream   # live updates over the WebSocket (Ctrl+C to stop)
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from moneyflow.client import MoneyFlowClient  # noqa: E402

c = MoneyFlowClient("http://127.0.0.1:8765")


def one_read():
    snap = c.snapshot("today")
    risk = snap["risk"]
    print(f"Risk meter: {risk['label']} {risk['score']}  ({snap['window']['label']})")
    names = {n["id"]: n["label"] for n in snap["nodes"]}
    for n in sorted(snap["nodes"], key=lambda n: -(n["z"] or -99)):
        print(f"  {n['label']:30s} z={n['z']}  {n['change_pct']}%  [{n['status']}, {n['source']['name']}, age {n['age_s']}s]")
    for e in snap["edges"][:5]:
        print(f"  flow {names[e['from']]} -> {names[e['to']]}  weight {e['weight']}")
    for b in snap["narrative"]:
        print("  -", b["text"])


async def stream():
    async for snap in c.stream("1h"):
        top = snap["edges"][0] if snap["edges"] else None
        print(snap["generated_at"], snap["risk"]["label"], snap["risk"]["score"],
              f"strongest: {top['from']} -> {top['to']}" if top else "no clear flow")


if __name__ == "__main__":
    if "--stream" in sys.argv:
        try:
            asyncio.run(stream())
        except KeyboardInterrupt:
            pass
    else:
        one_read()
