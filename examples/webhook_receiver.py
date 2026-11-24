"""A minimal webhook receiver to try the push events (standard library only).

1. Run this:            python examples/webhook_receiver.py          (listens on 127.0.0.1:9000)
2. In moneyflow.toml:   [webhooks]
                        targets = [{ url = "http://127.0.0.1:9000/hook", events = ["regime_change", "strongest_flow_change", "extreme_move"] }]
3. Start moneyflow. Each event arrives as a JSON POST (schema "moneyflow.webhook/1").
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        print(f"{body.get('generated_at')}  {body.get('event'):24s} risk={body.get('risk')}  "
              f"{ {k: v for k, v in body.items() if k in ('from', 'to', 'id', 'z', 'score')} }")
        self.send_response(204)
        self.end_headers()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print("listening on http://127.0.0.1:9000/hook")
    HTTPServer(("127.0.0.1", 9000), Hook).serve_forever()
