import os
import json
from typing import Literal

import requests

ChannelType = Literal["weekly", "position", "exit"]


class DiscordClient:
    def __init__(self):
        self.webhooks = {
            "weekly": os.getenv("DISCORD_WEEKLY_WEBHOOK", ""),
            "position": os.getenv("DISCORD_POSITION_WEBHOOK", ""),
            "exit": os.getenv("DISCORD_EXIT_WEBHOOK", ""),
        }

    def send(self, channel: ChannelType, content: str):
        url = self.webhooks.get(channel)
        if not url:
            print(f"[WARN] No webhook url set for channel={channel}")
            return

        payload = {"content": content}
        resp = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code >= 300:
            print(f"[ERROR] Discord send failed {resp.status_code}: {resp.text}")
