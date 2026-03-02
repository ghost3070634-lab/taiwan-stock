from datetime import datetime

from notifier.discord_client import DiscordClient


def main():
    dc = DiscordClient()

    today = datetime.today().date().isoformat()
    # TODO: 實際應該從策略裡取得「符合四大濾網的候選股」
    lines = [
        f"【本週策略觀察清單】{today}",
        "",
        "（示意）後續請在這裡列出：代號｜名稱｜產業｜市值｜近 3 月 YoY｜投信持股｜技術面摘要",
    ]
    content = "\n".join(lines)
    dc.send("weekly", content)


if __name__ == "__main__":
    main()
