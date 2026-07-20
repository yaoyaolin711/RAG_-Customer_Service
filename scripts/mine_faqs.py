"""
FAQ 自动挖掘 CLI — 从历史对话中提取高频问答，自动写入 FAQ 缓存。

用法:
  python scripts/mine_faqs.py                          # 默认阈值 3
  python scripts/mine_faqs.py --min-count 5             # 至少问 5 次才收录
  python scripts/mine_faqs.py --max-items 50            # 最多收 50 条
  python scripts/mine_faqs.py --clear                   # 清空追踪数据
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
for _p in [
    _REPO_ROOT / "crm_agent",
    _REPO_ROOT / "RAG_mode" / "mode",
]:
    if (_p / "settings.py").is_file():
        sys.path.append(str(_p))
        break

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="FAQ 自动挖掘")
    parser.add_argument("--min-count", type=int, default=3, help="频次阈值（默认 3）")
    parser.add_argument("--max-items", type=int, default=20, help="最大挖掘条数（默认 20）")
    parser.add_argument("--clear", action="store_true", help="清空追踪数据")
    args = parser.parse_args()

    if args.clear:
        from services.faq_mining_tracker import clear
        clear()
        print("追踪数据已清空")
        return

    from services.faq_mining import mine_candidates
    inserted = mine_candidates(min_count=args.min_count, max_items=args.max_items)

    print(f"\n新入库 FAQ: {len(inserted)} 条")
    for item in inserted:
        print(f"  #{item['id']} [{item['count']}次] {item['norm']}")
        print(f"    答案: {item['answer'][:60]}")


if __name__ == "__main__":
    main()
