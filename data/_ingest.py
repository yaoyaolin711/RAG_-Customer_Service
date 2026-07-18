"""产品资料库 + 客服行为规范 -> Milvus rag_collection"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
for p in [os.path.join(os.path.dirname(os.path.dirname(__file__)), "crm_agent", "crm_agent"),
          os.path.join(os.path.dirname(os.path.dirname(__file__)), "RAG_mode", "mode")]:
    if os.path.isfile(os.path.join(p, "settings.py")):
        sys.path.append(p)
        break

from rag.structured_xlsx import load_structured_xlsx
from rag.structured_policy_md import load_structured_policy_md
from rag.pipeline import IngestPipeline
from settings import RAG_COLLECTION_NAME
from vectorstore import get_collection_count, get_collection_chunk_types, check_milvus_connection

xlsx_path = os.path.join(os.path.dirname(__file__), "客服团队_产品资料库.xlsx")
md_path = os.path.join(os.path.dirname(__file__), "客服岗位职责与行为规范 .md")

print(f"产品资料库: {xlsx_path}")
print(f"行为规范: {md_path}")

paths = [xlsx_path, md_path]
print("\n=== 切分预览 ===")
for path in paths:
    suffix = os.path.splitext(path)[1].lower()
    if suffix in (".xlsx", ".xls"):
        chunks = load_structured_xlsx(path)
    else:
        chunks = load_structured_policy_md(path)
    print(f"{os.path.basename(path)}: {len(chunks)} chunks, type={chunks[0].metadata.get('chunk_type')}")
    print(f"  sample: {chunks[0].page_content[:120]}")

check_milvus_connection()
pipeline = IngestPipeline()
result = pipeline.run(file_paths=paths, structured=True, rebuild=True, clear_all=True)

count = get_collection_count(result.collection_name)
types = get_collection_chunk_types(result.collection_name, sample_limit=2000)
print("\n=== 入库摘要 ===")
print(f"Collection: {result.collection_name}")
print(f"Chunks written: {result.chunk_count}")
print(f"Milvus row_count: {count}")
print(f"chunk_type: {types}")
