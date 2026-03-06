import os
from datetime import datetime

def dump_html(name: str, html: str):
    os.makedirs("debug", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join("debug", f"{name}_{ts}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    #print(f"[debug] Wrote HTML dump: {path}")
