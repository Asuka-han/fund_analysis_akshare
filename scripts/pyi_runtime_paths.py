import sys
from pathlib import Path

# Ensure bundled exe can import project packages (src) when frozen.
def _add(path: Path):
    if path and path.exists():
        p = str(path)
        if p not in sys.path:
            sys.path.insert(0, p)

if getattr(sys, "frozen", False):
    base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
    exe_dir = Path(sys.executable).parent
    for candidate in (base, base / "src", exe_dir, exe_dir / "src"):
        _add(candidate)
else:
    here = Path(__file__).resolve().parent
    project_root = here.parent
    for candidate in (project_root, project_root / "src"):
        _add(candidate)
