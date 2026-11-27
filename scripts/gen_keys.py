"""Write dev API keys for team-a and team-b (git-ignored). Never overwrites existing files.

config/api_keys.yaml  -> read by the API server
config/rkp.toml       -> read by the rkp CLI (copy to ~/.config/rkp.toml to use it anywhere)
"""

import secrets
from pathlib import Path

TEAMS = ("team-a", "team-b")
ROOT = Path(__file__).resolve().parents[1]
server, client = ROOT / "config" / "api_keys.yaml", ROOT / "config" / "rkp.toml"

if server.exists() and client.exists():
    print(f"keys already exist in {server.parent.name}/; delete both files to regenerate")
    raise SystemExit(0)
if server.exists() or client.exists():
    raise SystemExit(f"only one of {server} / {client} exists; delete both to regenerate")
keys = {team: f"rkp_{secrets.token_urlsafe(24)}" for team in TEAMS}
server.parent.mkdir(exist_ok=True)
server.write_text("teams:\n" + "".join(f"  {t}: {k}\n" for t, k in keys.items()))
client.write_text(
    'api_url = "http://127.0.0.1:8000"\ndefault_team = "team-a"\n\n[keys]\n'
    + "".join(f'{t} = "{k}"\n' for t, k in keys.items())
)
server.chmod(0o600)
client.chmod(0o600)
print(f"wrote {server.relative_to(ROOT)} and {client.relative_to(ROOT)}")
