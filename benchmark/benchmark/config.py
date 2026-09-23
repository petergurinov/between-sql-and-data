from __future__ import annotations
import os, tomllib
from pathlib import Path

REQUIRED = {
    "pg": ("host", "port", "user", "database", "password_env"),
    "ch": ("host", "http_port", "tcp_port", "user", "database", "password_env"),
}

def load(path: str | Path, *, require_secrets: bool = True, engines=("pg", "ch")) -> dict:
    with Path(path).open("rb") as stream:
        cfg = tomllib.load(stream)
    errors=[]
    for engine in engines:
        fields = REQUIRED[engine]
        section=cfg.get(engine, {})
        for name in fields:
            if section.get(name, "") == "": errors.append(f"[{engine}] {name} is required")
        env_name=section.get("password_env")
        if require_secrets and env_name and not os.environ.get(env_name): errors.append(f"environment variable {env_name} is empty")
    if errors: raise ValueError("; ".join(errors))
    return cfg

def connection_env(cfg: dict, engine: str) -> dict[str,str]:
    c=cfg[engine]; password=os.environ.get(c["password_env"], "")
    if engine == "pg":
        return {"PG_HOST":str(c["host"]),"PG_PORT":str(c["port"]),"PG_USER":str(c["user"]),
                "PG_DB":str(c["database"]),"PGPASSWORD":password,"PG_SSLMODE":str(c.get("sslmode","disable")),
                "PG_FLIGHT_PORT":str(c.get("flight_port",15432))}
    return {"CH_HOST":str(c["host"]),"CH_HTTP_PORT":str(c["http_port"]),
            "CH_TCP_PORT":str(c["tcp_port"]),"CH_USER":str(c["user"]),"CH_PASSWORD":password,
            "CH_DB":str(c["database"]),"CH_SECURE":"1" if c.get("secure",False) else "0",
            "CH_FLIGHT_PORT":str(c.get("flight_port",9090)),
            "CH_ADBC_DRIVER":str(c.get("adbc_driver_path",""))}
