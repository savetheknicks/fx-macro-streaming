import os

TIMESCALE_DSN = os.environ.get(
    "TIMESCALE_DSN",
    "postgresql://fxmacro:fxmacro@localhost:5432/fxmacro",
)