import os
from dotenv import load_dotenv

ENV_MODE = os.getenv("ENV_MODE", "local")
env_file = ".env.render" if ENV_MODE == "render" else ".env.local"

# load the appropriate env file
load_dotenv(env_file)

class Config:
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    SENTRY_DSN = os.getenv("SENTRY_DSN", "")
    FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
