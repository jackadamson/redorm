from environs import Env

env = Env()
env.read_env()
REDORM_URL = env.str("REDORM_URL", default="")
