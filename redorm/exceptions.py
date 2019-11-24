class RedormException(Exception):
    pass


class RedisInstanceNotFound(RedormException):
    pass


class DevelopmentServerUsedInProd(RedormException):
    pass
