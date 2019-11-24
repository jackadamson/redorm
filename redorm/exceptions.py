class RedormException(Exception):
    pass


class InstanceNotFound(RedormException):
    pass


class DevelopmentServerUsedInProd(RedormException):
    pass


class UniqueContstraintViolation(RedormException):
    pass


class UnknownFieldName(RedormException):
    pass


class FilterOnUnindexedField(RedormException):
    pass


class MultipleInstancesReturned(RedormException):
    pass
