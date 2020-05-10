class APIError(Exception):
    pass


class UnexpectedStatusCodeError(APIError):
    pass


class UnimplementedError(Exception):
    pass


class MissingCredentialsError(Exception):
    pass


class NoImagesInElasticsearchError(Exception):
    pass
