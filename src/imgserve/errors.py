class AmbiguousDataError(Exception):
    pass


class APIError(Exception):
    pass


class UnexpectedStatusCodeError(APIError):
    pass


class InvalidSliceArgumentError(Exception):
    pass


class UnimplementedError(Exception):
    pass


class MalformedTagsError(Exception):
    pass


class MissingCredentialsError(Exception):
    pass


class NoDownloadsError(Exception):
    pass


class NoImagesInElasticsearchError(Exception):
    pass


class ElasticsearchError(Exception):
    pass


class ElasticsearchUnreachableError(ElasticsearchError):
    pass


class ElasticsearchNotReadyError(ElasticsearchError):
    pass


class MissingTemplateError(ElasticsearchError):
    pass


class NoQueriesGatheredError(Exception):
    pass
