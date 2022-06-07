from logging import Logger


def create_backoff_hdlr(logger: Logger):
    def backoff_req_hdlr(details):
        logger.info("Backing off {wait:0.1f} seconds after {tries} tries "
              "calling function {target} with args {args} and kwargs "
              "{kwargs}".format(**details))
    return backoff_req_hdlr
