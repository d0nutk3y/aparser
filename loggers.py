import logging.handlers


class LoggerFactory:
    __app_name = 'aparser'

    @classmethod
    def create_stream_handler(cls):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        stream_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        stream_handler.setFormatter(stream_formatter)

        return stream_handler

    @classmethod
    def create_file_handler(cls, logging_level, filename):
        file_handler = logging.handlers.RotatingFileHandler(
            filename=filename,
            mode='a',
            maxBytes=1024 * 512,
            backupCount=1,
            encoding='utf8',
            delay=False
        )
        file_handler.setLevel(logging_level)

        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(file_formatter)

        return file_handler

    @classmethod
    def get_logger(cls,
                   name: str,
                   with_file_handlers=False):
        logger = logging.getLogger(name=name)

        logger.propagate = False
        logger.setLevel(logging.DEBUG)

        logger.addHandler(cls.create_stream_handler())

        if with_file_handlers:
            error_file_handler = cls.create_file_handler(
                logging_level=logging.WARNING,
                filename=f'{cls.__app_name}_error.log')
            logger.addHandler(error_file_handler)

            debug_file_handler = cls.create_file_handler(
                logging_level=logging.DEBUG,
                filename=f'{cls.__app_name}_debug.log')
            logger.addHandler(debug_file_handler)
        return logger
